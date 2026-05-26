import re
from urllib.parse import urljoin, parse_qs, urlparse
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE


class EnbridgeAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            pipe_code = pipe["params"]["pipe_code"]
            base_url = pipe["params"]["base_url"]
            pipe_id = pipe["id"]
            try:
                home_url = f"{base_url}{pipe_code}Home.asp?Pipe={pipe_code}"
                html = self.fetch_with_retry(home_url)
                notices = self._parse_home(html, pipe_id, base_url, pipe_code)
                all_events.extend(notices)
            except Exception as e:
                self.logger.error(f"{pipe_id} scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _parse_home(
        self, html: str, pipe_id: str, base_url: str, pipe_code: str
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []

        # Find all notice links — Enbridge uses table rows with notice links
        notice_links = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Critical/non-critical notice detail pages
            m = re.search(r'NoticeId[=:](\d+)', href, re.IGNORECASE)
            if m:
                nid = m.group(1)
                if nid not in notice_links:
                    notice_links[nid] = {
                        "url": urljoin(base_url, href),
                        "title": a.get_text(strip=True),
                    }

        # Determine criticality by section context
        full_text = soup.get_text(" ").lower()
        critical_section_ids = set()
        non_critical_section_ids = set()

        # Walk sections
        for section in soup.find_all(["table", "div", "section"]):
            section_text = section.get_text(" ").lower()
            is_critical = "critical" in section_text and "non-critical" not in section_text
            for a in section.find_all("a", href=True):
                m = re.search(r'NoticeId[=:](\d+)', a["href"], re.IGNORECASE)
                if m:
                    nid = m.group(1)
                    if is_critical:
                        critical_section_ids.add(nid)
                    else:
                        non_critical_section_ids.add(nid)

        for notice_id, info in notice_links.items():
            critical = notice_id in critical_section_ids
            try:
                detail_html = self.fetch_with_retry(info["url"])
                ev = self._parse_notice_detail(
                    detail_html, notice_id, pipe_id, pipe_code,
                    critical, info["title"], base_url
                )
                if ev:
                    events.append(ev)
                    # Scan for PDFs (GotoLINK and direct)
                    pdf_links = self.find_pdf_links(detail_html, base_url)
                    for pdf_url in pdf_links:
                        base_ev = {
                            "pipe": pipe_id,
                            "company": self.company_id,
                            "critical": critical,
                            "notice_id": f"{notice_id}_pdf",
                            "title": ev.get("title", ""),
                            "posted_at": ev.get("posted_at", self.now_iso()),
                            "last_seen_at": self.now_iso(),
                            "ebb_url": info["url"],
                        }
                        pdf_evs = self.pdf_events_from_url(
                            pdf_url, {"id": pipe_id}, self.company_id, base_ev
                        )
                        events.extend(pdf_evs)
            except Exception as e:
                self.logger.warning(f"{pipe_id} notice {notice_id} failed: {e}")

        # Maintenance schedule section
        maint_events = self._parse_maintenance_schedule(soup, pipe_id, base_url, pipe_code)
        events.extend(maint_events)

        return events

    def _parse_notice_detail(
        self, html: str, notice_id: str, pipe_id: str, pipe_code: str,
        critical: bool, list_title: str, base_url: str
    ) -> dict | None:
        soup = BeautifulSoup(html, "lxml")
        full_text = soup.get_text(" ", strip=True)

        title = list_title or f"Notice {notice_id}"
        if NOTICE_SKIP_RE.search(title):
            return None

        start = end = posted_at = None
        for pattern, dtype in [
            (r'(?:begin|start|effective)\s*(?:date)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "start"),
            (r'(?:end|expir|through|thru)\s*(?:date)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "end"),
            (r'(?:post(?:ed)?|issu(?:ed)?)\s*(?:date)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "posted"),
        ]:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                parsed = self.parse_date(m.group(1))
                if dtype == "start":
                    start = parsed
                elif dtype == "end":
                    end = parsed
                else:
                    posted_at = parsed

        posted_at = posted_at or self.now_iso()

        locations = []
        for loc in re.findall(
            r'(?:location|segment|zone|meter|lateral)\s*[:\-]?\s*([^\n\r<]{3,80})',
            full_text, re.IGNORECASE
        )[:5]:
            c = self.norm_loc(loc.strip())
            if c:
                locations.append(c)

        ev = {
            "notice_id": notice_id,
            "pipe": pipe_id,
            "company": self.company_id,
            "title": title,
            "source": "critical_notice" if critical else "non_critical_notice",
            "critical": critical,
            "posted_at": posted_at,
            "last_seen_at": self.now_iso(),
            "start": start,
            "end": end,
            "locations": locations,
            "available_cap": None,
            "flow_detail": full_text[:500],
            "affected_meters": None,
            "ebb_url": None,
        }
        ev["severity"] = self.sev(ev)
        return self.validate_event(ev)

    def _parse_maintenance_schedule(
        self, soup: BeautifulSoup, pipe_id: str, base_url: str, pipe_code: str
    ) -> list[dict]:
        events = []
        # Look for maintenance schedule tables or sections
        for tag in soup.find_all(string=re.compile(r'maintenance\s+schedule', re.IGNORECASE)):
            parent = tag.find_parent(["table", "div", "section"])
            if not parent:
                continue
            rows = parent.find_all("tr")
            for i, row in enumerate(rows[1:], 1):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                title = " | ".join(c for c in cells if c)
                if NOTICE_SKIP_RE.search(title):
                    continue
                ev = {
                    "notice_id": f"{pipe_id}_maint_{i}",
                    "pipe": pipe_id,
                    "company": self.company_id,
                    "title": title[:200],
                    "source": "maint_schedule",
                    "critical": False,
                    "posted_at": self.now_iso(),
                    "last_seen_at": self.now_iso(),
                    "start": None,
                    "end": None,
                    "locations": [],
                    "available_cap": None,
                    "flow_detail": None,
                    "affected_meters": None,
                    "ebb_url": None,
                }
                ev["severity"] = self.sev(ev)
                validated = self.validate_event(ev)
                if validated:
                    events.append(validated)
        return events
