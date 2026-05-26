import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE


class BHEGTSAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            base_url = pipe["params"]["base_url"]
            pipe_id = pipe["id"]
            try:
                html = self.fetch_with_retry(base_url)
                events = self._parse_postings_page(html, pipe_id, base_url)
                all_events.extend(events)
            except Exception as e:
                self.logger.error(f"{pipe_id} scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _parse_postings_page(
        self, html: str, pipe_id: str, base_url: str
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []
        seen_ids = set()

        # BHE GTS InfoPost — similar structure to Enbridge
        notice_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r'[Nn]otice[Ii]d[=:](\d+)', href)
            if not m:
                m = re.search(r'/(\d{4,10})(?:/|$|\?)', href)
            if m:
                nid = m.group(1)
                if nid not in seen_ids:
                    seen_ids.add(nid)
                    notice_links.append((nid, a.get_text(strip=True), urljoin(base_url, href)))

        # Determine criticality by section
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(base_url, href)
            # Look for notice list endpoints
            if re.search(r'(critical|notice|posting)', href, re.IGNORECASE):
                if full_url not in [x[2] for x in notice_links]:
                    nid = re.sub(r'[^A-Za-z0-9]', '_', href)[-20:]
                    notice_links.append((nid, a.get_text(strip=True), full_url))

        for notice_id, list_title, detail_url in notice_links[:50]:
            try:
                # Determine critical from parent context
                critical = False
                parent_text = ""
                for a in soup.find_all("a", href=True):
                    if urljoin(base_url, a["href"]) == detail_url:
                        p = a.find_parent(["tr", "div", "section", "ul"])
                        if p:
                            parent_text = p.get_text(" ").lower()
                        break
                if "critical" in parent_text and "non-critical" not in parent_text:
                    critical = True

                detail_html = self.fetch_with_retry(detail_url)
                ev = self._parse_detail(
                    detail_html, notice_id, pipe_id, critical, list_title, detail_url
                )
                if ev:
                    events.append(ev)
                pdf_links = self.find_pdf_links(detail_html, base_url)
                for pdf_url in pdf_links:
                    base_ev = {
                        "pipe": pipe_id,
                        "company": self.company_id,
                        "critical": critical,
                        "notice_id": f"{notice_id}_pdf",
                        "title": list_title,
                        "posted_at": self.now_iso(),
                        "last_seen_at": self.now_iso(),
                        "ebb_url": detail_url,
                    }
                    events.extend(
                        self.pdf_events_from_url(
                            pdf_url, {"id": pipe_id}, self.company_id, base_ev
                        )
                    )
            except Exception as e:
                self.logger.warning(f"{pipe_id} notice {notice_id} failed: {e}")

        # Fallback: parse inline table
        if not events:
            rows = soup.find_all("tr")
            for i, row in enumerate(rows[1:], 1):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                title = " | ".join(c for c in cells if c)[:200]
                if NOTICE_SKIP_RE.search(title):
                    continue
                nid = f"{pipe_id}_row_{i}"
                ev = {
                    "notice_id": nid,
                    "pipe": pipe_id,
                    "company": self.company_id,
                    "title": title,
                    "source": "non_critical_notice",
                    "critical": False,
                    "posted_at": self.now_iso(),
                    "last_seen_at": self.now_iso(),
                    "start": None,
                    "end": None,
                    "locations": [],
                    "available_cap": None,
                    "flow_detail": None,
                    "affected_meters": None,
                    "ebb_url": base_url,
                }
                ev["severity"] = self.sev(ev)
                validated = self.validate_event(ev)
                if validated:
                    events.append(validated)

        return events

    def _parse_detail(
        self, html: str, notice_id: str, pipe_id: str,
        critical: bool, list_title: str, ebb_url: str
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
            (r'(?:post(?:ed)?|issu(?:ed)?|creat(?:ed)?)\s*(?:date)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "posted"),
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
            r'(?:location|segment|zone|meter|station|point)\s*[:\-]?\s*([^\n\r<]{3,80})',
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
            "ebb_url": ebb_url,
        }
        ev["severity"] = self.sev(ev)
        return self.validate_event(ev)
