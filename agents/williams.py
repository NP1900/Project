import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE


class WilliamsAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            base_url = pipe["params"]["base_url"]
            pipe_id = pipe["id"]
            try:
                if pipe_id == "TRANSCO":
                    events = self._scrape_transco(base_url, pipe_id)
                else:
                    events = self._scrape_generic(base_url, pipe_id)
                all_events.extend(events)
            except Exception as e:
                self.logger.error(f"{pipe_id} scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _scrape_transco(self, base_url: str, pipe_id: str) -> list[dict]:
        events = []
        endpoints = [
            ("info-postings/notices/critical-notices.html", True),
            ("info-postings/notices/non-critical-notices.html", False),
            ("info-postings/maintenance/", False),
        ]
        for path, critical in endpoints:
            url = urljoin(base_url, path)
            try:
                html = self.fetch_with_retry(url)
                page_events = self._parse_notice_page(html, url, pipe_id, critical, base_url)
                events.extend(page_events)
            except Exception as e:
                self.logger.warning(f"{pipe_id} {path} failed: {e}")
        return events

    def _scrape_generic(self, base_url: str, pipe_id: str) -> list[dict]:
        events = []
        try:
            html = self.fetch_with_retry(base_url)
            events.extend(self._parse_notice_page(html, base_url, pipe_id, None, base_url))
        except Exception as e:
            self.logger.warning(f"{pipe_id} home scrape failed: {e}")
        return events

    def _parse_notice_page(
        self, html: str, page_url: str, pipe_id: str,
        critical: bool | None, base_url: str
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []
        seen_ids = set()

        # Find all notice links
        notice_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Williams uses numeric IDs in URLs
            m = re.search(r'/(\d{4,10})(?:/|$|\?)', href)
            if m or "notice" in href.lower() or "posting" in href.lower():
                full_url = urljoin(base_url, href)
                nid = m.group(1) if m else re.sub(r'[^A-Za-z0-9]', '_', href)[-20:]
                title = a.get_text(strip=True)
                if nid not in seen_ids and title:
                    seen_ids.add(nid)
                    notice_links.append((nid, title, full_url))

        # Inline parsing if no sub-pages found
        if not notice_links:
            rows = soup.find_all("tr")
            for i, row in enumerate(rows):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2 and cells[0] and not cells[0].lower().startswith("notice"):
                    title = " | ".join(c for c in cells if c)
                    nid = f"{pipe_id}_row_{i}"
                    ev = self._build_inline_event(title, nid, pipe_id, critical)
                    if ev:
                        events.append(ev)
            # Scan for PDFs
            pdf_links = self.find_pdf_links(html, base_url)
            for pdf_url in pdf_links:
                base_ev = {
                    "pipe": pipe_id,
                    "company": self.company_id,
                    "critical": bool(critical),
                    "notice_id": f"pdf_{re.sub(r'[^A-Za-z0-9]', '_', pdf_url)[-30:]}",
                    "title": f"{pipe_id} Maintenance PDF",
                    "posted_at": self.now_iso(),
                    "last_seen_at": self.now_iso(),
                    "ebb_url": page_url,
                }
                events.extend(
                    self.pdf_events_from_url(pdf_url, {"id": pipe_id}, self.company_id, base_ev)
                )
            return events

        for notice_id, list_title, detail_url in notice_links[:50]:
            try:
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
                        "critical": bool(critical),
                        "notice_id": f"{notice_id}_pdf",
                        "title": list_title,
                        "posted_at": self.now_iso(),
                        "last_seen_at": self.now_iso(),
                        "ebb_url": detail_url,
                    }
                    events.extend(
                        self.pdf_events_from_url(pdf_url, {"id": pipe_id}, self.company_id, base_ev)
                    )
            except Exception as e:
                self.logger.warning(f"{pipe_id} notice {notice_id} failed: {e}")

        return events

    def _build_inline_event(
        self, title: str, notice_id: str, pipe_id: str, critical: bool | None
    ) -> dict | None:
        if NOTICE_SKIP_RE.search(title):
            return None
        ev = {
            "notice_id": notice_id,
            "pipe": pipe_id,
            "company": self.company_id,
            "title": title[:200],
            "source": "non_critical_notice",
            "critical": bool(critical),
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
        return self.validate_event(ev)

    def _parse_detail(
        self, html: str, notice_id: str, pipe_id: str,
        critical: bool | None, list_title: str, ebb_url: str
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
            (r'(?:post(?:ed)?|issu(?:ed)?|publish)\s*(?:date)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "posted"),
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
            r'(?:location|segment|zone|meter|lat(?:eral)?|station)\s*[:\-]?\s*([^\n\r<]{3,80})',
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
            "critical": bool(critical),
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
