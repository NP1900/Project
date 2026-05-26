import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE


class KinderMorganAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            code = pipe["params"]["code"]
            base_url = pipe["params"]["base_url"]
            pipe_id = pipe["id"]
            try:
                for notice_type, critical in [("C", True), ("NC", False)]:
                    url = f"{base_url}Notices/Notices.aspx?code={code}&type={notice_type}"
                    try:
                        html = self.fetch_with_retry(url)
                        notices = self._parse_notice_list(
                            html, pipe_id, code, critical, base_url
                        )
                        all_events.extend(notices)
                    except Exception as e:
                        self.logger.warning(f"{pipe_id} type={notice_type} failed: {e}")
            except Exception as e:
                self.logger.error(f"{pipe_id} scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _parse_notice_list(
        self, html: str, pipe_id: str, code: str, critical: bool, base_url: str
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []
        notice_ids = []

        # Extract notice IDs from links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r'noticeId[=:](\d+)', href, re.IGNORECASE)
            if m:
                nid = m.group(1)
                if nid not in [x[0] for x in notice_ids]:
                    notice_ids.append((nid, a.get_text(strip=True)))

        # Also check table rows for ID-only cells
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            for cell in cells:
                text = cell.get_text(strip=True)
                if re.match(r'^\d{4,10}$', text):
                    if text not in [x[0] for x in notice_ids]:
                        # Find link in same row
                        a = row.find("a")
                        title = a.get_text(strip=True) if a else ""
                        notice_ids.append((text, title))

        for notice_id, list_title in notice_ids:
            try:
                detail_url = f"{base_url}Notices/NoticeDetail.aspx?noticeId={notice_id}&code={code}"
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
                            "title": ev.get("title", ""),
                            "posted_at": ev.get("posted_at", self.now_iso()),
                            "last_seen_at": self.now_iso(),
                            "ebb_url": detail_url,
                        }
                        pdf_evs = self.pdf_events_from_url(
                            pdf_url, {"id": pipe_id}, self.company_id, base_ev
                        )
                        events.extend(pdf_evs)
            except Exception as e:
                self.logger.warning(f"{pipe_id} notice {notice_id} failed: {e}")

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
            r'(?:location|segment|zone|meter|lat(?:eral)?)\s*[:\-]?\s*([^\n\r<]{3,80})',
            full_text, re.IGNORECASE
        )[:5]:
            c = self.norm_loc(loc.strip())
            if c:
                locations.append(c)

        available_cap = None
        cap_m = re.search(
            r'(?:available\s+cap(?:acity)?|max(?:imum)?\s+(?:daily\s+)?vol(?:ume)?'
            r'|dth|mcf)\s*[:\-]?\s*([\d,]+(?:\.\d+)?)',
            full_text, re.IGNORECASE
        )
        if cap_m:
            available_cap = cap_m.group(1).replace(",", "")

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
            "available_cap": available_cap,
            "flow_detail": full_text[:500],
            "affected_meters": None,
            "ebb_url": ebb_url,
        }
        ev["severity"] = self.sev(ev)
        return self.validate_event(ev)
