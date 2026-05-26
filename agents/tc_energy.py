import re
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE


class TCEnergyAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            params = pipe["params"]
            asset_id = params["asset_id"]
            base_url = params["base_url"]
            pipe_id = pipe["id"]
            try:
                url_crit = (
                    f"{base_url}webmethods/SSRS_ListCriticalNotices.aspx"
                    f"?assetid={asset_id}"
                )
                html_crit = self.fetch_with_retry(url_crit)
                notices = self._parse_notice_list(
                    html_crit, pipe_id, critical=True,
                    base_url=base_url, asset_id=asset_id
                )

                url_non = (
                    f"{base_url}webmethods/SSRS_ListNonCriticalNotices.aspx"
                    f"?assetid={asset_id}"
                )
                html_non = self.fetch_with_retry(url_non)
                notices += self._parse_notice_list(
                    html_non, pipe_id, critical=False,
                    base_url=base_url, asset_id=asset_id
                )

                if pipe_id == "CGT":
                    notices += self._fetch_maintenance_timeline(base_url, pipe_id)

                all_events.extend(notices)
            except Exception as e:
                self.logger.error(f"{pipe_id} scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _parse_notice_list(
        self, html: str, pipe_id: str, critical: bool,
        base_url: str, asset_id: str
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []

        # Look for rows with notice IDs — handle both table and list structures
        notice_ids = []
        # Try table rows
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            for cell in cells:
                a = cell.find("a")
                if a:
                    href = a.get("href", "")
                    m = re.search(r'NoticeId[=:](\d+)', href, re.IGNORECASE)
                    if m:
                        notice_ids.append((m.group(1), a.get_text(strip=True)))
                        break
                # Check if cell text looks like a notice id
                text = cell.get_text(strip=True)
                if re.match(r'^\d{4,10}$', text):
                    notice_ids.append((text, ""))

        # Also scan all links for notice IDs
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r'[Nn]otice[Ii]d[=:](\d+)', href)
            if m:
                nid = m.group(1)
                if not any(n[0] == nid for n in notice_ids):
                    notice_ids.append((nid, a.get_text(strip=True)))

        for notice_id, list_title in notice_ids:
            try:
                detail_url = (
                    f"{base_url}ReportViewer.aspx"
                    f"?%2fInfoPost%2fNoticesSubreport"
                    f"&pNoticeId={notice_id}&AssetNbr={asset_id}"
                    f"&rs:Format=HTML4.0"
                )
                detail_html = self.fetch_with_retry(detail_url)
                detail_soup = BeautifulSoup(detail_html, "lxml")
                ev = self._parse_detail(
                    detail_soup, notice_id, pipe_id, critical,
                    list_title, base_url, asset_id
                )
                if ev:
                    events.append(ev)
                    # Check for PDFs in the detail page
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
                self.logger.warning(f"{pipe_id} notice {notice_id} detail failed: {e}")

        return events

    def _parse_detail(
        self, soup: BeautifulSoup, notice_id: str, pipe_id: str,
        critical: bool, list_title: str, base_url: str, asset_id: str
    ) -> dict | None:
        text_blocks = [t.get_text(" ", strip=True) for t in soup.find_all(["td", "p", "div", "span"])]
        full_text = " ".join(text_blocks)

        # Extract title
        title = list_title
        for tag in soup.find_all(["h1", "h2", "h3", "b", "strong"]):
            t = tag.get_text(strip=True)
            if len(t) > 10 and "notice" in t.lower():
                title = t
                break
        if not title:
            title = soup.get_text(" ", strip=True)[:120].strip()
        if not title:
            title = f"Notice {notice_id}"

        if NOTICE_SKIP_RE.search(title):
            return None

        # Extract dates
        start = end = posted_at = None
        date_patterns = [
            (r'(?:begin|start|effective)\s*(?:date|time)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "start"),
            (r'(?:end|expir|through|thru)\s*(?:date|time)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "end"),
            (r'(?:post(?:ed)?|issu(?:ed)?|creat(?:ed)?)\s*(?:date|time)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', "posted"),
        ]
        for pattern, dtype in date_patterns:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                parsed = self.parse_date(m.group(1))
                if dtype == "start":
                    start = parsed
                elif dtype == "end":
                    end = parsed
                elif dtype == "posted":
                    posted_at = parsed

        if not posted_at:
            posted_at = self.now_iso()

        # Extract locations
        locations = []
        loc_m = re.findall(
            r'(?:location|segment|zone|meter)\s*[:\-]?\s*([^\n\r<]{3,80})',
            full_text, re.IGNORECASE
        )
        for loc in loc_m[:5]:
            cleaned = self.norm_loc(loc.strip())
            if cleaned:
                locations.append(cleaned)

        # Extract capacity / flow detail
        available_cap = None
        cap_m = re.search(
            r'(?:available\s+cap(?:acity)?|max(?:imum)?(?:\s+daily)?\s+vol(?:ume)?'
            r'|dth|mcf|capacity)\s*[:\-]?\s*([\d,]+(?:\.\d+)?)',
            full_text, re.IGNORECASE
        )
        if cap_m:
            available_cap = cap_m.group(1).replace(",", "")

        flow_detail = full_text[:500] if full_text else ""

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
            "flow_detail": flow_detail,
            "affected_meters": None,
            "ebb_url": (
                f"{base_url}ReportViewer.aspx"
                f"?%2fInfoPost%2fNoticesSubreport"
                f"&pNoticeId={notice_id}&AssetNbr={asset_id}"
                f"&rs:Format=HTML4.0"
            ),
        }
        ev["severity"] = self.sev(ev)
        return self.validate_event(ev)

    def _fetch_maintenance_timeline(self, base_url: str, pipe_id: str) -> list[dict]:
        import json
        events = []
        try:
            url = (
                f"{base_url}webmethods/Documents_ListFiles_JSON.aspx"
                f"?folder=/cgt/CGTMaintenanceTimeline"
            )
            text = self.fetch_with_retry(url)
            try:
                data = json.loads(text)
            except Exception:
                return []
            files = data if isinstance(data, list) else data.get("files", data.get("Files", []))
            for f in files:
                file_url = f.get("url") or f.get("URL") or f.get("Path") or ""
                if not file_url:
                    name = f.get("name") or f.get("Name") or f.get("FileName") or ""
                    if name:
                        file_url = urljoin(base_url, f"DownloadFile.aspx?S3FN={name}")
                if not file_url:
                    continue
                if not file_url.startswith("http"):
                    file_url = urljoin(base_url, file_url)
                base_ev = {
                    "pipe": pipe_id,
                    "company": self.company_id,
                    "critical": False,
                    "notice_id": f"cgt_timeline_{re.sub(r'[^A-Za-z0-9]', '_', file_url)[-40:]}",
                    "title": f"CGT Maintenance Timeline",
                    "posted_at": self.now_iso(),
                    "last_seen_at": self.now_iso(),
                    "source": "cgt_maint_timeline",
                }
                pdf_evs = self.pdf_events_from_url(
                    file_url, {"id": pipe_id}, self.company_id, base_ev
                )
                for ev in pdf_evs:
                    ev["source"] = "cgt_maint_timeline"
                events.extend(pdf_evs)
        except Exception as e:
            self.logger.warning(f"CGT maintenance timeline failed: {e}")
        return events
