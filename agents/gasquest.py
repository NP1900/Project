import re
from urllib.parse import urljoin, parse_qs, urlparse
from bs4 import BeautifulSoup
from agents.base import BasePipelineAgent, NOTICE_SKIP_RE

# GasQuest URL changed May 21 2025.
# New: https://gasquest.com/informational-posting
# Old bwpipelines.com and txgt.com are dead — never use them.


class GasQuestAgent(BasePipelineAgent):
    def scrape(self) -> list[dict]:
        cache = self.load_cache()
        all_events = []

        # GasQuest serves both GSPL and TXGT from the same URL
        # differentiated by tsp_id in notice data
        tsp_map = {}
        for pipe in self.pipelines:
            if not pipe.get("active"):
                continue
            tsp_id = pipe["params"].get("tsp_id")
            if tsp_id:
                tsp_map[tsp_id] = pipe["id"]
            else:
                tsp_map["default"] = pipe["id"]

        base_url = None
        for pipe in self.pipelines:
            if pipe.get("active"):
                base_url = pipe["params"]["base_url"]
                break

        if not base_url:
            return []

        try:
            html = self.fetch_with_retry(base_url)
            events = self._parse_gasquest_page(html, base_url, tsp_map)
            all_events.extend(events)
        except Exception as e:
            self.logger.error(f"GasQuest scrape failed: {e}")

        merged, cache = self.merge_with_cache(all_events, cache)
        self.save_cache(cache)
        return merged

    def _parse_gasquest_page(
        self, html: str, base_url: str, tsp_map: dict
    ) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        events = []
        seen_ids = set()

        # GasQuest uses table of notices with links to detail pages
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(base_url, href)

            # Extract notice ID from URL
            m = re.search(r'[/=](\d{4,12})(?:[/&?]|$)', href)
            nid = m.group(1) if m else None

            if not nid:
                # Try query string
                qs = parse_qs(urlparse(href).query)
                for key in ("id", "noticeId", "notice_id", "postingId"):
                    if key in qs:
                        nid = qs[key][0]
                        break

            if not nid:
                continue
            if nid in seen_ids:
                continue
            seen_ids.add(nid)

            list_title = a.get_text(strip=True)

            try:
                detail_html = self.fetch_with_retry(full_url)
                ev = self._parse_detail(
                    detail_html, nid, list_title, full_url, tsp_map
                )
                if ev:
                    events.append(ev)
                pdf_links = self.find_pdf_links(detail_html, base_url)
                for pdf_url in pdf_links:
                    pipe_id = ev.get("pipe", list(tsp_map.values())[0]) if ev else list(tsp_map.values())[0]
                    base_ev = {
                        "pipe": pipe_id,
                        "company": self.company_id,
                        "critical": ev.get("critical", False) if ev else False,
                        "notice_id": f"{nid}_pdf",
                        "title": list_title,
                        "posted_at": self.now_iso(),
                        "last_seen_at": self.now_iso(),
                        "ebb_url": full_url,
                    }
                    events.extend(
                        self.pdf_events_from_url(
                            pdf_url, {"id": pipe_id}, self.company_id, base_ev
                        )
                    )
            except Exception as e:
                self.logger.warning(f"GasQuest notice {nid} failed: {e}")

        # Fallback: inline table parsing
        if not events:
            default_pipe = list(tsp_map.values())[0] if tsp_map else "GSPL"
            rows = soup.find_all("tr")
            for i, row in enumerate(rows[1:], 1):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                title = " | ".join(c for c in cells if c)[:200]
                if NOTICE_SKIP_RE.search(title):
                    continue

                # Try to detect which TSP from row content
                pipe_id = self._detect_pipe(title, cells, tsp_map, default_pipe)
                nid = f"{pipe_id}_row_{i}"
                if nid in seen_ids:
                    continue
                seen_ids.add(nid)

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

    def _detect_pipe(
        self, title: str, cells: list, tsp_map: dict, default: str
    ) -> str:
        combined = " ".join(cells).lower()
        if "gulf south" in combined or "gspl" in combined:
            for tid, pid in tsp_map.items():
                if pid == "GSPL":
                    return "GSPL"
        if "texas gas" in combined or "txgt" in combined:
            for tid, pid in tsp_map.items():
                if pid == "TXGT":
                    return "TXGT"
        for tid, pid in tsp_map.items():
            if tid != "default":
                return pid
        return default

    def _parse_detail(
        self, html: str, notice_id: str, list_title: str,
        ebb_url: str, tsp_map: dict
    ) -> dict | None:
        soup = BeautifulSoup(html, "lxml")
        full_text = soup.get_text(" ", strip=True)

        # Detect pipe from detail text
        default_pipe = list(tsp_map.values())[0] if tsp_map else "GSPL"
        pipe_id = self._detect_pipe(list_title, [full_text[:300]], tsp_map, default_pipe)

        title = list_title or f"Notice {notice_id}"
        if NOTICE_SKIP_RE.search(title):
            return None

        # Detect criticality
        critical = bool(re.search(r'\bcritical\b', full_text[:500], re.IGNORECASE)
                        and not re.search(r'non-?critical', full_text[:500], re.IGNORECASE))

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
