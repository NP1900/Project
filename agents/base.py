import json
import re
import io
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, urljoin, parse_qs
import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except Exception:
    pdfplumber = None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120 Safari/537.36"
    )
}

NOTICE_SKIP_RE = re.compile(
    r'computer\s+system\s+status'
    r'|scheduling\s+alert\s*[–-]\s*edi'
    r'|monthly\s+customer\s+update'
    r'|tceconnects\s+auth'
    r'|okta'
    r'|ssl\s+certificate'
    r'|certificate\s+renewal'
    r'|certificate\s+expir'
    r'|system\s+maintenance'
    r'|password\s+reset'
    r'|\bIT\s+(system|maintenance|update|notice|alert)\b'
    r'|test\s+notice'
    r'|testing\s+only',
    re.IGNORECASE,
)

HIGH_RE = re.compile(
    r'\b(ofo|operational\s+flow\s+order|force\s+majeure|FM|emergency'
    r'|curtail|overpressur|imbalance|maop|capacity\s+constraint'
    r'|operational\s+alert|no[\s-]notice)\b',
    re.IGNORECASE,
)

MED_RE = re.compile(
    r'\b(maintenance|construction|compressor|constraint|underperform'
    r'|off[\s-]*rate|restriction|reduced)\b',
    re.IGNORECASE,
)

PDF_SKIP_RE = re.compile(
    r'(tariff|rate_schedule|form_of_service|index_of_customers'
    r'|blank|template|sample)',
    re.IGNORECASE,
)

ALLOWED_DOMAINS = {
    "enbridge.com", "infopost.enbridge.com",
    "kindermorgan.com", "pipeline2.kindermorgan.com",
    "williams.com", "1line.williams.com", "northwest.williams.com",
    "tceconnects.com", "ebb.tceconnects.com", "anrpl.com", "ebb.anrpl.com",
    "energytransfer.com",
    "bhegts.com", "infopost.bhegts.com", "dekaflow.bhegts.com",
    "gasquest.com",
    "eia.gov", "ferc.gov",
    "peplmessenger.energytransfer.com",
    "tgcmessenger.energytransfer.com",
    "twtransfer.energytransfer.com",
}

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

_LOC_PREFIX_RE = re.compile(
    r'^(?:segment|seg\.|zone|lateral|lat\.|meter|mtr\.|'
    r'delivery\s+point|receipt\s+point|station|point|'
    r'location\s+id|loc\s+id|location|loc\.)\s*',
    re.IGNORECASE,
)
_LOC_CODE_RE = re.compile(r'\([A-Za-z0-9]{3,10}\)', re.IGNORECASE)


class BasePipelineAgent(ABC):
    def __init__(self, company_id: str, pipelines: list, cache_path: str):
        self.company_id = company_id
        self.pipelines = pipelines
        self.cache_path = Path(cache_path)
        self.logger = logging.getLogger(company_id)
        self._pdf_cache: dict[str, tuple[float, list]] = {}

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Implemented by each company agent. Must NEVER raise."""

    def _check_url(self, url: str) -> None:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        host = host.lower()
        for domain in ALLOWED_DOMAINS:
            if host == domain or host.endswith("." + domain):
                return
        raise ValueError(f"URL not in ALLOWED_DOMAINS: {url!r}")

    def fetch(self, url: str, **kwargs) -> str:
        self._check_url(url)
        resp = requests.get(url, headers=HEADERS, timeout=45, **kwargs)
        resp.raise_for_status()
        return resp.text

    def fetch_with_retry(self, url: str, retries: int = 3, delay: int = 5) -> str:
        self._check_url(url)
        last_err = None
        for attempt in range(retries):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=45)
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise
                last_err = e
            except requests.exceptions.ConnectionError as e:
                last_err = e
            wait = delay * (2 ** attempt)
            self.logger.warning(f"Retry {attempt + 1}/{retries} for {url} after {wait}s: {last_err}")
            time.sleep(wait)
        raise last_err

    def fetch_bytes(self, url: str) -> bytes:
        self._check_url(url)
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.content

    def find_pdf_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        links = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            url = None

            # Pattern 1: direct .pdf
            if ".pdf" in href.lower():
                url = urljoin(base_url, href)

            # Pattern 2: TETCO GotoLINK GetLINKdocument.asp
            elif "getlinkdocument.asp" in href.lower():
                url = urljoin(base_url, href)

            # Pattern 3: TC Energy DownloadFile.aspx
            elif "downloadfile.aspx" in href.lower():
                url = urljoin(base_url, href)

            if url and url not in seen:
                if not PDF_SKIP_RE.search(url):
                    seen.add(url)
                    links.append(url)
        return links

    def parse_pdf(self, pdf_bytes: bytes, source_url: str) -> list[dict]:
        if pdfplumber is None:
            return []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                # Strategy 1: table extraction
                result = self._pdf_strategy_table(pdf, source_url)
                if result:
                    return result

                # Strategy 2: section headers
                result = self._pdf_strategy_sections(pdf, source_url)
                if result:
                    return result

                # Strategy 3: slash-delimited
                result = self._pdf_strategy_slash(pdf, source_url)
                if result:
                    return result
        except Exception as e:
            self.logger.warning(f"PDF parse error {source_url}: {e}")
        return []

    def _pdf_strategy_table(self, pdf, source_url: str) -> list[dict]:
        header_keywords = {"location", "segment", "meter", "start", "end",
                           "capacity", "restriction", "dth"}
        events = []
        for page in pdf.pages:
            for table in (page.extract_tables() or []):
                if not table:
                    continue
                headers = [str(c or "").lower().strip() for c in table[0]]
                if not any(kw in h for h in headers for kw in header_keywords):
                    continue
                col = {h: i for i, h in enumerate(headers)}
                for row in table[1:]:
                    if not row or all(not c for c in row):
                        continue
                    ev = {
                        "title": self._cell(row, col, "location", "segment", "meter", "description"),
                        "locations": [self._cell(row, col, "location", "segment", "meter")],
                        "start": self.parse_date(self._cell(row, col, "start", "begin", "from")),
                        "end": self.parse_date(self._cell(row, col, "end", "thru", "through", "to")),
                        "available_cap": self._cell(row, col, "capacity", "dth", "available", "restriction"),
                        "affected_meters": self._cell(row, col, "meter", "meters"),
                    }
                    ev["locations"] = [l for l in ev["locations"] if l]
                    if ev.get("title") or ev.get("locations"):
                        events.append(ev)
        return events

    def _pdf_strategy_sections(self, pdf, source_url: str) -> list[dict]:
        section_re = re.compile(
            r'^(LOCATION|SEGMENT|ZONE)\s*:\s*(.+)$', re.IGNORECASE | re.MULTILINE
        )
        date_re = re.compile(
            r'(START|BEGIN|END|THRU|THROUGH)\s*:\s*(\S+(?:\s+\S+)?)',
            re.IGNORECASE
        )
        text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        events = []
        for m in section_re.finditer(text):
            location = m.group(2).strip()
            snippet = text[m.start():m.start() + 400]
            ev = {"title": location, "locations": [location],
                  "start": None, "end": None, "available_cap": None, "affected_meters": None}
            for dm in date_re.finditer(snippet):
                label = dm.group(1).upper()
                val = self.parse_date(dm.group(2))
                if "START" in label or "BEGIN" in label:
                    ev["start"] = val
                elif label in ("END", "THRU", "THROUGH"):
                    ev["end"] = val
            events.append(ev)
        return events

    def _pdf_strategy_slash(self, pdf, source_url: str) -> list[dict]:
        text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        events = []
        for line in text.splitlines():
            parts = [p.strip() for p in line.split(" / ")]
            if len(parts) < 3:
                continue
            ev = {
                "title": parts[0],
                "locations": [parts[0]],
                "start": self.parse_date(parts[2]) if len(parts) > 2 else None,
                "end": self.parse_date(parts[3]) if len(parts) > 3 else None,
                "available_cap": parts[4] if len(parts) > 4 else None,
                "affected_meters": None,
            }
            if ev["title"]:
                events.append(ev)
        return events

    def _cell(self, row: list, col: dict, *keys) -> str:
        for k in keys:
            for header, idx in col.items():
                if k in header and idx < len(row):
                    val = row[idx]
                    return str(val).strip() if val else ""
        return ""

    def pdf_events_from_url(self, url: str, pipe: dict, company: str, base_event: dict) -> list[dict]:
        now = time.time()
        if url in self._pdf_cache:
            cached_time, cached_events = self._pdf_cache[url]
            if now - cached_time < 6 * 3600:
                return cached_events
        try:
            pdf_bytes = self.fetch_bytes(url)
            parsed = self.parse_pdf(pdf_bytes, url)
        except Exception as e:
            self.logger.warning(f"PDF fetch/parse failed {url}: {e}")
            self._pdf_cache[url] = (now, [])
            return []

        events = []
        for item in parsed:
            ev = {**base_event, **item}
            ev["source"] = "pdf_outage"
            ev.setdefault("notice_id", re.sub(r'[^A-Za-z0-9]', '_', url)[-64:])
            ev.setdefault("pipe", pipe.get("id", ""))
            ev.setdefault("company", company)
            ev.setdefault("critical", False)
            ev.setdefault("posted_at", self.now_iso())
            ev["last_seen_at"] = self.now_iso()
            ev["severity"] = self.sev(ev)
            validated = self.validate_event(ev)
            if validated:
                events.append(validated)

        self._pdf_cache[url] = (now, events)
        return events

    def norm_loc(self, s: str) -> str:
        if not s:
            return s
        cleaned = _LOC_PREFIX_RE.sub("", s.strip())
        cleaned = _LOC_CODE_RE.sub("", cleaned)
        cleaned = cleaned.strip(" \t\n\r.,;:-")
        return cleaned if cleaned else s

    def parse_date(self, s) -> str | None:
        if not s:
            return None
        s = str(s).strip()
        if not s or s.lower() in ("none", "n/a", "tbd", "ongoing", "until further notice"):
            return None
        # YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
        m = re.match(r'^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?', s)
        if m:
            yr, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hr, mi, sc = int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0)
            try:
                dt = datetime(yr, mo, dy, hr, mi, sc, tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                pass
        # MM/DD/YYYY or MM/DD/YYYY HH:MM or MM/DD/YYYY HH:MM:SS
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?', s)
        if m:
            mo, dy, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hr, mi, sc = int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0)
            try:
                dt = datetime(yr, mo, dy, hr, mi, sc, tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                pass
        # Month DD, YYYY
        m = re.match(
            r'^([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?', s
        )
        if m:
            mon_str = m.group(1).lower()
            mo = _MONTH_FULL.get(mon_str) or _MONTH_ABBR.get(mon_str[:3])
            if mo:
                dy, yr = int(m.group(2)), int(m.group(3))
                hr, mi, sc = int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0)
                try:
                    dt = datetime(yr, mo, dy, hr, mi, sc, tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    pass
        # DD-Mon-YYYY
        m = re.match(r'^(\d{1,2})-([A-Za-z]{3})-(\d{4})', s)
        if m:
            dy = int(m.group(1))
            mo = _MONTH_ABBR.get(m.group(2).lower())
            yr = int(m.group(3))
            if mo:
                try:
                    dt = datetime(yr, mo, dy, tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    pass
        return None

    def sev(self, event: dict) -> str:
        text = (event.get("title") or "") + " " + (event.get("flow_detail") or "")
        if event.get("critical") is True or HIGH_RE.search(text):
            return "HIGH"
        if MED_RE.search(text):
            return "MED"
        return "LOW"

    def validate_event(self, event: dict) -> dict | None:
        required = ["notice_id", "pipe", "company", "title", "source",
                    "critical", "posted_at", "last_seen_at"]
        for field in required:
            val = event.get(field)
            if val is None or val == "":
                return None
        event["notice_id"] = str(event["notice_id"])
        event["critical"] = bool(event["critical"])
        return event

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def load_cache(self) -> dict:
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_cache(self, cache: dict) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(cache, indent=2, default=str), encoding="utf-8")

    def merge_with_cache(self, new_events: list[dict], cache: dict) -> tuple[list[dict], dict]:
        now = self.now_iso()
        stale_cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

        # Upsert new events
        for ev in new_events:
            key = f"{ev.get('pipe', '')}::{ev.get('notice_id', '')}"
            ev["last_seen_at"] = now
            cache[key] = ev

        # Build result: new events + non-stale cache entries
        new_keys = {
            f"{ev.get('pipe', '')}::{ev.get('notice_id', '')}"
            for ev in new_events
        }
        result = list(new_events)
        result_keys = set(new_keys)

        for key, ev in cache.items():
            if key in result_keys:
                continue
            last_seen = ev.get("last_seen_at")
            if last_seen:
                try:
                    ls_dt = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                    if ls_dt < stale_cutoff:
                        continue  # drop stale
                except Exception:
                    pass
            result.append(ev)
            result_keys.add(key)

        return result, cache
