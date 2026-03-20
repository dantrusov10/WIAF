import os
import re
import json
import hashlib
from html import escape
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests
import feedparser
from bs4 import BeautifulSoup, FeatureNotFound
from dateutil import parser as date_parser

from sources import SOURCES
from indices_sources import INDEX_SOURCES
from fx_sources import FX_SOURCES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "public", "data")
NEWS_DIR = os.path.join(BASE_DIR, "news")
SITE_URL = "https://wiaf.ru"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WIAFBot/1.3; +https://wiaf.ru)",
    "Accept-Language": "ru,en;q=0.8",
}
TIMEOUT = 20
RETENTION_DAYS = 365
MAX_ITEMS_PER_SOURCE = 20
MAX_FINAL_ITEMS = 300
ARTICLE_DETAIL_FETCH_LIMIT = 12
MIN_DETAIL_FETCH_PER_SOURCE = 6
MAX_RATE_ITEMS = 120

RUS_MONTHS = {
    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06',
    'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
}


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(NEWS_DIR, exist_ok=True)
    with open(os.path.join(BASE_DIR, '.nojekyll'), 'w', encoding='utf-8') as f:
        f.write('')


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def cutoff_dt() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)


def cutoff_iso() -> str:
    return cutoff_dt().replace(microsecond=0).isoformat()


def load_existing(filename: str) -> dict:
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json(filename: str, payload: dict):
    ensure_dirs()
    with open(os.path.join(DATA_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def normalize_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def make_hash(*parts: str) -> str:
    raw = "|".join([normalize_text(p or "") for p in parts])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def russian_month_normalize(value: str) -> str:
    txt = f" {value.lower()} "
    for k, v in RUS_MONTHS.items():
        txt = txt.replace(f" {k} ", f".{v}.")
    return re.sub(r"\s+", " ", txt).strip(" .")


def parse_date(value: str) -> str:
    if not value:
        return ""
    raw = normalize_text(str(value))
    if not raw:
        return ""
    candidates = [raw, russian_month_normalize(raw)]
    m = re.search(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:[+-]\d{2}:?\d{2}|Z)?)', raw)
    if m:
        candidates.insert(0, m.group(1))
    m = re.search(r'(\d{2}[./]\d{2}[./]\d{4}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)', raw)
    if m:
        candidates.insert(0, m.group(1).replace('/', '.'))
    for candidate in candidates:
        try:
            parsed = date_parser.parse(candidate, dayfirst=True, fuzzy=True)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.replace(microsecond=0).isoformat()
        except Exception:
            pass
        try:
            parsed = parsedate_to_datetime(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.replace(microsecond=0).isoformat()
        except Exception:
            pass
    return ""


def sort_key_date(value: str) -> int:
    if not value:
        return 0
    try:
        norm = value if "T" in value else value + "T00:00:00+00:00"
        return int(datetime.fromisoformat(norm.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def within_retention(date_str: str) -> bool:
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt >= cutoff_dt()
    except Exception:
        return False


def localize_category(category: str) -> str:
    mapping = {
        "news": "новости",
        "analytics": "аналитика",
        "analysis": "аналитика",
        "rates": "ставки",
        "ports": "порты",
        "rail": "железная дорога",
        "road": "автологистика",
        "air": "авиа",
        "sea": "морская логистика",
        "regulation": "регулирование",
        "operator_news": "новости операторов",
        "infrastructure": "инфраструктура",
        "restrictions": "ограничения",
        "multimodal": "мультимодальная логистика",
        "market": "рынок",
    }
    return mapping.get((category or "news").lower(), "новости")


def category_by_text(text: str, default_category: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["ставк", "tariff", "rate", "freight", "scfi", "wci"]):
        return "rates"
    if any(word in t for word in ["порт", "port", "terminal", "берег", "пристан"]):
        return "ports"
    if any(word in t for word in ["rail", "жд", "railway", "поезд", "контрейлер", "транссиб"]):
        return "rail"
    if any(word in t for word in ["авто", "truck", "road", "грузов"]):
        return "road"
    if any(word in t for word in ["аналит", "analysis", "review", "обзор"]):
        return "analytics"
    if any(word in t for word in ["тамож", "customs", "фтс", "санкц", "регулир"]):
        return "regulation"
    if any(word in t for word in ["море", "sea", "судоход", "судн", "контейнерн"]):
        return "sea"
    if any(word in t for word in ["мультимод", "transit", "corridor", "коридор"]):
        return "multimodal"
    return default_category or "news"


def transport_by_text(text: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["container", "мор", "sea", "vessel", "port", "feu", "teu", "линия", "судно"]):
        return "морская"
    if any(word in t for word in ["rail", "жд", "railway", "train", "вагон", "ржд"]):
        return "железнодорожная"
    if any(word in t for word in ["truck", "авто", "road", "грузов"]):
        return "авто"
    if any(word in t for word in ["air", "авиа", "cargo terminal"]):
        return "авиа"
    if any(word in t for word in ["multimodal", "мультимод"]):
        return "мультимодальная"
    return "смешанная"


def freshness_label(date_str: str) -> str:
    if not date_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except ValueError:
        return "unknown"
    delta_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    if delta_hours <= 48:
        return "fresh"
    if delta_hours <= 168:
        return "recent"
    return "archive"


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text


CYR_MAP = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y',
    'к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f',
    'х':'h','ц':'ts','ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'
}

def translit_ru(text: str) -> str:
    out = []
    for ch in (text or '').lower():
        out.append(CYR_MAP.get(ch, ch))
    return ''.join(out)

def slugify(text: str) -> str:
    base = translit_ru(text or '')
    cleaned = re.sub(r'[^a-z0-9\s-]', '', base).strip().lower()
    cleaned = re.sub(r'\s+', '-', cleaned)
    cleaned = re.sub(r'-+', '-', cleaned).strip('-')
    return cleaned[:80] or 'news'


def item_from_fields(source: dict, title: str, link: str, snippet: str = "", published_at: str = "", image_url: str = "", content_preview: str = "") -> dict | None:
    title = normalize_text(title)
    link = (link or "").strip()
    snippet = normalize_text(snippet)
    if not title or not link or link.startswith("javascript:"):
        return None
    item_hash = make_hash(source["source_name"], title, link)
    category = category_by_text(f"{title} {snippet} {content_preview}", source.get("category_default", "news"))
    slug = f"{source['source_id']}-{slugify(title)[:48]}-{item_hash[:6]}"
    return {
        "id": f"news_{item_hash[:12]}",
        "slug": slug,
        "source": source["source_name"],
        "source_id": source["source_id"],
        "source_group": source.get("source_group", "other"),
        "lang": source.get("language", ""),
        "priority": source.get("priority", 0),
        "date": published_at,
        "published_at": published_at,
        "title": title,
        "snippet": snippet[:500],
        "content_preview": normalize_text(content_preview or snippet)[:5000],
        "link": link,
        "image_url": (image_url or "").strip(),
        "category": category,
        "category_ru": localize_category(category),
        "transport_type": transport_by_text(f"{title} {snippet} {content_preview}"),
        "region": source.get("country", "unknown"),
        "freshness": freshness_label(published_at),
        "hash": item_hash,
        "status": "published",
    }


def article_detail_extract(source: dict, url: str) -> dict:
    html = fetch_html(url)
    soup = make_soup(html)
    title = ""
    title_meta = soup.select_one('meta[property="og:title"], meta[name="twitter:title"]')
    if title_meta and title_meta.get('content'):
        title = normalize_text(title_meta.get('content'))
    if not title and soup.title:
        title = normalize_text(soup.title.get_text(' ', strip=True))

    image_url = ""
    img_meta = soup.select_one('meta[property="og:image"], meta[name="twitter:image"]')
    if img_meta and img_meta.get('content'):
        image_url = urljoin(url, img_meta.get('content'))

    summary = ""
    summary_meta = soup.select_one('meta[property="og:description"], meta[name="description"], meta[name="twitter:description"]')
    if summary_meta and summary_meta.get('content'):
        summary = normalize_text(summary_meta.get('content'))

    published_at = ""
    date_candidates = []
    for sel in [
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[itemprop="datePublished"]',
        'time[datetime]',
        'meta[property="og:updated_time"]',
    ]:
        node = soup.select_one(sel)
        if not node:
            continue
        content = node.get('content') or node.get('datetime') or node.get_text(' ', strip=True)
        if content:
            date_candidates.append(content)
    if not date_candidates:
        page_text = normalize_text(soup.get_text(' ', strip=True))[:4000]
        for pattern in [
            r'\b\d{2}[./]\d{2}[./]\d{4}(?:\s+\d{2}:\d{2})?\b',
            r'\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?\b',
            r'\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+\d{4}(?:\s+\d{2}:\d{2})?\b',
        ]:
            m = re.search(pattern, page_text, flags=re.IGNORECASE)
            if m:
                date_candidates.append(m.group(0))
                break
    for dc in date_candidates:
        published_at = parse_date(dc)
        if published_at:
            break

    content_parts = []
    for sel in source.get('article_body_selectors', [
        'article', '.article-content', '.news-detail', '.post-content', '.entry-content', '.content', '.text', '.news-item', '.page-content'
    ]):
        for node in soup.select(sel):
            txt = normalize_text(node.get_text(' ', strip=True))
            if len(txt) > 220:
                content_parts.append(txt)
        if content_parts:
            break
    content_preview = ' '.join(content_parts)[:5000] if content_parts else ''
    if not summary:
        summary = content_preview[:420]
    return {
        'title': title,
        'image_url': image_url,
        'published_at': published_at,
        'snippet': summary[:500],
        'content_preview': content_preview[:5000],
    }


def enrich_item_from_article(source: dict, item: dict) -> dict:
    if not item.get('link'):
        return item
    needs_detail = (not item.get('published_at')) or (not item.get('image_url')) or len(item.get('content_preview', '')) < 140
    if not needs_detail:
        return item
    try:
        detail = article_detail_extract(source, item['link'])
    except Exception:
        return item
    if detail.get('title') and len(detail['title']) > len(item.get('title', '')):
        item['title'] = detail['title']
    if detail.get('published_at') and not item.get('published_at'):
        item['published_at'] = detail['published_at']
        item['date'] = detail['published_at']
    if detail.get('image_url') and not item.get('image_url'):
        item['image_url'] = detail['image_url']
    if detail.get('snippet') and len(item.get('snippet', '')) < 40:
        item['snippet'] = detail['snippet']
    if detail.get('content_preview') and len(item.get('content_preview', '')) < 140:
        item['content_preview'] = detail['content_preview']
    item['category'] = category_by_text(f"{item.get('title','')} {item.get('snippet','')} {item.get('content_preview','')}", item.get('category', 'news'))
    item['category_ru'] = localize_category(item['category'])
    item['transport_type'] = transport_by_text(f"{item.get('title','')} {item.get('snippet','')} {item.get('content_preview','')}")
    item['freshness'] = freshness_label(item.get('published_at') or item.get('date'))
    return item


def fetch_rss_items(source: dict, url: str | None = None) -> list[dict]:
    url = url or source["list_url"]
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    feed = feedparser.parse(resp.content)
    items = []
    for entry in getattr(feed, "entries", [])[:40]:
        image_url = ""
        media_content = getattr(entry, "media_content", None) or []
        media_thumbnail = getattr(entry, "media_thumbnail", None) or []
        links = getattr(entry, "links", None) or []
        if media_content and isinstance(media_content, list):
            image_url = media_content[0].get("url", "")
        elif media_thumbnail and isinstance(media_thumbnail, list):
            image_url = media_thumbnail[0].get("url", "")
        else:
            for link_obj in links:
                if getattr(link_obj, 'type', '').startswith('image/'):
                    image_url = getattr(link_obj, 'href', '')
                    break
        raw_summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
        item = item_from_fields(
            source,
            getattr(entry, "title", ""),
            getattr(entry, "link", ""),
            raw_summary,
            parse_date(getattr(entry, "published", "") or getattr(entry, "updated", "") or getattr(entry, "pubDate", "")),
            image_url=image_url,
            content_preview=raw_summary,
        )
        if item:
            items.append(item)
    return items


def pick_first(node, selector: str):
    try:
        return node.select_one(selector)
    except Exception:
        return None


def extract_generic_links(soup: BeautifulSoup, source: dict, base_url: str) -> list[dict]:
    items = []
    seen = set()
    for a in soup.select("a[href]"):
        href = urljoin(base_url, a.get("href", ""))
        title = normalize_text(a.get_text(" ", strip=True))
        if not title or len(title) < 25 or href in seen:
            continue
        lower = href.lower()
        if not any(token in lower for token in ["news", "press", "article", "novost", "media", "post", "/ru/news", "/news/"]):
            continue
        seen.add(href)
        item = item_from_fields(source, title, href, content_preview=title)
        if item:
            items.append(item)
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break
    return items


def fetch_html_items(source: dict, url: str | None = None) -> list[dict]:
    url = url or source["list_url"]
    html = fetch_html(url)
    soup = make_soup(html)
    items = []
    selector = source.get("article_selector", "article")
    nodes = soup.select(selector)
    for node in nodes[:40]:
        title_node = pick_first(node, source.get("title_selector", "h2 a, h3 a, h2, h3"))
        link_node = pick_first(node, source.get("link_selector", "a[href]"))
        date_node = pick_first(node, source.get("date_selector", "time, .date"))
        snippet_node = pick_first(node, source.get("snippet_selector", "p"))
        title = title_node.get_text(" ", strip=True) if title_node else ""
        href = urljoin(url, link_node.get("href", "") if link_node else "")
        date_raw = ""
        if date_node:
            date_raw = date_node.get('datetime', '') or date_node.get('content', '') or date_node.get_text(" ", strip=True)
        published_at = parse_date(normalize_text(date_raw))
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        image_node = pick_first(node, source.get("image_selector", "img"))
        image_url = ""
        if image_node:
            image_url = image_node.get('src') or image_node.get('data-src') or image_node.get('data-original') or ''
            image_url = urljoin(url, image_url)
        item = item_from_fields(source, title, href, snippet, published_at, image_url=image_url, content_preview=snippet)
        if item:
            items.append(item)
    if not items:
        items = extract_generic_links(soup, source, url)
    return items


def fetch_source_items(source: dict):
    candidates = [(source.get("fetch_method", "rss"), source.get("list_url", ""))]
    for fallback in source.get("fallbacks", []):
        candidates.append((fallback.get("fetch_method", source.get("fetch_method", "html")), fallback.get("list_url", "")))
    errors = []
    for method, url in candidates:
        if not url:
            continue
        try:
            items = fetch_rss_items(source, url) if method == "rss" else fetch_html_items(source, url)
            if items:
                enriched = []
                for idx, item in enumerate(items[:MAX_ITEMS_PER_SOURCE]):
                    if idx < ARTICLE_DETAIL_FETCH_LIMIT or idx < MIN_DETAIL_FETCH_PER_SOURCE:
                        item = enrich_item_from_article(source, item)
                    enriched.append(item)
                return enriched[:MAX_ITEMS_PER_SOURCE], method, url, ""
        except Exception as e:
            errors.append(f"{method}:{url} -> {e}")
    return [], candidates[0][0], candidates[0][1], " | ".join(errors)


def merge_with_existing(new_items: list[dict]):
    existing_payload = load_existing("news.json")
    existing_items = existing_payload.get("items", []) if isinstance(existing_payload, dict) else []
    existing_items = [x for x in existing_items if within_retention(x.get('published_at') or x.get('date', ''))]
    merged = []
    seen = set()
    for item in sorted(new_items + existing_items, key=lambda x: (sort_key_date(x.get('published_at') or x.get('date')), x.get('priority', 0)), reverse=True):
        key = item.get("hash") or make_hash(item.get("source", ""), item.get("title", ""), item.get("link", ""))
        if key in seen:
            continue
        if not within_retention(item.get('published_at') or item.get('date', '')):
            continue
        seen.add(key)
        merged.append(item)
    used_cache = not bool(new_items)
    return merged[:MAX_FINAL_ITEMS], used_cache


def display_date(value: str) -> str:
    if not value:
        return "Дата уточняется"
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime('%d.%m.%Y, %H:%M')
    except Exception:
        return value


def article_paragraphs(item: dict) -> list[str]:
    raw = normalize_text(item.get('content_preview') or item.get('snippet') or '')
    if not raw:
        return []
    parts = re.split(r'(?<=[.!?])\s+(?=[А-ЯA-Z0-9])', raw)
    out, buf = [], ''
    for p in [x.strip() for x in parts if x.strip()]:
        if len((buf + ' ' + p).strip()) > 460:
            if buf.strip():
                out.append(buf.strip())
            buf = p
        else:
            buf = (buf + ' ' + p).strip()
    if buf.strip():
        out.append(buf.strip())
    return out[:10]


def repo_static_url(item: dict) -> str:
    return f"news/{item.get('slug') or item.get('id')}.html"


def article_page_path(item: dict) -> str:
    return os.path.join(BASE_DIR, repo_static_url(item))


def absolute_news_url(item: dict) -> str:
    return f"{SITE_URL}/{repo_static_url(item)}"


def static_css() -> str:
    return (
        ':root{--ink:#0d0f14;--surface:#f5f3ef;--surface2:#eceae4;--accent:#e8a020;--blue:#2451a0;--muted:#6b7280;--border:#d8d3ca}'
        '*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;background:var(--surface);color:var(--ink);line-height:1.65}'
        'a{text-decoration:none;color:inherit}.wrap{max-width:1280px;margin:0 auto;padding:0 32px}'
        'header{background:var(--ink);position:sticky;top:0;z-index:50;border-bottom:2px solid var(--accent)}.header-inner{max-width:1280px;margin:0 auto;padding:0 32px;height:70px;display:flex;justify-content:space-between;align-items:center;gap:16px}'
        '.logo{font-family:Arial,sans-serif;font-weight:800;font-size:1.9rem;color:#fff;letter-spacing:.04em}.logo-dot,.logo span{color:var(--accent)}'
        'nav{display:flex;gap:2px;flex-wrap:wrap;align-items:center}.nav-item{background:none;border:none;color:rgba(255,255,255,.65);padding:10px 12px;cursor:pointer;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600}.nav-item:hover,.nav-item.active{color:#fff}.nav-divider{width:1px;height:18px;background:rgba(255,255,255,.15);margin:0 4px}.btn-nav{background:var(--accent);color:var(--ink);padding:10px 18px;border:none;font-weight:700;cursor:pointer;text-transform:uppercase;font-size:.78rem}'
        '.hero{padding:26px 0 14px;background:#fff;border-bottom:1px solid var(--border)}.eyebrow{font-size:.78rem;text-transform:uppercase;letter-spacing:.16em;color:var(--accent);font-weight:700;margin-bottom:8px}'
        '.hero h1{font-size:clamp(2rem,4vw,3.4rem);line-height:1.05;margin:0 0 10px;font-weight:800}.hero p{max-width:820px;color:#42526b}'
        '.article-shell,.panel,.news-card,.stat{background:#fff;border:1px solid var(--border)}.article-shell{padding:28px;margin:28px 0 44px}.news-card{padding:18px;display:flex;flex-direction:column;gap:12px;height:100%}'
        '.news-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}.news-media{aspect-ratio:16/9;border:1px solid var(--border);overflow:hidden;background:var(--surface2)}.news-media img{width:100%;height:100%;object-fit:cover;display:block}'
        '.badge{display:inline-flex;padding:7px 12px;border:1px solid var(--border);background:var(--surface2);font-size:.74rem;text-transform:uppercase;letter-spacing:.1em}.meta{display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:.9rem}'
        '.lead{font-size:1.08rem;color:#23334e}.article-content{display:grid;gap:14px;color:#23334e}.btn{display:inline-flex;align-items:center;justify-content:center;padding:14px 22px;background:var(--accent);color:var(--ink);font-weight:700;text-transform:uppercase;letter-spacing:.06em}.btn-outline{background:transparent;border:1px solid var(--blue);color:var(--blue)}.actions{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px}'
        '.kicker{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center}.section{padding:10px 0 46px}.section-head{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:18px}.section-head h2{font-size:clamp(1.5rem,2.5vw,2.2rem);line-height:1.1;margin:0}.muted{color:var(--muted)}'
        '.filters{display:flex;gap:10px;flex-wrap:wrap;margin:18px 0}.filters input,.filters select{padding:12px 14px;border:1px solid var(--border);background:#fff;font:inherit}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:18px}.stat{padding:18px}.stat b{display:block;font-size:1.8rem;margin-top:8px}'
        '.footer{background:var(--ink);color:#fff;padding:24px 0;margin-top:30px}.footer p{color:rgba(255,255,255,.68)}@media(max-width:960px){.news-grid,.stats{grid-template-columns:1fr 1fr}}@media(max-width:640px){.news-grid,.stats{grid-template-columns:1fr}.header-inner,.wrap{padding:0 18px}.header-inner{height:auto;align-items:flex-start;flex-direction:column;padding-top:16px;padding-bottom:16px}}'
    )





def main_header(prefix: str = '', active: str = '') -> str:
    def cls(name: str) -> str:
        return 'nav-item active' if active == name else 'nav-item'
    return (
        f'<header><div class="header-inner">'
        f'<a class="logo" href="{prefix}index.html">WI<span class="logo-dot">AF</span></a>'
        f'<nav>'
        f'<a class="{cls("home")}" href="{prefix}index.html#home">Главная</a>'
        f'<a class="{cls("market")}" href="{prefix}index.html#market">Рынок</a>'
        f'<a class="{cls("indices")}" href="{prefix}index.html#indices">Ставки и индексы</a>'
        f'<a class="{cls("directions")}" href="{prefix}index.html#directions">Направления</a>'
        f'<a class="{cls("auctions")}" href="{prefix}index.html#auctions">Аукционы</a>'
        f'<a class="{cls("blog")}" href="{prefix}blog.html">Блог</a>'
        f'<div class="nav-divider"></div>'
        f'<a class="{cls("importer")}" href="{prefix}index.html#importer">Импортёру</a>'
        f'<a class="{cls("forwarder")}" href="{prefix}index.html#forwarder">Экспедитору</a>'
        f'<a class="{cls("about")}" href="{prefix}index.html#about">О платформе</a>'
        f'<a class="{cls("contacts")}" href="{prefix}index.html#contacts">Контакты</a>'
        f'</nav>'
        f'<a class="btn-nav" href="https://wiaf.ru/Seller/Seller_login.php">Войти</a>'
        f'</div></header>'
    )

def build_article_html(item: dict, related: list[dict]) -> str:
    title = escape(item.get('title') or 'Материал WIAF')
    description = escape((item.get('snippet') or item.get('content_preview') or item.get('title') or '')[:220])
    image_url = escape(item.get('image_url') or '')
    canonical = absolute_news_url(item)
    date_iso = item.get('published_at') or item.get('date') or ''
    date_human = display_date(date_iso)
    paragraphs = article_paragraphs(item) or ['Полный разбор и контекст доступны в оригинальном материале источника.']
    category = escape(item.get('category_ru') or localize_category(item.get('category')))
    source = escape(item.get('source') or 'WIAF')
    original = escape(item.get('link') or SITE_URL)
    lead = escape(item.get('snippet') or item.get('content_preview') or '')

    related_cards = []
    for r in related[:3]:
        related_id = escape(r.get("id") or r.get("slug") or "")
        related_title = escape(r.get("title") or "Материал")
        related_image = escape(r.get("image_url") or "")
        related_category = escape(r.get("category_ru") or localize_category(r.get("category")))
        related_date = escape(display_date(r.get("published_at") or r.get("date") or ""))
        related_preview = escape((r.get("snippet") or r.get("content_preview") or "")[:160])
        rel_img = (
            f'<a class="news-media" href="../index.html#article:{related_id}">'
            f'<img src="{related_image}" alt="{related_title}"></a>'
        ) if r.get("image_url") else ''
        related_cards.append(
            f'<article class="news-card">{rel_img}'
            f'<div class="meta"><span class="badge">{related_category}</span><span>{related_date}</span></div>'
            f'<h3 style="margin:0;font-size:1.1rem;line-height:1.2"><a href="../index.html#article:{related_id}">{related_title}</a></h3>'
            f'<div class="muted">{related_preview}</div></article>'
        )
    related_html = ''.join(related_cards) if related_cards else '<div class="panel" style="padding:18px">Связанные материалы появятся после следующего обновления.</div>'

    ld_json = json.dumps({
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": item.get('title') or 'Материал WIAF',
        "datePublished": date_iso,
        "dateModified": date_iso,
        "author": {"@type": "Organization", "name": item.get('source') or 'WIAF'},
        "publisher": {"@type": "Organization", "name": "WIAF"},
        "mainEntityOfPage": canonical,
        "description": (item.get('snippet') or item.get('content_preview') or '')[:220],
    }, ensure_ascii=False)

    og_image = f'<meta property="og:image" content="{image_url}">' if image_url else ''
    image_block = f'<div class="news-media"><img src="{image_url}" alt="{title}"></div>' if image_url else ''
    paragraphs_html = ''.join(f'<p>{escape(p)}</p>' for p in paragraphs)

    return f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — WIAF</title>
<meta name="description" content="{description}">
<link rel="canonical" href="{canonical}">
<meta property="og:type" content="article">
<meta property="og:title" content="{title} — WIAF">
<meta property="og:description" content="{description}">
<meta property="og:url" content="{canonical}">
{og_image}
<meta name="twitter:card" content="summary_large_image">
<style>{static_css()}</style>
<script type="application/ld+json">{ld_json}</script>
</head>
<body>
{main_header('../', 'blog')}
<section class="hero"><div class="wrap"><div class="eyebrow">Новости и сигналы рынка</div><h1>{title}</h1><p>{description}</p></div></section>
<main class="wrap">
  <article class="article-shell">
    <div class="kicker"><div style="display:flex;gap:10px;flex-wrap:wrap"><span class="badge">{category}</span><span class="badge">{escape(item.get('transport_type') or 'смешанная')}</span></div><div class="muted">{escape(date_human)} · {source}</div></div>
    {image_block}
    <div class="lead">{lead}</div>
    <div class="article-content">{paragraphs_html}</div>
    <div class="actions"><a class="btn" href="{original}" target="_blank" rel="noopener">Читать в оригинале</a><a class="btn btn-outline" href="../blog.html">Назад в блог</a></div>
  </article>
  <section class="section">
    <div class="section-head"><div><div class="eyebrow">Еще материалы</div><h2>Похожие новости и сигналы</h2></div></div>
    <div class="news-grid">{related_html}</div>
  </section>
</main>
<footer class="footer"><div class="wrap"><p>WIAF — терминал рынка международной логистики. Новости, сигналы, ставки и аукционы.</p></div></footer>
</body></html>'''


def build_blog_html(items: list[dict]) -> str:
    items = sorted(
        items,
        key=lambda x: (sort_key_date(x.get('published_at') or x.get('date')), x.get('priority', 0)),
        reverse=True,
    )
    cards = []
    for item in items[:120]:
        title = escape(item.get('title') or 'Материал')
        snippet = escape((item.get('snippet') or item.get('content_preview') or '')[:180])
        date = escape(display_date(item.get('published_at') or item.get('date') or ''))
        category = escape(item.get('category_ru') or localize_category(item.get('category')))
        source = escape(item.get('source') or 'Источник')
        transport = escape(item.get('transport_type') or 'смешанная')
        article_id = escape(item.get('id') or item.get('slug') or '')
        image_url = escape(item.get('image_url') or '')

        if image_url:
            image = (
                '<a class="news-media" href="index.html#article:{}">'
                '<img src="{}" alt="{}"></a>'
            ).format(article_id, image_url, title)
        else:
            image = ''

        card_html = (
            '<article class="news-card" data-source="{source}" data-category="{category}">{image}'
            '<div class="meta"><span class="badge">{category}</span><span>{date}</span></div>'
            '<h3 style="margin:0;font-size:1.1rem;line-height:1.2"><a href="index.html#article:{article_id}">{title}</a></h3>'
            '<div class="muted">{snippet}</div>'
            '<div class="meta"><span>{source}</span><span>{transport}</span></div></article>'
        ).format(
            source=source,
            category=category,
            image=image,
            date=date,
            article_id=article_id,
            title=title,
            snippet=snippet,
            transport=transport,
        )
        cards.append(card_html)

    sources = sorted({(x.get('source') or '') for x in items if x.get('source')})
    categories = sorted({(x.get('category_ru') or localize_category(x.get('category'))) for x in items})
    options_source = ''.join(f'<option>{escape(v)}</option>' for v in sources)
    options_cat = ''.join(f'<option>{escape(v)}</option>' for v in categories)
    return f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Блог WIAF — новости и сигналы рынка</title>
<meta name="description" content="Блог WIAF: статические новости и сигналы рынка международной логистики за последние 12 месяцев.">
<link rel="canonical" href="{SITE_URL}/blog.html">
<style>{static_css()}</style>
</head>
<body>
{main_header('', 'blog')}
<section class="hero"><div class="wrap"><div class="eyebrow">Блог и сигналы</div><h1>Все новости, сигналы и статьи рынка</h1><p>Статическая витрина публикаций WIAF: материалы за последние 12 месяцев, фильтрация по источнику и теме. Каждая карточка ведет на внутреннюю страницу материала.</p></div></section>
<main class="wrap section">
  <div class="stats"><div class="stat">Материалов<b>{len(items)}</b></div><div class="stat">Источников<b>{len(sources)}</b></div><div class="stat">Тем<b>{len(categories)}</b></div><div class="stat">Период<b>12 мес.</b></div></div>
  <div class="filters"><input id="search" type="search" placeholder="Поиск по заголовкам и описанию"><select id="source"><option value="all">Все источники</option>{options_source}</select><select id="category"><option value="all">Все темы</option>{options_cat}</select></div>
  <div class="news-grid" id="grid">{''.join(cards) if cards else '<div class="panel" style="padding:18px">Материалы появятся после ближайшего обновления.</div>'}</div>
</main>
<footer class="footer"><div class="wrap"><p>WIAF — терминал рынка международной логистики. Блог обновляется автоматически.</p></div></footer>
<script>
const q=document.getElementById('search'),s=document.getElementById('source'),c=document.getElementById('category'),cards=[...document.querySelectorAll('#grid .news-card')];
function apply(){{const v=(q.value||'').toLowerCase().trim(),sv=s.value,cv=c.value;cards.forEach(card=>{{const txt=card.innerText.toLowerCase();const ok=(!v||txt.includes(v))&&(sv==='all'||card.dataset.source===sv)&&(cv==='all'||card.dataset.category===cv);card.style.display=ok?'':'none';}});}}
[q,s,c].forEach(el=>el&&el.addEventListener('input',apply));[s,c].forEach(el=>el&&el.addEventListener('change',apply));
</script>
</body></html>'''


def write_static_news(items: list[dict]):
    ensure_dirs()
    for filename in os.listdir(NEWS_DIR):
        if filename.endswith('.html') or filename.endswith('.xml'):
            try:
                os.remove(os.path.join(NEWS_DIR, filename))
            except Exception:
                pass
    for item in items:
        related = [x for x in items if x.get('id') != item.get('id') and x.get('category') == item.get('category')][:3]
        with open(article_page_path(item), 'w', encoding='utf-8') as f:
            f.write(build_article_html(item, related))
    with open(os.path.join(BASE_DIR, 'blog.html'), 'w', encoding='utf-8') as f:
        f.write(build_blog_html(items))
    sitemap_items = ''.join(
        f'<url><loc>{escape(absolute_news_url(item))}</loc><lastmod>{escape((item.get("published_at") or item.get("date") or now_iso())[:19])}</lastmod></url>'
        for item in items[:500]
    )
    with open(os.path.join(NEWS_DIR, 'sitemap.xml'), 'w', encoding='utf-8') as f:
        f.write(f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{sitemap_items}</urlset>')




def parse_float_maybe(value):
    if value is None:
        return None
    txt = normalize_text(str(value)).replace(' ', '')
    txt = txt.replace(' ', '').replace(' ', '')
    txt = txt.replace(',', '.') if txt.count(',') == 1 and txt.count('.') == 0 else txt.replace(',', '')
    m = re.search(r'-?\d+(?:\.\d+)?', txt)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def extract_index_value(text: str, patterns: list[str]):
    hay = normalize_text(text)
    for pattern in patterns:
        m = re.search(pattern, hay, flags=re.I | re.S)
        if m:
            return parse_float_maybe(m.group(1))
    return None


def load_existing_items(filename: str) -> list[dict]:
    payload = load_existing(filename)
    return payload.get('items', []) if isinstance(payload, dict) else []


def merge_records(new_items: list[dict], existing_items: list[dict], key_fn, limit: int = 500) -> tuple[list[dict], bool]:
    merged = []
    seen = set()
    for item in new_items + existing_items:
        key = key_fn(item)
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(item)
    used_cache = not bool(new_items)
    return merged[:limit], used_cache


def fetch_indices(existing_items: list[dict]) -> tuple[list[dict], dict]:
    items = []
    status = []
    existing_map = {str(x.get('index_name') or x.get('name') or x.get('id')): x for x in existing_items}
    for cfg in INDEX_SOURCES:
        if not cfg.get('enabled'):
            continue
        try:
            html = fetch_html(cfg['url'])
            value = extract_index_value(html, cfg.get('patterns', []))
            if value is None:
                raise ValueError('index value not found')
            prev = existing_map.get(cfg['name']) or {}
            prev_value = parse_float_maybe(prev.get('value'))
            change_abs = round(value - prev_value, 2) if prev_value is not None else 0.0
            change_pct = round((change_abs / prev_value) * 100, 2) if prev_value else 0.0
            items.append({
                'id': f"idx_{cfg['id']}",
                'source': cfg.get('source') or cfg.get('name'),
                'index_name': cfg['name'],
                'date': now_iso()[:10],
                'value': round(value, 2),
                'unit': cfg.get('unit', 'index_points'),
                'change_abs': change_abs,
                'change_pct': change_pct,
                'status': 'published',
                'source_url': cfg['url'],
            })
            status.append({'source_id': cfg['id'], 'source_name': cfg['name'], 'kind': 'index', 'status': 'ok', 'items_count': 1, 'checked_at': now_iso(), 'error': '', 'used_url': cfg['url']})
        except Exception as e:
            old = existing_map.get(cfg['name'])
            if old:
                items.append(old)
            status.append({'source_id': cfg['id'], 'source_name': cfg['name'], 'kind': 'index', 'status': 'cache' if old else 'error', 'items_count': 1 if old else 0, 'checked_at': now_iso(), 'error': str(e)[:300], 'used_url': cfg['url']})
    merged, cache_used = merge_records(items, existing_items, lambda x: str(x.get('index_name') or x.get('id')), limit=20)
    return merged, {'items': status, 'cache_used': cache_used}


def fetch_fx(existing_items: list[dict]) -> tuple[list[dict], dict]:
    items = []
    status = []
    existing_map = {str(x.get('code') or x.get('id')): x for x in existing_items}
    for cfg in FX_SOURCES:
        if not cfg.get('enabled'):
            continue
        try:
            raw = requests.get(cfg['url'], headers=HEADERS, timeout=TIMEOUT).text
            root = ET.fromstring(raw)
            date_attr = root.attrib.get('Date', '')
            fx_date = parse_date(date_attr)[:10] if date_attr else now_iso()[:10]
            for code, valute_id in cfg.get('currencies', {}).items():
                node = root.find(f".//Valute[@ID='{valute_id}']")
                if node is None:
                    continue
                nominal = parse_float_maybe(node.findtext('Nominal') or '1') or 1.0
                value = parse_float_maybe(node.findtext('Value') or '')
                if value is None:
                    continue
                rate = round(value / nominal, 6)
                prev = existing_map.get(code) or {}
                prev_rate = parse_float_maybe(prev.get('value'))
                delta = round(rate - prev_rate, 6) if prev_rate is not None else 0.0
                items.append({
                    'id': f'fx_{code.lower()}',
                    'code': code,
                    'base': 'RUB',
                    'value': rate,
                    'date': fx_date,
                    'change_abs': delta,
                    'change_pct': round((delta / prev_rate) * 100, 3) if prev_rate else 0.0,
                    'source': cfg.get('source') or cfg.get('name'),
                    'source_url': cfg['url'],
                    'status': 'published',
                })
            status.append({'source_id': cfg['id'], 'source_name': cfg['name'], 'kind': 'fx', 'status': 'ok', 'items_count': len(items), 'checked_at': now_iso(), 'error': '', 'used_url': cfg['url']})
        except Exception as e:
            status.append({'source_id': cfg['id'], 'source_name': cfg['name'], 'kind': 'fx', 'status': 'error', 'items_count': 0, 'checked_at': now_iso(), 'error': str(e)[:300], 'used_url': cfg['url']})
    merged, cache_used = merge_records(items, existing_items, lambda x: str(x.get('code') or x.get('id')), limit=10)
    return merged, {'items': status, 'cache_used': cache_used}


RATE_PATTERNS = [
    re.compile(r'(?P<currency>USD|EUR|RUB|CNY)\s?(?P<min>\d{1,3}(?:[\s,]\d{3})*(?:[\.,]\d+)?)\s?(?:-|–|—|до|to)\s?(?P<max>\d{1,3}(?:[\s,]\d{3})*(?:[\.,]\d+)?)', re.I),
    re.compile(r'от\s?(?P<min>\d{1,3}(?:[\s,]\d{3})*(?:[\.,]\d+)?)\s?(?P<currency>USD|EUR|RUB|CNY).*?до\s?(?P<max>\d{1,3}(?:[\s,]\d{3})*(?:[\.,]\d+)?)', re.I),
]


def detect_corridor(text: str) -> str:
    t = (text or '').lower()
    mapping = {
        'Китай': ['китай', 'china', 'шанхай', 'shanghai', 'ningbo', 'shenzhen', 'guangzhou'],
        'Турция': ['турц', 'istanbul', 'стамбул', 'izmir', 'мерсин', 'mersin'],
        'Индия': ['индия', 'india', 'mumbai', 'nhava', 'мунбаи'],
        'ОАЭ': ['оаэ', 'uae', 'dubai', 'дубай', 'jebel ali', 'джебель'],
        'Казахстан': ['казахстан', 'almaty', 'алматы', 'astana', 'астана'],
        'ЮВА': ['юва', 'вьетнам', 'vietnam', 'bangkok', 'хошимин', 'ho chi minh', 'singapore', 'малайз'],
    }
    for label, words in mapping.items():
        if any(w in t for w in words):
            return label
    return 'Смешанный'


def detect_mode(text: str) -> str:
    t = (text or '').lower()
    if any(w in t for w in ['fcl', '40hq', '40hc', '20dc', 'мор', 'sea', 'vessel', 'ocean']):
        return 'sea'
    if any(w in t for w in ['lcl', 'сборн']):
        return 'multimodal'
    if any(w in t for w in ['жд', 'rail', 'поезд', 'вагон']):
        return 'rail'
    if any(w in t for w in ['авиа', 'air', 'kg', 'кг']):
        return 'air'
    if any(w in t for w in ['ftl', 'авто', 'truck', 'road', 'фура']):
        return 'road'
    return 'multimodal'


def extract_rate_candidates_from_news(news_items: list[dict]) -> list[dict]:
    out = []
    for item in news_items:
        blob = ' '.join(filter(None, [item.get('title'), item.get('snippet'), item.get('content_preview')]))
        corridor = detect_corridor(blob)
        mode = detect_mode(blob)
        for pattern in RATE_PATTERNS:
            m = pattern.search(blob)
            if not m:
                continue
            curr = (m.groupdict().get('currency') or 'USD').upper()
            min_v = parse_float_maybe(m.groupdict().get('min'))
            max_v = parse_float_maybe(m.groupdict().get('max'))
            if min_v is None or max_v is None:
                continue
            if min_v > max_v:
                min_v, max_v = max_v, min_v
            direction = item.get('title') or corridor
            out.append({
                'id': f"rate_auto_{make_hash(direction, curr, str(min_v), str(max_v))[:10]}",
                'source': item.get('source') or 'WIAF parser',
                'source_link': item.get('link') or '',
                'date': (item.get('published_at') or item.get('date') or now_iso())[:10],
                'route_from': corridor,
                'route_to': 'Россия',
                'direction_name': direction[:90],
                'corridor': corridor,
                'transport_type': mode,
                'rate_type': 'freight',
                'value': round((min_v + max_v) / 2, 2),
                'min_value': round(min_v, 2),
                'max_value': round(max_v, 2),
                'currency': curr,
                'unit': 'market',
                'note': safe_note_from_news(item),
                'exactness': 'estimate',
                'trend_7d_pct': 0.0,
                'trend_30d_pct': 0.0,
            })
            break
    return out


def safe_note_from_news(item: dict) -> str:
    return normalize_text(item.get('snippet') or item.get('content_preview') or '')[:180]


def corridor_signal(rate: dict) -> str:
    t7 = parse_float_maybe(rate.get('trend_7d_pct')) or 0.0
    t30 = parse_float_maybe(rate.get('trend_30d_pct')) or 0.0
    if t7 >= 4 or t30 >= 8:
        return 'перегрет'
    if t7 <= -2 or t30 <= -5:
        return 'окно возможностей'
    if abs(t7) <= 2 and abs(t30) <= 4:
        return 'стабилен'
    return 'в движении'


def build_rates(news_items: list[dict], existing_items: list[dict]) -> tuple[list[dict], dict]:
    auto_rates = extract_rate_candidates_from_news(news_items)
    # existing first as a stable baseline, auto-rates can override by corridor+mode+date proximity
    existing_clean = [x for x in existing_items if x.get('corridor')]
    merged = []
    seen = set()
    for item in auto_rates + existing_clean:
        key = f"{item.get('corridor')}|{item.get('transport_type')}|{item.get('currency')}"
        if key in seen:
            continue
        seen.add(key)
        item['signal'] = corridor_signal(item)
        item['corridor'] = item.get('corridor') or detect_corridor(item.get('direction_name') or '')
        item['exactness'] = item.get('exactness') or 'indicative'
        merged.append(item)
    merged = sorted(merged, key=lambda x: sort_key_date(x.get('date')), reverse=True)[:MAX_RATE_ITEMS]
    return merged, {'auto_extracted': len(auto_rates), 'cache_used': not bool(auto_rates)}


def build_corridors(rates: list[dict], news_items: list[dict]) -> list[dict]:
    grouped = {}
    for rate in rates:
        corridor = rate.get('corridor') or 'Смешанный'
        grouped.setdefault(corridor, {'rates': [], 'news': []})
        grouped[corridor]['rates'].append(rate)
    for item in news_items:
        corridor = detect_corridor(' '.join([item.get('title') or '', item.get('snippet') or '', item.get('content_preview') or '']))
        grouped.setdefault(corridor, {'rates': [], 'news': []})
        grouped[corridor]['news'].append(item)
    out = []
    for corridor, payload in grouped.items():
        rates_part = payload['rates']
        news_part = payload['news']
        sample = rates_part[0] if rates_part else {}
        out.append({
            'id': f"corridor_{slugify(corridor)}",
            'corridor': corridor,
            'status': corridor_signal(sample) if sample else 'стабилен',
            'modes': sorted({localize_transport_key(x.get('transport_type')) for x in rates_part if x.get('transport_type')}),
            'news_count_30d': len(news_part),
            'rate_signal': corridor_signal(sample) if sample else 'стабилен',
            'summary': build_corridor_summary(corridor, rates_part, news_part),
            'top_route': sample.get('direction_name') or corridor,
            'date': (sample.get('date') or now_iso())[:10],
        })
    return sorted(out, key=lambda x: (x.get('news_count_30d', 0), x.get('corridor')), reverse=True)


def localize_transport_key(value: str) -> str:
    m = {'sea': 'морская', 'rail': 'железнодорожная', 'road': 'авто', 'air': 'авиа', 'multimodal': 'мультимодальная'}
    return m.get((value or '').lower(), 'смешанная')


def build_corridor_summary(corridor: str, rates_part: list[dict], news_part: list[dict]) -> str:
    if rates_part:
        sample = rates_part[0]
        return f"{corridor}: {len(rates_part)} ориентиров по ставкам, статус '{corridor_signal(sample)}', публикаций за окно — {len(news_part)}."
    return f"{corridor}: пока без ставки в витрине, но найдено {len(news_part)} сигналов и публикаций."


def build_market_stats(news_items: list[dict], rates: list[dict], indices: list[dict], fx_items: list[dict], existing_payload: dict) -> dict:
    base = (existing_payload.get('items') or [{}])[0] if isinstance(existing_payload, dict) else {}
    corridors = len({x.get('corridor') for x in rates if x.get('corridor')})
    modes = len({x.get('transport_type') for x in rates if x.get('transport_type')})
    wiaf_market_index = round(min(100, 45 + corridors * 3 + len(news_items[:30]) * 0.4 + len(indices) * 2), 1)
    fx_map = {x.get('code'): x.get('value') for x in fx_items}
    item = {
        'date': now_iso()[:10],
        'active_auctions': int(base.get('active_auctions', 43) or 43),
        'new_auctions_day': int(base.get('new_auctions_day', 9) or 9),
        'new_auctions_week': int(base.get('new_auctions_week', 34) or 34),
        'active_routes': corridors or int(base.get('active_routes', 0) or 0),
        'active_transport_types': modes or int(base.get('active_transport_types', 0) or 0),
        'avg_responses': round(float(base.get('avg_responses', 4.3) or 4.3), 1),
        'avg_internal_rate': float(base.get('avg_internal_rate', 1980) or 1980),
        'calculated_market_index': wiaf_market_index,
        'usd_rub': fx_map.get('USD'),
        'cny_rub': fx_map.get('CNY'),
        'eur_rub': fx_map.get('EUR'),
        'updated_at': now_iso(),
    }
    auctions = existing_payload.get('auctions') if isinstance(existing_payload, dict) else []
    if not auctions:
        auctions = [
            {'route': 'Шанхай → Москва', 'cargo': "FCL 40'", 'delivery_type': 'морская', 'status': 'активен', 'participants': 4, 'cta': 'https://wiaf.ru/Seller/Seller_login.php'},
            {'route': 'Стамбул → Санкт-Петербург', 'cargo': 'LCL', 'delivery_type': 'мультимодальная', 'status': 'новый', 'participants': 3, 'cta': 'https://wiaf.ru/Seller/Seller_login.php'},
            {'route': 'Мумбаи → Новороссийск', 'cargo': '24 т', 'delivery_type': 'морская', 'status': 'в торгах', 'participants': 5, 'cta': 'https://wiaf.ru/Seller/Seller_login.php'},
        ]
    return {'updated_at': now_iso(), 'items': [item], 'auctions': auctions}


def build_meta(news_items: list[dict], rates: list[dict], indices: list[dict], corridors: list[dict]) -> dict:
    return {
        'updated_at': now_iso(),
        'news_count': len(news_items),
        'rates_count': len(rates),
        'indices_count': len(indices),
        'corridors_count': len(corridors),
        'version': 'mvp-auto-3',
    }


def main():
    ensure_dirs()
    all_news = []
    source_status = []
    active_sources = sorted(
        [s for s in SOURCES if s.get("enabled") and s.get("fetch_method") in ("rss", "html")],
        key=lambda x: x.get("priority", 0),
        reverse=True,
    )

    for source in active_sources:
        try:
            items, used_method, used_url, error_text = fetch_source_items(source)
            retained = []
            stale_count = 0
            missing_date_count = 0
            for it in items:
                dt = it.get('published_at') or it.get('date')
                if not dt:
                    missing_date_count += 1
                    continue
                if within_retention(dt):
                    retained.append(it)
                else:
                    stale_count += 1
            all_news.extend(retained)
            source_status.append({
                "source_id": source["source_id"],
                "source_name": source["source_name"],
                "fetch_method": used_method,
                "used_url": used_url,
                "priority": source.get("priority", 0),
                "enabled": source.get("enabled", False),
                "country": source.get("country", ""),
                "language": source.get("language", ""),
                "items_count": len(retained),
                "raw_items_count": len(items),
                "stale_filtered": stale_count,
                "missing_date_filtered": missing_date_count,
                "status": "ok" if retained else ("empty" if not error_text else "error"),
                "checked_at": now_iso(),
                "error": error_text[:450],
                "kind": 'news',
            })
            print(f"{source['source_name']}: retained={len(retained)}, raw={len(items)}")
        except Exception as e:
            source_status.append({
                "source_id": source["source_id"],
                "source_name": source["source_name"],
                "fetch_method": source.get("fetch_method", ""),
                "used_url": source.get("list_url", ""),
                "priority": source.get("priority", 0),
                "enabled": source.get("enabled", False),
                "country": source.get("country", ""),
                "language": source.get("language", ""),
                "items_count": 0,
                "raw_items_count": 0,
                "stale_filtered": 0,
                "missing_date_filtered": 0,
                "status": "error",
                "checked_at": now_iso(),
                "error": str(e)[:450],
                "kind": 'news',
            })
            print(f"ERROR in {source['source_name']}: {e}")

    unique, seen = [], set()
    for item in sorted(all_news, key=lambda x: (sort_key_date(x.get('published_at') or x.get('date')), x.get('priority', 0)), reverse=True):
        key = item.get('hash')
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    merged_news, used_cache = merge_with_existing(unique)
    for item in merged_news:
        item['static_url'] = repo_static_url(item)
        item['canonical_url'] = absolute_news_url(item)

    # secondary datasets
    existing_indices = load_existing_items('indices.json')
    indices, indices_status = fetch_indices(existing_indices)
    existing_fx = load_existing_items('fx.json')
    fx_items, fx_status = fetch_fx(existing_fx)
    existing_rates = load_existing_items('rates.json')
    rates, rates_status = build_rates(merged_news, existing_rates)
    corridors = build_corridors(rates, merged_news)
    market_stats_payload = build_market_stats(merged_news, rates, indices, fx_items, load_existing('market_stats.json'))
    meta_payload = build_meta(merged_news, rates, indices, corridors)

    for entry in indices_status['items'] + fx_status['items']:
        source_status.append(entry)

    summary = {
        "updated_at": now_iso(),
        "retention_days": RETENTION_DAYS,
        "cutoff_date": cutoff_iso(),
        "active_sources": len(active_sources),
        "successful_sources": len([x for x in source_status if x["status"] == "ok"]),
        "empty_sources": len([x for x in source_status if x["status"] == "empty"]),
        "error_sources": len([x for x in source_status if x["status"] == "error"]),
        "fetched_items": len(unique),
        "published_items": len(merged_news),
        "cache_used": used_cache,
        "stale_filtered_total": sum(int(x.get('stale_filtered', 0)) for x in source_status if x.get('kind') == 'news'),
        "missing_date_filtered_total": sum(int(x.get('missing_date_filtered', 0)) for x in source_status if x.get('kind') == 'news'),
        "indices_count": len(indices),
        "rates_count": len(rates),
        "corridors_count": len(corridors),
        "fx_count": len(fx_items),
        "rate_auto_extracted": rates_status.get('auto_extracted', 0),
    }

    save_json("news.json", {"updated_at": now_iso(), "cache_used": used_cache, "retention_days": RETENTION_DAYS, "cutoff_date": cutoff_iso(), "items": merged_news})
    save_json("indices.json", {"updated_at": now_iso(), "cache_used": indices_status.get('cache_used', False), "items": indices})
    save_json("fx.json", {"updated_at": now_iso(), "cache_used": fx_status.get('cache_used', False), "items": fx_items})
    save_json("rates.json", {"updated_at": now_iso(), "cache_used": rates_status.get('cache_used', False), "items": rates})
    save_json("corridors.json", {"updated_at": now_iso(), "items": corridors})
    save_json("market_stats.json", market_stats_payload)
    save_json("meta.json", meta_payload)
    save_json("source_status.json", {"updated_at": now_iso(), "summary": summary, "items": source_status})
    write_static_news(merged_news)
    print(f"Saved {len(merged_news)} news items, {len(indices)} indices, {len(fx_items)} fx, {len(rates)} rates, {len(corridors)} corridors")


if __name__ == "__main__":
    main()
