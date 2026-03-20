
import os
import re
import json
import hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
from email.utils import parsedate_to_datetime

import requests
import feedparser
from bs4 import BeautifulSoup, FeatureNotFound
from dateutil import parser as date_parser

from sources import SOURCES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "public", "data")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WIAFBot/1.2; +https://wiaf.ru)",
    "Accept-Language": "ru,en;q=0.8",
}
TIMEOUT = 20
RETENTION_DAYS = 365
MAX_ITEMS_PER_SOURCE = 20
MAX_FINAL_ITEMS = 300
ARTICLE_DETAIL_FETCH_LIMIT = 12
MIN_DETAIL_FETCH_PER_SOURCE = 6

RUS_MONTHS = {
    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06',
    'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
}


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


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
    ensure_data_dir()
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

    # common explicit numeric patterns first
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
    return mapping.get(category or "news", "новости")


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
        return "sea"
    if any(word in t for word in ["rail", "жд", "railway", "train", "вагон", "ржд"]):
        return "rail"
    if any(word in t for word in ["truck", "авто", "road", "грузов"]):
        return "road"
    if any(word in t for word in ["air", "авиа", "cargo terminal"]):
        return "air"
    if any(word in t for word in ["multimodal", "мультимод"]):
        return "multimodal"
    return "unknown"


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


def item_from_fields(source: dict, title: str, link: str, snippet: str = "", published_at: str = "", image_url: str = "", content_preview: str = "") -> dict | None:
    title = normalize_text(title)
    link = (link or "").strip()
    snippet = normalize_text(snippet)
    if not title or not link or link.startswith("javascript:"):
        return None
    item_hash = make_hash(source["source_name"], title, link)
    category = category_by_text(f"{title} {snippet} {content_preview}", source.get("category_default", "news"))
    return {
        "id": f"news_{item_hash[:12]}",
        "slug": f"{source['source_id']}-{item_hash[:10]}",
        "source": source["source_name"],
        "source_id": source["source_id"],
        "source_group": source.get("source_group", "other"),
        "lang": source.get("language", ""),
        "priority": source.get("priority", 0),
        "date": published_at,
        "published_at": published_at,
        "title": title,
        "snippet": snippet[:500],
        "content_preview": normalize_text(content_preview or snippet)[:2500],
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
    content_preview = ' '.join(content_parts)[:3500] if content_parts else ''
    if not summary:
        summary = content_preview[:420]
    return {
        'title': title,
        'image_url': image_url,
        'published_at': published_at,
        'snippet': summary[:500],
        'content_preview': content_preview[:3500],
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
        if not title or len(title) < 25:
            continue
        if href in seen:
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


def fetch_source_items(source: dict) -> tuple[list[dict], str, str]:
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
                # enrich recent top items from article detail page
                enriched = []
                for idx, item in enumerate(items[:MAX_ITEMS_PER_SOURCE]):
                    if idx < ARTICLE_DETAIL_FETCH_LIMIT or idx < MIN_DETAIL_FETCH_PER_SOURCE:
                        item = enrich_item_from_article(source, item)
                    enriched.append(item)
                return enriched[:MAX_ITEMS_PER_SOURCE], method, url, ""
        except Exception as e:
            errors.append(f"{method}:{url} -> {e}")
    return [], candidates[0][0], candidates[0][1], " | ".join(errors)


def merge_with_existing(new_items: list[dict]) -> tuple[list[dict], bool]:
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


def main():
    ensure_data_dir()
    all_news = []
    source_status = []
    active_sources = sorted(
        [s for s in SOURCES if s.get("enabled") and s.get("fetch_method") in ("rss", "html")],
        key=lambda x: x.get("priority", 0),
        reverse=True,
    )

    for source in active_sources:
        error_text = ""
        used_method = source.get("fetch_method", "")
        used_url = source.get("list_url", "")
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
            })
            print(f"{source['source_name']}: retained={len(retained)}, raw={len(items)}, stale={stale_count}, missing_date={missing_date_count}")
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
            })
            print(f"ERROR in {source['source_name']}: {e}")

    unique = []
    seen = set()
    for item in sorted(all_news, key=lambda x: (sort_key_date(x.get('published_at') or x.get('date')), x.get('priority', 0)), reverse=True):
        key = item.get('hash')
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    merged_news, used_cache = merge_with_existing(unique)
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
        "stale_filtered_total": sum(int(x.get('stale_filtered', 0)) for x in source_status),
        "missing_date_filtered_total": sum(int(x.get('missing_date_filtered', 0)) for x in source_status),
    }

    save_json("news.json", {"updated_at": now_iso(), "cache_used": used_cache, "retention_days": RETENTION_DAYS, "cutoff_date": cutoff_iso(), "items": merged_news})
    save_json("source_status.json", {"updated_at": now_iso(), "summary": summary, "items": source_status})
    print(f"Saved {len(merged_news)} news items from {len(active_sources)} active sources; fetched now={len(unique)}; cache_used={used_cache}")


if __name__ == "__main__":
    main()
