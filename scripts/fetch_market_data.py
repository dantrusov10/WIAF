import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from sources import SOURCES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "public", "data")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WIAFBot/1.0; +https://wiaf.ru)"}


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def make_hash(*parts: str) -> str:
    raw = "|".join([normalize_text(p or "") for p in parts])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        return date_parser.parse(value).date().isoformat()
    except Exception:
        return ""


def category_by_text(text: str, default_category: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["ставк", "tariff", "rate", "freight"]):
        return "rates"
    if any(word in t for word in ["порт", "port", "terminal"]):
        return "ports"
    if any(word in t for word in ["rail", "жд", "railway"]):
        return "rail"
    if any(word in t for word in ["аналит", "analysis", "review"]):
        return "analytics"
    return default_category or "news"


def transport_by_text(text: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["container", "мор", "sea", "vessel", "port", "feu", "teu"]):
        return "sea"
    if any(word in t for word in ["rail", "жд", "railway", "train"]):
        return "rail"
    if any(word in t for word in ["truck", "авто", "road"]):
        return "road"
    if any(word in t for word in ["multimodal", "мультимод"]):
        return "multimodal"
    return "unknown"


def freshness_label(date_str: str) -> str:
    if not date_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(date_str + ("" if "T" in date_str else "T00:00:00"))
    except ValueError:
        return "unknown"
    delta_hours = (datetime.now() - dt).total_seconds() / 3600
    if delta_hours <= 48:
        return "fresh"
    if delta_hours <= 168:
        return "recent"
    return "archive"


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def fetch_rss_items(source: dict) -> list[dict]:
    feed = feedparser.parse(source["list_url"])
    items = []
    for entry in feed.entries[:30]:
        title = normalize_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "")
        snippet = normalize_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        published_at = parse_date(getattr(entry, "published", "") or getattr(entry, "updated", ""))
        item_hash = make_hash(source["source_name"], title, link)
        items.append({
            "id": f"news_{item_hash[:12]}",
            "source": source["source_name"],
            "source_id": source["source_id"],
            "date": published_at,
            "title": title,
            "snippet": snippet[:350],
            "link": link,
            "image_url": "",
            "category": category_by_text(f"{title} {snippet}", source.get("category_default", "news")),
            "transport_type": transport_by_text(f"{title} {snippet}"),
            "region": source.get("country", "unknown"),
            "freshness": freshness_label(published_at),
            "hash": item_hash,
            "status": "published"
        })
    return items


def pick_first(node, selector: str):
    try:
        return node.select_one(selector)
    except Exception:
        return None


def fetch_html_items(source: dict) -> list[dict]:
    html = fetch_html(source["list_url"])
    soup = BeautifulSoup(html, "lxml")
    items = []
    for node in soup.select(source.get("article_selector", "article"))[:30]:
        title_node = pick_first(node, source.get("title_selector", "h2, h3"))
        link_node = pick_first(node, source.get("link_selector", "a"))
        date_node = pick_first(node, source.get("date_selector", "time"))
        snippet_node = pick_first(node, source.get("snippet_selector", "p"))
        title = normalize_text(title_node.get_text(" ", strip=True) if title_node else "")
        link = urljoin(source["list_url"], link_node.get("href", "") if link_node else "")
        published_at = parse_date(normalize_text(date_node.get_text(" ", strip=True) if date_node else ""))
        snippet = normalize_text(snippet_node.get_text(" ", strip=True) if snippet_node else "")
        if not title or not link:
            continue
        item_hash = make_hash(source["source_name"], title, link)
        items.append({
            "id": f"news_{item_hash[:12]}",
            "source": source["source_name"],
            "source_id": source["source_id"],
            "date": published_at,
            "title": title,
            "snippet": snippet[:350],
            "link": link,
            "image_url": "",
            "category": category_by_text(f"{title} {snippet}", source.get("category_default", "news")),
            "transport_type": transport_by_text(f"{title} {snippet}"),
            "region": source.get("country", "unknown"),
            "freshness": freshness_label(published_at),
            "hash": item_hash,
            "status": "published"
        })
    return items


def save_json(filename: str, payload: dict):
    ensure_data_dir()
    with open(os.path.join(DATA_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    all_news = []
    for source in SOURCES:
        if not source.get("enabled"):
            continue
        try:
            items = fetch_rss_items(source) if source["fetch_method"] == "rss" else fetch_html_items(source)
            all_news.extend(items)
            print(f"{source['source_name']}: {len(items)}")
        except Exception as e:
            print(f"ERROR in {source['source_name']}: {e}")

    unique = []
    seen = set()
    for item in sorted(all_news, key=lambda x: x.get('date', ''), reverse=True):
        if item['hash'] in seen:
            continue
        seen.add(item['hash'])
        unique.append(item)

    save_json("news.json", {"updated_at": now_iso(), "items": unique[:200]})
    print(f"Saved {len(unique)} news items")


if __name__ == "__main__":
    main()
