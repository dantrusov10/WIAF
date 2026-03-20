# Парсеры и данные

Сейчас в проекте есть два режима:

1. Локальный / GitHub JSON
   - парсер пишет данные в `public/data/*.json`
   - сайт читает JSON напрямую

2. Google Sheets / Apps Script
   - витрина читает данные по `sheetApiBase` из `public/config.js`
   - API публикуется из `google-apps-script/api.gs`

## Запуск локально

```bash
pip install -r scripts/requirements.txt
python scripts/fetch_market_data.py
```

## GitHub Actions

Workflow уже лежит в `.github/workflows/update-data.yml`.
После пуша его можно запустить во вкладке Actions.
