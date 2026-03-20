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


## Расширенный пул источников

В `scripts/sources.py` добавлен расширенный реестр источников с приоритетом RU. После запуска парсера статус по каждому активному источнику сохраняется в `public/data/source_status.json`. Это позволяет быстро увидеть, какой сайт реально отдал материалы, а какой вернул 0 записей или ошибку.

Рекомендуемый порядок:
1. Сначала оставить включенными только RU-источники.
2. Запустить `Update market data`.
3. Проверить `source_status.json`.
4. После этого постепенно включать запасные INT-источники.
