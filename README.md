# WIAF

Обновлённый репозиторий для статического MVP терминала рынка.

## Что внутри

- `index.html` — витрина сайта
- `public/data/*.json` — локальные данные для быстрого запуска
- `public/config.js` — переключение между локальными JSON и Google Sheets API
- `.github/workflows/update-data.yml` — автозапуск парсера через GitHub Actions
- `scripts/` — Python-парсер и настройки источников
- `google-apps-script/api.gs` — API для Google Sheets

## Как включить Google Sheets

1. Открой `google-apps-script/api.gs`
2. Вставь код в Apps Script, привязанный к таблице
3. Задеплой как Web App
4. В `public/config.js` вставь URL в `sheetApiBase`
5. Перезалей сайт

## Локальный режим

Пока `sheetApiBase` пустой, сайт использует `public/data/*.json`.
