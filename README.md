# WIAF — updated MVP

Обновлённый статический репозиторий под многостраничный терминал рынка.

## Что добавлено
- Яндекс.Метрика `108163869`
- новые разделы: рынок, ставки и индексы, направления, аукционы, аналитика, страницы для импортёра и экспедитора
- кликабельные CTA на реальные URL входа и регистрации
- загрузка данных из `public/data/*.json`
- базовый Python-парсер и GitHub Actions по расписанию
- заготовка Apps Script API

## Основные внешние ссылки
- Вход: `https://wiaf.ru/Seller/Seller_login.php`
- Регистрация импортёра: `https://wiaf.ru/Seller/Captcha/Seller_ca_OOO.php`
- Регистрация экспедитора: `https://wiaf.ru/BUYER/Captcha/Buyer_ca_OOO.php`

## Структура
- `index.html` — основной фронт
- `public/data/` — JSON-данные для витрины
- `scripts/` — парсер и источники
- `.github/workflows/update-data.yml` — автообновление данных
- `google-apps-script/api.gs` — JSON endpoint из Google Sheets

## Дальше
1. Подменить тестовые JSON на реальные выгрузки.
2. Настроить секреты GitHub, если будете писать обратно в Sheets.
3. Разбить SPA на отдельные HTML/маршруты, если пойдёте в полноценную SEO-структуру на Next.js/Astro.
