# WIAF.RU — Frontend

Фронтенд платформы WIAF. Статический сайт, деплой через GitHub Pages.

## Деплой за 3 минуты

### 1. Создать репозиторий на GitHub
```
github.com → New repository → wiaf-site → Public → Create
```

### 2. Загрузить файл
```
Add file → Upload files → перетащить index.html → Commit changes
```

### 3. Включить GitHub Pages
```
Settings → Pages → Source: Deploy from branch → Branch: main / (root) → Save
```

Через ~1 минуту сайт появится на:
```
https://[ваш-username].github.io/wiaf-site/
```

---

## Обновление данных SCFI (каждую пятницу)

SCFI публикуется каждую пятницу ~10:00 UTC на [sse.net.cn](https://en.sse.net.cn/indices/scfinew.jsp)
или удобнее смотреть на [container-news.com/scfi/](https://container-news.com/scfi/).

В `index.html` найти массив `SCFI_DATA` и добавить строку:
```js
{d:'2026-03-20',v:XXXX.X},  // ← новое значение пятницы
```

Заменить в репозитории на GitHub (Edit file → commit) — деплой автоматический.

---

## Структура сайта

| Страница | ID |
|---|---|
| Главная | `page-home` |
| Правила для импортёра | `page-rules-importer` |
| Правила для экспедитора | `page-rules-forwarder` |
| Вход импортёра | `page-login-importer` |
| Вход экспедитора | `page-login-forwarder` |
| О компании | `page-about` |
| Контакты | `page-contacts` |

Навигация — SPA на чистом JS (функция `goTo(id)`).

## Кастомный домен (wiaf.ru)

После деплоя на GitHub Pages:
```
Settings → Pages → Custom domain → wiaf.ru → Save
```
Затем у регистратора домена добавить CNAME-запись:
```
CNAME wiaf.ru → [username].github.io
```

## Зависимости (CDN, интернет не нужен для деплоя)

- Google Fonts: Unbounded + Geologica
- Lucide Icons 0.383.0
- Chart.js 4.4.1
- Anthropic API (для блока новостей — нужен API ключ)
