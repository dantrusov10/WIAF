# Google Apps Script setup

1. Открой Google Sheet:
   https://docs.google.com/spreadsheets/d/1okmW_eURpTV_Uz5X7bc-JrvUruB2XMVPAIHVcwbqAJw/edit
2. Extensions → Apps Script.
3. Вставь содержимое `api.gs`.
4. Deploy → New deployment → Web app.
5. Execute as: Me.
6. Who has access: Anyone.
7. Скопируй URL deployment и вставь его в `public/config.js` в поле `sheetApiBase`.

После этого сайт будет читать данные напрямую из Google Sheets:
- `?entity=news`
- `?entity=rates`
- `?entity=indices`
- `?entity=market_stats`
