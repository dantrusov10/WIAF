window.WIAF_CONFIG = {
  // Для быстрого локального запуска оставляем JSON из public/data.
  localBase: './public/data',

  // После деплоя Apps Script вставь сюда URL вида:
  // https://script.google.com/macros/s/AKfycbxxxxxxxx/exec
  // Тогда сайт начнет читать данные напрямую из Google Sheets.
  sheetApiBase: '',

  // Если понадобятся разные endpoint для каждой сущности, можно указать их тут.
  // Например:
  // sheetEndpoints: {
  //   news: 'https://.../exec?sheet=news_final',
  //   rates: 'https://.../exec?sheet=rates_final',
  //   indices: 'https://.../exec?sheet=indices_final',
  //   market_stats: 'https://.../exec?sheet=market_stats'
  // }
  sheetEndpoints: {}
};
