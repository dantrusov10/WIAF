const SPREADSHEET_ID = '1okmW_eURpTV_Uz5X7bc-JrvUruB2XMVPAIHVcwbqAJw';

const ENTITY_SHEET_MAP = {
  news: 'news_final',
  rates: 'rates_final',
  indices: 'indices_final',
  market_stats: 'market_stats',
  sources: 'source_registry',
  parser_runs: 'parser_runs',
  directions: 'direction_dictionary'
};

function doGet(e) {
  const entity = (e.parameter.entity || '').trim();
  const explicitSheet = (e.parameter.sheet || '').trim();
  const sheetName = explicitSheet || ENTITY_SHEET_MAP[entity] || 'news_final';
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ss.getSheetByName(sheetName);

  if (!sh) {
    return jsonOutput({
      ok: false,
      error: 'Sheet not found',
      sheet: sheetName,
      spreadsheet_id: SPREADSHEET_ID
    });
  }

  const values = sh.getDataRange().getValues();
  const headers = values[0] || [];
  const items = values.slice(1)
    .filter(row => row.some(cell => String(cell).trim() !== ''))
    .map(row => {
      const obj = {};
      headers.forEach((h, i) => obj[String(h).trim()] = row[i]);
      return obj;
    });

  return jsonOutput({
    ok: true,
    updated_at: new Date().toISOString(),
    entity: entity || null,
    sheet: sheetName,
    spreadsheet_id: SPREADSHEET_ID,
    items
  });
}

function jsonOutput(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
