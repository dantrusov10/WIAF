function doGet(e) {
  const sheetName = (e.parameter.sheet || 'news_final').trim();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);

  if (!sh) {
    return ContentService.createTextOutput(JSON.stringify({ error: 'Sheet not found' }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  const values = sh.getDataRange().getValues();
  const headers = values[0] || [];
  const items = values.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = row[i]);
    return obj;
  });

  return ContentService.createTextOutput(JSON.stringify({
    updated_at: new Date().toISOString(),
    sheet: sheetName,
    items
  })).setMimeType(ContentService.MimeType.JSON);
}
