bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text?.toLowerCase() || '';
  console.log("🦨 Heard message:", text);

  // Updated regex to handle "on [month] [day]" correctly
  const regex = /(spend|spent|expense|paid|bought|earning|earned|revenue|paypal\s+fee)\s+\$?(\d+\.?\d{0,2})\s*(usd|cad)?\s*(?:for|from)\s*([^0-9].*?)?(?:\s+on\s+([a-zA-Z]+)\s+(\d{1,2}))?/i;
  const match = text.match(regex);
  if (match) {
    const [, typeRaw, amountStr, currencyRaw, nameRaw, monthStr, dayStr] = match;
    const amount = parseFloat(amountStr);
    const currency = (currencyRaw || 'CAD').toUpperCase();
    const name = nameRaw?.trim() || 'Unknown';
    const day = parseInt(dayStr) || 1;
    const capitalMonth = monthStr ? capitalize(monthStr) : 'June'; // Default to current month if missing
    const tabName = monthMap[capitalMonth] || capitalMonth;

    const knownMonthNames = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
    if (!knownMonthNames.includes(capitalMonth.toLowerCase())) {
      return bot.sendMessage(chatId, `⚠️ Invalid month: "${monthStr || 'none'}". Please use a valid month.`);
    }

    let sheet;
    try {
      sheet = await getSheet(tabName);
    } catch (err) {
      return bot.sendMessage(chatId, `⚠️ ${err.message}`);
    }

    const baseDate = new Date(`${capitalMonth} ${day}, ${new Date().getFullYear()}`);
    if (isNaN(baseDate)) {
      return bot.sendMessage(chatId, `⚠️ Invalid date: ${capitalMonth} ${day}`);
    }
    const formattedDate = baseDate.toLocaleDateString('en-CA');

    // Find insertion point: below matching date with empty columns B-L
    const insertIndex = sheet.findIndex((row, i) => {
      if (i === 0) return false;
      const rowDate = new Date(row[0]);
      return rowDate.getDate() === baseDate.getDate() &&
             rowDate.getMonth() === baseDate.getMonth() &&
             row.slice(1, 12).every(cell => !cell?.trim());
    });

    // Initialize row data (12 columns)
    const rowData = new Array(12).fill('');
    rowData[0] = formattedDate;
    rowData[1] = name;

    if (/spend|paid|expense|bought/i.test(typeRaw)) {
      rowData[currency === 'CAD' ? 2 : 3] = currency === 'CAD' ? amount : `$${amount}`;
    } else if (/earning|revenue/i.test(typeRaw)) {
      rowData[4] = currency === 'CAD' ? amount : '';
      rowData[5] = currency === 'CAD' ? amount : '';
      rowData[6] = currency === 'USD' ? `$${amount}` : '';
    } else if (/paypal\s+fee/i.test(typeRaw)) {
      rowData[8] = currency === 'USD' ? `$${amount}` : '';
      rowData[9] = currency === 'CAD' ? amount : '';
    }

    try {
      await insertRow(tabName, rowData, insertIndex > 0 ? insertIndex : null);
      bot.sendMessage(chatId, `✅ Added ${typeRaw} for ${name} on ${formattedDate}`);
    } catch (err) {
      bot.sendMessage(chatId, `❌ Failed to add entry: ${err.message}`);
    }
    return;
  }

  // Handle conversion requests
  const monthKeywords = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
  const monthMatch = monthKeywords.find(m => text.includes(m));
  const mentionedMonth = monthMatch
    ? monthMap[capitalize(monthMatch)] || capitalize(monthMatch)
    : (text.includes("all") ? "none" : null);

  const containsConversionRequest = /convert|update\.?*usd|usd\.?*convert|.?*issing.*usd|usd.?*issing/i.test(String(text || '.'));
  if (monthMatch && containsConversionRequest && mentionedMonth !== 'all') {
    await handleUSDConversion(chatId, mentionedMonth);
    return;
  }

  // Handle photo uploads
  if (msg.photo) {
    const fileId = msg.photo[msg.photo.length - 1].file_id;
    let fileUrl;
    try {
      fileUrl = await bot.getFileLink(fileId);
    } catch (err) {
      return bot.sendMessage(chatId, `❌ ${err.message}`);
    }

    let response;
    try {
      response = await axios.get(fileUrl.href, { responseType: 'arraybuffer' });
    } catch (err) {
      return bot.sendMessage(chatId, `❌ Failed to download image: ${err.message}`);
    }

    const base64Image = Buffer.from(response.data).toString('base64');
    const parsed = await extractFromImage(base64Image);
    if (!parsed) {
      return bot.sendMessage(chatId, `❌ Could not extract info from image.`);
    }

    console.log("Parsed data from image:", parsed);
    const { amount, currency, name, date } = parsed;
    const [day, monthStr] = date.split(" ");
    const capitalMonth = capitalize(monthStr);
    const tabName = monthMap[capitalMonth] || capitalMonth;

    const knownMonthNames = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
    if (!knownMonthNames.includes(capitalMonth.toLowerCase())) {
      return bot.sendMessage(chatId, `⚠️ Invalid month in receipt: "${monthStr}".`);
    }

    const formattedDate = new Date(`${capitalMonth} ${day}, ${new Date().getFullYear()}`).toLocaleDateString('en-CA');
    if (isNaN(new Date(formattedDate))) {
      return bot.sendMessage(chatId, `⚠️ Invalid date in receipt: ${day} ${monthStr}`);
    }

    const tempPath = path.join(TMP_DIR, `${uuidv4()}.jpg`);
    try {
      await fs.writeFile(tempPath, Buffer.from(response.data));
      const folderId = /earning|revenue|invoice|payment received/i.test(name) ? EARNINGS_FOLDER : EXPENSES_FOLDER;
      const link = await uploadToDrive(tempPath, `${name}_${formattedDate}.jpg`, folderId);

      const rowData = new Array(12).fill('');
      rowData[0] = formattedDate;
      rowData[1] = name || 'Unknown';
      rowData[2] = currency === 'CAD' ? amount : '';
      rowData[3] = currency === 'USD' ? `$${amount}` : '';
      rowData[10] = folderId === EXPENSES_FOLDER ? link : '';
      rowData[11] = folderId === EARNINGS_FOLDER ? link : '';

      let sheet;
      try {
        sheet = await getSheet(tabName);
      } catch (err) {
        return bot.sendMessage(chatId, `⚠️ ${err.message}`);
      }

      const insertIndex = sheet.findIndex((row, i) => {
        if (i === 0) return false;
        const rowDate = new Date(row[0]);
        return rowDate.getDate() === parseInt(day) &&
               rowDate.getMonth() === new Date(`${capitalMonth} 1`).getMonth() &&
               row.slice(1, 12).every(cell => !cell?.trim());
      });

      await insertRow(tabName, rowData, insertIndex > 0 ? insertIndex : null);
      bot.sendMessage(chatId, `📸 Added receipt for ${name || 'Unknown'} on ${formattedDate}`);
    } catch (err) {
      bot.sendMessage(chatId, `❌ Failed to process receipt: ${err.message}`);
    } finally {
      await fs.unlink(tempPath).catch(err => console.error('Failed to delete temp file:', err));
    }
  }
});
