require('dotenv').config();
 const TelegramBot = require('node-telegram-bot-api');
 const { google } = require('googleapis');
 const fsPromises = require('fs').promises;
 const fs = require('fs');
 const axios = require('axios');
 const path = require('path');
 const { v4: uuidv4 } = require('uuid');
 const OpenAI = require('openai');

 // Environment variables
 const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
 const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
 const SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
 const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
 const EXPENSES_FOLDER = '1LXNlURWVeVW8WMlMjkbs88kpgAbR_oYA';
 const EARNINGS_FOLDER = '1zczS11J9iCgbpvdNdqsvSxr1BDnHghTs';

 // Ensure tmp directory exists
 const TMP_DIR = './tmp';
 fsPromises.mkdir(TMP_DIR, { recursive: true }).catch(err => console.error('Failed to create tmp dir:', err));

 // Write service account key if provided via env
 if (SERVICE_ACCOUNT_JSON) {
   fsPromises.writeFile('./service-account-key.json', SERVICE_ACCOUNT_JSON)
     .catch(err => console.error('Failed to write service account key:', err));
 }

 const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

 const auth = new google.auth.GoogleAuth({
   keyFile: './service-account-key.json',
   scopes: ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'],
 });
 const sheets = google.sheets({ version: 'v4', auth });
 const drive = google.drive({ version: 'v3', auth });

 const monthMap = {
   'January': 'Janvier', 'February': 'Février', 'March': 'Mars', 'April': 'Avril',
   'May': 'Mai', 'June': 'Juin', 'July': 'Juillet', 'August': 'Août',
   'September': 'Septembre', 'October': 'Octobre', 'November': 'Novembre', 'December': 'Décembre'
 };

 function capitalize(str) {
   return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
 }

 async function getSheetId(tabName) {
   try {
     const spreadsheet = await sheets.spreadsheets.get({
       spreadsheetId: GOOGLE_SHEET_ID,
       fields: 'sheets(properties(sheetId,title))',
     });
     const sheet = spreadsheet.data.sheets.find(s => s.properties.title === tabName);
     if (!sheet) {
       throw new Error(`Sheet "${tabName}" not found`);
     }
     return sheet.properties.sheetId;
   } catch (err) {
     console.error(`Failed to get sheetId for ${tabName}:`, err.message);
     throw err;
   }
 }

 async function getSheet(tabName) {
   try {
     const res = await sheets.spreadsheets.values.get({
       spreadsheetId: GOOGLE_SHEET_ID,
       range: `${tabName}!A1:Z`,
     });
     return res.data.values || [];
   } catch (err) {
     console.error(`Failed to get sheet ${tabName}:`, err.message);
     throw new Error(`Cannot access sheet ${tabName}`);
   }
 }

 async function updateRow(tabName, rowIndex, rowData) {
   try {
     await sheets.spreadsheets.values.update({
       spreadsheetId: GOOGLE_SHEET_ID,
       range: `${tabName}!A${rowIndex + 1}`,
       valueInputOption: 'USER_ENTERED',
       resource: { values: [rowData] },
     });
   } catch (err) {
     console.error(`Failed to update row ${rowIndex + 1} in ${tabName}:`, err.message);
     throw err;
   }
 }

 async function insertRow(tabName, rowData, rowIndex = null) {
   try {
     if (rowIndex !== null) {
       const sheetId = await getSheetId(tabName);
       await sheets.spreadsheets.batchUpdate({
         spreadsheetId: GOOGLE_SHEET_ID,
         resource: {
           requests: [{
             insertRange: {
               range: {
                 sheetId: sheetId,
                 startRowIndex: rowIndex,
                 endRowIndex: rowIndex + 1,
               },
               shiftDimension: 'ROWS',
             },
           }],
         },
       });
       await updateRow(tabName, rowIndex, rowData);
     } else {
       await sheets.spreadsheets.values.append({
         spreadsheetId: GOOGLE_SHEET_ID,
         range: `${tabName}!A2`,
         valueInputOption: 'USER_ENTERED',
         resource: { values: [rowData] },
       });
     }
   } catch (err) {
     console.error(`Failed to insert row in ${tabName}:`, err.message);
     throw err;
   }
 }

 async function uploadToDrive(filePath, fileName, folderId) {
   try {
     await fsPromises.access(filePath, fs.constants.R_OK);
     console.log(`Drive: Accessing file at ${filePath} for upload`);

     const res = await drive.files.create({
       requestBody: { name: fileName, parents: [folderId] },
       media: { mimeType: 'image/jpeg', body: fs.createReadStream(filePath) },
     });
     await drive.permissions.create({
       fileId: res.data.id,
       requestBody: { role: 'reader', type: 'anyone' },
     });
     const link = `https://drive.google.com/file/d/${res.data.id}/view`;
     console.log(`Drive: Uploaded file ${fileName} to folder ${folderId}, link: ${link}`);
     return link;
   } catch (err) {
     console.error(`Drive: Failed to upload file ${filePath} to folder ${folderId}:`, err.message);
     throw err;
   }
 }

 async function extractFromImage(base64Image) {
   const prompt = `You're a receipt reader. Extract this information in JSON format with no markdown or code fences:\n{\n  "amount": 12.34,\n  "currency": "CAD",\n  "name": "Vendor name",\n  "date": "5 June"\n}\nReturn only the JSON object.`;

   try {
     const result = await openai.chat.completions.create({
       model: 'gpt-4o',
       messages: [
         {
           role: 'user',
           content: [
             { type: 'text', text: prompt },
             { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${base64Image}` } },
           ],
         },
       ],
     });

     const responseText = result.choices[0].message.content.trim();
     console.log("GPT Image Response:", responseText);

     const cleaned = responseText.replace(/```json|```/g, '').trim();
     return JSON.parse(cleaned);
   } catch (err) {
     console.error("Image extraction error:", err.message);
     return null;
   }
 }

 async function handleUSDConversion(chatId, input) {
   const frankfurter = 'https://api.frankfurter.app';
   const tabName = monthMap[capitalize(input)] || capitalize(input);
   let sheet;
   try {
     sheet = await getSheet(tabName);
     console.log(`Conversion: Loaded sheet ${tabName} with ${sheet.length} rows`);
   } catch (err) {
     console.error(`Conversion: Failed to load sheet ${tabName}:`, err.message);
     return bot.sendMessage(chatId, `WARNING ${err.message}`);
   }

   if (!sheet.length || sheet.length === 1) {
     console.log(`Conversion: No data rows in sheet ${tabName}`);
     return bot.sendMessage(chatId, `WARNING No data in sheet ${tabName}`);
   }

   let conversionsPerformed = 0;
   try {
     for (let i = 1; i < sheet.length; i++) {
       const row = sheet[i];
       if (!row || !row[0]?.trim()) {
         console.warn(`WARNING Skipping row ${i + 1}: No date or empty row`);
         continue;
       }

       const date = row[0].trim();
       const dateObj = new Date(date);
       if (isNaN(dateObj)) {
         console.warn(`WARNING Skipping row ${i + 1}: Invalid date "${date}"`);
         continue;
       }
       const dateFormatted = dateObj.toISOString().split('T')[0];

       const columnsToConvert = [
         { usdIndex: 3, cadIndex: 2 }, // Expense: D -> C
         { usdIndex: 6, cadIndex: 5 }, // Earning: G -> F
         { usdIndex: 8, cadIndex: 9 }, // PayPal fee: I -> J
       ];

       for (const { usdIndex, cadIndex } of columnsToConvert) {
         const usdRaw = row[usdIndex]?.toString().trim() || '';
         const cadConverted = row[cadIndex]?.toString().trim() || '';
         console.log(`Conversion: Checking row ${i + 1}, USD column ${usdIndex + 1}: "${usdRaw}", CAD column ${cadIndex + 1}: "${cadConverted}"`);

         // Detect USD: numeric, $ prefixed, or USD keyword
         const cleanedUSD = usdRaw.replace(/[^0-9.]/g, '');
         const isNumericUSD = !isNaN(parseFloat(cleanedUSD)) && cleanedUSD !== '';
         const hasUSD = isNumericUSD && (
           usdRaw.match(/^\$?\s*\d+(\.\d+)?$/) || // 10, 10.00, $10, $ 10, $10.00, $ 10.00
           usdRaw.match(/^\d+(\.\d+)?\s*USD$/i) || // 10 USD, 10.00 USD
           usdRaw.match(/^USD\s*\d+(\.\d+)?$/i) || // USD 10, USD 10.00
           usdRaw.match(/^\d+(\.\d+)?\s*\$/) // 10$, 10.00$
         );

         if (!hasUSD) {
           console.log(`Conversion: Skipped row ${i + 1}, USD column ${usdIndex + 1}: Not a valid USD value "${usdRaw}"`);
           continue;
         }

         if (cadConverted) {
           console.log(`Conversion: Skipped row ${i + 1}, USD column ${usdIndex + 1}: USD="${usdRaw}", CAD="${cadConverted}" (CAD not empty)`);
           continue;
         }

         const value = parseFloat(cleanedUSD);
         if (isNaN(value)) {
           console.warn(`Conversion: Invalid USD value "${usdRaw}" in row ${i + 1}, column ${usdIndex + 1}`);
           continue;
         }

         try {
           console.log(`Conversion: Attempting to convert ${value} USD for ${dateFormatted} in row ${i + 1}`);
           const res = await axios.get(`${frankfurter}/${dateFormatted}?from=USD&to=CAD`);
           const rate = res.data.rates.CAD;
           const converted = (value * rate).toFixed(2);
           row[cadIndex] = converted;
           await updateRow(tabName, i, row);
           conversionsPerformed++;
           bot.sendMessage(chatId, `CONVERT Converted ${value} USD → ${converted} CAD (column ${cadIndex + 1}) on ${date}`);
         } catch (err) {
           console.error(`Conversion: Failed for ${date} in row ${i + 1}:`, err.message);
           bot.sendMessage(chatId, `ERROR Failed to convert for ${date}: ${err.message}`);
         }
       }
     }
   } catch (err) {
     console.error(`Conversion: Unexpected error in conversion loop for ${tabName}:`, err.message);
     bot.sendMessage(chatId, `ERROR Unexpected error during conversion: ${err.message}`);
     return;
   }

   console.log(`Conversion: Completed with ${conversionsPerformed} conversions`);
   if (conversionsPerformed === 0) {
     bot.sendMessage(chatId, `INFO No USD values found to convert in ${tabName}`);
   } else {
     bot.sendMessage(chatId, `SUCCESS Completed conversion for ${tabName} with ${conversionsPerformed} update(s)`);
   }
 }

 bot.onText(/\/convert_missing_usd (.+)/, async (msg, match) => {
   const chatId = msg.chat.id;
   await handleUSDConversion(chatId, match[1].trim());
 });

 bot.on('message', async (msg) => {
   const chatId = msg.chat.id;
   const text = msg.text?.toLowerCase() || '';
   console.log("BOT Heard message:", text);

   // Handle conversion requests first
   const monthKeywords = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
   const monthMatch = monthKeywords.find(m => text.includes(m));
   const mentionedMonth = monthMatch
     ? monthMap[capitalize(monthMatch)] || capitalize(monthMatch)
     : (text.includes("all") ? "none" : null);

   if (text && !msg.photo) {
     const isConversionRequest = /convert.*usd|usd.*convert|update.*usd|missing.*usd|usd.*missing/i.test(text);
     if (isConversionRequest && mentionedMonth && mentionedMonth !== 'none') {
       console.log(`Conversion: Detected request for month ${mentionedMonth}`);
       await handleUSDConversion(chatId, mentionedMonth);
       return;
     }

     const conversionPrompt = `
       Determine if the user's message is a request to convert USD to CAD for a specific month.
       Return JSON (no markdown or code fences):
       {
         "isConversionRequest": boolean,
         "month": "month name or null"
       }
       If no month is specified or "all" is mentioned, set month to null.
       Example input: "convert USD in June"
       Example output: {"isConversionRequest":true,"month":"June"}
       Example input: "convert usd for june"
       Example output: {"isConversionRequest":true,"month":"June"}
       Input: "${text}"
     `;

     let conversionParsed;
     try {
       const result = await openai.chat.completions.create({
         model: 'gpt-4o',
         messages: [{ role: 'user', content: conversionPrompt }],
       });
       conversionParsed = JSON.parse(result.choices[0].message.content.trim());
       console.log(`Conversion: ChatGPT parsed: ${JSON.stringify(conversionParsed)}`);
     } catch (err) {
       console.error("ChatGPT conversion parse error:", err.message);
       conversionParsed = { isConversionRequest: false, month: null };
     }

     if (conversionParsed.isConversionRequest && conversionParsed.month && conversionParsed.month !== 'none') {
       console.log(`Conversion: ChatGPT detected request for month ${conversionParsed.month}`);
       await handleUSDConversion(chatId, conversionParsed.month);
       return;
     }
   }

   if (text && !msg.photo) {
     const prompt = `
       You're a financial assistant for a Telegram bot. Analyze the user's message and determine if it describes an expense, earning, or PayPal fee. Extract the following details in JSON format (no markdown or code fences):
       {
         "type": "expense|earning|paypal_fee",
         "amount": number,
         "currency": "CAD|USD",
         "name": "vendor or client name",
         "date": "day month" (e.g., "13 June"),
         "valid": boolean
       }
       If the input is ambiguous or not a financial transaction, set "valid": false.
       If currency is not specified, default to CAD.
       If date is not specified, use the current date.
       Example input: "spent 6.66 for capcut on june 13"
       Example output: {"type":"expense","amount":6.66,"currency":"CAD","name":"capcut","date":"13 June","valid":true}
       Input: "${text}"
     `;

     let parsed;
     try {
       const result = await openai.chat.completions.create({
         model: 'gpt-4o',
         messages: [{ role: 'user', content: prompt }],
       });

       const responseText = result.choices[0].message.content.trim();
       console.log("ChatGPT Transaction Response:", responseText);
       parsed = JSON.parse(responseText);
     } catch (err) {
       console.error("ChatGPT transaction parse error:", err.message);
       return bot.sendMessage(chatId, `ERROR Failed to process message: ${err.message}`);
     }

     if (!parsed || !parsed.valid) {
       return bot.sendMessage(chatId, `WARNING Sorry, I couldn't understand the transaction. Please clarify (e.g., "spent 10 CAD for coffee on 5 June").`);
     }

     const { type, amount, currency, name, date } = parsed;
     const [day, monthStr] = date.split(" ");
     const capitalMonth = capitalize(monthStr);
     const tabName = monthMap[capitalMonth] || capitalMonth;

     const knownMonthNames = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
     if (!knownMonthNames.includes(capitalMonth.toLowerCase())) {
       return bot.sendMessage(chatId, `WARNING Invalid month: "${monthStr || 'none'}". Please use a valid month.`);
     }

     const baseDate = new Date(`${capitalMonth} ${day}, ${new Date().getFullYear()}`);
     if (isNaN(baseDate)) {
       return bot.sendMessage(chatId, `WARNING Invalid date: ${capitalMonth} ${day}`);
     }
     const formattedDate = baseDate.toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' });

     let sheet;
     try {
       sheet = await getSheet(tabName);
     } catch (err) {
       return bot.sendMessage(chatId, `WARNING ${err.message}`);
     }

     let insertIndex = sheet.findIndex((row, i) => {
       if (i === 0) return false;
       const rowDate = new Date(row[0]);
       const targetDate = baseDate;
       return rowDate.getDate() === targetDate.getDate() &&
              rowDate.getMonth() === targetDate.getMonth() &&
              rowDate.getFullYear() === targetDate.getFullYear() &&
              row.slice(1, 12).every(cell => !cell?.trim());
     });

     let targetInsertIndex = null;
     if (insertIndex === -1) {
       let lastIndex = -1;
       for (let i = 1; i < sheet.length; i++) {
         const rowDate = new Date(sheet[i][0]);
         if (rowDate.getDate() === baseDate.getDate() &&
             rowDate.getMonth() === baseDate.getMonth() &&
             rowDate.getFullYear() === baseDate.getFullYear()) {
           lastIndex = i;
         }
       }
       if (lastIndex !== -1) {
         targetInsertIndex = lastIndex + 1;
       }
     }

     // Simplified log to avoid syntax issues
     const filteredRows = sheet.filter(row => new Date(row[0]).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' }) === formattedDate);
     console.log(`Text: Insert info: index=${insertIndex}, date=${formattedDate}, targetIndex=${targetInsertIndex}, rows=${JSON.stringify(filteredRows)}`);

     const rowData = new Array(12).fill('');
     rowData[0] = formattedDate;
     rowData[1] = name || 'Unknown';

     if (type === 'expense') {
       rowData[currency === 'CAD' ? 2 : 3] = currency === 'CAD' ? amount : `$${amount}`;
     } else if (type === 'earning') {
       rowData[4] = currency === 'CAD' ? amount : '';
       rowData[5] = currency === 'CAD' ? amount : '';
       rowData[6] = currency === 'USD' ? `$${amount}` : '';
     } else if (type === 'paypal_fee') {
       rowData[8] = currency === 'USD' ? `$${amount}` : '';
       rowData[9] = currency === 'CAD' ? amount : '';
     }

     try {
       if (insertIndex > 0) {
         console.log(`Text: Updating row ${insertIndex + 1} with data=${JSON.stringify(rowData)}`);
         await updateRow(tabName, insertIndex, rowData);
       } else {
         console.log(`Text: Inserting new row at ${targetInsertIndex !== null ? targetInsertIndex + 1 : 'bottom'} with data=${JSON.stringify(rowData)}`);
         await insertRow(tabName, rowData, targetInsertIndex);
       }
       bot.sendMessage(chatId, `SUCCESS Added ${type} for ${name || 'Unknown'} on ${formattedDate}`);
     } catch (err) {
       console.error(`Text: Error adding entry: ${err.message}`);
       bot.sendMessage(chatId, `ERROR Failed to add entry: ${err.message}`);
     }
     return;
   }
   if (msg.photo) {
     const fileId = msg.photo[msg.photo.length - 1].file_id;
     let fileUrl;
     try {
       fileUrl = await bot.getFileLink(fileId);
       console.log(`Image: ${fileId}: Retrieved file URL: ${fileUrl}`);
       if (typeof fileUrl !== 'string' || !fileUrl.startsWith('https://')) {
         throw new Error(`Invalid file URL: ${fileUrl}`);
       }
     } catch (err) {
       console.error(`Image: Failed to get file URL for fileId ${fileId}:`, err.message);
       return bot.sendMessage(chatId, `ERROR Failed to retrieve image URL: ${err.message}`);
     }

     let response;
     try {
       response = await axios.get(fileUrl, { responseType: 'arraybuffer' });
       console.log(`Image: ${fileId}: Downloaded image, size: ${response.data.length} bytes`);
     } catch (err) {
       console.error(`Image: Failed to download image from ${fileUrl}:`, err.message);
       return bot.sendMessage(chatId, `ERROR Failed to download image: ${err.message}`);
     }

     const base64Image = Buffer.from(response.data).toString('base64');
     let parsed;
     try {
       parsed = await extractFromImage(base64Image);
       if (!parsed) {
         throw new Error(`Image parsing returned no data`);
       }
     } catch (err) {
       console.error(`Image: Failed to extract data for fileId ${fileId}:`, err.message);
       return bot.sendMessage(chatId, `ERROR Failed to extract image: ${err.message}`);
     }

     console.log(`Image: ${fileId}: Parsed data: ${JSON.stringify(parsed)}`);
     const { amount, currency, name, date } = parsed;

     // Normalize date using GPT-4o
     const datePrompt = `
       You are a strict date parser. Analyze the input date string and return a JSON object with the day and month in the format:
       {
         "day": number,
         "month": "full month name in English"
       }
       - "day" MUST be a number (1-31), not a string.
       - "month" MUST be a full English month name (e.g., "January", "February").
       - Ignore any year provided.
       - Handle formats like "Feb 28, 2025", "28 Feb", "28, Feb", "28/02/2025", "28 February", "28-Feb-2025".
       - For "day Month" (e.g., "28 Feb"), treat first part as day, second as month.
       - For "Month day" (e.g., "Feb 28"), treat first part as month, second as day.
       - If ambiguous or invalid, return { "day": null, "month": null }.
       - Return ONLY the JSON object as plain text, no markdown or code fences.
       Examples:
       Input: "Feb 28, 2025" -> {"day":28,"month":"February"}
       Input: "28 Feb" -> {"day":28,"month":"February"}
       Input: "28, Feb" -> {"day":28,"month":"February"}
       Input: "28/02/2025" -> {"day":28,"month":"February"}
       Input: "28 February" -> {"day":28,"month":"February"}
       Input: "28" -> {"day":null,"month":null}
       Input: "${date}"
     `;

     let normalizedDate;
     try {
       const dateResult = await openai.chat.completions.create({
         model: 'gpt-4o',
         messages: [{ role: 'user', content: datePrompt }],
         max_tokens: 50,
         temperature: 0
       });
       const rawResponse = dateResult.choices[0].message.content.trim();
       console.log(`Image: ${fileId}: GPT-4o response: ${rawResponse}`);
       normalizedDate = JSON.parse(rawResponse);
       console.log(`Image: ${fileId}: Normalized date: ${JSON.stringify(normalizedDate)}`);
     } catch (err) {
       console.error(`Image: ${fileId}: Failed to normalize date "${date}":`, err.message);
       // Fallback: manual parsing
       const parts = date.trim().split(/[,\s]+/).filter(part => part);
       if (parts.length >= 2) {
         let dayStr, monthStr;
         if (/^[0-9]+$/.test(parts[0])) {
           // "28 Feb" -> day first
           dayStr = parts[0].replace(/[^0-9]/g, '');
           monthStr = parts[1].replace(/[^a-zA-Z]/g, '');
         } else {
           // "Feb 28" -> month first
           monthStr = parts[0].replace(/[^a-zA-Z]/g, '');
           dayStr = parts[1].replace(/[^0-9]/g, '');
         }
         const monthFull = {
           jan: 'January', feb: 'February', mar: 'March', apr: 'April',
           may: 'May', jun: 'June', jul: 'July', aug: 'August',
           sep: 'September', oct: 'October', nov: 'November', dec: 'December'
         }[monthStr.toLowerCase()] || monthStr;
         const dayNum = parseInt(dayStr);
         if (dayNum >= 1 && dayNum <= 31 && Object.keys(monthMap).includes(capitalize(monthFull))) {
           normalizedDate = { day: dayNum, month: monthFull };
           console.log(`Image: ${fileId}: Fallback date: ${JSON.stringify(normalizedDate)}`);
         }
       }
       if (!normalizedDate) {
         console.error(`Image: ${fileId}: Fallback failed for date "${date}"`);
         return bot.sendMessage(chatId, `ERROR Failed to parse date: "${date}"`);
       }
     }

     if (
       !normalizedDate.day ||
       !normalizedDate.month ||
       typeof normalizedDate.day !== 'number' ||
       normalizedDate.day < 1 ||
       normalizedDate.day > 31 ||
       typeof normalizedDate.month !== 'string'
     ) {
       console.error(`Image: ${fileId}: Invalid date: ${JSON.stringify(normalizedDate)}`);
       return bot.sendMessage(chatId, `WARNING Invalid date: "${date}"`);
     }

     const validMonths = Object.keys(monthMap).map(m => m.toLowerCase());
     const capitalMonth = capitalize(normalizedDate.month);
     if (!validMonths.includes(capitalMonth.toLowerCase())) {
       console.error(`Image: ${fileId}: Invalid month: "${normalizedDate.month}" -> "${capitalMonth}"`);
       return bot.sendMessage(chatId, `WARNING Invalid month: "${normalizedDate.month}"`);
     }

     const day = normalizedDate.day.toString();
     const tabName = monthMap[capitalMonth] || capitalMonth;

     // Numeric month for Date constructor
     const monthIndex = Object.keys(monthMap).indexOf(capitalMonth) + 1; // February = 2
     const dateString = `${monthIndex}/${day}/${new Date().getFullYear()}`;
     console.log(`Image: ${fileId}: Date: ${dateString}`);
     const formattedDate = new Date(dateString).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' });
     if (isNaN(new Date(formattedDate))) {
       console.error(`Image: ${fileId}: Invalid date: ${dateString}`);
       return bot.sendMessage(chatId, `WARNING Invalid date: ${day} ${capitalMonth}`);
     }

     const tempPath = path.join(TMP_DIR, `${uuidv4()}.jpg`);
     try {
       await fsPromises.writeFile(tempPath, Buffer.from(response.data));
       console.log(`Image: ${fileId}: Saved temp file: ${tempPath}`);
       const folderId = /earning|revenue|invoice|payment received/i.test(name) ? EARNINGS_FOLDER : EXPENSES_FOLDER;
       const link = await uploadToDrive(tempPath, `${name}_${formattedDate}.jpg`, folderId);
       console.log(`Image: ${fileId}: Drive link: ${link}`);

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
         console.error(`Image: ${fileId}: Failed to get sheet ${tabName}:`, err.message);
         return bot.sendMessage(chatId, `WARNING ${err.message}`);
       }

       let insertIndex = sheet.findIndex((row, i) => {
         if (i === 0) return false;
         const rowDate = new Date(row[0]);
         const targetDate = new Date(`${monthIndex}/${day}/${new Date().getFullYear()}`);
         return rowDate.getDate() === targetDate.getDate() &&
                rowDate.getMonth() === targetDate.getMonth() &&
                rowDate.getFullYear() === targetDate.getFullYear() &&
                row.slice(1, 12).every(cell => !cell?.trim());
       });

       let targetInsertIndex = null;
       if (insertIndex === -1) {
         let lastIndex = -1;
         for (let i = 1; i < sheet.length; i++) {
           const rowDate = new Date(sheet[i][0]);
           const targetDate = new Date(`${monthIndex}/${day}/${new Date().getFullYear()}`);
           if (rowDate.getDate() === targetDate.getDate() &&
               rowDate.getMonth() === targetDate.getMonth() &&
               rowDate.getFullYear() === targetDate.getFullYear()) {
             lastIndex = i;
           }
         }
         if (lastIndex !== -1) {
           targetInsertIndex = lastIndex + 1;
         }
       }

       // Simplified log to avoid syntax issues
       const filteredRows = sheet.filter(row => new Date(row[0]).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' }) === formattedDate);
       console.log(`Image: ${fileId}: Insert info: index=${insertIndex}, date=${formattedDate}, targetIndex=${targetInsertIndex}, rows=${JSON.stringify(filteredRows)}`);

       try {
         if (insertIndex > 0) {
           console.log(`Image: ${fileId}: Updating row ${insertIndex + 1} with data=${JSON.stringify(rowData)}`);
           await updateRow(tabName, insertIndex, rowData);
         } else {
           console.log(`Image: ${fileId}: Inserting row at ${targetInsertIndex !== null ? targetInsertIndex + 1 : 'bottom'} with data=${JSON.stringify(rowData)}`);
           await insertRow(tabName, rowData, targetInsertIndex);
         }
         bot.sendMessage(chatId, `IMAGE Added receipt for ${name || 'Unknown'} on ${formattedDate}`);
       } catch (err) {
         console.error(`Image: ${fileId}: Error adding entry:`, err.message);
         bot.sendMessage(chatId, `ERROR Failed to process receipt: ${err.message}`);
       }
     } catch (err) {
       console.error(`Image: ${fileId}: Error processing receipt:`, err.message);
       bot.sendMessage(chatId, `ERROR Failed to process receipt: ${err.message}`);
     } finally {
       await fsPromises.unlink(tempPath);
     }
   } // Closing brace for bot.on('message', ...) - Line 669