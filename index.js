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
 const serviceAccount = JSON.parse(process.env.SERVICE_ACCOUNT_KEY_JSON);
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
   const prompt = `
   You are a receipt reader. Extract the following information from the provided image in JSON format with no markdown or code fences:
   {
     "type": "expense|earning|paypal_fee",
     "amount": number,
     "currency": "CAD" | "USD",
     "name": "sender or vendor name",
     "date": {
       "day": number,
       "month": string (e.g., "June" or "juin" initially, then convert to English like "June"),
       "year": number (use current year 2025 if not specified)
     },
     "valid": boolean
   }
   - Determine the type (expense, earning, or paypal_fee) based on the image content (e.g., "invoice" or "payment received" for earning, "fee" for paypal_fee, otherwise expense).
   - Accept dates in French (e.g., "18 juin") or English (e.g., "18 June") and convert the month to the English full name (e.g., "June" for "juin" or "June").
   - If the day, month, or year cannot be determined, set the corresponding field to null and include a "valid": false flag in the JSON.
   - Return only the JSON object.
   `;

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

// Global variable to store pending image data for clarification
const pendingImageTransactions = {};

// Refactored function to process image transactions
async function processImageTransaction(chatId, fileId, parsed, transactionType, tempPath, responseData) {
    const { amount, currency, name, date, valid = true } = parsed;
    const { day, month, year = 2025 } = date;

    if (!day || !month || typeof day !== 'number' || day < 1 || day > 31 || typeof month !== 'string' || !valid) {
        throw new Error(`Invalid date or data from image: "${JSON.stringify(date)}"`);
    }

    const baseDate = new Date(`${month} ${day}, ${year}`);
    if (isNaN(baseDate)) {
        throw new Error(`Invalid date: "${JSON.stringify(date)}"`);
    }

    const formattedDate = baseDate.toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' });
    console.log(`Image: ${fileId}: Date: ${formattedDate}`);

    const capitalMonth = capitalize(month);
    const tabName = monthMap[capitalMonth] || capitalMonth;

    if (!Object.keys(monthMap).map(m => m.toLowerCase()).includes(capitalMonth.toLowerCase())) {
        throw new Error(`Invalid month: "${month}"`);
    }

    let folderId;
    let transactionTypeForMessage = transactionType;
    if (transactionType === 'earning') {
        folderId = EARNINGS_FOLDER;
    } else if (transactionType === 'expense') {
        folderId = EXPENSES_FOLDER;
    } else {
        folderId = EXPENSES_FOLDER; // Default for paypal_fee or other cases
    }

    const link = await uploadToDrive(tempPath, `${name || 'Unknown'}_${formattedDate}.jpg`, folderId);
    console.log(`Image: ${fileId}: Drive link: ${link}`);

    let sheet;
    try {
        sheet = await getSheet(tabName);
    } catch (err) {
        throw new Error(`Failed to get sheet ${tabName}: ${err.message}`);
    }

    let rowData = new Array(12).fill('');
    rowData[0] = formattedDate;

    let insertIndex = -1;
    for (let i = 1; i < sheet.length; i++) {
        const rowDate = new Date(sheet[i][0]);
        const expenseEmpty = [1, 2, 3].every(idx => !sheet[i][idx]?.trim());
        const earningEmpty = [4, 5, 6].every(idx => !sheet[i][idx]?.trim());
        const paypalFeeEmpty = [8, 9].every(idx => !sheet[i][idx]?.trim());

        if (rowDate.getTime() === baseDate.getTime()) {
            if (transactionType === 'expense' && expenseEmpty) {
                insertIndex = i;
                break;
            } else if (transactionType === 'earning' && earningEmpty) {
                insertIndex = i;
                break;
            } else if (transactionType === 'paypal_fee' && paypalFeeEmpty) {
                insertIndex = i;
                break;
            }
        }
    }

    if (insertIndex > 0) {
        rowData = sheet[insertIndex].slice();
    }

    let targetInsertIndex = null;
    if (insertIndex === -1) {
        let lastIndex = -1;
        for (let i = 1; i < sheet.length; i++) {
            const rowDate = new Date(sheet[i][0]);
            if (rowDate.getTime() === baseDate.getTime()) {
                lastIndex = i;
            }
        }
        if (lastIndex !== -1) {
            targetInsertIndex = lastIndex + 1;
        }
    }

    console.log(`Image: ${fileId}: Processing determined type: ${transactionType}, amount: ${amount}, currency: ${currency}, name: ${name}, date: ${formattedDate}`);

    if (transactionType === 'expense') {
        console.log(`Image: Assigning to expense columns: name=${name} to index 1, CAD=${amount} to index 2, USD=$${amount} to index 3`);
        rowData[1] = name || rowData[1] || 'Unknown';
        rowData[2] = currency === 'CAD' ? amount : (rowData[2] || '');
        rowData[3] = currency === 'USD' ? `$${amount}` : (rowData[3] || '');
        rowData[10] = link;
        rowData[4] = rowData[4] || '';
        rowData[5] = rowData[5] || '';
        rowData[6] = rowData[6] || '';
        rowData[8] = rowData[8] || '';
        rowData[9] = rowData[9] || '';
        rowData[11] = rowData[11] || '';
    } else if (transactionType === 'earning') {
        console.log(`Image: Assigning to earning columns: name=${name} to index 4, CAD=${amount} to index 5, USD=$${amount} to index 6`);
        rowData[4] = name || rowData[4] || 'Unknown';
        rowData[5] = currency === 'CAD' ? amount : (rowData[5] || '');
        rowData[6] = currency === 'USD' ? `$${amount}` : (rowData[6] || '');
        rowData[11] = link;
        rowData[1] = rowData[1] || '';
        rowData[2] = rowData[2] || '';
        rowData[3] = rowData[3] || '';
        rowData[8] = rowData[8] || '';
        rowData[9] = rowData[9] || '';
        rowData[10] = rowData[10] || '';
    } else if (transactionType === 'paypal_fee') {
        console.log(`Image: Assigning to PayPal fee columns: USD=$${amount} to index 8, CAD=${amount} to index 9`);
        rowData[8] = currency === 'USD' ? `$${amount}` : (rowData[8] || '');
        rowData[9] = currency === 'CAD' ? amount : (rowData[9] || '');
        rowData[10] = rowData[10] || '';
        rowData[11] = rowData[11] || '';
        rowData[1] = rowData[1] || '';
        rowData[2] = rowData[2] || '';
        rowData[3] = rowData[3] || '';
        rowData[4] = rowData[4] || '';
        rowData[5] = rowData[5] || '';
        rowData[6] = rowData[6] || '';
    }

    console.log(`Image: ${fileId}: Final rowData before insert: ${JSON.stringify(rowData)}`);

    try {
        if (insertIndex > 0) {
            console.log(`Image: ${fileId}: Updating row ${insertIndex + 1} with data=${JSON.stringify(rowData)}`);
            await updateRow(tabName, insertIndex, rowData);
        } else {
            console.log(`Image: ${fileId}: Inserting new row at ${targetInsertIndex !== null ? targetInsertIndex + 1 : 'bottom'} with data=${JSON.stringify(rowData)}`);
            await insertRow(tabName, rowData, targetInsertIndex);
        }
        bot.sendMessage(chatId, `IMAGE Added ${transactionTypeForMessage} for ${name || 'Unknown'} on ${formattedDate}`);
    } catch (err) {
        console.error(`Image: ${fileId}: Error adding entry:`, err.message);
        bot.sendMessage(chatId, `ERROR Failed to process receipt: ${err.message}`);
        throw err; // Re-throw to allow outer catch to handle cleanup
    } finally {
        // Ensure temp file is deleted even if there's an error during sheet update
        if (fs.existsSync(tempPath)) {
            await fsPromises.unlink(tempPath).catch(err => console.error(`Failed to delete temp file ${tempPath}:`, err.message));
        }
    }
}


 bot.on('message', async (msg) => {
   const chatId = msg.chat.id;
   const text = msg.text?.toLowerCase() || '';
   console.log("BOT Heard message:", text);

   // 1. Handle pending image transaction follow-up
   if (text && pendingImageTransactions[chatId]) {
       const pending = pendingImageTransactions[chatId];
       let followUpType = null;
       if (text.includes('expense')) {
           followUpType = 'expense';
       } else if (text.includes('earning')) {
           followUpType = 'earning';
       } else if (text.includes('paypal fee') || text.includes('paypal_fee')) {
           followUpType = 'paypal_fee';
       }

       if (followUpType) {
           console.log(`Follow-up: Received type "${followUpType}" for pending image ${pending.fileId}`);
           // Use the original parsed data, but override the type
           pending.parsed.type = followUpType;

           try {
               await processImageTransaction(
                   chatId,
                   pending.fileId,
                   pending.parsed,
                   pending.parsed.type,
                   pending.tempPath,
                   pending.responseData
               );
               delete pendingImageTransactions[chatId]; // Clear pending state
           } catch (err) {
               console.error(`Follow-up: Error processing pending image ${pending.fileId}:`, err.message);
               bot.sendMessage(chatId, `ERROR Failed to finalize image entry: ${err.message}`);
           } finally {
               // Ensure the temp file created during the initial image upload attempt is cleaned up
               if (fs.existsSync(pending.tempPath)) {
                   await fsPromises.unlink(pending.tempPath).catch(err => console.error(`Failed to delete temp file ${pending.tempPath}:`, err.message));
               }
               delete pendingImageTransactions[chatId]; // Ensure state is cleared on success or failure
           }
           return; // Stop further processing as this was a follow-up
       } else {
           // User replied, but not with a valid type clarification
           bot.sendMessage(chatId, `I'm still waiting for you to specify "expense", "earning", or "paypal fee" for the previous image. Please try again or send a new transaction.`);
           // Don't clear pendingImageTransactions[chatId] yet, give them another chance
           return;
       }
   }

   // Handle conversion requests first (existing logic)
   const monthKeywords = [...Object.keys(monthMap), ...Object.values(monthMap)].map(m => m.toLowerCase());
   const monthMatch = monthKeywords.find(m => text.includes(m));
   const mentionedMonth = monthMatch
     ? monthMap[capitalize(monthMatch)] || capitalize(monthMatch)
     : (text.includes("all") ? "none" : null);

   if (text && !msg.photo) { // This block handles regular text messages (not image captions)
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
       Example output: {"isConversionRequest":true,"month":null} // Modified example: "june" implies the month
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

   // Original text message processing (if not a conversion request or photo)
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

      let insertIndex = -1;
      for (let i = 1; i < sheet.length; i++) {
        const rowDate = new Date(sheet[i][0]);
        // Check if the date matches AND if the relevant columns for the transaction type are empty
        const expenseEmpty = [1, 2, 3].every(idx => !sheet[i][idx]?.trim());
        const earningEmpty = [4, 5, 6].every(idx => !sheet[i][idx]?.trim());
        const paypalFeeEmpty = [8, 9].every(idx => !sheet[i][idx]?.trim());

        if (rowDate.getTime() === baseDate.getTime()) {
          if ((type === 'expense' && expenseEmpty) ||
              (type === 'earning' && earningEmpty) ||
              (type === 'paypal_fee' && paypalFeeEmpty)) {
            insertIndex = i;
            break;
          }
        }
      }

      let targetInsertIndex = null;
      if (insertIndex === -1) {
        let lastIndex = -1;
        for (let i = 1; i < sheet.length; i++) {
          const rowDate = new Date(sheet[i][0]);
          if (rowDate.getTime() === baseDate.getTime()) {
            lastIndex = i;
          }
        }
        if (lastIndex !== -1) {
          targetInsertIndex = lastIndex + 1;
        }
      }

      const filteredRows = sheet.filter(row => new Date(row[0]).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: 'numeric' }) === formattedDate);
      console.log(`Text: Insert info: index=${insertIndex}, date=${formattedDate}, targetIndex=${targetInsertIndex}, rows=${JSON.stringify(filteredRows)}`);

      let rowData = new Array(12).fill('');
      rowData[0] = formattedDate; // Column A (Date)

      // Preserve existing row data if updating
      if (insertIndex > 0) {
        rowData = sheet[insertIndex].slice(); // Copy existing row
      }

      console.log(`Text: Processing type: ${type}, amount: ${amount}, currency: ${currency}, date: ${formattedDate}`); // Debug log

      if (type === 'expense') {
        console.log(`Text: Assigning to expense columns: name=${name} to index 1, CAD=${amount} to index 2, USD=$${amount} to index 3`);
        rowData[1] = name || rowData[1] || 'Unknown'; // Column B (Expense name)
        rowData[2] = currency === 'CAD' ? amount : (rowData[2] || ''); // Column C (CAD expense)
        rowData[3] = currency === 'USD' ? `$${amount}` : (rowData[3] || ''); // Column D (USD expense)
        // Preserve earnings and PayPal columns
        rowData[4] = rowData[4] || ''; // Column E (Earnings name)
        rowData[5] = rowData[5] || ''; // Column F (CAD earning)
        rowData[6] = rowData[6] || ''; // Column G (USD earning)
        rowData[8] = rowData[8] || ''; // Column I (USD PayPal fee)
        rowData[9] = rowData[9] || ''; // Column J (CAD PayPal fee)
        // Ensure image links are preserved/updated
        rowData[10] = rowData[10] || ''; // Column K (Expense Google Drive Link)
        rowData[11] = rowData[11] || ''; // Column L (Earnings Google Drive Link)
      } else if (type === 'earning') {
        console.log(`Text: Assigning to earning columns: name=${name} to index 4, CAD=${amount} to index 5, USD=$${amount} to index 6`);
        rowData[4] = name || rowData[4] || 'Unknown'; // Column E (Earnings name)
        rowData[5] = currency === 'CAD' ? amount : (rowData[5] || ''); // Column F (CAD earning)
        rowData[6] = currency === 'USD' ? `$${amount}` : (rowData[6] || ''); // Column G (USD earning)
        // Preserve expense and PayPal columns
        rowData[1] = rowData[1] || ''; // Column B (Expense name)
        rowData[2] = rowData[2] || ''; // Column C (CAD expense)
        rowData[3] = rowData[3] || ''; // Column D (USD expense)
        rowData[8] = rowData[8] || ''; // Column I (USD PayPal fee)
        rowData[9] = rowData[9] || ''; // Column J (CAD PayPal fee)
        // Ensure image links are preserved/updated
        rowData[10] = rowData[10] || ''; // Column K (Expense Google Drive Link)
        rowData[11] = rowData[11] || ''; // Column L (Earnings Google Drive Link)
      } else if (type === 'paypal_fee') {
        console.log(`Text: Assigning to PayPal fee columns: USD=$${amount} to index 8, CAD=${amount} to index 9`);
        rowData[8] = currency === 'USD' ? `$${amount}` : (rowData[8] || ''); // Column I (USD PayPal fee)
        rowData[9] = currency === 'CAD' ? amount : (rowData[9] || ''); // Column J (CAD PayPal fee)
        // Preserve expense and earnings columns
        rowData[1] = rowData[1] || ''; // Column B (Expense name)
        rowData[2] = rowData[2] || ''; // Column C (CAD expense)
        rowData[3] = rowData[3] || ''; // Column D (USD expense)
        rowData[4] = rowData[4] || ''; // Column E (Earnings name)
        rowData[5] = rowData[5] || ''; // Column F (CAD earning)
        rowData[6] = rowData[6] || ''; // Column G (USD earning)
        // Ensure image links are preserved/updated
        rowData[10] = rowData[10] || ''; // Column K (Expense Google Drive Link)
        rowData[11] = rowData[11] || ''; // Column L (Earnings Google Drive Link)
      }

      console.log(`Text: Final rowData before insert: ${JSON.stringify(rowData)}`); // Debug final state

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
     const caption = msg.caption?.toLowerCase() || ''; // Get the caption if it exists
     console.log(`Image: Detected caption: "${caption}"`);

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

     console.log(`Image: ${fileId}: Parsed data from image: ${JSON.stringify(parsed)}`);

     // Determine 'type' from caption first
     let transactionTypeFromCaption = null;
     if (caption.includes('expense')) {
       transactionTypeFromCaption = 'expense';
     } else if (caption.includes('earning')) {
       transactionTypeFromCaption = 'earning';
     } else if (caption.includes('paypal fee') || caption.includes('paypal_fee')) {
       transactionTypeFromCaption = 'paypal_fee';
     }

     // Use type from caption if available, otherwise from parsed image data
     let transactionType = transactionTypeFromCaption || parsed.type;

     // If no specific type is found in caption AND it's not a PayPal fee from image parsing, ask for clarification
     if (!transactionTypeFromCaption && parsed.type !== 'paypal_fee') {
         // Create temp file now, to store for later processing
         const tempPath = path.join(TMP_DIR, `${uuidv4()}.jpg`);
         await fsPromises.writeFile(tempPath, Buffer.from(response.data));

         pendingImageTransactions[chatId] = {
             fileId: fileId,
             parsed: parsed,
             tempPath: tempPath,
             responseData: response.data // Store original response data to avoid re-download
         };
         return bot.sendMessage(chatId, `Please specify if this is an 'expense' or an 'earning' for the image you sent. You can reply with "expense" or "earning" in the caption.`);
     }

     // If we reach here, either the type was in the caption or AI identified it as paypal_fee.
     // Proceed with processing the transaction.
     const tempPath = path.join(TMP_DIR, `${uuidv4()}.jpg`); // Still need a temp path for initial processing
     await fsPromises.writeFile(tempPath, Buffer.from(response.data)); // Save temp file now

     try {
         await processImageTransaction(
             chatId,
             fileId,
             parsed,
             transactionType,
             tempPath,
             response.data
         );
     } catch (err) {
         console.error(`Image: Error processing receipt:`, err.message);
         bot.sendMessage(chatId, `ERROR Failed to process receipt: ${err.message}`);
     } finally {
         // This finally block handles the tempPath cleanup for initial processing
         if (fs.existsSync(tempPath)) {
             await fsPromises.unlink(tempPath).catch(err => console.error(`Failed to delete temp file ${tempPath}:`, err.message));
         }
     }
   }
 });
