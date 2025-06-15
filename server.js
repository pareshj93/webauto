// --- server.js (Attractive Header Design) ---
require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const nodemailer = require('nodemailer');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { ToWords } = require('to-words');

const app = express();
const port = process.env.PORT || 3000;

// --- Database Configuration & Initialization ---
const dbConfig = {
    host: process.env.DB_HOST || '193.203.184.87',
    user: process.env.DB_USER || 'u420181319_Pjoshij',
    password: process.env.DB_PASSWORD || 'YOUR_DATABASE_PASSWORD_HERE',
    database: process.env.DB_NAME || 'u420181319_WEBAUTO',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 10000
};

let pool;
async function initializeDatabasePool() {
    try {
        pool = mysql.createPool(dbConfig);
        await pool.query('SELECT 1');
        console.log('>>> initializeDatabasePool: Database connection successful.');
        return true;
    } catch (error) {
        console.error("!!! initializeDatabasePool: DB POOL/CONNECTION ERROR !!!:", error.message);
        if (pool) await pool.end();
        pool = null;
        return false;
    }
}

// --- Middleware & Utilities ---
app.use(cors({
    origin: '*',
    methods: 'GET,POST,PUT,DELETE,OPTIONS',
    allowedHeaders: 'Content-Type,Authorization'
}));
app.use(express.json());
const sendResponse = (res, statusCode, data, message, meta) => res.status(statusCode).json({ success: statusCode < 400, data, message, meta });
app.use((req, res, next) => { if (!pool) return sendResponse(res, 503, null, "Database service unavailable."); next(); });

// --- API Endpoints (CRUD operations for Settings, Parties, Products, Bills) ---
app.get('/api/settings', async (req, res) => { try { const [rows] = await pool.query('SELECT * FROM seller_settings WHERE id = ? LIMIT 1', [1]); sendResponse(res, 200, rows[0] || {}); } catch (e) { console.error(e); sendResponse(res, 500, null, 'Failed to fetch settings.'); } });
app.post('/api/settings', async (req, res) => {
    console.log('--- API HIT: Saving Settings ---');
    const d=req.body; try { const sql=`INSERT INTO seller_settings (id, name, address, gstin, email, companyLogoUrl, bankName, accountNo, ifsc, terms, nextBillNumber, nextBillNumberPrefix, nextProformaNumber, nextProformaNumberPrefix) VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),address=VALUES(address),gstin=VALUES(gstin),email=VALUES(email),companyLogoUrl=VALUES(companyLogoUrl),bankName=VALUES(bankName),accountNo=VALUES(accountNo),ifsc=VALUES(ifsc),terms=VALUES(terms),nextBillNumber=VALUES(nextBillNumber),nextBillNumberPrefix=VALUES(nextBillNumberPrefix),nextProformaNumber=VALUES(nextProformaNumber),nextProformaNumberPrefix=VALUES(nextProformaNumberPrefix)`; await pool.query(sql,[d.name,d.address,d.gstin,d.email,d.companyLogoUrl,d.bankName,d.accountNo,d.ifsc,d.terms,d.nextBillNumber,d.nextBillNumberPrefix, d.nextProformaNumber, d.nextProformaNumberPrefix]); sendResponse(res,200,d,'Settings saved.'); } catch(e){ console.error(e); sendResponse(res,500,null,'Failed to save settings.'); } });

app.get('/api/parties', async (req, res) => { try { const [rows] = await pool.query('SELECT * FROM parties ORDER BY name ASC'); sendResponse(res, 200, rows); } catch (e) { console.error(e); sendResponse(res, 500, null, 'Failed to fetch parties.'); } });
app.post('/api/parties', async (req, res) => {
    console.log('--- API HIT: Creating Party ---', req.body);
    const d=req.body; try { const [e] = await pool.query('SELECT id FROM parties WHERE LOWER(name)=LOWER(?)',[d.name.trim()]); if(e.length>0)return sendResponse(res,409,null,'Party name already exists.'); const [r] = await pool.query('INSERT INTO parties (name,email,address,gstin) VALUES (?,?,?,?)',[d.name.trim(),d.email,d.address,d.gstin]); sendResponse(res, 201, {id:r.insertId,...d},'Party added.'); } catch(e){ console.error(e); sendResponse(res,500,null,'Failed to add party.'); } });
app.put('/api/parties/:id', async (req, res) => {
    console.log(`--- API HIT: Updating Party ${req.params.id} ---`, req.body);
    const d=req.body; const {id}=req.params; try { const [e]=await pool.query('SELECT id FROM parties WHERE LOWER(name)=LOWER(?) AND id!=?',[d.name.trim(),id]); if(e.length > 0)return sendResponse(res,409,null,'Another party with this name already exists.'); const [r]=await pool.query('UPDATE parties SET name=?,email=?,address=?,gstin=? WHERE id=?',[d.name.trim(),d.email,d.address,d.gstin,id]); if(r.affectedRows===0)return sendResponse(res,404,null,'Party not found.'); sendResponse(res,200,{id,...d},'Party updated.'); } catch (e){ console.error(e); sendResponse(res,500,null,'Failed to update party.'); } });
app.delete('/api/parties/:id', async (req, res) => { try { const [r]=await pool.query('DELETE FROM parties WHERE id=?',[req.params.id]); if(r.affectedRows===0)return sendResponse(res,404,null,'Party not found.'); sendResponse(res,204); } catch (e) { console.error(e); sendResponse(res,500,null,'Failed to delete party.'); } });
app.get('/api/products', async(req,res)=>{ const {search, page = 1, limit = 10} = req.query; const pN=parseInt(page), lN=parseInt(limit), off=(pN-1)*lN; let wc=[], qP=[]; if(search){ const sT=`%${search.toLowerCase()}%`; wc.push('(LOWER(description) LIKE ? OR LOWER(hsnSac) LIKE ? OR LOWER(partNo) LIKE ?)'); qP.push(sT, sT, sT); } const wS=wc.length>0?`WHERE ${wc.join(' AND ')}`:''; try{ const [[{total}]] = await pool.query(`SELECT COUNT(*) as total FROM products ${wS}`, qP); const tPg = Math.ceil(total/lN); const [p] = await pool.query(`SELECT * FROM products ${wS} ORDER BY description ASC LIMIT ? OFFSET ?`, [...qP, lN, off]); sendResponse(res, 200, p, null, {currentPage: pN, totalPages: tPg, totalItems: total}); } catch(e) { console.error(e); sendResponse(res, 500, null, 'Failed to fetch products.'); } });
app.post('/api/products',async(req,res)=>{
    console.log('--- API HIT: Creating Product ---', req.body);
    const d=req.body;
    try {
        const description = d.description.trim();
        const partNo = d.partNo ? d.partNo.trim() : null;
        const [descExists] = await pool.query('SELECT id FROM products WHERE LOWER(description)=LOWER(?)', [description]);
        if (descExists.length > 0) return sendResponse(res, 409, null, 'A product with this description already exists.');
        if (partNo) {
            const [partNoExists] = await pool.query('SELECT id FROM products WHERE partNo = ?', [partNo]);
            if (partNoExists.length > 0) return sendResponse(res, 409, null, 'A product with this Part No. already exists.');
        }
        const[r]=await pool.query('INSERT INTO products (description,hsnSac,unitPrice,gstRate,partNo) VALUES (?,?,?,?,?)',[description, d.hsnSac, d.unitPrice, d.gstRate, partNo]);
        sendResponse(res,201,{id:r.insertId,...d},'Product added.');
    } catch(e) { console.error(e); sendResponse(res,500,null,'Failed to add product.'); }
});
app.put('/api/products/:id',async(req,res)=>{
    console.log(`--- API HIT: Updating Product ${req.params.id} ---`, req.body);
    const d=req.body; const{id}=req.params;
    try {
        const description = d.description.trim();
        const partNo = d.partNo ? d.partNo.trim() : null;
        const [descExists]=await pool.query('SELECT id FROM products WHERE LOWER(description)=LOWER(?) AND id!=?',[description, id]);
        if(descExists.length > 0) return sendResponse(res,409,null,'Another product with this description already exists.');
        if(partNo) {
            const [partNoExists]=await pool.query('SELECT id FROM products WHERE partNo=? AND id!=?',[partNo, id]);
            if(partNoExists.length > 0) return sendResponse(res,409,null,'Another product with this Part No. already exists.');
        }
        const[r]=await pool.query('UPDATE products SET description=?,hsnSac=?,unitPrice=?,gstRate=?,partNo=? WHERE id=?',[description, d.hsnSac, d.unitPrice, d.gstRate, partNo, id]);
        if(r.affectedRows===0) return sendResponse(res,404,null,'Product not found.');
        sendResponse(res,200,{id,...d},'Product updated.');
    } catch(e) { console.error(e); sendResponse(res,500,null,'Failed to update product.'); }
});
app.delete('/api/products/:id',async(req,res)=>{try{const[r]=await pool.query('DELETE FROM parties WHERE id=?',[req.params.id]);if(r.affectedRows===0)return sendResponse(res,404,null,'Party not found.');sendResponse(res,204);}catch(e){console.error(e);sendResponse(res,500,null,'Failed to delete party.');}});

async function upsertProductsFromBillItems(connection, items) {
    for (const item of items) {
        if (item.description && item.description.trim() !== '') {
            const trimmedDesc = item.description.trim();
            const [existing] = await connection.query('SELECT id FROM products WHERE LOWER(description) = ?', [trimmedDesc.toLowerCase()]);
            if (existing.length > 0) {
                await connection.query('UPDATE products SET hsnSac=?, unitPrice=?, gstRate=?, partNo=? WHERE id=?', [item.hsnSac, item.unitPrice, item.gstRate, item.partNo || null, existing[0].id]);
            } else {
                await connection.query('INSERT INTO products (description, hsnSac, unitPrice, gstRate, partNo) VALUES (?, ?, ?, ?, ?)', [trimmedDesc, item.hsnSac, item.unitPrice, item.gstRate, item.partNo || null]);
            }
        }
    }
}

app.get('/api/bills', async (req, res) => {
    try {
        const query = `
            SELECT id, billNumber, date, partyDetails, grandTotal, vehicleModelNo, invoice_type FROM bills
            UNION ALL
            SELECT id, billNumber, date, partyDetails, grandTotal, vehicleModelNo, invoice_type FROM proforma_invoices
            ORDER BY date DESC, id DESC
        `;
        const [rows] = await pool.query(query);
        const bills = rows.map(item => ({...item, partyDetails: item.partyDetails ? JSON.parse(item.partyDetails) : {} }));
        sendResponse(res, 200, bills);
    } catch (e) {
        console.error(e);
        sendResponse(res, 500, null, 'Failed to fetch bills.');
    }
});

app.get('/api/bills/:id', async (req, res) => {
    try {
        const { type } = req.query;
        const billId = req.params.id;
        if (!type) return sendResponse(res, 400, null, 'Invoice type query parameter is required.');
        const isProforma = type === 'PROFORMA_INVOICE';
        const tableName = isProforma ? 'proforma_invoices' : 'bills';
        const itemsTableName = isProforma ? 'proforma_invoice_items' : 'bill_items';
        
        const [[bill]] = await pool.query(`SELECT * FROM ${tableName} WHERE id = ?`, [billId]);
        if (!bill) return sendResponse(res, 404, null, 'Bill not found.');
        
        const [items] = await pool.query(`SELECT * FROM ${itemsTableName} WHERE bill_id = ?`, [billId]);
        bill.items = items;
        bill.sellerDetails = JSON.parse(bill.sellerDetails);
        bill.partyDetails = JSON.parse(bill.partyDetails);
        
        sendResponse(res, 200, bill);
    } catch (e) {
        console.error(e);
        sendResponse(res, 500, null, 'Failed to fetch bill details.');
    }
});

app.post('/api/bills', async (req, res) => {
    const d = req.body;
    const isProforma = d.invoice_type === 'PROFORMA_INVOICE';
    console.log(`--- API HIT: Creating ${d.invoice_type || 'TAX_INVOICE'} ---`);
    let c;
    try {
        c = await pool.getConnection();
        await c.beginTransaction();

        const mainTable = isProforma ? 'proforma_invoices' : 'bills';
        const itemsTable = isProforma ? 'proforma_invoice_items' : 'bill_items';

        const s = `INSERT INTO ${mainTable}(billNumber,date,reference,vehicleModelNo,sellerDetails,partyDetails,subTotal,totalCGST,totalSGST,roundOff,grandTotal,amountInWords,invoice_type)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`;
        const [r] = await c.query(s, [d.billNumber, d.date, d.reference, d.vehicleModelNo, JSON.stringify(d.sellerDetails), JSON.stringify(d.partyDetails), d.subTotal, d.totalCGST, d.totalSGST, d.roundOff, d.grandTotal, d.amountInWords, d.invoice_type]);
        const nId = r.insertId;

        if (d.items.length > 0) {
            const iS = `INSERT INTO ${itemsTable}(bill_id,description,hsnSac,quantity,unitPrice,gstRate,taxableValue,cgstAmount,sgstAmount,totalAmount,removeRefitting,dentingAcGas,painting,partNo)VALUES ?`;
            const iV = d.items.map(i => [nId, i.description, i.hsnSac, i.quantity, i.unitPrice, i.gstRate, i.taxableValue, i.cgstAmount, i.sgstAmount, i.totalAmount, i.removeRefitting, i.dentingAcGas, i.painting, i.partNo || null]);
            await c.query(iS, [iV]);
        }
        
        if (!isProforma) {
            await upsertProductsFromBillItems(c, d.items);
        }

        await c.commit();
        sendResponse(res, 201, { id: nId, ...d }, `${isProforma ? 'Proforma' : 'Bill'} created.`);
    } catch (e) {
        if (c) await c.rollback();
        console.error(e);
        sendResponse(res, 500, null, `Failed to create invoice: ${e.message}`);
    } finally {
        if (c) c.release();
    }
});

app.put('/api/bills/:id', async (req, res) => {
    const bId = req.params.id;
    const d = req.body;
    const isProforma = d.invoice_type === 'PROFORMA_INVOICE';
    console.log(`--- API HIT: Updating ${d.invoice_type || 'TAX_INVOICE'} ${bId} ---`);
    let c;
    try {
        c = await pool.getConnection();
        await c.beginTransaction();

        const mainTable = isProforma ? 'proforma_invoices' : 'bills';
        const itemsTable = isProforma ? 'proforma_invoice_items' : 'bill_items';

        const u = `UPDATE ${mainTable} SET billNumber=?,date=?,reference=?,vehicleModelNo=?,sellerDetails=?,partyDetails=?,subTotal=?,totalCGST=?,totalSGST=?,roundOff=?,grandTotal=?,amountInWords=?,invoice_type=? WHERE id=?`;
        await c.query(u, [d.billNumber, d.date, d.reference, d.vehicleModelNo, JSON.stringify(d.sellerDetails), JSON.stringify(d.partyDetails), d.subTotal, d.totalCGST, d.totalSGST, d.roundOff, d.grandTotal, d.amountInWords, d.invoice_type, bId]);
        
        await c.query(`DELETE FROM ${itemsTable} WHERE bill_id=?`, [bId]);
        
        if (d.items.length > 0) {
            const iS = `INSERT INTO ${itemsTable}(bill_id,description,hsnSac,quantity,unitPrice,gstRate,taxableValue,cgstAmount,sgstAmount,totalAmount,removeRefitting,dentingAcGas,painting,partNo)VALUES ?`;
            const iV = d.items.map(i => [bId, i.description, i.hsnSac, i.quantity, i.unitPrice, i.gstRate, i.taxableValue, i.cgstAmount, i.sgstAmount, i.totalAmount, i.removeRefitting, i.dentingAcGas, i.painting, i.partNo || null]);
            await c.query(iS, [iV]);
        }
        
        if (!isProforma) {
            await upsertProductsFromBillItems(c, d.items);
        }
        
        await c.commit();
        sendResponse(res, 200, { id: bId, ...d }, `${isProforma ? 'Proforma' : 'Bill'} updated.`);
    } catch (e) {
        if (c) await c.rollback();
        console.error(e);
        sendResponse(res, 500, null, `Failed to update invoice: ${e.message}`);
    } finally {
        if (c) c.release();
    }
});

app.delete('/api/bills/:id', async (req, res) => {
    const { type } = req.query;
    const billId = req.params.id;
    if (!type) return sendResponse(res, 400, null, 'Invoice type query parameter is required.');
    
    const isProforma = type === 'PROFORMA_INVOICE';
    const mainTable = isProforma ? 'proforma_invoices' : 'bills';
    const itemsTable = isProforma ? 'proforma_invoice_items' : 'bill_items';

    let c;
    try {
        c = await pool.getConnection();
        await c.beginTransaction();
        await c.query(`DELETE FROM ${itemsTable} WHERE bill_id=?`, [billId]);
        const [r] = await c.query(`DELETE FROM ${mainTable} WHERE id=?`, [billId]);
        if (r.affectedRows === 0) return sendResponse(res, 404, null, 'Bill not found.');
        await c.commit();
        sendResponse(res, 204);
    } catch (e) {
        if (c) await c.rollback();
        console.error(e);
        sendResponse(res, 500, null, `Failed to delete invoice: ${e.message}`);
    } finally {
        if (c) c.release();
    }
});

const PDF_SETTINGS = {
    MARGIN: { TOP: 50, BOTTOM: 25, LEFT: 35, RIGHT: 35 },
    FONT: { NORMAL: 'Helvetica', BOLD: 'Helvetica-Bold' },
    COLOR: { 
        HEADER_BG: '#FFFFFF',
        LINE_COLOR: '#E0E0E0',
        TEXT_LIGHT: '#555555',
        ACCENT_BLUE: '#3498DB',
        HEADER_TEXT: '#000000', 
        INVOICE_TITLE: '#000000', 
        SECTION_TITLE: '#34495E', 
        TABLE_HEADER_BG: '#4A90E2', 
        TABLE_HEADER_TEXT: '#FFFFFF', 
        ROW_ALT_BG: '#F0F6FF', 
        TEXT_DARK: '#212529', 
        TEXT_MEDIUM: '#495057', 
        BORDER_DARK: '#000000', 
        BORDER_LIGHT: '#CCCCCC', 
        ACCENT: '#4A90E2', 
        GRAND_TOTAL_BG: '#EAF2F8' 
    },
    LOGO: { MAX_WIDTH: 180, MAX_HEIGHT: 100 } 
};

async function generateBillPdfBuffer(billDetails, logoBuffer) {
    return new Promise((resolve, reject) => {
        const doc = new PDFDocument({ size: 'A4', margins: PDF_SETTINGS.MARGIN, bufferPages: true });
        const buffers = [];
        doc.on('data', chunk => buffers.push(chunk));
        doc.on('end', () => resolve(Buffer.concat(buffers)));
        doc.on('error', reject);

        try {
            const context = { y: doc.page.margins.top };
            const itemCount = (billDetails.items || []).length;

            doc.on('pageAdded', () => { 
                context.y = doc.page.margins.top;
                doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(9).fillColor(PDF_SETTINGS.COLOR.TEXT_MEDIUM)
                    .text(`Invoice Continued: ${billDetails.billNumber}`, doc.page.margins.left, doc.page.margins.top - 20, { 
                        width: doc.page.width - doc.page.margins.left - doc.page.margins.right, 
                        align: 'center' 
                    });
            });
            
            drawHeader(doc, context, billDetails, logoBuffer, true);
            drawBillPartyAndMetaInfo(doc, context, billDetails);
            
            const title = (billDetails.invoice_type === 'PROFORMA_INVOICE') ? 'PROFORMA INVOICE' : 'TAX INVOICE';
            doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(14).fillColor(PDF_SETTINGS.COLOR.INVOICE_TITLE)
               .text(title, PDF_SETTINGS.MARGIN.LEFT, context.y, { 
                   width: doc.page.width - PDF_SETTINGS.MARGIN.LEFT - PDF_SETTINGS.MARGIN.RIGHT, 
                   align: 'center' 
               });
            context.y = doc.y + 15;
            
            let layoutOptions = { fontSize: 9, rowPadding: 5 };
            if (itemCount <= 10) {
                 layoutOptions = { fontSize: 7.8, rowPadding: 3 };
            }

            const itemTableConfig = getItemTableConfig(doc, billDetails, layoutOptions);
            drawItemTable(doc, context, itemTableConfig, layoutOptions);

            const hsnConfig = getHsnSummaryConfig(doc, billDetails, itemTableConfig.tableWidth, layoutOptions);
            
            if (itemCount <= 10) {
                drawTotalsSection(doc, context, billDetails, layoutOptions);
                if (hsnConfig) {
                    drawHsnSummary(doc, context, hsnConfig, layoutOptions);
                }
            } else if (itemCount <= 14) {
                drawTotalsSection(doc, context, billDetails, layoutOptions);
                if (hsnConfig) {
                    doc.addPage();
                    drawHsnSummary(doc, context, hsnConfig, layoutOptions);
                }
            } else {
                const totalsHeight = 140; 
                const hsnHeight = hsnConfig ? getTableHeight(doc, hsnConfig, layoutOptions) : 0;
                
                if (checkAndHandlePageBreak(doc, context, totalsHeight + hsnHeight)) {
                    drawTotalsSection(doc, context, billDetails, layoutOptions);
                     if (hsnConfig) {
                        checkAndHandlePageBreak(doc, context, hsnHeight);
                        drawHsnSummary(doc, context, hsnConfig, layoutOptions);
                     }
                } else {
                    drawTotalsSection(doc, context, billDetails, layoutOptions);
                    if (hsnConfig) {
                        drawHsnSummary(doc, context, hsnConfig, layoutOptions);
                    }
                }
            }
            
            finalizePages(doc, billDetails);
            doc.end();
        } catch (e) { console.error("[PDF Generation Error]", e.stack); reject(e); }
    });
}

function drawHeader(doc, context, billDetails, logoBuffer, isFirstPage) {
    const { sellerDetails } = billDetails;
    const { MARGIN, FONT, COLOR, LOGO } = PDF_SETTINGS;

    if (isFirstPage) {
        const headerStartY = MARGIN.TOP - 20; 
        
        let logoHeight = 0;
        let finalRightY = headerStartY;

        if (logoBuffer) {
            try {
                const logoDims = doc.image(logoBuffer, MARGIN.LEFT, headerStartY, {
                    fit: [LOGO.MAX_WIDTH, 80],
                    align: 'left',
                    valign: 'top'
                });
                logoHeight = logoDims.height;
            } catch (imgErr) {
                console.error("Error embedding logo in PDF:", imgErr.message);
            }
        }

        const companyDetailsX = MARGIN.LEFT + LOGO.MAX_WIDTH + 25;
        const textBlockMaxWidth = doc.page.width - companyDetailsX - MARGIN.RIGHT;

        if (textBlockMaxWidth > 0) {
            doc.font(FONT.BOLD).fontSize(18).fillColor(COLOR.TEXT_DARK)
                .text(sellerDetails.name || "", companyDetailsX, headerStartY, { width: textBlockMaxWidth });

            doc.moveDown(0.5);

            doc.font(FONT.NORMAL).fontSize(9).fillColor(COLOR.TEXT_LIGHT)
                .text(sellerDetails.address || "", { width: textBlockMaxWidth });
            
            if (sellerDetails.gstin) {
                 doc.moveDown(0.5);
                doc.text(`GSTIN/UIN: ${sellerDetails.gstin}`, { width: textBlockMaxWidth });
            }
            finalRightY = doc.y;
        }

        context.y = Math.max(headerStartY + logoHeight, finalRightY) + 30;

    } else { 
        doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(9).fillColor(COLOR.TEXT_MEDIUM)
            .text(`Invoice Continued: ${billDetails.billNumber}`, MARGIN.LEFT, MARGIN.TOP - 20, {
                width: doc.page.width - MARGIN.LEFT - MARGIN.RIGHT,
                align: 'center'
            });
        context.y = MARGIN.TOP;
    }
}

function drawBillPartyAndMetaInfo(doc, context, billDetails) {
    if (isNaN(context.y)) {
        console.error("!!! RECOVERED from NaN context.y in drawBillPartyAndMetaInfo. Defaulting Y to 150. !!!");
        context.y = 150; 
    }

    const { partyDetails } = billDetails;
    const { MARGIN, FONT, COLOR } = PDF_SETTINGS;
    const sectionStartY = context.y;
    const rightColumnX = doc.page.width / 2 + 10;
    const lineGap = 2;
    const sectionPadding = 10;

    let leftX = MARGIN.LEFT;
    let leftY = sectionStartY;

    doc.font(FONT.BOLD).fontSize(10).fillColor(COLOR.ACCENT_BLUE)
        .text('BUYER (BILL TO)', leftX, leftY, { characterSpacing: 1 });
    leftY = doc.y + 8;
    
    doc.font(FONT.BOLD).fontSize(10).fillColor(COLOR.TEXT_DARK)
    if (partyDetails.name) {
        doc.text(partyDetails.name, leftX, leftY);
        leftY = doc.y + lineGap;
    }
     doc.font(FONT.NORMAL).fontSize(9).fillColor(COLOR.TEXT_MEDIUM)
    if (partyDetails.address) {
        doc.text(partyDetails.address, leftX, leftY, { width: doc.page.width / 2 - MARGIN.LEFT - sectionPadding });
        leftY = doc.y + lineGap;
    }
    if (partyDetails.gstin) {
        doc.text(`GSTIN/UIN: ${partyDetails.gstin}`, leftX, leftY, { width: doc.page.width / 2 - MARGIN.LEFT - sectionPadding });
        leftY = doc.y + lineGap;
    }
    doc.text(`State Name: Gujarat, Code: 24`, leftX, leftY, { width: doc.page.width / 2 - MARGIN.LEFT - sectionPadding });
    const leftColumnBottom = doc.y;

    let rightY = sectionStartY;
    const labelWidth = 80;
    const valueWidth = 120;
    const metaData = [
        { label: 'Invoice No.', value: billDetails.billNumber },
        { label: 'Dated', value: billDetails.date ? new Date(billDetails.date).toLocaleDateString('en-GB') : '' },
        { label: 'Vehicle No.', value: billDetails.reference },
        { label: 'Model No.', value: billDetails.vehicleModelNo },
    ];
    
    metaData.forEach(row => {
        if (row.value) {
            doc.font(FONT.BOLD).fontSize(9).fillColor(COLOR.TEXT_DARK)
               .text(row.label, rightColumnX, rightY, { width: labelWidth, align: 'left' });
            doc.font(FONT.NORMAL).fontSize(9).fillColor(COLOR.TEXT_MEDIUM)
               .text(`: ${row.value}`, rightColumnX + labelWidth, rightY, { width: valueWidth, align: 'left' });
            rightY += 15;
        }
    });
    const rightColumnBottom = rightY;

    context.y = Math.max(leftColumnBottom, rightColumnBottom) + 25;
}


// <<< FIX: ALWAYS USE CURRENT SETTINGS FOR PDF GENERATION >>>
app.get('/api/bills/:id/download-pdf', async (req, res) => {
    try {
        const { type } = req.query;
        const billId = req.params.id;
        if (!type) return sendResponse(res, 400, null, 'Invoice type query parameter is required.');

        console.log(`--- API HIT: PDF Download for ${type} ID: ${billId} ---`);
        const isProforma = type === 'PROFORMA_INVOICE';
        const tableName = isProforma ? 'proforma_invoices' : 'bills';
        const itemsTableName = isProforma ? 'proforma_invoice_items' : 'bill_items';

        const [billRows] = await pool.query(`SELECT * FROM ${tableName} WHERE id = ?`, [billId]);
        if (billRows.length === 0) return sendResponse(res, 404, null, 'Bill not found.');
        
        let billData = billRows[0];
        
        // --- FETCH LATEST SETTINGS ---
        const [settingsRows] = await pool.query('SELECT * FROM seller_settings WHERE id = 1 LIMIT 1');
        const latestSettings = settingsRows[0] || {};
        
        // --- OVERWRITE SAVED DETAILS WITH LATEST SETTINGS ---
        billData.sellerDetails = latestSettings;
        billData.partyDetails = JSON.parse(billData.partyDetails || '{}');
        const [itemRows] = await pool.query(`SELECT * FROM ${itemsTableName} WHERE bill_id = ?`, [billId]);
        billData.items = itemRows || [];
        
        const logoUrl = billData.sellerDetails.companyLogoUrl || "https://jetsetbranding.com/Webauto.jpg";
        const logoBuffer = await getLogoBuffer(logoUrl);

        const pdfBuffer = await generateBillPdfBuffer(billData, logoBuffer);

        res.setHeader('Content-Type', 'application/pdf');
        const sanitizedBillNumber = String(billData.billNumber || billId).replace(/[^a-z0-9_.-]/gi, '_');
        res.setHeader('Content-Disposition', `attachment; filename="${type}-${sanitizedBillNumber}.pdf"`);
        res.send(pdfBuffer);
    } catch (error) {
        console.error(`--- ERROR generating PDF for download for bill ${req.params.id} ---:`, error.stack);
        res.status(500).type('text/plain').send(`Failed to generate PDF. Error: ${error.message}`);
    }
});

// <<< FIX: ALWAYS USE CURRENT SETTINGS FOR EMAIL >>>
app.post('/api/bills/:id/send-email', async (req, res) => {
    try {
        const { to, cc, subject, type } = req.body;
        const billId = req.params.id;
        console.log(`--- API HIT: Sending Email for ${type} ID: ${billId} to ${to} ---`);

        if (!to) return sendResponse(res, 400, null, 'Recipient email is required.');
        if (!type) return sendResponse(res, 400, null, 'Invoice type is required.');

        const isProforma = type === 'PROFORMA_INVOICE';
        const tableName = isProforma ? 'proforma_invoices' : 'bills';
        const itemsTableName = isProforma ? 'proforma_invoice_items' : 'bill_items';

        const [billRows] = await pool.query(`SELECT * FROM ${tableName} WHERE id = ?`, [billId]);
        if (billRows.length === 0) return sendResponse(res, 404, null, 'Bill not found.');

        let billData = billRows[0];

        // --- FETCH LATEST SETTINGS ---
        const [settingsRows] = await pool.query('SELECT * FROM seller_settings WHERE id = 1 LIMIT 1');
        const latestSettings = settingsRows[0] || {};

        // --- OVERWRITE SAVED DETAILS WITH LATEST SETTINGS ---
        billData.sellerDetails = latestSettings;
        billData.partyDetails = JSON.parse(billData.partyDetails || '{}');
        const [itemRows] = await pool.query(`SELECT * FROM ${itemsTableName} WHERE bill_id = ?`, [req.params.id]);
        billData.items = itemRows || [];

        const logoUrl = billData.sellerDetails.companyLogoUrl || "https://jetsetbranding.com/Webauto.jpg";
        const logoBuffer = await getLogoBuffer(logoUrl);
        const pdfBuffer = await generateBillPdfBuffer(billData, logoBuffer);

        const transporter = nodemailer.createTransport({
            host: process.env.SMTP_HOST,
            port: process.env.SMTP_PORT,
            secure: process.env.SMTP_SECURE === 'true',
            auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS, },
        });

        const mailOptions = {
            from: `"${process.env.SMTP_FROM_NAME}" <${process.env.SMTP_FROM_EMAIL}>`,
            to: to, cc: cc,
            subject: subject || `${isProforma ? 'Proforma Invoice' : 'Invoice'} from ${billData.sellerDetails.name}`,
            html: `Please find your ${isProforma ? 'proforma invoice' : 'invoice'} attached.`,
            attachments: [{
                filename: `${type}-${billData.billNumber}.pdf`,
                content: pdfBuffer,
                contentType: 'application/pdf',
            }],
        };

        await transporter.sendMail(mailOptions);
        sendResponse(res, 200, null, 'Email sent successfully.');

    } catch (error) {
        console.error(`--- ERROR sending email for bill ${req.params.id} ---:`, error.stack);
        res.status(500).type('text/plain').send(`Failed to send email. Error: ${error.message}`);
    }
});


function drawPageBorder(doc) { const {left, top, right, bottom} = doc.page.margins; doc.rect(left, top, doc.page.width - left - right, doc.page.height - top - bottom).stroke(PDF_SETTINGS.COLOR.BORDER_DARK); }
function drawPageNumber(doc, currentPage, totalPages) { doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(8).fillColor("#777").text(`Page ${currentPage} of ${totalPages}`, PDF_SETTINGS.MARGIN.LEFT, doc.page.height - PDF_SETTINGS.MARGIN.BOTTOM + 10, { align: 'center' }); }
function drawFooterContent(doc, billDetails) {
    const { sellerDetails } = billDetails;
    const { MARGIN, FONT, COLOR } = PDF_SETTINGS;
    const pageBottom = doc.page.height - MARGIN.BOTTOM;
    const footerStartY = pageBottom - 100;
    const signatureX = doc.page.width - MARGIN.RIGHT - 180;
    doc.font(FONT.BOLD).fontSize(9).fillColor(COLOR.TEXT_DARK).text(`For ${sellerDetails.name || ''}`, signatureX, footerStartY + 45, { width: 180, align: 'center' });
    doc.font(FONT.NORMAL).fontSize(8).fillColor(COLOR.TEXT_MEDIUM).text('Authorised Signatory', signatureX, footerStartY + 75, { width: 180, align: 'center' });
    const leftColumnX = MARGIN.LEFT;
    const leftColumnWidth = doc.page.width - MARGIN.LEFT - MARGIN.RIGHT - 200;
    const { bankName, accountNo, ifsc } = sellerDetails;
    if (bankName || accountNo || ifsc) {
        doc.font(FONT.BOLD).fontSize(9).fillColor(COLOR.TEXT_DARK).text('Bank Details', leftColumnX, footerStartY, { width: leftColumnWidth });
        doc.font(FONT.NORMAL).fontSize(8).fillColor(COLOR.TEXT_MEDIUM);
        if (bankName) doc.text(`Bank Name: ${bankName}`, { width: leftColumnWidth });
        if (accountNo) doc.text(`A/c No.: ${accountNo}`, { width: leftColumnWidth });
        if (ifsc) doc.text(`IFSC Code: ${ifsc}`, { width: leftColumnWidth });
        doc.moveDown(1);
    }
    doc.font(FONT.BOLD).fontSize(9).fillColor(COLOR.TEXT_DARK).text('Terms & Conditions', leftColumnX, doc.y, { width: leftColumnWidth });
    doc.font(FONT.NORMAL).fontSize(8).fillColor(COLOR.TEXT_MEDIUM).text('1. Goods once sold will not be taken back. 2. Interest @18% p.a. will be charged on overdue bills. 3. Subject to local jurisdiction.', leftColumnX, doc.y, { width: leftColumnWidth });
}
function finalizePages(doc, billDetails) {
    const totalPages = doc.bufferedPageRange().count;
    for (let i = 0; i < totalPages; i++) {
        doc.switchToPage(i);
        drawPageBorder(doc);
        if (i === totalPages - 1) drawFooterContent(doc, billDetails);
        drawPageNumber(doc, i + 1, totalPages);
    }
}
function checkAndHandlePageBreak(doc, context, neededHeight) {
    const pageBottom = doc.page.height - doc.page.margins.bottom;
    const footerHeight = 150; 
    if (context.y + neededHeight > pageBottom - footerHeight) {
        doc.addPage();
        return true;
    }
    return false;
}
function drawItemTable(doc, context, config, options) {
    const { headers, rows, colWidths, tableWidth } = config;
    const { fontSize, rowPadding } = options;
    const headerHeight = 25;

    const drawTableHeader = () => {
        doc.rect(PDF_SETTINGS.MARGIN.LEFT, context.y, tableWidth, headerHeight).fill(PDF_SETTINGS.COLOR.TABLE_HEADER_BG);
        let currentX = PDF_SETTINGS.MARGIN.LEFT;
        doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TABLE_HEADER_TEXT);
        headers.forEach((header, i) => {
            doc.text(header.text, currentX + 5, context.y + 8, { width: colWidths[i] - 10, align: header.align || 'left' });
            if (i < headers.length - 1) doc.moveTo(currentX + colWidths[i], context.y).lineTo(currentX + colWidths[i], context.y + headerHeight).strokeColor('#FFFFFF').lineWidth(0.5).stroke();
            currentX += colWidths[i];
        });
        context.y += headerHeight;
    };

    drawTableHeader();

    rows.forEach((rowData, index) => {
        const rowHeight = Math.max(18, doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize).heightOfString(String(rowData.values[0]), { width: colWidths[0] - 10}) + (rowPadding * 2));
        if (checkAndHandlePageBreak(doc, context, rowHeight)) drawTableHeader();
        const rowY = context.y;
        if (index % 2) doc.rect(PDF_SETTINGS.MARGIN.LEFT, rowY, tableWidth, rowHeight).fill(PDF_SETTINGS.COLOR.ROW_ALT_BG);
        let currentX = PDF_SETTINGS.MARGIN.LEFT;
        rowData.values.forEach((cell, i) => {
            const currentFontSize = headers[i].key === 'partNo' ? fontSize - 1 : fontSize;
            doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(currentFontSize).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
            doc.text(String(cell || ''), currentX + rowPadding, rowY + rowPadding, { width: colWidths[i] - (rowPadding * 2), align: headers[i].align || 'left' });
            currentX += colWidths[i];
        });
        doc.moveTo(PDF_SETTINGS.MARGIN.LEFT, context.y + rowHeight).lineTo(PDF_SETTINGS.MARGIN.LEFT + tableWidth, context.y + rowHeight).strokeColor(PDF_SETTINGS.COLOR.BORDER_LIGHT).lineWidth(0.5).stroke();
        context.y += rowHeight;
    });
    context.y += 10;
}
function getTableHeight(doc, config, options) {
    if (!config) return 0;
    let height = 0;
    const { fontSize, rowPadding } = options;

    if (config.title) height += 35; 
    height += config.isComplexHeader ? 35 : 25; 
    
    doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize);
    config.rows.forEach(row => {
        const cellText = String(row.values[0] || '');
        const textHeight = doc.heightOfString(cellText, { width: config.colWidths[0] - 10 });
        height += Math.max(18, textHeight + (rowPadding * 2));
    });

    if (config.footer) height += 25;
    return height;
}
function getItemTableConfig(doc, billDetails, options) {
    const { fontSize } = options;
    doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize);
    const headers = [
        { text: "Description", widthRatio: 0.32, align: 'left', key: 'description' },
        { text: "Part No.", widthRatio: 0.18, align: 'center', key: 'partNo' },
        { text: "HSN/SAC", widthRatio: 0.10, align: 'center', key: 'hsn' },
        { text: "Qty", widthRatio: 0.08, align: 'center', key: 'qty' },
        { text: "Rate", widthRatio: 0.12, align: 'center', key: 'rate' },
        { text: "GST", widthRatio: 0.08, align: 'center', key: 'gst' },
        { text: "Amount", widthRatio: 0.12, align: 'center', key: 'amount' }
    ];
    
    const tableWidth = doc.page.width - PDF_SETTINGS.MARGIN.LEFT - PDF_SETTINGS.MARGIN.RIGHT;
    const colWidths = headers.map(h => h.widthRatio * tableWidth);
    
    const rows = billDetails.items.map((item, index) => ({
        values: [
            `${index + 1}. ${item.description || ''}`,
            item.partNo || '-',
            item.hsnSac || '-',
            (item.itemType === 'labour') ? '' : (item.quantity || '0'),
            Number(item.unitPrice || 0).toFixed(2),
            item.gstRate ? `${item.gstRate}%` : '0%',
            Number(item.taxableValue || 0).toFixed(2)
        ]
    }));
    
    return { headers, rows, colWidths, tableWidth };
}

function drawTotalsSection(doc, context, billDetails, options) {
    const { fontSize } = options;
    const subTotal = parseFloat(billDetails.subTotal || 0);
    const totalCGST = parseFloat(billDetails.totalCGST || 0);
    const totalSGST = parseFloat(billDetails.totalSGST || 0);
    const roundOff = parseFloat(billDetails.roundOff || 0);
    const grandTotal = parseFloat(billDetails.grandTotal || 0);

    const totalsY = context.y;
    const labelWidth = 100;
    const valueWidth = 80;
    const valueX = doc.page.width - PDF_SETTINGS.MARGIN.RIGHT - valueWidth;
    const labelX = valueX - labelWidth;
    
    const amountInWordsY = totalsY + 5;
    doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
    doc.text('Amount Chargeable (in words)', PDF_SETTINGS.MARGIN.LEFT, amountInWordsY);
    
    doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TEXT_MEDIUM);
    doc.text(`${billDetails.amountInWords || ''} Only`, PDF_SETTINGS.MARGIN.LEFT, amountInWordsY + (fontSize + 4), { width: labelX - PDF_SETTINGS.MARGIN.LEFT - 20});
    
    let totalsTableY = totalsY;
    doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
    doc.text('Description', labelX, totalsTableY);
    doc.text('Amount', valueX, totalsTableY, { width: valueWidth, align: 'right' });
    totalsTableY += 15;

    const addTaxRow = (label, value) => {
        doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
        doc.text(label, labelX, totalsTableY, { width: labelWidth, align: 'left' });
        doc.text(Number(value || 0).toFixed(2), valueX, totalsTableY, { width: valueWidth, align: 'right' });
        totalsTableY += (fontSize + 3);
    };

    addTaxRow('Sub Total:', subTotal);
    if (totalCGST > 0) addTaxRow('CGST:', totalCGST);
    if (totalSGST > 0) addTaxRow('SGST:', totalSGST);

    if (Math.abs(roundOff) > 0.001) {
        addTaxRow('Round Off:', roundOff);
    }
    
    totalsTableY += 5;
    doc.rect(labelX - 10, totalsTableY, labelWidth + valueWidth + 20, 25).fill(PDF_SETTINGS.COLOR.GRAND_TOTAL_BG);
    doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize + 2).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
    doc.text('GRAND TOTAL', labelX, totalsTableY + 7, { width: labelWidth, align: 'left' });
    doc.text(Number(grandTotal || 0).toFixed(2), valueX, totalsTableY + 7, { width: valueWidth, align: 'right' });
    
    context.y = totalsTableY + 30;
}

function getHsnSummaryConfig(doc, billDetails, tableWidth, options) {
    const { items } = billDetails;
    if (!items || items.length === 0) return null;

    const gstRateSummary = {}; 

    items.forEach(item => {
        const gstRate = item.gstRate || 0;
        const hsnCode = item.hsnSac ? String(item.hsnSac) : null;

        const groupKey = (hsnCode === '998729') ? `hsn_${hsnCode}` : `rate_${gstRate}`;

        if (!gstRateSummary[groupKey]) {
            gstRateSummary[groupKey] = {
                taxableValue: 0,
                cgstAmount: 0,
                sgstAmount: 0,
                hsnCodes: new Set(),
                gstRate: gstRate 
            };
        }

        gstRateSummary[groupKey].taxableValue += Number(item.taxableValue || 0);
        gstRateSummary[groupKey].cgstAmount += Number(item.cgstAmount || 0);
        gstRateSummary[groupKey].sgstAmount += Number(item.sgstAmount || 0);
        if (hsnCode) {
            gstRateSummary[groupKey].hsnCodes.add(hsnCode);
        }
    });
    
    const colWidths = [0.18, 0.20, 0.20, 0.20, 0.22].map(w => w * tableWidth);

    const rows = Object.values(gstRateSummary).map(data => {
        const hsnString = Array.from(data.hsnCodes).join('/') || 'N/A';
        const gstRate = data.gstRate;
        const cgstRate = (Number(gstRate) / 2).toFixed(1);
        const sgstRate = (Number(gstRate) / 2).toFixed(1);
        
        return {
            values: [
                hsnString,
                data.taxableValue.toFixed(2),
                { rate: `${cgstRate}%`, amount: data.cgstAmount.toFixed(2) },
                { rate: `${sgstRate}%`, amount: data.sgstAmount.toFixed(2) },
                (data.cgstAmount + data.sgstAmount).toFixed(2)
            ]
        };
    });

    const hsnTotals = Object.values(gstRateSummary).reduce((acc, data) => {
        acc.taxableValue += data.taxableValue;
        acc.cgstAmount += data.cgstAmount;
        acc.sgstAmount += data.sgstAmount;
        acc.totalTax += (data.cgstAmount + data.sgstAmount);
        return acc;
    }, { taxableValue: 0, cgstAmount: 0, sgstAmount: 0, totalTax: 0 });

    const footer = [
        "TOTAL",
        hsnTotals.taxableValue.toFixed(2),
        hsnTotals.cgstAmount.toFixed(2),
        hsnTotals.sgstAmount.toFixed(2),
        hsnTotals.totalTax.toFixed(2)
    ];

    return { rows, colWidths, footer, title: "HSN/SAC Summary", isComplexHeader: true, tableWidth };
}

function drawHsnSummary(doc, context, config, options) {
    if (!config) return;
    
    context.y += 15;
    
    const { rows, colWidths, footer, tableWidth, title } = config;
    const { fontSize } = options;

    const startX = (doc.page.width - tableWidth) / 2;

    doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize + 1).fillColor(PDF_SETTINGS.COLOR.SECTION_TITLE)
       .text(title, startX, context.y, {
           width: tableWidth,
           align: 'center'
       });
    context.y += (fontSize + 15);


    const cellPadding = 5;
    const headerHeight = 35;
    
    const headerY = context.y;
    doc.rect(startX, headerY, tableWidth, headerHeight).fill(PDF_SETTINGS.COLOR.TABLE_HEADER_BG);
    doc.fillColor(PDF_SETTINGS.COLOR.TABLE_HEADER_TEXT).font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize - 0.5);

    let currentX = startX;
    
    doc.text('HSN/SAC', currentX, headerY + 12, { width: colWidths[0], align: 'center' });
    currentX += colWidths[0];
    doc.text('Taxable\nValue', currentX, headerY + 5, { width: colWidths[1], align: 'center' });
    currentX += colWidths[1];

    const centralTaxX = currentX;
    doc.text('Central Tax', centralTaxX, headerY + 5, { width: colWidths[2], align: 'center' });
    doc.text('Rate', centralTaxX, headerY + 18, { width: colWidths[2] / 2, align: 'center' });
    doc.text('Amount', centralTaxX + colWidths[2] / 2, headerY + 18, { width: colWidths[2] / 2, align: 'center' });
    currentX += colWidths[2];

    const stateTaxX = currentX;
    doc.text('State Tax', stateTaxX, headerY + 5, { width: colWidths[3], align: 'center' });
    doc.text('Rate', stateTaxX, headerY + 18, { width: colWidths[3] / 2, align: 'center' });
    doc.text('Amount', stateTaxX + colWidths[3] / 2, headerY + 18, { width: colWidths[3] / 2, align: 'center' });
    currentX += colWidths[3];
    
    doc.text('Total Tax\nAmount', currentX, headerY + 5, { width: colWidths[4], align: 'center' });

    context.y += headerHeight;

    rows.forEach((rowData, index) => {
        const hsnCellText = String(rowData.values[0] || '');
        const firstColWidth = colWidths[0] - (cellPadding * 2);
        const textHeight = doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize - 0.5).heightOfString(hsnCellText, {
            width: firstColWidth
        });
        const dynamicRowHeight = Math.max(20, textHeight + (cellPadding * 2));

        const rowY = context.y;
        if (index % 2) {
            doc.rect(startX, rowY, tableWidth, dynamicRowHeight).fill(PDF_SETTINGS.COLOR.ROW_ALT_BG);
        }
        
        doc.font(PDF_SETTINGS.FONT.NORMAL).fontSize(fontSize - 0.5).fillColor(PDF_SETTINGS.COLOR.TEXT_DARK);
        const vAlign = rowY + cellPadding;
        currentX = startX;

        doc.text(hsnCellText, currentX + cellPadding, vAlign, { width: firstColWidth, align: 'center' });
        currentX += colWidths[0];

        doc.text(String(rowData.values[1] || ''), currentX + cellPadding, vAlign, { width: colWidths[1] - (cellPadding * 2), align: 'center' });
        currentX += colWidths[1];

        doc.text(String(rowData.values[2].rate || ''), currentX, vAlign, { width: colWidths[2] / 2, align: 'center' });
        doc.text(String(rowData.values[2].amount || ''), currentX + colWidths[2] / 2, vAlign, { width: colWidths[2] / 2 - cellPadding, align: 'right' });
        currentX += colWidths[2];

        doc.text(String(rowData.values[3].rate || ''), currentX, vAlign, { width: colWidths[3] / 2, align: 'center' });
        doc.text(String(rowData.values[3].amount || ''), currentX + colWidths[3] / 2, vAlign, { width: colWidths[3] / 2 - cellPadding, align: 'right' });
        currentX += colWidths[3];

        doc.text(String(rowData.values[4] || ''), currentX + cellPadding, vAlign, { width: colWidths[4] - (cellPadding * 2), align: 'center' });

        doc.moveTo(startX, rowY + dynamicRowHeight).lineTo(startX + tableWidth, rowY + dynamicRowHeight).stroke(PDF_SETTINGS.COLOR.BORDER_LIGHT);
        context.y += dynamicRowHeight;
    });

    const footerY = context.y;
    const footerHeight = 22;
    doc.rect(startX, footerY, tableWidth, footerHeight).fill(PDF_SETTINGS.COLOR.TABLE_HEADER_BG);
    doc.font(PDF_SETTINGS.FONT.BOLD).fontSize(fontSize).fillColor(PDF_SETTINGS.COLOR.TABLE_HEADER_TEXT);
    
    currentX = startX;

    doc.text(String(footer[0] || ''), currentX + cellPadding, footerY + 7, { width: colWidths[0] - (cellPadding * 2), align: 'center' });
    currentX += colWidths[0];
    
    doc.text(String(footer[1] || ''), currentX + cellPadding, footerY + 7, { width: colWidths[1] - (cellPadding * 2), align: 'center' });
    currentX += colWidths[1];

    doc.text(String(footer[2] || ''), currentX + colWidths[2]/2, footerY + 7, { width: colWidths[2]/2 - cellPadding, align: 'right' });
    currentX += colWidths[2];

    doc.text(String(footer[3] || ''), currentX + colWidths[3]/2, footerY + 7, { width: colWidths[3]/2 - cellPadding, align: 'right' });
    currentX += colWidths[3];
    
    doc.text(String(footer[4] || ''), currentX + cellPadding, footerY + 7, { width: colWidths[4] - (cellPadding * 2), align: 'center' });

    context.y += footerHeight;
}
async function getLogoBuffer(url) {
    if (!url) return null;
    try {
        if (url.startsWith('http')) {
            const response = await axios.get(url, { responseType: 'arraybuffer' });
            return response.data;
        } else {
            const localPath = path.isAbsolute(url) ? url : path.join(__dirname, 'public', url);
            if (fs.existsSync(localPath)) {
                return fs.readFileSync(localPath);
            }
        }
    } catch (error) {
        console.error(`Failed to get logo from ${url}:`, error.message);
    }
    return null;
}

// --- Server Startup ---
async function startServer() {
    console.log(">>> startServer: Function started.");
    if (!await initializeDatabasePool()) {
        console.error(">>> startServer: FATAL - DB pool failed to initialize. Server cannot start.");
        process.exit(1);
    }
    const server = app.listen(port, () => console.log(`Backend server running at http://localhost:${port}`));
    server.on('error', (error) => {
        if (error.syscall !== 'listen') throw error;
        const bind = `Port ${port}`;
        switch (error.code) {
            case 'EACCES': console.error(`${bind} requires elevated privileges.`); process.exit(1); break;
            case 'EADDRINUSE': console.error(`${bind} is already in use.`); process.exit(1); break;
            default: throw error;
        }
    });
    process.on('SIGINT', async () => {
        console.log('>>> SIGINT: Closing server...');
        server.close(async () => {
            if (pool) { await pool.end(); console.log('>>> SIGINT: Database pool closed.'); }
            console.log('>>> SIGINT: Server closed.');
            process.exit(0);
        });
    });
}

startServer().catch(err => {
    console.error("!!! FATAL ERROR DURING SERVER STARTUP SEQUENCE !!!", err.stack);
    process.exit(1);
});
