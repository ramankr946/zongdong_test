const express    = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage }  = require('@google-cloud/storage');
const cors       = require('cors');
const multer     = require('multer');
const path       = require('path');
const { v4: uuidv4 } = require('uuid');

const app        = express();
const PROJECT    = process.env.GCP_PROJECT_ID || 'zongdong-backend';
const SECRET     = process.env.ADMIN_SECRET   || 'changeme';
const DATASET    = 'zongdong';
const BUCKET     = process.env.GCS_BUCKET     || 'zongdong-product-images';
const bq         = new BigQuery({ projectId: PROJECT });
const storage    = new Storage({ projectId: PROJECT });

const CORS_OPTS = {
  origin: '*',
  methods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','x-admin-secret','Authorization'],
  optionsSuccessStatus: 204
};
app.use(cors(CORS_OPTS));
app.options('*', cors(CORS_OPTS));
app.use(express.json({ limit: '10mb' }));

// Multer — store in memory, 4 slots, 10MB each
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = /^image\/(jpeg|jpg|png|webp|gif)$/.test(file.mimetype);
    cb(ok ? null : new Error('Only image files allowed'), ok);
  }
});

function auth(req, res, next) {
  if (req.headers['x-admin-secret'] !== SECRET)
    return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function tbl(n) { return '`' + PROJECT + '.' + DATASET + '.' + n + '`'; }

function normTags(t) {
  if (!t) return '';
  if (Array.isArray(t)) return t.join(',');
  return String(t);
}

function normHL(h) {
  if (!h) return [];
  if (Array.isArray(h)) return h.map(String).filter(Boolean);
  // Try JSON parse first (e.g. ["a","b"])
  try {
    const parsed = JSON.parse(h);
    if (Array.isArray(parsed)) return parsed.map(String).filter(Boolean);
  } catch {}
  // Split on newlines or pipe
  return String(h).split(/[\n|]/).map(s => s.trim()).filter(Boolean);
}

app.get('/', (req, res) => res.json({ status: 'ok', service: 'ZongDong API v2.3', project: PROJECT }));

// ── PRODUCTS ──────────────────────────────────────────────────────────────────
// GET /products — public (no auth) for storefront; banned products filtered out
// Admin dashboard passes x-admin-secret to get all products including banned
app.get('/products', async (req, res) => {
  try {
    const isAdmin = req.headers['x-admin-secret'] === SECRET;
    const query = isAdmin
      ? 'SELECT * FROM ' + tbl('products') + ' ORDER BY created_at DESC LIMIT 2000'
      : 'SELECT * FROM ' + tbl('products') + ' WHERE is_banned IS NOT TRUE ORDER BY created_at DESC LIMIT 2000';
    const [rows] = await bq.query(query);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/products', auth, async (req, res) => {
  try {
    const p = req.body;
    if (!p.name || !p.asin || !p.price_inr) return res.status(400).json({ error: 'name, asin and price_inr required' });
    const row = {
      product_id: 'p_' + Date.now() + '_' + Math.floor(Math.random()*1000),
      name: String(p.name).trim(), brand: String(p.brand||'').trim(),
      asin: String(p.asin).trim(), category: String(p.category||'').trim(),
      price_inr: parseFloat(p.price_inr)||0,
      original_price_inr: parseFloat(p.original_price_inr)||parseFloat(p.price_inr)||0,
      weight_grams: parseInt(p.weight_grams)||500,
      stock_status: p.stock_status||'in', is_banned: false, ban_reason: null,
      emoji: String(p.emoji||'box'),
      image_url: p.image_1 || p.image_url || null,
      image_1: p.image_1 || p.image_url || null,
      image_2: p.image_2 || null,
      image_3: p.image_3 || null,
      image_4: p.image_4 || null,
      description: String(p.description||'').trim(),
      highlights: normHL(p.highlights), tags: normTags(p.tags),
      custom_price_aed: p.custom_price_aed ? parseFloat(p.custom_price_aed) : null,
      created_at: bq.timestamp(new Date()), updated_at: bq.timestamp(new Date()),
    };
    await bq.dataset(DATASET).table('products').insert([row]);
    res.json({ success: true, product_id: row.product_id });
  } catch (e) { console.error('POST /products:', e.message); res.status(500).json({ error: e.message, details: e.errors||null }); }
});

app.put('/products/:id', auth, async (req, res) => {
  try {
    const p = req.body; const fields = []; const params = { id: req.params.id };
    const allowed = ['name','brand','asin','category','price_inr','original_price_inr','weight_grams','stock_status','emoji','image_url','image_1','image_2','image_3','image_4','description','highlights','tags','custom_price_aed'];
    let hlSql = null;
    for (const k of allowed) {
      if (p[k] !== undefined) {
        if (k === 'highlights') {
          // ARRAY<STRING> cannot use @param — inline it as SQL literal
          const arr = normHL(p[k]);
          hlSql = arr.length ? '[' + arr.map(s => "'" + s.replace(/'/g, "\\'") + "'").join(',') + ']' : '[]';
          fields.push('highlights = ' + hlSql);
        } else {
          fields.push(k + ' = @' + k);
          params[k] = k === 'tags' ? normTags(p[k]) : p[k];
        }
      }
    }
    if (!fields.length) return res.json({ success: true });
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET ' + fields.join(', ') + ', updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/products/:id/ban', auth, async (req, res) => {
  try {
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET is_banned = TRUE, ban_reason = @reason, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params: { reason: String(req.body.reason||''), id: req.params.id } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/products/:id/unban', auth, async (req, res) => {
  try {
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET is_banned = FALSE, ban_reason = NULL, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params: { id: req.params.id } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── IMAGE UPLOAD ──────────────────────────────────────────────────────────────
app.post('/products/:id/images', auth, upload.fields([
  { name: 'image_1', maxCount: 1 },
  { name: 'image_2', maxCount: 1 },
  { name: 'image_3', maxCount: 1 },
  { name: 'image_4', maxCount: 1 },
]), async (req, res) => {
  try {
    const productId = req.params.id;
    const bucket = storage.bucket(BUCKET);
    const urls = {};
    const bqFields = [];
    const bqParams = { id: productId };

    for (const slot of ['image_1','image_2','image_3','image_4']) {
      const files = req.files?.[slot];
      if (!files || !files[0]) continue;
      const file = files[0];
      const ext  = path.extname(file.originalname).toLowerCase() || '.jpg';
      const dest = `products/${productId}/${slot}_${uuidv4()}${ext}`;
      const gcsFile = bucket.file(dest);

      await gcsFile.save(file.buffer, {
        metadata: { contentType: file.mimetype },
        resumable: false,
      });

      const url = `https://storage.googleapis.com/${BUCKET}/${dest}`;
      urls[slot] = url;
      bqFields.push(`${slot} = @${slot}`);
      bqParams[slot] = url;

      if (slot === 'image_1') {
        bqFields.push('image_url = @image_url');
        bqParams['image_url'] = url;
      }
    }

    if (bqFields.length) {
      await bq.query({
        query: 'UPDATE ' + tbl('products') + ' SET ' + bqFields.join(', ') + ', updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id',
        params: bqParams,
      });
    }

    res.json({ success: true, urls });
  } catch (e) {
    console.error('POST /products/:id/images:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/products/:id/images/:slot', auth, async (req, res) => {
  try {
    const { id, slot } = req.params;
    if (!['image_1','image_2','image_3','image_4'].includes(slot))
      return res.status(400).json({ error: 'Invalid slot (use image_1..image_4)' });
    const extra = slot === 'image_1' ? ', image_url = NULL' : '';
    await bq.query({
      query: `UPDATE ${tbl('products')} SET ${slot} = NULL${extra}, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id`,
      params: { id },
    });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ORDERS ────────────────────────────────────────────────────────────────────
app.get('/orders', auth, async (req, res) => {
  try {
    let rows;
    [rows] = await bq.query('SELECT *, CAST(order_date AS STRING) as order_date_str, CAST(created_at AS STRING) as created_at_str FROM ' + tbl('orders') + ' ORDER BY updated_at DESC LIMIT 1000');
    rows = rows.map(r => ({
      ...r,
      // Normalise to dashboard expected field names
      order_id: r.id || r.order_number || '',
      customer_name: r.customer_name || '',
      customer_email: r.customer_email || '',
      customer_phone: r.customer_phone || '',
      delivery_address: r.shipping_address || '',
      product_name: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.name || r.items || ''; } catch { return r.items || ''; } })(),
      product_emoji: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.emoji || '📦'; } catch { return '📦'; } })(),
      product_asin: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.asin || ''; } catch { return ''; } })(),
      amount_aed: r.total || r.subtotal || 0,
      status: r.status || 'pending',
      tracking_number: r.tracking_number || '',
      notes: r.notes || '',
      order_date: r.order_date?.value || r.order_date_str || r.created_at_str?.slice(0,10) || ''
    }));
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/orders', async (req, res) => { // PUBLIC — COD orders from storefront
  try {
    const o = req.body;
    if (!o.customer_name || !o.product_name) return res.status(400).json({ error: 'customer_name and product_name required' });
    const orderId = 'ZD-' + Date.now().toString().slice(-6);
    await bq.query({
      query: `INSERT INTO ${tbl('orders')}
        (order_id, customer_name, customer_email, customer_phone, delivery_address,
         product_name, product_emoji, product_asin, amount_aed,
         status, tracking_number, notes, updated_at)
        VALUES
        (@order_id, @customer_name, @customer_email, @customer_phone, @delivery_address,
         @product_name, @product_emoji, @product_asin, @amount_aed,
         @status, @tracking_number, @notes, CURRENT_TIMESTAMP())`,
      params: {
        order_id:         orderId,
        customer_name:    String(o.customer_name).trim(),
        customer_email:   String(o.customer_email||'').trim(),
        customer_phone:   String(o.customer_phone||'').trim(),
        delivery_address: String(o.delivery_address||o.shipping_address||'').trim(),
        product_name:     String(o.product_name).trim(),
        product_emoji:    String(o.product_emoji||'box'),
        product_asin:     String(o.product_asin||'').trim(),
        amount_aed:       parseFloat(o.amount_aed||o.total)||0,
        status:           'pending',
        tracking_number:  '',
        notes:            String(o.notes||'').trim(),
      }
    });
    res.json({ success: true, order_id: orderId });
  } catch (e) {
    console.error('POST /orders:', e.message);
    res.status(500).json({ error: e.message });
  }
});

function auth(req, res, next) {
  if (req.headers['x-admin-secret'] !== SECRET)
    return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function tbl(n) { return '`' + PROJECT + '.' + DATASET + '.' + n + '`'; }

function normTags(t) {
  if (!t) return '';
  if (Array.isArray(t)) return t.join(',');
  return String(t);
}

function normHL(h) {
  if (!h) return [];
  if (Array.isArray(h)) return h.map(String).filter(Boolean);
  // Try JSON parse first (e.g. ["a","b"])
  try {
    const parsed = JSON.parse(h);
    if (Array.isArray(parsed)) return parsed.map(String).filter(Boolean);
  } catch {}
  // Split on newlines or pipe
  return String(h).split(/[\n|]/).map(s => s.trim()).filter(Boolean);
}

app.get('/', (req, res) => res.json({ status: 'ok', service: 'ZongDong API v2.3', project: PROJECT }));

// ── PRODUCTS ──────────────────────────────────────────────────────────────────
// GET /products — public (no auth) for storefront; banned products filtered out
// Admin dashboard passes x-admin-secret to get all products including banned
app.get('/products', async (req, res) => {
  try {
    const isAdmin = req.headers['x-admin-secret'] === SECRET;
    const query = isAdmin
      ? 'SELECT * FROM ' + tbl('products') + ' ORDER BY created_at DESC LIMIT 2000'
      : 'SELECT * FROM ' + tbl('products') + ' WHERE is_banned IS NOT TRUE ORDER BY created_at DESC LIMIT 2000';
    const [rows] = await bq.query(query);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/products', auth, async (req, res) => {
  try {
    const p = req.body;
    if (!p.name || !p.asin || !p.price_inr) return res.status(400).json({ error: 'name, asin and price_inr required' });
    const row = {
      product_id: 'p_' + Date.now() + '_' + Math.floor(Math.random()*1000),
      name: String(p.name).trim(), brand: String(p.brand||'').trim(),
      asin: String(p.asin).trim(), category: String(p.category||'').trim(),
      price_inr: parseFloat(p.price_inr)||0,
      original_price_inr: parseFloat(p.original_price_inr)||parseFloat(p.price_inr)||0,
      weight_grams: parseInt(p.weight_grams)||500,
      stock_status: p.stock_status||'in', is_banned: false, ban_reason: null,
      emoji: String(p.emoji||'box'),
      image_url: p.image_1 || p.image_url || null,
      image_1: p.image_1 || p.image_url || null,
      image_2: p.image_2 || null,
      image_3: p.image_3 || null,
      image_4: p.image_4 || null,
      description: String(p.description||'').trim(),
      highlights: normHL(p.highlights), tags: normTags(p.tags),
      custom_price_aed: p.custom_price_aed ? parseFloat(p.custom_price_aed) : null,
      created_at: bq.timestamp(new Date()), updated_at: bq.timestamp(new Date()),
    };
    await bq.dataset(DATASET).table('products').insert([row]);
    res.json({ success: true, product_id: row.product_id });
  } catch (e) { console.error('POST /products:', e.message); res.status(500).json({ error: e.message, details: e.errors||null }); }
});

app.put('/products/:id', auth, async (req, res) => {
  try {
    const p = req.body; const fields = []; const params = { id: req.params.id };
    const allowed = ['name','brand','asin','category','price_inr','original_price_inr','weight_grams','stock_status','emoji','image_url','image_1','image_2','image_3','image_4','description','highlights','tags','custom_price_aed'];
    let hlSql = null;
    for (const k of allowed) {
      if (p[k] !== undefined) {
        if (k === 'highlights') {
          // ARRAY<STRING> cannot use @param — inline it as SQL literal
          const arr = normHL(p[k]);
          hlSql = arr.length ? '[' + arr.map(s => "'" + s.replace(/'/g, "\\'") + "'").join(',') + ']' : '[]';
          fields.push('highlights = ' + hlSql);
        } else {
          fields.push(k + ' = @' + k);
          params[k] = k === 'tags' ? normTags(p[k]) : p[k];
        }
      }
    }
    if (!fields.length) return res.json({ success: true });
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET ' + fields.join(', ') + ', updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/products/:id/ban', auth, async (req, res) => {
  try {
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET is_banned = TRUE, ban_reason = @reason, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params: { reason: String(req.body.reason||''), id: req.params.id } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/products/:id/unban', auth, async (req, res) => {
  try {
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET is_banned = FALSE, ban_reason = NULL, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params: { id: req.params.id } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── IMAGE UPLOAD ──────────────────────────────────────────────────────────────
app.post('/products/:id/images', auth, upload.fields([
  { name: 'image_1', maxCount: 1 },
  { name: 'image_2', maxCount: 1 },
  { name: 'image_3', maxCount: 1 },
  { name: 'image_4', maxCount: 1 },
]), async (req, res) => {
  try {
    const productId = req.params.id;
    const bucket = storage.bucket(BUCKET);
    const urls = {};
    const bqFields = [];
    const bqParams = { id: productId };

    for (const slot of ['image_1','image_2','image_3','image_4']) {
      const files = req.files?.[slot];
      if (!files || !files[0]) continue;
      const file = files[0];
      const ext  = path.extname(file.originalname).toLowerCase() || '.jpg';
      const dest = `products/${productId}/${slot}_${uuidv4()}${ext}`;
      const gcsFile = bucket.file(dest);

      await gcsFile.save(file.buffer, {
        metadata: { contentType: file.mimetype },
        resumable: false,
      });

      const url = `https://storage.googleapis.com/${BUCKET}/${dest}`;
      urls[slot] = url;
      bqFields.push(`${slot} = @${slot}`);
      bqParams[slot] = url;

      if (slot === 'image_1') {
        bqFields.push('image_url = @image_url');
        bqParams['image_url'] = url;
      }
    }

    if (bqFields.length) {
      await bq.query({
        query: 'UPDATE ' + tbl('products') + ' SET ' + bqFields.join(', ') + ', updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id',
        params: bqParams,
      });
    }

    res.json({ success: true, urls });
  } catch (e) {
    console.error('POST /products/:id/images:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/products/:id/images/:slot', auth, async (req, res) => {
  try {
    const { id, slot } = req.params;
    if (!['image_1','image_2','image_3','image_4'].includes(slot))
      return res.status(400).json({ error: 'Invalid slot (use image_1..image_4)' });
    const extra = slot === 'image_1' ? ', image_url = NULL' : '';
    await bq.query({
      query: `UPDATE ${tbl('products')} SET ${slot} = NULL${extra}, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id`,
      params: { id },
    });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ORDERS ────────────────────────────────────────────────────────────────────
app.get('/orders', auth, async (req, res) => {
  try {
    let rows;
    [rows] = await bq.query('SELECT *, CAST(order_date AS STRING) as order_date_str, CAST(created_at AS STRING) as created_at_str FROM ' + tbl('orders') + ' ORDER BY updated_at DESC LIMIT 1000');
    rows = rows.map(r => ({
      ...r,
      // Normalise to dashboard expected field names
      order_id: r.id || r.order_number || '',
      customer_name: r.customer_name || '',
      customer_email: r.customer_email || '',
      customer_phone: r.customer_phone || '',
      delivery_address: r.shipping_address || '',
      product_name: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.name || r.items || ''; } catch { return r.items || ''; } })(),
      product_emoji: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.emoji || '📦'; } catch { return '📦'; } })(),
      product_asin: (() => { try { const it = JSON.parse(r.items||'[]'); return it[0]?.asin || ''; } catch { return ''; } })(),
      amount_aed: r.total || r.subtotal || 0,
      status: r.status || 'pending',
      tracking_number: r.tracking_number || '',
      notes: r.notes || '',
      order_date: r.order_date?.value || r.order_date_str || r.created_at_str?.slice(0,10) || ''
    }));
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

