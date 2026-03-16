const express   = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors      = require('cors');

const app       = express();
const PROJECT   = process.env.GCP_PROJECT_ID || 'zongdong-backend';
const SECRET    = process.env.ADMIN_SECRET   || 'changeme';
const DATASET   = 'zongdong';
const bq        = new BigQuery({ projectId: PROJECT });

const CORS_OPTS = {
  origin: '*',
  methods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','x-admin-secret','Authorization'],
  optionsSuccessStatus: 204
};
app.use(cors(CORS_OPTS));
app.options('*', cors(CORS_OPTS));
app.use(express.json({ limit: '10mb' }));

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
  if (!h) return '[]';
  if (Array.isArray(h)) return JSON.stringify(h);
  try { JSON.parse(h); return h; } catch { return JSON.stringify(String(h).split('|').filter(Boolean)); }
}

app.get('/', (req, res) => res.json({ status: 'ok', service: 'ZongDong API', project: PROJECT }));

app.get('/products', auth, async (req, res) => {
  try {
    const [rows] = await bq.query('SELECT * FROM ' + tbl('products') + ' ORDER BY created_at DESC LIMIT 2000');
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
      emoji: String(p.emoji||'box'), image_url: p.image_url||null,
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
    const allowed = ['name','brand','asin','category','price_inr','original_price_inr','weight_grams','stock_status','emoji','image_url','description','highlights','tags','custom_price_aed'];
    for (const k of allowed) {
      if (p[k] !== undefined) {
        fields.push(k + ' = @' + k);
        params[k] = k==='highlights' ? normHL(p[k]) : k==='tags' ? normTags(p[k]) : p[k];
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

app.patch('/products/:id/stock', auth, async (req, res) => {
  try {
    await bq.query({ query: 'UPDATE ' + tbl('products') + ' SET stock_status = @status, updated_at = CURRENT_TIMESTAMP() WHERE product_id = @id', params: { status: req.body.status, id: req.params.id } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/orders', auth, async (req, res) => {
  try {
    const [rows] = await bq.query('SELECT * FROM ' + tbl('orders') + ' ORDER BY order_date DESC, updated_at DESC LIMIT 1000');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/orders', auth, async (req, res) => {
  try {
    const o = req.body;
    if (!o.customer_name || !o.product_name || !o.product_asin) return res.status(400).json({ error: 'customer_name, product_name, product_asin required' });
    const row = {
      order_id: 'ZD-' + Math.floor(1000 + Math.random()*9000),
      customer_name: String(o.customer_name).trim(), customer_email: String(o.customer_email||'').trim(),
      customer_phone: String(o.customer_phone||'').trim(), delivery_address: String(o.delivery_address||'').trim(),
      product_name: String(o.product_name).trim(), product_emoji: String(o.product_emoji||'box'),
      product_asin: String(o.product_asin).trim(), amount_aed: parseFloat(o.amount_aed)||0,
      status: o.status||'pending', tracking_number: String(o.tracking_number||'').trim(),
      notes: String(o.notes||'').trim(), order_date: bq.date(new Date()), updated_at: bq.timestamp(new Date()),
    };
    await bq.dataset(DATASET).table('orders').insert([row]);
    res.json({ success: true, order_id: row.order_id });
  } catch (e) { console.error('POST /orders:', e.message); res.status(500).json({ error: e.message }); }
});

app.patch('/orders/:id', auth, async (req, res) => {
  try {
    const fields = []; const params = { id: req.params.id };
    if (req.body.status !== undefined)          { fields.push('status = @status');                   params.status          = req.body.status; }
    if (req.body.tracking_number !== undefined) { fields.push('tracking_number = @tracking_number'); params.tracking_number = req.body.tracking_number; }
    if (req.body.notes !== undefined)           { fields.push('notes = @notes');                      params.notes           = req.body.notes; }
    if (!fields.length) return res.json({ success: true });
    await bq.query({ query: 'UPDATE ' + tbl('orders') + ' SET ' + fields.join(', ') + ', updated_at = CURRENT_TIMESTAMP() WHERE order_id = @id', params });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/pricing', auth, async (req, res) => {
  try {
    const [rows] = await bq.query('SELECT * FROM ' + tbl('pricing_overrides') + ' ORDER BY set_at DESC');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/pricing', auth, async (req, res) => {
  try {
    const { product_id, custom_price_aed, override_reason } = req.body;
    if (!product_id) return res.status(400).json({ error: 'product_id required' });
    await bq.query({ query: 'DELETE FROM ' + tbl('pricing_overrides') + ' WHERE product_id = @id', params: { id: product_id } });
    if (custom_price_aed !== null && custom_price_aed !== undefined && custom_price_aed !== '') {
      await bq.dataset(DATASET).table('pricing_overrides').insert([{
        product_id: String(product_id), custom_price_aed: parseFloat(custom_price_aed),
        override_reason: String(override_reason||''), set_by: 'admin',
        set_at: bq.timestamp(new Date()), expires_at: null,
      }]);
    }
    res.json({ success: true });
  } catch (e) { console.error('POST /pricing:', e.message); res.status(500).json({ error: e.message }); }
});

app.get('/stats', auth, async (req, res) => {
  try {
    const [[pR],[oR]] = await Promise.all([
      bq.query('SELECT COUNT(*) as total, COUNTIF(is_banned=FALSE AND stock_status=\'in\') as active, COUNTIF(stock_status=\'out\') as out_of_stock, COUNTIF(is_banned=TRUE) as banned FROM ' + tbl('products')),
      bq.query('SELECT COUNT(*) as total, COUNTIF(status=\'pending\') as pending, COUNTIF(status=\'shipped\') as shipped, COUNTIF(status=\'delivered\') as delivered, SUM(IF(status!=\'cancelled\',amount_aed,0)) as revenue FROM ' + tbl('orders'))
    ]);
    res.json({ products: pR[0], orders: oR[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log('ZongDong API on port ' + PORT));
