const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'site-safety-secret-change-in-production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && !process.env.DATABASE_URL.includes('localhost') ? { rejectUnauthorized: false } : false
});

const ADMIN_EMAIL = process.env.ADMIN_EMAIL || '';
let transporter = null;
if (process.env.SMTP_HOST) {
  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: process.env.SMTP_SECURE === 'true',
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
  });
}

async function sendAdminEmail(subject, html) {
  if (!transporter || !ADMIN_EMAIL) return;
  try {
    await transporter.sendMail({
      from: process.env.SMTP_USER || 'noreply@sitesafety.local',
      to: ADMIN_EMAIL,
      subject: `[Site Safety] ${subject}`,
      html
    });
  } catch (err) {
    console.error('Email send failed:', err.message);
  }
}

async function startApp() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL,
    full_name TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'operative',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS near_miss_reports (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    date TEXT NOT NULL, time TEXT NOT NULL, location TEXT NOT NULL,
    description TEXT NOT NULL, potential_severity TEXT NOT NULL,
    immediate_actions TEXT, weather_conditions TEXT, witnesses TEXT,
    photos TEXT, signature TEXT, status TEXT DEFAULT 'open', admin_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS ladder_inspections (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    date TEXT NOT NULL, ladder_id TEXT NOT NULL, ladder_type TEXT NOT NULL,
    location TEXT NOT NULL, stiles_condition TEXT NOT NULL, rungs_condition TEXT NOT NULL,
    feet_condition TEXT NOT NULL, locking_mechanism TEXT NOT NULL,
    labels_visible TEXT NOT NULL, general_condition TEXT NOT NULL,
    safe_to_use TEXT NOT NULL, defects_found TEXT, actions_taken TEXT,
    photos TEXT, signature TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS tower_inspections (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    date TEXT NOT NULL, tower_id TEXT NOT NULL, location TEXT NOT NULL,
    base_plates_condition TEXT NOT NULL, castors_locked TEXT NOT NULL,
    braces_secure TEXT NOT NULL, platforms_condition TEXT NOT NULL,
    guardrails_fitted TEXT NOT NULL, toe_boards_fitted TEXT NOT NULL,
    outriggers_deployed TEXT NOT NULL, access_ladder_secure TEXT NOT NULL,
    safe_to_use TEXT NOT NULL, max_platform_height TEXT, defects_found TEXT,
    actions_taken TEXT, photos TEXT, signature TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS mewp_inspections (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    date TEXT NOT NULL, mewp_id TEXT NOT NULL, mewp_type TEXT NOT NULL,
    location TEXT NOT NULL, controls_functional TEXT NOT NULL,
    emergency_controls TEXT NOT NULL, guardrails_condition TEXT NOT NULL,
    platform_condition TEXT NOT NULL, hydraulics_condition TEXT NOT NULL,
    tyres_condition TEXT NOT NULL, outriggers_condition TEXT NOT NULL,
    harness_anchor_points TEXT NOT NULL, warning_devices TEXT NOT NULL,
    safe_to_use TEXT NOT NULL, hours_meter_reading TEXT, defects_found TEXT,
    actions_taken TEXT, photos TEXT, signature TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS rescue_plans (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    date TEXT NOT NULL, client_name TEXT NOT NULL, project_name TEXT NOT NULL,
    location TEXT NOT NULL, operation TEXT NOT NULL, project_manager TEXT NOT NULL,
    rescue_supervisor TEXT NOT NULL, attendant TEXT, rescue_team TEXT,
    comms_method TEXT NOT NULL, nearest_hospital TEXT NOT NULL,
    em_site_manager_name TEXT, em_site_manager_phone TEXT,
    em_first_aider_name TEXT, em_first_aider_phone TEXT,
    em_fire_marshal_name TEXT, em_fire_marshal_phone TEXT,
    rescue_method TEXT NOT NULL, scene_protection TEXT,
    checklist TEXT, equip_other TEXT, signature TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS training_records (
    id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
    category TEXT NOT NULL, course_name TEXT NOT NULL, provider TEXT,
    card_number TEXT, completion_date TEXT NOT NULL, expiry_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  // Add signature column to existing tables if not present
  const migrateSig = async (table) => {
    try { await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS signature TEXT`); } catch(e) {}
  };
  await Promise.all(['near_miss_reports','ladder_inspections','tower_inspections','mewp_inspections'].map(migrateSig));

  const { rows: admins } = await pool.query("SELECT id FROM users WHERE role = 'admin'");
  if (admins.length === 0) {
    const hash = bcrypt.hashSync('admin123', 10);
    await pool.query('INSERT INTO users (username, password, full_name, role) VALUES ($1, $2, $3, $4)', ['admin', hash, 'Site Admin', 'admin']);
    console.log('Default admin created: admin / admin123');
  }

  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));
  app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
  app.use(express.static(path.join(__dirname, 'public')));

  const storage = multer.diskStorage({
    destination: (req, file, cb) => cb(null, path.join(__dirname, 'uploads')),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
  });
  const upload = multer({ storage, limits: { fileSize: 10 * 1024 * 1024 } });

  function authenticate(req, res, next) {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No token provided' });
    try { req.user = jwt.verify(token, JWT_SECRET); next(); }
    catch { return res.status(401).json({ error: 'Invalid token' }); }
  }

  function adminOnly(req, res, next) {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
    next();
  }

  app.post('/api/auth/login', async (req, res) => {
    const { username, password } = req.body;
    const { rows } = await pool.query('SELECT * FROM users WHERE username = $1', [username]);
    const user = rows[0];
    if (!user || !bcrypt.compareSync(password, user.password))
      return res.status(401).json({ error: 'Invalid credentials' });
    const token = jwt.sign({ id: user.id, username: user.username, full_name: user.full_name, role: user.role }, JWT_SECRET, { expiresIn: '12h' });
    res.json({ token, user: { id: user.id, username: user.username, full_name: user.full_name, role: user.role } });
  });

  app.get('/api/auth/me', authenticate, async (req, res) => {
    const { rows } = await pool.query('SELECT id, username, full_name, role FROM users WHERE id = $1', [req.user.id]);
    res.json(rows[0]);
  });

  app.get('/api/users', authenticate, adminOnly, async (req, res) => {
    const { rows } = await pool.query('SELECT id, username, full_name, role, created_at FROM users ORDER BY created_at DESC');
    res.json(rows);
  });

  app.post('/api/users', authenticate, adminOnly, async (req, res) => {
    const { username, password, full_name, role } = req.body;
    try {
      const hash = bcrypt.hashSync(password, 10);
      const { rows } = await pool.query('INSERT INTO users (username, password, full_name, role) VALUES ($1, $2, $3, $4) RETURNING id', [username, hash, full_name, role || 'operative']);
      res.json({ id: rows[0].id, username, full_name, role: role || 'operative' });
    } catch (err) { res.status(400).json({ error: 'Username already exists' }); }
  });

  app.delete('/api/users/:id', authenticate, adminOnly, async (req, res) => {
    await pool.query('DELETE FROM users WHERE id = $1 AND role != $2', [req.params.id, 'admin']);
    res.json({ success: true });
  });

  app.post('/api/upload', authenticate, upload.array('photos', 5), (req, res) => {
    const files = req.files.map(f => `/uploads/${f.filename}`);
    res.json({ files });
  });

  app.post('/api/near-miss', authenticate, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      'INSERT INTO near_miss_reports (user_id, date, time, location, description, potential_severity, immediate_actions, weather_conditions, witnesses, photos, signature) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id',
      [req.user.id, d.date, d.time, d.location, d.description, d.potential_severity, d.immediate_actions || '', d.weather_conditions || '', d.witnesses || '', d.photos || '', d.signature || '']);
    sendAdminEmail(`New Near Miss Report #${rows[0].id}`,
      `<h2>Near Miss Report</h2><p><strong>Reported by:</strong> ${req.user.full_name}</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Severity:</strong> ${d.potential_severity}</p><p><strong>Description:</strong> ${d.description}</p>`);
    res.json({ id: rows[0].id, message: 'Near miss report submitted' });
  });

  app.get('/api/near-miss', authenticate, async (req, res) => {
    if (req.user.role === 'admin') {
      const { rows } = await pool.query('SELECT n.*, u.full_name as reported_by FROM near_miss_reports n JOIN users u ON n.user_id = u.id ORDER BY n.created_at DESC');
      res.json(rows);
    } else {
      const { rows } = await pool.query('SELECT n.*, u.full_name as reported_by FROM near_miss_reports n JOIN users u ON n.user_id = u.id WHERE n.user_id = $1 ORDER BY n.created_at DESC', [req.user.id]);
      res.json(rows);
    }
  });

  app.patch('/api/near-miss/:id', authenticate, adminOnly, async (req, res) => {
    const { status, admin_notes } = req.body;
    await pool.query('UPDATE near_miss_reports SET status = $1, admin_notes = $2 WHERE id = $3', [status, admin_notes, req.params.id]);
    res.json({ success: true });
  });

  app.delete('/api/near-miss/:id', authenticate, adminOnly, async (req, res) => {
    await pool.query('DELETE FROM near_miss_reports WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  });

  app.post('/api/ladder-inspection', authenticate, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      'INSERT INTO ladder_inspections (user_id, date, ladder_id, ladder_type, location, stiles_condition, rungs_condition, feet_condition, locking_mechanism, labels_visible, general_condition, safe_to_use, defects_found, actions_taken, photos, signature) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id',
      [req.user.id, d.date, d.ladder_id, d.ladder_type, d.location, d.stiles_condition, d.rungs_condition, d.feet_condition, d.locking_mechanism, d.labels_visible, d.general_condition, d.safe_to_use, d.defects_found || '', d.actions_taken || '', d.photos || '', d.signature || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`Ladder Inspection #${rows[0].id}${safetyFlag}`,
      `<h2>Ladder Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>Ladder:</strong> ${d.ladder_id} (${d.ladder_type})</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: rows[0].id, message: 'Ladder inspection submitted' });
  });

  app.get('/api/ladder-inspection', authenticate, async (req, res) => {
    if (req.user.role === 'admin') {
      const { rows } = await pool.query('SELECT l.*, u.full_name as inspected_by FROM ladder_inspections l JOIN users u ON l.user_id = u.id ORDER BY l.created_at DESC');
      res.json(rows);
    } else {
      const { rows } = await pool.query('SELECT l.*, u.full_name as inspected_by FROM ladder_inspections l JOIN users u ON l.user_id = u.id WHERE l.user_id = $1 ORDER BY l.created_at DESC', [req.user.id]);
      res.json(rows);
    }
  });

  app.post('/api/tower-inspection', authenticate, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      'INSERT INTO tower_inspections (user_id, date, tower_id, location, base_plates_condition, castors_locked, braces_secure, platforms_condition, guardrails_fitted, toe_boards_fitted, outriggers_deployed, access_ladder_secure, safe_to_use, max_platform_height, defects_found, actions_taken, photos, signature) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id',
      [req.user.id, d.date, d.tower_id, d.location, d.base_plates_condition, d.castors_locked, d.braces_secure, d.platforms_condition, d.guardrails_fitted, d.toe_boards_fitted, d.outriggers_deployed, d.access_ladder_secure, d.safe_to_use, d.max_platform_height || '', d.defects_found || '', d.actions_taken || '', d.photos || '', d.signature || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`Tower Inspection #${rows[0].id}${safetyFlag}`,
      `<h2>Mobile Tower Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>Tower:</strong> ${d.tower_id}</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: rows[0].id, message: 'Tower inspection submitted' });
  });

  app.get('/api/tower-inspection', authenticate, async (req, res) => {
    if (req.user.role === 'admin') {
      const { rows } = await pool.query('SELECT t.*, u.full_name as inspected_by FROM tower_inspections t JOIN users u ON t.user_id = u.id ORDER BY t.created_at DESC');
      res.json(rows);
    } else {
      const { rows } = await pool.query('SELECT t.*, u.full_name as inspected_by FROM tower_inspections t JOIN users u ON t.user_id = u.id WHERE t.user_id = $1 ORDER BY t.created_at DESC', [req.user.id]);
      res.json(rows);
    }
  });

  app.post('/api/mewp-inspection', authenticate, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      'INSERT INTO mewp_inspections (user_id, date, mewp_id, mewp_type, location, controls_functional, emergency_controls, guardrails_condition, platform_condition, hydraulics_condition, tyres_condition, outriggers_condition, harness_anchor_points, warning_devices, safe_to_use, hours_meter_reading, defects_found, actions_taken, photos, signature) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) RETURNING id',
      [req.user.id, d.date, d.mewp_id, d.mewp_type, d.location, d.controls_functional, d.emergency_controls, d.guardrails_condition, d.platform_condition, d.hydraulics_condition, d.tyres_condition, d.outriggers_condition, d.harness_anchor_points, d.warning_devices, d.safe_to_use, d.hours_meter_reading || '', d.defects_found || '', d.actions_taken || '', d.photos || '', d.signature || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`MEWP Inspection #${rows[0].id}${safetyFlag}`,
      `<h2>MEWP Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>MEWP:</strong> ${d.mewp_id} (${d.mewp_type})</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: rows[0].id, message: 'MEWP inspection submitted' });
  });

  app.get('/api/mewp-inspection', authenticate, async (req, res) => {
    if (req.user.role === 'admin') {
      const { rows } = await pool.query('SELECT m.*, u.full_name as inspected_by FROM mewp_inspections m JOIN users u ON m.user_id = u.id ORDER BY m.created_at DESC');
      res.json(rows);
    } else {
      const { rows } = await pool.query('SELECT m.*, u.full_name as inspected_by FROM mewp_inspections m JOIN users u ON m.user_id = u.id WHERE m.user_id = $1 ORDER BY m.created_at DESC', [req.user.id]);
      res.json(rows);
    }
  });

  app.get('/api/stats', authenticate, adminOnly, async (req, res) => {
    const today = new Date().toISOString().split('T')[0];
    const q = async (sql, params = []) => (await pool.query(sql, params)).rows[0].c;
    const stats = {
      near_miss_total: await q('SELECT COUNT(*) as c FROM near_miss_reports'),
      near_miss_today: await q('SELECT COUNT(*) as c FROM near_miss_reports WHERE date = $1', [today]),
      near_miss_open: await q("SELECT COUNT(*) as c FROM near_miss_reports WHERE status = 'open'"),
      ladder_total: await q('SELECT COUNT(*) as c FROM ladder_inspections'),
      ladder_today: await q('SELECT COUNT(*) as c FROM ladder_inspections WHERE date = $1', [today]),
      tower_total: await q('SELECT COUNT(*) as c FROM tower_inspections'),
      tower_today: await q('SELECT COUNT(*) as c FROM tower_inspections WHERE date = $1', [today]),
      mewp_total: await q('SELECT COUNT(*) as c FROM mewp_inspections'),
      mewp_today: await q('SELECT COUNT(*) as c FROM mewp_inspections WHERE date = $1', [today]),
      unsafe_ladders: await q("SELECT COUNT(*) as c FROM ladder_inspections WHERE safe_to_use = 'No'"),
      unsafe_towers: await q("SELECT COUNT(*) as c FROM tower_inspections WHERE safe_to_use = 'No'"),
      unsafe_mewps: await q("SELECT COUNT(*) as c FROM mewp_inspections WHERE safe_to_use = 'No'"),
      operatives: await q("SELECT COUNT(*) as c FROM users WHERE role = 'operative'")
    };
    Object.keys(stats).forEach(k => stats[k] = parseInt(stats[k]));
    res.json(stats);
  });

  // ═══════ RESCUE PLANS ═══════
  app.post('/api/rescue-plan', authenticate, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      `INSERT INTO rescue_plans (user_id, date, client_name, project_name, location, operation, project_manager,
        rescue_supervisor, attendant, rescue_team, comms_method, nearest_hospital,
        em_site_manager_name, em_site_manager_phone, em_first_aider_name, em_first_aider_phone,
        em_fire_marshal_name, em_fire_marshal_phone, rescue_method, scene_protection, checklist, equip_other, signature)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23) RETURNING id`,
      [req.user.id, d.date, d.client_name, d.project_name, d.location, d.operation, d.project_manager,
       d.rescue_supervisor, d.attendant || '', d.rescue_team || '', d.comms_method, d.nearest_hospital,
       d.em_site_manager_name || '', d.em_site_manager_phone || '', d.em_first_aider_name || '', d.em_first_aider_phone || '',
       d.em_fire_marshal_name || '', d.em_fire_marshal_phone || '', d.rescue_method, d.scene_protection || '',
       d.checklist || '{}', d.equip_other || '', d.signature || '']);
    sendAdminEmail(`New Rescue Plan: ${d.project_name}`,
      `<h2>Rescue Plan Submitted</h2><p><strong>By:</strong> ${req.user.full_name}</p><p><strong>Client:</strong> ${d.client_name}</p><p><strong>Project:</strong> ${d.project_name}</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Rescue Supervisor:</strong> ${d.rescue_supervisor}</p>`);
    res.json({ id: rows[0].id, message: 'Rescue plan submitted' });
  });

  app.get('/api/rescue-plan', authenticate, async (req, res) => {
    if (req.user.role === 'admin') {
      const { rows } = await pool.query('SELECT r.*, u.full_name as submitted_by FROM rescue_plans r JOIN users u ON r.user_id = u.id ORDER BY r.created_at DESC');
      res.json(rows);
    } else {
      const { rows } = await pool.query('SELECT r.*, u.full_name as submitted_by FROM rescue_plans r JOIN users u ON r.user_id = u.id WHERE r.user_id = $1 ORDER BY r.created_at DESC', [req.user.id]);
      res.json(rows);
    }
  });

  app.delete('/api/rescue-plan/:id', authenticate, adminOnly, async (req, res) => {
    await pool.query('DELETE FROM rescue_plans WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  });

  // ═══════ TRAINING MATRIX ═══════
  app.post('/api/training', authenticate, adminOnly, async (req, res) => {
    const d = req.body;
    const { rows } = await pool.query(
      'INSERT INTO training_records (user_id, category, course_name, provider, card_number, completion_date, expiry_date) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id',
      [d.user_id, d.category, d.course_name, d.provider || '', d.card_number || '', d.completion_date, d.expiry_date || null]);
    res.json({ id: rows[0].id, message: 'Training record added' });
  });

  app.get('/api/training', authenticate, async (req, res) => {
    const { rows } = await pool.query('SELECT t.*, u.full_name as operative_name FROM training_records t JOIN users u ON t.user_id = u.id ORDER BY t.expiry_date ASC NULLS LAST');
    res.json(rows);
  });

  app.delete('/api/training/:id', authenticate, adminOnly, async (req, res) => {
    await pool.query('DELETE FROM training_records WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  });

  // ═══════ EMAIL DIGEST ═══════
  async function sendDigest(period) {
    if (!transporter || !ADMIN_EMAIL) return;
    const since = new Date();
    if (period === 'daily') since.setDate(since.getDate() - 1);
    else since.setDate(since.getDate() - 7);
    const sinceStr = since.toISOString().split('T')[0];

    const nmRes = await pool.query('SELECT COUNT(*) as c FROM near_miss_reports WHERE date >= $1', [sinceStr]);
    const ldRes = await pool.query('SELECT COUNT(*) as c FROM ladder_inspections WHERE date >= $1', [sinceStr]);
    const twRes = await pool.query('SELECT COUNT(*) as c FROM tower_inspections WHERE date >= $1', [sinceStr]);
    const mwRes = await pool.query('SELECT COUNT(*) as c FROM mewp_inspections WHERE date >= $1', [sinceStr]);
    const openRes = await pool.query("SELECT COUNT(*) as c FROM near_miss_reports WHERE status = 'open'");
    const unsafeLd = await pool.query("SELECT COUNT(*) as c FROM ladder_inspections WHERE safe_to_use = 'No' AND date >= $1", [sinceStr]);
    const unsafeTw = await pool.query("SELECT COUNT(*) as c FROM tower_inspections WHERE safe_to_use = 'No' AND date >= $1", [sinceStr]);
    const unsafeMw = await pool.query("SELECT COUNT(*) as c FROM mewp_inspections WHERE safe_to_use = 'No' AND date >= $1", [sinceStr]);

    const label = period === 'daily' ? 'Daily' : 'Weekly';
    const html = `
      <h2 style="color:#8B1A1A">ManProjects Ltd — ${label} Safety Digest</h2>
      <p>Period: ${sinceStr} to ${new Date().toISOString().split('T')[0]}</p>
      <table style="border-collapse:collapse;width:100%">
        <tr style="background:#f5f5f5"><td style="padding:10px;border:1px solid #ddd"><strong>Near Miss Reports</strong></td><td style="padding:10px;border:1px solid #ddd">${nmRes.rows[0].c}</td></tr>
        <tr><td style="padding:10px;border:1px solid #ddd"><strong>Ladder Inspections</strong></td><td style="padding:10px;border:1px solid #ddd">${ldRes.rows[0].c}</td></tr>
        <tr style="background:#f5f5f5"><td style="padding:10px;border:1px solid #ddd"><strong>Tower Inspections</strong></td><td style="padding:10px;border:1px solid #ddd">${twRes.rows[0].c}</td></tr>
        <tr><td style="padding:10px;border:1px solid #ddd"><strong>MEWP Inspections</strong></td><td style="padding:10px;border:1px solid #ddd">${mwRes.rows[0].c}</td></tr>
        <tr style="background:#fff3cd"><td style="padding:10px;border:1px solid #ddd"><strong>Open Near Misses</strong></td><td style="padding:10px;border:1px solid #ddd">${openRes.rows[0].c}</td></tr>
        <tr style="background:#f8d7da"><td style="padding:10px;border:1px solid #ddd"><strong>Flagged Unsafe (${label})</strong></td><td style="padding:10px;border:1px solid #ddd">${parseInt(unsafeLd.rows[0].c) + parseInt(unsafeTw.rows[0].c) + parseInt(unsafeMw.rows[0].c)}</td></tr>
      </table>
      <p style="margin-top:20px;font-size:12px;color:#888">ManProjects Ltd — Site Safety System</p>
    `;
    await sendAdminEmail(`${label} Safety Digest`, html);
  }

  // API endpoint to trigger digest manually (admin only)
  app.post('/api/digest/:period', authenticate, adminOnly, async (req, res) => {
    const period = req.params.period === 'weekly' ? 'weekly' : 'daily';
    await sendDigest(period);
    res.json({ success: true, message: `${period} digest sent` });
  });

  // Auto-schedule digest: run check on each request (lightweight)
  let lastDailyDigest = null;
  let lastWeeklyDigest = null;
  function checkDigestSchedule() {
    const now = new Date();
    const hour = now.getHours();
    const day = now.getDay(); // 0=Sun
    const todayKey = now.toISOString().split('T')[0];
    const weekKey = `${now.getFullYear()}-W${Math.ceil((now.getDate() + 6 - now.getDay()) / 7)}`;

    // Daily digest at 7am
    if (hour >= 7 && lastDailyDigest !== todayKey) {
      lastDailyDigest = todayKey;
      sendDigest('daily').catch(err => console.error('Daily digest error:', err.message));
    }
    // Weekly digest on Monday at 7am
    if (day === 1 && hour >= 7 && lastWeeklyDigest !== weekKey) {
      lastWeeklyDigest = weekKey;
      sendDigest('weekly').catch(err => console.error('Weekly digest error:', err.message));
    }
  }
  // Check schedule every 30 minutes
  setInterval(checkDigestSchedule, 30 * 60 * 1000);
  checkDigestSchedule();

  // ═══════ INSPECTION REMINDERS ═══════
  app.get('/api/reminders', authenticate, adminOnly, async (req, res) => {
    // Find equipment not inspected in the last 7 days
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const cutoff = sevenDaysAgo.toISOString().split('T')[0];

    const ladders = await pool.query(`SELECT ladder_id, MAX(date) as last_inspected FROM ladder_inspections GROUP BY ladder_id HAVING MAX(date) < $1`, [cutoff]);
    const towers = await pool.query(`SELECT tower_id, MAX(date) as last_inspected FROM tower_inspections GROUP BY tower_id HAVING MAX(date) < $1`, [cutoff]);
    const mewps = await pool.query(`SELECT mewp_id, MAX(date) as last_inspected FROM mewp_inspections GROUP BY mewp_id HAVING MAX(date) < $1`, [cutoff]);

    res.json({
      overdue_ladders: ladders.rows,
      overdue_towers: towers.rows,
      overdue_mewps: mewps.rows
    });
  });

  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  });

  app.listen(PORT, () => {
    console.log(`Site Safety App running on http://localhost:${PORT}`);
    console.log('Default login: admin / admin123');
  });
}

startApp().catch(err => { console.error('Failed to start:', err); process.exit(1); });
