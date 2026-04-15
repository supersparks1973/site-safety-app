const express = require('express');
const initSqlJs = require('sql.js');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'site-safety-secret-change-in-production';
const DB_PATH = path.join(__dirname, 'safety.db');

// ─── Email Configuration ───
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

// ─── Database Helpers ───
let db;

function saveDb() {
  const data = db.export();
  const buffer = Buffer.from(data);
  fs.writeFileSync(DB_PATH, buffer);
}

// Auto-save every 30 seconds
setInterval(() => { if (db) saveDb(); }, 30000);

// query helper: returns array of objects
function dbAll(sql, params = []) {
  const stmt = db.prepare(sql);
  if (params.length) stmt.bind(params);
  const results = [];
  while (stmt.step()) results.push(stmt.getAsObject());
  stmt.free();
  return results;
}

function dbGet(sql, params = []) {
  const results = dbAll(sql, params);
  return results.length > 0 ? results[0] : null;
}

function dbRun(sql, params = []) {
  db.run(sql, params);
  saveDb();
  return { lastInsertRowid: dbGet('SELECT last_insert_rowid() as id').id };
}

// ─── Start App ───
async function startApp() {
  const SQL = await initSqlJs();

  // Load existing DB or create new
  if (fs.existsSync(DB_PATH)) {
    const fileBuffer = fs.readFileSync(DB_PATH);
    db = new SQL.Database(fileBuffer);
  } else {
    db = new SQL.Database();
  }

  db.run('PRAGMA foreign_keys = ON');

  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'operative',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS near_miss_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    location TEXT NOT NULL,
    description TEXT NOT NULL,
    potential_severity TEXT NOT NULL,
    immediate_actions TEXT,
    weather_conditions TEXT,
    witnesses TEXT,
    photos TEXT,
    status TEXT DEFAULT 'open',
    admin_notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS ladder_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    ladder_id TEXT NOT NULL,
    ladder_type TEXT NOT NULL,
    location TEXT NOT NULL,
    stiles_condition TEXT NOT NULL,
    rungs_condition TEXT NOT NULL,
    feet_condition TEXT NOT NULL,
    locking_mechanism TEXT NOT NULL,
    labels_visible TEXT NOT NULL,
    general_condition TEXT NOT NULL,
    safe_to_use TEXT NOT NULL,
    defects_found TEXT,
    actions_taken TEXT,
    photos TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS tower_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    tower_id TEXT NOT NULL,
    location TEXT NOT NULL,
    base_plates_condition TEXT NOT NULL,
    castors_locked TEXT NOT NULL,
    braces_secure TEXT NOT NULL,
    platforms_condition TEXT NOT NULL,
    guardrails_fitted TEXT NOT NULL,
    toe_boards_fitted TEXT NOT NULL,
    outriggers_deployed TEXT NOT NULL,
    access_ladder_secure TEXT NOT NULL,
    safe_to_use TEXT NOT NULL,
    max_platform_height TEXT,
    defects_found TEXT,
    actions_taken TEXT,
    photos TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS mewp_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    mewp_id TEXT NOT NULL,
    mewp_type TEXT NOT NULL,
    location TEXT NOT NULL,
    controls_functional TEXT NOT NULL,
    emergency_controls TEXT NOT NULL,
    guardrails_condition TEXT NOT NULL,
    platform_condition TEXT NOT NULL,
    hydraulics_condition TEXT NOT NULL,
    tyres_condition TEXT NOT NULL,
    outriggers_condition TEXT NOT NULL,
    harness_anchor_points TEXT NOT NULL,
    warning_devices TEXT NOT NULL,
    safe_to_use TEXT NOT NULL,
    hours_meter_reading TEXT,
    defects_found TEXT,
    actions_taken TEXT,
    photos TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  // Seed default admin if not exists
  const adminExists = dbGet('SELECT id FROM users WHERE role = ?', ['admin']);
  if (!adminExists) {
    const hash = bcrypt.hashSync('admin123', 10);
    dbRun('INSERT INTO users (username, password, full_name, role) VALUES (?, ?, ?, ?)', ['admin', hash, 'Site Admin', 'admin']);
    console.log('Default admin created: admin / admin123');
  }

  // ─── Middleware ───
  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));
  app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
  app.use(express.static(path.join(__dirname, 'public')));

  // File upload config
  const storage = multer.diskStorage({
    destination: (req, file, cb) => cb(null, path.join(__dirname, 'uploads')),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
  });
  const upload = multer({ storage, limits: { fileSize: 10 * 1024 * 1024 } });

  // Auth middleware
  function authenticate(req, res, next) {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No token provided' });
    try {
      req.user = jwt.verify(token, JWT_SECRET);
      next();
    } catch { return res.status(401).json({ error: 'Invalid token' }); }
  }

  function adminOnly(req, res, next) {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
    next();
  }

  // ─── Auth Routes ───
  app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body;
    const user = dbGet('SELECT * FROM users WHERE username = ?', [username]);
    if (!user || !bcrypt.compareSync(password, user.password)) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    const token = jwt.sign({ id: user.id, username: user.username, full_name: user.full_name, role: user.role }, JWT_SECRET, { expiresIn: '12h' });
    res.json({ token, user: { id: user.id, username: user.username, full_name: user.full_name, role: user.role } });
  });

  app.get('/api/auth/me', authenticate, (req, res) => {
    const user = dbGet('SELECT id, username, full_name, role FROM users WHERE id = ?', [req.user.id]);
    res.json(user);
  });

  // ─── User Management (Admin) ───
  app.get('/api/users', authenticate, adminOnly, (req, res) => {
    const users = dbAll('SELECT id, username, full_name, role, created_at FROM users ORDER BY created_at DESC');
    res.json(users);
  });

  app.post('/api/users', authenticate, adminOnly, (req, res) => {
    const { username, password, full_name, role } = req.body;
    try {
      const hash = bcrypt.hashSync(password, 10);
      const result = dbRun('INSERT INTO users (username, password, full_name, role) VALUES (?, ?, ?, ?)', [username, hash, full_name, role || 'operative']);
      res.json({ id: result.lastInsertRowid, username, full_name, role: role || 'operative' });
    } catch (err) {
      res.status(400).json({ error: 'Username already exists' });
    }
  });

  app.delete('/api/users/:id', authenticate, adminOnly, (req, res) => {
    dbRun('DELETE FROM users WHERE id = ? AND role != ?', [parseInt(req.params.id), 'admin']);
    res.json({ success: true });
  });

  // ─── Photo Upload ───
  app.post('/api/upload', authenticate, upload.array('photos', 5), (req, res) => {
    const files = req.files.map(f => `/uploads/${f.filename}`);
    res.json({ files });
  });

  // ─── Near Miss Reports ───
  app.post('/api/near-miss', authenticate, (req, res) => {
    const d = req.body;
    const result = dbRun('INSERT INTO near_miss_reports (user_id, date, time, location, description, potential_severity, immediate_actions, weather_conditions, witnesses, photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [req.user.id, d.date, d.time, d.location, d.description, d.potential_severity, d.immediate_actions || '', d.weather_conditions || '', d.witnesses || '', d.photos || '']);
    sendAdminEmail(`New Near Miss Report #${result.lastInsertRowid}`,
      `<h2>Near Miss Report</h2><p><strong>Reported by:</strong> ${req.user.full_name}</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Severity:</strong> ${d.potential_severity}</p><p><strong>Description:</strong> ${d.description}</p><p><a href="${process.env.APP_URL || 'http://localhost:3000'}">View in Dashboard</a></p>`);
    res.json({ id: result.lastInsertRowid, message: 'Near miss report submitted' });
  });

  app.get('/api/near-miss', authenticate, (req, res) => {
    const reports = req.user.role === 'admin'
      ? dbAll('SELECT n.*, u.full_name as reported_by FROM near_miss_reports n JOIN users u ON n.user_id = u.id ORDER BY n.created_at DESC')
      : dbAll('SELECT n.*, u.full_name as reported_by FROM near_miss_reports n JOIN users u ON n.user_id = u.id WHERE n.user_id = ? ORDER BY n.created_at DESC', [req.user.id]);
    res.json(reports);
  });

  app.patch('/api/near-miss/:id', authenticate, adminOnly, (req, res) => {
    const { status, admin_notes } = req.body;
    dbRun('UPDATE near_miss_reports SET status = ?, admin_notes = ? WHERE id = ?', [status, admin_notes, parseInt(req.params.id)]);
    res.json({ success: true });
  });

  // ─── Ladder Inspections ───
  app.post('/api/ladder-inspection', authenticate, (req, res) => {
    const d = req.body;
    const result = dbRun('INSERT INTO ladder_inspections (user_id, date, ladder_id, ladder_type, location, stiles_condition, rungs_condition, feet_condition, locking_mechanism, labels_visible, general_condition, safe_to_use, defects_found, actions_taken, photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [req.user.id, d.date, d.ladder_id, d.ladder_type, d.location, d.stiles_condition, d.rungs_condition, d.feet_condition, d.locking_mechanism, d.labels_visible, d.general_condition, d.safe_to_use, d.defects_found || '', d.actions_taken || '', d.photos || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`Ladder Inspection #${result.lastInsertRowid}${safetyFlag}`,
      `<h2>Ladder Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>Ladder:</strong> ${d.ladder_id} (${d.ladder_type})</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: result.lastInsertRowid, message: 'Ladder inspection submitted' });
  });

  app.get('/api/ladder-inspection', authenticate, (req, res) => {
    const records = req.user.role === 'admin'
      ? dbAll('SELECT l.*, u.full_name as inspected_by FROM ladder_inspections l JOIN users u ON l.user_id = u.id ORDER BY l.created_at DESC')
      : dbAll('SELECT l.*, u.full_name as inspected_by FROM ladder_inspections l JOIN users u ON l.user_id = u.id WHERE l.user_id = ? ORDER BY l.created_at DESC', [req.user.id]);
    res.json(records);
  });

  // ─── Tower Inspections ───
  app.post('/api/tower-inspection', authenticate, (req, res) => {
    const d = req.body;
    const result = dbRun('INSERT INTO tower_inspections (user_id, date, tower_id, location, base_plates_condition, castors_locked, braces_secure, platforms_condition, guardrails_fitted, toe_boards_fitted, outriggers_deployed, access_ladder_secure, safe_to_use, max_platform_height, defects_found, actions_taken, photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [req.user.id, d.date, d.tower_id, d.location, d.base_plates_condition, d.castors_locked, d.braces_secure, d.platforms_condition, d.guardrails_fitted, d.toe_boards_fitted, d.outriggers_deployed, d.access_ladder_secure, d.safe_to_use, d.max_platform_height || '', d.defects_found || '', d.actions_taken || '', d.photos || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`Tower Inspection #${result.lastInsertRowid}${safetyFlag}`,
      `<h2>Mobile Tower Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>Tower:</strong> ${d.tower_id}</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: result.lastInsertRowid, message: 'Tower inspection submitted' });
  });

  app.get('/api/tower-inspection', authenticate, (req, res) => {
    const records = req.user.role === 'admin'
      ? dbAll('SELECT t.*, u.full_name as inspected_by FROM tower_inspections t JOIN users u ON t.user_id = u.id ORDER BY t.created_at DESC')
      : dbAll('SELECT t.*, u.full_name as inspected_by FROM tower_inspections t JOIN users u ON t.user_id = u.id WHERE t.user_id = ? ORDER BY t.created_at DESC', [req.user.id]);
    res.json(records);
  });

  // ─── MEWP Inspections ───
  app.post('/api/mewp-inspection', authenticate, (req, res) => {
    const d = req.body;
    const result = dbRun('INSERT INTO mewp_inspections (user_id, date, mewp_id, mewp_type, location, controls_functional, emergency_controls, guardrails_condition, platform_condition, hydraulics_condition, tyres_condition, outriggers_condition, harness_anchor_points, warning_devices, safe_to_use, hours_meter_reading, defects_found, actions_taken, photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [req.user.id, d.date, d.mewp_id, d.mewp_type, d.location, d.controls_functional, d.emergency_controls, d.guardrails_condition, d.platform_condition, d.hydraulics_condition, d.tyres_condition, d.outriggers_condition, d.harness_anchor_points, d.warning_devices, d.safe_to_use, d.hours_meter_reading || '', d.defects_found || '', d.actions_taken || '', d.photos || '']);
    const safetyFlag = d.safe_to_use === 'No' ? ' ⚠️ UNSAFE' : '';
    sendAdminEmail(`MEWP Inspection #${result.lastInsertRowid}${safetyFlag}`,
      `<h2>MEWP Inspection</h2><p><strong>Inspected by:</strong> ${req.user.full_name}</p><p><strong>MEWP:</strong> ${d.mewp_id} (${d.mewp_type})</p><p><strong>Location:</strong> ${d.location}</p><p><strong>Safe to use:</strong> ${d.safe_to_use}</p>${d.defects_found ? `<p><strong>Defects:</strong> ${d.defects_found}</p>` : ''}`);
    res.json({ id: result.lastInsertRowid, message: 'MEWP inspection submitted' });
  });

  app.get('/api/mewp-inspection', authenticate, (req, res) => {
    const records = req.user.role === 'admin'
      ? dbAll('SELECT m.*, u.full_name as inspected_by FROM mewp_inspections m JOIN users u ON m.user_id = u.id ORDER BY m.created_at DESC')
      : dbAll('SELECT m.*, u.full_name as inspected_by FROM mewp_inspections m JOIN users u ON m.user_id = u.id WHERE m.user_id = ? ORDER BY m.created_at DESC', [req.user.id]);
    res.json(records);
  });

  // ─── Dashboard Stats (Admin) ───
  app.get('/api/stats', authenticate, adminOnly, (req, res) => {
    const today = new Date().toISOString().split('T')[0];
    const stats = {
      near_miss_total: dbGet('SELECT COUNT(*) as c FROM near_miss_reports').c,
      near_miss_today: dbGet('SELECT COUNT(*) as c FROM near_miss_reports WHERE date = ?', [today]).c,
      near_miss_open: dbGet("SELECT COUNT(*) as c FROM near_miss_reports WHERE status = 'open'").c,
      ladder_total: dbGet('SELECT COUNT(*) as c FROM ladder_inspections').c,
      ladder_today: dbGet('SELECT COUNT(*) as c FROM ladder_inspections WHERE date = ?', [today]).c,
      tower_total: dbGet('SELECT COUNT(*) as c FROM tower_inspections').c,
      tower_today: dbGet('SELECT COUNT(*) as c FROM tower_inspections WHERE date = ?', [today]).c,
      mewp_total: dbGet('SELECT COUNT(*) as c FROM mewp_inspections').c,
      mewp_today: dbGet('SELECT COUNT(*) as c FROM mewp_inspections WHERE date = ?', [today]).c,
      unsafe_ladders: dbGet("SELECT COUNT(*) as c FROM ladder_inspections WHERE safe_to_use = 'No'").c,
      unsafe_towers: dbGet("SELECT COUNT(*) as c FROM tower_inspections WHERE safe_to_use = 'No'").c,
      unsafe_mewps: dbGet("SELECT COUNT(*) as c FROM mewp_inspections WHERE safe_to_use = 'No'").c,
      operatives: dbGet("SELECT COUNT(*) as c FROM users WHERE role = 'operative'").c
    };
    res.json(stats);
  });

  // SPA fallback
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  });

  app.listen(PORT, () => {
    console.log(`Site Safety App running on http://localhost:${PORT}`);
    console.log('Default login: admin / admin123');
  });

  // Save DB on shutdown
  process.on('SIGTERM', () => { saveDb(); process.exit(0); });
  process.on('SIGINT', () => { saveDb(); process.exit(0); });
}

startApp().catch(err => { console.error('Failed to start:', err); process.exit(1); });
