const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const nodemailer = require('nodemailer');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        Header, Footer, AlignmentType, BorderStyle, WidthType,
        ShadingType, PageNumber, PageBreak, ImageRun } = require('docx');


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
    id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id),
    external_name TEXT,
    category TEXT NOT NULL, course_name TEXT NOT NULL, provider TEXT,
    card_number TEXT, completion_date TEXT, expiry_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    client_name TEXT NOT NULL,
    site_address TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    start_date TEXT,
    end_date TEXT,
    description TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS toolbox_talks (
    id SERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    content TEXT,
    presenter TEXT NOT NULL,
    site_project TEXT,
    talk_date TEXT NOT NULL,
    attendees TEXT,
    notes TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  // Add signature column to existing tables if not present
  const migrateSig = async (table) => {
    try { await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS signature TEXT`); } catch(e) {}
  };
  await Promise.all(['near_miss_reports','ladder_inspections','tower_inspections','mewp_inspections'].map(migrateSig));

  // Allow completion_date to be NULL for existing training_records table
  try { await pool.query(`ALTER TABLE training_records ALTER COLUMN completion_date DROP NOT NULL`); } catch(e) {}

  // Add external_name to training_records and make user_id nullable
  try { await pool.query('ALTER TABLE training_records ADD COLUMN IF NOT EXISTS external_name TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE training_records ALTER COLUMN user_id DROP NOT NULL'); } catch(e) {}

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
    try { req.user = jwt.verify(token, JWT_SECRET); }
    catch { return res.status(401).json({ error: 'Invalid token' }); }
    // Block write operations for external_view (read-only audit) users
    if (req.user.role === 'external_view' && req.method !== 'GET') {
      return res.status(403).json({ error: 'Read-only access — view only' });
    }
    next();
  }

  function adminOnly(req, res, next) {
    const allowed = ['admin', 'project_manager', 'external_view'];
    if (!allowed.includes(req.user.role)) return res.status(403).json({ error: 'Admin access required' });
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
    await pool.query("DELETE FROM users WHERE id = $1 AND role NOT IN ('admin', 'project_manager')", [req.params.id]);
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
    if (['admin', 'project_manager', 'external_view'].includes(req.user.role)) {
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

  // ═══════ REPORT / INSPECTION WORD DOC DOWNLOADS ═══════
  const docxHelpers = () => {
    const maroon = "8B1A1A", grey = "4A4A4A";
    const bdr = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
    const bds = { top: bdr, bottom: bdr, left: bdr, right: bdr };
    const cm = { top: 60, bottom: 60, left: 100, right: 100 };
    const pw = 9360;
    const lbl = (text, w) => new TableCell({ borders: bds, width: { size: w, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: cm,
      children: [new Paragraph({ children: [new TextRun({ text, bold: true, font: "Arial", size: 20, color: grey })] })] });
    const val = (text, w, span) => new TableCell({ borders: bds, width: { size: w, type: WidthType.DXA }, margins: cm, columnSpan: span || 1,
      children: [new Paragraph({ children: [new TextRun({ text: text || '\u2014', font: "Arial", size: 20 })] })] });
    const condCell = (text, w) => {
      const isPass = ['pass','good','yes','functional','secure','fitted','locked','deployed','visible'].includes((text||'').toLowerCase());
      const isFail = ['fail','poor','no','defective'].includes((text||'').toLowerCase());
      return new TableCell({ borders: bds, width: { size: w, type: WidthType.DXA }, margins: cm,
        shading: isPass ? { fill: "E6F4EA", type: ShadingType.CLEAR } : isFail ? { fill: "FCE8E6", type: ShadingType.CLEAR } : undefined,
        children: [new Paragraph({ children: [new TextRun({ text: text || '\u2014', font: "Arial", size: 20, bold: isFail, color: isFail ? "C0392B" : undefined })] })] });
    };
    const sh = (text) => new Paragraph({ spacing: { before: 300, after: 120 },
      children: [new TextRun({ text, bold: true, font: "Arial", size: 24, color: maroon })] });
    let logoData;
    try { logoData = fs.readFileSync(path.join(__dirname, 'public', 'logo.png')); } catch(e) { logoData = null; }
    const mkHeader = (subtitle) => ({
      default: new Header({ children: [new Paragraph({
        border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: maroon, space: 4 } },
        children: [
          ...(logoData ? [new ImageRun({ data: logoData, transformation: { width: 120, height: 46 }, type: 'png' }), new TextRun({ text: "  ", font: "Arial", size: 22 })] : []),
          new TextRun({ text: "ManProjects", bold: true, font: "Arial", size: 22, color: grey }),
          new TextRun({ text: " Ltd", font: "Arial", size: 18, color: "999999" }),
          new TextRun({ text: "    Electrical and Mechanical Building Services", font: "Arial", size: 14, color: "999999" }),
        ]
      })] })
    });
    const mkFooter = (docType) => ({
      default: new Footer({ children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        border: { top: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC", space: 4 } },
        children: [
          new TextRun({ text: `ManProjects Ltd \u2014 ${docType}  |  Page `, font: "Arial", size: 16, color: "999999" }),
          new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 16, color: "999999" }),
        ]
      })] })
    });
    const pageProps = {
      page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, right: 1200, bottom: 1200, left: 1200 } }
    };
    return { maroon, grey, bds, cm, pw, lbl, val, condCell, sh, mkHeader, mkFooter, pageProps };
  };

  // Near-miss Word doc
  app.get('/api/near-miss/:id/docx', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT n.*, u.full_name as reported_by FROM near_miss_reports n JOIN users u ON n.user_id = u.id WHERE n.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      const r = rows[0];
      const h = docxHelpers();
      const sevColor = r.potential_severity === 'High' ? "C0392B" : r.potential_severity === 'Medium' ? "E67E22" : "27AE60";

      const doc = new Document({
        styles: { default: { document: { run: { font: "Arial", size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader("Near Miss Report"),
          footers: h.mkFooter("Near Miss Report"),
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
              children: [new TextRun({ text: "MAN PROJECTS LTD", bold: true, font: "Arial", size: 32, color: h.maroon })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 },
              children: [new TextRun({ text: "NEAR MISS REPORT", bold: true, font: "Arial", size: 24, color: h.grey })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 40, after: 200 },
              children: [new TextRun({ text: `Reference: NM-${String(r.id).padStart(4, '0')}`, font: "Arial", size: 20, color: "999999" })] }),

            h.sh("INCIDENT DETAILS"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
              new TableRow({ children: [h.lbl("Reported By", 2200), h.val(r.reported_by, 2480), h.lbl("Date", 2200), h.val(r.date, 2480)] }),
              new TableRow({ children: [h.lbl("Time", 2200), h.val(r.time, 2480), h.lbl("Location", 2200), h.val(r.location, 2480)] }),
              new TableRow({ children: [h.lbl("Severity", 2200), new TableCell({ borders: h.bds, width: { size: 2480, type: WidthType.DXA }, margins: h.cm,
                children: [new Paragraph({ children: [new TextRun({ text: r.potential_severity, bold: true, font: "Arial", size: 20, color: sevColor })] })] }),
                h.lbl("Status", 2200), h.val((r.status || 'open').toUpperCase(), 2480)] }),
            ] }),

            h.sh("DESCRIPTION"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [h.pw], rows: [
              new TableRow({ children: [new TableCell({ borders: h.bds, width: { size: h.pw, type: WidthType.DXA }, margins: h.cm,
                children: [new Paragraph({ spacing: { after: 80 }, children: [new TextRun({ text: r.description || '\u2014', font: "Arial", size: 20 })] })] })] })
            ] }),

            ...(r.immediate_actions ? [
              h.sh("IMMEDIATE ACTIONS TAKEN"),
              new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [h.pw], rows: [
                new TableRow({ children: [new TableCell({ borders: h.bds, width: { size: h.pw, type: WidthType.DXA }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: r.immediate_actions, font: "Arial", size: 20 })] })] })] })
              ] })
            ] : []),

            ...(r.weather_conditions || r.witnesses ? [
              h.sh("ADDITIONAL INFORMATION"),
              new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 7160], rows: [
                ...(r.weather_conditions ? [new TableRow({ children: [h.lbl("Weather", 2200), h.val(r.weather_conditions, 7160)] })] : []),
                ...(r.witnesses ? [new TableRow({ children: [h.lbl("Witnesses", 2200), h.val(r.witnesses, 7160)] })] : []),
              ] })
            ] : []),

            ...(r.admin_notes ? [
              h.sh("ADMIN NOTES"),
              new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [h.pw], rows: [
                new TableRow({ children: [new TableCell({ borders: h.bds, width: { size: h.pw, type: WidthType.DXA }, margins: h.cm,
                  shading: { fill: "FFF8E1", type: ShadingType.CLEAR },
                  children: [new Paragraph({ children: [new TextRun({ text: r.admin_notes, font: "Arial", size: 20 })] })] })] })
              ] })
            ] : []),

            ...(r.signature ? [
              h.sh("SIGNATURE"),
              new Paragraph({ children: [new TextRun({ text: "Operative signature captured digitally in the Site Safety App.", font: "Arial", size: 20, color: "888888", italics: true })] }),
            ] : []),

            new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
              children: [new TextRun({ text: "ManProjects Ltd \u2014 Near Miss Report \u2014 Confidential", font: "Arial", size: 16, color: "999999" })] }),
          ]
        }]
      });

      const buffer = await Packer.toBuffer(doc);
      const filename = `Near_Miss_NM${String(r.id).padStart(4,'0')}_${r.date}.docx`;
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
    } catch (e) { console.error('Near miss DOCX error:', e); res.status(500).json({ error: 'Failed to generate document' }); }
  });

  // Ladder inspection Word doc
  app.get('/api/ladder-inspection/:id/docx', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT l.*, u.full_name as inspected_by FROM ladder_inspections l JOIN users u ON l.user_id = u.id WHERE l.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      const r = rows[0];
      const h = docxHelpers();
      const safeColor = r.safe_to_use === 'Yes' ? "27AE60" : "C0392B";

      const doc = new Document({
        styles: { default: { document: { run: { font: "Arial", size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader("Ladder Inspection"),
          footers: h.mkFooter("Ladder Inspection"),
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
              children: [new TextRun({ text: "MAN PROJECTS LTD", bold: true, font: "Arial", size: 32, color: h.maroon })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 },
              children: [new TextRun({ text: "LADDER INSPECTION REPORT", bold: true, font: "Arial", size: 24, color: h.grey })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 40, after: 200 },
              children: [new TextRun({ text: `Reference: LI-${String(r.id).padStart(4, '0')}`, font: "Arial", size: 20, color: "999999" })] }),

            h.sh("GENERAL INFORMATION"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
              new TableRow({ children: [h.lbl("Inspected By", 2200), h.val(r.inspected_by, 2480), h.lbl("Date", 2200), h.val(r.date, 2480)] }),
              new TableRow({ children: [h.lbl("Ladder ID", 2200), h.val(r.ladder_id, 2480), h.lbl("Ladder Type", 2200), h.val(r.ladder_type, 2480)] }),
              new TableRow({ children: [h.lbl("Location", 2200), h.val(r.location, 7160, 3)] }),
            ] }),

            h.sh("INSPECTION CHECKLIST"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [5000, 4360], rows: [
              new TableRow({ children: [
                new TableCell({ borders: h.bds, width: { size: 5000, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Check Item", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
                new TableCell({ borders: h.bds, width: { size: 4360, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Condition", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
              ] }),
              new TableRow({ children: [h.lbl("Stiles Condition", 5000), h.condCell(r.stiles_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Rungs Condition", 5000), h.condCell(r.rungs_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Feet Condition", 5000), h.condCell(r.feet_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Locking Mechanism", 5000), h.condCell(r.locking_mechanism, 4360)] }),
              new TableRow({ children: [h.lbl("Labels Visible", 5000), h.condCell(r.labels_visible, 4360)] }),
              new TableRow({ children: [h.lbl("General Condition", 5000), h.condCell(r.general_condition, 4360)] }),
            ] }),

            h.sh("OUTCOME"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 7160], rows: [
              new TableRow({ children: [h.lbl("Safe to Use", 2200), new TableCell({ borders: h.bds, width: { size: 7160, type: WidthType.DXA }, margins: h.cm,
                shading: { fill: r.safe_to_use === 'Yes' ? "E6F4EA" : "FCE8E6", type: ShadingType.CLEAR },
                children: [new Paragraph({ children: [new TextRun({ text: r.safe_to_use === 'Yes' ? 'YES \u2014 Safe to Use' : 'NO \u2014 Not Safe to Use', bold: true, font: "Arial", size: 22, color: safeColor })] })] })] }),
              ...(r.defects_found ? [new TableRow({ children: [h.lbl("Defects Found", 2200), h.val(r.defects_found, 7160)] })] : []),
              ...(r.actions_taken ? [new TableRow({ children: [h.lbl("Actions Taken", 2200), h.val(r.actions_taken, 7160)] })] : []),
            ] }),

            ...(r.signature ? [h.sh("SIGNATURE"), new Paragraph({ children: [new TextRun({ text: "Operative signature captured digitally in the Site Safety App.", font: "Arial", size: 20, color: "888888", italics: true })] })] : []),
            new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
              children: [new TextRun({ text: "ManProjects Ltd \u2014 Ladder Inspection \u2014 Confidential", font: "Arial", size: 16, color: "999999" })] }),
          ]
        }]
      });

      const buffer = await Packer.toBuffer(doc);
      const filename = `Ladder_Inspection_LI${String(r.id).padStart(4,'0')}_${r.date}.docx`;
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
    } catch (e) { console.error('Ladder DOCX error:', e); res.status(500).json({ error: 'Failed to generate document' }); }
  });

  // Tower inspection Word doc
  app.get('/api/tower-inspection/:id/docx', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT t.*, u.full_name as inspected_by FROM tower_inspections t JOIN users u ON t.user_id = u.id WHERE t.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      const r = rows[0];
      const h = docxHelpers();
      const safeColor = r.safe_to_use === 'Yes' ? "27AE60" : "C0392B";

      const doc = new Document({
        styles: { default: { document: { run: { font: "Arial", size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader("Tower Scaffold Inspection"),
          footers: h.mkFooter("Tower Scaffold Inspection"),
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
              children: [new TextRun({ text: "MAN PROJECTS LTD", bold: true, font: "Arial", size: 32, color: h.maroon })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 },
              children: [new TextRun({ text: "TOWER SCAFFOLD INSPECTION REPORT", bold: true, font: "Arial", size: 24, color: h.grey })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 40, after: 200 },
              children: [new TextRun({ text: `Reference: TI-${String(r.id).padStart(4, '0')}`, font: "Arial", size: 20, color: "999999" })] }),

            h.sh("GENERAL INFORMATION"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
              new TableRow({ children: [h.lbl("Inspected By", 2200), h.val(r.inspected_by, 2480), h.lbl("Date", 2200), h.val(r.date, 2480)] }),
              new TableRow({ children: [h.lbl("Tower ID", 2200), h.val(r.tower_id, 2480), h.lbl("Location", 2200), h.val(r.location, 2480)] }),
              ...(r.max_platform_height ? [new TableRow({ children: [h.lbl("Max Platform Height", 2200), h.val(r.max_platform_height, 7160, 3)] })] : []),
            ] }),

            h.sh("INSPECTION CHECKLIST"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [5000, 4360], rows: [
              new TableRow({ children: [
                new TableCell({ borders: h.bds, width: { size: 5000, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Check Item", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
                new TableCell({ borders: h.bds, width: { size: 4360, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Condition", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
              ] }),
              new TableRow({ children: [h.lbl("Base Plates", 5000), h.condCell(r.base_plates_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Castors Locked", 5000), h.condCell(r.castors_locked, 4360)] }),
              new TableRow({ children: [h.lbl("Braces Secure", 5000), h.condCell(r.braces_secure, 4360)] }),
              new TableRow({ children: [h.lbl("Platforms Condition", 5000), h.condCell(r.platforms_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Guardrails Fitted", 5000), h.condCell(r.guardrails_fitted, 4360)] }),
              new TableRow({ children: [h.lbl("Toe Boards Fitted", 5000), h.condCell(r.toe_boards_fitted, 4360)] }),
              new TableRow({ children: [h.lbl("Outriggers Deployed", 5000), h.condCell(r.outriggers_deployed, 4360)] }),
              new TableRow({ children: [h.lbl("Access Ladder Secure", 5000), h.condCell(r.access_ladder_secure, 4360)] }),
            ] }),

            h.sh("OUTCOME"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 7160], rows: [
              new TableRow({ children: [h.lbl("Safe to Use", 2200), new TableCell({ borders: h.bds, width: { size: 7160, type: WidthType.DXA }, margins: h.cm,
                shading: { fill: r.safe_to_use === 'Yes' ? "E6F4EA" : "FCE8E6", type: ShadingType.CLEAR },
                children: [new Paragraph({ children: [new TextRun({ text: r.safe_to_use === 'Yes' ? 'YES \u2014 Safe to Use' : 'NO \u2014 Not Safe to Use', bold: true, font: "Arial", size: 22, color: safeColor })] })] })] }),
              ...(r.defects_found ? [new TableRow({ children: [h.lbl("Defects Found", 2200), h.val(r.defects_found, 7160)] })] : []),
              ...(r.actions_taken ? [new TableRow({ children: [h.lbl("Actions Taken", 2200), h.val(r.actions_taken, 7160)] })] : []),
            ] }),

            ...(r.signature ? [h.sh("SIGNATURE"), new Paragraph({ children: [new TextRun({ text: "Operative signature captured digitally in the Site Safety App.", font: "Arial", size: 20, color: "888888", italics: true })] })] : []),
            new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
              children: [new TextRun({ text: "ManProjects Ltd \u2014 Tower Scaffold Inspection \u2014 Confidential", font: "Arial", size: 16, color: "999999" })] }),
          ]
        }]
      });

      const buffer = await Packer.toBuffer(doc);
      const filename = `Tower_Inspection_TI${String(r.id).padStart(4,'0')}_${r.date}.docx`;
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
    } catch (e) { console.error('Tower DOCX error:', e); res.status(500).json({ error: 'Failed to generate document' }); }
  });

  // MEWP inspection Word doc
  app.get('/api/mewp-inspection/:id/docx', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT m.*, u.full_name as inspected_by FROM mewp_inspections m JOIN users u ON m.user_id = u.id WHERE m.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      const r = rows[0];
      const h = docxHelpers();
      const safeColor = r.safe_to_use === 'Yes' ? "27AE60" : "C0392B";

      const doc = new Document({
        styles: { default: { document: { run: { font: "Arial", size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader("MEWP Inspection"),
          footers: h.mkFooter("MEWP Inspection"),
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
              children: [new TextRun({ text: "MAN PROJECTS LTD", bold: true, font: "Arial", size: 32, color: h.maroon })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 },
              children: [new TextRun({ text: "MEWP INSPECTION REPORT", bold: true, font: "Arial", size: 24, color: h.grey })] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 40, after: 200 },
              children: [new TextRun({ text: `Reference: MI-${String(r.id).padStart(4, '0')}`, font: "Arial", size: 20, color: "999999" })] }),

            h.sh("GENERAL INFORMATION"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
              new TableRow({ children: [h.lbl("Inspected By", 2200), h.val(r.inspected_by, 2480), h.lbl("Date", 2200), h.val(r.date, 2480)] }),
              new TableRow({ children: [h.lbl("MEWP ID", 2200), h.val(r.mewp_id, 2480), h.lbl("MEWP Type", 2200), h.val(r.mewp_type, 2480)] }),
              new TableRow({ children: [h.lbl("Location", 2200), h.val(r.location, 2480), h.lbl("Hours Meter", 2200), h.val(r.hours_meter_reading, 2480)] }),
            ] }),

            h.sh("INSPECTION CHECKLIST"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [5000, 4360], rows: [
              new TableRow({ children: [
                new TableCell({ borders: h.bds, width: { size: 5000, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Check Item", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
                new TableCell({ borders: h.bds, width: { size: 4360, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: h.cm,
                  children: [new Paragraph({ children: [new TextRun({ text: "Condition", bold: true, font: "Arial", size: 20, color: h.grey })] })] }),
              ] }),
              new TableRow({ children: [h.lbl("Controls Functional", 5000), h.condCell(r.controls_functional, 4360)] }),
              new TableRow({ children: [h.lbl("Emergency Controls", 5000), h.condCell(r.emergency_controls, 4360)] }),
              new TableRow({ children: [h.lbl("Guardrails Condition", 5000), h.condCell(r.guardrails_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Platform Condition", 5000), h.condCell(r.platform_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Hydraulics Condition", 5000), h.condCell(r.hydraulics_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Tyres Condition", 5000), h.condCell(r.tyres_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Outriggers Condition", 5000), h.condCell(r.outriggers_condition, 4360)] }),
              new TableRow({ children: [h.lbl("Harness Anchor Points", 5000), h.condCell(r.harness_anchor_points, 4360)] }),
              new TableRow({ children: [h.lbl("Warning Devices", 5000), h.condCell(r.warning_devices, 4360)] }),
            ] }),

            h.sh("OUTCOME"),
            new Table({ width: { size: h.pw, type: WidthType.DXA }, columnWidths: [2200, 7160], rows: [
              new TableRow({ children: [h.lbl("Safe to Use", 2200), new TableCell({ borders: h.bds, width: { size: 7160, type: WidthType.DXA }, margins: h.cm,
                shading: { fill: r.safe_to_use === 'Yes' ? "E6F4EA" : "FCE8E6", type: ShadingType.CLEAR },
                children: [new Paragraph({ children: [new TextRun({ text: r.safe_to_use === 'Yes' ? 'YES \u2014 Safe to Use' : 'NO \u2014 Not Safe to Use', bold: true, font: "Arial", size: 22, color: safeColor })] })] })] }),
              ...(r.defects_found ? [new TableRow({ children: [h.lbl("Defects Found", 2200), h.val(r.defects_found, 7160)] })] : []),
              ...(r.actions_taken ? [new TableRow({ children: [h.lbl("Actions Taken", 2200), h.val(r.actions_taken, 7160)] })] : []),
            ] }),

            ...(r.signature ? [h.sh("SIGNATURE"), new Paragraph({ children: [new TextRun({ text: "Operative signature captured digitally in the Site Safety App.", font: "Arial", size: 20, color: "888888", italics: true })] })] : []),
            new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
              children: [new TextRun({ text: "ManProjects Ltd \u2014 MEWP Inspection \u2014 Confidential", font: "Arial", size: 16, color: "999999" })] }),
          ]
        }]
      });

      const buffer = await Packer.toBuffer(doc);
      const filename = `MEWP_Inspection_MI${String(r.id).padStart(4,'0')}_${r.date}.docx`;
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
    } catch (e) { console.error('MEWP DOCX error:', e); res.status(500).json({ error: 'Failed to generate document' }); }
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

  // ═══════ DASHBOARD: RECENT ACTIVITY + WEEKLY STATS ═══════
  app.get('/api/dashboard-activity', authenticate, adminOnly, async (req, res) => {
    try {
      const { rows: recent } = await pool.query(`
        (SELECT 'near-miss' as type, n.id, n.date, n.location, n.potential_severity as detail, u.full_name as by_name, n.created_at FROM near_miss_reports n JOIN users u ON n.user_id = u.id ORDER BY n.created_at DESC LIMIT 5)
        UNION ALL
        (SELECT 'ladder' as type, l.id, l.date, l.location, l.safe_to_use as detail, u.full_name as by_name, l.created_at FROM ladder_inspections l JOIN users u ON l.user_id = u.id ORDER BY l.created_at DESC LIMIT 5)
        UNION ALL
        (SELECT 'tower' as type, t.id, t.date, t.location, t.safe_to_use as detail, u.full_name as by_name, t.created_at FROM tower_inspections t JOIN users u ON t.user_id = u.id ORDER BY t.created_at DESC LIMIT 5)
        UNION ALL
        (SELECT 'mewp' as type, m.id, m.date, m.location, m.safe_to_use as detail, u.full_name as by_name, m.created_at FROM mewp_inspections m JOIN users u ON m.user_id = u.id ORDER BY m.created_at DESC LIMIT 5)
        ORDER BY created_at DESC LIMIT 8
      `);

      // Weekly counts (last 7 days)
      const weekly = [];
      for (let i = 6; i >= 0; i--) {
        const d = new Date(); d.setDate(d.getDate() - i);
        const ds = d.toISOString().split('T')[0];
        const day = d.toLocaleDateString('en-GB', { weekday: 'short' });
        const { rows: [{c}] } = await pool.query(
          `SELECT (SELECT COUNT(*) FROM near_miss_reports WHERE date=$1) +
                  (SELECT COUNT(*) FROM ladder_inspections WHERE date=$1) +
                  (SELECT COUNT(*) FROM tower_inspections WHERE date=$1) +
                  (SELECT COUNT(*) FROM mewp_inspections WHERE date=$1) as c`, [ds]);
        weekly.push({ day, date: ds, count: parseInt(c) });
      }

      res.json({ recent, weekly });
    } catch(e) { res.status(500).json({ error: e.message }); }
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

  app.get('/api/rescue-plan/:id/docx', authenticate, async (req, res) => {
    const { rows } = await pool.query('SELECT r.*, u.full_name as submitted_by FROM rescue_plans r JOIN users u ON r.user_id = u.id WHERE r.id = $1', [req.params.id]);
    if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
    const p = rows[0];
    let checklist = {};
    try { checklist = JSON.parse(p.checklist || '{}'); } catch {}

    const maroon = "8B1A1A";
    const grey = "4A4A4A";
    const bdr = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
    const bds = { top: bdr, bottom: bdr, left: bdr, right: bdr };
    const cm = { top: 60, bottom: 60, left: 100, right: 100 };
    const pw = 9360;

    const lbl = (text, w) => new TableCell({ borders: bds, width: { size: w, type: WidthType.DXA }, shading: { fill: "E8E8E8", type: ShadingType.CLEAR }, margins: cm,
      children: [new Paragraph({ children: [new TextRun({ text, bold: true, font: "Arial", size: 20, color: grey })] })] });
    const val = (text, w, span) => new TableCell({ borders: bds, width: { size: w, type: WidthType.DXA }, margins: cm, columnSpan: span || 1,
      children: [new Paragraph({ children: [new TextRun({ text: text || '—', font: "Arial", size: 20 })] })] });
    const sh = (num, title) => new Paragraph({ spacing: { before: 300, after: 120 },
      children: [new TextRun({ text: `${num}. ${title}`, bold: true, font: "Arial", size: 24, color: maroon })] });
    const ci = (key, label) => new Paragraph({ spacing: { before: 40, after: 40 },
      children: [new TextRun({ text: (checklist[key] === 'Yes' ? '\u2611' : '\u2610') + '  ' + label, font: "Arial", size: 20 })] });

    const doc = new Document({
      styles: { default: { document: { run: { font: "Arial", size: 22 } } } },
      sections: [{
        properties: {
          page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, right: 1200, bottom: 1200, left: 1200 } }
        },
        headers: { default: new Header({ children: [new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: maroon, space: 4 } },
          children: [
            new TextRun({ text: "ManProjects", bold: true, font: "Arial", size: 22, color: grey }),
            new TextRun({ text: " Ltd", font: "Arial", size: 18, color: "999999" }),
            new TextRun({ text: "    Electrical and Mechanical Building Services", font: "Arial", size: 14, color: "999999" }),
          ]
        })] }) },
        footers: { default: new Footer({ children: [new Paragraph({
          alignment: AlignmentType.CENTER,
          border: { top: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC", space: 4 } },
          children: [
            new TextRun({ text: "ManProjects Ltd \u2014 Rescue Plan  |  Page ", font: "Arial", size: 16, color: "999999" }),
            new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 16, color: "999999" }),
          ]
        })] }) },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
            children: [new TextRun({ text: "MAN PROJECTS LTD", bold: true, font: "Arial", size: 32, color: maroon })] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 },
            children: [new TextRun({ text: "RESCUE PLAN / EMERGENCY RESPONSE", bold: true, font: "Arial", size: 24, color: grey })] }),

          sh("1", "PROJECT DETAILS"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
            new TableRow({ children: [lbl("Client Name", 2200), val(p.client_name, 2480), lbl("Project Name", 2200), val(p.project_name, 2480)] }),
            new TableRow({ children: [lbl("Location", 2200), val(p.location, 2480), lbl("Operation", 2200), val(p.operation, 2480)] }),
            new TableRow({ children: [lbl("Project Manager", 2200), val(p.project_manager, 2480), lbl("Date", 2200), val(p.date, 2480)] }),
            new TableRow({ children: [lbl("Submitted By", 2200), val(p.submitted_by, 7160, 3)] }),
          ] }),

          sh("2", "PERSONS RESPONSIBLE FOR RESCUE"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
            new TableRow({ children: [lbl("Rescue Supervisor", 2200), val(p.rescue_supervisor, 2480), lbl("Attendant", 2200), val(p.attendant, 2480)] }),
            new TableRow({ children: [lbl("Rescue Team", 2200), val(p.rescue_team, 7160, 3)] }),
          ] }),

          sh("3", "COMMUNICATION & EMERGENCY CONTACTS"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [2200, 2480, 2200, 2480], rows: [
            new TableRow({ children: [lbl("Comms Method", 2200), val(p.comms_method, 2480), lbl("Nearest Hospital", 2200), val(p.nearest_hospital, 2480)] }),
            new TableRow({ children: [lbl("Site Manager", 2200), val(p.em_site_manager_name, 2480), lbl("Phone", 2200), val(p.em_site_manager_phone, 2480)] }),
            new TableRow({ children: [lbl("First Aider", 2200), val(p.em_first_aider_name, 2480), lbl("Phone", 2200), val(p.em_first_aider_phone, 2480)] }),
            new TableRow({ children: [lbl("Fire Marshal", 2200), val(p.em_fire_marshal_name, 2480), lbl("Phone", 2200), val(p.em_fire_marshal_phone, 2480)] }),
          ] }),

          sh("4", "RESCUE PROCEDURE"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [2200, 7160], rows: [
            new TableRow({ children: [lbl("Planned Rescue Method", 2200), val(p.rescue_method, 7160, 3)] }),
            new TableRow({ children: [lbl("Scene Protection", 2200), val(p.scene_protection, 7160, 3)] }),
          ] }),

          sh("5", "PRE-RESCUE CHECKLIST"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [pw], rows: [
            new TableRow({ children: [new TableCell({ borders: bds, width: { size: pw, type: WidthType.DXA }, margins: cm, children: [
              ci('check_team_briefed', 'Rescue team briefed and competent'),
              ci('check_equipment_checked', 'Rescue equipment checked and in position'),
              ci('check_comms_tested', 'Communications tested'),
              ci('check_first_aid', 'First aid provision confirmed'),
              ci('check_access_routes', 'Access / egress routes confirmed'),
              ci('check_emergency_services', 'Emergency services access confirmed'),
            ] })] })
          ] }),

          sh("6", "RESCUE EQUIPMENT AVAILABLE"),
          new Table({ width: { size: pw, type: WidthType.DXA }, columnWidths: [4680, 4680], rows: [
            new TableRow({ children: [
              new TableCell({ borders: bds, width: { size: 4680, type: WidthType.DXA }, margins: cm, children: [
                ci('equip_harness', 'Full body harness'), ci('equip_lanyard', 'Rescue lanyard / rope'),
                ci('equip_tripod', 'Tripod / davit system'), ci('equip_winch', 'Winch / descent device'),
              ] }),
              new TableCell({ borders: bds, width: { size: 4680, type: WidthType.DXA }, margins: cm, children: [
                ci('equip_first_aid', 'First aid kit'), ci('equip_stretcher', 'Stretcher / spine board'),
                ci('equip_radio', 'Two-way radios'), ci('equip_gas_monitor', 'Gas monitor'),
              ] })
            ] }),
          ] }),

          ...(p.signature ? [
            sh("7", "SIGNATURE"),
            new Paragraph({ children: [new TextRun({ text: "Operative signature captured digitally in the Site Safety App.", font: "Arial", size: 20, color: "888888", italics: true })] }),
          ] : []),

          new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
            children: [new TextRun({ text: "ManProjects Ltd \u2014 Rescue Plan \u2014 Confidential", font: "Arial", size: 16, color: "999999" })] }),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const filename = `Rescue_Plan_${p.project_name.replace(/[^a-zA-Z0-9]/g, '_')}_${p.date}.docx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buffer);
  });

  app.delete('/api/rescue-plan/:id', authenticate, adminOnly, async (req, res) => {
    await pool.query('DELETE FROM rescue_plans WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  });

  // ═══════ TRAINING MATRIX ═══════
  app.post('/api/training', authenticate, adminOnly, async (req, res) => {
    const d = req.body;
    const userId = d.user_id && d.user_id !== 'null' ? d.user_id : null;
    const { rows } = await pool.query(
      'INSERT INTO training_records (user_id, external_name, category, course_name, provider, card_number, completion_date, expiry_date) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id',
      [userId, d.external_name || null, d.category, d.course_name, d.provider || '', d.card_number || '', d.completion_date || null, d.expiry_date || null]);
    res.json({ id: rows[0].id, message: 'Training record added' });
  });

  app.get('/api/training', authenticate, async (req, res) => {
    const { rows } = await pool.query(`SELECT t.*, COALESCE(u.full_name, t.external_name, 'Unknown') as operative_name FROM training_records t LEFT JOIN users u ON t.user_id = u.id ORDER BY t.expiry_date ASC NULLS LAST`);
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

  // ═══════ TRAINING EXPIRY API ═══════
  app.get('/api/training-alerts', authenticate, adminOnly, async (req, res) => {
    const today = new Date().toISOString().split('T')[0];
    const now = new Date();
    const day14 = new Date(now); day14.setDate(day14.getDate() + 14);
    const day30 = new Date(now); day30.setDate(day30.getDate() + 30);

    try {
      const { rows: expired } = await pool.query(
        `SELECT t.*, COALESCE(u.full_name, t.external_name, 'Unknown') as operative_name
         FROM training_records t LEFT JOIN users u ON t.user_id = u.id
         WHERE t.expiry_date < $1 AND t.expiry_date IS NOT NULL ORDER BY t.expiry_date ASC`, [today]);

      const { rows: within14 } = await pool.query(
        `SELECT t.*, COALESCE(u.full_name, t.external_name, 'Unknown') as operative_name
         FROM training_records t LEFT JOIN users u ON t.user_id = u.id
         WHERE t.expiry_date >= $1 AND t.expiry_date <= $2 ORDER BY t.expiry_date ASC`,
        [today, day14.toISOString().split('T')[0]]);

      const { rows: within30 } = await pool.query(
        `SELECT t.*, COALESCE(u.full_name, t.external_name, 'Unknown') as operative_name
         FROM training_records t LEFT JOIN users u ON t.user_id = u.id
         WHERE t.expiry_date > $1 AND t.expiry_date <= $2 ORDER BY t.expiry_date ASC`,
        [day14.toISOString().split('T')[0], day30.toISOString().split('T')[0]]);

      res.json({ expired, within14, within30 });
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // ═══════ TOOLBOX TALKS ═══════
  app.get('/api/toolbox-talks', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT t.*, u.full_name as created_by_name FROM toolbox_talks t LEFT JOIN users u ON t.created_by = u.id ORDER BY t.talk_date DESC, t.created_at DESC');
      res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  app.get('/api/toolbox-talks/:id', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT t.*, u.full_name as created_by_name FROM toolbox_talks t LEFT JOIN users u ON t.created_by = u.id WHERE t.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      res.json(rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/api/toolbox-talks', authenticate, async (req, res) => {
    try {
      const d = req.body;
      const { rows } = await pool.query(
        'INSERT INTO toolbox_talks (topic, content, presenter, site_project, talk_date, attendees, notes, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',
        [d.topic, d.content || null, d.presenter, d.site_project || null, d.talk_date, JSON.stringify(d.attendees || []), d.notes || null, req.user.id]
      );
      res.json(rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/api/toolbox-talks/:id', authenticate, adminOnly, async (req, res) => {
    try {
      await pool.query('DELETE FROM toolbox_talks WHERE id = $1', [req.params.id]);
      res.json({ message: 'Deleted' });
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // Toolbox Talk Word Doc export
  app.get('/api/toolbox-talks/:id/docx', authenticate, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT t.*, u.full_name as created_by_name FROM toolbox_talks t LEFT JOIN users u ON t.created_by = u.id WHERE t.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Not found' });
      const t = rows[0];
      const attendees = typeof t.attendees === 'string' ? JSON.parse(t.attendees) : (t.attendees || []);
      const h = docxHelpers();
      const halfW = h.pw / 2;
      const doc = new docx.Document({
        styles: { default: { document: { run: { font: 'Arial', size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader('Toolbox Talk Record'),
          footers: h.mkFooter('Toolbox Talk Record'),
          children: [
            h.sh('TOOLBOX TALK RECORD'),
            new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [
              new TableRow({ children: [h.lbl('Topic', halfW), h.val(t.topic || '', halfW)] }),
              new TableRow({ children: [h.lbl('Date', halfW), h.val(t.talk_date ? new Date(t.talk_date).toLocaleDateString('en-GB') : '', halfW)] }),
              new TableRow({ children: [h.lbl('Presenter', halfW), h.val(t.presenter || '', halfW)] }),
              new TableRow({ children: [h.lbl('Site / Project', halfW), h.val(t.site_project || '', halfW)] }),
            ] }),
            new Paragraph({ spacing: { before: 200 } }),
            h.sh('TALK CONTENT'),
            new Paragraph({ children: [new TextRun({ text: t.content || 'No content recorded.', font: 'Arial', size: 20 })], spacing: { after: 200 } }),
            new Paragraph({ spacing: { before: 200 } }),
            h.sh('ATTENDEES'),
            new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [
              new TableRow({ children: [h.lbl('#', 1000), h.lbl('Name', 5000), h.lbl('Signed', 3360)] }),
              ...attendees.map((a, i) => new TableRow({ children: [
                h.val(String(i + 1), 1000),
                h.val(typeof a === 'string' ? a : (a.name || ''), 5000),
                h.val(a.signed ? '✓' : '', 3360)
              ] }))
            ] }),
            ...(t.notes ? [
              new Paragraph({ spacing: { before: 200 } }),
              h.sh('NOTES'),
              new Paragraph({ children: [new TextRun({ text: t.notes, font: 'Arial', size: 20 })], spacing: { after: 200 } })
            ] : [])
          ]
        }]
      });
      const buf = await docx.Packer.toBuffer(doc);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="Toolbox-Talk-${t.id}.docx"`);
      res.send(buf);
    } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // ═══════ SITE TEMPLATES ═══════
  app.get('/api/site-templates/:key/docx', authenticate, async (req, res) => {
    try {
      const h = docxHelpers();
      const key = req.params.key;
      const templates = {
        'risk-assessment': {
          title: 'Risk Assessment',
          docType: 'Risk Assessment Form',
          sections: [
            { header: 'PROJECT DETAILS', rows: [['Project Name',''],['Site Address',''],['Client',''],['Assessment Date',''],['Assessor',''],['Review Date','']] },
            { header: 'HAZARD IDENTIFICATION & RISK CONTROL', table: { heads: ['Hazard','Who at Risk','Existing Controls','Severity (1-5)','Likelihood (1-5)','Risk Rating','Additional Controls','Residual Risk'], rows: Array(8).fill(['','','','','','','','']) } },
            { header: 'SIGN-OFF', rows: [['Assessed By',''],['Signature',''],['Date',''],['Approved By',''],['Signature',''],['Date','']] },
          ]
        },
        'method-statement': {
          title: 'Method Statement',
          docType: 'Method Statement',
          sections: [
            { header: 'PROJECT INFORMATION', rows: [['Project Name',''],['Site Address',''],['Client',''],['Document Ref',''],['Revision',''],['Date','']] },
            { header: 'SCOPE OF WORKS', freeText: true, lines: 6 },
            { header: 'SEQUENCE OF OPERATIONS', table: { heads: ['Step','Activity','Hazards','Controls','Responsible Person'], rows: Array(10).fill(['','','','','']) } },
            { header: 'PLANT & EQUIPMENT', freeText: true, lines: 4 },
            { header: 'PPE REQUIREMENTS', freeText: true, lines: 3 },
            { header: 'EMERGENCY PROCEDURES', freeText: true, lines: 4 },
            { header: 'SIGN-OFF', rows: [['Prepared By',''],['Date',''],['Approved By',''],['Date','']] },
          ]
        },
        'permit-to-work': {
          title: 'Permit to Work',
          docType: 'Permit to Work',
          sections: [
            { header: 'PERMIT DETAILS', rows: [['Permit Number',''],['Permit Type',''],['Date Issued',''],['Valid From',''],['Valid To',''],['Location',''],['Description of Work','']] },
            { header: 'HAZARDS IDENTIFIED', freeText: true, lines: 4 },
            { header: 'PRECAUTIONS REQUIRED', freeText: true, lines: 4 },
            { header: 'PPE REQUIREMENTS', freeText: true, lines: 3 },
            { header: 'ISOLATION DETAILS', rows: [['Isolation Point(s)',''],['Isolation Method',''],['Isolated By',''],['Proved Dead By','']] },
            { header: 'AUTHORISATION', rows: [['Issued By',''],['Signature',''],['Accepted By',''],['Signature',''],['Date / Time','']] },
            { header: 'PERMIT CANCELLATION', rows: [['Work Completed',''],['Area Left Safe',''],['Cancelled By',''],['Signature',''],['Date / Time','']] },
          ]
        },
        'inspection-checklist': {
          title: 'Site Inspection Checklist',
          docType: 'Inspection Checklist',
          sections: [
            { header: 'INSPECTION DETAILS', rows: [['Site / Project',''],['Date',''],['Inspector',''],['Area Inspected','']] },
            { header: 'INSPECTION ITEMS', table: { heads: ['Item','Yes','No','N/A','Comments'], rows: [
              ['PPE being worn correctly','','','',''],['Housekeeping acceptable','','','',''],['Access/egress clear','','','',''],
              ['Fire extinguishers accessible','','','',''],['First aid kit available','','','',''],['Scaffold tagged and safe','','','',''],
              ['Edge protection in place','','','',''],['Electrical leads in good condition','','','',''],['COSHH storage correct','','','',''],
              ['Welfare facilities clean','','','',''],['Signage displayed','','','',''],['Waste segregated correctly','','','',''],
              ['','','','',''],['','','','',''],['','','','',''],
            ] } },
            { header: 'ACTIONS REQUIRED', table: { heads: ['Action','Responsible Person','Due Date','Completed'], rows: Array(6).fill(['','','','']) } },
            { header: 'SIGN-OFF', rows: [['Inspector Signature',''],['Date','']] },
          ]
        },
        'hot-work-permit': {
          title: 'Hot Work Permit',
          docType: 'Hot Work Permit',
          sections: [
            { header: 'PERMIT DETAILS', rows: [['Permit Number',''],['Date',''],['Location',''],['Description of Hot Work',''],['Equipment to be Used','']] },
            { header: 'PRE-WORK CHECKS', table: { heads: ['Check','Yes','No','N/A'], rows: [
              ['Area cleared of combustible materials','','',''],['Fire extinguisher available at work point','','',''],
              ['Fire watch person assigned','','',''],['Smoke/heat detectors isolated (with permit)','','',''],
              ['Combustible floors protected','','',''],['Flammable liquids/gases removed','','',''],
              ['Adjacent areas checked','','',''],['Ventilation adequate','','',''],
            ] } },
            { header: 'AUTHORISATION', rows: [['Issued By',''],['Signature',''],['Date / Time',''],['Accepted By',''],['Signature','']] },
            { header: 'FIRE WATCH', rows: [['Fire Watch Duration (min 60 mins after)',''],['Fire Watch Person',''],['All Clear Confirmed',''],['Signature',''],['Date / Time','']] },
          ]
        },
        'project-handover': {
          title: 'Project Handover Form',
          docType: 'Project Handover',
          sections: [
            { header: 'PROJECT DETAILS', rows: [['Project Name',''],['Client',''],['Site Address',''],['Contract Value',''],['Start Date',''],['Completion Date',''],['ManProjects Project Manager','']] },
            { header: 'HANDOVER CHECKLIST', table: { heads: ['Item','Completed','N/A','Comments'], rows: [
              ['O&M Manuals provided','','',''],['As-built drawings issued','','',''],['Test certificates provided','','',''],
              ['Commissioning records issued','','',''],['Spare parts/keys handed over','','',''],['Training provided to client','','',''],
              ['Defects/snags list completed','','',''],['Building log book updated','','',''],['Warranties issued','','',''],
              ['Final account agreed','','',''],
            ] } },
            { header: 'CLIENT ACCEPTANCE', rows: [['Client Name',''],['Signature',''],['Date',''],['ManProjects Representative',''],['Signature',''],['Date','']] },
          ]
        },
        'commissioning-record': {
          title: 'Commissioning Record',
          docType: 'Commissioning Record',
          sections: [
            { header: 'PROJECT INFORMATION', rows: [['Project Name',''],['Site Address',''],['System/Equipment',''],['Manufacturer',''],['Model/Serial No',''],['Location/Zone',''],['Date Commissioned','']] },
            { header: 'PRE-COMMISSIONING CHECKS', table: { heads: ['Check','Pass','Fail','N/A','Comments'], rows: [
              ['Installation complete','','','',''],['Visual inspection satisfactory','','','',''],
              ['Electrical connections verified','','','',''],['Fixings secure','','','',''],
              ['Labelling complete','','','',''],['Access for maintenance confirmed','','','',''],
              ['','','','',''],['','','','',''],
            ] } },
            { header: 'TEST RESULTS', table: { heads: ['Test','Expected Value','Measured Value','Pass/Fail'], rows: Array(8).fill(['','','','']) } },
            { header: 'SIGN-OFF', rows: [['Commissioned By',''],['Signature',''],['Date',''],['Witnessed By',''],['Signature',''],['Date','']] },
          ]
        },
        'daily-site-diary': {
          title: 'Daily Site Diary',
          docType: 'Daily Site Diary',
          sections: [
            { header: 'SITE DETAILS', rows: [['Project Name',''],['Site Address',''],['Date',''],['Weather Conditions',''],['Temperature (approx)',''],['Completed By','']] },
            { header: 'PERSONNEL ON SITE', table: { heads: ['Name','Company','Trade/Role','Hours'], rows: Array(10).fill(['','','','']) } },
            { header: 'WORK CARRIED OUT TODAY', freeText: true, lines: 8 },
            { header: 'MATERIALS DELIVERED', freeText: true, lines: 4 },
            { header: 'VISITORS', table: { heads: ['Name','Company','Purpose','Time In','Time Out'], rows: Array(4).fill(['','','','','']) } },
            { header: 'ISSUES / DELAYS', freeText: true, lines: 4 },
            { header: 'SIGN-OFF', rows: [['Site Manager Signature',''],['Date','']] },
          ]
        },
        'db-schedule': {
          title: 'DB Schedule',
          docType: 'Distribution Board Schedule',
          custom: true
        },
      };

      const tmpl = templates[key];
      if (!tmpl) return res.status(404).json({ error: 'Template not found' });

      // ── Custom DB Schedule template ──
      if (tmpl.custom && key === 'db-schedule') {
        const colWidths = [700, 900, 900, 900, 3060, 1100, 900, 900];
        const colHeads = ['Cct No','Cct Ref','MCB Rating (A)','MCB Type','Supply/ng','Cable Type','Cable Size','CPC Size'];
        const headerRow = new TableRow({ children: colHeads.map((head, i) =>
          new TableCell({ borders: h.bds, width: { size: colWidths[i], type: WidthType.DXA },
            shading: { fill: "8B1A1A", type: ShadingType.CLEAR }, margins: h.cm,
            children: [new Paragraph({ children: [new TextRun({ text: head, bold: true, font: "Arial", size: 16, color: "FFFFFF" })] })] })
        ) });
        const dataRows = Array(30).fill(null).map((_, idx) =>
          new TableRow({ children: colWidths.map((w) =>
            new TableCell({ borders: h.bds, width: { size: w, type: WidthType.DXA }, margins: h.cm,
              children: [new Paragraph({ children: [new TextRun({ text: ' ', font: "Arial", size: 16 })] })] })
          ) })
        );

        const children = [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200, after: 0 },
            children: [new TextRun({ text: "MANPROJECTS LTD", bold: true, font: "Arial", size: 32, color: h.maroon })] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 200 },
            children: [new TextRun({ text: "DISTRIBUTION BOARD SCHEDULE", bold: true, font: "Arial", size: 24, color: h.grey })] }),

          h.sh("BOARD DETAILS"),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [
            new TableRow({ children: [h.lbl("DB-Ref", 2340), h.val('', 2340), h.lbl("Location", 2340), h.val('', 2340)] }),
            new TableRow({ children: [h.lbl("Board Size & Rating", 2340), h.val('', 2340), h.lbl("Manufacturer", 2340), h.val('', 2340)] }),
            new TableRow({ children: [h.lbl("Supply Cable Ref", 2340), h.val('', 2340), h.lbl("PFC (kA)", 2340), h.val('', 2340)] }),
            new TableRow({ children: [h.lbl("Project / Site", 2340), h.val('', 2340), h.lbl("Date", 2340), h.val('', 2340)] }),
            new TableRow({ children: [h.lbl("Pod Room", 2340), h.val('', 2340), h.lbl("ZDB ID", 2340), h.val('', 2340)] }),
          ] }),

          h.sh("CIRCUIT SCHEDULE"),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [headerRow, ...dataRows] }),

          h.sh("NOTES"),
          ...Array(4).fill(null).map(() => new Paragraph({
            spacing: { after: 80 },
            border: { bottom: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC", space: 6 } },
            children: [new TextRun({ text: ' ', font: "Arial", size: 20 })]
          })),

          h.sh("SIGN-OFF"),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [
            new TableRow({ children: [h.lbl("Completed By", h.pw/2), h.val('', h.pw/2)] }),
            new TableRow({ children: [h.lbl("Signature", h.pw/2), h.val('', h.pw/2)] }),
            new TableRow({ children: [h.lbl("Date", h.pw/2), h.val('', h.pw/2)] }),
            new TableRow({ children: [h.lbl("Checked By", h.pw/2), h.val('', h.pw/2)] }),
            new TableRow({ children: [h.lbl("Date", h.pw/2), h.val('', h.pw/2)] }),
          ] }),
        ];

        const doc = new Document({
          styles: { default: { document: { run: { font: 'Arial', size: 22 } } } },
          sections: [{
            properties: { ...h.pageProps, page: { ...h.pageProps.page, size: { width: 16838, height: 11906 }, margin: { top: 1000, right: 1000, bottom: 1000, left: 1000 } } },
            headers: h.mkHeader('DB Schedule'),
            footers: h.mkFooter('Distribution Board Schedule'),
            children
          }]
        });
        const buf = await Packer.toBuffer(doc);
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
        res.setHeader('Content-Disposition', 'attachment; filename="ManProjects-DB-Schedule.docx"');
        return res.send(buf);
      }

      const children = [h.sh(tmpl.title.toUpperCase())];

      for (const sec of tmpl.sections) {
        children.push(h.sh(sec.header));

        if (sec.rows) {
          // Key-value pair rows
          const halfW = h.pw / 2;
          const tableRows = sec.rows.map(([label, value]) =>
            new TableRow({ children: [h.lbl(label, halfW), h.val(value || ' ', halfW)] })
          );
          children.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: tableRows }));
        }

        if (sec.table) {
          // Full table with headers
          const colW = Math.floor(h.pw / sec.table.heads.length);
          const headerRow = new TableRow({ children: sec.table.heads.map(head =>
            new TableCell({ borders: h.bds, width: { size: colW, type: WidthType.DXA },
              shading: { fill: "8B1A1A", type: ShadingType.CLEAR }, margins: h.cm,
              children: [new Paragraph({ children: [new TextRun({ text: head, bold: true, font: "Arial", size: 18, color: "FFFFFF" })] })] })
          ) });
          const dataRows = sec.table.rows.map(row =>
            new TableRow({ children: row.map(cell =>
              new TableCell({ borders: h.bds, width: { size: colW, type: WidthType.DXA }, margins: h.cm,
                children: [new Paragraph({ children: [new TextRun({ text: cell || ' ', font: "Arial", size: 18 })] })] })
            ) })
          );
          children.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [headerRow, ...dataRows] }));
        }

        if (sec.freeText) {
          // Empty lined area for writing
          for (let i = 0; i < (sec.lines || 4); i++) {
            children.push(new Paragraph({
              spacing: { after: 80 },
              border: { bottom: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC", space: 6 } },
              children: [new TextRun({ text: ' ', font: "Arial", size: 20 })]
            }));
          }
        }
      }

      const doc = new Document({
        styles: { default: { document: { run: { font: 'Arial', size: 22 } } } },
        sections: [{
          properties: h.pageProps,
          headers: h.mkHeader(tmpl.title),
          footers: h.mkFooter(tmpl.docType),
          children
        }]
      });

      const buf = await Packer.toBuffer(doc);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      res.setHeader('Content-Disposition', `attachment; filename="ManProjects-${tmpl.title.replace(/\s+/g, '-')}.docx"`);
      res.send(buf);
    } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // ═══════ PROJECTS ═══════
  // List all projects (admin)
  app.get('/api/projects', authenticate, adminOnly, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT p.*, u.full_name as created_by_name FROM projects p LEFT JOIN users u ON p.created_by = u.id ORDER BY p.created_at DESC');
      res.json(rows);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // Get single project
  app.get('/api/projects/:id', authenticate, adminOnly, async (req, res) => {
    try {
      const { rows } = await pool.query('SELECT p.*, u.full_name as created_by_name FROM projects p LEFT JOIN users u ON p.created_by = u.id WHERE p.id = $1', [req.params.id]);
      if (rows.length === 0) return res.status(404).json({ error: 'Project not found' });
      res.json(rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // Create project
  app.post('/api/projects', authenticate, adminOnly, async (req, res) => {
    try {
      const d = req.body;
      const { rows } = await pool.query(
        'INSERT INTO projects (name, client_name, site_address, status, start_date, end_date, description, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',
        [d.name, d.client_name, d.site_address || null, d.status || 'active', d.start_date || null, d.end_date || null, d.description || null, req.user.id]
      );
      res.json(rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // Update project
  app.put('/api/projects/:id', authenticate, adminOnly, async (req, res) => {
    try {
      const d = req.body;
      const { rows } = await pool.query(
        'UPDATE projects SET name=$1, client_name=$2, site_address=$3, status=$4, start_date=$5, end_date=$6, description=$7, updated_at=CURRENT_TIMESTAMP WHERE id=$8 RETURNING *',
        [d.name, d.client_name, d.site_address || null, d.status || 'active', d.start_date || null, d.end_date || null, d.description || null, req.params.id]
      );
      if (rows.length === 0) return res.status(404).json({ error: 'Project not found' });
      res.json(rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
  });

  // Delete project
  app.delete('/api/projects/:id', authenticate, adminOnly, async (req, res) => {
    try {
      await pool.query('DELETE FROM projects WHERE id = $1', [req.params.id]);
      res.json({ message: 'Project deleted' });
    } catch(e) { res.status(500).json({ error: e.message }); }
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
