/**
 * Data Sync Script
 *
 * Fetches real data from DadosJusBr API and populates the local SQLite database.
 *
 * Usage:
 *   npx tsx scripts/sync-data.ts                         # Sync latest available month
 *   npx tsx scripts/sync-data.ts --year 2024             # Sync all 12 months of 2024
 *   npx tsx scripts/sync-data.ts --year 2024 --month 6  # Sync specific month
 *   npx tsx scripts/sync-data.ts --all                  # Sync from 2024 to current month
 *   npx tsx scripts/sync-data.ts --seed                 # Seed with mock data (dev only)
 *   npx tsx scripts/sync-data.ts --force                 # Re-sync even if data exists
 */

import path from "path";
import fs from "fs";
import Database from "better-sqlite3";

// Ensure data directory exists
const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const DB_PATH = path.join(dataDir, "extrateto.db");
const TETO = 46366.19;

// Real DadosJusBr API endpoint (uiapi/v2/download returns CSV)
const DADOSJUSBR_DOWNLOAD = "https://api.dadosjusbr.org/uiapi/v2/download";

const sqlite = new Database(DB_PATH);
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("foreign_keys = ON");

// Create tables
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS membros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    cargo TEXT NOT NULL,
    orgao TEXT NOT NULL,
    estado TEXT NOT NULL,
    remuneracao_base REAL NOT NULL,
    verbas_indenizatorias REAL NOT NULL,
    direitos_eventuais REAL NOT NULL,
    direitos_pessoais REAL NOT NULL,
    remuneracao_total REAL NOT NULL,
    acima_teto REAL NOT NULL,
    percentual_acima_teto REAL NOT NULL,
    abate_teto REAL NOT NULL DEFAULT -1,
    mes_referencia TEXT NOT NULL,
    ano_referencia INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS historico_mensal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    membro_id INTEGER NOT NULL REFERENCES membros(id),
    mes TEXT NOT NULL,
    remuneracao_base REAL NOT NULL,
    remuneracao_total REAL NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    orgao TEXT NOT NULL,
    mes_referencia TEXT NOT NULL,
    total_membros INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    synced_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_membros_estado ON membros(estado);
  CREATE INDEX IF NOT EXISTS idx_membros_orgao ON membros(orgao);
  CREATE INDEX IF NOT EXISTS idx_membros_cargo ON membros(cargo);
  CREATE INDEX IF NOT EXISTS idx_membros_nome ON membros(nome);
  CREATE INDEX IF NOT EXISTS idx_membros_remuneracao ON membros(remuneracao_total DESC);
  CREATE INDEX IF NOT EXISTS idx_membros_acima_teto ON membros(acima_teto DESC);
  CREATE INDEX IF NOT EXISTS idx_membros_percentual ON membros(percentual_acima_teto DESC);
  CREATE INDEX IF NOT EXISTS idx_membros_mes ON membros(mes_referencia);
  CREATE INDEX IF NOT EXISTS idx_membros_ano ON membros(ano_referencia);
  CREATE INDEX IF NOT EXISTS idx_historico_membro ON historico_mensal(membro_id);
  CREATE INDEX IF NOT EXISTS idx_historico_mes ON historico_mensal(mes);
  CREATE INDEX IF NOT EXISTS idx_synclog_status ON sync_log(status);
  CREATE INDEX IF NOT EXISTS idx_synclog_orgao ON sync_log(orgao);
  CREATE VIRTUAL TABLE IF NOT EXISTS membros_fts USING fts5(
    nome, cargo, orgao,
    content='membros',
    content_rowid='id'
  );
`);

// Migration: add abate_teto column for existing databases
try {
  sqlite.exec(`ALTER TABLE membros ADD COLUMN abate_teto REAL NOT NULL DEFAULT -1`);
} catch { /* Column already exists */ }

const insertMembro = sqlite.prepare(`
  INSERT INTO membros (nome, cargo, orgao, estado, remuneracao_base, verbas_indenizatorias,
    direitos_eventuais, direitos_pessoais, remuneracao_total, acima_teto,
    percentual_acima_teto, abate_teto, mes_referencia, ano_referencia)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertSyncLog = sqlite.prepare(`
  INSERT INTO sync_log (orgao, mes_referencia, total_membros, status, error_message)
  VALUES (?, ?, ?, ?, ?)
`);

// All organs available in DadosJusBr
const ORGAOS = [
  "tjac", "tjal", "tjam", "tjap", "tjba", "tjce", "tjdft", "tjes",
  "tjgo", "tjma", "tjmg", "tjms", "tjmt", "tjpa", "tjpb", "tjpe",
  "tjpi", "tjpr", "tjrj", "tjrn", "tjro", "tjrr", "tjrs", "tjsc",
  "tjse", "tjsp", "tjto",
  // Tribunais Regionais Federais
  "trf1", "trf2", "trf3", "trf4", "trf5", "trf6",
  // MPs estaduais
  "mppb", "mpac", "mpal", "mpam", "mpap", "mpba", "mpce",
  "mpes", "mpgo", "mpma", "mpmg", "mpms", "mpmt", "mppa", "mppe",
  "mppi", "mppr", "mprj", "mprn", "mpro", "mprr", "mprs", "mpsc",
  "mpse", "mpsp", "mpto",
  // MPs federais (ramos do MPU)
  "mpf", "mpt", "mpm", "mpdft",
];

// Maps organ API id to readable name
function mapOrgaoId(id: string): string {
  // Federal MP branches (ramos do MPU)
  const federalMPs: Record<string, string> = {
    mpf: "MPF",
    mpt: "MPT",
    mpm: "MPM",
    mpdft: "MPDFT",
  };
  if (federalMPs[id]) return federalMPs[id];

  // TRFs
  const trf = id.match(/^trf(\d)$/i);
  if (trf) return `TRF-${trf[1]}`;

  const prefix = id.substring(0, 2).toUpperCase();
  const suffix = id.substring(2).toUpperCase();
  if (prefix === "TJ") return suffix === "DFT" ? "TJ-DF" : `TJ-${suffix}`;
  if (prefix === "MP") return `MP-${suffix}`;
  return id.toUpperCase();
}

// Maps organ API id to state abbreviation (used for non-federal organs)
function mapEstado(orgaoId: string): string {
  const suffix = orgaoId.substring(2).toUpperCase();
  if (suffix === "DFT") return "DF";
  return suffix;
}

// TRT regions → state of headquarters (for MPT lotação)
const TRT_REGIAO_ESTADO: Record<string, string> = {
  "1": "RJ", "2": "SP", "3": "MG", "4": "RS", "5": "BA",
  "6": "PE", "7": "CE", "8": "PA", "9": "PR", "10": "DF",
  "11": "AM", "12": "SC", "13": "PB", "14": "RO", "15": "SP",
  "16": "MA", "17": "ES", "18": "GO", "19": "AL", "20": "SE",
  "21": "RN", "22": "PI", "23": "MT", "24": "MS",
};

// PRR regions → state of headquarters (for MPF lotação)
const PRR_REGIAO_ESTADO: Record<string, string> = {
  "1": "DF", "2": "RJ", "3": "SP", "4": "RS", "5": "PE", "6": "MG",
};

// Known PRM cities → state (for MPF municipal offices)
const PRM_CIDADE_ESTADO: Record<string, string> = {
  "ALAGOINHAS": "BA", "ALTAMIRA-PA": "PA", "ANAPOLIS": "GO",
  "ANGRA REIS": "RJ", "ARACATUBA": "SP", "ARAGUAINA": "TO",
  "ARAPIRACA": "AL", "ARARAQUARA": "SP", "ASSIS": "SP",
  "B.DO GARÇAS": "MT", "B.GONCALVES": "RS", "BACABAL": "MA",
  "BAGÉ": "RS", "BARREIRAS": "BA", "BAURU": "SP",
  "BLUMENAU": "SC", "BRAGANÇA": "PA", "C. MOURAO": "PR",
  "C.GRANDE": "MS", "CACERES": "MT", "CAMPINAS": "SP",
  "CAMPOS": "RJ", "CARAGUATA": "SP", "CARUARU": "PE",
  "CASCAVEL": "PR", "CAXIAS": "MA", "CAXIAS SUL": "RS",
  "CAÇADOR": "SC", "CHAPECO": "SC", "CORRENTE": "PI",
  "CORUMBA": "MS", "CRICIUMA": "SC", "CRUZ ALTA": "RS",
  "DIVINÓPOLIS": "MG", "DOURADOS": "MS", "ERECHIM/P.M": "RS",
  "EUNAPOLIS": "BA", "F.BELTRAO": "PR", "FEIRA": "BA",
  "FLORIANO": "PI", "FOZ": "PR", "FRANCA": "SP",
  "GARANHUNS": "PE", "GOV VALADAR": "MG", "GUANAMBI": "BA",
  "GUARULHOS": "SP", "ILHEUS": "BA", "IMPERATRIZ": "MA",
  "IRECÊ": "BA", "ITAJAI": "SC", "ITAPERUNA": "RJ",
  "ITAPEVA": "SP", "J. NORTE": "CE", "JALES": "SP",
  "JAU": "SP", "JEQUIE": "BA", "JI PARANÁ": "RO",
  "JOINVILLE": "SC", "JUIZ FORA": "MG", "JUNDIAI": "SP",
  "LAGES": "SC", "LIMOEIRO": "PE", "LONDRINA": "PR",
  "LUZIANIA": "GO", "M. CLAROS": "MG", "MACAE": "RJ",
  "MARABA": "PA", "MARINGA": "PR", "MARÍLIA": "SP",
  "MOSSORO": "RN", "N.FRIBURGO": "RJ", "N.HAMBURGO": "RS",
  "NITEROI": "RJ", "OURINHOS": "SP", "P.FUNDO": "RS",
  "P.GROSSA": "PR", "P.PRUDENTE": "SP", "PARAGOMINAS": "PA",
  "PARNAIBA": "PI", "PATO BCO": "PR", "PELOTAS-RS": "RS",
  "PETROLINA": "PE", "PETROPOLIS": "RJ", "PICOS-PI": "PI",
  "PIRACICABA": "SP", "R.GRANDE": "RS", "R.PRETO": "SP",
  "REDENÇÃO": "PA", "RESENDE-RJ": "RJ", "RONDONOPOLI": "MT",
  "S. TALHADA": "PE", "S.ANGELO": "RS", "S.BERNARDO": "SP",
  "S.CARLOS": "SP", "S.GONÇALO": "RJ", "S.J. MERITI": "RJ",
  "S.J.CAMP": "SP", "S.J.DEL REI": "MG", "S.J.R.PRETO": "SP",
  "S.LIVRAMENT": "RS", "S.MARIA": "RS", "S.MIGUEL": "RN",
  "S.P.ALDEIA": "RJ", "S.R.NONATO": "PI", "SANTA ROSA": "RS",
  "SANTAREM": "PA", "SANTOS": "SP", "SETE LAGOAS": "MG",
  "SINOP": "MT", "SOBRAL": "CE", "SOROCABA": "SP",
  "SOUSA": "PB", "STA CRUZ SU": "RS", "TABATINGA": "AM",
  "TAUBATE": "SP", "TEFÉ": "AM", "TRES LAGOAS": "MS",
  "TUBARAO": "SC", "TUCURUI": "PA", "UBERABA": "MG",
  "UBERLANDIA": "MG", "UMUARAMA": "PR", "URUGUAIANA": "RS",
  "V.REDONDA": "RJ", "VARGINHA": "MG", "VIT. CONQUI": "BA",
};

/**
 * Extracts state from the lotação field for federal MP organs.
 * Falls back to "DF" if the lotação cannot be parsed.
 */
function mapEstadoFromLotacao(orgaoId: string, lotacao: string): string {
  if (!lotacao) return "DF";
  const lot = lotacao.trim().toUpperCase();

  if (orgaoId === "mpdft") return "DF";

  // TRFs — each has a different lotação format
  if (orgaoId.startsWith("trf")) {
    const trfSede: Record<string, string> = {
      trf1: "DF", trf2: "RJ", trf3: "SP", trf4: "RS", trf5: "PE", trf6: "MG",
    };

    // TRF-1: "VARA1/SSJCFS/SJBA" → state is the LAST "/SJ{XX}" segment
    if (orgaoId === "trf1") {
      const sjMatch = lot.match(/\/SJ([A-Z]{2})(?:$|\/)/);
      if (sjMatch) return sjMatch[1];
      // Simple fallback: last 2 chars after last "SJ"
      const lastSJ = lot.lastIndexOf("/SJ");
      if (lastSJ >= 0 && lastSJ + 4 <= lot.length) return lot.substring(lastSJ + 3, lastSJ + 5);
      return trfSede[orgaoId];
    }

    // TRF-2: city names → map known ES cities, default RJ
    if (orgaoId === "trf2") {
      const esCities = ["VITÓRIA", "VITORIA", "CACHOEIRO", "LINHARES", "SERRA", "COLATINA", "GUARAPARI", "SÃO MATEUS", "SAO MATEUS", "ARACRUZ", "CARIACICA", "VILA VELHA"];
      if (esCities.some(c => lot.includes(c))) return "ES";
      return "RJ";
    }

    // TRF-3: covers SP and MS only. "SJSP"/"SJMS" are state codes; "SJCAMPOS"/"SJRIO PRETO" are SP cities
    if (orgaoId === "trf3") {
      // Only match exact state codes SJSP or SJMS (not SJCAMPOS, SJRIO PRETO, etc.)
      if (lot.includes("SJMS")) return "MS";
      const msCities = ["CAMPO GRANDE", "CORUMBA", "CORUMBÁ", "DOURADOS", "COXIM", "NAVIRAI", "NAVIRAÍ", "PONTA PORA", "PONTA PORÃ", "TRES LAGOAS", "TRÊS LAGOAS"];
      if (msCities.some(c => lot.includes(c))) return "MS";
      return "SP";
    }

    // TRF-4: "PRCTB01" → first 2 chars = state (PR, SC, RS)
    if (orgaoId === "trf4") {
      const prefix2 = lot.substring(0, 2);
      if (["PR", "SC", "RS"].includes(prefix2)) return prefix2;
      return "RS";
    }

    // TRF-5: "10ª VARA - FORTALEZA-CE" → state after last "-"
    if (orgaoId === "trf5") {
      const stateMatch = lot.match(/-([A-Z]{2})$/);
      if (stateMatch) return stateMatch[1];
      return "PE";
    }

    // TRF-6: only MG
    return trfSede[orgaoId] || "DF";
  }

  if (orgaoId === "mpf") {
    // PR-XX → state code (e.g. PR-SP → SP)
    const prMatch = lot.match(/^PR-([A-Z]{2})$/);
    if (prMatch) return prMatch[1];

    // PRRXª REGIÃO → region headquarters
    const prrMatch = lot.match(/PRR(\d)/);
    if (prrMatch) return PRR_REGIAO_ESTADO[prrMatch[1]] || "DF";

    // PRM-CITY → lookup city
    const prmMatch = lot.match(/^PRM-(.+)$/);
    if (prmMatch) return PRM_CIDADE_ESTADO[prmMatch[1]] || "DF";

    // PGR, GABPGR, ESMPU, OFAMOC, OFAMOR → DF
    return "DF";
  }

  if (orgaoId === "mpt") {
    // PRT DA Xª REGIÃO → TRT region headquarters
    const trtMatch = lot.match(/(\d+)[ªº]\s*REGI/);
    if (trtMatch) return TRT_REGIAO_ESTADO[trtMatch[1]] || "DF";

    // PROCURADORIA GERAL DO TRABALHO → DF
    return "DF";
  }

  if (orgaoId === "mpm") {
    // "... EM CITY/STATE" → extract state after "/"
    const slashMatch = lot.match(/\/([A-Z]{2})\s*$/);
    if (slashMatch) return slashMatch[1];

    // PROCURADORIA-GERAL, SECRETARIA, GABINETE → DF
    return "DF";
  }

  return "DF";
}

// Maps cargo string to our categories
function mapCargo(role: string, orgaoId: string): string {
  const r = role.toLowerCase();
  if (r.includes("desembargador")) return "Desembargador(a)";
  if (r.includes("juiz") || r.includes("juíz")) return "Juiz(a)";
  if (r.includes("ministro")) return "Ministro(a)";
  if (r.includes("promotor")) return "Promotor(a)";
  if (r.includes("procurador")) return "Procurador(a)";
  if (r.includes("defensor")) return "Defensor(a) Público(a)";
  if (orgaoId.startsWith("mp")) return "Promotor(a)";
  return "Juiz(a)";
}

// Parse value that can be in two formats:
// 1. Brazilian format: "33.924,92" or "33924,92" → R$ 33,924.92
// 2. Centavos integer: 3392492 (no comma, no dot) → R$ 33,924.92
function parseValor(valor: string): number {
  if (!valor) return 0;
  const trimmed = valor.trim();
  if (trimmed === "0" || trimmed === "") return 0;

  // If contains comma → Brazilian format (comma = decimal separator)
  if (trimmed.includes(",")) {
    const cleaned = trimmed.replace(/\./g, "").replace(",", ".");
    const num = parseFloat(cleaned);
    return isNaN(num) ? 0 : num;
  }

  // If pure integer (no comma, no dot) → centavos, divide by 100
  if (/^-?\d+$/.test(trimmed)) {
    return parseInt(trimmed, 10) / 100;
  }

  // Fallback: try parsing as-is (e.g. "123.45" US format)
  const num = parseFloat(trimmed);
  return isNaN(num) ? 0 : num;
}

// Simple CSV parser that handles quoted fields with commas
function parseCSV(csv: string): Record<string, string>[] {
  const lines = csv.split("\n");
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const rows: Record<string, string>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] || "";
    }
    rows.push(row);
  }

  return rows;
}

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

interface MemberAgg {
  nome: string;
  cargo: string;
  orgao: string;
  estado: string;
  remuneracaoBase: number;
  verbasIndenizatorias: number;
  direitosEventuais: number;
  direitosPessoais: number;
  abateTeto: number;
}

/**
 * Downloads CSV data from DadosJusBr for a specific organ and month.
 * Returns aggregated member data.
 */
async function fetchOrgaoCSV(
  orgaoId: string,
  year: number,
  month: number
): Promise<MemberAgg[]> {
  const url = `${DADOSJUSBR_DOWNLOAD}?anos=${year}&meses=${month}&orgaos=${orgaoId}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${orgaoId}`);
  }

  const csv = await response.text();
  if (!csv || csv.length < 50) {
    throw new Error(`Empty response for ${orgaoId}`);
  }

  const rows = parseCSV(csv);
  if (rows.length === 0) {
    throw new Error(`No data rows for ${orgaoId}`);
  }

  // Aggregate by member name
  const members = new Map<string, MemberAgg>();
  const orgaoName = mapOrgaoId(orgaoId);
  const isFederalMP = ["mpf", "mpt", "mpm", "mpdft"].includes(orgaoId);
  const isTRF = /^trf\d$/i.test(orgaoId);
  const isFederalOrgan = isFederalMP || isTRF;
  const defaultEstado = isFederalOrgan ? "DF" : mapEstado(orgaoId);

  for (const row of rows) {
    const nome = row.nome?.trim();
    if (!nome) continue;

    const valor = parseValor(row.valor);
    const categoria = row.categoria_contracheque?.toLowerCase();
    const macro = row.desambiguacao_macro?.toLowerCase() || "";

    if (!members.has(nome)) {
      // For federal organs (MPs + TRFs), derive state from lotação field
      const estado = isFederalOrgan
        ? mapEstadoFromLotacao(orgaoId, row.lotacao || "")
        : defaultEstado;

      members.set(nome, {
        nome,
        cargo: mapCargo(row.cargo || "", orgaoId),
        orgao: orgaoName,
        estado,
        remuneracaoBase: 0,
        verbasIndenizatorias: 0,
        direitosEventuais: 0,
        direitosPessoais: 0,
        abateTeto: -1, // -1 = no data; >= 0 = rubric found
      });
    }

    const m = members.get(nome)!;

    if (categoria === "base") {
      m.remuneracaoBase += valor;
    } else if (categoria === "outras") {
      // Classify "outras" (benefits) into subcategories
      if (
        macro.includes("aux-") ||
        macro.includes("alimentacao") ||
        macro.includes("saude") ||
        macro.includes("moradia") ||
        macro.includes("transporte")
      ) {
        m.verbasIndenizatorias += valor;
      } else if (
        macro.includes("ferias") ||
        macro.includes("natalina") ||
        macro.includes("abono") ||
        macro.includes("licenca") ||
        macro.includes("diarias") ||
        macro.includes("pecunia")
      ) {
        m.direitosEventuais += valor;
      } else if (
        macro.includes("tempo-de-servico") ||
        macro.includes("gratificacao") ||
        macro.includes("substituicao")
      ) {
        m.direitosPessoais += valor;
      } else {
        // Default: classify as verbas indenizatórias
        m.verbasIndenizatorias += valor;
      }
    } else if (categoria === "descontos") {
      const detalhe = (row.detalhamento_contracheque || "").toLowerCase();
      if (detalhe.includes("teto") || macro.includes("teto")) {
        // Mark rubric as found (transition from -1 to 0), then accumulate
        if (m.abateTeto < 0) m.abateTeto = 0;
        m.abateTeto += Math.abs(valor);
      }
    }
  }

  return Array.from(members.values());
}

/**
 * Syncs a single organ: fetches CSV, aggregates, inserts into DB.
 */
async function syncOrgao(
  orgaoId: string,
  year: number,
  month: number
): Promise<number> {
  const mesRef = `${year}-${String(month).padStart(2, "0")}`;
  const orgaoName = mapOrgaoId(orgaoId);

  try {
    console.log(`  Fetching ${orgaoId}...`);
    const members = await fetchOrgaoCSV(orgaoId, year, month);

    if (members.length === 0) {
      console.log(`  - ${orgaoId}: no members`);
      insertSyncLog.run(orgaoName, mesRef, 0, "empty", null);
      return 0;
    }

    const insertMany = sqlite.transaction((data: MemberAgg[]) => {
      for (const m of data) {
        const total =
          m.remuneracaoBase +
          m.verbasIndenizatorias +
          m.direitosEventuais +
          m.direitosPessoais;
        const acima = Math.max(0, total - TETO);
        const percentual = acima > 0 ? (acima / TETO) * 100 : 0;

        insertMembro.run(
          m.nome,
          m.cargo,
          m.orgao,
          m.estado,
          m.remuneracaoBase,
          m.verbasIndenizatorias,
          m.direitosEventuais,
          m.direitosPessoais,
          total,
          acima,
          percentual,
          m.abateTeto,
          mesRef,
          year
        );
      }
    });

    insertMany(members);
    insertSyncLog.run(orgaoName, mesRef, members.length, "success", null);
    console.log(`  + ${orgaoId}: ${members.length} membros`);
    return members.length;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.log(`  x ${orgaoId}: ${msg}`);
    insertSyncLog.run(orgaoName, mesRef, 0, "error", msg);
    return 0;
  }
}

async function seedWithMockData() {
  console.log("Seeding database with mock data...");
  const { mockMembers } = await import("../src/data/mock-data");

  const insertMany = sqlite.transaction((members: typeof mockMembers) => {
    for (const m of members) {
      insertMembro.run(
        m.nome,
        m.cargo,
        m.orgao,
        m.estado,
        m.remuneracaoBase,
        m.verbasIndenizatorias,
        m.direitosEventuais,
        m.direitosPessoais,
        m.remuneracaoTotal,
        m.acimaTeto,
        m.percentualAcimaTeto,
        0, // abate_teto
        "2025-06",
        2025
      );
    }
  });

  insertMany(mockMembers);
  rebuildFTS();

  console.log(`Done: ${mockMembers.length} members seeded`);
}

async function syncAll(year: number, month: number, force: boolean) {
  const mesRef = `${year}-${String(month).padStart(2, "0")}`;
  console.log(`\nSyncing real data for ${mesRef} from DadosJusBr...\n`);

  // Check if already synced
  if (!force) {
    const existing = sqlite
      .prepare("SELECT COUNT(*) as count FROM membros WHERE mes_referencia = ?")
      .get(mesRef) as { count: number };

    if (existing.count > 0) {
      console.log(
        `Already have ${existing.count} records for ${mesRef}. Use --force to re-sync.\n`
      );
      return;
    }
  } else {
    // Delete existing data for this month
    sqlite
      .prepare("DELETE FROM membros WHERE mes_referencia = ?")
      .run(mesRef);
    console.log(`Cleared existing data for ${mesRef}.\n`);
  }

  let total = 0;
  let success = 0;
  let errors = 0;
  const concurrency = 3;

  // Process in batches
  for (let i = 0; i < ORGAOS.length; i += concurrency) {
    const batch = ORGAOS.slice(i, i + concurrency);
    const results = await Promise.allSettled(
      batch.map((orgao) => syncOrgao(orgao, year, month))
    );

    for (const result of results) {
      if (result.status === "fulfilled") {
        total += result.value;
        if (result.value > 0) success++;
      } else {
        errors++;
      }
    }

    // Rate limit: 500ms between batches
    if (i + concurrency < ORGAOS.length) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  console.log(`\n--- Sync Complete ---`);
  console.log(`Total members: ${total.toLocaleString()}`);
  console.log(`Successful organs: ${success}/${ORGAOS.length}`);
  console.log(`Failed organs: ${errors}`);
  console.log(`Month: ${mesRef}\n`);
}

function freshDb() {
  sqlite.exec(`
    DELETE FROM historico_mensal;
    DELETE FROM membros;
    DELETE FROM sync_log;
  `);
  sqlite.exec(`INSERT INTO membros_fts(membros_fts) VALUES('rebuild')`);
  console.log("Database cleared.");
}

function rebuildFTS() {
  console.log("Rebuilding FTS index...");
  sqlite.exec(`INSERT INTO membros_fts(membros_fts) VALUES('rebuild')`);
  console.log("FTS index rebuilt.\n");
}

// CLI
async function main() {
  const args = process.argv.slice(2);

  if (args.includes("--fresh")) {
    freshDb();
    return;
  }

  if (args.includes("--seed")) {
    await seedWithMockData();
    return;
  }

  const force = args.includes("--force");

  const yearIdx = args.indexOf("--year");
  const monthIdx = args.indexOf("--month");
  const allIdx = args.indexOf("--all");

  const now = new Date();
  // Default to 3 months ago (recent months often lack data)
  const defaultDate = new Date(now.getFullYear(), now.getMonth() - 3, 1);
  const defaultYear = defaultDate.getFullYear();
  const defaultMonth = defaultDate.getMonth() + 1;

  const hasYear = yearIdx >= 0 && !isNaN(parseInt(args[yearIdx + 1], 10));
  const hasMonth = monthIdx >= 0 && !isNaN(parseInt(args[monthIdx + 1], 10));
  const year = hasYear ? parseInt(args[yearIdx + 1], 10) : defaultYear;

  if (year < 2018 || year > 2030) {
    console.error(`Invalid year: ${args[yearIdx + 1]}. Must be between 2018 and 2030.`);
    process.exit(1);
  }

  // --year 2024 without --month: sync all 12 months of that year
  if (hasYear && !hasMonth) {
    console.log(`\nSyncing all 12 months of ${year}...\n`);
    for (let m = 1; m <= 12; m++) {
      await syncAll(year, m, force);
    }
    rebuildFTS();
    console.log(`\n--- Full Year Sync Complete ---\n`);
    return;
  }

  // --year 2024 --month 6: sync single month
  if (hasMonth) {
    const month = parseInt(args[monthIdx + 1], 10);
    if (isNaN(month) || month < 1 || month > 12) {
      console.error(`Invalid month: ${args[monthIdx + 1]}. Must be between 1 and 12.`);
      process.exit(1);
    }
    await syncAll(year, month, force);
    rebuildFTS();
    return;
  }

  // --all: sync from start to current (respect --year if provided)
  if (allIdx >= 0) {
    const startYear = hasYear ? year : 2024;
    const startMonth = 1;
    const endYear = now.getFullYear();
    const endMonth = now.getMonth() + 1;

    let y = startYear;
    let m = startMonth;
    while (y < endYear || (y === endYear && m <= endMonth)) {
      await syncAll(y, m, force);
      m++;
      if (m > 12) { m = 1; y++; }
    }
    rebuildFTS();
    return;
  }

  // No flags: sync default month
  await syncAll(year, defaultMonth, force);
  rebuildFTS();
}

main().catch(console.error).finally(() => sqlite.close());
