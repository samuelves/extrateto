/**
 * Data access layer that reads from SQLite database when available,
 * falling back to mock data for development.
 */

import { unstable_cache } from "next/cache";
import type { Member } from "./mock-data";
import { getKPIs } from "./mock-data";
import { TETO_CONSTITUCIONAL, type Cargo } from "@/lib/constants";

// Re-export types and functions that other modules need
export type { Member } from "./mock-data";
export { getKPIs } from "./mock-data";

const _cache = new Map<string, { members: Member[]; timestamp: number }>();
const CACHE_TTL = 60_000; // 1 minute

function rowToMember(row: Record<string, unknown>): Member {
  return {
    id: row.id as number,
    nome: row.nome as string,
    cargo: row.cargo as Cargo,
    orgao: row.orgao as string,
    estado: row.estado as string,
    remuneracaoBase: row.remuneracao_base as number,
    verbasIndenizatorias: row.verbas_indenizatorias as number,
    direitosEventuais: row.direitos_eventuais as number,
    direitosPessoais: row.direitos_pessoais as number,
    remuneracaoTotal: row.remuneracao_total as number,
    acimaTeto: row.acima_teto as number,
    percentualAcimaTeto: row.percentual_acima_teto as number,
    historico: [],
  };
}

function openDB() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const Database = require("better-sqlite3");
  const path = require("path");
  const fs = require("fs");

  const dbPath = path.join(process.cwd(), "data", "extrateto.db");
  if (!fs.existsSync(dbPath)) return null;

  const db = new Database(dbPath, { readonly: true });
  db.pragma("journal_mode = WAL");
  return db;
}

/**
 * Tries to load members from the SQLite database.
 * If mesReferencia is provided (e.g. "2024-06"), loads that month.
 * Otherwise loads the latest month.
 */
function loadFromDB(mesReferencia?: string): Member[] | null {
  try {
    const db = openDB();
    if (!db) return null;

    let targetMonth = mesReferencia;

    if (!targetMonth) {
      const latestMonth = db
        .prepare(
          "SELECT mes_referencia FROM membros ORDER BY mes_referencia DESC LIMIT 1"
        )
        .get() as { mes_referencia: string } | undefined;

      if (!latestMonth) {
        db.close();
        return null;
      }
      targetMonth = latestMonth.mes_referencia;
    }

    const rows = db
      .prepare(
        `SELECT * FROM membros
         WHERE mes_referencia = ?
         ORDER BY remuneracao_total DESC`
      )
      .all(targetMonth) as Record<string, unknown>[];

    db.close();

    if (rows.length === 0) return null;
    return rows.map(rowToMember);
  } catch {
    return null;
  }
}

/**
 * Returns all members — from DB if available, otherwise mock data.
 * Optionally pass mesReferencia (e.g. "2024-06") to load a specific month.
 * Cached for 1 minute to avoid repeated DB reads during rendering.
 */
export function getMembers(mesReferencia?: string): Member[] {
  const cacheKey = mesReferencia || "__latest__";
  const now = Date.now();
  const cached = _cache.get(cacheKey);
  if (cached && now - cached.timestamp < CACHE_TTL) {
    return cached.members;
  }

  const dbMembers = loadFromDB(mesReferencia);
  if (dbMembers && dbMembers.length > 0) {
    _cache.set(cacheKey, { members: dbMembers, timestamp: now });
    return dbMembers;
  }

  // Fallback to mock data
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { mockMembers } = require("./mock-data");
  _cache.set(cacheKey, { members: mockMembers, timestamp: now });
  return mockMembers;
}

/**
 * Returns available months from the database as { value: "2024-06", label: "Jun/2024" }[]
 * sorted from newest to oldest.
 */
export function getAvailableMonths(): { value: string; label: string }[] {
  try {
    const db = openDB();
    if (!db) return [];

    const rows = db
      .prepare(
        "SELECT DISTINCT mes_referencia FROM membros ORDER BY mes_referencia DESC"
      )
      .all() as { mes_referencia: string }[];

    db.close();

    const monthNames = [
      "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
      "Jul", "Ago", "Set", "Out", "Nov", "Dez",
    ];

    return rows.map((r) => {
      const [year, month] = r.mes_referencia.split("-");
      return {
        value: r.mes_referencia,
        label: `${monthNames[parseInt(month) - 1]}/${year}`,
      };
    });
  } catch {
    return [];
  }
}

/**
 * Returns available years from the database as { value: "2024", label: "2024" }[]
 * sorted from newest to oldest.
 */
export function getAvailableYears(): { value: string; label: string }[] {
  try {
    const db = openDB();
    if (!db) return [];

    const rows = db
      .prepare(
        "SELECT DISTINCT ano_referencia FROM membros ORDER BY ano_referencia DESC"
      )
      .all() as { ano_referencia: number }[];

    db.close();

    return rows.map((r) => ({
      value: String(r.ano_referencia),
      label: String(r.ano_referencia),
    }));
  } catch {
    return [];
  }
}

/**
 * Returns members aggregated by year. For each unique (nome, orgao) pair,
 * sums all monthly values across that year.
 */
export function getMembersByYear(year: string): Member[] {
  try {
    const db = openDB();
    if (!db) return [];

    const rows = db
      .prepare(
        `SELECT
          nome, cargo, orgao, estado,
          SUM(remuneracao_base) as remuneracao_base,
          SUM(verbas_indenizatorias) as verbas_indenizatorias,
          SUM(direitos_eventuais) as direitos_eventuais,
          SUM(direitos_pessoais) as direitos_pessoais,
          SUM(remuneracao_total) as remuneracao_total,
          SUM(acima_teto) as acima_teto,
          AVG(percentual_acima_teto) as percentual_acima_teto
        FROM membros
        WHERE ano_referencia = ?
        GROUP BY nome, orgao
        ORDER BY remuneracao_total DESC`
      )
      .all(parseInt(year)) as Record<string, unknown>[];

    db.close();

    if (rows.length === 0) return [];
    return rows.map((row, i) => ({
      ...rowToMember(row),
      id: i + 1,
    }));
  } catch {
    return [];
  }
}

/**
 * Returns the reference month string (e.g., "Jan/2024") for the loaded data.
 */
export function getDataMonth(mesReferencia?: string): string {
  const monthNames = [
    "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
  ];

  if (mesReferencia) {
    const [year, month] = mesReferencia.split("-");
    return `${monthNames[parseInt(month) - 1]}/${year}`;
  }

  try {
    const db = openDB();
    if (!db) return "Jun/2025";

    const result = db
      .prepare(
        "SELECT mes_referencia FROM membros ORDER BY mes_referencia DESC LIMIT 1"
      )
      .get() as { mes_referencia: string } | undefined;
    db.close();

    if (!result) return "Jun/2025";

    const [year, month] = result.mes_referencia.split("-");
    return `${monthNames[parseInt(month) - 1]}/${year}`;
  } catch {
    return "Jun/2025";
  }
}

export interface YearComparisonData {
  year: number;
  totalMembers: number;
  membersAboveTeto: number;
  totalAboveTeto: number;
  averageAboveTeto: number;
  averageTotalRemuneration: number;
  topOrgans: { orgao: string; total: number }[];
  topState: { estado: string; total: number } | null;
}

export interface Anomalia {
  nome: string;
  cargo: string;
  orgao: string;
  estado: string;
  mesAnterior: string;
  mesAtual: string;
  totalAnterior: number;
  totalAtual: number;
  variacaoAbs: number;
  variacaoPct: number;
}

/**
 * Detects salary anomalies: members with >200% salary spike between consecutive months.
 * Uses LAG window function for efficiency.
 */
export function getAnomalias(ano: number, minVariacaoPct = 200): Anomalia[] {
  try {
    const db = openDB();
    if (!db) return [];

    const anoAnterior = ano - 1;
    const anoStr = String(ano);
    const multiplicador = 1 + minVariacaoPct / 100;

    const rows = db
      .prepare(
        `WITH ordered AS (
          SELECT
            nome, cargo, orgao, estado,
            mes_referencia,
            remuneracao_total,
            LAG(mes_referencia) OVER (PARTITION BY nome, orgao ORDER BY mes_referencia) AS mes_anterior,
            LAG(remuneracao_total) OVER (PARTITION BY nome, orgao ORDER BY mes_referencia) AS total_anterior
          FROM membros
          WHERE ano_referencia IN (?, ?)
        )
        SELECT
          nome, cargo, orgao, estado,
          mes_anterior,
          mes_referencia AS mes_atual,
          total_anterior,
          remuneracao_total AS total_atual,
          remuneracao_total - total_anterior AS variacao_abs,
          ((remuneracao_total - total_anterior) / total_anterior) * 100 AS variacao_pct
        FROM ordered
        WHERE mes_anterior IS NOT NULL
          AND total_anterior >= 50000
          AND substr(mes_referencia, 1, 4) = ?
          AND remuneracao_total > total_anterior * ?
        ORDER BY variacao_abs DESC
        LIMIT 750`
      )
      .all(ano, anoAnterior, anoStr, multiplicador) as Record<string, unknown>[];

    db.close();

    return rows.map((r) => ({
      nome: r.nome as string,
      cargo: r.cargo as string,
      orgao: r.orgao as string,
      estado: r.estado as string,
      mesAnterior: r.mes_anterior as string,
      mesAtual: r.mes_atual as string,
      totalAnterior: r.total_anterior as number,
      totalAtual: r.total_atual as number,
      variacaoAbs: r.variacao_abs as number,
      variacaoPct: r.variacao_pct as number,
    }));
  } catch (err) {
    console.error("getAnomalias error:", err);
    return [];
  }
}

function getYearAggregates(year: number): YearComparisonData | null {
  try {
    const db = openDB();
    if (!db) return null;

    const rows = db
      .prepare(
        `SELECT
          COUNT(DISTINCT nome || '-' || orgao) as total_members,
          SUM(CASE WHEN acima_teto > 0 THEN 1 ELSE 0 END) as members_above_teto,
          SUM(acima_teto) as total_above_teto,
          AVG(CASE WHEN acima_teto > 0 THEN acima_teto END) as average_above_teto,
          AVG(remuneracao_total) as average_total_remuneration
        FROM membros
        WHERE ano_referencia = ?`
      )
      .get(year) as {
        total_members: number;
        members_above_teto: number;
        total_above_teto: number;
        average_above_teto: number;
        average_total_remuneration: number;
      } | undefined;

    const topOrgansRows = db
      .prepare(
        `SELECT orgao, SUM(acima_teto) as total
         FROM membros
         WHERE ano_referencia = ? AND acima_teto > 0
         GROUP BY orgao
         ORDER BY total DESC`
      )
      .all(year) as { orgao: string; total: number }[];

    const topStateRow = db
      .prepare(
        `SELECT estado, SUM(acima_teto) as total
         FROM membros
         WHERE ano_referencia = ? AND acima_teto > 0
         GROUP BY estado
         ORDER BY total DESC
         LIMIT 1`
      )
      .get(year) as { estado: string; total: number } | undefined;

    db.close();

    if (!rows) return null;

    return {
      year,
      totalMembers: rows.total_members || 0,
      membersAboveTeto: rows.members_above_teto || 0,
      totalAboveTeto: rows.total_above_teto || 0,
      averageAboveTeto: rows.average_above_teto || 0,
      averageTotalRemuneration: rows.average_total_remuneration || 0,
      topOrgans: topOrgansRows.map((r) => ({ orgao: r.orgao, total: r.total })),
      topState: topStateRow ? { estado: topStateRow.estado, total: topStateRow.total } : { estado: "", total: 0 },
    };
  } catch {
    return null;
  }
}

export function getYearComparison(year1: number, year2: number): {
  year1: YearComparisonData;
  year2: YearComparisonData;
  growth: {
    membersAboveTeto: number;
    totalAboveTeto: number;
    averageAboveTeto: number;
  };
} | null {
  const y1 = getYearAggregates(year1);
  const y2 = getYearAggregates(year2);

  if (!y1 || !y2) return null;

  const growth = {
    membersAboveTeto: y1.membersAboveTeto > 0
      ? ((y2.membersAboveTeto - y1.membersAboveTeto) / y1.membersAboveTeto) * 100
      : 0,
    totalAboveTeto: y1.totalAboveTeto > 0
      ? ((y2.totalAboveTeto - y1.totalAboveTeto) / y1.totalAboveTeto) * 100
      : 0,
    averageAboveTeto: y1.averageAboveTeto > 0
      ? ((y2.averageAboveTeto - y1.averageAboveTeto) / y1.averageAboveTeto) * 100
      : 0,
  };

  return { year1: y1, year2: y2, growth };
}

export function getAllYearsTrend(): YearComparisonData[] {
  try {
    const db = openDB();
    if (!db) return [];

    const rows = db.prepare(`
      SELECT 
        ano_referencia as year,
        COUNT(DISTINCT nome || '-' || orgao) as totalMembers,
        SUM(CASE WHEN acima_teto > 0 THEN 1 ELSE 0 END) as membersAboveTeto,
        SUM(acima_teto) as totalAboveTeto,
        AVG(CASE WHEN acima_teto > 0 THEN acima_teto END) as averageAboveTeto,
        AVG(remuneracao_total) as averageTotalRemuneration
      FROM membros
      GROUP BY ano_referencia
      ORDER BY ano_referencia
    `).all() as {
      year: number;
      totalMembers: number;
      membersAboveTeto: number;
      totalAboveTeto: number;
      averageAboveTeto: number;
      averageTotalRemuneration: number;
    }[];

    db.close();

    return rows.map((row) => ({
      year: row.year,
      totalMembers: row.totalMembers || 0,
      membersAboveTeto: row.membersAboveTeto || 0,
      totalAboveTeto: row.totalAboveTeto || 0,
      averageAboveTeto: row.averageAboveTeto || 0,
      averageTotalRemuneration: row.averageTotalRemuneration || 0,
      topOrgans: [],
      topState: null,
    }));
  } catch {
    return [];
  }
}

export const getCachedAvailableYears = unstable_cache(
  async () => getAvailableYears(),
  ["available-years"],
  { revalidate: 600 }
);

export const getCachedAllYearsTrend = unstable_cache(
  async () => getAllYearsTrend(),
  ["all-years-trend"],
  { revalidate: 600 }
);
