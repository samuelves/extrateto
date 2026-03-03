import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import path from "path";
import * as schema from "./schema";

const DB_PATH = path.join(process.cwd(), "data", "extrateto.db");

let _db: ReturnType<typeof createDb> | null = null;

function createDb(readonly = true) {
  const sqlite = new Database(DB_PATH, readonly ? { readonly: true } : undefined);
  sqlite.pragma("journal_mode = WAL");
  if (!readonly) sqlite.pragma("foreign_keys = ON");
  return drizzle(sqlite, { schema });
}

export function getDb() {
  if (!_db) {
    _db = createDb(true);
  }
  return _db;
}

export { schema };
