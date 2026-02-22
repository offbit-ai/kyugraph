/**
 * Integration tests for the KyuGraph Node.js bindings.
 *
 * Uses the Node.js built-in test runner (node:test).
 * Run with: node --test tests/test_kyugraph.mjs
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { Database } from '../index.js';

// ---------------------------------------------------------------------------
// Database creation
// ---------------------------------------------------------------------------

describe('Database', () => {
  it('creates an in-memory database', () => {
    const db = new Database();
    const conn = db.connect();
    assert.ok(conn);
  });

  it('creates a persistent database', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kyu-node-'));
    try {
      const db = new Database(join(dir, 'testdb'));
      const conn = db.connect();
      conn.execute('CREATE NODE TABLE T (id INT64, PRIMARY KEY (id))');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('persistent database survives restart with explicit checkpoint', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kyu-node-'));
    const dbPath = join(dir, 'persist_db');
    try {
      // Phase 1: create, populate, and explicitly checkpoint.
      {
        const db = new Database(dbPath);
        const conn = db.connect();
        conn.execute('CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))');
        conn.execute("CREATE (n:Person {id: 1, name: 'Alice'})");
        conn.execute("CREATE (n:Person {id: 2, name: 'Bob'})");
        conn.execute('CHECKPOINT');
      }

      // Phase 2: reopen and verify.
      {
        const db2 = new Database(dbPath);
        const conn2 = db2.connect();
        const result = conn2.query('MATCH (p:Person) RETURN p.id, p.name');
        assert.equal(result.numRows(), 2);
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

describe('DDL', () => {
  it('creates a node table', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))');
    const result = conn.query('MATCH (p:Person) RETURN p.id');
    assert.equal(result.numRows(), 0);
  });

  it('creates a rel table', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))');
    conn.execute('CREATE REL TABLE KNOWS (FROM Person TO Person, since INT64)');
  });

  it('drops a table', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))');
    conn.execute('DROP TABLE Person');
    assert.throws(() => {
      conn.query('MATCH (p:Person) RETURN p.id');
    });
  });

  it('errors on duplicate table', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))');
    assert.throws(() => {
      conn.execute('CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))');
    });
  });
});

// ---------------------------------------------------------------------------
// DML — CREATE / SET / DELETE
// ---------------------------------------------------------------------------

describe('DML', () => {
  let conn;

  beforeEach(() => {
    const db = new Database();
    conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))');
  });

  it('creates a node', () => {
    conn.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})");
    const result = conn.query('MATCH (p:Person) RETURN p.name');
    assert.equal(result.numRows(), 1);
    const rows = result.toArray();
    assert.equal(rows[0][0], 'Alice');
  });

  it('creates multiple nodes', () => {
    conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 30}), (b:Person {id: 2, name: 'Bob', age: 25})");
    const result = conn.query('MATCH (p:Person) RETURN p.name');
    assert.equal(result.numRows(), 2);
  });

  it('creates and returns', () => {
    const result = conn.query("CREATE (n:Person {id: 1, name: 'Alice', age: 30}) RETURN n.name, n.age");
    assert.equal(result.numRows(), 1);
    const row = result.getRow(0);
    assert.equal(row[0], 'Alice');
    assert.equal(row[1], 30);
  });

  it('creates with partial properties (null default)', () => {
    conn.execute('CREATE (n:Person {id: 1})');
    const result = conn.query('MATCH (p:Person) RETURN p.name');
    const rows = result.toArray();
    assert.equal(rows[0][0], null);
  });

  it('sets a property', () => {
    conn.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 25})");
    conn.execute("MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31");
    const result = conn.query('MATCH (p:Person) RETURN p.age');
    assert.equal(result.toArray()[0][0], 31);
  });

  it('sets with WHERE filter', () => {
    conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})");
    conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})");
    conn.execute('MATCH (p:Person) WHERE p.id = 1 SET p.age = 26');

    const result = conn.query('MATCH (p:Person) RETURN p.name, p.age');
    const rows = result.toArray();
    const ages = Object.fromEntries(rows.map(r => [r[0], r[1]]));
    assert.equal(ages['Alice'], 26);
    assert.equal(ages['Bob'], 30);
  });

  it('deletes a node', () => {
    conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})");
    conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})");
    conn.execute("MATCH (p:Person) WHERE p.name = 'Alice' DELETE p");

    const result = conn.query('MATCH (p:Person) RETURN p.name');
    assert.equal(result.numRows(), 1);
    assert.equal(result.toArray()[0][0], 'Bob');
  });

  it('deletes all nodes', () => {
    conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})");
    conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})");
    conn.execute('MATCH (p:Person) DELETE p');

    const result = conn.query('MATCH (p:Person) RETURN p.id');
    assert.equal(result.numRows(), 0);
  });
});

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

describe('Query', () => {
  it('returns a literal', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN 1 AS x');
    assert.equal(result.numRows(), 1);
    assert.equal(result.toArray()[0][0], 1);
  });

  it('returns arithmetic', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN 2 + 3 AS sum');
    assert.equal(result.toArray()[0][0], 5);
  });

  it('returns a string', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query("RETURN 'hello' AS greeting");
    assert.equal(result.toArray()[0][0], 'hello');
  });

  it('throws on parse error', () => {
    const db = new Database();
    const conn = db.connect();
    assert.throws(() => {
      conn.query('THIS IS NOT VALID CYPHER !!!');
    });
  });

  it('scans with filter', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, age INT64, PRIMARY KEY (id))');
    conn.execute('CREATE (a:Person {id: 1, age: 20})');
    conn.execute('CREATE (b:Person {id: 2, age: 35})');
    conn.execute('CREATE (c:Person {id: 3, age: 50})');
    const result = conn.query('MATCH (p:Person) WHERE p.age > 30 RETURN p.id');
    assert.equal(result.numRows(), 2);
  });
});

// ---------------------------------------------------------------------------
// QueryResult API
// ---------------------------------------------------------------------------

describe('QueryResult', () => {
  let result;

  beforeEach(() => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))');
    conn.execute("CREATE (a:Person {id: 1, name: 'Alice'})");
    conn.execute("CREATE (b:Person {id: 2, name: 'Bob'})");
    result = conn.query('MATCH (p:Person) RETURN p.id, p.name');
  });

  it('returns column names', () => {
    assert.deepEqual(result.columnNames(), ['id', 'name']);
  });

  it('returns num rows', () => {
    assert.equal(result.numRows(), 2);
  });

  it('returns num columns', () => {
    assert.equal(result.numColumns(), 2);
  });

  it('converts to array', () => {
    const rows = result.toArray();
    assert.equal(rows.length, 2);
    const ids = new Set(rows.map(r => r[0]));
    assert.deepEqual(ids, new Set([1, 2]));
  });

  it('gets a row by index', () => {
    const row = result.getRow(0);
    assert.equal(row.length, 2);
  });

  it('throws on out-of-bounds getRow', () => {
    assert.throws(() => {
      result.getRow(100);
    });
  });

  it('returns string representation', () => {
    const repr = result.toStringRepr();
    assert.ok(repr.includes('columns=2'));
    assert.ok(repr.includes('rows=2'));
  });
});

// ---------------------------------------------------------------------------
// Type round-trips
// ---------------------------------------------------------------------------

describe('Types', () => {
  it('int64', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN 42 AS x');
    assert.equal(result.toArray()[0][0], 42);
  });

  it('double', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN 3.14 AS x');
    assert.ok(Math.abs(result.toArray()[0][0] - 3.14) < 1e-9);
  });

  it('string', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query("RETURN 'hello world' AS x");
    assert.equal(result.toArray()[0][0], 'hello world');
  });

  it('boolean true', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN true AS x');
    assert.strictEqual(result.toArray()[0][0], true);
  });

  it('boolean false', () => {
    const db = new Database();
    const conn = db.connect();
    const result = conn.query('RETURN false AS x');
    assert.strictEqual(result.toArray()[0][0], false);
  });

  it('null', () => {
    const db = new Database();
    const conn = db.connect();
    conn.execute('CREATE NODE TABLE T (id INT64, v STRING, PRIMARY KEY (id))');
    conn.execute('CREATE (n:T {id: 1})');
    const result = conn.query('MATCH (t:T) RETURN t.v');
    assert.equal(result.toArray()[0][0], null);
  });
});

// ---------------------------------------------------------------------------
// COPY FROM CSV
// ---------------------------------------------------------------------------

describe('COPY FROM', () => {
  it('imports CSV', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kyu-node-csv-'));
    const csvPath = join(dir, 'people.csv');
    try {
      writeFileSync(csvPath, 'id,name\n1,Alice\n2,Bob\n3,Charlie\n');

      const db = new Database();
      const conn = db.connect();
      conn.execute('CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))');
      conn.execute(`COPY Person FROM '${csvPath}'`);

      const result = conn.query('MATCH (p:Person) RETURN p.id, p.name');
      assert.equal(result.numRows(), 3);
      const names = new Set(result.toArray().map(r => r[1]));
      assert.deepEqual(names, new Set(['Alice', 'Bob', 'Charlie']));
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Multiple connections
// ---------------------------------------------------------------------------

describe('Connections', () => {
  it('share state between connections', () => {
    const db = new Database();
    const conn1 = db.connect();
    const conn2 = db.connect();

    conn1.execute('CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))');
    conn1.execute('CREATE (n:Person {id: 1})');

    // conn2 sees conn1's writes.
    const result = conn2.query('MATCH (p:Person) RETURN p.id');
    assert.equal(result.numRows(), 1);
  });
});
