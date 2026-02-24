//! Comprehensive parser validation using queries from the openCypher TCK
//! (Technology Compatibility Kit) and the Kuzu database test suite.
//!
//! These tests validate that our parser can successfully parse real-world
//! Cypher queries without errors. They do NOT verify AST correctness —
//! only that parsing succeeds.

use kyu_parser::parse;

/// Assert that a query parses without errors.
fn assert_parses(query: &str) {
    let result = parse(query);
    assert!(
        result.errors.is_empty(),
        "Failed to parse:\n  {query}\nErrors: {:?}",
        result.errors
    );
    assert!(
        result.ast.is_some(),
        "Parse returned no AST for:\n  {query}"
    );
}

/// Assert that a query parses (allowing errors from error recovery).
/// Used for queries that should at least produce a partial AST.
fn assert_parses_or_recovers(query: &str) {
    let result = parse(query);
    // We only care that it doesn't panic — error recovery is fine
    let _ = result;
}

// =============================================================================
// TCK Match — Node matching (Match1, Match3)
// =============================================================================

#[test]
fn tck_match_nodes() {
    let queries = [
        // Match1 - Basic node matching
        "MATCH (n) RETURN n",
        "MATCH (n:A) RETURN n",
        "MATCH (n:A:B) RETURN n",
        "MATCH (n {name: 'bar'}) RETURN n",
        "MATCH (n), (m) RETURN n, m",
        // Match3 - Fixed length patterns
        "MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2",
        "MATCH (a)-[r]->(b) RETURN a, r, b",
        "MATCH (a)-[r]-(b) RETURN a, r, b",
        "MATCH ()-[rel:KNOWS]->(x) RETURN x",
        "MATCH (a)-[r {name: 'r'}]-(b) RETURN a, b",
        "MATCH (a)-->(b:Foo) RETURN b",
        "MATCH (n:A:B:C:D:E:F:G:H:I:J:K:L:M)-[:T]->(m:Z:Y:X:W:V:U) RETURN n, m",
        "MATCH (a)-[:T|:T]->(b) RETURN b",
        "MATCH (n)-->(a)-->(b) RETURN b",
        "MATCH (a)-->(b), (b)-->(b) RETURN b",
        "MATCH (a)-[r]-(b) RETURN a, r, b",
        "MATCH (n)-[r]-(n) RETURN n, r",
        "MATCH (n)-[r]->(n) RETURN n, r",
        // Cyclic patterns
        "MATCH (a)-[:A]->()-[:B]->(a) RETURN a.name",
        "MATCH (a)-[:A]->(b), (b)-[:B]->(a) RETURN a.name",
        // Multiple MATCHes
        "MATCH (a {name: 'A'}), (b {name: 'B'}) MATCH (a)-->(x)<-->(b) RETURN x",
        "MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) MATCH (a)-->(x), (b)-->(x), (c)-->(x) RETURN x",
        // Disconnected patterns
        "MATCH (a)-->(b) MATCH (c)-->(d) RETURN a, b, c, d",
        // Re-matching with WITH
        "MATCH (a1)-[r:T]->() WITH r, a1 MATCH (a1)-[r:T]->(b2) RETURN a1, r, b2",
        "MATCH (a1)-[r]->() WITH r, a1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2",
        "MATCH (a1:X:Y)-[r]->() WITH r, a1 MATCH (a1:Y)-[r]->(b2) RETURN a1, r, b2",
        // Self-loops
        "MATCH (x:A)-[r1]->(y)-[r2]-(z) RETURN x, r1, y, r2, z",
        "MATCH (x)-[r1]-(y)-[r2]-(z) RETURN x, r1, y, r2, z",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Match — Variable length patterns (Match5)
// =============================================================================

#[test]
fn tck_match_variable_length() {
    let queries = [
        "MATCH (a:A) MATCH (a)-[:LIKES*]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*..]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*0]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*1]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*2]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*0..2]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*1..2]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*0..0]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*1..1]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*2..2]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*..1]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*..2]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*0..]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*1..]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*2..]->(c) RETURN c.name",
        // Mixed variable-length and fixed
        "MATCH (a:A) MATCH (a)-[:LIKES*0]->()-[:LIKES]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES]->()-[:LIKES*0]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES*1]->()-[:LIKES]->(c) RETURN c.name",
        "MATCH (a:A) MATCH (a)-[:LIKES]->()-[:LIKES*2]->(c) RETURN c.name",
        // Variable length with direction mixing
        "MATCH (a:A) MATCH (a)<-[:LIKES]-()-[:LIKES*3]->(c) RETURN c.name",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Match — Optional match (Match7)
// =============================================================================

#[test]
fn tck_optional_match() {
    let queries = [
        "OPTIONAL MATCH (n) RETURN n",
        "MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n, x",
        "MATCH (a:A), (b:C) OPTIONAL MATCH (x)-->(b) RETURN x",
        "MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2",
        "MATCH ()-[r]->() WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2",
        "MATCH (a {name: 'A'}) OPTIONAL MATCH (a)-[:KNOWS]->()-[:KNOWS]->(foo) RETURN foo",
        "MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b",
        "OPTIONAL MATCH (a) WITH a OPTIONAL MATCH (a)-->(b) RETURN b",
        "MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, b, c",
        // Variable length optional
        "MATCH (a:Single) OPTIONAL MATCH (a)-[*]->(b) RETURN b",
        "MATCH (a:Single), (x:C) OPTIONAL MATCH (a)-[*]->(x) RETURN x",
        "MATCH (a:Single) OPTIONAL MATCH (a)-[*3..]-(b) RETURN b",
        // Optional match with paths
        "MATCH (a:A) OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p",
        "MATCH (a {name: 'A'}), (x) WHERE x.name IN ['B', 'C'] OPTIONAL MATCH p = (a)-->(x) RETURN x, p",
        "MATCH (a {name: 'A'}) OPTIONAL MATCH p = (a)-->(b)-[*]->(c) RETURN p",
        // Optional match labels
        "MATCH (a:X) OPTIONAL MATCH (a)-->(b:Y) RETURN b",
        // Self-loop optional
        "MATCH (a:B) OPTIONAL MATCH (a)-[r]-(a) RETURN r",
        "MATCH (a) WHERE NOT (a:B) OPTIONAL MATCH (a)-[r]->(a) RETURN r",
        // Correlated optional
        "MATCH (a:A), (b:B) OPTIONAL MATCH (a)-->(x) OPTIONAL MATCH (x)-[r]->(b) RETURN x, r",
        "OPTIONAL MATCH (a:NotThere) WITH a MATCH (b:B) WITH a, b OPTIONAL MATCH (b)-[r:NOR_THIS]->(a) RETURN a, b, r",
        // Real-world: open world assumption
        "MATCH (p:Player)-[:PLAYS_FOR]->(team:Team) OPTIONAL MATCH (p)-[s:SUPPORTS]->(team) RETURN count(*) AS matches, s IS NULL AS optMatch",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Match — Clause interop (Match8)
// =============================================================================

#[test]
fn tck_match_interop() {
    let queries = [
        "MATCH (a) WITH a MATCH (b) RETURN a, b",
        "MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*)",
        "MATCH ()-->() WITH 1 AS x MATCH ()-[r1]->()<--() RETURN sum(r1.times)",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Match — Variable length relationship variables (Match9)
// =============================================================================

#[test]
fn tck_match_varlen_rels() {
    let queries = [
        "MATCH ()-[r*0..1]-() RETURN last(r) AS l",
        "MATCH (a)-[r:REL*2..2]->(b:End) RETURN r",
        "MATCH (a)-[r:REL*2..2]-(b:End) RETURN r",
        "MATCH (a:Start)-[r:REL*2..2]-(b) RETURN r",
        "MATCH (a:Blue)-[r*]->(b:Green) RETURN count(r)",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Create (Create1, Create2, Create5)
// =============================================================================

#[test]
fn tck_create() {
    let queries = [
        // Nodes
        "CREATE ()",
        "CREATE (), ()",
        "CREATE (:Label)",
        "CREATE (:Label), (:Label)",
        "CREATE (:A:B:C:D)",
        "CREATE (:B:A:D), (:B:C), (:D:E:B)",
        "CREATE ({created: true})",
        "CREATE (n {name: 'foo'}) RETURN n.name AS p",
        "CREATE (n {id: 12, name: 'foo'})",
        "CREATE (n {id: 12, name: 'foo'}) RETURN n.id AS id, n.name AS p",
        "CREATE (n {id: 12, name: null}) RETURN n.id AS id, n.name AS p",
        "CREATE (p:TheLabel {id: 4611686018427387905}) RETURN p.id",
        // Relationships
        "CREATE ()-[:R]->()",
        "CREATE (a), (b), (a)-[:R]->(b)",
        "CREATE (a) CREATE (b) CREATE (a)-[:R]->(b)",
        "CREATE (:A)<-[:R]-(:B)",
        "CREATE (root)-[:LINK]->(root)",
        "CREATE (root), (root)-[:LINK]->(root)",
        "CREATE (root) CREATE (root)-[:LINK]->(root)",
        "CREATE ()-[:R {num: 42}]->()",
        "CREATE ()-[r:R {num: 42}]->() RETURN r.num AS num",
        "CREATE ()-[:R {id: 12, name: 'foo'}]->()",
        "CREATE ()-[r:R {id: 12, name: 'foo'}]->() RETURN r.id AS id, r.name AS name",
        // Multi-hop create
        "CREATE (:A)-[:R]->(:B)-[:R]->(:C)",
        "CREATE (:A)<-[:R]-(:B)<-[:R]-(:C)",
        "CREATE (:A)-[:R]->(:B)<-[:R]-(:C)",
        "CREATE ()-[:R1]->()<-[:R2]-()-[:R3]->()",
        "CREATE (:A)<-[:R1]-(:B)-[:R2]->(:C)",
        // Create from match
        "MATCH (x:X), (y:Y) CREATE (x)-[:R]->(y)",
        "MATCH (x:X), (y:Y) CREATE (x)<-[:R]-(y)",
        "MATCH (root:Root) CREATE (root)-[:LINK]->(root)",
        "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)",
        "MATCH (x:End) CREATE (:Begin)-[:TYPE]->(x)",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Merge (Merge1, Merge3)
// =============================================================================

#[test]
fn tck_merge() {
    let queries = [
        "MERGE (a) RETURN count(*) AS n",
        "MERGE (a:TheLabel) RETURN labels(a)",
        "MERGE (a:TheLabel) RETURN a.id",
        "MERGE (a {num: 43}) RETURN a.num",
        "MERGE (a:TheLabel {num: 43}) RETURN a.num",
        "MERGE (a:TheLabel {num: 42}) RETURN a.num",
        "CREATE (:X) CREATE (:X) MERGE (:X)",
        "MERGE (test:L:B {num: 42}) RETURN labels(test) AS labels",
        "MATCH (person:Person) MERGE (city:City {name: person.bornIn})",
        "CREATE (a {num: 1}) MERGE ({v: a.num})",
        "MERGE p = (a {num: 1}) RETURN p",
        // ON MATCH SET
        "MERGE (a) ON MATCH SET a:L",
        "MERGE (a:TheLabel) ON MATCH SET a:Foo RETURN labels(a)",
        "MERGE (a:TheLabel) ON MATCH SET a.num = 42 RETURN a.num",
        // Complex merge
        "MATCH (person:Person) MERGE (city:City) ON MATCH SET city.name = person.bornIn RETURN person.bornIn",
        "UNWIND [0, 1, 2] AS x UNWIND [0, 1, 2] AS y CREATE ({x: x, y: y})",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Delete (Delete1)
// =============================================================================

#[test]
fn tck_delete() {
    let queries = [
        "MATCH (n) DELETE n",
        "MATCH (n) DETACH DELETE n",
        "MATCH (n:X) DETACH DELETE n",
        "OPTIONAL MATCH (n) DELETE n",
        "OPTIONAL MATCH (a:DoesNotExist) DELETE a RETURN a",
        "OPTIONAL MATCH (n) DETACH DELETE n",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Set (Set1)
// =============================================================================

#[test]
fn tck_set() {
    let queries = [
        "MATCH (n:A) WHERE n.name = 'Andres' SET n.name = 'Michael' RETURN n",
        "MATCH (n:A) WHERE n.name = 'Andres' SET n.name = n.name + ' was here' RETURN n",
        "MATCH (n:A) SET n.numbers = [1, 2, 3] RETURN [i IN n.numbers | i / 2.0] AS x",
        "CREATE (a {numbers: [1, 2, 3]}) SET a.numbers = a.numbers + [4, 5] RETURN a.numbers",
        "CREATE (a {numbers: [3, 4, 5]}) SET a.numbers = [1, 2] + a.numbers RETURN a.numbers",
        "OPTIONAL MATCH (a:DoesNotExist) SET a.num = 42 RETURN a",
        "MATCH (n:X) SET n.name = 'A', n.name2 = 'B', n.num = 5 RETURN n",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Unwind (Unwind1)
// =============================================================================

#[test]
fn tck_unwind() {
    let queries = [
        "UNWIND [1, 2, 3] AS x RETURN x",
        "UNWIND range(1, 3) AS x RETURN x",
        "WITH [1, 2, 3] AS first, [4, 5, 6] AS second UNWIND (first + second) AS x RETURN x",
        "UNWIND RANGE(1, 2) AS row WITH collect(row) AS rows UNWIND rows AS x RETURN x",
        "WITH [[1, 2, 3], [4, 5, 6]] AS lol UNWIND lol AS x UNWIND x AS y RETURN y",
        "UNWIND [] AS empty RETURN empty",
        "UNWIND null AS nil RETURN nil",
        "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS duplicate RETURN duplicate",
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN *",
        "WITH [1, 2] AS xs, [3, 4] AS ys, [5, 6] AS zs UNWIND xs AS x UNWIND ys AS y UNWIND zs AS z RETURN *",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Return (Return1, Return2, Return3, Return6)
// =============================================================================

#[test]
fn tck_return() {
    let queries = [
        // Basic return
        "MATCH (n) RETURN n",
        "RETURN 1 + (2 - (3 * (4 / (5 ^ (6 % null))))) AS a",
        "MATCH (a) RETURN a.num",
        "MATCH (a) RETURN a.name",
        "MATCH ()-[r]->() RETURN r.num",
        "MATCH (a) RETURN a.num + 1 AS foo",
        "MATCH (a) RETURN a.list2 + a.list1 AS foo",
        "RETURN {a: 1, b: 'foo'}",
        "MATCH (a) RETURN count(a) > 0",
        "MATCH (n)-[r]->(m) RETURN [n, r, m] AS r",
        "MATCH (n)-[r]->(m) RETURN {node1: n, rel: r, node2: m} AS m",
        // Return with delete
        "MATCH ()-[r]->() DELETE r RETURN type(r)",
        // Multiple return items
        "MATCH (a) RETURN a.id IS NOT NULL AS a, a IS NOT NULL AS b",
        "MATCH (a) RETURN a.name, a.age, a.seasons",
        "MATCH (a)-[r]->() RETURN a AS foo, r AS bar",
        // Aggregation
        "MATCH (n) RETURN n.num AS n, count(n) AS count",
        "MATCH (a) RETURN a, count(a) + 3",
        "MATCH (a) WITH a.num AS a, count(*) AS count RETURN count",
        "MATCH (n) RETURN count(n) / 60 / 60 AS count",
        "MATCH (a) RETURN size(collect(a))",
        "MATCH (n) RETURN n.num, count(*)",
        "MATCH () RETURN count(*) * 10 AS c",
        "MATCH (n) RETURN count(n), collect(n)",
        "MATCH () RETURN count(*)",
        // RETURN star
        "MATCH (n) RETURN *",
        // RETURN DISTINCT
        "MATCH (n) RETURN DISTINCT n.name",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Return — ORDER BY (ReturnOrderBy1, ReturnOrderBy3)
// =============================================================================

#[test]
fn tck_return_order_by() {
    let queries = [
        "UNWIND [true, false] AS bools RETURN bools ORDER BY bools",
        "UNWIND [true, false] AS bools RETURN bools ORDER BY bools DESC",
        "UNWIND ['.*', '', ' ', 'one'] AS strings RETURN strings ORDER BY strings",
        "UNWIND ['.*', '', ' ', 'one'] AS strings RETURN strings ORDER BY strings DESC",
        "UNWIND [1, 3, 2] AS ints RETURN ints ORDER BY ints",
        "UNWIND [1, 3, 2] AS ints RETURN ints ORDER BY ints DESC",
        "UNWIND [1.5, 1.3, 999.99] AS floats RETURN floats ORDER BY floats",
        "UNWIND [1.5, 1.3, 999.99] AS floats RETURN floats ORDER BY floats DESC",
        // Multi-column ordering
        "MATCH (n) RETURN n.division, count(*) ORDER BY count(*) DESC, n.division ASC",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Return — SKIP / LIMIT (ReturnSkipLimit1)
// =============================================================================

#[test]
fn tck_return_skip_limit() {
    let queries = [
        "MATCH (n) RETURN n ORDER BY n.name ASC SKIP 2",
        "MATCH (n) RETURN n ORDER BY n.name ASC SKIP $skipAmount",
        "MATCH (n) WHERE 1 = 0 RETURN n SKIP 0",
        "MATCH (n) RETURN n SKIP n.count",
        "MATCH (p:Person) RETURN p.name AS name SKIP -1",
        "MATCH (p:Person) RETURN p.name AS name SKIP 1.5",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK WITH (With1, With2, With4)
// =============================================================================

#[test]
fn tck_with() {
    let queries = [
        // Forward variables
        "MATCH (a:A) WITH a MATCH (a)-->(b) RETURN *",
        "MATCH (a:A) WITH a MATCH (x:X), (a)-->(b) RETURN *",
        "MATCH ()-[r1]->(:X) WITH r1 AS r2 MATCH ()-[r2]->() RETURN r2 AS rel",
        "MATCH p = (a) WITH p RETURN p",
        "OPTIONAL MATCH (a:Start) WITH a MATCH (a)-->(b) RETURN *",
        "OPTIONAL MATCH (a:A) WITH a AS a MATCH (b:B) RETURN a, b",
        // Forward expressions
        "MATCH (a:Begin) WITH a.num AS property MATCH (b) WHERE b.id = property RETURN b",
        "WITH {name: {name2: 'baz'}} AS nestedMap RETURN nestedMap.name.name2",
        // Aliasing
        "MATCH ()-[r1]->() WITH r1 AS r2 RETURN r2 AS rel",
        "MATCH (a:Begin) WITH a.num AS property MATCH (b:End) WHERE property = b.num RETURN b",
        "MATCH (n) WITH n.name AS n RETURN n",
        // Complex WITH chains
        "MATCH (person:Person)<--(message)<-[like]-(:Person) WITH like.creationDate AS likeTime, person AS person ORDER BY likeTime, message.id WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person WITH latestLike.likeTime AS likeTime ORDER BY likeTime RETURN likeTime",
        "CREATE (m {id: 0}) WITH {first: m.id} AS m WITH {second: m.first} AS m RETURN m.second",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK MatchWhere (MatchWhere1, MatchWhere3)
// =============================================================================

#[test]
fn tck_match_where() {
    let queries = [
        // Label predicates
        "MATCH (a)-[:ADMIN]-(b) WHERE a:A RETURN a.id, b.id",
        // Property predicates
        "MATCH (n) WHERE n.name = 'Bar' RETURN n",
        "MATCH (n:Person)-->() WHERE n.name = 'Bob' RETURN n",
        "MATCH ()-[rel:X]-(a) WHERE a.name = 'Andres' RETURN a",
        // Parameter predicates
        "MATCH (a)-[r]->(b) WHERE b.name = $param RETURN r",
        // Relationship type predicates
        "MATCH (n {name: 'A'})-[r]->(x) WHERE type(r) = 'KNOWS' RETURN x",
        // Relationship property predicates
        "MATCH (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a",
        "MATCH (a)-[r]->(b) WHERE r.name = $param RETURN b",
        // Disjunctive predicates
        "MATCH (n) WHERE n.p1 = 12 OR n.p2 = 13 RETURN n",
        "MATCH (n)-[r]->(x) WHERE type(r) = 'KNOWS' OR type(r) = 'HATES' RETURN r",
        // Path length predicates
        "MATCH p = (n)-->(x) WHERE length(p) = 1 RETURN x",
        "MATCH p = (n)-->(x) WHERE length(p) = 10 RETURN x",
        // Equi-joins
        "MATCH (a), (b) WHERE a = b RETURN a, b",
        "MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b",
        "MATCH (n)-[rel]->(x) WHERE n.animal = x.animal RETURN n, x",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Expressions — Literals (Literals1-8)
// =============================================================================

#[test]
fn tck_literals() {
    let queries = [
        // Integer literals
        "RETURN 0 AS literal",
        "RETURN 1 AS literal",
        "RETURN 372036854 AS literal",
        "RETURN -1 AS literal",
        "RETURN -372036854 AS literal",
        // Float literals
        "RETURN 0.0 AS literal",
        "RETURN 3.14 AS literal",
        "RETURN -3.14 AS literal",
        "RETURN 6.022E23 AS literal",
        // String literals
        "RETURN '' AS literal",
        "RETURN 'hello' AS literal",
        "RETURN \"hello\" AS literal",
        // Boolean literals
        "RETURN true AS literal",
        "RETURN false AS literal",
        // Null literal
        "RETURN null AS literal",
        // List literals
        "RETURN [] AS literal",
        "RETURN [1, 2, 3] AS literal",
        "RETURN [1, 'two', null, true] AS literal",
        "RETURN [[1, 2], [3, 4]] AS literal",
        // Map literals
        "RETURN {} AS literal",
        "RETURN {name: 'Alice', age: 30} AS literal",
        "RETURN {nested: {key: 'value'}} AS literal",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Expressions — Arithmetic Precedence (Precedence2)
// =============================================================================

#[test]
fn tck_arithmetic_precedence() {
    let queries = [
        // Mul over add
        "RETURN 4 * 2 + 3 * 2 AS a, 4 * 2 + (3 * 2) AS b, 4 * (2 + 3) * 2 AS c",
        "RETURN 4 * 2 - 3 * 2 AS a, 4 * 2 - (3 * 2) AS b, 4 * (2 - 3) * 2 AS c",
        "RETURN 4 / 2 + 3 / 2 AS a, 4 / 2 + (3 / 2) AS b, 4 / (2 + 3) / 2 AS c",
        "RETURN 4 % 2 + 3 % 2 AS a, 4 % 2 + (3 % 2) AS b, 4 % (2 + 3) % 2 AS c",
        // Power over mul
        "RETURN 4 ^ 3 * 2 ^ 3 AS a, (4 ^ 3) * (2 ^ 3) AS b",
        "RETURN 4 ^ 3 + 2 ^ 3 AS a, (4 ^ 3) + (2 ^ 3) AS b",
        // Unary minus
        "RETURN -3 ^ 2 AS a, (-3) ^ 2 AS b, -(3 ^ 2) AS c",
        "RETURN -3 + 2 AS a, (-3) + 2 AS b, -(3 + 2) AS c",
        "RETURN -3 - 2 AS a, (-3) - 2 AS b, -(3 - 2) AS c",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Expressions — Boolean Precedence (Precedence1)
// =============================================================================

#[test]
fn tck_boolean_precedence() {
    let queries = [
        // AND over OR
        "RETURN true AND false OR true AS result",
        "RETURN (true AND false) OR true AS result",
        "RETURN true AND (false OR true) AS result",
        // XOR
        "RETURN true XOR false AS result",
        "RETURN true XOR false XOR true AS result",
        // NOT over AND
        "RETURN NOT false AND true AS result",
        "RETURN (NOT false) AND true AS result",
        "RETURN NOT (false AND true) AS result",
        // NOT over OR
        "RETURN NOT false OR true AS result",
        // IS NULL precedence
        "RETURN NOT null IS NULL AS a, NOT (null IS NULL) AS b",
        "RETURN null AND null IS NULL AS a, null AND (null IS NULL) AS b",
        "RETURN true OR null IS NULL AS a, true OR (null IS NULL) AS b",
        "RETURN true XOR false IS NULL AS a, true XOR (false IS NULL) AS b",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// TCK Expressions — List Precedence (Precedence3)
// =============================================================================

#[test]
fn tck_list_precedence() {
    let queries = [
        // List concatenation
        "RETURN [[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10][3] AS a",
        "RETURN [[1], [2, 3], [4, 5]] + ([5, [6, 7], [8, 9], 10][3]) AS b",
        // List element containment
        "RETURN [1, 2] = [3, 4] IN [[3, 4], false] AS a",
        "RETURN [1, 2] = ([3, 4] IN [[3, 4], false]) AS b",
        "RETURN ([1, 2] = [3, 4]) IN [[3, 4], false] AS c",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Kuzu-style queries — Multi-part queries, complex patterns
// =============================================================================

#[test]
fn kuzu_match_patterns() {
    let queries = [
        "MATCH (a:person)-[e:knows]->(b:person) RETURN COUNT(*)",
        "MATCH (a:person)-[e1:studyAt]->(b:organisation) RETURN e1.code",
        "MATCH (a:person)-[:knows]->(b:person), (c:person)-[:knows]->(d:person) RETURN COUNT(*)",
        "MATCH (a)-[e:knows]-(b) WHERE a.ID = 0 AND b.ID = 2 WITH e RETURN e",
        "MATCH (a:person)-[e:knows]->(b:person) WHERE a.fName = 'Alice' RETURN b.fName ORDER BY b.fName",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Kuzu-style DDL
// =============================================================================

#[test]
fn kuzu_ddl() {
    let queries = [
        "CREATE NODE TABLE Person (ID INT64, name STRING, PRIMARY KEY(ID))",
        "CREATE NODE TABLE IF NOT EXISTS Person (ID INT64, PRIMARY KEY(ID))",
        "CREATE REL TABLE knows (FROM Person TO Person)",
        "CREATE REL TABLE likes (FROM Person TO Organisation, rating INT64)",
        "DROP TABLE studyAt",
        "DROP TABLE IF EXISTS nonexistent",
        "ALTER TABLE person ADD random INT64",
        "ALTER TABLE person ADD random STRING DEFAULT 'hello'",
        "ALTER TABLE person DROP fName",
        "ALTER TABLE person RENAME fName TO name",
        "ALTER TABLE person RENAME TO student",
        "COPY Person FROM 'data.csv'",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Kuzu-style DML — CREATE, SET, DELETE
// =============================================================================

#[test]
fn kuzu_dml() {
    let queries = [
        // Create nodes with properties
        "CREATE (:person {ID:80, isWorker:true, age:22, eyeSight:1.1})",
        "CREATE (:person {ID:32, fName:'A'}), (:person {ID:33, fName:'BCD'})",
        // Create from match
        "MATCH (a:person) CREATE (:person {ID:a.ID+11, age:a.age*2})",
        // Create relationships from match
        "MATCH (a:person), (b:person) WHERE a.ID = 9 AND b.ID = 10 CREATE (a)-[:knows]->(b)",
        // Set
        "MATCH (a:person) WHERE a.ID=0 SET a.age=20 + 50",
        "MATCH (a:person) WHERE a.ID=0 SET a.isStudent=false",
        // Delete
        "MATCH (a:person) WHERE a.ID = 100 DELETE a",
        "MATCH (a) DETACH DELETE a",
        "OPTIONAL MATCH (a:person) WHERE a.ID > 100 DELETE a",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Transaction statements
// =============================================================================

#[test]
fn transaction_statements() {
    let queries = [
        "BEGIN TRANSACTION",
        "BEGIN TRANSACTION READ ONLY",
        "BEGIN TRANSACTION READ WRITE",
        "COMMIT",
        "ROLLBACK",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// EXPLAIN / PROFILE
// =============================================================================

#[test]
fn explain_profile() {
    let queries = [
        "EXPLAIN MATCH (n) RETURN n",
        "PROFILE MATCH (n:Person)-[:KNOWS]->(m) RETURN m",
        "EXPLAIN MATCH (a:person)-[e:knows]->(b:person) RETURN COUNT(*)",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// CALL statements
// =============================================================================

#[test]
fn call_statements() {
    let queries = ["CALL db.schema", "CALL table_info('Person')"];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Complex real-world queries (combined from TCK and Kuzu)
// =============================================================================

#[test]
fn complex_real_world_queries() {
    let queries = [
        // Multi-part query with aggregation
        "MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you) WHERE me.name = 'Michael' WITH me, count(DISTINCT r1) AS H1, count(DISTINCT r2) AS H2, you MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you) RETURN me, you",
        // Subgraph counting
        "MATCH ()-->() WITH 1 AS x MATCH ()-[r1]->()<--() RETURN sum(r1.times)",
        // Path with variable length and filtering
        "MATCH p = (a:T {name: 'a'})-[:R*]->(other:T) WHERE other <> a WITH a, other, min(length(p)) AS len RETURN a.name AS name, collect(other.name) AS others, len",
        // Complex property access and function calls
        "MATCH (a) RETURN a.list2 + a.list1 AS foo",
        "MATCH (n)-[r]->(m) RETURN {node1: n, rel: r, node2: m} AS m",
        // WITH chaining
        "MATCH (a:Begin) WITH a.num AS property MATCH (b:End) WHERE property = b.num RETURN b",
        // Aggregation with ORDER BY
        "MATCH (n) RETURN n.division, count(*) ORDER BY count(*) DESC, n.division ASC",
        // OPTIONAL MATCH + WITH + MATCH
        "MATCH (a:Single) OPTIONAL MATCH (a)-->(b:NonExistent) OPTIONAL MATCH (a)-->(c:NonExistent) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d",
        // UNWIND + MATCH + MERGE
        "UNWIND $events AS event MATCH (y:Year {year: event.year}) MERGE (e:Event {id: event.id}) MERGE (y)<-[:IN]-(e) RETURN e.id AS x ORDER BY x",
        // CASE expression in context
        "MATCH (n) RETURN CASE WHEN n.age > 30 THEN 'old' WHEN n.age > 20 THEN 'young' ELSE 'child' END AS category",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Expression edge cases
// =============================================================================

#[test]
fn expression_edge_cases() {
    let queries = [
        // Nested expressions
        "RETURN 1 + (2 - (3 * (4 / (5 ^ (6 % 7)))))",
        // Multiple comparisons (chained)
        "MATCH (a) WHERE a.age > 0 AND a.age < 100 RETURN a",
        // String operations
        "MATCH (n) WHERE n.name STARTS WITH 'Al' RETURN n",
        "MATCH (n) WHERE n.name ENDS WITH 'ice' RETURN n",
        "MATCH (n) WHERE n.name CONTAINS 'lic' RETURN n",
        // IN list
        "MATCH (a {name: 'A'}), (x) WHERE x.name IN ['B', 'C'] RETURN x",
        // IS NULL / IS NOT NULL
        "MATCH (n) WHERE n.name IS NULL RETURN n",
        "MATCH (n) WHERE n.name IS NOT NULL RETURN n",
        // Subscript
        "RETURN [1, 2, 3][0] AS first",
        "RETURN [1, 2, 3][1] AS second",
        // Nested list/map
        "RETURN [[1, 2], [3, 4]][0] AS nested",
        "RETURN {a: {b: 1}}.a AS nested",
        // Complex boolean
        "MATCH (n) WHERE (n.a = 1 OR n.b = 2) AND NOT n.c = 3 RETURN n",
        "MATCH (n) WHERE n.a = 1 XOR n.b = 2 RETURN n",
        // Regex match
        "MATCH (n) WHERE n.name =~ 'Al.*' RETURN n",
        // Negative numbers in expressions
        "RETURN -1 + -2 AS result",
        "RETURN 5 * -3 AS result",
        // Empty list and map
        "RETURN [] AS empty_list",
        "RETURN {} AS empty_map",
        // Count star in aggregation
        "MATCH (n) RETURN count(*)",
        "MATCH (n) RETURN count(*) AS total, n.type",
        // Function calls
        "RETURN abs(-42) AS result",
        "RETURN toUpper('hello') AS result",
        "RETURN coalesce(null, 'default') AS result",
        "RETURN size([1, 2, 3]) AS result",
        "RETURN head([1, 2, 3]) AS result",
        "RETURN tail([1, 2, 3]) AS result",
        "RETURN last([1, 2, 3]) AS result",
        "RETURN range(1, 10) AS result",
        "RETURN collect(1) AS result",
        // DISTINCT in function call
        "MATCH (n) RETURN count(DISTINCT n.name) AS unique_names",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Semicolon-terminated queries (common in Kuzu)
// =============================================================================

#[test]
fn semicolon_terminated() {
    let queries = [
        "MATCH (n) RETURN n;",
        "CREATE (:Person {name: 'Alice'});",
        "MATCH (a:person)-[e:knows]->(b:person) RETURN COUNT(*);",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Queries that test error recovery (should produce errors)
// =============================================================================

#[test]
fn error_recovery_queries() {
    // These should NOT parse cleanly but should not panic
    let queries = [
        // Missing RETURN
        "MATCH (n)",
        // Incomplete pattern
        "MATCH (n)-[:KNOWS]-> RETURN n",
        // Empty query
        "",
    ];
    for q in queries {
        assert_parses_or_recovers(q);
    }
}

// =============================================================================
// Mixed case keywords (Cypher is case-insensitive for keywords)
// =============================================================================

#[test]
fn case_insensitive_keywords() {
    let queries = [
        "match (n) return n",
        "Match (n) Return n",
        "MATCH (n) return n",
        "match (n:Person) WHERE n.age > 30 RETURN n",
        "Match (a)-[:KNOWS]->(b) Return a, b",
        "create (:Person {name: 'Alice'})",
        "optional match (n) return n",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Spec query from the KyuGraph spec document
// =============================================================================

#[test]
fn kyugraph_spec_query() {
    // The reference query from scope/spec.md
    let query = r#"
        MATCH (f:Function)-[:calls]->(g:Function)<-[:modifies]-(c:Commit)
        WHERE c.timestamp > $since
        RETURN f.name, f.file, g.name
        ORDER BY f.name DESC
        LIMIT 20
    "#;
    assert_parses(query);
}

// =============================================================================
// Multi-hop patterns from TCK
// =============================================================================

#[test]
fn tck_multi_hop_patterns() {
    let queries = [
        // 2-hop
        "MATCH (a)-[:R1]->(b)-[:R2]->(c) RETURN a, b, c",
        "MATCH (a)<-[:R1]-(b)<-[:R2]-(c) RETURN a, b, c",
        "MATCH (a)-[:R1]->(b)<-[:R2]-(c) RETURN a, b, c",
        // 3-hop
        "MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d) RETURN a, d",
        // Mixed directions
        "MATCH ()-[:R1]->()<-[:R2]-()-[:R3]->() RETURN *",
        "MATCH (:A)<-[:R1]-(:B)-[:R2]->(:C) RETURN *",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// CASE expressions from TCK
// =============================================================================

#[test]
fn tck_case_expressions() {
    let queries = [
        // Simple CASE
        "MATCH (n) RETURN CASE n.eyes WHEN 'blue' THEN 1 WHEN 'brown' THEN 2 ELSE 3 END AS result",
        // Searched CASE
        "MATCH (n) RETURN CASE WHEN n.eyes = 'blue' THEN 1 WHEN n.age < 40 THEN 2 ELSE 3 END AS result",
        // CASE without ELSE
        "MATCH (n) RETURN CASE WHEN n.eyes = 'blue' THEN 1 END AS result",
        // Nested CASE
        "RETURN CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2 END ELSE 3 END AS result",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Parameter usage patterns
// =============================================================================

#[test]
fn parameter_usage() {
    let queries = [
        "MATCH (n) WHERE n.id = $id RETURN n",
        "MATCH (n {name: $name}) RETURN n",
        "RETURN $value AS result",
        "MATCH (n) RETURN n SKIP $skip LIMIT $limit",
        "MATCH (a:Person) WHERE a.age > $minAge AND a.name STARTS WITH $prefix RETURN a",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Relationship type alternatives (pipe-separated)
// =============================================================================

#[test]
fn relationship_type_alternatives() {
    let queries = [
        "MATCH (a)-[:KNOWS|:LIKES]->(b) RETURN b",
        "MATCH (a)-[:R1|:R2|:R3]->(b) RETURN b",
        "MATCH (a)-[r:KNOWS|:LIKES]->(b) RETURN r, b",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Map and nested property patterns
// =============================================================================

#[test]
fn map_property_patterns() {
    let queries = [
        // Node with map properties
        "CREATE (n:Person {name: 'Alice', age: 30, scores: [1, 2, 3]})",
        // Relationship with properties
        "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a, b",
        // Property access chains
        "MATCH (n) RETURN n.address.city",
        "MATCH (n) RETURN n.a.b.c",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// RETURN with complex expressions
// =============================================================================

#[test]
fn return_complex_expressions() {
    let queries = [
        // Arithmetic
        "RETURN 1 + 2 * 3 - 4 / 2 AS result",
        "RETURN 2 ^ 10 AS result",
        "RETURN 10 % 3 AS result",
        // String concatenation (using +)
        "RETURN 'hello' + ' ' + 'world' AS greeting",
        // Boolean
        "RETURN true AND false OR NOT true AS result",
        "RETURN true XOR false AS result",
        // Comparison
        "RETURN 1 < 2 AS lt, 2 <= 2 AS le, 3 > 2 AS gt, 3 >= 3 AS ge, 1 = 1 AS eq, 1 <> 2 AS neq",
        // Function calls with complex args
        "RETURN abs(-42) + ceil(3.14) * floor(2.7) AS result",
        // Nested function calls
        "RETURN toUpper(left('hello', 3)) AS result",
    ];
    for q in queries {
        assert_parses(q);
    }
}

// =============================================================================
// Escaped identifiers
// =============================================================================

#[test]
fn escaped_identifiers() {
    let queries = [
        "MATCH (n:`My Label`) RETURN n",
        "MATCH (n:`Node Type`)-[r:`rel type`]->(m) RETURN n, m",
        "MATCH (n) RETURN n.`my property` AS prop",
    ];
    for q in queries {
        assert_parses(q);
    }
}
