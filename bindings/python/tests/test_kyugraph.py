"""Integration tests for the KyuGraph Python bindings."""

import os
import tempfile

import pytest
import kyugraph


# ---------------------------------------------------------------------------
# Database creation
# ---------------------------------------------------------------------------


class TestDatabase:
    def test_in_memory(self):
        db = kyugraph.Database()
        conn = db.connect()
        assert conn is not None

    def test_persistent(self, tmp_path):
        db = kyugraph.Database(str(tmp_path / "testdb"))
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE T (id INT64, PRIMARY KEY (id))"
        )

    def test_persistent_survives_restart(self, tmp_path):
        db_path = str(tmp_path / "persist_db")

        # Phase 1: create and populate.
        db = kyugraph.Database(db_path)
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))"
        )
        conn.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        conn.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
        del conn
        del db

        # Phase 2: reopen and verify.
        db2 = kyugraph.Database(db_path)
        conn2 = db2.connect()
        result = conn2.query("MATCH (p:Person) RETURN p.id, p.name")
        assert result.num_rows() == 2


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


class TestDDL:
    def test_create_node_table(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))"
        )
        result = conn.query("MATCH (p:Person) RETURN p.id")
        assert result.num_rows() == 0

    def test_create_rel_table(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))"
        )
        conn.execute(
            "CREATE REL TABLE KNOWS (FROM Person TO Person, since INT64)"
        )

    def test_drop_table(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))"
        )
        conn.execute("DROP TABLE Person")
        with pytest.raises(RuntimeError):
            conn.query("MATCH (p:Person) RETURN p.id")

    def test_create_duplicate_errors(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))"
        )
        with pytest.raises(RuntimeError):
            conn.execute(
                "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))"
            )


# ---------------------------------------------------------------------------
# DML — CREATE / SET / DELETE
# ---------------------------------------------------------------------------


class TestDML:
    @pytest.fixture(autouse=True)
    def setup_db(self):
        self.db = kyugraph.Database()
        self.conn = self.db.connect()
        self.conn.execute(
            "CREATE NODE TABLE Person "
            "(id INT64, name STRING, age INT64, PRIMARY KEY (id))"
        )

    def test_create_node(self):
        self.conn.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
        result = self.conn.query("MATCH (p:Person) RETURN p.name")
        assert result.num_rows() == 1
        rows = result.to_list()
        assert rows[0][0] == "Alice"

    def test_create_multiple_nodes(self):
        self.conn.execute(
            "CREATE (a:Person {id: 1, name: 'Alice', age: 30}), "
            "(b:Person {id: 2, name: 'Bob', age: 25})"
        )
        result = self.conn.query("MATCH (p:Person) RETURN p.name")
        assert result.num_rows() == 2

    def test_create_and_return(self):
        result = self.conn.query(
            "CREATE (n:Person {id: 1, name: 'Alice', age: 30}) RETURN n.name, n.age"
        )
        assert result.num_rows() == 1
        row = result.to_list()[0]
        assert row[0] == "Alice"
        assert row[1] == 30

    def test_create_partial_properties(self):
        self.conn.execute("CREATE (n:Person {id: 1})")
        result = self.conn.query("MATCH (p:Person) RETURN p.name")
        rows = result.to_list()
        assert rows[0][0] is None

    def test_set_property(self):
        self.conn.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 25})")
        self.conn.execute(
            "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31"
        )
        result = self.conn.query("MATCH (p:Person) RETURN p.age")
        assert result.to_list()[0][0] == 31

    def test_set_with_where(self):
        self.conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})")
        self.conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})")
        self.conn.execute(
            "MATCH (p:Person) WHERE p.id = 1 SET p.age = 26"
        )
        result = self.conn.query(
            "MATCH (p:Person) RETURN p.name, p.age"
        )
        rows = result.to_list()
        ages = {r[0]: r[1] for r in rows}
        assert ages["Alice"] == 26
        assert ages["Bob"] == 30

    def test_delete_node(self):
        self.conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})")
        self.conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})")
        self.conn.execute(
            "MATCH (p:Person) WHERE p.name = 'Alice' DELETE p"
        )
        result = self.conn.query("MATCH (p:Person) RETURN p.name")
        assert result.num_rows() == 1
        assert result.to_list()[0][0] == "Bob"

    def test_delete_all(self):
        self.conn.execute("CREATE (a:Person {id: 1, name: 'Alice', age: 25})")
        self.conn.execute("CREATE (b:Person {id: 2, name: 'Bob', age: 30})")
        self.conn.execute("MATCH (p:Person) DELETE p")
        result = self.conn.query("MATCH (p:Person) RETURN p.id")
        assert result.num_rows() == 0


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


class TestQuery:
    def test_return_literal(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 1 AS x")
        assert result.num_rows() == 1
        assert result.to_list()[0][0] == 1

    def test_return_arithmetic(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 2 + 3 AS sum")
        assert result.to_list()[0][0] == 5

    def test_return_string(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 'hello' AS greeting")
        assert result.to_list()[0][0] == "hello"

    def test_parse_error(self):
        db = kyugraph.Database()
        conn = db.connect()
        with pytest.raises(RuntimeError):
            conn.query("THIS IS NOT VALID CYPHER !!!")

    def test_scan_with_filter(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, age INT64, PRIMARY KEY (id))"
        )
        conn.execute("CREATE (a:Person {id: 1, age: 20})")
        conn.execute("CREATE (b:Person {id: 2, age: 35})")
        conn.execute("CREATE (c:Person {id: 3, age: 50})")
        result = conn.query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.id"
        )
        assert result.num_rows() == 2


# ---------------------------------------------------------------------------
# QueryResult API
# ---------------------------------------------------------------------------


class TestQueryResult:
    @pytest.fixture(autouse=True)
    def setup_result(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))"
        )
        conn.execute("CREATE (a:Person {id: 1, name: 'Alice'})")
        conn.execute("CREATE (b:Person {id: 2, name: 'Bob'})")
        self.result = conn.query(
            "MATCH (p:Person) RETURN p.id, p.name"
        )

    def test_column_names(self):
        assert self.result.column_names() == ["id", "name"]

    def test_num_rows(self):
        assert self.result.num_rows() == 2

    def test_num_columns(self):
        assert self.result.num_columns() == 2

    def test_len(self):
        assert len(self.result) == 2

    def test_to_list(self):
        rows = self.result.to_list()
        assert len(rows) == 2
        ids = {r[0] for r in rows}
        assert ids == {1, 2}

    def test_iteration(self):
        rows = list(self.result)
        assert len(rows) == 2
        names = {r[1] for r in rows}
        assert names == {"Alice", "Bob"}

    def test_repr(self):
        r = repr(self.result)
        assert "columns=2" in r
        assert "rows=2" in r


# ---------------------------------------------------------------------------
# Type round-trips
# ---------------------------------------------------------------------------


class TestTypes:
    def test_int64(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 42 AS x")
        assert result.to_list()[0][0] == 42
        assert isinstance(result.to_list()[0][0], int)

    def test_double(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 3.14 AS x")
        val = result.to_list()[0][0]
        assert abs(val - 3.14) < 1e-9
        assert isinstance(val, float)

    def test_string(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN 'hello world' AS x")
        assert result.to_list()[0][0] == "hello world"

    def test_bool_true(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN true AS x")
        val = result.to_list()[0][0]
        assert val is True

    def test_bool_false(self):
        db = kyugraph.Database()
        conn = db.connect()
        result = conn.query("RETURN false AS x")
        val = result.to_list()[0][0]
        assert val is False

    def test_null(self):
        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE T (id INT64, v STRING, PRIMARY KEY (id))"
        )
        conn.execute("CREATE (n:T {id: 1})")
        result = conn.query("MATCH (t:T) RETURN t.v")
        assert result.to_list()[0][0] is None


# ---------------------------------------------------------------------------
# COPY FROM CSV
# ---------------------------------------------------------------------------


class TestCopyFrom:
    def test_csv_import(self, tmp_path):
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

        db = kyugraph.Database()
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))"
        )
        conn.execute(f"COPY Person FROM '{csv_file}'")

        result = conn.query("MATCH (p:Person) RETURN p.id, p.name")
        assert result.num_rows() == 3
        names = {r[1] for r in result.to_list()}
        assert names == {"Alice", "Bob", "Charlie"}


# ---------------------------------------------------------------------------
# Multiple connections
# ---------------------------------------------------------------------------


class TestConnections:
    def test_shared_state(self):
        db = kyugraph.Database()
        conn1 = db.connect()
        conn2 = db.connect()

        conn1.execute(
            "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))"
        )
        conn1.execute("CREATE (n:Person {id: 1})")

        # conn2 sees conn1's writes.
        result = conn2.query("MATCH (p:Person) RETURN p.id")
        assert result.num_rows() == 1
