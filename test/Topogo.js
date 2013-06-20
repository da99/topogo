
var _      = require('underscore')
, Topogo   = require('../lib/Topogo').Topogo
, H        = require('./helpers/main')
, River    = require('da_river').River
, assert   = require('assert')
;

var table = Topogo.test_table_name.toUpperCase();
var T     = Topogo.new(table);
var Q     = T.pool();
var no_fin = function () {};

var R = function (done) {
  var r = River.new(null);
  return {
    job : function () {
      r.job.apply(r, arguments);
      return this;
    },
    run : function () {
      r.job(function () {
        done();
      });
      r.run.apply(r, arguments);
      return this;
    }
  };
};

function is_recent(date) {
  return ((new Date).getTime() - date.getTime()) < 80;
}

function rand() { return parseInt(Math.random() * 1000); }

function strip(s) {
  return s.trim().split(/[\s\n]+/).join(" ");
}

describe( 'Model:', function () {

  describe( '.sql_quote', function () {
    it( 'removes all instances of invalid chars', function () {
      assert.equal(Topogo.sql_quote("-T", "-id"), '"T"."id"');
    });
  }); // === end desc

  describe( '.select_as', function () {
    it( 'returns a SELECT substring', function () {
      var target = '"Website"."id" AS "Website_id", "Website"."trashed_at" AS "Website_trashed_at"';
      assert.equal(Topogo.select_as('Website', 'id', 'trashed_at'), target);
    });
  }); // === end desc

  describe( '.values_from', function () {
    it( 'returns an object without table prefixes', function () {
      var o = Topogo.values_from('Website', {'Website_id': 1, 'Website_trashed_at': 2});
      assert.deepEqual(o, {id: 1,trashed_at: 2});
    });
  }); // === end desc

  describe( '.select', function () {
    it( 'generates a SELECT statement', function () {
      var target = 'SELECT "T".*, "T"."id", "W"."trashed_at" FROM "T" LIMIT 15';
      var pair = Topogo.select(['T', '*'], ['T', 'id'], ['W', 'trashed_at']).from('T').end();
      assert.equal(strip(pair[0]), strip(target));
    });
  }); // === end desc

  describe( '.from', function () {
    it( 'generates a FROM statement', function () {
      var target = 'SELECT "T".* FROM "T" INNER JOIN "W" ON "T"."id" = "W"."id" LIMIT 15';
      var pair = Topogo.select(['T', '*']).from(['T','id'], ['W','id'], 'inner join').end();
      assert.equal(strip(pair[0]), strip(target));
    });
  }); // === end desc

  describe( '.where', function () {
    it( 'generates a WHERE statement', function () {
      var target = 'SELECT "T".* FROM "T" WHERE "T"."id" = $1 AND "SN"."s" = $2 LIMIT 15';
      var pair = Topogo
      .select(['T', '*'])
      .from('T')
      .where([['T','id'], '=', 0], 'AND', [['SN', 's'], '=', 3])
      .end();

      assert.equal(strip(pair[0]), strip(target));
    });
  }); // === end desc

  describe( '.where_readable', function () {
    after(function () {
      Topogo._tables = {};
    });

    it( 'uses col: trashed_at', function () {
      Topogo._tables = {T: ['trashed_at'], W: ['trashed_at'], Z: []}
      var target = '( ("T"."trashed_at" IS NULL AND "W"."trashed_at" IS NULL) )';
      var val    = Topogo.where_readable('T', 'W', 'Z');
      assert.equal(strip(val), strip(target));
    });

    it( 'uses cols: author_id, owner_id', function () {
      Topogo._tables = {T: ['owner_id'], W: ['author_id'], Z: []}
      var target = '( ("T"."owner_id" IN @sn_ids AND "W"."author_id" IN @sn_ids) )';
      var val    = Topogo.where_readable('T', 'W', 'Z');
      assert.equal(strip(val), strip(target));
    });

    it( 'uses cols: trashed_at, author_id, owner_id', function () {
      Topogo._tables = {T: ['trashed_at', 'owner_id'], W: ['author_id'], Z: ['trashed_at']}
      var target = '( ("T"."trashed_at" IS NULL AND "Z"."trashed_at" IS NULL) OR \
      ("T"."owner_id" IN @sn_ids AND "W"."author_id" IN @sn_ids) )';
      var val    = Topogo.where_readable('T', 'W', 'Z');
      assert.equal(strip(val), strip(target));
    });
  }); // === end desc

}); // === end desc

describe( 'Describe tables:', function () {
  it( 'describes tables', function (done) {
    River.new()
    .job(function (j) {
      Topogo.tables(j);
    })
    .job(function (j, o) {
      assert.deepEqual(o[table], ["id","name","body","created_at","updated_at","trashed_at"]);
      done();
    })
    .run();
  });
}); // === end desc

describe('Topogo:', function () {

  before(function (done) {
    R(done)
    .job(function (j) {
      Topogo.run("DROP TABLE IF EXISTS \"" + table + "\";", [], j);
    })
    .job(function (j) {
      Topogo.run("CREATE TABLE IF NOT EXISTS \"" + table +
                 "\" (\n" +
                 " id serial PRIMARY KEY, \n" +
                 " name varchar(10), \n" +
                 " body text ,   \n" +
                 " website_id  $id_type , \n" +
                 " $owner_able , \n" +
                 " $author_able , \n" +
                 " $created_at , \n" +
                 " $updated_at , \n" +
                 " $trashed_at   \n);", [], j);
    })
    .run();
  });

  var name = "ro ro" + rand();
  var body = "body: " + rand();
  var id   = "wrong_id";



  before(function (done) {
    R(done)
    .job(function (j) {
      var sql = 'INSERT INTO \"' + table +  '\" (name, body) VALUES ($1, $2) RETURNING * ;';
      Topogo.run(sql, [name, body], j);
    })
    .job(function (j, last) {
      id = last[0].id;
      j.finish();
    })
    .run();
  });

  after(function (done) {
    R(done)
    .job(function (j) {
      Topogo.run("DELETE FROM \"" + table + "\";", [], j);
    })
    .run();
  });

  // ================================================================
  // ================== .run ========================================
  // ================================================================
  describe( '.run', function () {
    it( 'uses process.DATABASE_URL by default', function (done) {
      River.new(null)
      .job(function (j) {
        Topogo.run("SELECT now()", {}, j);
      })
      .job(function (j, result) {
        assert.equal(H.is_date(result[0].now), true);
        done();
      })
      .run();
    });

    it( 'replaces all instances of @table with quoted table name', function (done) {
      River.new(null)
      .job(function (j) {
        Topogo.run(Topogo.new(table), "SELECT @table.* FROM @table WHERE name = @name AND name = @name", {name: name}, j);
      })
      .job(function (j, result) {
        assert.equal(result[0].name, name);
        done();
      })
      .run();
    });

    it( 'replaces all instances of values in SELECT', function (done) {
      River.new(null)
      .job(function (j) {
        Topogo.run("SELECT @t.* FROM @t WHERE name = @name AND name = @name", {TABLES: {t: table}, name: name}, j);
      })
      .job(function (j, result) {
        assert.equal(result[0].name, name);
        done();
      })
      .run();
    });

    it( 'replaces vars for arrays with "( $n, $n+1, ...)"', function (done) {
      River.new(null)
      .job(function (j) {
        Topogo.run("SELECT @t.* FROM @t WHERE name IN @names", {TABLES: {t: table}, names: [name, name]}, j);
      })
      .job(function (j, result) {
        assert.equal(result[0].name, name);
        done();
      })
      .run();
    });

    it( 'replaces all instances of values in INSERT statements', function (done) {
      River.new(null)
      .job(function (j) {
        Topogo.run(Topogo.new(table), "INSERT INTO @table (name, body) VALUES (@name, @body) RETURNING * ;", {name: name, body: "123"}, j);
      })
      .job(function (j, result) {
        assert.equal(result[0].name, name);
        assert.equal(result[0].body, '123');
        done();
      })
      .run();
    });

  }); // === end desc

  // ================================================================
  // ================== CREATE ======================================
  // ================================================================


  describe( '.create', function () {
    it( 'inserts object as row', function (done) {
      var body = Math.random(1000) + "";
      River.new(null)
      .job(function (j) {
        Topogo.new(table).create({name: "hi 1", body: body}, j);
      })
      .job(function (j, result) {
        assert.equal(result.id > 0, true);
        assert.equal(result.body, body);
        done();
      })
      .run();
    });

    it( 'runs .on_dup function if duplicate field name is created', function (done) {
      var body = Math.random(1000) + "";
      var t    = Topogo.new(table);
      t.on_dup('id', function (name) {
        assert.equal("id", name);
        done();
      });

      River.new(null)
      .job(function (j) {
        t.create({id: 1, name: "hi 1", body: body}, j);
      })
      .run();
    });
  }); // === end desc


  // ================================================================
  // ================== READ ========================================
  // ================================================================

  describe( '.read', function () {

    describe( '.read_by_id', function () {

      it( 'returns a single result', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_by_id(id, j);
        })
        .job(function (j, result) {
          assert.equal(result.id, id);
          assert.equal(result.body, body);
          done();
        })
        .run();
      });

    }); // === end desc

    describe( '.read_one', function () {

      it( 'returns a single result', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_one({body: body}, j);
        })
        .job(function (j, last) {
          assert.equal(last.id, id);
          done();
        })
        .run();
      });
    }); // === end desc

    describe( '.read_list', function () {

      it( 'returns a list', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_list({body: body}, j);
        })
        .job(function (j, last) {
          assert.equal(last.length, 1);
          done();
        })
        .run();
      });

      it( 'convers id to: {id: id}', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_list(id, j);
        })
        .job(function (j, last) {
          assert.equal(last.length, 1);
          done();
        })
        .run();
      });
    }); // === end desc

  }); // === end desc



  // ================================================================
  // ================== UPDATE ======================================
  // ================================================================


  describe( '.update', function () {

    it( 'updates record with string id', function (done) {
      body = "new body " + rand();
      River.new(null)
      .job(function (j) {
        T.update(id.toString(), {body: body}, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        Q.query('SELECT * from \"' + table + '\" WHERE body = $1 LIMIT 1;', [body], function (err, result) {
          if (err) throw err;
          var row = result.rows[0];
          assert.equal(row.body, body);
          assert.equal(row.id, id);
          done();
        });
      }).run();
    });

  }); // === end desc

  describe( '.update_and_stamp', function () {
    it( 'updates record with time stamp', function (done) {
      body = "new body " + rand();
      River.new(null)
      .job(function (j) {
        T.update_and_stamp(id.toString(), {body: body}, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        Q.query('SELECT * from \"' + table + '\" WHERE body = $1 LIMIT 1;', [body], function (err, result) {
          if (err) throw err;
          var row = result.rows[0];
          assert.equal(row.body, body);
          assert.equal(row.id, id);
          assert.equal(is_recent(row.updated_at), true);
          done();
        });
      }).run();
    });
  }); // === end desc

  // ================================================================
  // ================== Trash/Untrash ===============================
  // ================================================================

  describe( '.trash', function () {
    it( 'updates column trashed_at to: timestamp epoch', function (done) {

      var l = ((new Date).getTime() + '').length;

      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j, last) {
        assert.equal(is_recent(last.trashed_at), true);
        j.finish();
      })
      .job(function (j) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(is_recent(last.trashed_at), true);
        done();
      })
      .run();
    });

    it( 'passes first record to next job', function (done) {
      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });
  }); // === end desc

  describe( '.untrash', function () {
    it( 'updates column trashed_at to: null', function (done) {
      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j) {
        T.untrash(id, j);
      })
      .job(function (j, last) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.trashed_at, null);
        assert.equal(last.id, id);
        done();
      })
      .run();
    });

    it( 'passes first record to next job', function (done) {
      River.new(null)
      .job(function (j) {
        T.untrash(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });
  }); // === end desc

  describe( '.delete_trashed', function () {

    it( 'does not delete records younger than days specified', function (done) {
      var day_4 = H.days_ago(4);
      var day_almost_4 = H.days_ago(4, 3000);

      River.new(null)
      .job(function (j) {
        T.update(id, {trashed_at: day_almost_4}, j);
      })
      .job(function (j, last) {
        T.delete_trashed(4, j);
      })
      .job(function (j, last) {
        assert.equal(last.length, 0);
        j.finish();
      })
      .job(function (j) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });

    it( 'deletes records older than days specified', function (done) {
      var day_3 = H.days_ago(3);
      River.new(null)
      .job(function (j) {
        T.update(id, {trashed_at: day_3}, j);
      })
      .job(function (j, last) {
        T.delete_trashed(3, j);
      })
      .job(function (j, last) {
        assert.equal(last[0].id, id);
        done();
      })
      .run();
    });


  }); // === end desc

}); // describe Topogo





