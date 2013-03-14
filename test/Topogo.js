
var _      = require('underscore')
, Topogo   = require('topogo').Topogo
, River    = require('da_river').River
, assert   = require('assert')
;

var table = Topogo.test_table_name;
var T     = Topogo.new(table);
var Q     = T.pool();
var no_fin = function () {};

describe( 'Topogo', function () {

  before(function (done) {
    Topogo.run("CREATE TABLE IF NOT EXISTS " + table +
               " ( id serial PRIMARY KEY, name varchar(10), " +
               " body text , " +
               " trashed_at BIGINT );", [], redo(done));
  });

  var name = "ro ro" + rand();
  var body = "body: " + rand();
  var id   = "wrong_id";

  before(function (done) {
    Topogo.run('INSERT INTO ' + table +  ' (name, body) VALUES ($1, $2) RETURNING * ;',
               [name, body], swap(done, function (j) {
                 id = j.result[0].id;
               }));
  });

  after(function (done) {
    Topogo.run("DELETE FROM " + table + ";", [], swap(done, function (results, err) {
      if (err)
        throw err;
    }));
  });

  describe( '.run', function () {
    it( 'uses process.DATABASE_URL by default', function (done) {
      Topogo.run("SELECT now()", {}, swap(done, function (result) {
        assert.equal(is_date(result.result[0].now), true);
      }));
    });
  }); // === end desc

  // ****************************************************************
  // ****************** CREATE **************************************
  // ****************************************************************


  describe( '.create', function () {
    it( 'inserts object as row', function (done) {
      var body = Math.random(1000) + "";
      Topogo.new(table).create({name: "hi 1", body: body}, swap(done, function (j) {
        assert.equal(j.result.id > 0, true);
        assert.equal(j.result.body, body);
      })
      );
    });
  }); // === end desc


  // ****************************************************************
  // ****************** READ ****************************************
  // ****************************************************************

  describe( '.read', function () {

    describe( '.read_by_id', function () {

      it( 'returns a single result', function (done) {
        T.read_by_id(id, swap(done, function (j) {
          assert.equal(j.result.id, id);
          assert.equal(j.result.body, body);
        })
                    );
      });

    }); // === end desc

    describe( '.read_one', function () {

      it( 'returns a single result', function (done) {
        T.read_one({body: body}, flow(function (j) {
          assert.equal(j.result.id, id);
          done();
        }));
      });
    }); // === end desc

    describe( '.read_list', function () {

      it( 'returns a list', function (done) {
        T.read_list({body: body}, flow(function (j) {
          assert.equal(j.result.length, 1);
          done();
        }));
      });
    }); // === end desc

  }); // === end desc



  // ****************************************************************
  // ****************** UPDATE **************************************
  // ****************************************************************


  describe( '.update', function () {

    it( 'updates record with string id', function (done) {
      body = "new body " + rand();
      T.update(id.toString(), {body: body}, flow(function (j) {
        assert.equal(j.result.id, id);
        Q.query('SELECT * from ' + table + ' WHERE body = $1 LIMIT 1;', [body], function (err, result) {
          var row = result.rows[0];
          assert.equal(row.body, body);
          assert.equal(row.id, id);
          done();
        });
      }));
    });

  }); // === end desc

  // ****************************************************************
  // ****************** Trash/Untrash *******************************
  // ****************************************************************

  describe( '.trash', function () {
    it( 'updates column trashed_at to: timestamp epoch', function (done) {

      var l = ((new Date).getTime() + '').length;

      T.trash(id, flow(function (j) {

        assert.equal((j.result.trashed_at+'').length, l);

        T.read_by_id(id, flow(function (j) {
          var val = j.result.trashed_at;
          assert.equal(_.isNumber(val), true);
          assert.equal((val+'').length, l);
          done();
        }));

      }));
    });
  }); // === end desc

  describe( '.untrash', function () {
    it( 'updates column trashed_at to: null', function (done) {
      T.trash(id, flow(function (j) {
        T.untrash(id, flow(function (j) {
          T.read_by_id(id, flow(function (j) {
            assert.equal(j.result.trashed_at, null);
            assert.equal(j.result.id, id);
            done();
          }));
        }));
      }));
    });
  }); // === end desc

  describe( '.delete_trashed', function () {

    it( 'does not delete records younger than days specified', function (done) {
      var day_4 = (new Date).getTime() - (1000 * 60 * 60 * 24 * 4);
      var day_almost_4 = day_4 + 30000;
      T.update(id, {trashed_at: day_almost_4}, flow(function (j) {
        T.delete_trashed(4, flow(function (j) {
          assert.equal(j.result.length, 0);

          T.read_by_id(id, flow(function (j) {
            assert.equal(j.result.id, id);
            done();
          }));

        }));
      }));
    });

    it( 'deletes records older than days specified', function (done) {
      var day_3 = (new Date).getTime() - (1000 * 60 * 60 * 24 * 3);
      T.update(id, {trashed_at: day_3}, flow(function (j) {
        T.delete_trashed(3, flow(function (j) {
          assert.equal(j.result[0].id, id);
          done();
        }));
      }));
    });


  }); // === end desc
}); // === end desc





// ****************************************************************
// ****************** Helpers *************************************
// ****************************************************************


function redo(done) {
  return flow(function () {}, done);
}

function fin(f) {
  return flow(f, function () {});
}

function swap(done, f) {
  return flow(f, done);
}

function flow(f, done) {
  var reps = [function (rep) {
    f(rep);
    if (done)
      done();
  }];
  var fake_job =  {
    replys: [],
    reply : function (func) {
      reps.push(func);
      return this;
    },
    finish : function (results, err) {
      if (err)
        throw err;
      this.result = results;
      this.replys.push(results);
      reps.pop()(this);
      return this;
    }
  };

  return fake_job
}

function is_date(obj) {
  return !!obj.toString().match(/\w+ \w+ \d\d \d+ \d\d/)[0];
}

function rand() {
  return parseInt(Math.random() * 100);
};


