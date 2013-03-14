
var _      = require('underscore')
, Topogo   = require('topogo').Topogo
, River    = require('da_river').River
, assert   = require('assert')
;

var table = "test_test_Topogo_xyz";

describe( 'Topogo', function () {

  before(function (done) {
    Topogo.run("CREATE TABLE IF NOT EXISTS " + table +
               " ( my_id serial PRIMARY KEY, name varchar(10), " + 
               " body text );", [], flow(done, function (results) {
    }));
  });

  after(function (done) {
    Topogo.run("DELETE FROM " + table + ";", [], flow(done, function (results, err) {
      if (err)
        throw err;
    }));
  });

  describe( '.run', function () {
    it( 'uses process.DATABASE_URL by default', function (done) {
      Topogo.run("SELECT now()", {}, flow(done, function (result) {
        assert.equal(is_date(result.result[0].now), true);
      }));
    });
  }); // === end desc

  describe( 'Topogo .create', function () {
    it( 'inserts object as row', function (done) {
      var body = Math.random(1000) + "";
      Topogo.new(table).create({name: "hi 1", body: body}, flow(done, function (j) {
        assert.equal(j.result.my_id > 0, true);
        assert.equal(j.result.body, body);
      })
      );
    });
  }); // === end desc
}); // === end desc





// ****************************************************************
// ****************** Helpers *************************************
// ****************************************************************


function flow(done, f) {
  var reps = [function (rep) {
    f(rep);
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
