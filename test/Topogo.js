
var _      = require('underscore')
, Topogo       = require('okdoki/lib/Topogo').Topogo
, River    = require('da_river').River
, SQL      = require('okdoki/lib/SQL').SQL
, Customer = require('okdoki/lib/Customer').Customer
, assert   = require('assert')
;

describe( 'PG', function () {

  describe( '.run', function () {
    it( 'removes pass_phrase_hash from each result', function (done) {
      var opts = {
        pass_phrase         : "this is a pass phrase",
        confirm_pass_phrase : "this is a pass phrase",
        ip                  : '000.00.000'
      };

      var opts_1 = _.extend( _.clone(opts), {screen_name: 'r_0_1'});
      var opts_2 = _.extend( _.clone(opts), {screen_name: 'r_0_2'});

      River.new(null)
      .job('create' , '1', [Customer, 'create', opts_1])
      .job('create' , '2', [Customer, 'create', opts_2])
      .job('read', 'customers', function (j) {
        PG.new()
        .q(SQL.select('*').from(Customer.TABLE_NAME))
        .run(function (rows) {
          _.each(rows, function (r) {
            assert.equal(r.hasOwnProperty('pass_phrase_hash'), false);
          })
          j.finish(rows);
        });
      })
      .run(function () {
        done();
      });
    });
  }); // === describe

  describe( '.on_error', function () {

    it( 'executes on error', function (done) {
      PG.new('test on_error')
      .on_error(function (err, meta, me) {
        assert.equal(err.toString(), "error: relation \"no-table\" does not exist");
      })
      .on_error(function (err) {
        done();
      })
      .q('SELECT now() AS TIME')
      .q('SELECT * FROM "no-table";')
      .run(function () { throw new Error('Not suppose to reach here.') }) ;
    });

    it( 'runs both on_error functions and River.job.error', function (done) {
      var val = null;
      River.new(null)
      .on_error(function (err) {
        assert.equal(err.toString(), "error: relation \"no-table\" does not exist");
        assert.equal(val, 'reached');
        done();
      })
      .job(function (j) {
        PG.new('test job', j)
        .on_error(function () {
          val = 'reached';
        })
        .q('SELECT now() AS TIME')
        .q('SELECT * FROM "no-table";')
        .run(function () { throw new Error('Not suppose to reach here.') })
      })
      .run()
      ;
    });

  }); // === describe


  describe( '.on_finish', function () {

    it( 'executes on_finish functions after no more querys to run', function (done) {
      PG.new('test .on_finish')
      .q('SELECT now() AS TIME')
      .q('SELECT current_database() AS NAME')
      .on_finish(function (rows) {
        assert.equal(rows.length, 1);
        done();
      })
      .run();

    });
  }); // === describe



}); // === describe

