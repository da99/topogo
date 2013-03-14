
var _      = require('underscore')
, Topogo   = require('topogo').Topogo
, River    = require('da_river').River
, assert   = require('assert')
;

describe( 'Topogo', function () {

  describe( '.run', function () {
    it( 'uses process.DATABASE_URL by default', function (done) {
      Topogo.run("SELECT now()", {}, flow(done, function (result) {
        assert.equal(is_date(result[0].now), true);
      }));
    });
  }); // === end desc

}); // === end desc





// ****************************************************************
// ****************** Helpers *************************************
// ****************************************************************


function flow(done, f) {
  return {
    finish : function () {
      f.apply(null, arguments);
      done();
    }
  };
}

function is_date(obj) {
  return !!obj.toString().match(/\w+ \w+ \d\d \d+ \d\d/)[0];
}
