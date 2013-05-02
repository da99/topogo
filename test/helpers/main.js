var _         = require('underscore')
, Topogo      = require('topogo').Topogo
;

process.on('SIGTERM', Topogo.close);
process.on('SIGINT',  Topogo.close);
process.on('exit',    Topogo.close);

Topogo.prototype.drop = function (flow) {
  var me = this;
  return Topogo.run(me, "DROP TABLE IF EXISTS " + me.table + '; ', [], flow);
};

Topogo.prototype.delete_all = function (flow) {
  var sql = 'DELETE FROM ' + this.table + ' ;';
  return Topogo.run(this, sql, [], flow);
};

var o = module.exports = {};

o.throw_it = function () {
  throw new Error(arguments.toString());
}

o.utc_timestamp = function () {
  var d = new Date;
  return (d.getTime() + d.getTimezoneOffset()*60*1000);
}

o.utc_diff = function (date) {
  if (date && date.getTime)
    date = date.getTime();
  return exports.utc_timestamp() - date;
}

o.is_recent = function (date) {
  if (_.isNumber(date) && !_.isNaN(date))
    return ((new Date()).getTime() - date) < 500;
  return exports.utc_diff(date) < 1000;
}

// ****************************************************************
// ****************** Main Helpers ********************************
// ****************************************************************

o.redo = function (done) {
  return flow(function () {}, done);
}

o.fin = function (f) {
  return flow(f, function () {});
}

o.swap = function (done, f) {
  return flow(f, done);
}

o.flow = function (f, done) {
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

o.is_date = function (obj) {
  return !!obj.toString().match(/\w+ \w+ \d\d \d+ \d\d/)[0];
}

o.rand = function () {
  return parseInt(Math.random() * 100);
};

o.days_ago = function (i, almost) {
  return new Date((new Date).getTime() - (1000 * 60 * 60 * 24 * i) + (almost || 0));
}












