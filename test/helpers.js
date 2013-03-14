var _         = require('underscore')
, River       = require('da_river').River
, Topogo      = require('topogo').Topogo
, reltime = require('reltime')
;


process.on('SIGTERM', Topogo.close);
process.on('SIGINT',  Topogo.close);
process.on('exit',    Topogo.close);

exports.throw_it = function () {
  throw new Error(arguments[0].toString());
  return false;
}

exports.utc_timestamp = function () {
  var d = new Date;
 return (d.getTime() + d.getTimezoneOffset()*60*1000);
}

exports.utc_diff = function (date) {
  if (date && date.getTime)
    date = date.getTime();
  return exports.utc_timestamp() - date;
}
exports.is_recent = function (date) {
  if (_.isNumber(date) && !_.isNaN(date))
    return ((new Date()).getTime() - date) < 500;
  return exports.utc_diff(date) < 1000;
}

exports.ago = function (english) {
  switch (english) {
    case '-1d -22h':
      exports.utc_timestamp() - (1000 * 60 * 60 * 24) - (1000 * 60 * 60 *22);
      break;
    case '-3d':
      exports.utc_timestamp() - (1000 * 60 * 60 * 24 * 3);
      break;
    default:
      throw new Error('Unknown: ' + english);
  };
  return reltime.parse((new Date), english).getTime();
};

Topogo.prototype.drop = function (flow) {
  var me = this;
  return Topogo.run(me, "DROP TABLE " + me.table + '; ', [], flow);
};

Topogo.prototype.delete_all = function (flow) {
  var sql = 'DELETE FROM ' + this.table + ' ;';
  return Topogo.run(this, sql, [], flow);
};
