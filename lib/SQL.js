var _ = require('underscore')
;

var SQL = exports.SQL = function () {
};

SQL.new = function () {
  var sql = Query.new.apply(Query, arguments);
  return sql;
};

_.each('select insert_into delete_from update trash untrash'.split(' '), function (name) {
  SQL[name] = function () {
    var sql = SQL.new();
    return sql[name].apply( sql, arguments );
  };
});

function trim(arr) {
  return _.map(arr, function (v) { return v.trim(); });
}

// ****************************************************************
// ****************** Helpers *************************************
// ****************************************************************

SQL.now = "(now() AT TIME ZONE 'UTC')";

SQL.interval = function (str) {
  return "'" + str + "'::INTERVAL";
}

SQL.now_minus = function (str) {
  return '(' + SQL.now + ' - ' + SQL.interval(str) + ')';
};

SQL.join_array_by_comma = function (arr, vals) {
  var sql = [];
  _.each(arr, function(v, i) {
     vals.push(v);
     sql.push( '$' + ( vals.length ) );
  });

  return sql.join(', ');
};

function To_SQL(v, memo) {
  if( _.isString(v) )
    return v;

  if(v.is_join)
    return v.to_sql(memo)[0];

  return Query_To_SQL(v);
}

function Query_To_WHERE(data, table_name) {
  var vals       = [];
  var where      = [];
  var update     = [];
  var set_offset = 0;
  var curr_vals  = null;

  if (data.update_set) {
    _.each(data.update_set, function (v, key) {
      if (key === 'trashed_at' && v === 'now()') {
        update.push(key + " = (now() AT TIME ZONE 'UTC')");
      } else if (_.isArray(v)) {
        var new_vals = new Array();
        if (!_.isArray(v[1]))
          v[1] = [v[1]];
        update.push((key + " = " + v[0]).replace(/\$[0-9]+/ig, function (sub) {
          var num = parseInt(sub.replace('$', ''));
          set_offset += num;
          new_vals[num - 1] = v[1][num - 1];
          return '$' + (set_offset);
        }));
        vals = vals.concat(new_vals);
      } else {
        set_offset += 1;
        update.push(key + " = $" + set_offset);
        vals.push(v);
      }
    });
  }

  _.each(data.where, function (v, i) {
    v         = v.trim();
    curr_vals = data.vals[i];

    if (v.indexOf('$') === -1 && curr_vals.length === 1)
      v = v + ' = $1 '

    where.push(
      add_table_name(table_name, v).replace(/\$[0-9]+/g, function (sub) {
      return '$' + (parseInt( sub.replace('$', '') ) + vals.length);
    }));

    vals = vals.concat(curr_vals);
  });

  return [where.join(' '), vals, update.join(', ')];
}

function Query_To_SQL(sql_query) {
  var sql        = [];
  var data       = sql_query.data();
  var table_name = sql_query.table_name;
  var meta = {vals : data.vals};

  if (data.select.length) {
    sql.push('SELECT ' + add_table_name_to_each(table_name || '', data.select)
    .join(', ') );

    if (data.from.length) {
      sql
      .push( 'FROM ' + _.inject(data.from, function (memo, v) {
        return To_SQL(v, memo);
      }, null))
      ;
    }

    if (data.where.length) {
      sql.push( "WHERE ");

      var temp = Query_To_WHERE(data, table_name);
      sql.push( temp[0] );
      meta.vals = temp[1];
    }

    if (data.limit.length > 0)
      sql.push( "LIMIT " + data.limit.join(', ') );

  } // ==== end SELECT

  if (data.insert_values.length) {
    sql.push('INSERT INTO ' + table_name);
    sql.push('( ' + _.pluck(data.insert_values, 0).join(', ') + ' )');
    sql.push('VALUES ( ' + _.map(data.insert_values, function (pair, i) {

      var key = pair[0], v = pair[1];

      if (!_.isArray(v)) {
        meta.vals.push(v);
        return '$' + ( meta.vals.length );
      }

      var fragment = v[0];
      var vals = v[1];
      if(!_.isArray(vals)) {
        vals = [vals];
      }

      var new_vals = new Array();
      var final_fragment = fragment.replace(/\$[0-9]+/g, function (sub) {
        var orig_n = parseInt( sub.replace('$', '') );
        var new_n  = (orig_n + meta.vals.length);
        new_vals[orig_n - 1] = vals[orig_n - 1];
        return '$' + new_n;
      });

      meta.vals = meta.vals.concat(new_vals);
      return final_fragment;

    }).join(', ') + ' )');

    sql.push('RETURNING * ');
  }

  if (sql_query.is_delete) {
    sql.push('DELETE FROM ' + table_name);

    var temp = Query_To_WHERE(data, table_name);
    sql.push('WHERE ' + temp[0]);
    meta.vals = temp[1];
  }

  if (sql_query.is_update) {
    sql.push('UPDATE ' + table_name);
    var temp = Query_To_WHERE(data, table_name);
    sql.push('SET ' + temp[2]);
    sql.push('WHERE ' + temp[0]);
    sql.push(' RETURNING * ');
    meta.vals = temp[1];
  }

  sql = sql.join("\n");

  sql += " ;";

  var limit_1 = _.last(data.limit) === 1;

  return {
    sql       : sql,
    vals      : meta.vals,
    is_select : sql_query.is_select,
    limit_1   : limit_1,
    row_1     : sql_query.is_insert || sql_query.is_update || limit_1
  };
}

function standard_var_names(wheres, vals) {
  _.each(wheres, function (raw_sql, i) {
    s_vals = s_vals.concat(vals[i]);

    s_where[i] = raw_sql.replace(/\$[0-9]+/g, function (sub, pos, full_string) {
      var orig_num = parseInt(sub.replace('$', ''));
      var new_num  = orig_num + length;
      return '$' + new_num;
    });

    length = s_vals.length;
  });
}

function add_table_name(name, v) {
  name = name.trim();
  v = v.trim();
  if (v.indexOf('.') === 0)
    v = name + v;
  return v;
}

function add_table_name_to_each(name, arr) {
  return _.map(arr, function (v) {
    return add_table_name(name, v);
  });
}

// ****************************************************************
// ****************** Query ***************************************
// ****************************************************************

var Query = function (parent) {
  this.parent     = parent;
  this.table_name = (parent && parent.table_name) || null;
  this.is_query   = true;
  this.is_delete  = false;
  this.is_update  = false;

  this.d = {};
  this.d.select        = [];
  this.d.from          = [];
  this.d.where         = [];
  this.d.vals          = [];
  this.d.join          = [];
  this.d.limit         = [];
  this.d.insert_values = [];
};

Query.new = function (parent) {
  var t = new Query(parent);
  return t;
};

Query.prototype.insert_into = function (name) {
  this.is_insert = true;
  return this.from(name);
};

Query.prototype.value = function (name, val) {
  this.d.insert_values.push([name, val]);
  return this;
};

Query.prototype.values = function (o) {
  var me = this;
  _.each(o, function (v, key) {
    me.value(key, v);
  });
  return this;
};

Query.prototype.select = function () {
  this.d.select = this.d.select.concat(_.toArray(arguments));
  this.is_select = true;
  return this;
};

Query.prototype.from = function (o) {
  this.d.from.push(o);
  if (!this.table_name && _.isString(o))
    this.table_name = o;
  return this;
};

Query.prototype.where = function (sql, vals) {
  this.d.where.push(sql);
  if (arguments.length > 1 && !_.isArray(vals))
    vals = [vals];
  this.d.vals.push(vals || []);

  return this;
};

Query.prototype.and = function (sql, vals) {
  this.where(' AND ' + sql, vals);

  return this;
};

Query.prototype.or = function (sql, vals) {
  this.where(' OR ' + sql, vals);

  return this;
};

_.each('left right inner'.split(' '), function (name) {
  name = name || 'inner';
  Query.prototype[name + '_join'] = function (sql) {
    var lj = Join.new(name, sql, this);
    this.d.from.push(lj);
    return this;
  };
});

_.each('as on'.split(' '), function (name) {
  Query.prototype[name] = function () {
    var j = _.last(this.d.from);
    j[name].apply(j, arguments);
    return this;
  };
});

Query.prototype.limit = function (start, quantity) {
  this.d.limit = _.compact([start, quantity]);
  return this;
};

Query.prototype.delete_from = function (name) {
  this.is_delete = true;
  return this.from(name);
};

Query.prototype.update = function (name) {
  this.is_update = true;
  return this.from(name);
};

Query.prototype.set = function (o) {
  this.d.update_set = _.extend(this.d.update_set || {}, o);
  return this;
};

Query.prototype.trash = function (name) {
  this.update(name);
  this.set({trashed_at: 'now()'});
  return this;
};

Query.prototype.data = function () {
  var data = _.clone(this.d);
  if (!this.parent)
    return data;

  // Merge two datas together:
  var p_data = this.parent.data();
  _.each(p_data, function (v, key) {
    if (_.isArray(v)) {
      data[key] = p_data[key].concat(data[key]);

      if (key === 'where' && p_data['where'].length > 0 && data['where'].length > 0) {
        data[key][p_data['where'].length] = ' AND ' + data[key][p_data['where'].length];
      }
      return;
    }

    if (_.isObject(v)) {
      data[key] = _.extend(p_data[key], data[key]);
      return;
    }

    return;

  });

  return data;
};

Query.prototype.to_sql = function () {
  return Query_To_SQL(this);
};

// ****************************************************************
// ****************** Join ****************************************
// ****************************************************************

var Join = function (dir, sql) {
  this.d          = {};
  this.d.dir      = dir.toUpperCase();
  this.d.sql      = sql;
  this.d.on       = [];
  this.d.vals     = [];
  this.table_name = sql.table_name || sql;
  this.is_join    = true;
};

Join.new = function (dir, sql, parent) {
  var j = new Join(dir, sql, parent);
  return j;
};

Join.prototype.on = function (left, right, vals) {
  this.d.on.push( [left, right] );
  if( vals )
    this.d.vals.push(vals);
  return this;
};

Join.prototype.as = function (name) {
  this.table_name = name;
  return this;
};

Join.prototype.to_sql = function (name) {
  var me = this;

  var sql = "( " + name + ' ' + this.d.dir + ' JOIN ' + me.table_name + ' ON ';

  var ons = [];
  _.each(this.d.on, function (pair, i) {
    ons.push(add_table_name(name, pair[0]) + ' = ' + add_table_name(me.table_name, pair[1]));
  });

  if (me.d.sql) {
    if (me.d.sql.is_query)
      ons = ons.concat(Query_To_WHERE(me.d.sql.data(), this.table_name)[0]);
  }

  sql += ons.join(' AND ') +  ' )';
  return [sql, this.d.vals];
};











































