var  _  = require('underscore')
, anyDB = require('any-db')
, pool  = {}
, uri   = require('uri-js')
, River = require('da_river').River
;


// ================================================================
// ================== Helpers =====================================
// ================================================================
//

var double_slashs = /\/\//g;
var mb            = function (num) { return num * 1024 * 1024; };
var now           = function () { return (new Date).getTime(); };

function replace_var(target, v_name, str) {
  return target.replace(new RegExp("\\" + v_name + '(?=[^a-zA-Z0-9\\_]|$)', 'g'), str);
}

function doc_to_update_sql(set_doc, where_doc) {
  var vals  = [];
  var ret   = "*";
  if(where_doc.returning) {
    ret = where_doc.returning.join(', ');
    delete where_doc['returning'];
  }

  var sql   = "UPDATE @table SET @set WHERE @where RETURNING " + ret + " ;";
  var set   = [];
  var where = [];

  return {
    sql: sql
    .replace('@set',   doc_to_set(set_doc, vals))
    .replace('@where', doc_to_and(where_doc, vals)),
    vals: vals
  };
}


function doc_to_set(doc, val_arr) {
  var arr = doc_to_equals(doc, val_arr);
  return arr.join(', ');
}

function doc_to_and(doc, val_arr) {
  var arr = doc_to_equals(doc, val_arr);
  return arr.join(' AND ');
}

function doc_to_equals(doc, val_arr) {
  if (!_.isObject(doc))
    doc = {id: doc};
  var i = val_arr.length;
  var sql_vals = [];
  _.each(doc, function (v, k) {
    if (_.isArray(v)) {
      var partial = k + " IN ( ";
      var $nums = [];

      _.each(v, function (arr_v) {
        ++i;
        $nums.push(' $' + i);
        val_arr.push(arr_v);
      });

      partial += ($nums.join(', ') + ' ) ');
      sql_vals.push(partial);
      return;
    }

    if (_.isNull(v)) {
      sql_vals.push( k + " = NULL " );
    } else if ((k.indexOf('_at') > 0) && v === '$now') {
      sql_vals.push( k +  " = (now() AT TIME ZONE 'UTC') ");
    } else {
      ++i;
      sql_vals.push( k + " = $" + i );
      val_arr.push(v);
    }

  });

  return sql_vals;
}


// ================================================================
// ================== Configs =====================================
// ================================================================


var T = exports.Topogo = function () {};
var Topogo = T;
T.test_table_name = "test_test_Topogo_xyz";
T.days_ago = function (i) {
  return new Date( (new Date).getTime() - (1000 * 60 * 60 * 24 * i) );
};
T.close = function (f) {
  if (_.isNumber(f))
    f = undefined;
  var waits = [];
  _.each(pool, function (v, key) {
    waits.push(key);
    v.close(function () {
      waits.pop();
      if (waits.length === 0 && f)
        f();
    });
  });
};

T.id_size = 26;

T.sql = {
  select_default_owner : "\
    SELECT usename AS owner            \
    FROM pg_database, pg_user          \
    WHERE datname = current_database() \
      AND datdba = usesysid;           \
  ",

  select_databases : "\
    SELECT datname AS name            \
    FROM pg_database                  \
    WHERE datistemplate = false       \
        AND datname LIKE 'custom%';   \
  ",

  // FROM: http://stackoverflow.com/questions/769683/show-tables-in-postgresql
  select_tables : "SELECT table_name AS name                         \
    FROM    information_schema.tables                                \
    WHERE   table_type = 'BASE TABLE'                                \
    AND     table_schema NOT IN ('pg_catalog', 'information_schema') \
  ;",

  // FROM: http://dba.stackexchange.com/questions/22362/how-do-i-list-all-columns-for-a-specified-table
  select_cols : "\
    SELECT column_name                     \n\
    FROM information_schema.columns        \n\
    WHERE table_schema = 'public'          \n\
    AND table_name   = 'NAME'              \n\
  ;"
};

var NAME_WHITELIST = /[^a-zA-Z0-9\_]/g;

T.clean_name = function (s) {
  return s.replace(NAME_WHITELIST, '');
};

T.sql_quote = function (s) {
  var sql_quote = T.sql_quote;
  if (arguments.length > 1)
    return sql_quote(_.toArray(arguments));
  if (s === '*')
    return s;
  if (_.isArray(s) && s.length === 2)
    return sql_quote(s[0]) + '.' + sql_quote(s[1]);
  return '"' + T.clean_name(s) + '"';
};

T.where_readable = function () {
  var tables = (arguments.length === 1 && _.isObject(arguments[0])) ?
    _.values(arguments[0]):
    _.flatten(_.toArray(arguments));
  var sql_quote = T.sql_quote;
  var trash = _.compact(_.map(tables, function (n) {
    if (T._tables[n].indexOf('trashed_at') > -1)
      return sql_quote(n,'trashed_at') + ' IS NULL';
  })).join(' AND ');

  var author = _.compact(_.map(tables, function (n) {
    var t = T._tables[n];
    if (t.indexOf('author_id') > -1)
      return sql_quote(n, 'author_id') + ' IN @sn_ids';
    if (t.indexOf('owner_id') > -1)
      return sql_quote(n, 'owner_id') + ' IN @sn_ids';
  })).join(' AND ');

  var sql = [];
  if (trash.length)
    sql.push( '(' + trash + ')' );
  if (author.length)
    sql.push( '(' + author + ')' );
  return '( ' + sql.join(' OR ') + ' )';
};

// ================================================================
// ================== Describe Database/Tables=====================
// ================================================================

T.tables = function (flow) {
  var info = {};
  River.new(flow)
  .job(function (j) {
    T.run(T.sql.select_tables, [], j);
  })
  .job(function (j, names) {
    var tr = River.new(j);

    _.each(names, function (r) {
      tr.job(function (j2) {
        T.run(T.sql.select_cols.replace(/NAME/g, r.name), [], j2);
      }).job(function (j2, rows) {
        info[r.name] = _.pluck(rows, 'column_name');
        j2.finish(rows);
      });
    });

    tr.job(function (j2) {
      T._tables = info;
      j2.finish(info);
    })
    .run();
  })
  .run();
};


// ================================================================
// ================== Main Stuff ==================================
// ================================================================

T.new = function (name, db) {
  var t   = new T;
  t.table = name;
  t.quoted_table = T.sql_quote( name );
  t.name  = name;
  t.is_topogo = true;

  if (!db) {
    db = process.env.DATABASE_URL;
    if (!pool[db])
      pool[db] = anyDB.createPool(db, {min: 1, max: 5});
  }

  t.pool_key = db;

  return t;
};

T.return_rows_for = ['INSERT', 'SELECT', 'UPDATE', 'DELETE'];

T.prototype.pool = function () {
  return pool[this.pool_key];
};

T.sql_proc = function (func) {
  if (func)
    T._sql_proc = func;
  return T._sql_proc;
};

T.run = function (topogo, q, vars, flow) {
  if (T.sql_proc()) {
    var temp = T.sql_proc()(q, vars, topogo);
    q      = temp[0];
    vars   = temp[1];
    topogo = (temp[2] || topogo);
  }

  var args = _.toArray(arguments);
  var args_l = args.length;

  if (args_l !== 3 && args_l !== 4) {
    var e = new Error('Missing args. There can only be 3 or 4 args.');
    var f = _.last(args);
    if (f.error)
      return f.error(e);
    else
      throw e;
  }

  if (args.length === 3) {
    topogo    = Topogo.new("unknown table");
    var q     = args[0];
    var vars  = args[1];
    var flow  = args[2];
  }

  if (!_.isArray(vars)) {
    var arr  = [];
    var i = 0;
    _.each(vars, function (v, name) {
      if (name === 'TABLES' && _.isObject(v)) {
        _.each(v, function (t_name, v_name) {
          q = replace_var(q, '@' + v_name, T.sql_quote(t_name));
        });
        return;
      }
      if (_.isArray(v)) {
        var arr_sub = [];
        _.each(v, function (v2) {
          ++i;
          arr_sub.push('$' + i);
          arr.push(v2);
        });
        q = replace_var(q, '@' + name, '(' + arr_sub.join(',') + ')');
      } else {
        ++i;
        q = replace_var(q, '@' + name, '$' + i);
        arr.push(v);
      }
    });
    vars = arr;
  }

  q = replace_var(q, '@table', topogo.quoted_table);


  if (q.indexOf('$')) {
    q = q
    .replace(/\$created_at/, 'created_at $now_tz')
    .replace(/\$updated_at/, 'updated_at $null_tz')
    .replace(/\$trashed_at/, 'trashed_at $null_tz')

    .replace(/\$owner_able/, "$owner_id , \nis_owner_trashed boolean DEFAULT FALSE")
    .replace(/\$author_able/, "$author_id , \nis_author_trashed boolean DEFAULT FALSE")

    .replace(/\$now_tz/gi,  'timestamptz DEFAULT $now')
    .replace(/\$null_tz/gi, 'timestamptz DEFAULT NULL')
    .replace(/\$now/gi,     '(NOW() AT TIME ZONE \'UTC\')')

    .replace(/\$id_type/, "int DEFAULT 0 NOT NULL")
    .replace(/\$owner_id/, "owner_id int DEFAULT 0 NOT NULL")
    .replace(/\$author_id/, "author_id int DEFAULT 0 NOT NULL")
    ;
  }

  pool[topogo.pool_key].query(q, vars, function (err, result) {
    if (err) {

      if (err.detail && topogo.on_dup_func) {

        topogo.dup_name = replace_var(topogo.dup_name, '@table', topogo.table);
        topogo.dup_name = replace_var(topogo.dup_name, '$table', topogo.table);

        if (err.detail.indexOf("Key (" + topogo.dup_name + ")=") > -1 &&
            err.detail.indexOf(") already exists") > 0)
          return topogo.on_dup_func(topogo.dup_name);

        if (err.message.indexOf("duplicate key value violates unique constraint") > -1 &&
            err.message.indexOf('"' + topogo.dup_name + '"') > 33)
          return topogo.on_dup_func(topogo.dup_name);

      }

      if (process.env.IS_TESTING || process.env.IS_DEV)
        console['log'](q);

      return flow.finish('error', err);
    }

    if (result && _.contains(T.return_rows_for, result.command) && result.rows)
      return flow.finish(result.rows);
    else
      return flow.finish(result);
  });
};

T.prototype.on_dup = function (k, f) {
  this.dup_name = k;
  this.on_dup_func = f;
  return this;
};

T.prototype.toString = function () {
  return "Topogo (instance, table: " + this.table + ")";
};

T.prototype.run = function (sql, vals, flow) {
  return T.run(this, sql, vals, flow);
};

T.show_tables = function () {
  var sql  = T.sql.select_tables
  , on_fin = null
  , flow   = null;

  _.each(arguments, function (v) {
    if (_.isString(v))
      sql += ' ' + v;
    else if (_.isFunction(v))
      on_fin = v;
    else
      flow = v;
  });

  var db = T.run(T.new('no table'), sql, [], {
    finish: function (rows, err) {
      var tables = _.pluck(rows, 'name');
      if (err)
        flow.finish('error', err);
      if (on_fin)
        return on_fin(tables);
      if (flow)
        return flow.finish(tables);
  }});

  return db;
};

// ================================================================
// ================== SQL Helpers =================================
// ================================================================

var Select = function () {};

Select.new = function () {
  var s = new Select;
  s._select = "";
  s._from   = null;
  s._where  = null;
  s._limit  = "15";
  s._order_by = null;
  s._vals   = [];

  var args = _.toArray(arguments);
  s._select =  _.map(arguments, function (s, i) {
    if (_.isString(s))
      return s;
    return T.sql_quote(s[0], s[1]);
  }).join(', ');

  return s;
};

Select.prototype.from = function () {
  var from = "";
  var stack = [];
  _.each(arguments, function (v) {
    if (!_.isString(v))
      return stack.push(v);
    if (v.toLowerCase() === 'inner join')
      return (from = inner_join(from, stack));
    return (from += ' ' + T.sql_quote(v) + ' ');
  });

  this._from = from;
  return this;
};

function where_sub(v, vals) {
  if (v.length === 3) {
    vals.push(v[2]);
    return T.sql_quote(v[0]) + ' ' + v[1] + ' $' + vals.length;
  }
  return T.sql_quote(v.shift()) + ' ' + v.join( ' ' );
}

Select.prototype.where = function () {
  var stack = [];
  var vals  = this._vals;
  _.each(arguments, function (v) {
    if (_.isArray(v))
      return stack.push(where_sub(v, vals));
    return stack.push(v);
  });
  this._where = stack.join(' ');
  return this;
};

Select.prototype.end = function () {
  var sql = ["SELECT ", this._select, " FROM ", this._from];
  if (this._where) {
    sql.push("WHERE");
    sql.push(this._where);
  }
  if (this._order_by) {
    sql.push('ORDER BY');
    sql.push(this._order_by);
  }
  sql.push('LIMIT');
  sql.push(this._limit);
  return [sql.join("\n"), []];
}

T.select_as = function () {
  var args = _.flatten(_.toArray(arguments));
  var t = args.shift();
  return _.map(args, function (f, i) {
    return T.sql_quote(  t , f ) + ' AS ' + T.sql_quote(t + '_' + f );
  }).join(', ');
};

T.values_from = function (table_name, obj) {
  var prefix = table_name + '_';
  var o = {};
  _.each(obj, function (v, k) {
    if (k.indexOf(prefix) === 0) {
      o[k.replace(prefix, '')] = v;
    }
  });
  return o;
};

T.select = function () {
  return Select.new.apply(Select, arguments);
};

function inner_join(from, stack) {
  var t = [];
  var v = [];
  var fin = "";
  _.each(stack, function (p) {
    if (_.isArray(p)) {
      t.push( T.sql_quote( p[0] ) );
      v.push( T.sql_quote(  p[0] , p[1] ) );
    }
  });

  if (from.trim().length === 0) {
    fin = t.join(' INNER JOIN ') + ' ON ' + v.join(' = ');
  } else {
    fin = from + " INNER JOIN " + t[0] + " ON " + v.join(' = ');
  }

  return fin;
}

// ================================================================
// ================== CREATE ======================================
// ================================================================

T.prototype.create = function (data, flow) {
  var me = this;

  var cols = [], vals = [], places = [], i = 0;
  _.each(data, function (v, col) {
    cols.push(col);
    vals.push(v);
    ++i;
    places.push('$' + i);
  });

  var sql = "\
    INSERT INTO @table (@cols)   \
    VALUES (@vals)               \
    RETURNING * ;                \
  "
  .replace('@table', me.quoted_table)
  .replace('@cols', cols.join(', '))
  .replace('@vals', places.join(', '));

  T.run(me, sql, vals, flow.reply(function (j) {
    j.finish(j.result[0]);
  }));
};




// ================================================================
// ================== READ ========================================
// ================================================================


T.prototype.read_by_id = function (id, flow) {
  return this.read_one({id: id}, flow);
};

T.prototype.read_one = function (doc, flow) {
  var me   = this;
  var vals = [];
  var sql  = "SELECT * FROM @table WHERE @vals LIMIT 1 ;"
  .replace('@table', me.quoted_table)
  .replace('@vals', doc_to_and(doc, vals));

  T.run(me, sql, vals, flow.reply(function (j, last) {
    j.finish(last[0]);
  }));
};

T.prototype.read_list = function (doc, flow) {
  var me   = this;
  var vals = [];
  var sql  = "SELECT * FROM @table WHERE @vals ;"
  .replace('@table', me.quoted_table)
  .replace('@vals', doc_to_and(doc, vals));

  T.run(me, sql, vals, flow);
};




// ================================================================
// ================== UPDATE ======================================
// ================================================================

T.prototype.update = function (where, set, flow) {

  if (_.isString(where) || _.isNumber(where))
    where = {id: where};

  var me = this;
  var sql_and_vals = doc_to_update_sql(set, where);
  var vals         = sql_and_vals.vals;
  var sql          = sql_and_vals.sql.replace('@table', me.quoted_table);

  T.run(me, sql, vals, flow.reply(function (j) {
    if (where.id)
      flow.finish(j.result[0]);
    else
      flow.finish(j.result);
  }));
};

T.prototype.update_and_stamp = function (where, set, flow) {
  set['updated_at'] = '$now';
  return this.update(where, set, flow);
};

// ================================================================
// ================== Trash/Untrash================================
// ================================================================

T.prototype.untrash = function (id, j) {
  var me = this;
  me.update(id, {trashed_at: null}, j);
};

T.prototype.trash = function (id, flow) {
  var me = this;
  me.run("UPDATE " + me.quoted_table +  " SET trashed_at = $now WHERE id = $1 RETURNING id, trashed_at;", [id], flow.reply(function (j, last) {
    j.finish(last[0]);
  }));
};

T.prototype.trash_list = function (data, flow) {
  var me = this;
  if (!data.returning)
    data.returning = ['follower_id'];
  me.update(data, {trashed_at: '$now'}, flow);
};

// ================================================================
// ================== DELETE ======================================
// ================================================================

T.prototype.delete_trashed = function (days, flow) {
  if (arguments.length < 2) {
    flow = arguments[0];
    days = 2
  }
  var time = T.days_ago(days);

  var sql = "\
    DELETE FROM " + this.quoted_table + "         \
    WHERE trashed_at IS NOT NULL AND              \
          trashed_at < $1    \
    RETURNING *;        \
  ";

  T.run(this, sql, [ time ], flow);
  return this;
};



// ================================================================
// ================== OLD CODE ====================================
// ================================================================


T.rollback = function rollback(client, on_err) {
  return function (err, meta) {
    if (err) {
      client.query('ROLLBACK', [], function (new_err) {
        client.end();
        if (on_err)
          on_err(new_err || err);
        else
          throw (new_err || err);
      });
    };

  };
};















