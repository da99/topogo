
var _    = require('underscore')
, SQL    = require('okdoki/lib/SQL').SQL
, assert = require('assert')
;

function clean(s) {
  return s.trim().split(/\s+/).join(' ');
}

describe( 'SQL: select', function () {

  it( 'generates sql for SELECT', function () {
    var sql = SQL
    .select('*')
    .from('tbl')
    .where('fld = $1', [2])
    ;

    var target_sql = "SELECT * FROM tbl WHERE fld = $1 ;";
    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, [2]);
  });

  it( 'keeps track of var names in WHERE', function () {
    var sql = SQL
    .select('*')
    .from('tbl')
    .where(' f IN [$1, $1, $2]', ['a', 'b'])
    .and(  ' d IN [$1, $2, $2]', ['c', 'd'])
    ;

    var target_sql = "\
    SELECT * \
    FROM tbl \
    WHERE    \
    f IN [$1, $1, $2] \
    AND    \
    d IN [$3, $4, $4] ;";
    var results = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, "a b c d".split(' '));
  });

  it( 'can use another query as a table', function () {
    var target = "SELECT * \
    FROM screen_names \
    WHERE trashed_at IS NULL \
    AND name IS NOT NULL ;";

    var names = SQL.new().from('screen_names').where('trashed_at IS NULL');

    var r = SQL
    .new(names)
    .select('*')
    .where('name IS NOT NULL')
    .to_sql()
    ;

    assert.equal(clean(r.sql), clean(target));
  });

  it( 'can generate LEFT JOIN with ON expression', function () {
    var target = "SELECT * \
    FROM ( \
      customers LEFT JOIN screen_names \
      ON customers.id = screen_names.customer_id \
    ) \
    WHERE customers.name IS NOT NULL ;";

    var names = SQL.new().from('screen_names').where('.trashed_at IS NULL');

    var sql = SQL
    .select('*')
    .from('customers')
      .left_join('screen_names')
        .on('.id', '.customer_id')
    .where('.name IS NOT NULL')
    ;

    var r = sql.to_sql();
    assert.equal(clean(r.sql), clean(target));
  });

  it( 'can generate LEFT JOIN using a SQL.Table instead of a table name string', function () {
    var target = "SELECT * \
    FROM ( customers LEFT JOIN screen_names \
    ON customers.id = screen_names.customer_id \
        AND screen_names.trashed_at IS NULL ) \
    WHERE customers.name IS NOT NULL ;";

    var names = SQL.new().from('screen_names').where('.trashed_at IS NULL');

    var sql = SQL
    .select('*')
    .from('customers')
      .left_join(names)
        .on('.id', '.customer_id')
    .where('.name IS NOT NULL')
    ;

    var r = sql.to_sql();
    assert.equal(clean(r.sql), clean(target));
  });

  it( 'can generate LIMIT expression', function () {
    var sql = SQL
    .select('*')
    .from('tbl')
    .where('fld = $1', [2])
    .limit(1)
    ;

    var target_sql = "SELECT * FROM tbl WHERE fld = $1 LIMIT 1 ;";
    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.equal(results.limit_1, true);
  });
}); // === describe

describe( 'SQL: insert', function () {

  it( 'generates INSERT statement', function () {
    var sql = SQL
    .insert_into('names')
    .value('name', 'okdoki')
    .value('about', 'website')
    ;

    var target_sql = "\
      INSERT INTO names ( name, about ) \
      VALUES ( $1, $2 ) \
      RETURNING * ;";

    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, ['okdoki', 'website']);
    assert.equal(results.row_1, true);
  });

  it( 'generates INSERT statement from a HASH values', function () {
    var sql = SQL
    .insert_into('names')
    .values({name: 'okdoki', about: 'website'})
    ;

    var target_sql = "\
      INSERT INTO names ( name, about ) \
      VALUES ( $1, $2 ) \
      RETURNING * ;";

    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, ['okdoki', 'website']);
  });

  it( 'generates INSERT statement with SQL functions', function () {
    var sql = SQL
    .insert_into('names')
    .values({
      name    : 'okdoki',
      about   : ['upper($1)', 'website'],
      display : ['upper($1, $2)', ['website', 'app']]
    })
    ;

    var target_sql = "\
      INSERT INTO names ( name, about, display ) \
      VALUES ( $1, upper($2), upper($3, $4) ) \
      RETURNING * ;";

    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, ['okdoki', 'website', 'website', 'app']);
  });

}); // === describe

describe( 'SQL: delete', function () {

  it( 'generates DELETE statement', function () {
    var sql = SQL
    .delete_from('names')
    .where('name', 'okdoki')
      .and('about = $1', 'website')
    ;

    var target_sql = "\
      DELETE FROM names \
      WHERE name = $1   \
      AND about = $2  \
      ;";

    var results    = sql.to_sql();
    assert.equal(clean(results.sql), clean(target_sql));
    assert.deepEqual(results.vals, ['okdoki', 'website']);
  });

}); // === describe

describe( 'SQL: update', function () {

  it( 'generates UPDATE statement', function () {
    var sql = SQL
    .update('names')
    .set({ name: 'okdoki', about: 'website'})
    .where('name', 'ok')
    .and('about', 'webapp')
    ;

    var target = "\
      UPDATE names                \
      SET name = $1, about = $2   \
      WHERE name = $3 AND about = $4 \
      RETURNING * \
    ;";

    var r = sql.to_sql();
    assert.equal(clean(r.sql), clean(target));
    assert.deepEqual(r.vals, ['okdoki', 'website', 'ok', 'webapp']);
  });

  it( 'generates UPDATE statement with SQL functions', function () {
    var sql = SQL
    .update('names')
    .set({ name: 'okdoki', about: ['UPPER($1)', 'website']})
    .where('name', 'ok')
    ;

    var target = "\
      UPDATE names                \
      SET name = $1, about = UPPER($2)   \
      WHERE name = $3 \
      RETURNING * \
    ;";

    var r = sql.to_sql();
    assert.equal(clean(r.sql), clean(target));
    assert.deepEqual(r.vals, ['okdoki', 'website', 'ok']);
  });

}); // === describe





