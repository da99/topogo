#!/usr/bin/env node

var  _  = require('underscore')
, anyDB = require('any-db')
, pool  = {}
, uri   = require('uri-js')
, River = require('da_river').River
, Topogo = require('topogo').Topogo
, h     = require('topogo/test/helpers')
;

Topogo
.new(Topogo.test_table_name)
.drop({finish: function(result, err) {
  Topogo.close(function () {
      if (err)
        throw err;
      console.log('Table dropped.');
  });
}});

