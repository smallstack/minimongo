var async, bowser, compileDocumentSelector, compileSort, deg2rad, getDistanceFromLatLngInM, isLocalStorageSupported, pointInPolygon, processGeoIntersectsOperator, processNearOperator, _;

_ = require('lodash');

async = require('async');

bowser = require('bowser');

compileDocumentSelector = require('./selector').compileDocumentSelector;

compileSort = require('./selector').compileSort;

isLocalStorageSupported = function() {
  var e;
  if (!window.localStorage) {
    return false;
  }
  try {
    window.localStorage.setItem("test", "test");
    window.localStorage.removeItem("test");
    return true;
  } catch (_error) {
    e = _error;
    return false;
  }
};

exports.compileDocumentSelector = compileDocumentSelector;

exports.autoselectLocalDb = function(options, success, error) {
  var IndexedDb, LocalStorageDb, MemoryDb, WebSQLDb, browser;
  IndexedDb = require('./IndexedDb');
  WebSQLDb = require('./WebSQLDb');
  LocalStorageDb = require('./LocalStorageDb');
  MemoryDb = require('./MemoryDb');
  browser = bowser.browser;
  if (!isLocalStorageSupported()) {
    return new MemoryDb(options, success);
  }
  if (window.cordova) {
    console.log("Selecting WebSQLDb for Cordova");
    return new WebSQLDb(options, success, error);
  }
  if (browser.android || browser.ios || browser.chrome || browser.safari || browser.opera || browser.blackberry) {
    console.log("Selecting WebSQLDb for browser");
    return new WebSQLDb(options, success, (function(_this) {
      return function(err) {
        console.log("Failed to create WebSQLDb: " + (err ? err.message : void 0));
        return new IndexedDb(options, success, function(err) {
          console.log("Failed to create IndexedDb: " + (err ? err.message : void 0));
          return new MemoryDb(options, success);
        });
      };
    })(this));
  }
  if (browser.firefox && browser.version >= 16) {
    console.log("Selecting IndexedDb for browser");
    return new IndexedDb(options, success, (function(_this) {
      return function(err) {
        console.log("Failed to create IndexedDb: " + (err ? err.message : void 0));
        return new MemoryDb(options, success);
      };
    })(this));
  }
  console.log("Selecting LocalStorageDb for fallback");
  return new LocalStorageDb(options, success, error);
};

exports.migrateLocalDb = function(fromDb, toDb, success, error) {
  var HybridDb, col, hybridDb, name, _ref;
  HybridDb = require('./HybridDb');
  hybridDb = new HybridDb(fromDb, toDb);
  _ref = fromDb.collections;
  for (name in _ref) {
    col = _ref[name];
    if (toDb[name]) {
      hybridDb.addCollection(name);
    }
  }
  return hybridDb.upload(success, error);
};

exports.cloneLocalDb = function(fromDb, toDb, success, error) {
  var col, name, _ref;
  _ref = fromDb.collections;
  for (name in _ref) {
    col = _ref[name];
    if (!toDb[name]) {
      toDb.addCollection(name);
    }
  }
  return async.each(_.values(fromDb.collections), (function(_this) {
    return function(fromCol, cb) {
      var toCol;
      toCol = toDb[fromCol.name];
      return fromCol.find({}).fetch(function(items) {
        return toCol.seed(items, function() {
          return fromCol.pendingUpserts(function(upserts) {
            return toCol.upsert(_.pluck(upserts, "doc"), _.pluck(upserts, "base"), function() {
              return fromCol.pendingRemoves(function(removes) {
                return async.eachSeries(removes, function(remove, cb2) {
                  return toCol.remove(remove, function() {
                    return cb2();
                  }, cb2);
                }, cb);
              }, cb);
            }, cb);
          }, cb);
        }, cb);
      }, cb);
    };
  })(this), (function(_this) {
    return function(err) {
      if (err) {
        return error(err);
      }
      return success();
    };
  })(this));
};

exports.processFind = function(items, selector, options) {
  var filtered;
  filtered = _.filter(items, compileDocumentSelector(selector));
  filtered = processNearOperator(selector, filtered);
  filtered = processGeoIntersectsOperator(selector, filtered);
  if (options && options.sort) {
    filtered.sort(compileSort(options.sort));
  }
  if (options && options.skip) {
    filtered = _.slice(filtered, options.skip);
  }
  if (options && options.limit) {
    filtered = _.take(filtered, options.limit);
  }
  if (options && options.fields) {
    filtered = exports.filterFields(filtered, options.fields);
  }
  return filtered;
};

exports.filterFields = function(items, fields) {
  if (fields == null) {
    fields = {};
  }
  if (_.keys(fields).length === 0) {
    return items;
  }
  return _.map(items, function(item) {
    var field, from, newItem, obj, path, pathElem, to, _i, _j, _k, _l, _len, _len1, _len2, _len3, _len4, _m, _ref, _ref1, _ref2, _ref3;
    newItem = {};
    if (_.first(_.values(fields)) === 1) {
      _ref = _.keys(fields).concat(["_id"]);
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        field = _ref[_i];
        path = field.split(".");
        obj = item;
        for (_j = 0, _len1 = path.length; _j < _len1; _j++) {
          pathElem = path[_j];
          if (obj) {
            obj = obj[pathElem];
          }
        }
        if (obj == null) {
          continue;
        }
        from = item;
        to = newItem;
        _ref1 = _.initial(path);
        for (_k = 0, _len2 = _ref1.length; _k < _len2; _k++) {
          pathElem = _ref1[_k];
          to[pathElem] = to[pathElem] || {};
          to = to[pathElem];
          from = from[pathElem];
        }
        to[_.last(path)] = from[_.last(path)];
      }
      return newItem;
    } else {
      item = _.cloneDeep(item);
      _ref2 = _.keys(fields);
      for (_l = 0, _len3 = _ref2.length; _l < _len3; _l++) {
        field = _ref2[_l];
        path = field.split(".");
        obj = item;
        _ref3 = _.initial(path);
        for (_m = 0, _len4 = _ref3.length; _m < _len4; _m++) {
          pathElem = _ref3[_m];
          if (obj) {
            obj = obj[pathElem];
          }
        }
        if (obj == null) {
          continue;
        }
        delete obj[_.last(path)];
      }
      return item;
    }
  });
};

exports.createUid = function() {
  return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r, v;
    r = Math.random() * 16 | 0;
    v = c === 'x' ? r : r & 0x3 | 0x8;
    return v.toString(16);
  });
};

processNearOperator = function(selector, list) {
  var distances, geo, key, value;
  for (key in selector) {
    value = selector[key];
    if ((value != null) && value['$near']) {
      geo = value['$near']['$geometry'];
      if (geo.type !== 'Point') {
        break;
      }
      list = _.filter(list, function(doc) {
        return doc[key] && doc[key].type === 'Point';
      });
      distances = _.map(list, function(doc) {
        return {
          doc: doc,
          distance: getDistanceFromLatLngInM(geo.coordinates[1], geo.coordinates[0], doc[key].coordinates[1], doc[key].coordinates[0])
        };
      });
      distances = _.filter(distances, function(item) {
        return item.distance >= 0;
      });
      distances = _.sortBy(distances, 'distance');
      if (value['$near']['$maxDistance']) {
        distances = _.filter(distances, function(item) {
          return item.distance <= value['$near']['$maxDistance'];
        });
      }
      list = _.pluck(distances, 'doc');
    }
  }
  return list;
};

pointInPolygon = function(point, polygon) {
  if (!_.isEqual(_.first(polygon.coordinates[0]), _.last(polygon.coordinates[0]))) {
    throw new Error("First must equal last");
  }
  if (point.coordinates[0] < Math.min.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[0];
  }))) {
    return false;
  }
  if (point.coordinates[1] < Math.min.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[1];
  }))) {
    return false;
  }
  if (point.coordinates[0] > Math.max.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[0];
  }))) {
    return false;
  }
  if (point.coordinates[1] > Math.max.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[1];
  }))) {
    return false;
  }
  return true;
};

getDistanceFromLatLngInM = function(lat1, lng1, lat2, lng2) {
  var R, a, c, d, dLat, dLng;
  R = 6370986;
  dLat = deg2rad(lat2 - lat1);
  dLng = deg2rad(lng2 - lng1);
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  d = R * c;
  return d;
};

deg2rad = function(deg) {
  return deg * (Math.PI / 180);
};

processGeoIntersectsOperator = function(selector, list) {
  var geo, key, value;
  for (key in selector) {
    value = selector[key];
    if ((value != null) && value['$geoIntersects']) {
      geo = value['$geoIntersects']['$geometry'];
      if (geo.type !== 'Polygon') {
        break;
      }
      list = _.filter(list, function(doc) {
        if (!doc[key] || doc[key].type !== 'Point') {
          return false;
        }
        return pointInPolygon(doc[key], geo);
      });
    }
  }
  return list;
};

exports.regularizeUpsert = function(docs, bases, success, error) {
  var item, items, _i, _len, _ref;
  if (_.isFunction(bases)) {
    _ref = [void 0, bases, success], bases = _ref[0], success = _ref[1], error = _ref[2];
  }
  if (!_.isArray(docs)) {
    docs = [docs];
    bases = [bases];
  } else {
    bases = bases || [];
  }
  items = _.map(docs, function(doc, i) {
    return {
      doc: doc,
      base: i < bases.length ? bases[i] : void 0
    };
  });
  for (_i = 0, _len = items.length; _i < _len; _i++) {
    item = items[_i];
    if (!item.doc._id) {
      item.doc._id = exports.createUid();
    }
    if (item.base && !item.base._id) {
      throw new Error("Base needs _id");
    }
    if (item.base && item.base._id !== item.doc._id) {
      throw new Error("Base needs same _id");
    }
  }
  return [items, success, error];
};
