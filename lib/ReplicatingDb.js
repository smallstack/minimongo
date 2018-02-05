var Collection, ReplicatingDb, compileSort, utils, _;

_ = require('lodash');

utils = require('./utils');

compileSort = require('./selector').compileSort;

module.exports = ReplicatingDb = (function() {
  function ReplicatingDb(masterDb, replicaDb) {
    this.collections = {};
    this.masterDb = masterDb;
    this.replicaDb = replicaDb;
  }

  ReplicatingDb.prototype.addCollection = function(name, success, error) {
    var collection;
    collection = new Collection(name, this.masterDb[name], this.replicaDb[name]);
    this[name] = collection;
    this.collections[name] = collection;
    if (success != null) {
      return success();
    }
  };

  ReplicatingDb.prototype.removeCollection = function(name, success, error) {
    delete this[name];
    delete this.collections[name];
    if (success != null) {
      return success();
    }
  };

  ReplicatingDb.prototype.getCollectionNames = function() {
    return _.keys(this.collections);
  };

  return ReplicatingDb;

})();

Collection = (function() {
  function Collection(name, masterCol, replicaCol) {
    this.name = name;
    this.masterCol = masterCol;
    this.replicaCol = replicaCol;
  }

  Collection.prototype.find = function(selector, options) {
    return this.masterCol.find(selector, options);
  };

  Collection.prototype.findOne = function(selector, options, success, error) {
    return this.masterCol.findOne(selector, options, success, error);
  };

  Collection.prototype.upsert = function(docs, bases, success, error) {
    var items, _ref;
    _ref = utils.regularizeUpsert(docs, bases, success, error), items = _ref[0], success = _ref[1], error = _ref[2];
    return this.masterCol.upsert(_.pluck(items, "doc"), _.pluck(items, "base"), (function(_this) {
      return function() {
        return _this.replicaCol.upsert(_.pluck(items, "doc"), _.pluck(items, "base"), function(results) {
          return success(docs);
        }, error);
      };
    })(this), error);
  };

  Collection.prototype.remove = function(id, success, error) {
    return this.masterCol.remove(id, (function(_this) {
      return function() {
        return _this.replicaCol.remove(id, success, error);
      };
    })(this), error);
  };

  Collection.prototype.cache = function(docs, selector, options, success, error) {
    var docsMap, sort;
    docsMap = _.indexBy(docs, "_id");
    if (options.sort) {
      sort = compileSort(options.sort);
    }
    return this.masterCol.find(selector, options).fetch((function(_this) {
      return function(results) {
        var doc, performCaches, performUncaches, result, resultsMap, toCache, toUncache, _i, _j, _len, _len1;
        resultsMap = _.indexBy(results, "_id");
        toCache = [];
        for (_i = 0, _len = docs.length; _i < _len; _i++) {
          doc = docs[_i];
          result = resultsMap[doc._id];
          if (!result) {
            toCache.push(doc);
            continue;
          }
          if (doc._rev && result._rev && doc._rev <= result._rev) {
            continue;
          }
          if (!_.isEqual(doc, result)) {
            toCache.push(doc);
          }
        }
        toUncache = [];
        for (_j = 0, _len1 = results.length; _j < _len1; _j++) {
          result = results[_j];
          if (options.limit && docs.length === options.limit) {
            if (options.sort && sort(result, _.last(docs)) >= 0) {
              continue;
            }
            if (!options.sort) {
              continue;
            }
          }
          if (!docsMap[result._id]) {
            toUncache.push(result._id);
          }
        }
        performCaches = function(next) {
          if (toCache.length > 0) {
            return _this.masterCol.cacheList(toCache, function() {
              return _this.replicaCol.cacheList(toCache, function() {
                return next();
              }, error);
            }, error);
          } else {
            return next();
          }
        };
        performUncaches = function(next) {
          if (toUncache.length > 0) {
            return _this.masterCol.uncacheList(toUncache, function() {
              return _this.replicaCol.uncacheList(toUncache, function() {
                return next();
              }, error);
            }, error);
          } else {
            return next();
          }
        };
        return performCaches(function() {
          return performUncaches(function() {
            if (success != null) {
              success();
            }
          });
        });
      };
    })(this), error);
  };

  Collection.prototype.pendingUpserts = function(success, error) {
    return this.masterCol.pendingUpserts(success, error);
  };

  Collection.prototype.pendingRemoves = function(success, error) {
    return this.masterCol.pendingRemoves(success, error);
  };

  Collection.prototype.resolveUpserts = function(upserts, success, error) {
    return this.masterCol.resolveUpserts(upserts, (function(_this) {
      return function() {
        return _this.replicaCol.resolveUpserts(upserts, success, error);
      };
    })(this), error);
  };

  Collection.prototype.resolveRemove = function(id, success, error) {
    return this.masterCol.resolveRemove(id, (function(_this) {
      return function() {
        return _this.replicaCol.resolveRemove(id, success, error);
      };
    })(this), error);
  };

  Collection.prototype.seed = function(docs, success, error) {
    return this.masterCol.seed(docs, (function(_this) {
      return function() {
        return _this.replicaCol.seed(docs, success, error);
      };
    })(this), error);
  };

  Collection.prototype.cacheOne = function(doc, success, error) {
    return this.masterCol.cacheOne(doc, (function(_this) {
      return function() {
        return _this.replicaCol.cacheOne(doc, success, error);
      };
    })(this), error);
  };

  Collection.prototype.cacheList = function(docs, success, error) {
    return this.masterCol.cacheList(docs, (function(_this) {
      return function() {
        return _this.replicaCol.cacheList(docs, success, error);
      };
    })(this), error);
  };

  Collection.prototype.uncache = function(selector, success, error) {
    return this.masterCol.uncache(selector, (function(_this) {
      return function() {
        return _this.replicaCol.uncache(selector, success, error);
      };
    })(this), error);
  };

  Collection.prototype.uncacheList = function(ids, success, error) {
    return this.masterCol.uncacheList(ids, (function(_this) {
      return function() {
        return _this.replicaCol.uncacheList(ids, success, error);
      };
    })(this), error);
  };

  return Collection;

})();
