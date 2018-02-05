var Collection, MemoryDb, async, compileSort, processFind, utils, _,
  __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

_ = require('lodash');

async = require('async');

utils = require('./utils');

processFind = require('./utils').processFind;

compileSort = require('./selector').compileSort;

module.exports = MemoryDb = (function() {
  function MemoryDb(options, success) {
    this.collections = {};
    this.options = _.defaults(options, {
      safety: "clone"
    });
    if (success) {
      success(this);
    }
  }

  MemoryDb.prototype.addCollection = function(name, success, error) {
    var collection;
    collection = new Collection(name, this.options);
    this[name] = collection;
    this.collections[name] = collection;
    if (success != null) {
      return success();
    }
  };

  MemoryDb.prototype.removeCollection = function(name, success, error) {
    delete this[name];
    delete this.collections[name];
    if (success != null) {
      return success();
    }
  };

  MemoryDb.prototype.getCollectionNames = function() {
    return _.keys(this.collections);
  };

  return MemoryDb;

})();

Collection = (function() {
  function Collection(name, options) {
    this._applySafety = __bind(this._applySafety, this);
    this.name = name;
    this.items = {};
    this.upserts = {};
    this.removes = {};
    this.options = options || {};
  }

  Collection.prototype.find = function(selector, options) {
    return {
      fetch: (function(_this) {
        return function(success, error) {
          return _this._findFetch(selector, options, success, error);
        };
      })(this)
    };
  };

  Collection.prototype.findOne = function(selector, options, success, error) {
    var _ref;
    if (_.isFunction(options)) {
      _ref = [{}, options, success], options = _ref[0], success = _ref[1], error = _ref[2];
    }
    return this.find(selector, options).fetch((function(_this) {
      return function(results) {
        if (success != null) {
          return success(_this._applySafety(results.length > 0 ? results[0] : null));
        }
      };
    })(this), error);
  };

  Collection.prototype._findFetch = function(selector, options, success, error) {
    return setTimeout((function(_this) {
      return function() {
        var results;
        results = processFind(_.values(_this.items), selector, options);
        if (success != null) {
          return success(_this._applySafety(results));
        }
      };
    })(this), 0);
  };

  Collection.prototype._applySafety = function(items) {
    if (!items) {
      return items;
    }
    if (_.isArray(items)) {
      return _.map(items, this._applySafety);
    }
    if (this.options.safety === "clone" || !this.options.safety) {
      return JSON.parse(JSON.stringify(items));
    }
    if (this.options.safety === "freeze") {
      Object.freeze(items);
      return items;
    }
    throw new Error("Unsupported safety " + this.options.safety);
  };

  Collection.prototype.upsert = function(docs, bases, success, error) {
    var item, items, _i, _len, _ref;
    _ref = utils.regularizeUpsert(docs, bases, success, error), items = _ref[0], success = _ref[1], error = _ref[2];
    items = JSON.parse(JSON.stringify(items));
    for (_i = 0, _len = items.length; _i < _len; _i++) {
      item = items[_i];
      if (item.base === void 0) {
        if (this.upserts[item.doc._id]) {
          item.base = this.upserts[item.doc._id].base;
        } else {
          item.base = this.items[item.doc._id] || null;
        }
      }
      this.items[item.doc._id] = item.doc;
      this.upserts[item.doc._id] = item;
    }
    if (_.isArray(docs)) {
      if (success) {
        return success(this._applySafety(_.pluck(items, "doc")));
      }
    } else {
      if (success) {
        return success(this._applySafety(_.pluck(items, "doc")[0]));
      }
    }
  };

  Collection.prototype.remove = function(id, success, error) {
    if (_.isObject(id)) {
      this.find(id).fetch((function(_this) {
        return function(rows) {
          return async.each(rows, function(row, cb) {
            return _this.remove(row._id, (function() {
              return cb();
            }), cb);
          }, function() {
            return success();
          });
        };
      })(this), error);
      return;
    }
    if (_.has(this.items, id)) {
      this.removes[id] = this.items[id];
      delete this.items[id];
      delete this.upserts[id];
    } else {
      this.removes[id] = {
        _id: id
      };
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.cache = function(docs, selector, options, success, error) {
    var doc, docsMap, sort, _i, _len;
    for (_i = 0, _len = docs.length; _i < _len; _i++) {
      doc = docs[_i];
      this.cacheOne(doc);
    }
    docsMap = _.object(_.pluck(docs, "_id"), docs);
    if (options.sort) {
      sort = compileSort(options.sort);
    }
    return this.find(selector, options).fetch((function(_this) {
      return function(results) {
        var result, _j, _len1;
        for (_j = 0, _len1 = results.length; _j < _len1; _j++) {
          result = results[_j];
          if (!docsMap[result._id] && !_.has(_this.upserts, result._id)) {
            if (options.limit && docs.length === options.limit) {
              if (options.sort && sort(result, _.last(docs)) >= 0) {
                continue;
              }
              if (!options.sort) {
                continue;
              }
            }
            delete _this.items[result._id];
          }
        }
        if (success != null) {
          return success();
        }
      };
    })(this), error);
  };

  Collection.prototype.pendingUpserts = function(success) {
    return success(_.values(this.upserts));
  };

  Collection.prototype.pendingRemoves = function(success) {
    return success(_.pluck(this.removes, "_id"));
  };

  Collection.prototype.resolveUpserts = function(upserts, success) {
    var id, upsert, _i, _len;
    for (_i = 0, _len = upserts.length; _i < _len; _i++) {
      upsert = upserts[_i];
      id = upsert.doc._id;
      if (this.upserts[id]) {
        if (_.isEqual(upsert.doc, this.upserts[id].doc)) {
          delete this.upserts[id];
        } else {
          this.upserts[id].base = upsert.doc;
        }
      }
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.resolveRemove = function(id, success) {
    delete this.removes[id];
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.seed = function(docs, success) {
    var doc, _i, _len;
    if (!_.isArray(docs)) {
      docs = [docs];
    }
    for (_i = 0, _len = docs.length; _i < _len; _i++) {
      doc = docs[_i];
      if (!_.has(this.items, doc._id) && !_.has(this.removes, doc._id)) {
        this.items[doc._id] = doc;
      }
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.cacheOne = function(doc, success, error) {
    return this.cacheList([doc], success, error);
  };

  Collection.prototype.cacheList = function(docs, success) {
    var doc, existing, _i, _len;
    for (_i = 0, _len = docs.length; _i < _len; _i++) {
      doc = docs[_i];
      if (!_.has(this.upserts, doc._id) && !_.has(this.removes, doc._id)) {
        existing = this.items[doc._id];
        if (!existing || !doc._rev || !existing._rev || doc._rev > existing._rev) {
          this.items[doc._id] = doc;
        }
      }
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.uncache = function(selector, success, error) {
    var compiledSelector, items;
    compiledSelector = utils.compileDocumentSelector(selector);
    items = _.filter(_.values(this.items), (function(_this) {
      return function(item) {
        return (_this.upserts[item._id] != null) || !compiledSelector(item);
      };
    })(this));
    this.items = _.object(_.pluck(items, "_id"), items);
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.uncacheList = function(ids, success, error) {
    var idIndex, items;
    idIndex = _.indexBy(ids);
    items = _.filter(_.values(this.items), (function(_this) {
      return function(item) {
        return (_this.upserts[item._id] != null) || !idIndex[item._id];
      };
    })(this));
    this.items = _.object(_.pluck(items, "_id"), items);
    if (success != null) {
      return success();
    }
  };

  return Collection;

})();
