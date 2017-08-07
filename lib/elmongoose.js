const request = require('request');
const util = require('util');
const helpers = require('./helpers');
const sync = require('./sync');
const constants = require('./constants');
const logger = require('./logger');

// turn off request pooling
request.defaults({
  agent: false,
});

let elmongoose;

// cache elasticsearch url options for elmongoose.search() to use
let elasticUrlOptions = null;

/**
 * Index a document in elasticsearch (create if not existing)
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function index(options) {
  const self = this;
  const esearchDoc = helpers.serializeModel(this, options);
  let indexUri = helpers.makeDocumentUri(options, self);

  if (esearchDoc._id) {
    indexUri = indexUri.replace(esearchDoc);
    esearchDoc.id = esearchDoc._id;
    delete esearchDoc._id;
  }

  const reqOpts = {
    method: 'PUT',
    url: indexUri,
    body: JSON.stringify(esearchDoc),
  };

  if (options.auth) {
    reqOpts.auth = {
      user: options.auth.user,
      pass: options.auth.password,
      sendImmediately: false,
    };
  }

  helpers.backOffRequest(reqOpts, (err, res, body) => {
    if (err) {
      const error = new Error(`Elasticsearch document indexing error: ${util.inspect(err, true, 10, true)}`);
      error.details = err;
      logger.error(error);
      return;
    }

    self.emit(constants.INDEXED, body);
  });
}

/**
 * Remove a document from elasticsearch
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function unindex(options) {
  const self = this;
  const unindexUri = helpers.makeDocumentUri(options, self);
  const reqOpts = {
    method: 'DELETE',
    url: unindexUri,
  };

  if (options.auth) {
    reqOpts.auth = {
      user: options.auth.user,
      pass: options.auth.password,
      sendImmediately: false,
    };
  }

  helpers.backOffRequest(reqOpts, (err, res, body) => {
    if (err) {
      const error = new Error(`Elasticsearch document index deletion error: ${util.inspect(err, true, 10, true)}`);
      error.details = err;
      logger.error(error);
      return;
    }

    self.emit(constants.UNINDEXED, body);
  });
}

/**
 * Attach mongoose plugin for elasticsearch indexing
 *
 * @param  {Object} schema      mongoose schema
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
module.exports = elmongoose = function elIndex(schema, options) {
  schema.methods.index = index;
  schema.methods.unindex = unindex;

  schema.statics.sync = function _sync(cb) {
    options = helpers.mergeModelOptions(options, this);
    return sync.call(this, schema, options, cb);
  };

  schema.statics.search = function _search(searchOpts, cb) {
    options = helpers.mergeModelOptions(options, this);
    const searchUri = `${helpers.makeIndexUri(options)}*/_search?search_type=dfs_query_then_fetch&preference=_primary_first`;
    return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb, options);
  };

  schema.statics.aggregateCount = function _aggregateCount(searchOpts, cb) {
    options = helpers.mergeModelOptions(options, this);
    const searchUri = `${helpers.makeIndexUri(options)}*/_search?search_type=count&preference=_primary_first`;
    if (!searchOpts) {
      searchOpts = {};
    }
    return helpers.doAggAndNormalizeResults(searchUri, searchOpts, cb, options);
  };

  // attach mongoose middleware hooks
  schema.post('save', function _post() {
    options = helpers.mergeModelOptions(options, this);
    this.index(options);
  });

  /**
   * Remove content
   */
  schema.post('remove', function _remove() {
    options = helpers.mergeModelOptions(options, this);
    this.unindex(options);
  });

  schema.statics.resync = function _resyncIndex(resyncIndex, cb) {
    options = helpers.mergeModelOptions(options, this, resyncIndex);
    return sync.call(this, schema, options, this);
  };
};

/**
 * Search across multiple collections. Same usage as model search,
 * but with an extra key on `searchOpts` - `collections`
 * @param  {Object}   searchOpts
 * @param  {Function} cb
 */
elmongoose.search = function _search(searchOpts, cb) {
  // merge elasticsearch url config options
  elasticUrlOptions = helpers.mergeOptions(elasticUrlOptions);

  // determine collections to search on
  let collections = searchOpts.collections;

  if (elasticUrlOptions.prefix) {
    if (searchOpts.collections && searchOpts.collections.length) {
      // collections were specified - prepend the prefix on each collection name
      collections = collections.map((collection) => {
        return `${elasticUrlOptions.prefix}-${collection}`;
      });
    } else {
      // no collections specified, but prefix specified - use wildcard index with prefix
      collections = [`${elasticUrlOptions.prefix}*`];
    }
  } else if (!collections) {
    // no collections were specified so use _all (searches all collections), without prefix
    searchOpts.collections = ['_all'];
  }

  const searchUri = `${helpers.makeDomainUri(elasticUrlOptions)}/${collections.join(',')}/_search?search_type=dfs_query_then_fetch&preference=_primary_first`;
  return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb);
};

/**
 * Configure the Elasticsearch url options for `elmongoose.search()`.
 *
 * @param  {Object} options - keys: host, port, prefix (optional)
 */
elmongoose.search.config = function _config(options) {
  // only overwrite `options` values that are being specified in this call to `config`
  if (elasticUrlOptions) {
    Object
      .keys(elasticUrlOptions)
      .forEach((key) => {
        elasticUrlOptions[key] = options[key] || elasticUrlOptions[key];
      });
  }
  // normalize the `options` object
  elasticUrlOptions = helpers.mergeOptions(options);
};