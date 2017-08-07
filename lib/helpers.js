const request = require('request');
const mongoose = require('mongoose');
const util = require('util');
const url = require('url');
const logger = require('./logger');

const ObjectId = mongoose.Types.ObjectId;

/**
 * Sends an http request using `reqOpts`, calls `cb` upon completion.
 * Upon ECONNRESET, backs off linearly in increments of 500ms with some noise to reduce concurrency.
 *
 * @param  {Object}   reqOpts   request options object
 * @param  {Function} cb        Signature: function (err, res, body)
 */
exports.backOffRequest = function _backOffRequest(reqOpts, cb) {
  const maxAttempts = 3;
  const backOffRate = 500;
  let parsedBody = null;

  function makeAttempts(attempts) {
    attempts += 1;

    request(reqOpts, (err, res, body) => {
      if (err) {
        if (
          (err.code === 'ECONNRESET' || err.code === 'EPIPE' || err.code === 'ETIMEDOUT')
          && attempts <= maxAttempts
        ) {
          const waitTime = (backOffRate * attempts) + (Math.random() * backOffRate);
          setTimeout(() => {
            makeAttempts(attempts);
          }, waitTime);
        } else {
          const error = new Error(`elasticsearch request error: ${err}`);
          error.details = err;
          error.attempts = attempts;
          error.reqOpts = reqOpts;
          cb(error);
        }
      }

      // parse the response body as JSON
      try {
        parsedBody = JSON.parse(body);
        if (parsedBody.error) {
          return cb(parsedBody.error);
        }
      } catch (parseErr) {
        const error = new Error(`Elasticsearch did not send back a valid JSON reply: ${util.inspect(body, true, 10, true)}`);
        error.elasticsearchReply = body;
        error.reqOpts = reqOpts;
        error.details = parseErr;
        return cb(error);
      }

      // success case
      return cb(err, res, parsedBody);
    });
  }

  makeAttempts(0);
};

/**
 * Performs deep-traversal on `thing` and converts
 * any object ids to hex strings, and dates to ISO strings.
 *
 * @param  {Any type} thing
 */
exports.serialize = function _serialize(thing) {
  if (Array.isArray(thing)) {
    return thing.map(exports.serialize);
  } else if (thing instanceof ObjectId) {
    return thing.toString();
  } else if (thing instanceof Date) {
    return thing.toISOString();
  } else if (typeof thing === 'object' && thing !== null) {
    Object
      .keys(thing)
      .forEach((key) => {
        thing[key] = exports.serialize(thing[key]);
      });
    return thing;
  }
  return thing;
};

/**
 * Flattens sub documents in thing
 *
 * @param  {Any type} thing
 */
exports.flatten = function _flatten(thing, sufix) {
  const result = {};

  function recurse(cur, prop) {
    prop += sufix;
    result[prop] = cur;
  }

  recurse(thing, '');
  return result;
};

/**
 * Serialize a mongoose model instance for elasticsearch.
 *
 * @param  {Mongoose model instance} model
 * @return {Object}
 */
exports.serializeModel = function _serializeModel(model, options) {
  const deflated = model.toObject({ depopulate: true });
  const serialized = exports.serialize(deflated);
  if (options.flatten && options.grouper) {
    serialized[options.flatten] =
      exports.flatten(serialized[options.flatten], serialized[options.grouper]);
  }
  return serialized;
};

/**
 * Merge user-supplied `options` object with defaults (to configure Elasticsearch url)
 * @param  {Object} options
 * @return {Object}
 */
exports.mergeOptions = function _mergeOptions(options) {
  const defaultOptions = {
    protocol: 'http',
    host: 'localhost',
    port: 9200,
    prefix: '',
  };

  if (!options) {
    return defaultOptions;
  }

  // if user specifies an `options` value, ensure it's an object
  if (typeof options !== 'object') {
    throw new Error(`elmongoose options was specified, but is not an object. Got: ${util.inspect(options, true, 10, true)}`);
  }

  const mergedOptions = {};

  if (options.url) {
    // node's url module doesn't parse imperfectly formed URLs sanely.
    // use a regex so the user can pass in urls flexibly.

    // Rules:
    // url must specify at least host and port (protocol falls back to options.protocol
    // or defaults to http)
    // if `host`, `port` or `protocol` specified in `options` are different than those
    // in url, throw.
    const rgxUrl = /^((http|https):\/\/)?(.+):([0-9]+)/;
    const urlMatch = rgxUrl.exec(options.url);

    if (!urlMatch) {
      throw new Error(`url from options must contain host and port. url: ${options.url}`);
    }

    // if no protocol in url, default to options protocol, or http
    const protocol = urlMatch[2];
    if (protocol && options.protocol && protocol !== options.protocol) {
      // user passes in `protocol` and a different protocol in `url`.
      throw new Error('url specifies different protocol than protocol specified in `options`. Pick one to use in `options`.');
    }
    mergedOptions.protocol = protocol || options.protocol || defaultOptions.protocol;
    const hostname = urlMatch[3];
    if (!hostname) {
      // hostname must be parseable from the url
      throw new Error(`url from options must contain host and port. url: ${options.url}`);
    }
    mergedOptions.host = hostname;
    const port = urlMatch[4];
    if (!port) {
      // port must be specified in url
      throw new Error(`url from options must contain host and port. url: ${options.url}`);
    }

    if (port && options.port && port !== options.port) {
      // if port is specified in `options` too, and its a different value, throw.
      throw new Error('url specifies different port than port specified in `options`. Pick one to use in `options`.');
    }
    mergedOptions.port = port;
    mergedOptions.prefix = typeof options.prefix === 'string' ? options.prefix : '';
  } else {
    Object.keys(defaultOptions).forEach((key) => {
      mergedOptions[key] = options[key] || defaultOptions[key];
    });
  }
  mergedOptions.grouper = options.grouper;
  mergedOptions.flatten = options.flatten;
  mergedOptions.auth = options.auth;

  return mergedOptions;
};

/**
 * Merge the default elmongoose collection options with the user-supplied options object
 *
 * @param  {Object} options (optional)
 * @param  {Object}
 * @return {Object}
 */
exports.mergeModelOptions = function _mergeModelOptions(options, model, resyncIndex) {
  const mergedOptions = exports.mergeOptions(options);

  // use lower-case model name as elasticsearch type
  mergedOptions.type = model.collection.name.toLowerCase();

  if (typeof resyncIndex !== 'undefined') {
    mergedOptions.type = resyncIndex.toLowerCase();
  }
  return mergedOptions;
};

/**
 * Merge the default elmongoose search options with the user-supplied `searchOpts`
 * @param  {Object} searchOpts
 * @return {Object}
 */
exports.mergeSearchOptions = function _mergeSearchOptions(searchOpts) {
  const defaultSearchOpts = {
    mustMatch: null,
    mustMatchPhrase: null,
    shouldMatch: null,
    mustFuzzyMatch: null,
    shouldFuzzyMatch: null,
    mustNotMatch: null,
    shouldNotMatch: null,
    mustAllMatch: null,
    shouldAllMatch: null,
    mustRange: null,
    shouldRange: null,
    mustMatchMulti: null,
    shouldMatchMulti: null,
    mustArray: null,
    shouldArray: null,
    sort: null,
    matchAll: null,
    fuzziness: 0.0,
    pageSize: 25,
    page: 1,
  };

  const mergedSearchOpts = {};

  // merge the user's `options` object with `defaultOptions`
  Object
    .keys(defaultSearchOpts)
    .forEach((key) => {
      mergedSearchOpts[key] = searchOpts[key] || defaultSearchOpts[key];
    });

  return mergedSearchOpts;
};

/**
 * Merge the default elmongoose agg options with the user-supplied `aggOpts`
 * @param  {Object} aggOpts
 * @return {Object}
 */
exports.mergeAggOptions = function _mergeAggOptions(aggOpts) {
  const defaultAggOpts = {
    mustMatch: null,
    mustMatchPhrase: null,
    shouldMatch: null,
    mustFuzzyMatch: null,
    shouldFuzzyMatch: null,
    mustRange: null,
    shouldRange: null,
    groupBy: null,
    pageSize: 25,
    page: 1,
  };

  const mergedAggOpts = {};

  // merge the user's `options` object with `defaultOptions`
  Object
    .keys(defaultAggOpts)
    .forEach((key) => {
      mergedAggOpts[key] = aggOpts[key] || defaultAggOpts[key];
    });

  return mergedAggOpts;
};

/**
 * Build term filters
 *
 * @param  {Object} termOpts
 * @return {Object}
 */
exports.buildTermFilters = function _buildTermFilters(termOpts) {
  const filters = [];

  for (let i = 0, keys = Object.keys(termOpts); i < keys.length; i += 1) {
    const key = keys[i];
    let termOpt = termOpts[key];

    if (Array.isArray(termOpt)) {
      for (let j = 0; j < termOpt.length; j += 1) {
        const termFilter = {
          term: {},
        };
        termFilter.term[key] = termOpt[j];
        filters.push(termFilter);
      }
    } else {
      if (typeof termOpt === 'string') {
        termOpt = termOpt.toLowerCase();
      }

      const termFilter = {
        term: {},
      };

      termFilter.term[key] = termOpt;
      filters.push(termFilter);
    }
  }
  return filters;
};

/**
 * Build array filters
 *
 * @param  {Object} termOpts
 * @return {Object}
 */
exports.buildArrayFilters = function _buildArrayFilters(termOpts) {
  const filters = [];
  Object.keys(termOpts).forEach((key) => {
    if (Array.isArray(termOpts[key])) {
      const termsFilter = {
        terms: {},
      };

      termsFilter.terms[key] = termOpts[key];
      filters.push(termsFilter);
    } else {
      throw new Error(`Value is not an array: ${key} ${termOpts}`);
    }
  });
  return filters;
};

/**
 * Build not matching query
 *
 * @param  {Object} mustNotOpts
 * @return {Object}
 */
exports.buildNotMatchingQuery = function _buildNotMatchingQuery(mustNotOpts) {
  const queries = [];
  Object.keys(mustNotOpts).forEach((key) => {
    if (Array.isArray(mustNotOpts[key])) {
      for (let j = 0; j < mustNotOpts[key].length; j += 1) {
        const body = {
          query: {
            bool: {
              must_not: [
                {
                  multi_match: {
                    query: mustNotOpts[key][j],
                    fields: key,
                    zero_terms_query: 'all',
                    boost: 3,
                  },
                },
              ],
              minimum_should_match: 1,
            },
          },
        };
        queries.push(body);
      }
    } else {
      const body = {
        query: {
          bool: {
            must_not: [
              {
                multi_match: {
                  query: mustNotOpts[key],
                  fields: key,
                  zero_terms_query: 'all',
                  boost: 3,
                },
              },
            ],
            minimum_should_match: 1,
          },
        },
      };
      queries.push(body);
    }
  });
  return queries;
};

/**
 * Build fuzzy matching query
 *
 * @param  {Object} fuzzyOpts
 * @param {Object} fuzziness
 * @return {Object}
 */
exports.buildFuzzyMatchingQuery = function _buildFuzzyMatchingQuery(fuzzyOpts, fuzziness) {
  const queries = [];
  Object.keys(fuzzyOpts).forEach((key) => {
    if (Array.isArray(fuzzyOpts[key])) {
      for (let j = 0; j < fuzzyOpts[key].length; j += 1) {
        const body = {
          query: {
            bool: {
              should: [
                {
                  multi_match: {
                    query: fuzzyOpts[key][j],
                    fields: key,
                    zero_terms_query: 'all',
                    boost: 3,
                  },
                },
                {
                  multi_match: {
                    query: fuzzyOpts[key][j],
                    fields: key,
                    zero_terms_query: 'all',
                    fuzziness,
                    boost: 1,
                  },
                },
              ],
              minimum_should_match: 1,
            },
          },
        };
        queries.push(body);
      }
    } else {
      if (typeof fuzzyOpts[key] === 'string') {
        fuzzyOpts[key] = fuzzyOpts[key].toLowerCase();
      }
      const body = {
        query: {
          bool: {
            should: [
              {
                multi_match: {
                  query: fuzzyOpts[key],
                  fields: key,
                  // if analyzer causes zero terms to be produced from the query, return all results
                  zero_terms_query: 'all',
                  boost: 3,
                },
              },
              // fuzzy query with lower boost than exact match query
              {
                multi_match: {
                  query: fuzzyOpts[key],
                  fields: key,
                  // if analyzer causes zero terms to be produced from the query, return all results
                  zero_terms_query: 'all',
                  fuzziness,
                  boost: 1,
                },
              },
            ],
            minimum_should_match: 1,
          },
        },
      };
      queries.push(body);
    }
  });
  return queries;
};

/**
 * Build all matching query
 *
 * @param  {Object} allOpts
 * @return {Object}
 */
exports.buildAllMatchingQuery = function _buildAllMatchingQuery(allOpts) {
  const queries = [];
  for (let i = 0; i < allOpts.length; i += 1) {
    const body = {
      query: {
        match: {
          _all: allOpts[i],
        },
      },
    };
    queries.push(body);
  }

  return queries;
};

exports.buildAllMatchQuery = function _buildAllMatchQuery() {
  const queries = [];
  const body = {
    match_all: {},
  };
  queries.push(body);
  return queries;
};

exports.buildMatchPhraseQuery = function _buildMatchPhraseQuery(opts) {
  const queries = [];
  if (opts.length) {
    opts.forEach((opt) => {
      const body = {
        query: {
          match_phrase: opt,
        },
      };
      queries.push(body);
    });
  }
  return queries;
};

/**
 * Build range filters
 *
 * @param  {Object} rangeOpts
 * @return {Object}
 */
exports.buildRangeFilter = function (rangeOpts) {
  const filters = [];
  // var keys = Object.keys(rangeOpts);
  for (let i = 0, keys = Object.keys(rangeOpts); i < keys.length; i += 1) {
    const key = keys[i];
    if (typeof rangeOpts[key] === 'object') {
      const rangeFilter = {
        range: {},
      };

      rangeFilter.range[key] = rangeOpts[key];
      filters.push(rangeFilter);
    } else {
      logger.info(`error: Range given is not an Object: ${rangeOpts[key]}`);
    }
  }

  return filters;
};

/**
 * Generate a search request body from `searchOpts`.
 *
 * @param  {Object} searchOpts
 * @return {Object}
 */
exports.mergeSearchBody = function (searchOpts) {
  let mustFilters = [];
  let shouldFilters = [];
  const body = {
    query: { // dsl was changed for version 5.x
      bool: {},
    },
  };

  // set response size (for paging)
  body.from = searchOpts.page ? (searchOpts.page - 1) * searchOpts.pageSize : 0;
  body.size = searchOpts.pageSize;

  // set sort if defined
  if (searchOpts.sort) {
    body.sort = searchOpts.sort;
  }
  // set mustNotMatch filters
  if (searchOpts.mustNotMatch && Object.keys(searchOpts.mustNotMatch).length) {
    logger.info('Must Not Match');
    mustFilters = mustFilters.concat(exports.buildNotMatchingQuery(searchOpts.mustNotMatch));
  }
  // set shouldNotMatch filters
  if (searchOpts.shouldNotMatch && Object.keys(searchOpts.shouldNotMatch).length) {
    logger.info('Should Not Match');
    shouldFilters = shouldFilters.concat(exports.buildNotMatchingQuery(searchOpts.shouldNotMatch));
  }
  // set mustFuzzyMatch filters
  if (searchOpts.mustFuzzyMatch && Object.keys(searchOpts.mustFuzzyMatch).length) {
    mustFilters = mustFilters.concat(exports.buildFuzzyMatchingQuery(searchOpts.mustFuzzyMatch,
      searchOpts.fuzziness));
  }

  // set shouldFuzzyMatch filters
  if (searchOpts.shouldFuzzyMatch && Object.keys(searchOpts.shouldFuzzyMatch).length) {
    shouldFilters = shouldFilters.concat(exports.buildFuzzyMatchingQuery(
      searchOpts.shouldFuzzyMatch, searchOpts.fuzziness));
  }

  // set mustAllMatch filters
  if (searchOpts.mustAllMatch && Object.keys(searchOpts.mustAllMatch).length) {
    mustFilters = mustFilters.concat(exports.buildAllMatchingQuery(searchOpts.mustAllMatch));
  }

  // set shouldAllMatch filters
  if (searchOpts.shouldAllMatch && Object.keys(searchOpts.shouldAllMatch).length) {
    shouldFilters = shouldFilters.concat(exports.buildAllMatchingQuery(searchOpts.shouldAllMatch));
  }

  // set must filters
  if (searchOpts.mustMatch && Object.keys(searchOpts.mustMatch).length) {
    mustFilters = mustFilters.concat(exports.buildTermFilters(searchOpts.mustMatch));
  }

  if (searchOpts.mustMatchPhrase && Object.keys(searchOpts.mustMatchPhrase).length) {
    mustFilters = mustFilters.concat(exports.buildMatchPhraseQuery(searchOpts.mustMatchPhrase));
  }

  // set should filters
  if (searchOpts.shouldMatch && Object.keys(searchOpts.shouldMatch).length) {
    shouldFilters = shouldFilters.concat(exports.buildTermFilters(searchOpts.shouldMatch));
  }

  // set must array filters
  if (searchOpts.mustArray && Object.keys(searchOpts.mustArray).length) {
    mustFilters = mustFilters.concat(exports.buildArrayFilters(searchOpts.mustArray));
  }

  // set should array filters
  if (searchOpts.shouldArray && Object.keys(searchOpts.shouldArray).length) {
    shouldFilters = shouldFilters.concat(exports.buildArrayFilters(searchOpts.shouldArray));
  }

  // set must range filters
  if (searchOpts.mustRange && Object.keys(searchOpts.mustRange).length) {
    mustFilters = mustFilters.concat(exports.buildRangeFilter(searchOpts.mustRange));
  }

  // set should range filters
  if (searchOpts.shouldRange && Object.keys(searchOpts.shouldRange).length) {
    shouldFilters = shouldFilters.concat(exports.buildRangeFilter(searchOpts.shouldRange));
  }

  if (searchOpts.matchAll && Object.keys(searchOpts.matchAll).length) {
    mustFilters = mustFilters.concat(exports.buildAllMatchQuery(searchOpts.mustMatch));
  }

  if (mustFilters.length) {
    body.query.bool.must = mustFilters;
  }

  if (shouldFilters.length) {
    body.query.bool.should = shouldFilters;
  }

  logger.info(`mergeSearchBody body: ${util.inspect(body, true, 10, true)}`);
  return body;
};

/**
 * Generate a agg request body from `aggOpts`.
 *
 * @param  {Object} aggOpts
 * @return {Object}
 */
exports.mergeAggBody = function (aggOpts) {
  let mustFilters = [];
  let shouldFilters = [];
  const body = {};
  const aggBody = {
    ElmongooseAgg: {
      terms: {
        field: aggOpts.groupBy,
        size: 0,
      }, // show all results
    },
  };

  // set response size (for paging)
  body.from = aggOpts.page ? (aggOpts.page - 1) * aggOpts.pageSize : 0;
  body.size = aggOpts.pageSize;

  // set must filters
  if (aggOpts.mustMatch && Object.keys(aggOpts.mustMatch).length) {
    mustFilters = mustFilters.concat(exports.buildTermFilters(aggOpts.mustMatch));
  }

  // set should filters
  if (aggOpts.shouldMatch && Object.keys(aggOpts.shouldMatch).length) {
    shouldFilters = shouldFilters.concat(exports.buildTermFilters(aggOpts.shouldMatch));
  }

  if (mustFilters.length || shouldFilters.length) {
    // if filters were set, create a top level agg to wrap your filters and aggBody in
    body.aggs = {
      ElmongooseAggWrapper: {
        filter: {
          bool: {},
        },
        aggs: aggBody,
      },
    };

    // add your filters
    if (mustFilters.length) {
      body.aggs.ElmongooseAggWrapper.filter.bool.must = mustFilters;
    }
    if (shouldFilters.length) {
      body.aggs.ElmongooseAggWrapper.filter.bool.should = shouldFilters;
    }
  } else {
    // if no filters were set just do a normal agg
    body.aggs = aggBody;
  }

  logger.info(`mergeAggBody body: ${util.inspect(body, true, 10, true)}`);
  return body;
};

/**
 * Make a search request using `reqOpts`, normalize results and call `cb`.
 * @param searchUri
 * @param searchOpts
 * @param cb
 * @param options
 */
exports.doSearchAndNormalizeResults = function (searchUri, searchOpts, cb, options) {
  searchOpts = exports.mergeSearchOptions(searchOpts);

  const body = exports.mergeSearchBody(searchOpts);
  const reqOpts = {
    method: 'POST',
    url: searchUri,
    body: JSON.stringify(body),
  };

  if (options.auth) {
    reqOpts.auth = {
      user: options.auth.user,
      pass: options.auth.password,
      sendImmediately: false,
    };
  }

  exports.backOffRequest(reqOpts, (err, res, body) => {
    if (err) {
      const error = new Error(`Elasticsearch search error:${util.inspect(err, true, 10, true)}`);
      error.details = err;
      return cb(new Error(error));
    }

    if (!body.hits) {
      const error = new Error(`Unexpected Elasticsearch reply:${util.inspect(body, true, 10, true)}`);
      error.elasticsearchReply = body;
      return cb(new Error(error));
    }
    const searchResults = {
      total: body.hits.total,
      hits: [],
    };

    if (body.hits.hits && body.hits.hits.length) {
      searchResults.hits = body.hits.hits;
    }

    return cb(null, searchResults);
  });
};

/**
 * Make a search request using `reqOpts`, normalize results and call `cb`.
 *
 * @param  {Object}   reqOpts
 * @param  {Function} cb
 */
exports.doAggAndNormalizeResults = function (searchUri, aggOpts, cb, options) {
  // merge `searchOpts` with default user-level search options
  searchOpts = exports.mergeAggOptions(aggOpts);
  const body = exports.mergeAggBody(aggOpts);


  const reqOpts = {
    method: 'POST',
    url: searchUri,
    body: JSON.stringify(body),
  };

  if (options.auth) {
    reqOpts.auth = {
      user: options.auth.user,
      pass: options.auth.password,
      sendImmediately: false,
    };
  }

  exports.backOffRequest(reqOpts, (err, res, body) => {
    if (err) {
      const error = new Error(`Elasticsearch search error:${util.inspect(err, true, 10, true)}`);
      error.details = err;
      return cb(new Error(error));
    }

    if (!body.hits) {
      const error = new Error(`Unexpected Elasticsearch reply:${util.inspect(body, true, 10, true)}`);
      error.elasticsearchReply = body;
      return cb(new Error(error));
    }

    const searchResults = {
      total: body.hits.total,
      aggregation: body.aggregations,
    };

    if (body.hits.hits && body.hits.hits.length) {
      searchResults.hits = body.hits.hits;
    }

    return cb(null, searchResults);
  });
};

/**
 * Make index name (with prefix) from `options`
 *
 * @param  {Object} options
 * @return {String}
 */
exports.makeIndexName = function (options) {
  return options.prefix ? (`${options.prefix}-${options.type}`) : options.type;
};

/**
 * Form the elasticsearch URI for indexing/deleting a document
 *
 * @param  {Object} options
 * @param  {Mongoose document} doc
 * @return {String}
 */
exports.makeDocumentUri = function (options, doc) {
  const typeUri = exports.makeTypeUri(options);

  const docUri = `${typeUri}/${doc._id}`;

  return docUri;
};

/**
 * Form the elasticsearch URI up to the type of the document
 *
 * @param  {Object} options
 * @return {String}
 */
exports.makeTypeUri = function (options) {
  const indexUri = exports.makeIndexUri(options);

  const typeUri = `${indexUri}/${options.type}`;

  return typeUri;
};

/**
 * Form the elasticsearch URI up to the index of the document (index is same as type due to aliasing)
 * @param  {Object} options
 * @return {String}
 */
exports.makeIndexUri = function (options) {
  const domainUri = exports.makeDomainUri(options);

  const indexName = exports.makeIndexName(options);

  const indexUri = `${domainUri}/${indexName}`;

  return indexUri;
};

exports.makeDomainUri = function (options) {
  let domainUri = url.format({
    protocol: options.protocol,
    hostname: options.host,
  });
  if (options.port) {
    domainUri = url.format({
      protocol: options.protocol,
      hostname: options.host,
      port: options.port,
    });
  }

  return domainUri;
};

exports.makeAliasUri = function (options) {
  const domainUri = exports.makeDomainUri(options);

  const aliasUri = `${domainUri}/_aliases`;

  return aliasUri;
};

exports.makeBulkIndexUri = function (indexName, options) {
  const domainUri = exports.makeDomainUri(options);

  const bulkIndexUri = `${domainUri}/${indexName}/_bulk`;

  return bulkIndexUri;
};

// Checks that a response body from elasticsearch reported success
exports.elasticsearchBodyOk = function (elasticsearchBody) {
  // `ok` for elasticsearch version < 1, `acknowledged` for v1
  return elasticsearchBody && (elasticsearchBody.ok || elasticsearchBody.acknowledged ||
    elasticsearchBody.total === elasticsearchBody.successful);
};
