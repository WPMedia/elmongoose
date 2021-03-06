var request = require('request'),
    mongoose = require('mongoose'),
    util = require('util'),
    url = require('url'),
    helpers = require('./helpers'),
    sync = require('./sync')

// turn off request pooling
request.defaults({ agent:false })

// cache elasticsearch url options for elmongoose.search() to use
var elasticUrlOptions = null

/**
 * Attach mongoose plugin for elasticsearch indexing
 *
 * @param  {Object} schema      mongoose schema
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
module.exports = elmongoose = function (schema, options) {

    // attach methods to schema
    schema.methods.index = index
    schema.methods.unindex = unindex

    schema.statics.sync = function (cb) {
        options = helpers.mergeModelOptions(options, this)
        return sync.call(this, schema, options, cb)
    }

    schema.statics.search = function (searchOpts, cb) {
        options = helpers.mergeModelOptions(options, this)

        var searchUri = helpers.makeIndexUri(options) + '*/_search?search_type=dfs_query_then_fetch&preference=_primary_first'

        return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb, options)
    }

    schema.statics.aggregateCount = function (searchOpts, cb) {
        options = helpers.mergeModelOptions(options, this)

        var searchUri = helpers.makeIndexUri(options) + '*/_search?search_type=count&preference=_primary_first'
        if(!searchOpts){
            searchOpts = {};
        }
        return helpers.doAggAndNormalizeResults(searchUri, searchOpts, cb, options)
    }

    // attach mongoose middleware hooks
    schema.post('save', function () {
        options = helpers.mergeModelOptions(options, this)
        this.index(options)
    })
    schema.post('remove', function () {
        options = helpers.mergeModelOptions(options, this)
        this.unindex(options)
    })
}

/**
 * Search across multiple collections. Same usage as model search, but with an extra key on `searchOpts` - `collections`
 * @param  {Object}   searchOpts
 * @param  {Function} cb
 */
elmongoose.search = function (searchOpts, cb) {
    // merge elasticsearch url config options
    elasticUrlOptions = helpers.mergeOptions(elasticUrlOptions)

    // determine collections to search on
    var collections = searchOpts.collections;

    if (elasticUrlOptions.prefix) {
        // prefix was specified - namespace the index names to use the prefix for each collection's index

        if (searchOpts.collections && searchOpts.collections.length) {
            // collections were specified - prepend the prefix on each collection name
            collections = collections.map(function (collection) {
                return elasticUrlOptions.prefix + '-' + collection
            })
        } else {
            // no collections specified, but prefix specified - use wildcard index with prefix
            collections = [ elasticUrlOptions.prefix + '*' ]
        }
    } else {
        // no prefix used

        // if collections specified, just use their names without the prefix

        if (!collections) {
            // no collections were specified so use _all (searches all collections), without prefix
            searchOpts.collections = [ '_all' ]
        }
    }

    var searchUri = helpers.makeDomainUri(elasticUrlOptions) + '/' + collections.join(',') + '/_search?search_type=dfs_query_then_fetch&preference=_primary_first'

    return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb)
}

/**
 * Configure the Elasticsearch url options for `elmongoose.search()`.
 *
 * @param  {Object} options - keys: host, port, prefix (optional)
 */
elmongoose.search.config = function (options) {
    // only overwrite `options` values that are being specified in this call to `config`
    if (elasticUrlOptions) {
        Object
        .keys(elasticUrlOptions)
        .forEach(function (key) {
            elasticUrlOptions[key] = options[key] || elasticUrlOptions[key]
        })
    }
    // normalize the `options` object
    elasticUrlOptions = helpers.mergeOptions(options)
}

/**
 * Index a document in elasticsearch (create if not existing)
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function index (options) {
    var self = this
    // strip mongoose-added functions, depopulate any populated fields, and serialize the doc
    var esearchDoc = helpers.serializeModel(this,options)

    var indexUri = helpers.makeDocumentUri(options, self)

    var reqOpts = {
        method: 'PUT',
        url: indexUri,
        body: JSON.stringify(esearchDoc)
    }

    if(options.auth) {
        reqOpts.auth = {
            user: options.auth.user,
            pass: options.auth.password,
            sendImmediately: false
        };
    }

    // console.log('index:', indexUri)

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document indexing error: '+util.inspect(err, true, 10, true))
            error.details = err

            console.log('error', error);
            //self.emit('error', error)
            return
        }

        self.emit('elmongoose-indexed', body)
    })
}

/**
 * Remove a document from elasticsearch
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function unindex (options) {
    var self = this

    var unindexUri = helpers.makeDocumentUri(options, self)

    // console.log('unindex:', unindexUri)

    var reqOpts = {
        method: 'DELETE',
        url: unindexUri
    }

    if(options.auth) {
        reqOpts.auth = {
            user: options.auth.user,
            pass: options.auth.password,
            sendImmediately: false
        };
    }

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document index deletion error: '+util.inspect(err, true, 10, true))
            error.details = err

            console.log('error', error);
            //self.emit('error', error)
            return
        }

        self.emit('elmongoose-unindexed', body)
    })
}
