/* global describe */
/* global it */

const mongoose = require('mongoose');
const request = require('request');
const assert = require('assert');
const models = require('./models');
const async = require('async');
const util = require('util');
const elmongoose = require('../lib/elmongoose');
const helpers = require('../lib/helpers');
const testHelper = require('./testHelper');
const logger = require('../lib/logger');

const Schema = mongoose.schema;

// connect to DB
const connStr = 'mongodb://localhost/elmongoose-test';

/**
 *
 * Basic tests for Elmongoose functionality - load tests are done in load.js
 *
 */
describe('elmongoose plugin', () => {
  // array of test cat models that tests in this suite share
  const testCats = [];

  before((done) => {
    async.series({
      connectMongo(next) {
        mongoose.connect(connStr, next);
      },
      dropCollections: testHelper.dropCollections,
      checkSearchRunning(next) {
        // make sure elasticsearch is running
        request('http://localhost:9200', (err, res, body) => {
          assert.equal(err, null);
          assert(body);
          const parsedBody = JSON.parse(body);
          assert.equal(helpers.elasticsearchBodyOk(parsedBody), true);
          assert.equal(res.statusCode, 200);
          return next();
        });
      },
      deleteIndicies: testHelper.deleteIndices,
      syncCats(next) {
        models.Cat.sync(next);
      },
      waitForYellowStatus: testHelper.waitForYellowStatus,
      insertCats(next) {
        testCats[0] = new models.Cat({
          name: 'Puffy',
          breed: 'siamese',
          age: 10,
        });

        testCats[1] = new models.Cat({
          name: 'Mango',
          breed: 'siamese',
          age: 15,
        });

        testCats[2] = new models.Cat({
          name: 'Siamese',
          breed: 'persian',
          age: 12,
        });

        testCats[3] = new models.Cat({
          name: 'Zing Doodle',
          breed: 'savannah',
          age: 20,
        });

        testHelper.saveDocs(testCats, next);
      },
      refreshIndex(next) {
        testHelper.refresh(() => {
          next();
        });
      },
    }, done);
  });

  after((done) => {
    async.series({
      refreshIndex: testHelper.refresh,
      disconnectMongo(next) {
        mongoose.disconnect();
        return next();
      },
    }, done);
  });

  it('Model.search() query with no matches should return empty array', (done) => {
    models.Cat.search({ mustMatch: { name: 'nothingShouldMatchThis' } }, (err, results) => {
      assert.equal(err, null);
      assert(results);

      if (results.hits.length || results.hits.total) {
        logger.info('results', util.inspect(results, true, 10, true));
      }

      assert.equal(results.total, 0);
      assert.equal(results.hits.length, 0);

      return done();
    });
  });

  it('after creating a cat model instance, it should show up in Model.search()', (done) => {
    let testCat = null;

    async.series({
      addCat(next) {
        testCat = new models.Cat({
          name: 'simba',
        });
        testHelper.saveDocs([testCat], next);
      },
      refreshIndex: testHelper.refresh,
      doSearch(next) {
        // search to make sure the cat got indexed
        models.Cat.search({ mustMatchPhrase: [{ name: 'simba' }] }, (err, results) => {
          testHelper.assertErrNull(err);
          assert.equal(results.total, 1);
          assert.equal(results.hits.length, 1);

          const firstResult = results.hits[0];

          assert(firstResult);
          assert.equal(firstResult._source.name, 'simba');

          return next();
        });
      },
      cleanup(next) {
        testHelper.removeDocs([testCat], next);
      },
      refreshIndex: testHelper.refresh,
    }, done);
  });


  it('after creating a cat model instance with a `Person` ref, and populating it, test fuzzy match on name: `populate`', (done) => {
    let testCat = null;
    let testPerson = null;
    let testCat2 = null;

    async.series({
      addCat(next) {
        testPerson = new models.Person({
          name: 'Tolga',
          email: 'foo@bar.com',
        });

        testCat = new models.Cat({
          name: 'populateTest',
          age: 11,
          owner: testPerson,
        });

        testCat2 = new models.Cat({
          name: 'populate',
          age: 12,
          owner: testPerson,
        });

        testHelper.saveDocs([testCat, testCat2, testPerson], next);
      },
      populateCat(next) {
        models.Cat.findById(testCat._id, (err, foundTestCat) => {
          models.Cat.populate(foundTestCat, { path: 'owner' }, (err, populatedCat) => {
            testHelper.assertErrNull(err);

            assert.equal(populatedCat.owner.name, testPerson.name);
            assert.equal(populatedCat.owner.email, testPerson.email);

            testHelper.saveDocs([populatedCat], next);
          });
        });
      },
      refreshIndex: testHelper.refresh,
      doSearch(next) {
        // search to make sure the cat got indexed
        models.Cat.search({ mustMatch: { name: 'populate' } }, (err, results) => {
          testHelper.assertErrNull(err);

          assert.equal(results.total, 2);
          assert.equal(results.hits.length, 2);

          const firstResult = results.hits[0];
          assert(firstResult);
          assert.equal(firstResult._source.name, 'populate');
          assert.equal(firstResult._source.owner, testPerson.id);

          return next();
        });
      },
      cleanup(next) {
        testHelper.removeDocs([testCat, testCat2, testPerson], next);
      },
      refreshIndex: testHelper.refresh,
      waitForYellowStatus: testHelper.waitForYellowStatus,
    }, done);
  });

  it('autocomplete behavior should work on a schema field with autocomplete: true', (done) => {
    const queries = ['M', 'Ma', 'Man', 'Mang', 'Mango'];

    const searchFns = queries.map(query => (next) => {
      models.Cat.search({ mustMatch: { name: query } }, (err, results) => {
        testHelper.assertErrNull(err);

        assert.equal(results.total, 1);
        assert.equal(results.hits.length, 1);

        const firstResult = results.hits[0];

        assert(firstResult);
        assert.equal(firstResult._source.name, 'Mango');

        return next();
      });
    });

    async.series(searchFns, done);
  });
  //
  // it('autocomplete should split on spaces', function (done) {
  //
  // 	var queries = [ 'z', 'zi', 'zin', 'zing', 'do', 'doo', 'dood', 'doodl', 'doodle' ];
  //
  // 	var searchFns = queries.map(function (query) {
  // 		return function (next) {
  // 			models.Cat.search({ query: query, fields: [ 'name' ] }, function (err, results) {
  // 				testHelper.assertErrNull(err)
  //
  // 				// console.log('results', util.inspect(results, true, 10, true))
  //
  // 				assert.equal(results.total, 1)
  // 				assert.equal(results.hits.length, 1)
  //
  // 				var firstResult = results.hits[0];
  //
  // 				assert(firstResult)
  // 				assert.equal(firstResult._source.name, 'Zing Doodle')
  //
  // 				return next()
  // 			})
  // 		}
  // 	})
  //
  // 	async.series(searchFns, done)
  // })
  //
  it('creating a cat model instance and editing properties should be reflected in Model.search()', (done) => {
    let testCat = null;

    async.series({
      addCat(next) {
        testCat = new models.Cat({
          name: 'Tolga',
          breed: 'turkish',
          age: 5,
        });

        testHelper.saveDocs([testCat], next);
      },
      refreshIndex: testHelper.refresh,
      // search to make sure the cat got indexed
      doSearch(next) {
        models.Cat.search({ mustMatch: { name: 'Tolga' } }, (err, results) => {
          testHelper.assertErrNull(err);

          assert.equal(results.total, 1);
          assert.equal(results.hits.length, 1);

          const firstResult = results.hits[0];

          assert(firstResult);
          assert.equal(firstResult._source.name, 'Tolga');

          return next();
        });
      },
      // update the `testCat` model
      update(next) {
        models.Cat.findById(testCat._id).exec((err, cat) => {
          assert.equal(err, null);

          assert(cat);
          cat.age = 7;
          cat.breed = 'bengal';

          testHelper.saveDocs([cat], next);
        });
      },
      wait(next) {
        // wait 3s for age update
        setTimeout(next, 3000);
      },
      refreshIndex: testHelper.refresh,
      checkUpdates(next) {
        models.Cat.search({ mustMatch: { name: 'Tolga' } }, (err, results) => {
          assert.equal(err, null);

          assert.equal(results.total, 1);
          assert.equal(results.hits.length, 1);

          const firstResult = results.hits[0];

          assert(firstResult);
          assert.equal(firstResult._source.name, 'Tolga');
          assert.equal(firstResult._source.age, 7);
          assert.equal(firstResult._source.breed, 'bengal');

          return next();
        });
      },
      cleanup(next) {
        testHelper.removeDocs([testCat], next);
      },
      refreshIndex: testHelper.refresh,
    }, done);
  });

  it('Model.search() with * should return all results', (done) => {
    setTimeout(() => {
      models.Cat.search({ matchAll: '*' }, (err, results) => {
        testHelper.assertErrNull(err);

        logger.log('info', results);

        assert.equal(results.total, testCats.length);
        assert.equal(results.hits.length, testCats.length);

        return done();
      });
    }, 5000);
  });

  it('elmongoose.search() with * should return all results', (done) => {
    elmongoose.search({ query: '*', collections: ['cats'] }, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, testCats.length);
      assert.equal(results.hits.length, testCats.length);

      return done();
    });
  });

  it('elmongoose.search.config() then elmongoose.search with * should return all results', (done) => {
    elmongoose.search.config({ host: '127.0.0.1', port: 9200 });

    elmongoose.search({ query: '*', collections: ['cats'] }, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, testCats.length);
      assert.equal(results.hits.length, testCats.length);

      return done();
    });
  });

  it('Model.search() with fuzziness 0.5 should return results for `Mangoo`', (done) => {
    models.Cat.fuzzy({ field: 'name', value: 'Mangoo', fuzziness: 2, boost: 1.0 }, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, 1);
      assert.equal(results.hits.length, 1);

      const firstResult = results.hits[0];

      assert(firstResult);
      assert.equal(firstResult._source.name, 'Mango');

      return done();
    });
  });

  it('Model.search() must match any, simaese is defined in name and in breed', (done) => {
    models.Cat.search({ mustAllMatch: ['Siamese'] }, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, 3);
      assert.equal(results.hits.length, 3);

      // when getting these type of results from es, there is no ordering
      const len = results.hits.filter(result => result._source.breed && result._source.breed === 'siamese').length;

      assert.equal(len, 2);

      return done();
    });
  });

  it('Model.search() w/ fields returns the exact match', (done) => {
    models.Cat.search({ mustMatchPhrase: { name: 'Puffy' } }, (err, results) => {
      testHelper.assertErrNull(err);
      assert.equal(results.total, 1);

      return done();
    });
  });

  it('Model.search() with a range', (done) => {
    models.Cat.search({ mustRange: { key: 'age', expression: { gt: 8, lt: 11 } } }, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, 1);
      assert.equal(results.hits.length, 1);

      const firstResult = results.hits[0];

      assert(firstResult);
      assert.equal(firstResult._source.age, 10);

      return done();
    });
  });

  it('Model.search() with `must_not` clause returns correct results', (done) => {
    const numTestCatsExpected = testCats.filter(testCat => testCat.age > 10).length;
    const searchOpts = {
      mustNotMatch: {
        type: 'range',
        expression: {
          age: {
            gt: 10,
          },
        },
      },
      sort: [
        {
          age: 'asc',
        },
      ],
    };

    models.Cat.search(searchOpts, (err, results) => {
      testHelper.assertErrNull(err);

      assert.equal(results.total, numTestCatsExpected);
      assert.equal(results.hits.length, numTestCatsExpected);

      const firstResult = results.hits[1];

      assert(firstResult);
      assert.equal(firstResult._source.age, 15);
      assert.equal(firstResult._source.breed, 'siamese');
      assert.equal(firstResult._source.name, 'Mango');

      return done();
    });
  });
});
