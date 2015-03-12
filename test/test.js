var assert = require('chai').assert;
var Queue = require('../lib/Queue');
var redis = require('redis');

var cli;
var pop;

describe('Queue', function() {

	before(function (done) {
		cli = redis.createClient();
		pop = redis.createClient();
		cli.select(7, function (err) {
			if (err) return done(err);
			pop.select(7, function (err) {
				if (err) return done(err);
				return done();
			});
		});
	});

	beforeEach(function (done) {
		cli.flushdb(function (err) {
			if (err) return done(err);
			return done();
		});
	});

	it('Should add a new task', function (done) {
		var queue = new Queue({ 
			name: 'foo',
			redisClient: cli,
			redisPopClient: pop
		});
		var task = { foo: 'bar' };
		queue.add(task, function (err, hash) {
			if (err) return done(err);
			cli.llen('queue:foo', function (err, len) {
				if (err) return done(err);
				assert.equal(len, 1);
				// Try to add the same task for the second time,
				// this should have no effect
				queue.add(task, function (err, hash) {
					if (err) return done(err);
					cli.llen('queue:foo', function (err, len) {
						if (err) return done(err);
						assert.equal(len, 1);
						return done();
					});
				});
			});
		});
	});

	it('Should polling a queue', function (done) {
		var queue = new Queue({ 
			name: 'foo',
			redisClient: cli,
			redisPopClient: pop
		});
		var counter = 0;
		queue.startPolling(function (data, next) {
			assert.equal(data.foo, 'bar');
			assert.equal(data.num, counter + 1);
			counter = data.num;
			setTimeout(function() {
				if (counter < 5)
					next();
				else
					next(new Error('Fake error'));
			}, 0);
		}, function (err) {
			assert.equal(counter, 5);
			assert.equal(err.message, 'Fake error');
			return done();
		});

		for (var i = 1; i <= 5; i++) {
			queue.add({ foo: 'bar', num: i }, function (err) {
				if (err) return done(err);
			});
		}
	});

});