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

	it('should add a new task', function (done) {
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

	it('should polling a queue', function (done) {
		var queue = new Queue({ 
			name: 'foo',
			redisClient: cli,
			redisPopClient: pop
		});
		var counter = 0;
		queue.startPolling(function (task, next) {
			assert.equal(task.data.foo, 'bar');
			assert.equal(task.data.num, counter + 1);
			counter = task.data.num;
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

	it('should complete a task', function (done) {
		var task = { foo: 'bar' };
		var queue = new Queue({ 
			name: 'foo',
			redisClient: cli,
			redisPopClient: pop
		});
		queue.add(task, function (err) {
			if (err) return done(err);
		});
		queue.startPolling(function (task, next) {
			assert.equal(task.hash, Queue.getHashFromData(task.data));
			assert.equal(task.data.foo, 'bar');
			queue.complete(task.hash, function (err) {
				if (err) return done(err);
				cli.hget('queue:foo:hash', task.hash, function (err, res) {
					if (err) return done(err);
					assert.isNull(res);
					return done();
				})
			})
		});
	});

});





