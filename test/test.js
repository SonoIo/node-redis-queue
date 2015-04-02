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
			cli: cli,
			pop: pop
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
			cli: cli,
			pop: pop
		});
		var counter = 0;
		queue.startPolling(function (task, next) {
			assert.equal(task.data.foo, 'bar');
			assert.equal(task.data.num, counter + 1);
			counter = task.data.num;
			setTimeout(function() {
				if (counter < 5) {
					next();
				}
				if (counter === 5) 
					queue.stopPolling();
				
			}, 0);
		});
		queue.once('stop', function() {
			assert.equal(counter, 5);
			done();
		});
		queue.add({ foo: 'bar', num: 1 }, function (err) { if (err) done(err); });
		queue.add({ foo: 'bar', num: 2 }, function (err) { if (err) done(err); });
		queue.add({ foo: 'bar', num: 3 }, function (err) { if (err) done(err); });
		queue.add({ foo: 'bar', num: 4 }, function (err) { if (err) done(err); });
		queue.add({ foo: 'bar', num: 5 }, function (err) { if (err) done(err); });
	});

	it('should complete a task', function (done) {
		var task = { foo: 'bar' };
		var queue = new Queue({ 
			name: 'foo',
			cli: cli,
			pop: pop
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
				});
			});
		});
		queue.add(task, function (err) { if (err) done(err); });
	});

	it('should retry a task', function (done) {
		var task  = { foo: 'bar' };
		var task2 = { foo: 'bar2' };
		var iteration = 0;
		var queue = new Queue({ 
			name: 'foo',
			cli: cli,
			pop: pop
		});
		queue.add(task, function (err) {
			if (err) return done(err);
		});
		queue.add(task2, function (err) {
			if (err) return done(err);
		});
		queue.startPolling(function (task, next) {
			switch (iteration++) {
				case 0: 
					assert.equal(task.data.foo, 'bar');
					assert.equal(task.retry, 0);
					return next(new Error('Fake error'));
				case 1:
					assert.equal(task.data.foo, 'bar2');
					assert.equal(task.retry, 0);
					return next();
				case 2:
					assert.equal(task.data.foo, 'bar');
					assert.equal(task.retry, 1);
					return next(new Error('Fake error'));
				case 3:
					assert.equal(task.data.foo, 'bar');
					assert.equal(task.retry, 2);
					return next(new Error('Fake error'));
				case 4:
					assert.equal(task.data.foo, 'bar');
					assert.equal(task.retry, 3);
					queue.stopPolling();
					return next(new Error('Fake error'));
			}
		});
		queue.on('stop', function() {
			done();
		});
	});

});





