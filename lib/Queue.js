var crypto = require('crypto');
var util = require('util');
var EventEmitter = require('events').EventEmitter;


var Queue = module.exports = function Queue(options) {
	if (!options || typeof options.name === 'undefined') {
		throw new Error('Cannot initialize a queue without a name');
	}
	this.name = options.name;

	// Redis
	this.redis = {};

	// Redis read
	if (options.redisClient) {
		this.redis.cli = options.redisClient;
	}

	// Redis polling
	if (options.redisPopClient) {
		this.redis.pop = options.redisPopClient;
	}

	this.status = {};
	this.status.polling = false;
};
util.inherits(Queue, EventEmitter);

// Restituisce l'hash MD5 a partire da data
Queue.getHashFromData = function getHashFromData(data) {
	var string = JSON.stringify(data);
	var hash = crypto
		.createHash('md5')
		.update(string)
		.digest('hex');
	return hash
};

// Aggiunge alla coda un task impedendo che non venga
// inserito più volte
Queue.prototype.add = function add(data, done) {
	if (!this.redis.cli)
		throw new Error('Cannot add a task to the queue without a redis connection');
	var cli  = this.redis.cli;
	var hash = Queue.getHashFromData(data);
	var name = this.name;
	// Verifica che non esista un task uguale accodato o in lavorazione
	cli.hexists('queue:' + name + ':hash', hash, function (err, exists) {
		if (err) return done(err);
		// Il task è già accodato in attesa di elaborazione
		if (exists) return done(null, hash);
		// Accoda il task
		cli.lpush('queue:' + name, JSON.stringify(data), function (err) {
			if (err) return done(err);
			// Aggiunge ad un HASH di redis questo task, serve ad impedire
			// che lo stesso task finisca accodato più volte.
			// ATTENZIONE! È chi consuma il task che deve rimuoverlo da qui
			// una volta ultimata l'elaborazione!
			cli.hset('queue:' + name + ':hash', hash, 'true', function (err) {
				if (err) return done(err);
				return done(null, hash);
			});
		});
	});
};

// Rimuove il task dalla tabella HASH in lavorazione
Queue.prototype.complete = function complete(hash, done) {
	if (!this.redis.cli)
		throw new Error('Cannot complete the task without a redis connection');
	var cli = this.redis.cli;
	var name = this.name;
	cli.hdel('queue:' + name + ':hash', hash, function (err) {
		if (err) return done(err);
		return done();
	});
};

// Controlla se la cosa è in polling
Queue.prototype.isPolling = function isPolling() {
	return this.status.polling;
};

// Inizia il polling sulla LIST di Redis con nome this.name
Queue.prototype.startPolling = function startPolling(iterator, done) {
	if (!this.redis.pop)
		throw new Error('Cannot polling the queue without a redis connection');

	// Polling già avviato
	if (this.isPolling())
		return done();

	var self = this;
	this.status.polling = true;
	polling();

	function polling () {
		process.nextTick(function() {
			self._polling(callback);
		});
	}

	function callback (err, task) {
		if (err) return done(err);
		if (!self.isPolling()) return done();
		iterator(task, function (err) {
			if (err) return done(err);
			self._polling(callback);
		});
	}
};

Queue.prototype._polling = function _polling(iterator) {
	var pop  = this.redis.pop;
	var name = this.name;

	pop.brpop('queue:' + name, 0, function (err, data) {
		if (err) return iterator(err);
		var hash;
		var parsedData;
		try {
			parsedData = JSON.parse(data[1]);
			hash       = Queue.getHashFromData(parsedData);
		}
		catch (e) {
			iterator(e);
		}
		return iterator(null, {
			hash: hash,
			data: parsedData
		});
	});
};

// Ferma il polling a partire dalla prossima lettura
Queue.prototype.stopPolling = function stopPolling() {
	this.status.polling = false;
};










