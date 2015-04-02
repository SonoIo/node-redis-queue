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
	else if (options.cli) {
		this.redis.cli = options.cli;
	}

	// Redis polling
	if (options.redisPopClient) {
		this.redis.pop = options.redisPopClient;
	}
	else if (options.pop) {
		this.redis.pop = options.pop;
	}

	this.status = {};
	this.status.polling = false;
	this.maxRetry = 3;
};
util.inherits(Queue, EventEmitter);

// Restituisce l'hash MD5 a partire da data
Queue.getHashFromData = function getHashFromData(data) {
	var string = JSON.stringify(data);
	var hash = crypto
		.createHash('md5')
		.update(string)
		.digest('hex');
	return hash;
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
		// Accoda il codice hash del task
		cli.lpush('queue:' + name, hash, function (err) {
			if (err) return done(err);
			// Aggiunge ad un HASH di redis il task, serve ad impedire
			// che lo stesso task finisca accodato più volte.
			// ATTENZIONE! È chi consuma il task che deve rimuoverlo
			// una volta ultimata l'elaborazione!
			var task = {
				hash: hash,
				data: data,
				retry: 0
			};
			cli.hset('queue:' + name + ':hash', hash, JSON.stringify(task), function (err) {
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
Queue.prototype.startPolling = function startPolling(iterator) {
	if (!this.redis.pop)
		throw new Error('Cannot polling the queue without a redis connection');

	// Polling già avviato
	if (this.isPolling())
		return;

	var self = this;
	var cli  = this.redis.cli;
	var pop  = this.redis.pop;
	var name = this.name;

	self.status.polling = true;	
	process.nextTick(function() {
		self.emit('start');
		polling(callback);
	});

	function callback (err, task) {
		if (err) self.emit('error', err);
		if (!self.isPolling()) return self.emit('stop');
		iterator(task, function (err) {
			if (err) {
				// In caso di errore riprova il task
				retry(task, function (err) {
					if (err) return done(err);
					polling(callback);
				});
			}
			else {
				polling(callback);
			}
		});
	}

	function retry (task, done) {
		if (task.retry >= self.maxRetry) {
			return self.complete(task.hash, done);
		}
		task.retry++;
		self.redis.cli.hset('queue:' + self.name + ':hash', task.hash, JSON.stringify(task), function (err) {
			if (err) return done(err);
			self.redis.cli.lpush('queue:' + self.name, task.hash, function (err) {
				if (err) return done(err);
				done();
			});
		});
	}

	function polling(iterator) {
		pop.brpop('queue:' + name, 0, function (err, data) {
			if (err) return iterator(err);
			var hash = data[1];
			cli.hget('queue:' + name + ':hash', hash, function (err, data) {
				if (err) return iterator(err);
				var parsedData;
				try { 
					parsedData = JSON.parse(data); 
				}
				catch (e) { 
					return iterator(e);
				}
				return iterator(null, {
					hash: parsedData.hash,
					data: parsedData.data,
					retry: parsedData.retry
				});
			});
		});
	};

};

// Ferma il polling a partire dalla prossima lettura
Queue.prototype.stopPolling = function stopPolling() {
	this.status.polling = false;
	this.emit('stop');
};



