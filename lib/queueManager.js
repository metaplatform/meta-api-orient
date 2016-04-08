/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var logger = require("meta-logger").facility("OrientQueue");
var OrientDB = require('orientjs');

var bootstrap = require("./bootstrap.js");

/*
 * Memory queue manager
 *
 * Create memory queue manager
 */
var OrientQueue = function(handler, options){

	var self = this;

	if(!options) options = {};

	this.defaultTTL = options.defaultTTL || 1800; //18
	this.maxTTL = options.maxTTL || 1800; //32
	this.timeout = options.timeout || 5000;
	this.errorRatio = options.errorTimeout || 1000;
	this.interval = options.flushInterval || 5000;
	this.limit = options.flushLimit || 20;

	this.handler = handler;

	this.checker = setInterval(function(){

		self.flushQueue();

	}, this.interval);

	//Connect DB
	var server = OrientDB({
		host:  		options.orientdb.host,
		port:  		options.orientdb.port,
		username: 	options.orientdb.username,
		password: 	options.orientdb.password
	});

	this.db = server.use(options.orientdb.database);

	//Init
	var initTasks = [];

	initTasks.push(bootstrap.ensureClass(this.db, "broker_queue"));
	initTasks.push(bootstrap.ensureClass(this.db, "broker_queue_subscribers"));
	
	initTasks.push(bootstrap.ensureFunction(this.db, "pow", function(base, exponent){
		return Math.pow(base, exponent);
	}));

	Promise.all(initTasks).then(function(){

		logger.info("Initialized.");

	}, function(err){

		logger.error("Cannot initialize Queue DB:", err);

	});

};

OrientQueue.prototype.subscribe = function(channel, serviceName){

	var self = this;

	return this.db.select("COUNT(*)").from('broker_queue_subscribers').where({ channel: channel, service: serviceName }).scalar().then(function(cnt){

		if(cnt > 0) return true;

		return this.db.insert().into("broker_queue_subscribers").set({
			channel: channel,
			service: serviceName
		}).scalar().then(function(){
			
			return true;

		});

	});

};

OrientQueue.prototype.unsubscribe = function(channel, serviceName){

	return this.db.delete().from('broker_queue_subscribers').where({ channel: channel, service: serviceName }).scalar().then(function(){

		return true;

	});

};

/*
 * Add message to queue
 *
 * @param Mixed message
 * @param Array recipients
 * @param Integer ttl
 * @return Promise
 * @resolve true
 */
OrientQueue.prototype.enqueue = function(channel, message, ttl){

	var self = this;

	return this.db.select().from('broker_queue_subscribers').where({ channel: channel }).all().then(function(records){

		if(records.length === 0) return false;

		var recipients = [];

		for(var i in records) recipients.push(records[i].service);

		return new Promise(function(resolve, reject){

			try {

				var msg = {
					channel: channel,
					message: message,
					recipients: recipients,
					ttl: Math.max(0, Math.min(ttl, self.maxTTL)) || self.defaultTTL,
					locked: false,
					lockTimestamp: null,
					lockOwner: null,
					errors: 0
				};

				self.db.insert().into("broker_queue").set(msg).one().then(function(record){

					logger.debug("Message #%s enqueued.", record["@rid"]);

					self.flushMessage(record["@rid"]);

					resolve(true);

				}, reject);

			} catch(e){
				reject(e);
			}

		});

	});

};

/*
 * Removes message from queue
 *
 * @param String rid
 */
OrientQueue.prototype.removeMessage = function(rid){

	this.db.delete().from('broker_queue').where({ "@rid": rid }).limit(1).scalar().then(function(cnt){

		if(cnt == 1)
			logger.debug("Message %s removed from queue.", rid);
		else
			logger.debug("Message %s not in queue.", rid);

	}, function(err) {

		logger.error("Cannot remove message %s:", rid, err);

	});

};

/*
 * Flushes current queue
 */
OrientQueue.prototype.flushQueue = function(){

	var self = this;

	var currentTimestamp = (new Date()).getTime();
	var timestampLimit = currentTimestamp - this.timeout;

	logger.debug("Flushing queue.");

	//Remove expired TTLs
	this.db.query("DELETE FROM broker_queue WHERE ttl < 0").then(function(){

		//Get available messages
		self.db.query("SELECT * FROM broker_queue WHERE locked = FALSE OR ( locked = TRUE AND eval(\"lockTimestamp + pow(2, (errors - 1)) * " + self.errorRatio + "\") < " + timestampLimit + " ) LIMIT " + self.limit).then(function(results){

			for(var i in results)
				self.handleMessage(results[i]);

		}, function(err){

			logger.error("Cannot query queue:", err);

		});

	});

};

OrientQueue.prototype.flushMessage = function(rid){

	var self = this;

	logger.debug("Flushing message {" + rid + "}");

	this.db.record.get(rid).then(function(record){

		self.handleMessage(record);

	}, function(err){
		logger.warn("Cannot flush message {" + rid + "}:", err);
	});

};

/*
 * Handles message delivery
 *
 * @param Object record
 */
OrientQueue.prototype.handleMessage = function(record){

	var self = this;

	var msg = {
		channel: record.channel,
		message: record.message,
		recipients: record.recipients,
		ttl: record.ttl,
		locked: record.locked,
		lockTimestamp: record.lockTimestamp,
		lockOwner: record.lockOwner,
		errors: record.errors
	};

	logger.debug("Handling message %s, lock: %s, TTL: %d", record["@rid"], ( msg.locked ? msg.lockOwner : "no" ), msg.ttl);

	var currentTimestamp = (new Date()).getTime();

	//Check if timed-out
	if(msg.locked && (msg.lockTimestamp + Math.pow(2, msg.errors - 1) * this.errorRatio) < currentTimestamp - this.timeout){

		//Pass owner to recipients end
		msg.recipients.push(msg.lockOwner);
		msg.errors++;
		msg.ttl--;

		logger.debug("Message %s timed-out.", record["@rid"]);

	}

	var recipient = msg.recipients.shift();

	msg.locked = true;
	msg.lockTimestamp = currentTimestamp;
	msg.lockOwner = recipient;

	//Update lock
	this.db.update('broker_queue').set(msg).where({ "@rid": record["@rid"], lockTimestamp: record.lockTimestamp }).scalar().then(function(cnt){

		if(cnt === 0 || cnt == "0"){
			logger.debug("Message %s lock missed.", record["@rid"], cnt);
			return;
		}

		/*
		 * Deliver message
		 */
		var deliver = function(localRecipient){

			if(!localRecipient) return;

			self.handler(msg.channel, localRecipient, msg.message).then(function(remove){

				if(localRecipient != msg.lockOwner){

					logger.warn("Message %s got confirmation but from invalid recipient {%s}.", record["@rid"], localRecipient);
					return;

				}

				logger.debug("Message %s successfully delivered to {%s}.", record["@rid"], localRecipient);

				var oldTimestamp = msg.lockTimestamp;

				msg.locked = false;
				msg.lockTimestamp = null;
				msg.lockOwner = null;
				msg.errors = 0;

				self.db.update('broker_queue').set(msg).where({ "@rid": record["@rid"], lockTimestamp: oldTimestamp }).scalar().then(function(cnt){

					if(cnt === 0 || cnt == "0"){
						logger.debug("Message %s lock missed (#2).", record["@rid"]);
						return;
					}					

					//Remove?
					if(remove === true || msg.recipients.length === 0)
						self.removeMessage(record["@rid"]);
					else
						self.flushMessage(record["@rid"]);

				}, function(err) {

					logger.error("Cannot update message %s:", record["@rid"], err);

				});

			}, function(err){

				logger.warn("Failed to deliver message %s to {%s}, reason: %s.", record["@rid"], localRecipient, err, err.stack);

				var oldTimestamp = msg.lockTimestamp;

				//Pass owner and reset
				if(msg.recipients.length > 0){
					msg.recipients.push(localRecipient);
					msg.locked = false;
					msg.lockTimestamp = null;
					msg.lockOwner = null;
				}

				msg.errors++;
				msg.ttl--;

				self.db.update('broker_queue').set(msg).where({ "@rid": record["@rid"], lockTimestamp: oldTimestamp }).scalar().then(function(cnt){

					if(cnt === 0){
						logger.debug("Message %s lock missed (#3).", record["@rid"]);
						return;
					}					

				}, function(err) {

					logger.error("Cannot update message %s:", record["@rid"], err);

				});

			});

		};

		//Try to deliver
		if(recipient)
			deliver(recipient);
		else
			self.removeMessage(record["@rid"]);

	}, function(err){

		logger.error("Cannot update message %s:", record["@rid"], err);

	});

};

//EXPORT
module.exports = OrientQueue;