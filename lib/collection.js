/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var Shared = require("meta-api-shared");
var Query = require("./query.js");

var excludeParams = Shared.Utils.excludeParams;

var lockTimeout = 1800;

/*
 * Schematic endpoint constructor
 */
var OrientCollection = function(db, schema){

	//Validate schema
	if(!schema.className)
		throw new Error("Class name must be defined.");

	var recordMethods = schema.recordMethods || {};

	recordMethods.restore = function(params){

		if(schema.allowRestore !== true) return Promise.reject(new Error("Not allowed."));

		var self = this;

		return db.update(schema.className).set({"_deleted": false}).where({ "@rid": this.record["@rid"] }).scalar().then(function(res){

			if(schema.afterRestore)
				return schema.afterRestore.call(self, self._schema.id, params, res);
			else
				return res;

		});

	};

	recordMethods.lock = function(params){

		if(!params._caller) return Promise.reject(new Error("Caller is not set."));

		if(schema.allowLock === false) return Promise.reject(new Error("Not allowed."));

		var self = this;

		var lock = {
			user: params._caller,
			timestamp: Math.round((new Date()).getTime() / 1000)
		};

		return db.update(schema.className).set({"_locked": lock}).where({ "@rid": this.record["@rid"] }).scalar().then(function(res){

			if(schema.afterLock)
				return schema.afterLock.call(self, self._schema.id, lock, params, res);
			else
				return lock;

		});

	};

	recordMethods.unlock = function(params){

		if(!params._caller) return Promise.reject(new Error("Caller is not set."));

		if(schema.allowLock === false) return Promise.reject(new Error("Not allowed."));

		if(!this.record._locked) return Promise.reject(new Error("Not locked."));

		var now = Math.round((new Date()).getTime() / 1000);

		if(this.record._locked && this.record._locked.user != params._caller && this.record._locked.timestamp > (now - lockTimeout)) return Promise.reject(new Error("Record locked by another user."));

		var self = this;

		return db.update(schema.className).set({"_locked": null}).where({ "@rid": this.record["@rid"] }).scalar().then(function(res){

			if(schema.afterLock)
				return schema.afterLock.call(self, self._schema.id, params, res);
			else
				return true;

		});

	};

	/*
	 * Create endpoint
	 */
	return Shared.Endpoints.Collection({

		schema:  	schema.collection || {},
		properties: schema.collectionProperties || {},
		methods:  	schema.collectionMethods || {},

		record: {
			schema:  		schema.record || {},
			properties: 	schema.properties || {},
			_properties: 	schema.recordProperties || {},
			methods: 		recordMethods || {},

			init: function(id){

				var self = this;

				var conds = { "@rid": id };

				if(schema.conditionsCb)
					conds = schema.conditionsCb.call(self, conds, true);

				var sql = Query.builder("SELECT", schema.className, null, conds);

				return db.query(sql).then(function(result){

					if(!result[0])
						throw new Shared.Endpoints.Errors.EndpointNotFound(self._schema.path);

					Query.normalizeRecord(result[0]);

					self.record = result[0];
					return true;

				}, function(err){

					throw err;

				});

			},

			get: function(params){

				if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

				if(this.record._deleted)
					return Promise.reject(new Shared.Endpoints.Errors.EndpointNotFound(this._schema.path));

				if(schema.getMapper)
					return schema.getMapper.call(this, this.record, params.resolve);
				else
					return Promise.resolve(this.record);

			},

			update: function(params){

				if(schema.allowUpdate === false) return Promise.reject(new Error("Not allowed."));

				var now = Math.round((new Date()).getTime() / 1000);

				if(this.record._locked){
					if(!params._caller) return Promise.reject(new Error("Caller must be set when record is locked."));
					if(this.record._locked.user != params._caller && this.record._locked.timestamp > (now - lockTimeout)) return Promise.reject(new Error("Record locked by another user."));
				}

				var self = this;
				var p;

				if(schema.beforeUpdate)
					p = schema.beforeUpdate.call(self, params);
				else
					p = Promise.resolve(params);

				return p.then(function(params){

					return db.update(schema.className).set(excludeParams(params, ["_caller"])).where({ "@rid": self.record["@rid"] }).scalar().then(function(res){

						if(schema.afterUpdate)
							return schema.afterUpdate.call(self, self._schema.id, params, res);
						else
							return res;

					});

				});

			},

			delete: function(params){

				if(schema.allowDelete === false) return Promise.reject(new Error("Not allowed."));

				var self = this;
				var p;

				if(schema.beforeDelete)
					p = schema.beforeDelete.call(self, params, true);
				else
					p = Promise.resolve(params);

				return p.then(function(params){

					return db.update(schema.className).set({"_deleted": true}).where({ "@rid": self.record["@rid"] }).scalar().then(function(res){

						if(schema.afterDelete)
							return schema.afterDelete.call(self, self._schema.id, params, res, true);
						else
							return res;

					});

				});

			},

			live: function(params){

				if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

				return db.liveQuery("LIVE SELECT * FROM " + schema.className + " WHERE @rid = " + this.record["@rid"]);

			},

			liveMapper: function(record, data, op){

				if(!record._id)
					record._id = data.cluster + ":" + data.position;

				if(schema.liveRecordMapper)
					return schema.liveRecordMapper.call(this, record, data, op);
				else
					return Promise.resolve(record);

			},

		},

		count: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			var self = this;

			return new Promise(function(resolve, reject){

				try {

					var conds = params.where || {};

					if(schema.conditionsCb)
						conds = schema.conditionsCb.call(self, conds, false);

					if(Object.keys(conds).length > 0)
						conds = { "$and": [ params.where, {"$or": [ {"_deleted": null}, {"_deleted": { "$ne": true }} ] } ] };
					else
						conds = { "$or": [ {"_deleted": null}, {"_deleted": { "$ne": true }} ] };

					var where = Query.buildConditions(conds);

					db.query("SELECT COUNT(*) FROM " + schema.className + where).then(function(res){
						resolve(res[0].COUNT);
					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		query: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			var self = this;

			return new Promise(function(resolve, reject){

				try {

					var conds = params.where || {};

					if(schema.conditionsCb)
						conds = schema.conditionsCb.call(self, conds, false);

					if(Object.keys(conds).length > 0)
						conds = { "$and": [ params.where, {"$or": [ {"_deleted": null}, {"_deleted": { "$ne": true }} ] } ] };
					else
						conds = { "$or": [ {"_deleted": null}, {"_deleted": { "$ne": true }} ] };

					var sql = Query.builder("SELECT", schema.className, params.properties, conds, params.sort, params.skip, params.limit);

					return db.query(sql).then(function(records){

						for(var i in records)
							Query.normalizeRecord(records[i]);

						if(schema.queryMapper)
							schema.queryMapper.call(self, records, params.resolve).then(resolve, reject);
						else
							resolve(records);

					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		create: function(params){
			
			if(schema.allowCreate === false) return Promise.reject(new Error("Not allowed."));

			var self = this;
			var p;

			if(schema.beforeCreate)
				p = schema.beforeCreate.call(self, params);
			else
				p = Promise.resolve(params);

			return p.then(function(params){

				return db.insert().into(schema.className).set(excludeParams(params, ["_caller"])).one().then(function(record){

					var rid = record["@rid"].toString().substr(1);

					if(schema.afterCreate)
						return schema.afterCreate.call(self, rid, params);
					else
						return rid;

				});

			});

		},

		delete: function(params){

			if(schema.allowDelete === false) return Promise.reject(new Error("Not allowed."));

			var self = this;

			var createContext = function(record){

				var ctx = {};

				for(var i in self)
					ctx[i] = self[i];

				ctx.record = record;

				return ctx;

			};

			return new Promise(function(resolve, reject){

				try {

					var idList = params.id.map(function(i){ return "#" + i; });

					var sql = Query.builder("SELECT", schema.className, [], { "$and": [ { "@rid": { "$in": idList } }, {"$or": [ {"_deleted": null}, {"_deleted": { "$ne": true }} ] } ] });
					
					db.query(sql).then(function(records){

						var idMap = [];

						for(var i in records){
							Query.normalizeRecord(records[i]);
							idMap.push(records[i]._id);
						}

						//Check IDs
						for(var j in params.id)					
							if(idMap.indexOf(params.id[j]) < 0) return reject(new Error("Record #" + params.id[j] + " not found."));

						//Call before hooks
						var beforeTasks = [];

						if(schema.beforeDelete)
							for(var k in records)
								beforeTasks.push( schema.beforeDelete.call(createContext(records[k]), params, false) );

						//Set deleted
						Promise.all(beforeTasks).then(function(){

							return db.exec("UPDATE " + schema.className + " SET _deleted = TRUE WHERE @rid IN [" + idList.join(",") + "]").then(function(){

								//Call after hooks
								var afterHooks = [];

								if(schema.afterDelete)
									for(var l in records)
										afterHooks.push( schema.afterDelete.call(createContext(records[l]), records[l]._id, params, false) );

								Promise.all(afterHooks).then(function(){
									
									resolve(params.id);

								}, reject);

							}, reject);

						}, reject);

					}, reject);

				} catch(e) {
					reject(e);
				}

			}).catch(function(err){
				console.error(err, err.stack);
				throw err;
			});

		},

		live: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			var self = this;

			return new Promise(function(resolve, reject){

				try {

					var conds = params.where || {};

					if(schema.conditionsCb)
						conds = schema.conditionsCb.call(self, conds, false);

					var sql = Query.builder("LIVE SELECT", schema.className, params.properties, conds, params.sort, params.skip, params.limit);

					return db.liveQuery(sql).then(resolve, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		liveMapper: function(record, data, op){

			if(!record._id)
				record._id = data.cluster + ":" + data.position;

			if(schema.liveMapper)
				return schema.liveMapper.call(this, record, data, op);
			else
				return Promise.resolve(record);

		},

		map: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			return new Promise(function(resolve, reject){

				try {

					var idList = params.id.map(function(i){ return "#" + i; });

					var sql = Query.builder("SELECT", schema.className, params.properties, { "@rid": { "$in": idList } });

					return db.query(sql).then(function(records){

						var map = {};

						for(var i in records){
							Query.normalizeRecord(records[i]);
							map[records[i]._id] = records[i];
						}

						resolve(map);

					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		liveMap: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			return new Promise(function(resolve, reject){

				try {

					var idList = params.id.map(function(i){ return "#" + i; });

					var sql = Query.builder("LIVE SELECT", schema.className, params.properties, { "@rid": { "$in": idList } });

					return db.liveQuery(sql).then(resolve, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		liveMapMapper: function(record, data, op){

			if(!record._id)
				record._id = data.cluster + ":" + data.position;

			var res = {};
			res[record._id] = record;

			if(schema.liveMapMapper)
				return schema.liveMapMapper.call(this, res, data, op);
			else
				return Promise.resolve(res);

		}

	});

};

//EXPORT
module.exports = OrientCollection;