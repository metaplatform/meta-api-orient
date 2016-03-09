/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var Shared = require("meta-api-shared");
var Query = require("./query.js");

/*
 * Schematic endpoint constructor
 */
var OrientCollection = function(db, schema){

	//Validate schema
	if(!schema.className)
		throw new Error("Class name must be defined.");

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
			methods: 		schema.recordMethods || {},

			init: function(id){

				var self = this;

				return db.record.get("#" + id).then(function(record){

					Query.normalizeRecord(record);

					self.record = record;
					return true;

				}, function(err){

					if(err.name == "OrientDB.RequestError")
						throw new Shared.Endpoints.Errors.EndpointNotFound(self._schema.path);
					else
						throw err;

				});

			},

			get: function(){

				if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

				return Promise.resolve(this.record);

			},

			update: function(params){

				if(schema.allowUpdate === false) return Promise.reject(new Error("Not allowed."));

				return db.update(schema.className).set(params).where({ "@rid": this.record["@rid"] }).scalar();

			},

			delete: function(){

				if(schema.allowDelete === false) return Promise.reject(new Error("Not allowed."));

				return db.delete().from(schema.className).where({ "@rid": this.record["@rid"] }).scalar();

			},

			live: function(params){

				if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

				return db.liveQuery("LIVE SELECT * FROM " + schema.className + " WHERE @rid = " + this.record["@rid"]);

			}

		},

		count: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			return new Promise(function(resolve, reject){

				try {

					var conds = Query.buildConditions(params.where);

					db.query("SELECT COUNT(*) FROM " + schema.className + conds).then(function(res){
						resolve(res[0].COUNT);
					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		query: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			return new Promise(function(resolve, reject){

				try {

					var sql = Query.builder("SELECT", schema.className, params.properties, params.where, params.sort, params.skip, params.limit);

					return db.query(sql).then(function(records){

						for(var i in records)
							Query.normalizeRecord(records[i]);

						resolve(records);

					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		create: function(params){
			
			if(schema.allowCreate === false) return Promise.reject(new Error("Not allowed."));

			return db.insert().into(schema.className).set(params).one().then(function(record){

				return record["@rid"].toString().substr(1);
				
			});

		},

		delete: function(params){

			if(schema.allowDelete === false) return Promise.reject(new Error("Not allowed."));

			var ids = [];

			for(var i in params.id)
				ids.push("#" + params.id[i]);
			
			return db.exec("DELETE FROM " + schema.className + " WHERE @rid IN [" + ids.join(",") + "]");

		},

		live: function(params){

			if(schema.allowGet === false) return Promise.reject(new Error("Not allowed."));

			return new Promise(function(resolve, reject){

				try {

					var sql = Query.builder("LIVE SELECT", schema.className, params.properties, params.where, params.sort, params.skip, params.limit);

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