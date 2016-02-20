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

				return Promise.resolve(this.record);

			},

			update: function(params){

				return db.update(schema.className).set(params).where({ "@rid": this.record["@rid"] }).scalar();

			},

			delete: function(){

				return db.delete().from(schema.className).where({ "@rid": this.record["@rid"] }).scalar();

			},

			live: function(params){

				return db.liveQuery("LIVE SELECT * FROM " + schema.className + " WHERE @rid = " + this.record["@rid"]);

			}

		},

		count: function(params){

			return new Promise(function(resolve, reject){

				try {

					var conds = Query.buildConditions(params.where);

					console.log("SQL DEBUG", "SELECT COUNT(*) FROM " + schema.className + conds);

					db.query("SELECT COUNT(*) FROM " + schema.className + conds).then(function(res){
						console.log("SQL RESULT", res);
						resolve(res[0].COUNT);
					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		query: function(params){

			return new Promise(function(resolve, reject){

				try {

					var sql = Query.builder("SELECT", schema.className, params.properties, params.where, params.sort, params.skip, params.limit);

					console.log("SQL DEBUG", sql);

					return db.query(sql).then(function(records){

						for(var i in records){
							records[i]._id = String(records[i]._id != records[i] ? String(records[i]._id) : records[i]["@rid"] ).substr(1);
						}

						resolve(records);

					}, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		create: function(params){
			
			return db.insert().into(schema.className).set(params).one().then(function(record){
				return record["@rid"].toString().substr(1);
			});

		},

		delete: function(params){

			var ids = [];

			for(var i in params.id)
				ids.push("#" + params.id[i]);
			
			return db.exec("DELETE FROM " + schema.className + " WHERE @rid IN [" + ids.join(",") + "]");

		},

		live: function(params){

			return new Promise(function(resolve, reject){

				try {

					var sql = Query.builder("LIVE SELECT", schema.className, params.properties, params.where, params.sort, params.skip, params.limit);

					console.log("SQL DEBUG", sql);

					return db.liveQuery(sql).then(resolve, reject);

				} catch(e) {
					reject(e);
				}

			});

		},

		liveMapper: function(record, data){

			if(!record._id)
				record._id = data.cluster + ":" + data.position;

			return record;

		}

	});

};

//EXPORT
module.exports = OrientCollection;