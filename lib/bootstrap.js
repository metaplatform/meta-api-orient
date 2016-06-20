/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var ensureClass = function(db, className, superClass, properties){

	return new Promise(function(resolve, reject){

		try {

			var checkProperties = function(classInstance){

				try {

					if(!properties || properties.length === 0) return resolve();

					classInstance.property.list().then(function(propList){

						var propIndex = {};
						var newProps = [];
						var updateProps = [];

						var tasks = [];

						for(var i in propList)
							propIndex[propList[i].name] = propList[i];
								
						for(var p in properties)
							if(propIndex[properties[p].name])
								updateProps.push(properties[p]);
							else
								newProps.push(properties[p]);

						tasks.push(classInstance.property.create(newProps));

						for(var u in updateProps)
							tasks.push(classInstance.property.update(updateProps[u]));

						Promise.all(tasks).then(resolve, reject);

					}, reject);

				} catch(e){
					reject(e);
				}

			};

			db.class.get(className).then(function(classInstance){
				
				checkProperties(classInstance);

			}, function(err){

				return db.class.create(className, superClass).then(function(classInstance){
					checkProperties(classInstance);
				}, reject);

			});

		} catch(e) {
			reject(e);
		}

	});

};

var ensureFunction = function(db, name, fn){

	return new Promise(function(resolve, reject){

		try {

			db.delete().from('OFunction').where({ name: name }).one().then(function(res){
				
				db.createFn(name, fn).then(resolve, reject);

			}, reject);

		} catch(e) {
			reject(e);
		}

	});

};

//EXPORT
module.exports = {

	ensureClass: ensureClass,
	ensureFunction: ensureFunction

};