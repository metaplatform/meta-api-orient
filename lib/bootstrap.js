/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var ensureClass = function(db, className, superClass){

	return new Promise(function(resolve, reject){

		try {

			db.class.get(className).then(function(res){
				resolve();
			}, function(err){

				return db.class.create(className, superClass).then(resolve, reject);

			});

		} catch(e) {
			reject(e);
		}

	});

};

var ensureFunction = function(db, name, fn){

	return new Promise(function(resolve, reject){

		try {

			db.query("SELECT " + name + "()").then(function(res){
				
				resolve();

			}, function(err){

				return db.createFn(name, fn);

			});

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