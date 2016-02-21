/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

module.exports = {
	
	//Endpoints
	Collection: require("./lib/collection.js"),

	//Utils
	Bootstrap: require("./lib/bootstrap.js"),
	Query: require("./lib/query.js"),

	//Queue manager
	QueueManager: require("./lib/queueManager.js")

};