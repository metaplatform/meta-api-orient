/*
 * META API
 *
 * @author META Platform <www.meta-platform.com>
 * @license See LICENSE file distributed with this source code
 */

var escapeValue = function(str){

	return String(str).replace(/[\0\x08\x09\x1a\n\r"'\\]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });

};

var sanitizeColumn = function(val){
	if(val.indexOf("`") >= 0) throw new Error("Invalid query.");
	//return "`" + escapeValue(val) + "`";
	return escapeValue(val);
};

var sanitizeValue = function(val){

	var num = parseFloat(val);

	if(String(val).match(/^#[0-9]+\:[0-9]+$/))
		return val;
	else if(val === null)
		return "NULL";
	else if(val === "$notNull")
		return "NOT NULL";
	else if(val === true)
		return "TRUE";
	else if(val === false)
		return "FALSE";
	else if(isNaN(num)){
		return "'" + escapeValue(val) + "'";
	} else {
		return val;
	}
};

/*
 * SELECT SQL BUILDER
 */
var buildSelect = function(properties){

	if(properties.length === 0)
		return "@rid AS `_id`, *";

	for(var i in properties)
		properties[i] = sanitizeColumn(properties[i]);

	properties.unshift("@rid AS `_id`");

	return properties.join(",");

};

/*
 * WHERE SQL BUILDER
 */
var buildConditions = function(where){

	var checkArray = function(sep, statements){

		var out = [];

		for(var i in statements)
			out.push(checkStatement(statements[i]).join(" AND "));

		return "(" + out.join(" " + sep + " ") + ")";

	};

	var checkStatement = function(statement, key){

		var out = [];

		for(var i in statement){

			switch(i){

				case '$and':
					out.push( checkArray("AND", statement[i]) );
					break;

				case '$or':
					out.push( checkArray("OR", statement[i]) );
					break;

				case '$not':
					out.push( "NOT " + checkArray("AND", statement[i]) );
					break;

				case '$contains':
					out.push( "CONTAINS " + checkArray("AND", statement[i]) );
					break;

				case '$containsall':
					out.push( "CONTAINSALL " + checkArray("AND", statement[i]) );
					break;

				case '$containskey':
					out.push( "CONTAINSKEY " + checkArray("AND", statement[i]) );
					break;

				case '$containsvalue':
					out.push( "CONTAINSVALUE " + checkArray("AND", statement[i]) );
					break;

				case '$containstext':
					out.push( "CONTAINSTEXT " + checkArray("AND", statement[i]) );
					break;

				case '$eq':
					if(!key) throw new Error("Invalid query syntax.");
					if(statement[i] === null)
						out.push(sanitizeColumn(key) + " IS NULL");
					else
						out.push(sanitizeColumn(key) + " = " + sanitizeValue(statement[i]));
					break;

				case '$ne':
					if(!key) throw new Error("Invalid query syntax.");
					if(statement[i] === null)
						out.push(sanitizeColumn(key) + " IS NOT NULL");
					else
						out.push(sanitizeColumn(key) + " <> " + sanitizeValue(statement[i]));
					break;

				case '$gt':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " > " + sanitizeValue(statement[i]));
					break;

				case '$gte':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " >= " + sanitizeValue(statement[i]));
					break;

				case '$lt':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " < " + sanitizeValue(statement[i]));
					break;

				case '$lte':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " < " + sanitizeValue(statement[i]));
					break;

				case '$match':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " MATCHES " + sanitizeValue(statement[i]));
					break;

				case '$like':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " LIKE " + sanitizeValue(statement[i]));
					break;

				case '$range':
					if(!key || !statement[i].from || !statement[i].to) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " BETWEEN " + sanitizeValue(statement[i].from) + " AND " + sanitizeValue(statement[i].to));
					break;

				case '$instanceof':
					if(!key) throw new Error("Invalid query syntax.");
					out.push(sanitizeColumn(key) + " INSTANCEOF " + sanitizeValue(statement[i]));
					break;

				case '$in':
					if(!key) throw new Error("Invalid query syntax.");
					var inValues = [];
					for(var v in statement[i]) inValues.push(sanitizeValue(statement[i][v]));
					out.push(sanitizeColumn(key) + " IN [" + inValues.join(",") + "]");
					break;

				case '$nin':
					if(!key) throw new Error("Invalid query syntax.");
					var ninValues = [];
					for(var w in statement[i]) ninValues.push(sanitizeValue(statement[i][w]));
					out.push(sanitizeColumn(key) + " NOT IN [" + ninValues.join(",") + "]");
					break;

				default:
					if(statement[i] instanceof Object){
						
						var s = checkStatement(statement[i], i);
						out.push( s );

					}
					else if(statement[i] === null)
						out.push(sanitizeColumn(i) + " IS NULL");
					else if(statement[i] === "$notNull")
						out.push(sanitizeColumn(i) + " IS NOT NULL");
					else
						out.push(sanitizeColumn(i) + " = " + sanitizeValue(statement[i]));

			}

		}

		return out;

	};

	var out = checkStatement(where);

	return (out.length > 0 ? " WHERE " + out.join(" AND "): "" );

};

/*
 * SORT SQL BUILDER
 */
var buildSort = function(sort){
	
	parts = [];

	for(var i in sort)
		if(sort[i] > 0)
			parts.push(i + " ASC");
		else
			parts.push(i + " DESC");

	return (parts.length > 0 ? " ORDER BY " + parts.join(",") : "" );

};

/*
 * NORMALIZE RECORD
 */
var normalizeRecord = function(record){

	record._id = String(record._id && record._id != record ? String(record._id) : record["@rid"] ).substr(1);

	return record;

};

//EXPORT
module.exports = {
	
	escapeValue:  			escapeValue,
	sanitizeColumn: 		sanitizeColumn,
	sanitizeValue: 			sanitizeValue,
	buildSelect: 			buildSelect,
	buildConditions: 		buildConditions,
	buildSort: 				buildSort,
	normalizeRecord: 		normalizeRecord,

	builder: function(command, from, properties, where, sort, offset, limit){

		var selectSql = buildSelect(properties || []);
		var whereSql = buildConditions(where || {});
		var sortSql = buildSort(sort || {});

		return command + " " + selectSql + " FROM " + from + whereSql + sortSql + ( offset ? " SKIP " + offset : "" ) + ( limit ? " LIMIT " + limit : "" );

	}

};