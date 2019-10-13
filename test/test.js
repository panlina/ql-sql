var assert = require('assert');
var ql = require('ql');
var generate = require('sql-generate');
var mysql = require('mysql');
var qlsql = require('..');
var type = require('./type');
var type = require('lodash.mapvalues')(type, require('ql/Type.parse'));
var type = require('lodash.mapvalues')(type, value => [value]);
var connection;
before(function () {
	connection = mysql.createConnection({
		host: 'stillnotworking.today',
		user: 'root',
		database: 'sakila'
	});
	connection.connect();
});
after(function () {
	connection.end();
});
it('', async function () {
	var q = ql.parse("store");
	var [sql, t] = qlsql.call(new ql.Environment(new ql.Scope(type)), q);
	assert(require('ql/Type.equals')(t, type.store));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query("select * from store")
	]);
	assert.deepEqual(actual, expected);
});
function query(sql) {
	return new Promise((resolve, reject) => {
		connection.query(sql, function (error, results) {
			if (error) reject(error);
			else resolve(results);
		});
	});
}
