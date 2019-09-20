var assert = require('assert');
var ql = require('ql');
var generate = require('sql-generate');
var mysql = require('mysql');
var qlsql = require('..');
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
	var sql = qlsql(q);
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
