var assert = require('assert');
var ql = require('ql');
var generate = require('sql-generate');
var mysql = require('mysql');
var qlsql = require('..');
var type = require('./type');
var type = require('lodash.mapvalues')(type, require('ql/Type.parse'));
var local = require('lodash.mapvalues')(type, value => [value]);
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
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, [type.store]));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query("select * from store")
	]);
	assert.deepEqual(actual, expected);
});
it('', async function () {
	var q = ql.parse("store#1.address.city.country.country");
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, 'string'));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query(`
			select country from country where country_id=(
				select country_id from city where city_id=(
					select city_id from address where address_id=(
						select address_id from store where store_id=1
					)
				)
			)
		`)
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
