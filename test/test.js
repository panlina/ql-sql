var assert = require('assert');
var ql = require('ql');
var generate = require('sql').generate;
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
it('store', async function () {
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
it('false', async function () {
	var q = ql.parse('false');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, 'boolean'));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query("select false")
	]);
	assert.deepEqual(actual, expected);
});
it('store#1.address.city.country.country', async function () {
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
it('actor#1.films', async function () {
	var q = ql.parse("actor#1.films");
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, [type.film]));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query(`
			select film.* from film_actor
			join film on film_actor.film_id=film.film_id
			join actor on film_actor.actor_id=actor.actor_id
			where actor.actor_id=1;
		`)
	]);
	assert.deepEqual(actual, expected);
});
it("How many Academy Dinosaur's are available from store 1?", async function () {
	var q = ql.parse('(inventory|store_id=1&&film.title="ACADEMY DINOSAUR")#');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, 'number'));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query(`
			select count(*) from inventory
			join film on inventory.film_id=film.film_id
			where store_id=1 and film.title='ACADEMY DINOSAUR'
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
