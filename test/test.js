var fs = require('fs');
var path = require('path');
var assert = require('assert');
var ql = require('ql');
var generate = require('sql').generate;
var mysql = require('mysql');
var qlsql = require('..');
var type = ql.parse(fs.readFileSync(path.join(__dirname, 'type.ql'), 'utf8'), 'Declarations');
var type = require('ql/Type.compile')(type);
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
it('length "abc"', async function () {
	var q = ql.parse('length "abc"');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, 'number'));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query('select length("abc")')
	]);
	assert.deepEqual(actual, expected);
});
it('{a:0,b:"a"}', async function () {
	var q = ql.parse('{a:0,b:"a"}');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, { a: { type: 'number' }, b: { type: 'string' } }));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query('select 0 as a, "a" as b')
	]);
	assert.deepEqual(actual, expected);
});
it('[0,1]', async function () {
	var q = ql.parse('[0,1]');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, ['number']));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query('select 0 union select 1')
	]);
	assert.deepEqual(actual, expected);
});
it('[{a:0,b:"a"},{a:1,b:"b"}]', async function () {
	var q = ql.parse('[{a:0,b:"a"},{a:1,b:"b"}]');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, [{ a: { type: 'number' }, b: { type: 'string' } }]));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query('select 0 as a, "a" as b union select 1 as a, "b" as b')
	]);
	assert.deepEqual(actual, expected);
});
it('0 in [0,1]', async function () {
	var q = ql.parse('0 in [0,1]');
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, 'boolean'));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query("select 0 in (0,1)")
	]);
	assert.equal(Object.values(actual[0])[0], Object.values(expected[0])[0]);
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
describe('map', function () {
	it('store map address', async function () {
		var q = ql.parse("store map this map address");
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.address]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`
				select address.* from store, address
				where address.address_id=store.address_id
			`)
		]);
		assert.deepEqual(actual, expected);
	});
	it('store map address.city', async function () {
		var q = ql.parse("store map address.city");
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.city]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`
				select city.* from store, address, city
				where address.address_id=store.address_id and city.city_id=address.city_id
			`)
		]);
		assert.deepEqual(actual, expected);
	});
	it('store map address.city.country', async function () {
		var q = ql.parse("store map address.city.country");
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.country]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`
				select country.* from store, address, city, country
				where address.address_id=store.address_id
					and city.city_id=address.city_id
					and country.country_id=city.country_id
			`)
		]);
		assert.deepEqual(actual, expected);
	});
	it('store map this map address', async function () {
		var q = ql.parse("store map this map address");
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.address]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`
				select address.* from store, address
				where address.address_id=store.address_id
			`)
		]);
		assert.deepEqual(actual, expected);
	});
})
describe('limit', function () {
	it("film limit [1,10]", async function () {
		var q = ql.parse('film limit [1,10]');
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.film]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`select * from film limit 1, 10`)
		]);
		assert.deepEqual(actual, expected);
	});
	it("film limit [1,10] limit [1,9]", async function () {
		var q = ql.parse('film limit [1,10] limit [1,9]');
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.film]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`select * from film limit 2, 9`)
		]);
		assert.deepEqual(actual, expected);
	});
});
describe('order', function () {
	it("actor order first_name limit [0,10]", async function () {
		var q = ql.parse('actor order first_name limit [0,10]');
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.actor]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`select * from actor order by first_name limit 0, 10`)
		]);
		assert.deepEqual(actual, expected);
	});
	it("actor limit [0,10] order first_name", async function () {
		var q = ql.parse('actor limit [0,10] order first_name');
		var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
		assert(require('ql/Type.equals')(t, [type.actor]));
		var sql = generate(sql);
		var [actual, expected] = await Promise.all([
			query(sql),
			query(`
				select * from (
					select * from actor limit 0, 10
				)
				_ order by first_name
			`)
		]);
		assert.deepEqual(actual, expected);
	});
});
it("How many Academy Dinosaur's are available from store 1?", async function () {
	var q = ql.parse('(inventory where store_id=1&film.title="ACADEMY DINOSAUR")#');
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
it('category map {category:name,length:(avg (films map length))} order category', async function () {
	var q = ql.parse("category map {category:name,length:(avg (films map length))} order category");
	var [sql, t] = qlsql.call(new ql.Environment(Object.assign(new ql.Scope(local), { type: type })), q);
	assert(require('ql/Type.equals')(t, [{ category: { type: 'string' }, length: { type: 'number' } }]));
	var sql = generate(sql);
	var [actual, expected] = await Promise.all([
		query(sql),
		query(`
			select category.name as category, avg(length) as length from film, film_category, category
			where film.film_id=film_category.film_id and category.category_id=film_category.category_id
			group by category.category_id
			order by category
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
