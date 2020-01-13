var { Scope, Expression } = require('ql');
var Context = require('ql/Context');
var QL = Symbol('ql');
var TYPE = Symbol('type');
function qlsql(ql) {
	var global = this;
	var i = 0;
	this.scope.alias = { local: {} };
	for (var name in this.scope.local)
		this.scope.alias.local[name] = name;
	var [sql, type] = qlsql.call(this, ql);
	decorrelate(sql);
	sql = require('sql').reduce(sql);
	return [selectize(sql), type];
	function decorrelate(sql) {
		for (var s of require('sql').traverse(sql))
			if (
				s[QL] && s[QL].type == 'map' &&
				typeof s[QL][TYPE][0] == 'object' &&
				!(s[QL][TYPE][0] instanceof Array)
			)
				require('sql').decorrelate(s);
	}
	function qlsql(ql) {
		var sql;
		switch (ql.type) {
			case 'literal':
				sql = [{
					type: 'literal',
					value: ql.value
				}, typeof ql.value];
				break;
			case 'name':
				var resolution = Context.resolve.call(this, global, ql);
				if (!resolution)
					if (ql.identifier in constant) {
						var $identifier = ql.identifier;
						sql = [runtime.constant[$identifier], constant[ql.identifier]];
						break;
					}
				var [value, [depth, key]] = resolution;
				var scope = Context.ancestor.call(this, global, depth).scope;
				var alias = scope.alias;
				if (key == 'this')
					sql = qlsql.call(this,
						new Expression.Property(
							new Expression.Name('this', depth),
							ql.identifier
						)
					);
				else {
					sql = ql.identifier == 'this' && !scope.local.this && alias.from ?
						qlsql.call(
							global,
							new Expression.Index(
								new Expression.Name(tablename(value), Infinity),
								Object.assign(
									new Expression('sql'),
									{
										sql: {
											type: 'name',
											qualifier: alias.from,
											identifier: require('ql/Type.id')(value)
										}
									}
								)
							)
						) :
						[
							{
								type: 'name',
								identifier: ql.identifier == 'this' && !scope.local.this ?
									alias.this :
									alias.local[ql.identifier],
								kind: 'table'
							},
							value
						];
				}
				break;
			case 'this':
				var type = global.scope.type[ql.identifier];
				var [, , , depth] = this.find(value => value == type, { key: 'local', name: 'this' });
				sql = qlsql.call(this, new Expression.Name('this', depth));
				break;
			case 'property':
				var thisResolution = resolveThis.call(this, ql.expression);
				if (thisResolution) {
					var [scope] = thisResolution;
					if (scope.this[ql.property].value) {
						sql = qlsql.call(
							global.push(
								Object.assign(
									new Scope({}, scope.this),
									{ alias: { this: scope.alias.this, from: scope.alias.from } }
								)
							),
							scope.this[ql.property].value
						);
						break;
					} else
						if (scope.alias.from) {
							sql = [{
								type: 'name',
								qualifier: scope.alias.from,
								identifier: ql.property,
								kind: 'scalar'
							}, scope.this[ql.property].type];
							break;
						}
				}
				function resolveThis(expression) {
					if (expression.type == 'name' && expression.identifier == 'this') {
						var [value, [depth, key]] = Context.resolve.call(this, global, expression);
						var scope = Context.ancestor.call(this, global, depth).scope;
						if (!scope.local.this)
							return [scope, depth];
					}
					if (expression.type == 'this') {
						var type = global.scope.type[expression.identifier];
						var [, , , depth] = this.find(value => value == type, { key: 'local', name: 'this' });
						var scope = Context.ancestor.call(this, global, depth).scope;
						return [scope, depth];
					}
				}
				var [$expression, type] = qlsql.call(this, ql.expression);
				if (type[ql.property].value) {
					var aliasThis = `_${i++}`;
					var [$value, type] = qlsql.call(
						global.push(
							Object.assign(
								new Scope({}, type),
								{ alias: { this: aliasThis } }
							)
						),
						type[ql.property].value
					);
					var aliasValue = `_${i++}`;
					sql = [{
						type: 'select',
						with: {
							name: aliasThis,
							value: selectize($expression)
						},
						field: [{ type: 'name', qualifier: aliasValue, identifier: '*' }],
						from: [Object.assign(tabulize($value), {
							alias: aliasValue
						})]
					}, type];
				} else {
					var alias = `_${i++}`;
					sql = [{
						type: 'select',
						field: [{ type: 'name', qualifier: alias, identifier: ql.property }],
						from: [Object.assign($expression, {
							alias: alias
						})]
					}, type[ql.property].type];
				}
				break;
			case 'index':
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$index] = qlsql.call(this, ql.index);
				var alias = `_${i++}`;
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					where: {
						type: 'operation',
						operator: '=',
						left: { type: 'name', qualifier: alias, identifier: require('ql/Type.id')(type[0]) },
						right: $index
					}
				}, type[0]];
				break;
			case 'call':
				var [$expression, type] = qlsql.call(this, ql.expression);
				sql = [$expression(qlsql.bind(this))(ql.argument), type.result];
				break;
			case 'operation':
				if (ql.left)
					var [$left, typeLeft] = qlsql.call(this, ql.left);
				if (ql.right)
					var [$right, typeRight] = qlsql.call(this, ql.right);
				sql = [
					ql.operator == '#' ?
						{
							type: 'select',
							field: [{
								type: 'call',
								callee: { type: 'name', identifier: 'count' },
								argument: [{ type: 'name', identifier: '*' }]
							}],
							from: [Object.assign($left, {
								alias: `_${i++}`
							})]
						} : {
							type: 'operation',
							operator: operator[ql.operator] || ql.operator,
							left: $left,
							right: $right
						},
					require('ql/Type.operate')(ql.operator, typeLeft, typeRight)
				];
				break;
			case 'filter':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$filter, typeFilter] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({}, type[0]),
							{ alias: { from: alias } }
						)
					),
					ql.filter
				);
				$filter = truthy($filter, typeFilter);
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					where: $filter
				}, type];
				break;
			case 'map':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$mapper, typeMapper] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({}, type[0]),
							{ alias: { from: alias } }
						)
					),
					ql.mapper
				);
				sql = [{
					type: 'select',
					field: [$mapper],
					from: [Object.assign($expression, {
						alias: alias
					})]
				}, [typeMapper]];
				break;
			case 'comma':
				var aliasHead = `_${i++}`;
				var [$value, type] = qlsql.call(this, ql.head.value);
				var [$body, type] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({ [ql.head.name]: type }),
							{ alias: { local: { [ql.head.name]: aliasHead } } }
						)
					),
					ql.body
				);
				var aliasBody = `_${i++}`;
				sql = [{
					type: 'select',
					with: {
						name: aliasHead,
						value: selectize($value)
					},
					field: [{ type: 'name', qualifier: aliasBody, identifier: '*' }],
					from: [Object.assign(tabulize($body), {
						alias: aliasBody
					})]
				}, type];
				break;
			case 'sql':
				sql = [ql.sql];
				break;
		}
		sql[0][QL] = ql;
		ql[TYPE] = sql[1];
		return sql;
	}
	function truthy(sql, type) {
		if (typeof type == 'object')
			sql = {
				type: 'operation',
				operator: 'exists',
				right: sql
			};
		return sql;
	}
	function tablename(type) {
		var local = global.scope.local;
		for (var name in local)
			if (local[name][0] == type)
				return name;
	}
}
var constant = {
	false: 'boolean',
	true: 'boolean',
	length: new (require('ql/Type').Function)('string', 'number')
};
var runtime = {
	constant: {
		false: { type: 'name', identifier: 'false', kind: 'scalar' },
		true: { type: 'name', identifier: 'true', kind: 'scalar' },
		length: qlsql => argument => ({ type: 'call', callee: { type: 'name', identifier: 'length' }, argument: [qlsql(argument)[0]] })
	}
};
var operator = {
	'&': '&&',
	'|': '||'
};

function tabulize(sql) {
	if (sql.type != 'select')
		if (sql.type != 'name' || sql.kind == 'scalar')
			sql = {
				type: 'select',
				field: [sql],
				from: []
			};
	return sql;
}

function selectize(sql) {
	if (sql.type != 'select')
		if (sql.type == 'name' && sql.kind != 'scalar')
			sql = {
				type: 'select',
				field: [{ type: 'name', identifier: '*' }],
				from: [sql]
			};
		else
			sql = {
				type: 'select',
				field: [sql],
				from: []
			};
	return sql;
}

module.exports = qlsql;
