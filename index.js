var { Scope, Expression } = require('ql');
var Context = require('ql/Context');
var selectize = require('sql').selectize;
var tabulize = require('sql').tabulize;
var QL = Symbol('ql');
var TYPE = Symbol('type');
function qlsql(ql) {
	var global = this;
	var table = require('lodash.transform')(global.scope.type, (result, value, key) => {
		result[global.scope.table ? global.scope.table(key) : key] = [value];
	});
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
				if (!resolution) {
					if (ql.identifier in table) {
						var $identifier = ql.identifier;
						sql = [{ type: 'name', identifier: ql.identifier }, table[ql.identifier]];
						break;
					}
					if (ql.identifier in constant) {
						var $identifier = ql.identifier;
						sql = [runtime.constant[$identifier], constant[ql.identifier]];
						break;
					}
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
							new Expression.Id(
								tablename(value),
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
			case 'object':
				var $property = ql.property.map(
					property => ({
						name: property.name,
						value: qlsql.call(this, property.value)
					})
				);
				sql = [{
					type: 'select',
					field: $property.map(
						property => Object.assign(
							property.value[0],
							{ as: property.name }
						)
					),
					from: []
				}, $property.reduce(
					(o, p) => Object.assign(
						o,
						{ [p.name]: { type: p.value[1] } }
					),
					{}
				)];
				break;
			case 'array':
				var $element = ql.element.map(
					element => qlsql.call(this, element)
				);
				sql = [$element.reduce((left, right) => ({
					type: 'union',
					all: true,
					left: tabulize(left[0]),
					right: tabulize(right[0])
				})), [$element[0][1]]];
				break;
			case 'tuple':
				var $element = ql.element.map(
					element => qlsql.call(this, element)
				);
				sql = [{
					type: 'select',
					field: $element.map(e => e[0]),
					from: []
				}, new (require('ql/Type').Tuple)($element.map(e => e[1]))];
				break;
			case 'id':
				var type = global.scope.type[ql.identifier];
				var [$id] = qlsql.call(this, ql.id);
				var $table = global.scope.table ? global.scope.table(ql.identifier) : ql.identifier;
				var alias = `_${i++}`;
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [{ type: 'name', identifier: $table, kind: 'table', alias: alias }],
					where: {
						type: 'operation',
						operator: '=',
						left: { type: 'name', qualifier: alias, identifier: require('ql/Type.id')(type) },
						right: $id
					}
				}, type];
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
			case 'element':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$index] = qlsql.call(this, ql.index);
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					limit: { type: 'literal', value: 1 },
					offset: $index
				}, type[0]];
				break;
			case 'call':
				var [$expression, type] = qlsql.call(this, ql.expression);
				if (
					type.argument instanceof Array &&
					typeof type.argument[0] == 'string'
				) {
					[$argument] = qlsql.call(this, ql.argument);
					sql = [{
						type: 'select',
						field: [{
							type: 'call',
							callee: { type: 'name', identifier: runtime.constant[$expression] },
							argument: [{ type: 'name', identifier: '*' }]
						}],
						from: [Object.assign($argument, { alias: `_${i++}` })]
					}, type.result];
				} else
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
			case 'conditional':
				var [$condition, typeCondition] = qlsql.call(this, ql.condition);
				var [$true, type] = qlsql.call(this, ql.true);
				var [$false] = qlsql.call(this, ql.false);
				$condition = truthy($condition, typeCondition);
				sql = [{
					type: 'call',
					callee: { type: 'name', identifier: 'if' },
					argument: [$condition, $true, $false]
				}, type];
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
			case 'limit':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$start] = qlsql.call(this, ql.limiter.element[0]);
				var [$length] = qlsql.call(this, ql.limiter.element[1]);
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					limit: $length,
					offset: $start
				}, type];
				break;
			case 'order':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$orderer] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({}, type[0]),
							{ alias: { from: alias } }
						)
					),
					ql.orderer
				);
				sql = [{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					order: $orderer,
					direction: ql.direction
				}, type];
				break;
			case 'distinct':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				sql = [{
					type: 'select',
					distinct: true,
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})]
				}, type];
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
		var _type = global.scope.type;
		for (var name in _type)
			if (_type[name] == type)
				return global.scope.table ? global.scope.table(name) : name;
	}
}
var constant = {
	false: 'boolean',
	true: 'boolean',
	length: new (require('ql/Type').Function)('string', 'number'),
	substr: new (require('ql/Type').Function)(new (require('ql/Type').Tuple)(['string', 'number']), 'string'),
	sum: new (require('ql/Type').Function)(['number'], 'number'),
	avg: new (require('ql/Type').Function)(['number'], 'number'),
	min: new (require('ql/Type').Function)(['number'], 'number'),
	max: new (require('ql/Type').Function)(['number'], 'number')
};
var runtime = {
	constant: {
		false: { type: 'name', identifier: 'false', kind: 'scalar' },
		true: { type: 'name', identifier: 'true', kind: 'scalar' },
		length: qlsql => argument => ({ type: 'call', callee: { type: 'name', identifier: 'length' }, argument: [qlsql(argument)[0]] }),
		substr: qlsql => argument => ({ type: 'call', callee: { type: 'name', identifier: 'substr' }, argument: [qlsql(argument.element[0])[0], { type: 'operation', operator: '+', left: qlsql(argument.element[1])[0], right: { type: 'literal', value: 1 } }] }),
		sum: 'sum',
		avg: 'avg',
		min: 'min',
		max: 'max'
	}
};
var operator = {
	'&': '&&',
	'|': '||'
};

module.exports = qlsql;
