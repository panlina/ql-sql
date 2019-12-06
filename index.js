var { Scope, Expression } = require('ql');
var Context = require('ql/Context');
function qlsql(ql) {
	var global = this;
	var i = 0;
	this.scope.alias = { local: {} };
	for (var name in this.scope.local)
		this.scope.alias.local[name] = name;
	var [sql, type] = qlsql.call(this, ql);
	sql = require('sql').reduce(sql);
	return [selectize(sql), type];
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
				var [value, [depth, key]] = Context.resolve.call(this, global, ql);
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
					sql = ql.identifier == 'this' && !scope.local.this && alias.filteree ?
						qlsql.call(
							global,
							new Expression.Index(
								new Expression.Name(tablename(value), Infinity),
								Object.assign(
									new Expression('sql'),
									{
										sql: {
											type: 'name',
											identifier: `${alias.filteree}.${require('ql/Type.id')(value)}`
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
									{ alias: { this: scope.alias.this, filteree: scope.alias.filteree } }
								)
							),
							scope.this[ql.property].value
						);
						break;
					} else
						if (scope.alias.filteree) {
							sql = [{
								type: 'name',
								identifier: `${scope.alias.filteree}.${ql.property}`,
								kind: 'field'
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
					var alias = `_${i++}`;
					var [$value, type] = qlsql.call(
						global.push(
							Object.assign(
								new Scope({}, type),
								{ alias: { this: alias } }
							)
						),
						type[ql.property].value
					);
					sql = [{
						type: 'select',
						with: {
							name: alias,
							value: selectize($expression)
						},
						field: [{ type: 'name', identifier: '*' }],
						from: Object.assign(tabulize($value), {
							alias: `_${i++}`
						})
					}, type];
				} else
					sql = [{
						type: 'select',
						field: [{ type: 'name', identifier: ql.property }],
						from: Object.assign($expression, {
							alias: `_${i++}`
						})
					}, type[ql.property].type];
				break;
			case 'index':
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$index] = qlsql.call(this, ql.index);
				sql = [{
					type: 'select',
					field: [{ type: 'name', identifier: '*' }],
					from: Object.assign($expression, {
						alias: `_${i++}`
					}),
					where: {
						type: 'operation',
						operator: '=',
						left: { type: 'name', identifier: require('ql/Type.id')(type[0]) },
						right: $index
					}
				}, type[0]];
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
							field: [{ type: 'name', identifier: 'count(*)' }],
							from: Object.assign($left, {
								alias: `_${i++}`
							})
						} : {
							type: 'operation',
							operator: ql.operator,
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
							{ alias: { filteree: alias } }
						)
					),
					ql.filter
				);
				$filter = truthy($filter, typeFilter);
				sql = [{
					type: 'select',
					field: [{ type: 'name', identifier: '*' }],
					from: Object.assign($expression, {
						alias: alias
					}),
					where: $filter
				}, type];
				break;
			case 'comma':
				var alias = `_${i++}`;
				var [$value, type] = qlsql.call(this, ql.head.value);
				var [$body, type] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({ [ql.head.name]: type }),
							{ alias: { local: { [ql.head.name]: alias } } }
						)
					),
					ql.body
				);
				sql = [{
					type: 'select',
					with: {
						name: alias,
						value: selectize($value)
					},
					field: [{ type: 'name', identifier: '*' }],
					from: Object.assign(tabulize($body), {
						alias: `_${i++}`
					})
				}, type];
				break;
			case 'sql':
				sql = [ql.sql];
				break;
		}
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

function tabulize(sql) {
	if (sql.type != 'select')
		if (sql.type != 'name' || sql.kind == 'field')
			sql = {
				type: 'select',
				field: [sql]
			};
	return sql;
}

function selectize(sql) {
	if (sql.type != 'select')
		if (sql.type == 'name')
			sql = {
				type: 'select',
				field: [{ type: 'name', identifier: '*' }],
				from: sql
			};
		else
			sql = {
				type: 'select',
				field: [sql]
			};
	return sql;
}

module.exports = qlsql;
