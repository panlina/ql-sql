var { Scope, Expression } = require('ql');
function qlsql(ql) {
	var global = this;
	var i = 0;
	this.scope.alias = { local: {} };
	for (var name in this.scope.local)
		this.scope.alias.local[name] = name;
	var [sql, type] = qlsql.call(this, ql);
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
				var [value, [depth, key]] = resolve.call(this, ql);
				var scope = ancestor.call(this, global, depth).scope;
				var alias = scope.alias;
				if (key == 'this' && value.value)
					sql = qlsql.call(
						global.push(
							Object.assign(
								new Scope({}, scope.this),
								{ alias: { this: alias.this } }
							)
						),
						scope.this[ql.identifier].value
					);
				else
					sql = [
						ql.identifier == 'this' ?
							qlsql.call(this, new Expression.Index(
								new Expression.Name(typename(scope.this), Infinity),
								new Expression.Name(require('ql/Type.id')(scope.this))
							))[0] :
							{
								type: 'name',
								identifier:
									key == 'local' ?
										alias.local[ql.identifier] :
										key == 'this' ? `${alias.this}.${ql.identifier}` :
											undefined
							},
						value
					];
				function resolve(expression) {
					if (expression.depth == Infinity)
						var [value, key] = global.scope.resolve(expression.identifier),
							depth = Infinity;
					else
						var [value, [depth, key]] = this.resolve(expression.identifier);
					return [value, [depth, key]];
				}
				function ancestor(global, depth) {
					return depth == Infinity ?
						global :
						this.ancestor(depth);
				}
				break;
			case 'property':
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
						from: Object.assign($value, {
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
						type: 'binary',
						operator: '=',
						left: { type: 'name', identifier: require('ql/Type.id')(type[0]) },
						right: $index
					}
				}, type[0]];
				break;
			case 'unary':
				var [$operand, type] = qlsql.call(this, ql.operand);
				sql = [{
					type: 'unary',
					operator: ql.operator,
					operand: $operand
				}, operate(ql.operator, type)];
				break;
			case 'binary':
				var [$left, typeLeft] = qlsql.call(this, ql.left);
				var [$right, typeRight] = qlsql.call(this, ql.right);
				sql = [{
					type: 'binary',
					operator: ql.operator,
					left: $left,
					right: $right
				}, operate(ql.operator, typeLeft, typeRight)];
				break;
			case 'filter':
				var alias = `_${i++}`;
				var [$expression, type] = qlsql.call(this, ql.expression);
				var [$filter] = qlsql.call(
					this.push(
						Object.assign(
							new Scope({}, type[0]),
							{ alias: { this: alias } }
						)
					),
					ql.filter
				);
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
					from: Object.assign($body, {
						alias: `_${i++}`
					})
				}, type];
				break;
		}
		return sql;
	}
	function typename(type) {
		var local = global.scope.local;
		for (var name in local)
			if (local[name][0] == type)
				return name;
	}
}
function operate(operator, left, right) {
	switch (operator) {
		case '+':
			return left;
		case '-':
			return 'number';
		case '<=':
		case '=':
		case '>=':
		case '<':
		case '!=':
		case '>':
		case '!':
		case '&&':
		case '||':
			return 'boolean';
	}
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
