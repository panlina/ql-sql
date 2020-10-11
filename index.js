var TYPE = require('ql').TYPE;
var QL = require('ql').QL;
var ALIAS = Symbol('alias');
var { Scope, Expression } = require('ql');
var Context = require('ql/Context');
var selectize = require('./selectize');
var tabulize = require('./tabulize');
var i;
var compile;
var global;
var interpretation = {
	pre(_global, _compile) {
		global = _global;
		compile = _compile;
		i = 0;
		global.scope.alias = { local: {} };
		for (var name in global.scope.local)
			global.scope.alias.local[name] = name;
	},
	post(sql) {
		decorrelate(sql);
		sql = require('sql').reduce(sql);
		return selectize(sql, sql[TYPE]);
		function decorrelate(sql) {
			for (var s of require('sql').traverse(sql))
				if (
					s[QL] && s[QL].type == 'map' &&
					typeof s[TYPE][0] == 'object' &&
					!(s[TYPE][0] instanceof Array)
				)
					require('sql').decorrelate(s);
		}
	},
	expression: {
		literal($value) {
			return {
				type: 'literal',
				value: $value
			};
		},
		name: {
			table($identifier) {
				return { type: 'name', identifier: $identifier };
			},
			constant($identifier) {
				return runtime.constant[$identifier];
			},
			name($identifier, resolution) {
				var [value, [depth, key]] = resolution;
				var scope = Context.ancestor.call(this, global, depth).scope;
				var alias = scope.alias;
				// If it's a this, and it's a row,
				return $identifier == 'this' && !scope.local.this && alias.thisrow ?
					// If it's not a primitive, reselect it from the original table, since sql does not support "all columns in current row" as a value;
					typeof value != 'string' ?
						compile.call(
							global,
							new Expression.Id(
								tablename(value),
								Object.assign(
									new Expression('sql'),
									{
										sql: {
											type: 'name',
											qualifier: alias.thisrow,
											identifier: require('ql/Type.id')(value)
										}
									}
								)
							)
						) :
						// else it's a primitive, directly member-access it;
						{ type: 'name', qualifier: alias.thisrow, identifier: '', kind: 'scalar' } :
				// else either it's a local, or it's a table, it can be referenced by an alias.
					(alias =>
						typeof scope.local[$identifier] != 'string' ?
							{
								type: 'name',
								identifier: alias,
								kind: 'table'
							} :
							{
								type: 'select',
								field: [{ type: 'name', qualifier: alias, identifier: '' }],
								from: [{ type: 'name', identifier: alias }],
								kind: 'scalar'
							}
					)(
						$identifier == 'this' && !scope.local.this ?
							alias.this :
							alias.local[$identifier]
					);
			}
		},
		object($property) {
			return {
				type: 'select',
				field: $property.map(
					property => Object.assign(
						property.value,
						{ as: property.name }
					)
				),
				from: []
			};
		},
		array($element) {
			return $element.reduce((left, right) => ({
				type: 'union',
				all: true,
				left: tabulize(left, left[TYPE]),
				right: tabulize(right, right[TYPE])
			}));
		},
		tuple($element) {
			return {
				type: 'select',
				field: $element.map((e, i) => Object.assign(e, { as: i })),
				from: []
			};
		},
		find($table, $property, $id) {
			var alias = `_${i++}`;
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: alias, identifier: '*' }],
				from: [{ type: 'name', identifier: $table, kind: 'table', alias: alias }],
				where: {
					type: 'operation',
					operator: '=',
					left: { type: 'name', qualifier: alias, identifier: $property },
					right: $id
				}
			};
		},
		field($expression, $property) {
			var alias = `_${i++}`;
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: alias, identifier: $property, as: '' }],
				from: [Object.assign($expression, {
					alias: alias
				})]
			};
		},
		element($expression, $index) {
			var alias = `_${i++}`;
			return $expression[TYPE] instanceof Array ?
				{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: '*' }],
					from: [Object.assign($expression, {
						alias: alias
					})],
					limit: { type: 'literal', value: 1 },
					offset: $index
				} :
				{
					type: 'select',
					field: [{ type: 'name', qualifier: alias, identifier: $index.value, as: '' }],
					from: [Object.assign($expression, {
						alias: alias
					})]
				};
		},
		call($expression, $argument) {
			if (
				$expression[TYPE].argument instanceof Array &&
				typeof $expression[TYPE].argument[0] == 'string'
			)
				return {
					type: 'select',
					field: [{
						type: 'call',
						callee: { type: 'name', identifier: runtime.constant[$expression] },
						argument: [{ type: 'name', identifier: '*' }],
						as: ''
					}],
					from: [Object.assign($argument, { alias: `_${i++}` })]
				};
			else
				return $expression(compile.bind(this))($argument[QL]);
		},
		operation($operator, $left, $right) {
			return $operator == '#' ?
				{
					type: 'select',
					field: [{
						type: 'call',
						callee: { type: 'name', identifier: 'count' },
						argument: [{ type: 'name', identifier: '*' }],
						as: ''
					}],
					from: [Object.assign($left, {
						alias: `_${i++}`
					})]
				} :
				$operator == '+' && $left[TYPE] == 'string' && $right[TYPE] == 'string' ? {
					type: 'call',
					callee: { type: 'name', identifier: 'concat' },
					argument: [$left, $right]
				} : {
						type: 'operation',
						operator: operator[$operator] || $operator,
						left: $left,
						right: $right
					};
		},
		conditional($condition, $true, $false) {
			$condition = truthy($condition, $condition[TYPE]);
			return {
				type: 'call',
				callee: { type: 'name', identifier: 'if' },
				argument: [$condition, $true, $false]
			};
		},
		filter($expression, $filter) {
			$filter = truthy($filter, $filter[TYPE]);
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: $expression[ALIAS], identifier: '*' }],
				from: [Object.assign($expression, {
					alias: $expression[ALIAS]
				})],
				where: $filter
			};
		},
		which($expression, $filter) {
			$filter = truthy($filter, $filter[TYPE]);
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: $expression[ALIAS], identifier: '*' }],
				from: [Object.assign($expression, {
					alias: $expression[ALIAS]
				})],
				where: $filter
			};
		},
		map($expression, $mapper) {
			return {
				type: 'select',
				field: [typeof $mapper[TYPE] == 'string' ? Object.assign($mapper, { as: '' }) : $mapper],
				from: [Object.assign($expression, {
					alias: $expression[ALIAS]
				})]
			};
		},
		limit($expression, [$start, $length]) {
			var alias = `_${i++}`;
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: alias, identifier: '*' }],
				from: [Object.assign($expression, {
					alias: alias
				})],
				limit: $length,
				offset: $start
			};
		},
		order($expression, $orderer, $direction) {
			return {
				type: 'select',
				field: [{ type: 'name', qualifier: $expression[ALIAS], identifier: '*' }],
				from: [Object.assign($expression, {
					alias: $expression[ALIAS]
				})],
				order: $orderer,
				direction: $direction
			};
		},
		distinct($expression) {
			var alias = `_${i++}`;
			return {
				type: 'select',
				distinct: true,
				field: [{ type: 'name', qualifier: alias, identifier: '*' }],
				from: [Object.assign($expression, {
					alias: alias
				})]
			};
		},
		bind($value, scope, environment = 0) {
			var aliasValue = `_${i++}`;
			var $expression = scope.this || Object.values(scope.local)[0];
			return {
				type: 'select',
				with: {
					name: $expression[ALIAS],
					value: selectize($expression, $expression[TYPE])
				},
				field: [{ type: 'name', qualifier: aliasValue, identifier: '*' }],
				from: [Object.assign(tabulize($value, $value[TYPE]), {
					alias: aliasValue
				})]
			};
		},
		scope($expression, expression) {
			var alias = {};
			if ($expression.this) {
				var a = `_${i++}`;
				if (expression.type == 'property')
					alias.this = a;
				else
					alias.thisrow = a;
				$expression.this[ALIAS] = a;
			} else {
				var $name = Object.keys($expression.local)[0];
				var $value = $expression.local[$name];
				alias.local = { [$name]: `_${i++}` };
				$value[ALIAS] = alias.local[$name];
			}
			return { alias: alias };
		},
		compile(expression) {
			switch (expression.type) {
				case 'property':
					// If it's a property expression of this expression, execute a quick path.
					var thisResolution = resolveThis.call(this, expression.expression);
					if (thisResolution) {
						var [scope] = thisResolution;
						if (scope.this[expression.property].value) {
							return compile.call(
								global.push(
									Object.assign(
										new Scope({}, scope.this),
										{ alias: { this: scope.alias.this, thisrow: scope.alias.thisrow } }
									)
								),
								scope.this[expression.property].value
							);
						} else
							if (scope.alias.thisrow) {
								return t({
									type: 'name',
									qualifier: scope.alias.thisrow,
									identifier: expression.property,
									kind: 'scalar'
								}, scope.this[expression.property].type);
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
				case 'sql':
					return expression.sql;
			}
			function t($expression, type) {
				$expression[TYPE] = type;
				$expression[QL] = expression;
				return $expression;
			}
		}
	},
	constant: {
		false: 'boolean',
		true: 'boolean',
		length: new (require('ql/Type').Function)('string', 'number'),
		substr: new (require('ql/Type').Function)(new (require('ql/Type').Tuple)(['string', 'number']), 'string'),
		sum: new (require('ql/Type').Function)(['number'], 'number'),
		avg: new (require('ql/Type').Function)(['number'], 'number'),
		min: new (require('ql/Type').Function)(['number'], 'number'),
		max: new (require('ql/Type').Function)(['number'], 'number')
	}
};
function truthy(sql, type) {
	if (typeof type == 'object')
		sql = {
			type: 'operation',
			operator: 'exists',
			right: sql
		};
	else if (type == 'string')
		sql = {
			type: 'operation',
			operator: '!=',
			left: sql,
			right: { type: 'literal', value: '' }
		};
	return sql;
}
function tablename(type) {
	var _type = global.scope.type;
	for (var name in _type)
		if (_type[name] == type)
			return global.scope.table ? global.scope.table(name) : name;
}
var runtime = {
	constant: {
		false: { type: 'name', identifier: 'false', kind: 'scalar' },
		true: { type: 'name', identifier: 'true', kind: 'scalar' },
		length: qlsql => argument => ({ type: 'call', callee: { type: 'name', identifier: 'length' }, argument: [qlsql(argument)] }),
		substr: qlsql => argument => ({ type: 'call', callee: { type: 'name', identifier: 'substr' }, argument: [qlsql(argument.element[0]), { type: 'operation', operator: '+', left: qlsql(argument.element[1]), right: { type: 'literal', value: 1 } }] }),
		sum: new String('sum'),
		avg: new String('avg'),
		min: new String('min'),
		max: new String('max')
	}
};
var operator = {
	'&': '&&',
	'|': '||'
};

module.exports = interpretation;
