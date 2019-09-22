function qlsql(ql) {
	var sql = qlsql(ql);
	return selectize(sql);
	function qlsql(ql) {
		var sql;
		switch (ql.type) {
			case 'literal':
				sql = {
					type: 'literal',
					value: ql.value
				};
				break;
			case 'name':
				sql = {
					type: 'name',
					identifier: ql.identifier
				};
				break;
			case 'property':
				sql = {
					type: 'select',
					field: [{ type: 'name', identifier: ql.property }],
					from: qlsql(ql.expression)
				};
				break;
			case 'index':
				sql = {
					type: 'select',
					field: [{ type: 'name', identifier: '*' }],
					from: qlsql(ql.expression),
					where: {
						type: 'binary',
						operator: '=',
						left: { type: 'name', identifier: 'id' },
						right: qlsql(ql.index)
					}
				};
				break;
			case 'unary':
				sql = {
					type: 'unary',
					operator: ql.operator,
					operand: qlsql(ql.operand)
				};
				break;
			case 'binary':
				sql = {
					type: 'binary',
					operator: ql.operator,
					left: qlsql(ql.left),
					right: qlsql(ql.right)
				};
				break;
			case 'filter':
				sql = {
					type: 'select',
					field: [{ type: 'name', identifier: '*' }],
					from: qlsql(ql.expression),
					where: qlsql(ql.filter)
				};
				break;
			case 'comma':
				sql = {
					type: 'select',
					with: {
						name: ql.head.name,
						value: selectize(qlsql(ql.head.value))
					},
					field: [{ type: 'name', identifier: '*' }],
					from: qlsql(ql.body)
				};
				break;
		}
		return sql;
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
