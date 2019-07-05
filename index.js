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
	}
	return sql;
}

module.exports = function (ql) {
	var sql = qlsql(ql);
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
};
