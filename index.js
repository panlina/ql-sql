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
