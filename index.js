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
	if (sql.type == 'name')
		sql = {
			type: 'select',
			field: [{ type: 'name', identifier: '*' }],
			from: sql
		};
	else if (sql.type == 'literal')
		sql = {
			type: 'select',
			field: [sql]
		};
	return sql;
}

module.exports = qlsql;
