function qlsql(ql) {
	var sql;
	switch (ql.type) {
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
	return sql;
}

module.exports = qlsql;
