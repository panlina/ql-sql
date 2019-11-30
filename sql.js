exports.with = function (head, body) {
	if (body.with)
		return {
			type: 'select',
			with: head,
			field: [{ type: 'name', identifier: '*' }],
			from: body
		};
	body.with = head;
	return body;
};
exports.count = function (sql) {
	if (sql.type == 'name')
		return {
			type: 'select',
			field: [{ type: 'name', identifier: 'count(*)' }],
			from: sql
		};
	sql.field = [{ type: 'name', identifier: 'count(*)' }];
	return sql;
};
exports.project = function (sql, field) {
	if (sql.type == 'name')
		return {
			type: 'select',
			field: [{ type: 'name', identifier: field }],
			from: sql
		};
	sql.field = [{ type: 'name', identifier: field }];
	return sql;
};
