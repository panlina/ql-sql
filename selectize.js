var selectize = require('sql').selectize;
module.exports = (sql, type) => {
	sql = selectize(sql);
	if (typeof type == 'string')
		sql.field[0].as = '';
	return sql;
};
