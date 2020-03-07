var tabulize = require('sql').tabulize;
module.exports = (sql, type) => {
	sql = tabulize(sql);
	if (typeof type == 'string')
		sql.field[0].as = '';
	return sql;
};
