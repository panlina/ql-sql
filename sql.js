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
