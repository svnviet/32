from flask import Blueprint, request as flask_request, jsonify

from cartmigration.libs.utils import *

route_path = Blueprint('route_path', __name__)

@route_path.route("/action/<string:method>", methods = ['post'])
def action(method):
	request_data = flask_request.data
	if isinstance(request_data, bytes):
		request_data = request_data.decode()

	request_data = json_decode(request_data)
	buffer = dict()
	if not buffer.get('controller'):
		buffer['controller'] = 'migration'
	buffer['action'] = to_str(method).replace('-', '_')
	buffer['data'] = request_data
	res = start_subprocess(None, buffer, True)
	if isinstance(res, dict) and 'next' in res:
		migration_id = res['next'].get('migration_id')
		start_subprocess(migration_id, res['next'])
		del (res['next'])
	return jsonify(res)

