from flask import Blueprint, render_template, session, abort, request as flask_request, jsonify

from cartmigration.libs.utils import *

autotest_path = Blueprint('autotest_path', __name__)

@autotest_path.route("autotest", methods = ['post'])
def autotest():
	buffer = flask_request.data
	if isinstance(buffer, bytes):
		buffer = buffer.decode()
	buffer = json_decode(buffer)
	if not buffer.get('auto_test_id'):
		return jsonify(response_error('Data invalid'))
	start_autotest(buffer['auto_test_id'])
	return jsonify(response_success())
