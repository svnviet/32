from flask import Blueprint, request as flask_request, jsonify

from cartmigration.libs.base_controller import BaseController
from cartmigration.libs.utils import *

migration_path = Blueprint('migration_path', __name__)

@migration_path.route("start/<int:migration_id>", methods = ['post'])
def start(migration_id):
	request_data = flask_request.data
	if isinstance(request_data, bytes):
		request_data = request_data.decode()
	request_data = json_decode(request_data)
	if not request_data:
		request_data = {'migration_id': migration_id}
	buffer = dict()
	if not buffer.get('controller'):
		buffer['controller'] = 'migration'
	buffer['action'] = 'start'
	buffer['data'] = request_data
	prepare_subprocess(request_data)
	start_subprocess(migration_id, buffer)
	return jsonify(response_success())

@migration_path.route("recent/<int:migration_id>", methods = ['post'])
def recent(migration_id):
	request_data = flask_request.data
	if isinstance(request_data, bytes):
		request_data = request_data.decode()
	request_data = json_decode(request_data)
	if not request_data:
		request_data = {'migration_id': migration_id}
	buffer = dict()
	if not buffer.get('controller'):
		buffer['controller'] = 'migration'
	buffer['action'] = 'recent'
	buffer['data'] = request_data
	recent_data = start_subprocess(migration_id, buffer, True)
	if recent_data:
		return jsonify(response_success())
	return jsonify(response_error())

@migration_path.route("remigrate/<int:migration_id>", methods = ['post'])
def remigrate(migration_id):
	request_data = flask_request.data
	if isinstance(request_data, bytes):
		request_data = request_data.decode()
	request_data = json_decode(request_data)
	if not request_data:
		request_data = {'migration_id': migration_id}
	buffer = dict()
	if not buffer.get('controller'):
		buffer['controller'] = 'migration'
	buffer['action'] = 'remigrate'
	buffer['data'] = request_data
	recent_data = start_subprocess(migration_id, buffer, True)
	if recent_data:
		return jsonify(response_success())
	return jsonify(response_error())
def kill_previous_subprocess(notice):
	controller = BaseController(notice)
	info_migration_id = controller.get_info_migration_id(notice['migration_id'])
	if info_migration_id:
		pid = info_migration_id['pid']
		if pid and check_pid(pid):
			subprocess.call(['kill', '-9', str(pid)])

def prepare_subprocess(notice):
	kill_previous_subprocess(notice)
