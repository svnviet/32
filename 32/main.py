from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

from app.route.route import route_path
from app.route.migration import migration_path
from app.route.autotest import autotest_path
from app.route.file import file_path
from app.middleware.auth import Auth
from cartmigration.libs.utils import *
mode = get_config_ini('local', 'mode')
if mode == 'live':
	api_url = get_config_ini('server', 'api_url')
	if not api_url:
		print("Please add api_url in file cartmigration/etc/config.ini under section server")
		sys.exit()

app = Flask(__name__)
app.wsgi_app = Auth(app.wsgi_app)
app.debug = to_bool(get_config_ini('local', 'debug', False))
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.register_blueprint(route_path, url_prefix = '/api/v1')
app.register_blueprint(migration_path, url_prefix = '/api/v1')
app.register_blueprint(autotest_path, url_prefix = '/api/v1')
app.register_blueprint(file_path, url_prefix = '/api/v1/file')

@app.route("/hello", methods = ['post', 'get'])
@app.route("/hello/<string:name>/", methods = ['post', 'get'])
def hello(name = None):
	return 'hello' + (name if name else '')

if not to_bool(get_config_ini('local', 'debug')):
	@app.errorhandler(500)
	def internal_server_error(error):
		app_log = get_config_ini('local', 'app_log')
		if not app_log:
			app_log = APP_LOG_DAILY
		if app_log == APP_LOG_SINGLE:
			file_log = 'exceptions.log'
		elif app_log == APP_LOG_DAILY:
			file_log = 'exceptions_{}.log'.format(get_current_time("%Y-%m-%d"))
		else:
			file_log = get_config_ini('local', 'log_file', 'exceptions.log')
		file_log = get_pub_path() + '/log/flask/' + file_log
		folder_log = os.path.dirname(file_log)
		if not os.path.isdir(folder_log):
			os.makedirs(folder_log)
			change_permissions_recursive(folder_log, 0x777)
		msg = '{}: \nPath: {}\nMethod: {}\nData: {}\nResponse status: 500\nError: {}'
		ts = time.strftime('%Y/%m/%d %H:%M:%S')
		data = request.data.decode()
		if data and (isinstance(data, list)) or isinstance(data, dict):
			data = json_encode(data)
		msg = msg.format(ts, request.full_path,request.method,data,traceback.format_exc())
		check_exist = False
		if os.path.isfile(file_log):
			check_exist = True
		with open(file_log, 'a') as log_file:
			log_file.write(msg)
		if not check_exist and os.path.isfile(file_log):
			os.chmod(file_log, 0o777)
		return error.args[0], error.code
else:
	@app.errorhandler(Exception)
	def all_exception_error(error):
		app_log = get_config_ini('local', 'app_log')
		if not app_log:
			app_log = APP_LOG_DAILY
		if app_log == APP_LOG_SINGLE:
			file_log = 'exceptions.log'
		elif app_log == APP_LOG_DAILY:
			file_log = 'exceptions_{}.log'.format(get_current_time("%Y-%m-%d"))
		else:
			file_log = get_config_ini('local', 'log_file', 'exceptions.log')
		file_log = get_pub_path() + '/log/flask/' + file_log
		folder_log = os.path.dirname(file_log)
		if not os.path.isdir(folder_log):
			os.makedirs(folder_log)
			change_permissions_recursive(folder_log, 0x777)
		msg = '{}: \nPath: {}\nMethod: {}\nData: {}\nResponse status: {}\nError: {}'
		ts = time.strftime('%Y/%m/%d %H:%M:%S')
		data = request.data.de
		if data and (isinstance(data, list)) or isinstance(data, dict):
			data = json_encode(data)
		response_status = 500
		if hasattr(error, 'code'):
			response_status = error.code
		msg = msg.format(ts, request.full_path, request.method, data, response_status, traceback.format_exc())
		check_exist = False
		if os.path.isfile(file_log):
			check_exist = True
		with open(file_log, 'a') as log_file:
			log_file.write(msg)
		if not check_exist and os.path.isfile(file_log):
			os.chmod(file_log, 0o777)
		msg_error = error.args[0] if len(error.args) > 0 else (error.description if hasattr(error, 'description') else '')
		code = error.code if hasattr(error, 'code') else 500
		return msg_error, code
if __name__ == '__main__':
	port = to_int(get_config_ini('local', 'port'))
	app.run(host = '0.0.0.0', port = port)
