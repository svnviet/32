import base64
import configparser
import hashlib
import hmac
import importlib
import json
from packaging import version
import re
import shutil
import time
import subprocess
import socket, struct
import traceback
from datetime import datetime

import sys
from phpserialize import *
from pathlib import Path
import urllib.parse
import os

# from libs.base_thread import BaseThread

TABLE_MIGRATION = "migration_process"
TABLE_FLAG_STOP = "migration_flag_stop"
TABLE_MAP = "migration_map"
TABLE_RECENT = "migration_recent"
TABLE_SETTING = "settings"
TABLE_MIGRATION_TEST = 'migration_test'
TABLE_MIGRATION_HISTORY = 'migration_history'
TABLE_AUTO_TEST = 'auto_test'
TABLE_DEMO_ERROR = 'migration_demo_errors'
TABLE_REGION = 'directory_country_region'
MIGRATION_FULL = 2
MIGRATION_DEMO = 1
GROUP_USER = 1
GROUP_TEST = 2
STATUS_NEW = 1
STATUS_RUN = 2
STATUS_STOP = 3
STATUS_COMPLETED = 4
STATUS_KILL = 5
STATUS_CONFIGURING = 6
STATUS_PAYMENT = 7
DIR_UPLOAD = 'uploads'
BASE_DIR = 'cartmigration'
CONFIG_FILE = 'cartmigration/etc/config.ini'
DIR_PROCESS = 'processes/'
FLAG_STOP = 1
FLAG_KILL_ALL = 2
APP_LOG_SINGLE = 'single'
APP_LOG_DAILY = 'daily'
APP_LOG_CUSTOM = 'custom'

def get_value_by_key_in_dict(dictionary, key, default = None):
	if not dictionary or not isinstance(dictionary, dict):
		return default
	if key in dictionary:
		return dictionary[key] if dictionary[key] else default
	return default

def check_pid(pid):
	pid = int(pid)
	""" Check For the existence of a unix pid. """
	try:
		os.kill(pid, 0)
	except OSError:
		return False
	else:
		return True

def get_controller(controller_name, data = None):
	# if controller_name == 'base':
	# 	if data:
	# 		my_instance = BaseThread(data)
	# 	else:
	# 		my_instance = BaseThread()
	# 	return my_instance
	module_class = importlib.import_module(BASE_DIR + '.controllers.' + controller_name)
	my_class = getattr(module_class, controller_name.capitalize())
	# if data:
	my_instance = my_class(data)
	# else:
	# 	my_instance = my_class()
	return my_instance

def get_model(name, data = None, class_name = None):
	if not name:
		return None
	# name_path = name.replace('_', '/')
	file_path = get_root_path() + '/' + BASE_DIR + '/' + 'models/' + name + '.py'
	file_model = Path(file_path)
	if not file_model.is_file():
		return None
	name_path = name.split('_')
	model_name = BASE_DIR + ".models." + name.replace('/', '.')
	module_class = importlib.import_module(model_name)
	class_name = class_name if class_name else get_model_class_name(name)

	try:
		model_class = getattr(module_class, class_name)
		if data:
			model = model_class(data)
		else:
			model = model_class()
		return model
	except Exception as e:
		log(e)
		return None

def get_model_class_name(name, char = '/'):
	name = name.replace(BASE_DIR, '')
	split = re.split(r'[^a-z0-9]', name)
	upper = list(map(lambda x: x.capitalize(), split))
	new_name = 'Le' + ''.join(upper)
	return new_name

def md5(s, raw_output = False):
	res = hashlib.md5(s.encode())
	if raw_output:
		return res.digest()
	return res.hexdigest()

def hash_hmac(algo, data, key):
	res = hmac.new(key.encode(), data.encode(), algo).hexdigest()
	return to_str(res)

def to_str(value):
	if isinstance(value, bool):
		return str(value)
	if (isinstance(value, int) or isinstance(value, float)) and value == 0:
		return '0'
	if not value:
		return ''
	if isinstance(value, dict) or isinstance(value, list):
		return json_encode(value)
	try:
		value = str(value)
		return value
	except Exception:
		return ''
def change_permissions_recursive(path, mode):
	os.chmod(path, mode)
	for root, dirs, files in os.walk(path):
		for sub_dir in dirs:
			os.chmod(os.path.join(root, sub_dir), mode)
		for sub_file in files:
			os.chmod(os.path.join(root, sub_file), mode)

def to_timestamp(value, str_format = '%Y-%m-%d %H:%M:%S'):
	try:
		timestamp = int(time.mktime(time.strptime(value, str_format)))
		if timestamp:
			return timestamp
		return int(time.time())
	except:
		return int(time.time())

def to_int(value):
	if not value:
		return 0
	try:
		value = int(float(value))
		return value
	except Exception:
		return 0

def to_bool(value):
	if isinstance(value, str):
		if value.lower().strip() == 'false':
			return False
	if value:
		return True
	return False

def to_decimal(value, length = None):
	if not value:
		return 0.00
	try:
		value = round(float(value), length) if length else float(value)
		return value
	except Exception:
		return 0.00

def to_len(value):
	if not value:
		return 0
	try:
		res = len(value)
	except Exception:
		res = 0
	return res

def convert_format_time(time_data, old_format = '%Y-%m-%d %H:%M:%S', new_format = '%Y-%m-%d %H:%M:%S'):
	if to_int(re.sub('[^0-9]', '', to_str(time_data))) == 0:
		return None
	is_timestamp = to_decimal(time_data)
	try:
		if is_timestamp != 0.0:
			timestamp = datetime.fromtimestamp(is_timestamp)
			res = timestamp.strftime(new_format)
			return res
		if not old_format:
			old_format = '%Y-%m-%d %H:%M:%S'
		new_time = datetime.strptime(time_data, old_format)
		res = new_time.strftime(new_format)
		return res

	except Exception:
		return get_current_time(new_format)

def print_time(thread_name):
	time.sleep(10)
	print("%s: %s" % (thread_name, time.ctime(time.time())))

def gmdate(str_format, int_time_stamp = None):
	if not int_time_stamp:
		return time.strftime(str_format, time.gmtime())
	else:
		return time.strftime(str_format, time.gmtime(int_time_stamp))

def log(msg, migration_id = None, type_error = 'exceptions'):
	path = get_pub_path() + '/log/'
	if migration_id:
		migration_id = str(migration_id)
		path = path + migration_id
	if not os.path.exists(path):
		os.makedirs(path)
		os.chmod(path, 0o777)
	path_log = path + '/' + type_error + '.log'
	if os.path.exists(path_log) and os.path.getsize(path_log) >= 10485760:
		os.remove(path_log)

	if isinstance(msg, dict):
		msg = json.dumps(msg)
	msg = str(msg) + '\r\n'
	date_time = time.strftime('%Y/%m/%d %H:%M:%S')
	msg = date_time + ' : ' + msg
	file_name = path + '/' + type_error + '.log'
	check_exist = False
	if os.path.isfile(file_name):
		check_exist = True
	with open(file_name, 'a') as log_file:
		log_file.write(msg)
	if not check_exist and os.path.isfile(file_name):
		os.chmod(file_name, 0o777)

def clear_log(migration_id):
	if not migration_id:
		return response_success()
	path = get_pub_path() + '/log/' + str(migration_id)
	if os.path.isdir(path):
		shutil.rmtree(path)
	return response_success()

def log_traceback(migration_id = None, type_error = 'exceptions'):
	error = traceback.format_exc()
	log(error, migration_id, type_error)

def get_current_time(str_format = '%Y-%m-%d %H:%M:%S'):
	try:
		current_time = time.strftime(str_format)
	except Exception:
		current_time = time.strftime('%Y-%m-%d %H:%M:%S')
	return current_time

def ip2long(ip):
	"""
	Convert an IP string to long
	"""
	try:
		packedIP = socket.inet_aton(ip)
		res = struct.unpack("!L", packedIP)[0]
	except Exception:
		res = ''
	return res

# response
def create_response(result = '', msg = '', data = None):
	return {'result': result, 'msg': msg, 'data': data}

def response_error(msg = '', elm_error = '', title = ''):
	return {'result': 'error', 'msg': msg, 'data': None, 'elm_error': elm_error, 'title': title}

def response_api(msg = '', elm_error = '', title = ''):
	return {'result': 'api', 'msg': msg, 'data': None, 'elm_error': elm_error, 'title': title}


def response_success(data = None, msg = ''):
	return {
		'result': 'success', 'msg': msg, 'data': data
	}

def response_warning(msg = None):
	return {'result': 'warning', 'msg': msg, 'data': None}

def error_database():
	return response_error("Database isn't working.")

# base64
def string_to_base64(s):
	if not isinstance(s, str):
		s = str(s)
	return base64.b64encode(s.encode('utf-8')).decode('utf-8')

def base64_to_string(b):
	try:
		s = base64.b64decode(b).decode('utf-8')
		return s
	except Exception as e:
		try:
			s = base64.b64decode(b.encode('utf-8')).decode('utf-8')
			return s
		except Exception as e:
			# log_traceback()
			return None

def php_serialize(obj):
	try:
		res = serialize(obj).decode('utf-8')
	except Exception as e:
		res = False
	return res

def php_unserialize(str_serialize):
	try:
		res = unserialize(str_serialize.encode('utf-8'))
	except Exception:
		try:
			res = unserialize(str_serialize)
		except Exception:
			res = False
	res = decode_after_unserialize(res)
	if isinstance(res, dict):
		keys = list(res.keys())
		keys = list(map(lambda x: to_str(x), keys))
		keys.sort()
		for index, key in enumerate(keys):
			if to_str(index) != to_str(key):
				return res
		res = list(res.values())
	return res

def decode_after_unserialize(data):
	res = None
	if isinstance(data, dict):
		res = dict()
		for k, v in data.items():
			try:
				key = k.decode('utf-8')
			except Exception:
				key = k
			if isinstance(v, dict):
				value = decode_after_unserialize(v)
			else:
				try:
					value = v.decode('utf-8')
				except Exception:
					value = v
			res[key] = value
	elif isinstance(data, list):
		res = list()
		for row in data:
			value = decode_after_unserialize(row)
			res.append(value)
	else:
		try:
			res = data.decode('utf-8')
		except Exception:
			res = data
	return res

# Get one array from list array by field value
def get_row_from_list_by_field(data, field, value):
	result = dict()
	if not data or not field:
		return result
	for row in data:
		if (field in row) and str(row[field]) == str(value):
			result = row
			break
	return result

# Get array value from list array by field value and key of field need
def get_row_value_from_list_by_field(data, field, value, need):
	if not data:
		return False
	row = get_row_from_list_by_field(data, field, value)
	if not row:
		return False
	row = dict(row)
	return row.get(need, False)

# Get and unique array value by key
def duplicate_field_value_from_list(data, field):
	result = list()
	if not data:
		return result
	data = list(data)
	for item in data:
		if to_str(field) in item:
			result.append(item[field])
	result = list(set(result))
	return result

# Get list array from list by list field value
def get_list_from_list_by_list_field(data, field, values):
	if not data or not field:
		return list()
	if not isinstance(data, list):
		values = [values]
	values = list(map(lambda x: to_str(x), values))
	result = list()
	try:
		for row in data:
			if to_str(row[field]) in values:
				result.append(row)
	except Exception:
		return list()
	return result

# Get list array from list by field  value
def get_list_from_list_by_field(data, field, value):
	if not data:
		return list()
	result = list()
	try:
		for row in data:
			if isinstance(value, list):
				for item in value:
					if to_str(row[field]) == to_str(item):
						result.append(row)
			else:
				if to_str(row[field]) == to_str(value):
					result.append(row)
	except Exception:
		return list()
	return result

# url
def strip_domain_from_url(url):
	parse = urllib.parse.urlparse(url)
	path_url = parse.path
	query = parse.query
	fragment = parse.fragment
	if query:
		path_url += '?' + query
	if fragment:
		path_url += '#' + fragment
	return path_url

def join_url_path(url, path_url):
	full_url = url.rstrip('/')
	if path_url:
		full_url += '/' + path_url.lstrip('/')
	return full_url

def send_data_socket(data, conn):
	if isinstance(data, list) or isinstance(data, dict):
		data = json_encode(data)
	data = str(data).encode('utf-8')
	conn.send(data)
	conn.close()

def get_root_path():
	path = os.path.dirname(os.path.abspath(__file__))
	path = path.replace('/cartmigration/libs', '')
	# path = path.replace('/processes', '')
	return path

def get_pub_path():
	path = get_root_path()
	if 'pub' in path:
		index = path.find('pub')
		path = path[0:index]
	path = path.rstrip('/') + '/pub'
	return path

def console_success(msg):
	result = '<p class="success"> - ' + msg + '</p>'
	return result

def console_error(msg):
	result = '<p class="error"> - ' + msg + '</p>'
	return result

def console_warning(msg):
	result = '<p class="warning"> - ' + msg + '</p>'
	return result

# json
def json_decode(data):
	try:
		data = json.loads(data)
	except Exception:
		try:
			data = json.loads(data.decode('utf-8'))
		except Exception:
			data = False
	return data if isinstance(data, (list,dict)) else False

def json_encode(data):
	try:
		data = json.dumps(data)
	except Exception:
		data = False
	return data

def clone_code_for_migration_id(migration_id):
	if check_folder_clone(migration_id):
		return True
	base_dir = get_pub_path() + '/' + DIR_PROCESS + to_str(migration_id)
	if not os.path.isdir(base_dir):
		os.makedirs(base_dir)
	folder_copy = ['controllers', 'libs', 'models']
	file_copy = ['bootstrap.py']
	for folder in folder_copy:
		if os.path.isdir(base_dir + '/' + BASE_DIR + '/' + folder):
			continue
		shutil.copytree(BASE_DIR + '/' + folder, base_dir + '/' + BASE_DIR + '/' + folder)
	for file in file_copy:
		if os.path.isfile(base_dir + '/' + file):
			continue
		shutil.copy(file, base_dir + '/' + file)

	git_ignore_file = base_dir + '/' + '.gitignore'
	f = open(git_ignore_file, "w+")
	f.write('*')
	change_permissions_recursive(base_dir, 0o777)

def start_subprocess(migration_id = None, buffer = None, wait = False):
	list_special = ['reset_migration', 'clone_migration', 'stop_auto_test', 'restart_migration', 'kill_end_loop_migration', 'kill_migration', 'delete_migration']
	action = buffer.get('action')
	if not migration_id and isinstance(buffer, dict) and get_value_by_key_in_dict(buffer, 'data', dict()).get('migration_id'):
		migration_id = buffer['data']['migration_id']
	if action not in list_special and migration_id and check_folder_clone(migration_id):
		path = get_pub_path() + '/' + DIR_PROCESS + str(migration_id)
		if to_decimal(os.path.getctime(path)) < to_decimal(get_config_ini('local', 'time_clone', 1589795205)):
			old_path = path + '_v30'
			os.rename(path, old_path)
			clone_code_for_migration_id(migration_id)
			folder_clear = '/cartmigration/models/cart'
			shutil.rmtree(path + folder_clear)
			shutil.copytree(old_path + folder_clear, path + folder_clear)

	else:
		path = get_root_path()
	if wait:
		proc = subprocess.Popen(['python3', path + '/bootstrap.py', json_encode(buffer)], stdout = subprocess.PIPE)
		data = ''
		while True:
			line = proc.stdout.readline().decode('utf8')
			if line != '':
				data += line
			else:
				break
		decode_data = json_decode(data)
		return decode_data if decode_data else data
	else:
		subprocess.Popen(['python3', path + '/bootstrap.py', json_encode(buffer)])

def start_autotest(auto_test_id):
	dir_test = 'test/' + str(auto_test_id)
	if auto_test_id and check_folder_clone(dir_test):
		path = get_pub_path() + '/' + DIR_PROCESS + dir_test
	else:
		path = get_root_path()
	buffer = {
		'auto_test_id': auto_test_id
	}
	subprocess.Popen(['python3', path + '/autotest.py', json_encode(buffer)])

def check_folder_clone(migration_id):
	path = get_pub_path()
	if not isinstance(migration_id, str):
		migration_id = str(migration_id)
	base_dir = path + '/' + DIR_PROCESS + str(migration_id)
	if not os.path.isdir(base_dir):
		return False
	folder_check = ['controllers', 'libs', 'models']
	file_check = ['bootstrap.py']
	for folder in folder_check:
		if not os.path.isdir(base_dir + '/' + BASE_DIR + '/' + folder):
			return False
	for file in file_check:
		if not os.path.isfile(base_dir + '/' + file):
			return False
	return True

def clear_folder_clone(migration_id):
	path = get_pub_path()
	if not isinstance(migration_id, str):
		migration_id = str(migration_id)
	base_dir = path + '/' + DIR_PROCESS + str(migration_id)
	if not os.path.isdir(base_dir):
		return True
	shutil.rmtree(base_dir)
	return True

def response_from_subprocess(data, conn = True):
	if conn:
		if isinstance(data, list) or isinstance(data, dict):
			data = json_encode(data)
		print(data, end = '')
		sys.exit(1)
	return data

def get_config_ini(section, key = None, default = None, migration_id = None, file = 'config.ini'):
	config_root_file = get_pub_path() + '/../cartmigration/etc/' + file
	config_file = config_root_file
	config_processes_file = get_root_path() + '/cartmigration/etc/' + file
	if to_str(migration_id) and to_str(migration_id) in config_processes_file:
		if os.path.isfile(config_processes_file):
			config_file = config_processes_file
	if os.path.isfile(config_file):
		config = configparser.ConfigParser()
		config.read(config_file)
		try:
			if not key:
				return config[section]
			value = config[section][key]
			return value
		except Exception as e:
			return default
	return default

def parse_version(str_version):
	return version.parse(str_version)