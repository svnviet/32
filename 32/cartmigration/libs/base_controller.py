import math
from datetime import datetime
import psutil
from cartmigration.libs.utils import *
import sendgrid
from sendgrid.helpers.mail import *

class BaseController:
	NEW = 1
	RUN = 2
	STOP = 3
	FINISH = 4
	DEV_MODE = True
	LIMIT_LINE_ERROR = 200
	ACTION_STOP = 1
	ACTION_COMPLETED = 2
	ACTION_APP_MODE = 3
	ACTION_DEMO_ERROR = 4

	def __init__(self,  data = None):
		self._migration_id = data.get('migration_id') if isinstance(data, dict) else None
		self.data = data
		self.pid = None
		self._notice = None
		self.router = None
		self.source_cart = None
		self.target_cart = None
		self.test = data.get('test') if isinstance(data, dict) else False

	def set_migration_id(self, _migration_id):
		self._migration_id = _migration_id

	def get_migration_id(self):
		return self._migration_id

	def set_notice(self, notice):
		self._notice = notice

	def get_notice(self):
		return self._notice

	def init_cart(self, new = False):
		if self._notice and self.router:
			return self
		self.router = get_model('basecart')
		getattr(self.router, 'set_is_test')(self.test)
		if not self._migration_id or new:
			if self._migration_id:
				getattr(self.router, 'set_migration_id')(self._migration_id)
			self._notice = getattr(self.router, 'get_default_notice')()
		else:
			getattr(self.router, 'set_migration_id')(self._migration_id)
			if not self._notice:
				self._notice = getattr(self.router, 'init_notice')()
		getattr(self.router, 'set_notice')(self._notice)
		# self.source_cart = self.get_source_cart()
		# self.target_cart = self.get_target_cart()
		return self

	def delete_notice(self):
		# router = get_model('migration')
		delete = getattr(self.get_router(), 'delete_migration_notice')(self._migration_id)
		if delete:
			self._notice = None
		return delete

	def update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False):
		# router = get_model('migration')
		return getattr(self.get_router(), 'update_notice')(_migration_id, notice, pid , mode, status, finish)

	def get_router(self):
		if self.router:
			return self.router
		self.init_cart()
		return self.router

	def reset_cart(self):
		self.source_cart = None
		self.target_cart = None
		self.get_source_cart()
		self.get_target_cart()

	def get_source_cart(self):

		if self.source_cart:
			return self.source_cart

		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		special_type = source_cart_type == target_cart_type
		cart_version = self._notice['src']['config']['version']
		cart_name = getattr(self.router, 'get_cart')(source_cart_type, cart_version, special_type)
		self.source_cart = get_model(cart_name)
		if not self.source_cart:
			return None
		getattr(self.source_cart, 'set_migration_id')(self._migration_id)
		getattr(self.source_cart, 'set_type')('src')
		getattr(self.source_cart, 'set_notice')(self._notice)
		getattr(self.source_cart, 'set_db')(getattr(self.router, 'get_db')())
		getattr(self.source_cart, 'set_is_test')(self.test)

		return self.source_cart

	def get_target_cart(self):

		# cart_custom_name = getattr(basecart, 'get_target_custom_cart')(self._migration_id)
		# target_cart = get_model(cart_custom_name)
		if self.target_cart:
			return self.target_cart
		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		special_type = source_cart_type == target_cart_type
		cart_version = self._notice['target']['config']['version']
		cart_name = getattr(self.get_router(), 'get_cart')(target_cart_type, cart_version, special_type)
		self.target_cart = get_model(cart_name)
		if not self.target_cart:
			return None
		getattr(self.target_cart, 'set_type')('target')
		getattr(self.target_cart, 'set_migration_id')(self._migration_id)
		getattr(self.target_cart, 'set_notice')(self._notice)
		getattr(self.target_cart, 'set_db')(getattr(self.router, 'get_db')())
		getattr(self.target_cart, 'set_is_test')(self.test)

		return self.target_cart

	def get_target_cart_name(self):
		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		check = False
		if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
			check = True
		cart_version = self._notice['target']['config']['version']
		cart_name = getattr(self.get_router(), 'get_cart')(target_cart_type, cart_version, check)
		return cart_name

	def get_source_cart_name(self):
		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		check = False
		if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
			check = True
		cart_version = self._notice['src']['config']['version']
		cart_name = getattr(self.get_router(), 'get_cart')(source_cart_type, cart_version, check)
		return cart_name

	def save_notice(self, status = None, sv_pid = True, pid = None, clear_entity_warning = False):
		notice = self._notice
		demo = None
		# if 'demo' in notice and notice['demo']:
		# 	demo = 2
		if sv_pid:
			process_id = pid if pid else self.pid
		else:
			process_id = None
		res = getattr(self.get_router(), 'save_user_notice')(self._migration_id, notice, process_id, demo, status, clear_entity_warning = clear_entity_warning)
		return res

	def save_migration(self, after_kill = False, kill_all = False, extend_data = dict):
		notice = self._notice
		data = {
			'notice': notice,
			'migration_id': self._migration_id
		}
		if kill_all:
			data['status'] = STATUS_KILL
			data['pid'] = None
		if after_kill:
			data['status'] = STATUS_STOP
			data['pid'] = None
		if extend_data and isinstance(extend_data, dict):
			for extend_key, extend_value in extend_data.items():
				if extend_key not in data:
					data[extend_key] = extend_value
		res = getattr(self.get_router(), 'save_migration')(self._migration_id, data)
		return res

	def clear_stop_flag(self):
		self.init_cart()
		return getattr(self.router, 'clear_stop_flag')(self._migration_id)

	def get_user_notice(self):
		getattr(self.get_router(), 'set_migration_id')(self._migration_id)
		notice = getattr(self.get_router(), 'get_migration_notice')(self._migration_id)
		return notice

	def save_recent(self):
		return getattr(self.get_router(), 'save_recent')(self._migration_id, self._notice)

	def default_result_migration(self):
		return {
			'result': '',
			'msg': '',
			'process': {
				'next': '',
				'total': 0,
				'imported': 0,
				'error': 0,
				'point': 0,
			}
		}

	def get_process_migration(self, notice, con):

		notice = getattr(self.get_router(), 'get_migration_notice')(notice['setting']['migration_id'])
		send_data_socket(notice, con)


	def get_info_migration_id(self, user_migration_id):
		# cart = get_model('basecart')
		return getattr(self.get_router(), 'get_info_migration')(user_migration_id)

	def check_migration_id(self, user_migration_id):
		cart = get_model('basecart')
		check_migration_id = getattr(self.get_router(), 'check_migration_id')(user_migration_id)
		if not check_migration_id:
			return response_error()
		return check_migration_id

	def log(self, msg, type_log = 'exceptions'):
		log(msg, self._migration_id, type_log)
		if type_log not in ['process', 'time_requests', 'time_images']:
			path = BASE_DIR + '/log'
			if self._migration_id:
				migration_id = to_str(self._migration_id)
				path = DIR_PROCESS + migration_id + '/' + path
			if os.path.isfile(path+'/exceptions_top.log'):
				os.remove(path+'/exceptions_top.log')
			log(msg, self._migration_id, 'exceptions_top')

	def log_traceback(self, type_error = 'exceptions', entity_id = None):
		error = traceback.format_exc()
		if entity_id:
			error = type_error + ' ' + to_str(entity_id) + ': ' + error
		self.log(error, type_error)

	def setup_source_cart(self, cart_type = None):
		# cart = get_model('basecart')
		if not cart_type:
			cart_type = self.get_first_source_cart_type()
		setup_type = getattr(self.get_router(), 'source_cart_setup')(cart_type)
		view_path = 'templates.migration.source.' + setup_type
		support_info = 'templates.migration.source.support.info'
		return {
			'setup_type': setup_type,
			'cart_type': cart_type,
			'view_path': view_path,
			'info': support_info,
		}

	def setup_target_cart(self, cart_type = None):
		# cart = get_model('basecart')
		if not cart_type:
			cart_type = self.get_first_target_cart_type()
		setup_type = getattr(self.get_router(), 'target_cart_setup')(cart_type)
		view_path = 'templates.migration.target.' + setup_type
		support_info = 'templates.migration.target.support.info'
		return {
			'setup_type': setup_type,
			'cart_type': cart_type,
			'view_path': view_path,
			'info': support_info,
		}

	def get_first_source_cart_type(self):
		source_cart_type = get_model('type')
		lists = getattr(source_cart_type, 'source_cart')()
		first_cart = ''
		for cart_type, label in lists.items():
			first_cart = cart_type
			break
		return first_cart

	def get_first_target_cart_type(self):
		target_cart_type = get_model('type')
		lists = getattr(target_cart_type, 'target_cart')()
		first_cart = ''
		for cart_type, label in lists.items():
			first_cart = cart_type
			break
		return first_cart

	def get_migration_info(self, data):
		self.set_migration_id(data['migration_id'])
		self._notice = None
		self.init_cart()
		notice_clone = self._notice.copy()
		response_from_subprocess(notice_clone)
		return

	def update_migration_info(self, data):
		if not data:
			response_from_subprocess(response_success())
		self.set_migration_id(data['migration_id'])
		self._notice = None
		self.init_cart()
		cart_filter_keys = ['cart_type', 'cart_url', 'token', 'api', 'database']
		filter_keys = ['mode', 'status']

		for cart_key in ['src', ['target']]:
			for filter_key in cart_filter_keys:
				if data.get(filter_key):
					self._notice[cart_key][filter_key] = data[filter_key]
		extend_data = dict()
		for filter_key in filter_keys:
			if data.get(filter_key):
				if filter_key in self._notice:
					self._notice[filter_key] = data[filter_key]
				extend_data[filter_key] = data[filter_key]
		update = self.save_migration(extend_data = extend_data)
		return update

	def get_migration_history(self, data):
		migration = get_model('migration')
		history = getattr(migration, 'get_migration_history')(data['migration_id'])
		response_from_subprocess(history)
		return

	def get_file(self, migration_id, path_file = 'exceptions_top', is_limit = True, limit_line = None):
		if migration_id:
			log_file = get_pub_path() + '/log/' + to_str(migration_id) + '/' + path_file + '.log'
		else:
			log_file = get_pub_path() + '/log/' + path_file + '.log'
		lines = list()
		_limit = to_int(limit_line if limit_line else self.LIMIT_LINE_ERROR)
		if os.path.isfile(log_file):
			file_handle = open(log_file, "r")
			line_lists = file_handle.readlines()
			file_handle.close()
			if (not is_limit) or (to_len(line_lists) <= _limit):
				lines = line_lists
			else:
				index = 0 - _limit
				while index <= -1:
					lines.append(line_lists[index])
					index += 1
		return lines

	def get_process_log(self, data):
		response_from_subprocess(list(reversed(self.get_file(data.get('migration_id'), 'process'))))
		return

	def get_error_entity(self, data):
		if not data['type']:
			return list()
		_type = data['type'] + '_errors'
		response_from_subprocess(self.get_file(data.get('migration_id'), _type))
		return

	def get_errors(self, data):
		response_from_subprocess(self.get_file(data.get('migration_id'), 'exceptions_top'))
		return

	def get_exceptions(self, data):
		response_from_subprocess(self.get_file(data.get('migration_id'), 'exceptions'))
		return

	def client_get_list_migration(self, data):
		cart = get_model('basecart')
		notice = getattr(cart, 'get_list_migration')(data['user_id'], data['page'], data['limit'])
		if notice['result'] == 'success':
			return notice
		return None

	def client_new_migration(self, data):
		migration = get_model('migration')
		return getattr(migration, 'new_migration')(data['user_id'])

	def change_source(self, data):
		self.init_cart()
		cart_type = data.get('source_cart_type')
		self._notice['src']['cart_type'] = cart_type
		setup_source_cart = self.setup_source_cart(cart_type)
		response_from_subprocess({
			'result': 'show',
			'html': setup_source_cart['view_path'],
			'show_next': False if setup_source_cart['setup_type'] == 'file' else True,
			'info': setup_source_cart['info'],
			'setup_type': setup_source_cart['setup_type'],
			'notice': self._notice
		})

	def change_target(self, data):
		self.init_cart()
		cart_type = data.get('target_cart_type')
		self._notice['target']['cart_type'] = cart_type
		setup_target_cart = self.setup_target_cart(cart_type)
		response_from_subprocess({
			'result': 'show',
			'html': setup_target_cart['view_path'],
			'show_next': False if setup_target_cart['setup_type'] == 'file' else True,
			'info': setup_target_cart['info'],
			'setup_type': setup_target_cart['setup_type'],
			'notice': self._notice
		})

	def client_setup_cart(self, data):
		self._migration_id = data['migration_id']
		self.init_cart()
		self._notice['src']['cart_type'] = data.get('source_cart_type')
		self._notice['src']['cart_url'] = data.get('source_cart_url')
		self._notice['target']['cart_url'] = data.get('target_cart_url')
		self._notice['target']['cart_type'] = data.get('target_cart_type')
		# clone_code_for_migration_id(self._migration_id)
		buffer = dict()
		buffer['controller'] = 'migration'
		buffer['action'] = 'setup_cart'
		buffer['data'] = data
		setup_cart = start_subprocess(self._migration_id, buffer, True)
		# subprocess.call(['python3', get_root_path() + '/' + DIR_PROCESS + '/' + self._migration_id + '/bootstrap.py', json.dumps(buffer)])
		# self._notice = self.client_get_migration_info(data)
		# notice = self._notice.copy()
		# self._notice['response'] = dict()
		# self.save_notice(None, False)
		return setup_cart

	def client_config(self, data):
		self._migration_id = data['migration_id']
		self.init_cart()
		buffer = dict()
		buffer['controller'] = 'migration'
		buffer['action'] = 'config'
		buffer['data'] = data
		config = start_subprocess(self._migration_id, buffer, True)

		# subprocess.call(['python3', get_root_path() + '/' + DIR_PROCESS + '/' + self._migration_id + '/bootstrap.py', json.dumps(buffer)])
		# self._notice = self.client_get_migration_info(data)
		# notice = self._notice.copy()
		# self._notice['response'] = dict()
		# self.save_notice(None, False)
		return config


	def kill_end_loop_migration(self, data):
		migration_id = data.get('migration_id')
		self.init_cart()
		stop = getattr(self.get_router(), 'set_flag_stop')(migration_id)
		if stop['result'] != 'success':
			response_from_subprocess(response_error("Don't stop"))
			return
		else:
			response_from_subprocess(response_success())
			return

	def get_cart_type(self, data):
		cart_type = data['type']
		model_type = get_model('type')
		all_cart_type = getattr(model_type, cart_type + '_cart')()
		return all_cart_type

	def get_cart_setup(self, data):
		cart_type = data['type']
		if cart_type != 'target':
			cart_type = 'source'
		model = get_model('basecart')
		cart_setup = getattr(model, cart_type + '_cart_setup')(data.get('cart_type'))
		return cart_setup

	def kill_all_process(self, data):
		server_id = data['server_id']
		migration_model = get_model('migration')
		list_migration = getattr(migration_model, 'get_list_migration_run')(server_id)
		if list_migration['result'] == 'success':
			for migration in list_migration['data']:
				getattr(migration_model, 'set_flag_stop')(migration['migration_id'])
				# pid = to_int(migration['pid'])
				# retry = 5
				# while check_pid(pid) and retry > 0:
				# 	subprocess.call(['kill', '-9', to_str(pid)])
				# 	retry -= 1
				getattr(migration_model, 'set_status_migration')(migration['migration_id'], STATUS_KILL)
		response_from_subprocess(True)


	def kill_migration(self, data, conn = True):
		# cart = get_model('basecart')
		info_migration_id = getattr(self.get_router(), 'get_info_migration')(data['migration_id'])
		if not info_migration_id or not info_migration_id['pid']:
			if conn:
				response_from_subprocess(response_success())
				return
			else:
				return response_success()
		pid = to_int(info_migration_id['pid'])
		retry = 5
		while check_pid(pid) and retry > 0:
			subprocess.call(['kill', '-9', to_str(pid)])
			retry -= 1
		# if check_pid(pid):
		# 	if conn:
		# 		response_from_subprocess(response_error("Don't kill"))
		# 		return
		# 	else:
		# 		return response_error("Don't kill")
		# else:
		self._notice = json_decode(info_migration_id['notice'])
		self.init_cart()
		self.save_migration(True)
		if conn:
			response_from_subprocess(response_success())
			return
		else:
			return response_success()

	def check_run(self, data, conn = True):
		cart = get_model('basecart')
		info_migration_id = getattr(cart, 'get_info_migration')(data['migration_id'])
		if not info_migration_id or not info_migration_id['pid']:
			if conn:
				response_from_subprocess(False)
				return
			else:
				return False
		pid = to_int(info_migration_id['pid'])
		if check_pid(pid) and to_int(info_migration_id['status']) == STATUS_RUN:
			if conn:
				response_from_subprocess(True)
				return
			else:
				return True
		if conn:
			response_from_subprocess(False)
			return
		return False

	def check_custom(self, data):
		migration_id = data['migration_id']
		check = check_folder_clone(migration_id)
		response_from_subprocess(response_success(check))
		return

	def save_history(self, data):
		self._migration_id = data['migration_id']
		getattr(self.get_router(), 'save_migration_history')()
		response_from_subprocess(True)

	def client_get_file_info(self, data):
		buffer = dict()
		buffer['controller'] = 'migration'
		buffer['action'] = 'get_file_info'
		buffer['data'] = data
		file_info = start_subprocess(self._migration_id, buffer, True)
		return file_info

	def get_average(self, data):
		return round(sum(data) / to_len(data), 1)

	def get_server_status(self, data):
		cpu_percent = []
		memory_percent = []
		disk_usage_percent = []
		readio_mps = []
		writeio_mps = []
		new_info = 0
		for x in range(10):
			cpu_percent.append(psutil.cpu_percent(interval = 0.2))
			memory_percent.append(psutil.virtual_memory().percent)
			disk_usage_percent.append(psutil.disk_usage('/')[3])
			if x == 0:
				new_info = psutil.disk_io_counters()
			else:
				old_info = new_info
				new_info = psutil.disk_io_counters()
				r = round((new_info.read_bytes - old_info.read_bytes) / 1024 ** 2, 1)
				readio_mps.append(r)
				w = round((new_info.write_bytes - old_info.write_bytes) / 1024 ** 2, 1)
				writeio_mps.append(w)
		status = {
			"cpu_percent"       : self.get_average(cpu_percent),
			"memory_percent"    : self.get_average(memory_percent),
			"disk_usage_percent": self.get_average(disk_usage_percent),
			"readio_mps"        : self.get_average(readio_mps),
			"writeio_mps"       : self.get_average(writeio_mps)
		}

		#get migrations info
		migrations = []
		for proc in psutil.process_iter():
			proc_cmd = proc.cmdline()
			if not proc_cmd or 'python' not in proc_cmd[0]:
				continue
			if len(proc_cmd) > 2 and proc_cmd[0] and proc_cmd[0] == "python3" and proc_cmd[1] and "bootstrap.py" in proc_cmd[1]:
				proc_status = {
					"pid": proc.pid,
					"cpu_percent": proc.cpu_percent(interval = 0.2),
					"memory_info": str(math.ceil(proc.memory_info().rss / (1024 * 1024))) + "M",
					"create_time": datetime.fromtimestamp(proc.create_time()).strftime("%Y-%m-%d %H:%M:%S"),
					"path": proc_cmd[1],
					"migration_info": json_decode(proc_cmd[2])

				}
				# try:
				# 	migration_info = json.loads(proc_cmd[2])
				# 	proc_status["migration_id"] = migration_info.get("data", dict()).get("migration_id")
				# except Exception as e:
				# 	proc_status["migration_id"] = ""
				migrations.append(proc_status)
		status["processes"] = migrations
		if data and isinstance(data, dict) and data.get('default'):
			migration = get_model('migration')
			default_notice = getattr(migration, 'get_default_notice')()
			status['default_notice'] = default_notice

		response_from_subprocess(status)
		return


	def restart_migration(self, data):
		self.kill_migration(data, False)
		buffer = dict()
		buffer['controller'] = 'migration'
		buffer['action'] = 'start'
		buffer['data'] = dict()
		buffer['data']['migration_id'] = data['migration_id']
		res = response_success()
		res['next'] = buffer
		# start_subprocess(data['migration_id'], buffer)
		response_from_subprocess(res)
		return

	def client_update_token(self, data):
		migration_id = data.get('migration_id')
		type_token = data.get('type')
		token = data.get('token')
		if not migration_id or not type_token or not token:
			return True
		router = get_model('migration')
		getattr(router, 'update_token')(migration_id, type_token, token)
		return True

	def delete_migration(self, data):
		# check_run = self.check_run(data, False)
		# if check_run is False:
		# 	response_from_subprocess(False)
		# 	return
		self.kill_migration(data, False)
		migration_id = data.get('migration_id')
		if not migration_id:
			response_from_subprocess(True)
			return
		path = get_pub_path()
		path = path.replace('processes', '')
		path = path.rstrip('/')
		path_delete = [
			path + '/' + DIR_PROCESS + to_str(migration_id),
			path + '/log/' + to_str(migration_id),
			path + '/uploads/' + to_str(migration_id)
		]
		for path in path_delete:
			if os.path.isdir(path):
				shutil.rmtree(path)
		response_from_subprocess(True)
		return

	def reset_migration(self, data):
		kill_process = self.kill_migration(data, False)
		if kill_process['result'] != 'success':
			response_from_subprocess(kill_process)
			return
		# migration = get_model('migration')
		reset = getattr(self.get_router(), 'reset_migration')(data['migration_id'])
		if not reset:
			response_from_subprocess(response_error("Don't reset"))
			return
		clear_log(data['migration_id'])
		buffer = dict()
		buffer['controller'] = 'migration'
		buffer['action'] = 'start'
		buffer['data'] = dict()
		buffer['data']['migration_id'] = data['migration_id']
		buffer['data']['test'] = data.get('test')
		# start_subprocess(data['migration_id'], buffer)
		res = response_success()
		res['next'] = buffer
		response_from_subprocess(res)
		# subprocess.Popen(['python3', get_root_path() + '/' + DIR_PROCESS + '/' + data['migration_id'] + '/bootstrap.py', json.dumps(buffer)])
		return

	def clone_migration(self, data):
		migration_id = data.get('migration_id')

		if check_folder_clone(migration_id):
			if not data.get('replace'):
				response_from_subprocess({
					'result': 'exist',
					'msg': '',
					'data': None
				})
				return
			clear_folder_clone(migration_id)
		clone_code_for_migration_id(migration_id)
		response_from_subprocess(response_success())
		return

	def get_default_notice(self, data):
		migration = get_model('migration')
		default_notice = getattr(migration, 'get_default_notice')()
		response_from_subprocess(default_notice)
		return

	def stop_auto_test(self, data):
		auto_test_id = data.get('auto_test_id')
		if not auto_test_id:
			response_from_subprocess(response_success())
		auto_test = get_model('autotest')
		stop = getattr(auto_test, 'stop')(auto_test_id)
		response_from_subprocess(stop)
		return

	def clear_log_auto_test(self, data):
		auto_test_id = data.get('auto_test_id')
		if not auto_test_id:
			return response_success()
		auto_test = get_model('autotest')
		migration_ids = getattr(auto_test, 'get_all_migration')(auto_test_id)
		for migration_id in migration_ids:
			clear_log(migration_id)
		return response_success()

	def clear_previous_data(self, data, conn = True):
		migration_id = data.get('migration_id')
		if not migration_id:
			return response_success()
		clear_log(migration_id)
		route = get_model('migration')
		getattr(route, 'clear_previous_data')(migration_id, test = data.get('test'))
		return response_from_subprocess(response_success(), conn)


	def start_autotest(self, old_migration_id):
		cart = get_model('basecart')
		info = getattr(cart, 'get_info_migration')(old_migration_id)
		if info['migration_group'] == GROUP_TEST:
			auto_test = get_model('autotest')
			auto_test_id = getattr(auto_test, 'get_auto_test_id')(old_migration_id)
			getattr(auto_test, 'set_id')(auto_test_id)
			new_migration_test_id = getattr(auto_test, 'get_migration_id')()
			if new_migration_test_id:
				start_autotest(auto_test_id)
			else:
				getattr(auto_test, 'set_status_auto_test')(auto_test_id, getattr(auto_test, 'STATUS_STOP'))

	def get_content_mail_to_dev(self, action = 0, file_log = 'exceptions_top'):
		try:
			server = socket.gethostbyname(socket.gethostname())  # Default to any avialable network interface
		except Exception:
			server = '127.0.0.1'
		if server == '127.0.1.1':
			server = '127.0.0.1'
		lines = list()
		title = self._notice['src']['cart_type'].capitalize() + ' to ' + self._notice['target']['cart_type']
		if self._migration_id:
			title += ' ID ' + to_str(self._migration_id)
		if action == self.ACTION_COMPLETED:
			title += ' in server ' + to_str(server) + ' completed with errors. Details: '
		elif action == self.ACTION_APP_MODE:
			title += ' in server ' + to_str(server) + ' completed with App Mode. Details: '
		elif action == self.ACTION_DEMO_ERROR:
			step = 'setup'
			if self.data and self.data.get('step'):
				step = to_str(self.data.get('step'))
			title += ' in server ' + to_str(server) + ' error at step ' + step + '. Details: '
		else:
			title += ' in server ' + to_str(server) + ' was stopped. Details: '
		lines.append(title)
		lines.append('- Source Type: ' + self._notice['src']['cart_type'])
		lines.append('- Source Url: ' + to_str(self._notice['src']['cart_url']))
		lines.append('- Target Type: ' + self._notice['target']['cart_type'])
		lines.append('- Target Url: ' + to_str(self._notice['target']['cart_url']))
		lines.append('- Time Stop: ' + to_str(convert_format_time(to_int(time.time()))))
		if action not in [self.ACTION_APP_MODE, self.ACTION_DEMO_ERROR]:
			lines.append('- Error log: (' + file_log + ')' + '.log')
			error_log = self.get_file(self._migration_id, file_log, True, 20)
			lines = list(lines + error_log)
		return lines


	def send_email_to_dev(self, check_dev = True, msg = None):
		migration_model = get_model('migration')
		migration = getattr(migration_model, 'get_info_migration')(self._migration_id)
		if not migration:
			return
		if check_dev:
			if migration['on_error'] != getattr(migration_model, 'ERROR_STOP') or migration['dev_notification'] != getattr(migration_model, 'DEV_NOTIFY'):
				return
			dev_emails = migration['dev_emails']
			if not dev_emails:
				return
		else:
			dev_emails = self.get_config_ini('sendgrid', 'email_to')
		if not dev_emails:
			return
		email_content = self.get_content_mail_to_dev(msg)
		dev_emails = to_str(dev_emails).split(';')
		if dev_emails:
			for dev_email in dev_emails:
				self.send_email(dev_email, '\n'.join(email_content), email_content[0])
		return

	def send_email(self, to_email, content_mail, subject = None, from_email = None):
		content_mail = to_str(content_mail).replace('https://', 'ht_tps://').replace('http://', 'ht_tp://')
		api_key = self.get_config_ini('sendgrid', 'api_key')
		if not from_email:
			from_email = self.get_config_ini('sendgrid', 'email_from', 'litextension@com.vn')
		sg = sendgrid.SendGridAPIClient(api_key)
		from_email = Email(from_email)
		to_email = Email(to_email)
		subject = subject if subject else "Title"
		mail_content = Content("text/plain", content_mail)
		send_mail = Mail(from_email, subject, to_email, mail_content)
		try:
			sg.client.mail.send.post(request_body = send_mail.get())
		except Exception:
			self.log_traceback('sendgrid')

	def get_config_ini(self, section, key, default = None):
		return get_config_ini(section, key, default, self._migration_id)