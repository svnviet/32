from cartmigration.libs.base_model import BaseModel
from cartmigration.libs.utils import *
from cartmigration.models.setup import Setup

class LeMigration(BaseModel):
	MIGRATION_FULL = 2
	MIGRATION_DEMO = 1
	GROUP_USER = 1
	GROUP_TEST = 2
	ERROR_CONTINUE = 1
	ERROR_STOP = 2
	DEV_NOTIFY = 1
	DEV_NO_NOTIFY = 2
	DEMO_INIT = 1
	DEMO_SKIP = 2
	DEMO_SUCCESS = 3
	USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"

	def __init__(self, data = None):
		super().__init__()
		self._user_id = None
		self._migration_id = data.get('migration_id') if isinstance(data, dict) else None
		self._notice = None
		self.abs_migration = None
		self._is_test = False

	def set_is_test(self, is_test = False):
		self._is_test = is_test

	def init_notice(self):
		if self._migration_id:
			self._notice = self.get_migration_notice(self._migration_id)
		if not self._notice:
			self._notice = self.get_default_notice()
		self._notice = self.sync_data_in_notice(self._notice, self.get_default_notice())
		return self._notice

	def list_to_dict(self, list_data):
		if isinstance(list_data, dict):
			return list_data
		dict_data = dict()
		if not isinstance(list_data, list) or not list_data:
			return dict_data
		for index, value in enumerate(list_data):
			dict_data[str(index)] = value
		return dict_data

	def set_migration_id(self, migration_id):
		self._migration_id = migration_id

	def get_list_migration(self, user_id, page = None, limit = None):
		if not user_id:
			return dict()
		if not page:
			page = 1
		page = to_int(page)
		limit = to_int(limit)
		number_migration = to_int(self.count_table(TABLE_MIGRATION, {'user_id': user_id}))
		base_page = number_migration // limit
		extend_page = number_migration % limit
		offset = (page - 1) * limit
		number_page = base_page + 1 if extend_page > 0 else base_page
		list_migration = self.select_page(TABLE_MIGRATION, {'user_id': user_id}, None, limit, offset, 'migration_id')
		if list_migration['result'] != 'success':
			return response_error()
		list_migration['number_page'] = number_page
		return list_migration

	def get_migration_notice(self, migration_id):
		return getattr(self.get_abstract_migration(), 'get_migration_notice')(migration_id)

	def get_info_migration(self, migration_id):
		return getattr(self.get_abstract_migration(), 'get_info_migration')(migration_id)

	def save_user_notice(self, _migration_id, notice, pid = None, mode = None, status = None, clear_entity_warning = False):
		notice = json_encode(notice) if isinstance(notice, dict) else notice
		if not _migration_id:
			return False
		result = self.update_notice(_migration_id, notice, pid, mode, status, clear_entity_warning = clear_entity_warning)
		return True if result['result'] == 'success' else False

	def update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False, clear_entity_warning = False):
		return getattr(self.get_abstract_migration(), 'update_notice')(_migration_id, notice, pid, mode, status, finish, clear_entity_warning)

	def delete_migration_notice(self, migration_id):
		return getattr(self.get_abstract_migration(), 'delete_migration_notice')(migration_id)

	def save_migration(self, migration_id, data):
		return getattr(self.get_abstract_migration(), 'save_migration')(migration_id, data)

	def set_status_migration(self, migration_id, status):
		return getattr(self.set_status_migration(), 'set_status_migration')(migration_id, status)

	def get_default_notice(self):
		return getattr(self.get_abstract_migration(), 'get_default_notice')()

	def get_default_process(self):
		return getattr(self.get_abstract_migration(), 'get_default_process')()

	def after_finish(self):
		return getattr(self.get_abstract_migration(), 'after_finish')(self._migration_id)


	def get_app_mode_limit(self):
		setting = getattr(self.get_abstract_migration(), 'get_app_mode_limit')()
		if not setting:
			return response_error()
		limit_data = json_decode(setting)
		if not limit_data:
			return response_error()
		if not limit_data.get(self._notice['target']['cart_type']):
			return response_error()
		limit = limit_data[self._notice['target']['cart_type']]
		if not limit.get('limit') or not limit.get('entities'):
			return response_error()
		entities = limit['entities']
		if entities == 'all':
			return response_success({'limit': limit['limit'], 'entities': 'all'})
		entities = to_str(limit['entities']).split(',')
		return response_success({'limit': limit['limit'], 'entities': entities})

	def get_default_file_details(self):
		return {
			'upload': False,
			'name': '',
			're_upload': False,
			'storage': False,
		}

	def update_token(self, migration_id, token_type, token):
		if not migration_id or not token or not token_type:
			return True
		if token_type != 'src' and token_type != 'target':
			return True
		migration = self.get_info_migration(migration_id)
		if not migration:
			return True
		try:
			notice = json.loads(migration['notice'])
		except Exception:
			notice = None

		field_token = token_type + '_token'
		data_update = {
			field_token: token
		}
		if notice:
			notice[token_type]['config']['token'] = token
			data_update['notice'] = json.dumps(notice)

		self.update_obj(TABLE_MIGRATION, data_update, {'migration_id': migration_id})
		return True


	def new_migration(self, user_id):
		if not user_id:
			return None
		migration_data = {
			'user_id': user_id,
		}
		migration = self.insert_obj(TABLE_MIGRATION, migration_data, True)
		if migration['result'] == 'success':
			return migration['data']
		return None

	def reset_migration(self, migration_id):
		notice = self.get_migration_notice(migration_id)
		if not notice:
			return False
		for key, value in notice['process'].items():
			notice['process'][key] = self.reset_process(value, notice['mode'])
		notice['config']['reset'] = True
		notice['finish'] = False
		notice['resume']['action'] = 'display_import'
		notice['resume']['type'] = ''
		notice['target']['clear']['function'] = 'clear_target_taxes'
		notice['target']['clear']['result'] = 'process'

		res = self.update_notice(migration_id, notice)
		if res['result'] == 'success':
			return True
		return False

	def reset_process(self, process, mode):
		new_process = self.get_default_process()
		key_total = 'total'
		if to_int(mode) == self.MIGRATION_FULL:
			key_total = 'real_total'
		new_process['total'] = process[key_total]
		new_process['real_total'] = process['real_total']
		for key, value in process.items():
			if key not in new_process:
				# reset smart collection shopify
				if key == 'id_src_smart':
					new_process[key] = 0
				else:
					new_process[key] = value
		return new_process

	def sync_data_in_notice(self, data, default_data):
		for key, value in default_data.items():
			if isinstance(value, dict):
				if not data.get(key):
					data[key] = default_data[key]
				elif isinstance(data[key], dict):
					data[key] = self.sync_data_in_notice(data[key], default_data[key])
				elif isinstance(data[key], list):
					data[key] = self.list_to_dict(data[key])
		return data

	def get_list_migration_run(self, server_id):
		data = {
			'server_id': server_id,
			'status': STATUS_RUN
		}
		list_migration = self.select_obj(TABLE_MIGRATION, data)
		return list_migration

	def clear_previous_data(self, migration_id, test = False):
		if not migration_id:
			return response_success()
		setup = Setup()
		check_setup = setup.setup_db_for_migration(migration_id, test = test)
		if not check_setup:
			return create_response('stop', "Can't setup db")
		# self.delete_obj(TABLE_MAP, {'migration_id': migration_id})
		# self.delete_obj(TABLE_RECENT, {'migration_id': migration_id})
		# self.delete_obj(TABLE_FLAG_STOP, {'migration_id': migration_id})
		return response_success()

	def is_stop_flag(self, migration_id):
		if not migration_id:
			return True
		stop = self.select_row(TABLE_FLAG_STOP, {'migration_id': migration_id})
		if stop:
			return stop['flag']
		return False

	def clear_stop_flag(self, migration_id):
		if not migration_id:
			return True

		self.delete_obj(TABLE_FLAG_STOP, {'migration_id': migration_id})
		return True

	def get_migration_history(self, migration_id):
		if not migration_id:
			return True
		self._migration_id = migration_id
		query = "SELECT * FROM " + TABLE_MIGRATION_HISTORY + " WHERE migration_id = " + to_str(migration_id) + " AND type NOT LIKE '%demo%'"
		history = self.select_raw(query)
		res = list()
		if history['result'] == 'success':
			for row in history['data']:
				if isinstance(row['created_at'], datetime):
					row['created_at'] = row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
				res.append(row)
		return res

	def set_flag_stop(self, migration_id, kill_all = False):
		if not migration_id:
			return response_success()
		self.delete_obj(TABLE_FLAG_STOP, {'migration_id': migration_id})
		return self.insert_obj(TABLE_FLAG_STOP, {'migration_id': migration_id, 'flag': FLAG_STOP if not kill_all else FLAG_KILL_ALL})

	def create_demo_error(self, notice):
		check_data = {
			'action': 'completed',
			'migration_id': notice['migration_id']
		}
		check = self.select_row(TABLE_DEMO_ERROR, check_data)
		if not check:
			migration = self.select_row(TABLE_MIGRATION, {'migration_id': notice['migration_id']})
			if not migration:
				return True
			error_detail = 'Entity error: '
			for entity, process in self._notice['process'].items():
				if process['error']:
					error_detail += "\n" + entity.capitalize() + ': ' + to_str(process['error']) + '/' + to_str(process['imported'])
			data = {
				'src_cart_type': notice['src']['cart_type'],
				'src_cart_url': notice['src']['cart_url'],
				'target_cart_type': notice['target']['cart_type'],
				'target_cart_url': notice['target']['cart_url'],
				'action': 'completed',
				'migration_id': notice['migration_id'],
				'error_detail': error_detail,
				'user_id': migration['user_id'],
				'created_at': get_current_time(),
				'updated_at': get_current_time()
			}
			return self.insert_obj(TABLE_DEMO_ERROR, data)
		return True

	def log_error(self, url, data_log = None, type_log = 'request', is_proxies = ''):
		if not data_log:
			data_log = dict()
		msg_log = 'Url: ' + to_str(url)
		if data_log.get('data'):
			msg_log += '\nData: ' + to_str(data_log.get('data'))
		if data_log.get('data_encode'):
			msg_log += '\nData Encode: ' + to_str(data_log.get('data_encode'))
		if data_log.get('method'):
			msg_log += '\nMethod: ' + to_str(data_log.get('method'))
		if data_log.get('status'):
			msg_log += '\nResponse status: ' + to_str(data_log.get('status'))
		if data_log.get('response'):
			msg_log += '\nResponse data: ' + to_str(data_log.get('response'))
		if data_log.get('error'):
			msg_log += '\nError: ' + to_str(data_log.get('error'))
		if data_log.get('header'):
			msg_log += '\nHeader: ' + to_str(data_log.get('header'))
		if is_proxies:
			msg_log += '\nProxy: ' + to_str(is_proxies)

		self.log(msg_log, type_log)

	def log_traceback(self, type_error = 'exceptions'):
		error = traceback.format_exc()
		self.log(error, type_error)

	def log(self, msg, type_log = 'exceptions', is_log = True):
		if is_log:
			log(msg, self._migration_id, type_log)

	def get_abstract_migration(self):
		if self.abs_migration:
			return self.abs_migration
		server_mode = get_config_ini('local', 'mode')
		if not server_mode or self._is_test or (self._notice and self._notice['config'].get('test')):
			server_mode = 'test'

		model_name = 'abs_migration/' + server_mode + '_migration'
		model = get_model(model_name, class_name = 'Le' + server_mode.capitalize() + 'Migration')
		if model:
			getattr(model, 'set_migration_id')(self._migration_id)
			getattr(model, 'set_notice')(self._notice)
			getattr(model, 'set_mode')(server_mode)
		self.abs_migration = model
		return model
