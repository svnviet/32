import copy

from cartmigration.libs.base_controller import BaseController
from cartmigration.libs.utils import *

class Migration(BaseController):
	_db = None
	_import_action = (
		'taxes',
		'manufacturers',
		'categories',
		'attributes',
		'products',
		'customers',
		'orders',
		'reviews',
		'pages',
		'blogs',
		'coupons',
		'cartrules')
	_import_next_action = {
		'taxes': 'manufacturers',
		'manufacturers': 'categories',
		'categories': 'attributes',
		'attributes': 'products',
		'products': 'customers',
		'customers': 'quotes',
		'quotes': 'newsletters',
		'newsletters': 'orders',
		'orders': 'reviews',
		'reviews': 'pages',
		'pages': 'blogs',
		'blogs': 'coupons',
		'coupons': 'cartrules',
		'cartrules': False,
	}
	_import_simple_action = {
		'taxes': 'tax',
		'manufacturers': 'manufacturer',
		'categories': 'category',
		'products': 'product',
		'attributes': 'attribute',
		'customers': 'customer',
		'quotes': 'quote',
		'newsletters': 'newsletter',
		'orders': 'order',
		'reviews': 'review',
		'pages': 'page',
		'blogs': 'blog',
		'coupons': 'coupon',
		'cartrules': 'cartrule',
	}

	_current_import_action = 'taxes'

	_next_action = {
		# 'setup_database': 'storage_data',
		'storage_data': 'display_import',
		'display_import': 'clear',
		'clear': 'clear_demo',
		'clear_demo': 'prepare_migration',
		'prepare_migration': 'migration',
		'migration': 'finish_migration',
		'finish_migration': False
	}

	def __init__(self, data = None):
		super().__init__(data)
		self._exit_flag = 0
		self._retry = 0
		self.pid = os.getpid()
		self._migration_id = data.get('migration_id') if data else None

	# self._notice = data

	def start(self, data = None):
		# previous_notice = self.
		self.init_cart()
		self._notice['finish'] = False
		self._notice['running'] = True
		# self._notice['step'] = 'import'
		self._notice['resume']['process'] = 'migrating'
		self.clear_stop_flag()
		self.log_start()
		self.save_notice(STATUS_RUN)
		self.run()

	def prepare_run(self, data):
		self._exit_flag = 0
		self._migration_id = data['migration_id']
		self._notice = data
		self._notice['finish'] = False
		self._notice['running'] = True
		self._notice['resume']['process'] = 'migrating'
		self.save_notice()

	def log_start(self):
		if not self._notice.get('log_start'):
			msg = ''
			if self._notice['mode'] == MIGRATION_DEMO:
				msg = 'Starting Demo Migration ID: ' + to_str(self._migration_id)
			else:
				msg = 'Starting Full Migration ID: ' + to_str(self._migration_id)
			if self._notice['config'].get('recent'):
				msg = 'Starting Recent Migration ID: ' + to_str(self._migration_id)
			if self._notice['config'].get('remigrate'):
				msg = 'Starting Remigrate ID: ' + to_str(self._migration_id)
			msg = "---------- " + msg + " ----------"
			self.log(msg, 'process')
			self._notice['log_start'] = True

	def log_finish(self):
		msg = ""
		if self._notice['mode'] == MIGRATION_DEMO:
			msg = 'Demo Migration ID: ' + to_str(self._migration_id) + ' completed'
		else:
			msg = 'Full Migration ID: ' + to_str(self._migration_id) + ' completed'
		if self._notice['config'].get('recent'):
			msg = 'Recent Migration ID: ' + to_str(self._migration_id) + ' completed'
		if self._notice['config'].get('remigrate'):
			msg = 'Remigrate  #' + to_str(self._migration_id) + ' completed'
		self.log(msg, 'process')

	def run(self, data = None):
		print("Starting " + to_str(self.pid))
		if data:
			self.prepare_run(data)
		self.init_cart()
		action = self.get_action()
		check_stop = to_int(self.is_stop_process())
		while check_stop not in [FLAG_KILL_ALL, FLAG_STOP]:
			result = getattr(self, action)()
			# self.log(result, 'result')
			if result['result'] == 'stop':
				self.send_email_to_dev()
				self.save_migration(True)
				break
			if result['result'] == 'stop_export':
				self.send_email_to_dev(False, result.get('msg'))
				self.save_migration(True)
				break

			if result['result'] == 'success':
				if result.get('msg'):
					self.log(result['msg'], 'process')
				if self._next_action[action]:
					action = self._next_action[action]
					self._notice['resume']['action'] = action
					self.save_notice()
				else:
					break
			else:
				if 'current' in result:
					self.set_current(result['current'])
			time.sleep(0.1)
			check_stop = to_int(self.is_stop_process())
		if check_stop == FLAG_KILL_ALL:
			self.save_migration(False, True)
		elif check_stop == FLAG_STOP:
			self.save_migration(True)
		else:
			self.save_migration()
		print("Exiting " + to_str(self.pid))

	def display_upload(self, data = None):
		self.init_cart()
		files_data = data.get('upload_res')
		upload_res = data.get('upload_res')
		# router = get_model('basecart')
		# getattr(router, 'set_migration_id')(self._migration_id)
		# getattr(router, 'set_notice')(self._notice)
		previous_notice = self._notice.copy()
		if previous_notice and previous_notice['src']['config']['folder']:
			# previous_folder = DIR_UPLOAD + '/' + previous_notice['src']['config']['folder']
			# getattr(router, 'delete_dir')(previous_folder)
			if previous_notice['src']['cart_type']:
				previous_cart_name = getattr(self.get_router(), 'get_cart')(previous_notice['src']['cart_type'], previous_notice['src']['config']['version'])
				previous_cart = get_model(previous_cart_name)
				getattr(previous_cart, 'set_migration_id')(self._migration_id)

				if previous_cart and previous_notice.get('migration_id'):
					getattr(previous_cart, 'set_type')('src')
					getattr(previous_cart, 'set_notice')(previous_notice)
					if not self._notice['config']['recent']:
						getattr(previous_cart, 'clear_previous_section')(files_data.keys())

		# source_cart = self.get_source_cart(router)
		if not self.get_source_cart():
			response_from_subprocess(response_error())
			return
		file_info = getattr(self.source_cart, 'get_file_info')()
		folder_upload = get_pub_path() + '/' + DIR_UPLOAD + '/' + to_str(self._migration_id) + '/' + self._notice['src']['config']['folder']

		# folder_upload = get_root_path() + '/' + DIR_UPLOAD + '/' + self._notice['src']['config']['folder']
		for info_key, info_label in file_info.items():
			file_details = getattr(self.router, 'get_default_file_details')()
			if (info_key in files_data) and files_data[info_key]['result'] == 'success':
				upload_name = getattr(self.source_cart, 'get_upload_file_name')(info_key)
				try:
					path_upload = folder_upload
					file_upload = path_upload + '/' + files_data[info_key]['file']
					new_file = get_pub_path() + '/' + DIR_UPLOAD + '/' + to_str(
						self._migration_id) + '/' + upload_name

					# new_file = get_root_path() + '/' + DIR_UPLOAD + '/' + upload_name

					if os.path.isfile(file_upload):
						if os.path.isfile(new_file):
							os.remove(new_file)
						shutil.move(file_upload, new_file)
					file_details['upload'] = True
					file_details['name'] = upload_name
					file_details['storage'] = False
					self._notice['src']['config']['file'][info_key] = file_details
					upload_res[info_key]['result'] = 'success'
					upload_res[info_key]['name'] = upload_name
				except Exception as e:
					self.log_traceback()
					file_details['upload'] = False
					upload_res[info_key]['result'] = 'error'
					self._notice['src']['config']['file'][info_key] = file_details
			else:
				file_details['upload'] = False
				self._notice['src']['config']['file'][info_key] = file_details
		if os.path.isdir(folder_upload):
			os.rmdir(folder_upload)
		getattr(self.router, 'set_notice')(self._notice)
		if not self._notice['config'].get('recent'):
			prepare_display_upload = getattr(self.router, 'prepare_display_upload')(data)
			self._notice = getattr(self.router, 'get_notice')()
		# source_cart = self.get_source_cart(router)
		getattr(self.source_cart, 'set_notice')(self._notice)
		display_upload = getattr(self.source_cart, 'display_upload')(upload_res)
		if display_upload['result'] != 'success':
			response_from_subprocess(display_upload)
			return
		self._notice = getattr(self.source_cart, 'get_notice')()
		self.save_notice()
		response_from_subprocess(display_upload['msg'])
		return

	def setup_source(self, request):
		self.init_cart()
		cart_type = request['src_cart_type']
		src_setup_type = getattr(self.router, 'source_cart_setup')(cart_type)
		previous_notice = copy.deepcopy(self._notice)
		is_init_notice = getattr(self.router, 'is_init_notice')(cart_type)
		if self._migration_id:
			self._notice = None
		self.init_cart(True)
		self._notice['src']['cart_type'] = cart_type
		self._notice['src']['config']['token'] = get_value_by_key_in_dict(request, 'src_token', '123456')
		self._notice['target']['cart_type'] = request.get('target_cart_type')
		self._notice['target']['cart_url'] = request.get('target_cart_url')
		self._notice['target']['config']['token'] = get_value_by_key_in_dict(request, 'target_token', '123456')
		if request.get('demo_store'):
			self._notice['config']['demo_store'] = True
		if request.get('test'):
			self._notice['config']['test'] = True
		if previous_notice['config'].get('remigrate'):
			self._notice['config']['remigrate'] = True
		if request.get('src_auth_user'):
			self._notice['src']['config']['auth'] = dict()
			self._notice['src']['config']['auth']['user'] = request.get('src_auth_user')
			self._notice['src']['config']['auth']['pass'] = request.get('src_auth_pass')
		else:
			if 'auth' in self._notice['src']['config']:
				del (self._notice['src']['config']['auth'])
		if request.get('target_auth_user'):
			self._notice['target']['config']['auth'] = dict()
			self._notice['target']['config']['auth']['user'] = request.get('target_auth_user')
			self._notice['target']['config']['auth']['pass'] = request.get('target_auth_pass')
		else:
			if 'auth' in self._notice['target']['config']:
				del (self._notice['target']['config']['auth'])
		cart_url = getattr(self.get_source_cart(), 'format_url')(request['src_cart_url'])
		if previous_notice['migration_id'] and src_setup_type == 'file':
			if previous_notice['src']['cart_url'] == cart_url:
				self._notice['src']['config']['file'] = previous_notice['src']['config']['file'].copy()
		self._notice['src']['cart_url'] = cart_url

		getattr(self.get_router(), 'set_notice')(self._notice)
		getattr(self.get_source_cart(), 'set_notice')(self._notice)
		getattr(self.get_target_cart(), 'set_notice')(self._notice)
		prepare_display_setup_source = getattr(self.source_cart, 'prepare_display_setup_source')(request)
		self._notice = getattr(self.source_cart, 'get_notice')()
		if prepare_display_setup_source['result'] != 'success':
			if src_setup_type == 'connector' or (src_setup_type == 'module_connector' and self._notice['src']['config'].get('type_upload') == 'connector'):
				prepare_display_setup_source['source_connector'] = True
				# self.save_notice()
			return prepare_display_setup_source
		# source_cart = self.get_source_cart(router)
		display_setup_source = getattr(self.source_cart, 'display_setup_source')(request)
		self._notice = getattr(self.source_cart, 'get_notice')()
		# if not self.save_notice():
		# 	return response_error()
		if display_setup_source['result'] != 'success':
			return display_setup_source
		self.reset_cart()
		return response_success()

	def get_file_info(self, data):
		self.init_cart()
		cart_type = data['cart_type']
		router = get_model('basecart')
		cart_name = getattr(router, 'get_cart')(cart_type)
		cart = get_model(cart_name)
		file_info = getattr(cart, 'get_file_info')()
		response_from_subprocess(file_info)
		return

	def get_api_info(self, data):
		self.init_cart()
		cart_type = data['cart_type']
		router = get_model('basecart')
		cart_name = getattr(router, 'get_cart')(cart_type)
		cart = get_model(cart_name)
		api_info = getattr(cart, 'get_api_info')()
		response_from_subprocess(api_info)
		return

	def get_module_connector_info(self, data):
		self.init_cart()
		cart_type = data['cart_type']
		router = get_model('basecart')
		cart_name = getattr(router, 'get_cart')(cart_type)
		cart = get_model(cart_name)
		api_info = getattr(cart, 'get_module_connector_info')()
		response_from_subprocess(api_info)
		return

	def get_database_info(self, data):
		self.init_cart()
		cart_type = data['cart_type']
		router = get_model('basecart')
		cart_name = getattr(router, 'get_cart')(cart_type)
		cart = get_model(cart_name)
		api_info = getattr(cart, 'get_database_info')()
		response_from_subprocess(api_info)
		return

	def setup_cart(self, data, conn = True):
		# self.init_cart()
		# previous_notice = copy.deepcopy(self._notice)
		current = data.get('current')
		if current != 'storage':
			try:
				setup_source = self.setup_source(data)
			except Exception as e:
				# self._notice = previous_notice
				# self.save_notice()
				self.log_traceback()
				response = response_error('There is an error with your Source Cart connection. ', '#source-cart-url', 'General error')
				if self._notice['src']['setup_type'] == 'connector':
					response['msg'] += '<a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">More details!</a>'

				response['entity'] = 'src'
				if conn:
					error = traceback.format_exc()
					self.notify_demo_error(error)
					response_from_subprocess(response)
					return
				else:
					return response
			if setup_source['result'] != 'success':
				setup_source['entity'] = 'src'
				# if self._notice['src']['setup_type'] == 'connector':
				# 	setup_source['msg'] += ' Please <a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">click</a> read more details!'

				# self._notice = previous_notice
				# self.save_notice()
				self.log(setup_source['msg'], 'setup_cart')
				if conn:
					self.notify_demo_error(setup_source['msg'])
					response_from_subprocess(setup_source)
					return
				else:
					return setup_source
		try:
			setup_target = self.setup_target(data)
		except Exception as e:
			# self._notice = previous_notice
			# self.save_notice()
			self.log_traceback()
			response = response_error('There is an error with your Target Cart connection.', '#target-cart-url', 'General error')
			if self._notice['target']['setup_type'] == 'connector':
				response['msg'] += '<a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">More details!</a>'
			if not response.get('entity'):
				response['entity'] = 'target'

			if conn:
				error = traceback.format_exc()
				self.notify_demo_error(error)
				response_from_subprocess(response)
				return
			else:
				return response
		if setup_target['result'] != 'success':
			# self._notice = previous_notice
			# self.save_notice()
			# setup_target['entity'] = 'target'
			if not setup_target.get('entity'):
				setup_target['entity'] = 'target'
			# if self._notice[setup_target['entity']]['setup_type'] == 'connector':
			# 	setup_target['msg'] += ' Please <a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">click</a> read more details!'

			if conn:
				self.notify_demo_error(setup_target['msg'])
				response_from_subprocess(setup_target)
				return
			else:
				return setup_target
		if conn:
			response_from_subprocess(response_success(self._notice))
			return
		else:
			return response_success(self._notice)

	def storage_or_setup(self, data):
		router = get_model('basecart', data)
		self.init_cart()
		current = data.get('current', '')
		src_cart_type = self._notice['src']['cart_type']
		src_setup_type = getattr(router, 'source_cart_setup')(src_cart_type)
		if (src_setup_type == 'file') and (current != 'storage'):
			return self.storage(data)
		return self.setup_target(data)

	def storage(self, data):
		self.init_cart()
		src_cart_type = self._notice['src']['cart_type']
		src_setup_type = getattr(self.router, 'source_cart_setup')(src_cart_type)
		target_cart_type = data.get('target_cart_type', '')
		target_cart_url = data.get('target_cart_url', '')
		target_cart_url = getattr(self.router, 'format_url')(target_cart_url)
		self._notice['target']['cart_type'] = target_cart_type
		self._notice['target']['cart_url'] = target_cart_url
		self.target_cart = self.get_target_cart()
		prepare_display_setup_target = getattr(self.target_cart, 'prepare_display_setup_target')()
		self._notice = getattr(self.target_cart, 'get_notice')()
		if prepare_display_setup_target['result'] != 'success':
			target_setup_type = getattr(self.router, 'target_cart_setup')(target_cart_type)
			if target_setup_type == 'connector':
				self._notice['response']['target_connector'] = False
				self.save_notice()
				return prepare_display_setup_target
		# target_cart = self.get_target_cart(self.router)
		display_setup_target = getattr(self.target_cart, 'display_setup_target')(data)
		self._notice = getattr(self.target_cart, 'get_notice')()
		if display_setup_target['result'] != 'success':
			self.save_notice()
			return display_setup_target

		getattr(self.router, 'set_notice')(self._notice)

		prepare_display_storage = getattr(self.router, 'prepare_display_storage')()
		self._notice = getattr(self.router, 'get_notice')()
		if prepare_display_storage['result'] != 'success':
			self.save_notice()
			return prepare_display_storage

		self.source_cart = self.get_source_cart()
		display_storage_source = getattr(self.source_cart, 'display_storage_source')()
		self._notice = getattr(self.source_cart, 'get_notice')()

		if display_storage_source['result'] != 'success':
			self.save_notice()
			return display_storage_source

		# target_cart = self.get_target_cart(router)
		display_storage_target = getattr(self.target_cart, 'display_storage_target')()
		self._notice = getattr(self.target_cart, 'get_notice')()
		if display_storage_target['result'] != 'success':
			self.save_notice()
			return display_storage_target

		getattr(self.router, 'set_notice')(self._notice)
		display_storage = getattr(self.router, 'display_storage')()
		self._notice = getattr(self.router, 'get_notice')()
		if display_storage['result'] != 'success':
			self.save_notice()
			return display_storage

		if not self.save_notice():
			return response_error()
		return response_success()

	def setup_target(self, data):
		self.init_cart()
		src_cart_type = self._notice['src']['cart_type']
		src_setup_type = getattr(self.router, 'source_cart_setup')(src_cart_type)
		target_cart_type = data.get('target_cart_type', '')
		target_setup_type = getattr(self.router, 'target_cart_setup')(target_cart_type)

		target_cart_url = data.get('target_cart_url', '')
		target_cart_url = getattr(self.get_target_cart(), 'format_url')(target_cart_url)
		self._notice['target']['cart_type'] = target_cart_type
		self._notice['target']['cart_url'] = target_cart_url
		# self.target_cart = self.get_target_cart()
		getattr(self.get_router(), 'set_notice')(self._notice)
		getattr(self.get_source_cart(), 'set_notice')(self._notice)
		getattr(self.get_target_cart(), 'set_notice')(self._notice)

		prepare_display_setup_target = getattr(self.target_cart, 'prepare_display_setup_target')(data)
		self._notice = getattr(self.get_target_cart(), 'get_notice')()
		if prepare_display_setup_target['result'] != 'success':
			if target_setup_type == 'connector' or (target_setup_type == 'module_connector' and self._notice['target']['config'].get('type_upload') == 'connector'):
				prepare_display_setup_target['target_connector'] = False
				# self.save_notice()
			return prepare_display_setup_target
		# target_cart = self.get_target_cart(router)
		# getattr(self.target_cart, 'set_notice')(self._notice)
		self.reset_cart()
		display_setup_target = getattr(self.target_cart, 'display_setup_target')(data)
		self._notice = getattr(self.target_cart, 'get_notice')()
		if display_setup_target['result'] != 'success':
			# self.save_notice()
			return display_setup_target

		getattr(self.router, 'set_notice')(self._notice)

		prepare_display_config = getattr(self.router, 'prepare_display_config')()
		self._notice = getattr(self.router, 'get_notice')()
		if prepare_display_config['result'] != 'success':
			# self.save_notice()
			return prepare_display_config

		getattr(self.get_source_cart(), 'set_notice')(self._notice)
		error_response = response_error('There is an error with your Source Cart connection. Please try again later!', '#source-cart-url', 'General error')
		if src_setup_type in ['connector', 'database']:
			error_response = response_error('There is an error when reading your Source Cart Database.', '#source-cart-url', 'Database error')
		if src_setup_type == 'api':
			error_response = response_error('There is an error when getting source data.', '#source-cart-url', 'Api error')
		try:
			display_config_source = getattr(self.source_cart, 'display_config_source')()
		except Exception:
			self.log_traceback()
			return error_response
		self._notice = getattr(self.source_cart, 'get_notice')()

		if display_config_source['result'] != 'success':
			return error_response
		getattr(self.target_cart, 'set_notice')(self._notice)
		error_response = response_error('There is an error with your Target Cart connection. Please try again later!', '#target-cart-url', 'General error')
		if target_setup_type in ['connector', 'database']:
			error_response = response_error('There is an error when reading your Target Cart Database.', '#target-cart-url', 'Database error')
		if target_setup_type == 'api':
			error_response = response_error('There is an error when getting Target data.', '#target-cart-url', 'Api error')
		try:
			display_config_target = getattr(self.target_cart, 'display_config_target')()
		except Exception:
			self.log_traceback()
			return error_response
		self._notice = getattr(self.target_cart, 'get_notice')()
		if display_config_target['result'] != 'success':
			return error_response

		getattr(self.router, 'set_notice')(self._notice)
		display_config = getattr(self.router, 'display_config')()
		self._notice = getattr(self.router, 'get_notice')()
		if display_config['result'] != 'success':
			# self.save_migration()
			return display_config

		getattr(self.source_cart, 'set_notice')(self._notice)
		if src_setup_type in ['connector', 'database']:
			try:
				display_import_source = getattr(self.source_cart, 'display_import_source')()
				self._notice = getattr(self.source_cart, 'get_notice')()
				if display_import_source['result'] != 'success':
					self._notice['src']['count_error'] = True
			except Exception:
				self.log_traceback()
				self._notice['src']['count_error'] = True

				# error_response = response_error('There is an error when reading your Source Cart Database.', '#source-cart-url', 'Database error')
				# return error_response
		# getattr(self.target_cart, 'set_notice')(self._notice)
		# display_import_target = getattr(self.target_cart, 'display_import_target')()
		# if display_import_target['result'] != 'success':
		# 	return display_import_target
		# self._notice = getattr(self.target_cart, 'get_notice')()
		# getattr(self.router, 'set_notice')(self._notice)
		# getattr(self.router, 'display_import')()
		# self._notice = getattr(self.router, 'get_notice')()
		return response_success()

	def config(self, data, conn = True):
		try:
			self.init_cart()

			if not self._notice['config'].get('recent'):
				prepare_display_confirm = getattr(self.router, 'prepare_display_confirm')(data)
				self._notice = getattr(self.router, 'get_notice')()
				if prepare_display_confirm['result'] != 'success':
					return response_from_subprocess(prepare_display_confirm, conn)
			getattr(self.get_source_cart(), 'set_notice')(self._notice)

			display_confirm_source = getattr(self.source_cart, 'display_confirm_source')()
			self._notice = getattr(self.source_cart, 'get_notice')()
			if display_confirm_source['result'] != 'success':
				return response_from_subprocess(display_confirm_source, conn)

			self.target_cart = self.get_target_cart()
			getattr(self.target_cart, 'set_notice')(self._notice)

			display_confirm_target = getattr(self.target_cart, 'display_confirm_target')()
			self._notice = getattr(self.target_cart, 'get_notice')()
			if display_confirm_target['result'] != 'success':
				return response_from_subprocess(display_confirm_target, conn)

			display_confirm = getattr(self.router, 'display_confirm')()
			self._notice = getattr(self.router, 'get_notice')()
			if display_confirm['result'] != 'success':
				return response_from_subprocess(display_confirm, conn)
			setup_src = self.setup_source_cart(self._notice['src']['cart_type'])
			if (setup_src['setup_type'] != 'file' or self._notice['config']['recent']) and (data and not data.get('reconfig')):
				display_import_source = getattr(self.source_cart, 'display_import_source')()
				if display_import_source['result'] != 'success':
					return response_from_subprocess(display_import_source, conn)
				self._notice = getattr(self.source_cart, 'get_notice')()
				getattr(self.target_cart, 'set_notice')(self._notice)
				display_import_target = getattr(self.target_cart, 'display_import_target')()
				if display_import_target['result'] != 'success':
					return response_from_subprocess(display_import_target, conn)
				self._notice = getattr(self.target_cart, 'get_notice')()
				getattr(self.router, 'set_notice')(self._notice)
				display_import = getattr(self.router, 'display_import')()
				if display_import['result'] != 'success':
					return response_from_subprocess(display_import, conn)
				self._notice = getattr(self.router, 'get_notice')()
			if self._notice['config']['clear_shop']:
				self._notice['start_msg'] = getattr(self.router, 'console_success')('Clearing store ...')
			else:
				self._notice['start_msg'] = getattr(self.router, 'get_msg_start_import')('taxes')
			getattr(self.router, 'set_notice')(self._notice)
			self.save_notice()
			return response_from_subprocess(response_success(), conn)
		except Exception:
			self.log_traceback()
			return response_from_subprocess(response_error(), conn)

	def storage_data(self):
		self.init_cart()
		if self._notice['src']['storage']['result'] == 'success':
			return response_success()
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# source_cart = self.get_source_cart(cart)
		if not self.get_source_cart():
			result['result'] = 'success'
			return result
		try:
			storage = getattr(self.get_source_cart(), 'storage_data')()
		except:
			self.log_traceback()
			return {
				'result': 'stop',
				'msg': 'error storage data'
			}
		self._notice = getattr(self.source_cart, 'get_notice')()
		if storage['result'] == 'success':
			try:
				getattr(self.get_source_cart(), 'finish_storage_data')()
				self._notice = getattr(self.get_source_cart(), 'get_notice')()
			except:
				self.log_traceback()
		save_notice = self.save_notice()
		# if not save_notice:
		# 	result['result'] = 'error'
		# 	result['msg'] = 'error save notice2'
		#
		# 	return result
		return storage

	def clear(self):
		self.init_cart()
		if self._notice['target']['clear']['result'] == 'success' or self._notice['config'].get('recent') or self._notice['config'].get('add_new'):
			return response_success()
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# self.init_cart()
		# target_cart = self.get_target_cart(cart)
		if not self.get_target_cart():
			result['result'] = 'success'
			return result
		clear_data = getattr(self.target_cart, 'clear_data')()
		self._notice = getattr(self.target_cart, 'get_notice')()
		if clear_data['result'] == 'success' and self._notice['config']['taxes']:
			self.source_cart = self.get_source_cart()
			if not self.source_cart:
				result['result'] = 'success'
				return result
			prepare_souce = getattr(self.source_cart, 'prepare_taxes_export')()
			self._notice = getattr(self.source_cart, 'get_notice')()
			getattr(self.get_target_cart(), 'set_notice')(self._notice)
			prepare_target = getattr(self.target_cart, 'prepare_taxes_import')()
			self._notice = getattr(self.target_cart, 'get_notice')()
			self._notice['process']['taxes']['time_start'] = time.time()

			self._notice['resume']['type'] = 'taxes'

		save_notice = self.save_notice()
		if not save_notice:
			return response_error()
		return clear_data

	def clear_demo(self):
		self.init_cart()
		if self._notice['target']['clear_demo']['result'] == 'success':
			return response_success()
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# self.init_cart()
		# target_cart = self.get_target_cart(cart)
		if not self.get_target_cart():
			result['result'] = 'success'
			return result
		clear_data = getattr(self.target_cart, 'clear_demo')()
		self._notice = getattr(self.target_cart, 'get_notice')()

		save_notice = self.save_notice()
		if not save_notice:
			return response_error()
		return clear_data

	def prepare_migration(self):
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		self.init_cart()
		# source_cart = self.get_source_cart(cart)
		if not self.get_source_cart():
			result['result'] = 'success'
			return result
		if self._notice['config'].get('reset'):
			prepare_display_setup_source = getattr(self.source_cart, 'prepare_display_setup_source')()
			if prepare_display_setup_source['result'] == 'success':
				self._notice = getattr(self.source_cart, 'get_notice')()
			if not self.get_target_cart():
				result['result'] = 'success'
				return result
			getattr(self.get_target_cart(), 'set_notice')(self._notice)
			prepare_display_setup_target = getattr(self.target_cart, 'prepare_display_setup_target')()
			if prepare_display_setup_target['result'] == 'success':
				self._notice = getattr(self.target_cart, 'get_notice')()
				getattr(self.source_cart, 'set_notice')(self._notice)
		prepare_import_source = getattr(self.source_cart, 'prepare_import_source')()
		if prepare_import_source['result'] != 'success':
			return prepare_import_source
		self._notice = getattr(self.source_cart, 'get_notice')()
		# target_cart = self.get_target_cart(cart)
		if not self.get_target_cart():
			result['result'] = 'success'
			return result
		getattr(self.target_cart, 'set_notice')(self._notice)
		prepare_target_cart = getattr(self.target_cart, 'prepare_import_target')()
		if prepare_target_cart['result'] != 'success':
			return prepare_target_cart
		self._notice = getattr(self.target_cart, 'get_notice')()
		getattr(self.router, 'set_notice')(self._notice)
		getattr(self.router, 'prepare_import')()
		self._notice = getattr(self.router, 'get_notice')()
		# self._notice['resume']['process'] = 'import'
		save_notice = self.save_notice()
		if not save_notice:
			return response_error()
		return response_success()

	def display_recent(self, data):
		self.init_cart()
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# self.init_cart()
		# source_cart = self.get_source_cart(cart)
		if not self.get_source_cart():
			result['result'] = 'success'
			response_from_subprocess(result)
			return
		prepare_display_import_source = getattr(self.source_cart, 'prepare_display_import_source')()
		if prepare_display_import_source['result'] != 'success':
			response_from_subprocess(prepare_display_import_source)
			return
		self._notice = getattr(self.source_cart, 'get_notice')()
		display_import_source = getattr(self.source_cart, 'display_import_source')()
		if display_import_source['result'] != 'success':
			response_from_subprocess(display_import_source)
			return
		after_display_import_source = getattr(self.source_cart, 'after_display_import_source')()
		if after_display_import_source['result'] != 'success':
			response_from_subprocess(after_display_import_source)
			return

		self._notice = getattr(self.source_cart, 'get_notice')()
		# target_cart = self.get_target_cart(cart)
		if not self.get_target_cart():
			result['result'] = 'success'
			response_from_subprocess(result)
			return
		getattr(self.target_cart, 'set_notice')(self._notice)
		display_import_target = getattr(self.target_cart, 'display_import_target')()
		if display_import_target['result'] != 'success':
			response_from_subprocess(display_import_target)
			return
		self._notice = getattr(self.target_cart, 'get_notice')()
		getattr(self.router, 'set_notice')(self._notice)
		getattr(self.router, 'display_import')()
		self._notice = getattr(self.router, 'get_notice')()
		if self._notice['config'].get('update_latest_data'):
			display_update_source = getattr(self.source_cart, 'display_update_source')()
			if display_update_source['result'] != 'success':
				response_from_subprocess(display_update_source)
				return
			self._notice = getattr(self.source_cart, 'get_notice')()
			# target_cart = self.get_target_cart(cart)
			if not self.get_target_cart():
				result['result'] = 'success'
				response_from_subprocess(result)
				return
			getattr(self.target_cart, 'set_notice')(self._notice)
			display_update_target = getattr(self.target_cart, 'display_update_target')()
			if display_update_target['result'] != 'success':
				response_from_subprocess(display_update_target)
				return
			self._notice = getattr(self.target_cart, 'get_notice')()
			getattr(self.router, 'set_notice')(self._notice)
			getattr(self.router, 'display_update')()
			self._notice = getattr(self.router, 'get_notice')()
		if self._notice['config']['clear_shop']:
			self._notice['start_msg'] = getattr(self.router, 'console_success')('Clearing store ...')
		else:
			self._notice['start_msg'] = getattr(self.router, 'get_msg_start_import')('taxes')
		save_notice = self.save_notice()
		if not save_notice:
			response_from_subprocess(response_error())
			return
		response_from_subprocess(response_success())
		return

	def display_import(self):
		self.init_cart()
		setup_src = self.setup_source_cart(self._notice['src']['cart_type'])
		if setup_src['setup_type'] != 'file':
			return response_success()
		result = self.default_result_migration()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# self.init_cart()
		# source_cart = self.get_source_cart(cart)
		if not self.get_source_cart():
			result['result'] = 'success'
			return result
		display_import_source = getattr(self.source_cart, 'display_import_source')()
		if display_import_source['result'] != 'success':
			return display_import_source
		self._notice = getattr(self.source_cart, 'get_notice')()
		# target_cart = self.get_target_cart(cart)
		if not self.get_target_cart():
			result['result'] = 'success'
			return result
		getattr(self.target_cart, 'set_notice')(self._notice)
		display_import_target = getattr(self.target_cart, 'display_import_target')()
		if display_import_target['result'] != 'success':
			return display_import_target
		self._notice = getattr(self.target_cart, 'get_notice')()
		getattr(self.router, 'set_notice')(self._notice)
		getattr(self.router, 'display_import')()
		self._notice = getattr(self.router, 'get_notice')()
		if self._notice['config']['clear_shop']:
			self._notice['start_msg'] = getattr(self.router, 'console_success')('Clearing store ...')
		else:
			self._notice['start_msg'] = getattr(self.router, 'get_msg_start_import')('taxes')
		save_notice = self.save_notice()
		if not save_notice:
			return response_error()
		return response_success()

	def migration(self):
		result = self.default_result_migration()
		current = self._notice['resume']['type']
		if not current:
			current = 'taxes'
		self.init_cart()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		if not self._notice:
			result['result'] = 'success'
			result['msg'] = 'Finish Migration!'
			return result
		result['result'] = 'process'
		result['process']['next'] = current
		self._notice['resume']['type'] = current
		if not (self._notice['config'].get(current)):
			next_action = self._import_next_action[current]
			if next_action:
				if (not (next_action in self._notice['config'])) or (self._notice['config'][next_action]):
					# source_cart = self.get_source_cart(cart)
					# target_cart = self.get_target_cart(cart)
					if (not self.get_source_cart()) or (not self.get_target_cart()):
						result['result'] = 'success'
						result['msg'] = 'Finish Migration!'
						return result
					fn_prepare_source = 'prepare_' + next_action + '_export'
					fn_prepare_target = 'prepare_' + next_action + '_import'
					prepare_source = getattr(self.source_cart, fn_prepare_source)()
					self._notice = getattr(self.source_cart, 'get_notice')()
					getattr(self.target_cart, 'set_notice')(self._notice)
					prepare_target = getattr(self.target_cart, fn_prepare_target)()
					self._notice = getattr(self.target_cart, 'get_notice')()
				self._notice['process'][next_action]['time_start'] = to_int(time.time())
				self._notice['process'][next_action]['time_finish'] = 0
				self._notice['resume']['type'] = next_action
				result['process']['next'] = next_action

			else:

				result['result'] = 'success'
				result['msg'] = ''
			save_notice = self.save_notice()
			# if not save_notice:
			# 	result['result'] = 'error'
			# 	result['msg'] = 'error save notice'
			# 	return result
			save_recent = self.save_recent()
			# if not save_recent:
			# 	result['result'] = 'error'
			# 	result['msg'] = 'error save recent'
			#
			# 	return result
			return result
		total = to_int(self._notice['process'][current]['total'])
		imported = to_int(self._notice['process'][current]['imported'])
		imported = to_int(imported)
		error = self._notice['process'][current]['error']
		error = to_int(error)
		id_src = self._notice['process'][current]['id_src']
		simple_action = self._import_simple_action[current]
		next_action = self._import_next_action[current]
		self.get_source_cart()
		self.get_target_cart()
		if imported < total:

			# source_cart = self.get_source_cart(cart)
			# target_cart = self.get_target_cart(cart)
			if (not self.get_source_cart()) or (not self.get_target_cart()):
				result['result'] = 'success'
				result['msg'] = 'Finish Migration!'
				return result
			fn_get_main = getattr(self.source_cart, 'get_' + current + '_main_export')
			fn_get_ext = getattr(self.source_cart, 'get_' + current + '_ext_export')
			fn_convert_export = getattr(self.source_cart, 'convert_' + simple_action + '_export')
			fn_get_id = getattr(self.source_cart, 'get_' + simple_action + '_id_import')
			fn_check_import = getattr(self.target_cart, 'check_' + simple_action + '_import')
			fn_router_import = getattr(self.target_cart, 'router_' + simple_action + '_import')
			fn_before_import = getattr(self.target_cart, 'before_' + simple_action + '_import')
			fn_import = getattr(self.target_cart, simple_action + '_import')
			fn_after_import = getattr(self.target_cart, 'after_' + simple_action + '_import')
			fn_addition_import = getattr(self.target_cart, 'addition_' + simple_action + '_import')
			log_times = list()
			try:
				start_time = time.time()
				mains = fn_get_main()

				if mains['result'] != 'success':
					if mains['result'] == 'pass':
						result = self.next_migration(current, next_action)
						return result
					result = self.default_result_migration()
					if self._retry <= 10:
						time.sleep(self._retry * 10)
						self.log('get main error, sleep ' + to_str(self._retry * 10) + 's', 'mains')
						self._retry += 1
						return mains
					else:
						self._retry = 0
						result['result'] = 'stop_export'
						result['msg'] = mains.get('msg') if mains.get('msg') else 'get main error'
						return result
				if ('data' not in mains) or (not isinstance(mains['data'], list) or (not mains['data'])):
					if self._notice['src'].get('setup_type') == 'api':
						if self._retry >= 5:
							result = self.default_result_migration()
							result['result'] = 'stop_export'
							result['msg'] = mains.get('msg') if mains.get('msg') else 'get ext error'
							return result
						self._retry += 1
						time.sleep(60)
						result = self.default_result_migration()
						result['result'] = 'process'
						return result
					else:
						result = self.next_migration(current, next_action)
						return result
				if self._notice['src'].get('setup_type') == 'api':
					self._retry = 0
				ext = fn_get_ext(mains)
				log_times.append('request source ' + to_str(time.time() - start_time) + 's')
				if ext['result'] != 'success':
					result = self.default_result_migration()
					self.log('get ext error', 'ext')
					result['result'] = 'stop_export'
					result['msg'] = ext.get('msg') if ext.get('msg') else 'get ext error'
					return result
			except Exception:
				self.log_traceback()
				if self._notice['config'].get('stop_on_error'):
					result = self.default_result_migration()
					result['result'] = 'stop'
					return result
				else:
					result = self.default_result_migration()
					result['result'] = 'stop_export'
					return result
			# result = self.next_migration(current, next_action)
			# return result
			# notify error
			if mains.get('type'):
				self._notice['process'][current]['type'] = mains.get('type')
			for main in mains['data']:
				path_img_log = get_pub_path() + '/log/' + to_str(self._migration_id) + '/time_images.log'
				if os.path.isfile(path_img_log):
					os.remove(path_img_log)
				id_src = fn_get_id(None, main, ext)
				try:
					if imported >= total:
						break
					imported += 1
					if current == 'categories':
						fn_prepare = getattr(self.source_cart, 'prepare_convert_' + simple_action + '_export')
						fn_prepare()
					start_time = time.time()
					convert = fn_convert_export(main, ext)
					if convert['result'] == 'skip':
						continue
					if convert['result'] == 'error':
						error += 1
						if not convert['msg']:
							convert['msg'] = "convert " + to_str(id_src) + " error"
						self.log(convert['msg'], current + '_errors')
						if self._notice['config'].get('stop_on_error'):
							result = self.default_result_migration()
							result['result'] = 'stop'
							return result
						continue
					if convert['result'] == 'warning':

						if not convert['msg']:
							convert['msg'] = "convert " + to_str(id_src) + " error"
						result['msg'] += convert['msg']
						self.log(convert['msg'], current + '_errors')
						if self._notice['config'].get('stop_on_error'):
							result = self.default_result_migration()
							result['result'] = 'stop'
							return result
						continue
					if convert['result'] == 'pass':
						continue
					convert_data = convert['data']
					check_import = fn_check_import(convert_data, main, ext)
					if check_import:
						if self._notice['config'].get('update_latest_data') and self._notice['target']['config'].get('entity_update', dict()).get(current):
							fn_update_name = 'update_latest_data_' + simple_action
							if hasattr(self.target_cart, fn_update_name):
								fn_update_import = getattr(self.target_cart, fn_update_name)
								fn_update_import(check_import, convert_data, main, ext)
						continue
					getattr(self.get_target_cart(), 'set_convert_data')(convert_data)
					import_data = fn_import(convert_data, main, ext)
					if import_data['result'] != 'success':
						msg = import_data.get('msg')
						if not msg:
							msg = "import " + to_str(id_src) + " error"
						self.log(msg, current + '_errors')

					if import_data['result'] == 'skip_error':
						error += 1
						continue

					if import_data['result'] == 'error':
						error += 1
						if self._notice['config'].get('stop_on_error'):
							result = self.default_result_migration()
							result['result'] = 'stop'
							return result
						continue
					if import_data['result'] == 'warning':
						if self._notice['config'].get('stop_on_error'):
							result = self.default_result_migration()
							result['result'] = 'stop'
							return result
						result['msg'] += import_data['msg'] if isinstance(import_data['msg'], str) and import_data[
							'msg'] else ''
						error += 1
						continue
					id_desc = import_data['data']
					if isinstance(id_desc, list):
						for id_desc_row in id_desc:
							if id_desc_row['result'] != 'success':
								error += 1
								result['msg'] += import_data['msg']
								break
							id_desc_data = id_desc_row['data']
							after_import = fn_after_import(id_desc_data, convert_data, main, ext)
							if after_import['result'] == 'error':
								return after_import
							if after_import['result'] == 'success' and after_import['msg']:
								result['msg'] += after_import['msg']
					else:
						after_import = fn_after_import(id_desc, convert_data, main, ext)
						if after_import['result'] == 'error':
							return after_import
						if after_import['result'] == 'success' and after_import['msg']:
							result['msg'] += after_import['msg']
					log_times.append(current + ' id ' + to_str(id_src) + ': ' + 'request target ' + to_str(time.time() - start_time) + 's')
				except Exception:
					self.log_traceback(current + '_errors', id_src)
					if self._notice['config'].get('stop_on_error'):
						result = self.default_result_migration()
						result['result'] = 'stop'
						return result
					error += 1
					continue

			result['process']['type'] = current
			self.log_time(log_times)
			self._notice['process'][current]['imported'] = imported
			self._notice['process'][current]['error'] = error
			key_id_src = 'id_src'
			if self._notice['src']['cart_type'] == 'shopify' and current == 'categories':
				if self._notice['process'][current].get('type') == getattr(self.source_cart, 'TYPE_SMART_COLLECTION'):
					key_id_src = 'id_src_smart'
			self._notice['process'][current][key_id_src] = id_src

			response_types = ['total', 'imported', 'error', 'point']
			for response_type in response_types:
				result['process'][response_type] = self._notice['process'][current][response_type]
		else:
			if hasattr(self.get_source_cart(), 'finish_' + simple_action + '_export'):
				try:
					finish_import = getattr(self.get_source_cart(), 'finish_' + simple_action + '_export')()
					self._notice = getattr(self.source_cart, 'get_notice')()
					getattr(self.target_cart, 'set_notice')(self._notice)

				except Exception:
					self.log_traceback()
			if hasattr(self.get_target_cart(), 'finish_' + simple_action + '_import'):
				try:
					finish_import = getattr(self.get_target_cart(), 'finish_' + simple_action + '_import')()
					self._notice = getattr(self.target_cart, 'get_notice')()
				except Exception:
					self.log_traceback()
			if current != 'attributes':
				self.log('Migration of ' + current.capitalize() + ' is completed', 'process')
			result = self.next_migration(current, next_action)
		# time_finish = time.time()
		# self._notice['process'][current]['time_finish'] = time_finish
		# msg_time = self.create_time_to_show(int(time_finish) - to_int(self._notice['process'][current]['time_start']))
		# if current != 'attributes':
		# 	result['msg'] = 'Finished importing ' + to_str(self._notice['process'][current]['total']) + ' ' + current + '! Run time: ' + msg_time
		# if next_action:
		# 	if self._notice['config'][next_action]:
		# 		source_cart = self.get_source_cart(cart)
		# 		target_cart = self.get_target_cart(cart)
		# 		if (not source_cart) or (not target_cart):
		# 			result['result'] = 'success'
		# 			result['msg'] = 'Finish Migration!'
		# 			return result
		# 		source_cart = self.get_source_cart(cart)
		# 		target_cart = self.get_target_cart(cart)
		# 		if (not source_cart) or (not target_cart):
		# 			result['result'] = 'success'
		# 			result['msg'] = 'Finish Migration!'
		# 			return result
		# 		fn_prepare_source = 'prepare_' + next_action + '_export'
		# 		fn_prepare_target = 'prepare_' + next_action + '_import'
		# 		prepare_source = getattr(source_cart, fn_prepare_source)()
		# 		self._notice = getattr(source_cart, 'get_notice')()
		# 		getattr(target_cart, 'set_notice')(self._notice)
		# 		prepare_target = getattr(target_cart, fn_prepare_target)()
		# 		self._notice = getattr(target_cart, 'get_notice')()
		# 	self._notice['process'][next_action]['time_start'] = time.time()
		# 	self._notice['resume']['type'] = next_action
		# 	result['process']['next'] = next_action
		#
		# else:
		# 	# self._notice['running'] = False
		# 	result['result'] = 'success'
		if getattr(self.source_cart, 'get_stop_action')() or getattr(self.target_cart, 'get_stop_action')():
			result['result'] = 'stop_export'
			if getattr(self.source_cart, 'get_stop_action')():
				msg = 'src stop action'
			else:
				msg = 'target stop action'
			result['msg'] = msg
			return result


		clear_entity_warning = getattr(self.source_cart, 'get_clear_entity_warning')() and getattr(self.target_cart, 'get_clear_entity_warning')()
		save_notice = self.save_notice(clear_entity_warning = clear_entity_warning)
		# if not save_notice:
		# 	result['result'] = 'error'
		# 	result['msg'] = 'error save notice2'
		#
		# 	return result
		save_recent = self.save_recent()
		# if not save_recent:
		# 	result['result'] = 'error'
		# 	result['msg'] = 'error save recent2'
		#
		# 	return result
		return result

	def recent(self, data):
		migration_id = data.get('migration_id') if data else False
		if not migration_id:
			response_from_subprocess(response_error('Data invalid'))
		self.set_migration_id(migration_id)
		self._notice = None
		self.init_cart()
		last_full_mig_notice = None
		if self._notice['resume']['process'] == 'completed' and self._notice['mode'] == MIGRATION_FULL and self._notice['finish']:
			last_full_mig_notice = copy.deepcopy(self._notice)
		self._notice['resume']['process'] = 'configuring'
		status = STATUS_CONFIGURING
		self._notice['resume']['type'] = 'taxes'
		self._notice['resume']['action'] = 'storage_data'
		self._notice['config']['recent'] = True
		if data.get('update'):
			self._notice['config']['update_latest_data'] = True
		else:
			self._notice['config']['update_latest_data'] = False

		self._notice['config']['add_new'] = True
		self._notice['config']['clear_shop'] = False
		self._notice['config']['remigrate'] = False
		self._notice['config']['reset'] = False
		self._notice['log_start'] = False
		self._notice['running'] = False
		self._notice['finish'] = False
		if self._notice['src'].get('setup_type') != 'file':
			self._notice['resume']['process'] = 'payment'
			status = STATUS_PAYMENT
		extend_data = {
			'status': status
		}
		if last_full_mig_notice:
			extend_data['last_full_mig_notice'] = last_full_mig_notice
		update = self.save_migration(extend_data = extend_data)
		response_from_subprocess(update)
		return

	def remigrate(self, data):
		migration_id = data.get('migration_id') if data else False
		if not migration_id:
			response_from_subprocess(response_error('Data invalid'))
		self.set_migration_id(migration_id)
		self._notice = None
		self.init_cart()
		last_full_mig_notice = None
		if self._notice['resume']['process'] == 'completed' and self._notice['mode'] == MIGRATION_FULL and self._notice['finish']:
			last_full_mig_notice = copy.deepcopy(self._notice)
		self._notice['resume']['process'] = 'new'
		self._notice['resume']['type'] = 'taxes'
		self._notice['config']['clear_shop'] = False
		self._notice['config']['recent'] = False
		self._notice['config']['reset'] = False
		self._notice['config']['remigrate'] = True
		self._notice['log_start'] = False
		self._notice['running'] = False
		self._notice['finish'] = False
		extend_data = {
			'status': STATUS_NEW
		}
		if last_full_mig_notice:
			extend_data['last_full_mig_notice'] = last_full_mig_notice
		update = self.save_migration(extend_data = extend_data)
		response_from_subprocess(update)
		return

	def construct_log_time(self):
		return {
			'get_main': list(),
			'get_ext': list(),
			'convert': list(),
			'import': list(),
			'after_import': list()
		}

	def log_time(self, log_times):
		file_log = get_pub_path() + '/log/' + to_str(self._migration_id) + '/time_requests.log'
		if os.path.isfile(file_log):
			os.remove(file_log)
		for log_time in log_times:
			self.log(log_time, 'time_requests')

	def create_time_to_show(self, time_input):
		hour = to_int(gmdate("%H", time_input))
		minute = to_int(gmdate("%M", time_input))
		second = to_int(gmdate("%S", time_input))
		result = ''
		if hour and hour > 0:
			result += to_str(hour) + ' hours '
		if minute and minute > 0:
			result += to_str(minute) + ' minutes '
		if second and second > 0:
			result += to_str(second) + ' seconds'
		return result

	def get_current(self):
		return self._current_import_action

	def set_current(self, current):
		self._current_import_action = current

	def stop(self):
		self._exit_flag = 1

	def set_exit_flag(self, exit_flag):
		self._exit_flag = exit_flag

	# def default_result_migration(self):
	# 	return {
	# 		'result': '',
	# 		'msg': '',
	# 		'process': {
	# 			'next': '',
	# 			'total': 0,
	# 			'imported': 0,
	# 			'error': 0,
	# 			'point': 0,
	# 		}
	# 	}

	def finish_migration(self):
		self.init_cart()
		# cart = get_model('basecart')
		# getattr(cart, 'set_migration_id')(self._migration_id)
		# getattr(cart, 'set_notice')(self._notice)
		prepare_display_finish = getattr(self.router, 'prepare_display_finish')()
		if prepare_display_finish['result'] != 'success':
			return prepare_display_finish
		self._notice = getattr(self.router, 'get_notice')()

		# source_cart = self.get_source_cart()
		getattr(self.get_source_cart(), 'set_notice')(self._notice)
		display_finish_source = getattr(self.source_cart, 'display_finish_source')()
		if display_finish_source['result'] != 'success':
			return display_finish_source
		self._notice = getattr(self.source_cart, 'get_notice')()

		# target_cart = self.get_target_cart()
		getattr(self.get_target_cart(), 'set_notice')(self._notice)
		display_finish_target = getattr(self.target_cart, 'display_finish_target')()
		if display_finish_target['result'] != 'success':
			return display_finish_target
		self._notice = getattr(self.target_cart, 'get_notice')()

		getattr(self.router, 'set_notice')(self._notice)
		display_finish = getattr(self.router, 'display_finish')()
		if display_finish['result'] != 'success':
			return display_finish
		self._notice = getattr(self.router, 'get_notice')()
		self._notice['running'] = False
		self._notice['finish'] = True
		if self._notice['resume']['process'] == 'completed':
			status = STATUS_COMPLETED
		else:
			status = STATUS_PAYMENT
		self.log_finish()
		self.update_notice(self._migration_id, self._notice, None, None, status, True)
		for entity, process in self._notice['process'].items():
			if self._notice['config'].get(entity) and process['error'] and to_int(process['imported']) > 0 and (((to_int(process['error']) / to_int(process['imported'])) * 100) > 30):
				self.notify_error(entity)
				break
		if self._notice['config'].get('app_mode'):
			self.notify_app_mode()
		info = getattr(self.router, 'get_info_migration')(self._migration_id)
		if info and info['migration_group'] == GROUP_TEST:
			autotest = get_model('autotest')
			getattr(autotest, 'set_result_migration_test')(self._migration_id, getattr(autotest, 'RESULT_SUCCESS'))

			self.start_autotest(self._migration_id)

		return response_success()

	def notify_app_mode(self):
		dev_email = self.get_config_ini('sendgrid', 'email_to')
		if not dev_email:
			return
		email_content = self.get_content_mail_to_dev(action = self.ACTION_APP_MODE)
		email_content.append('Entity error/imported: ')
		for entity, process in self._notice['process'].items():
			if process['imported']:
				email_content.append(entity.capitalize() + ': ' + to_str(process['error']) + '/' + to_str(process['imported']))
		self.send_email(dev_email, '\n'.join(email_content), email_content[0])
		return

	def notify_error(self, entity_error):
		getattr(self.get_router(), 'create_demo_error')(self._notice)
		dev_email = self.get_config_ini('sendgrid', 'email_to')
		if not dev_email:
			return
		email_content = self.get_content_mail_to_dev(action = self.ACTION_COMPLETED, file_log = entity_error + '_errors')
		email_content.append('Entity error: ')
		for entity, process in self._notice['process'].items():
			if process['error']:
				email_content.append(entity.capitalize() + ': ' + to_str(process['error']) + '/' + to_str(process['imported']))
		if dev_email:
			self.send_email(dev_email, '\n'.join(email_content), email_content[0])
		return
	def notify_demo_error(self, error):
		dev_email = self.get_config_ini('sendgrid', 'email_to')
		if not dev_email:
			return
		email_content = self.get_content_mail_to_dev(action = self.ACTION_DEMO_ERROR)
		email_content.append("Error: " + error)

		if dev_email:
			self.send_email(dev_email, '\n'.join(email_content), email_content[0])
		return
	def next_migration(self, current, next_action):
		self.init_cart()
		# if not cart:
		# 	cart = get_model('basecart')
		# 	getattr(cart, 'set_migration_id')(self._migration_id)
		# 	getattr(cart, 'set_notice')(self._notice)

		result = self.default_result_migration()
		time_finish = time.time()
		self._notice['process'][current]['time_finish'] = to_int(time_finish)
		msg_time = self.create_time_to_show(int(time_finish) - to_int(self._notice['process'][current]['time_start']))
		if current != 'attributes':
			result['msg'] = 'Finished importing ' + to_str(
				self._notice['process'][current]['total']) + ' ' + current + '! Run time: ' + msg_time
		if next_action:
			if self._notice['config'].get(next_action):
				# source_cart = self.get_source_cart(cart)
				# target_cart = self.get_target_cart(cart)
				if (not self.source_cart) or (not self.target_cart):
					result['result'] = 'success'
					result['msg'] = 'Finish Migration!'
					return result
				# source_cart = self.get_source_cart(cart)
				# target_cart = self.get_target_cart(cart)
				# if (not source_cart) or (not target_cart):
				# 	result['result'] = 'success'
				# 	result['msg'] = 'Finish Migration!'
				# 	return result
				fn_prepare_source = 'prepare_' + next_action + '_export'
				fn_prepare_target = 'prepare_' + next_action + '_import'
				prepare_source = getattr(self.source_cart, fn_prepare_source)()
				self._notice = getattr(self.source_cart, 'get_notice')()
				getattr(self.target_cart, 'set_notice')(self._notice)
				prepare_target = getattr(self.target_cart, fn_prepare_target)()
				self._notice = getattr(self.target_cart, 'get_notice')()
			self._notice['process'][next_action]['time_start'] = time.time()
			self._notice['resume']['type'] = next_action
			result['process']['next'] = next_action

		else:
			# self._notice['running'] = False
			result['result'] = 'success'
		return result

	def is_stop_process(self):
		self.init_cart()
		return getattr(self.router, 'is_stop_flag')(self._migration_id)

	def get_action(self):
		resume_process = self._notice['resume'].get('action')
		if resume_process:
			return resume_process
		process = self._notice['resume']['process']
		if process in self._next_action:
			return process
		return 'storage_data'

	def summary_demo(self, data):
		summary = dict()
		self.init_cart()
		if to_int(self._notice['mode']) != MIGRATION_DEMO:
			response_success(summary)
			return
		# entities = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules']
		entities = ['products', 'customers', 'orders']
		for entity in entities:
			if not self._notice['config'][entity]:
				continue
			summary[entity] = getattr(self.get_router(), 'get_summary_demo_by_type')(self._import_simple_action[entity])
		response_from_subprocess(summary)
		return
