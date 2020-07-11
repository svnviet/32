from cartmigration.libs.base_model import BaseModel
from abc import abstractmethod
from cartmigration.libs.utils import *
class LeAbstractMigration(BaseModel):
	DEMO_INIT = 1
	DEMO_SKIP = 2
	DEMO_SUCCESS = 3
	MODE_LIVE = 'live'
	MODE_TEST = 'test'

	def __init__(self, data = None):
		super().__init__()
		self._migration_id = data.get('migration_id') if isinstance(data, dict) else None
		self._notice = None
		self._mode = None

	@abstractmethod
	def get_migration_notice(self, migration_id):
		pass

	@abstractmethod
	def delete_migration_notice(self, migration_id):
		pass

	@abstractmethod
	def update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False, clear_entity_warning = False):
		pass

	@abstractmethod
	def set_status_migration(self, migration_id, status):
		pass

	@abstractmethod
	def save_migration(self,migration_id, data):
		pass

	@abstractmethod
	def get_info_migration(self, migration_id):
		pass

	@abstractmethod
	def get_app_mode_limit(self):
		pass

	def get_default_notice(self):
		return {
			'src': {
				'cart_type': '',
				'setup_type': '',
				'cart_url': '',
				'config': {
					'cookie_key': '',
					'token': '',
					'version': '',
					'connector_version': '',
					'table_prefix': '',
					'charset': '',
					'image_category': '',
					'image_product': '',
					'image_manufacturer': '',
					'api': dict(),
					'folder': '',
					'file': dict(),
					'database': dict(),
					'upload_name': dict(),
					'extend': dict(),
					'seo_module': dict()
				},
				'site': dict(),
				'languages': dict(),
				'language_default': '',
				'category_data': dict(),
				'category_root': '',
				'store_category': dict(),
				'attributes': dict(),
				'order_status': dict(),
				'currencies': dict(),
				'currency_default': '',
				'countries': dict(),
				'customer_group': dict(),
				'storage': {
					'init': False,
					'result': 'process',
					'function': 'no_storage_data',
					'type': '',
					'msg': console_success("Preparing import data ..."),
					'count': 0,
				},
				'clear': {
					'result': 'process',
					'function': '_noClear',
					'table_index': 0,
					'msg': '',
					'limit': 20,
				},
				'support': {
					'languages_select': False,
					'site_select': False,
					'site_map': False,
					'language_map': True,
					'category_map': False,
					'attribute_map': False,
					'order_status_map': True,
					'currency_map': True,
					'country_map': False,
					'customer_group_map': True,
					'taxes': True,
					'manufacturers': True,
					'categories': True,
					'attributes': False,
					'products': True,
					'customers': True,
					'orders': True,
					'reviews': True,
					'pages': False,
					'blogs': False,
					'coupons': False,
					'cartrules': False,
					'add_new': True,
					'clear_shop': True,
					'img_des': True,
					'ignore_image': False,
					'smart_collection': True,
					'pre_prd': True,
					'real_pre_prd': True,
					'pre_cus': True,
					'pre_ord': True,
					'seo': True,
					'skip_demo': True,
					'seo_301': True,
					'strip_html': True,
					'cus_pass': False,
					'quotes': False,
					'newsletters': False,
					'auto_map': True,
					'multi_languages_select': False,
				},
				'extends': dict(),
				'languages_select': list(),
				'site_select': list()
			},
			'target': {
				'cart_type': '',
				'setup_type': '',

				'cart_url': '',
				'config': {
					'cookie_key': '',
					'token': '',
					'version': '',
					'connector_version': '',
					'table_prefix': '',
					'charset': '',
					'image_category': '',
					'image_product': '',
					'image_manufacturer': '',
					'api': dict(),
					'folder': '',
					'file': dict(),
					'database': dict(),
					'extend': dict(),
					'seo_module': dict(),
					'entity_update': {
						'taxes': False,
						'manufacturers': False,
						'categories': False,
						'attributes': False,
						'products': False,
						'customers': False,
						'orders': False,
						'reviews': False,
						'pages': False,
						'blogs': False,
						'coupons': False,
						'cartrules': False,
					}
				},
				'site': dict(),
				'languages': dict(),
				'language_default': '',
				'category_data': dict(),
				'category_root': '',
				'store_category': dict(),
				'attributes': dict(),
				'order_status': dict(),
				'currencies': dict(),
				'currency_default': '',
				'countries': dict(),
				'customer_group': dict(),
				'storage': {
					'result': 'process',
					'function': 'noStorageData',
					'type': '',
					'msg': console_success("Preparing import data ..."),
					'count': 0,
				},
				'clear': {
					'result': 'process',
					'function': 'no_clear',
					'msg': '',
				},
				'clear_demo': {
					'result'     : 'process',
					'function'   : 'no_clear_demo',
					'msg'        : '',
				},
				'support': {
					'languages_select': False,
					'site_select': False,
					'site_map': False,
					'language_map': True,
					'category_map': False,
					'attribute_map': False,
					'order_status_map': True,
					'currency_map': False,
					'country_map': False,
					'customer_group_map': True,
					'taxes': True,
					'manufacturers': True,
					'categories': True,
					'attributes': False,
					'products': True,
					'customers': True,
					'orders': True,
					'reviews': True,
					'pages': False,
					'blogs': False,
					'coupons': False,
					'cartrules': False,
					'add_new': True,
					'clear_shop': True,
					'img_des': False,
					'ignore_image': False,
					'pre_prd': True,
					'real_pre_prd': True,
					'pre_cus': True,
					'pre_ord': True,
					'seo': False,
					'product_customer_group': False,
					'skip_demo': True,
					'seo_301': False,
					'strip_html': True,
					'cus_pass': False,
					'smart_collection': False,
					'update_latest_data': False,
					'quotes': False,
					'newsletters': False,
					'multi_languages_select': False,

				},
				'extends': dict(),
				'disable_reload':
					{
						'site': False,
						'languages': False,
						'category_data': False,
						'attributes': False,
						'currencies': False,
						'order_status': False,
						'customer_group': False,
					}
			},
			'support': {
				'languages_select': False,
				'site_select': False,
				'site_map': False,
				'language_map': False,
				'category_map': False,
				'attribute_map': False,
				'order_status_map': False,
				'currency_map': False,
				'country_map': False,
				'customer_group_map': False,
				'taxes': False,
				'manufacturers': False,
				'categories': False,
				'attributes': False,
				'products': False,
				'customers': False,
				'orders': False,
				'reviews': False,
				'pages': False,
				'blogs': False,
				'coupons': False,
				'cartrules': False,
				'add_new': True,
				'clear_shop': True,
				'img_des': False,
				'ignore_image': False,
				'pre_prd': True,
				'real_pre_prd': True,
				'pre_cus': True,
				'pre_ord': True,
				'seo': True,
				'skip_demo': True,
				'seo_301': False,
				'strip_html': True,
				'cus_pass': False,
				'smart_collection': False,
				'quotes': False,
				'newsletters': False,
				'multi_languages_select': False,

			},
			'map': {
				'site': dict(),
				'languages': dict(),
				'category_data': dict(),
				'attributes': dict(),
				'currencies': dict(),
				'order_status': dict(),
				'customer_group': dict(),
			},
			'config': {
				'taxes': False,
				'manufacturers': False,
				'categories': False,
				'attributes': False,
				'products': False,
				'customers': False,
				'orders': False,
				'reviews': False,
				'pages': False,
				'blogs': False,
				'coupons': False,
				'cartrules': False,
				'add_new': False,
				'clear_shop': False,
				'img_des': False,
				'ignore_image': False,
				'pre_prd': False,
				'real_pre_prd': False,
				'pre_cus': False,
				'pre_ord': False,
				'seo': False,
				'seo_301': False,
				'seo_plugin': '',
				'skip_demo': False,
				'recent': False,
				'remigrate': False,
				'url_app': '',
				'demo_store': False,
				'strip_html': False,
				'stop_on_error': False,
				'smart_collection': False,
				'app_mode': False,
				'update_latest_data': False,
				'quotes': False,
				'newsletters': False,
				'cus_pass': False
			},
			# 'option_help': {
			# 	'add_new': None,
			# 	'clear_shop': None,
			# 	'img_des': None,
			# 	'ignore_image': None,
			# 	'pre_prd': None,
			# 	'real_pre_prd': None,
			# 	'pre_cus': None,
			# 	'pre_ord': None,
			# 	'seo': None,
			# 	'seo_301': None,
			# 	'strip_html': None,
			# },
			# 'init': {
			# 	'running': False,
			# 	'resume': {
			# 		'process': 'clear',
			# 		'type': ''
			# 	},
			# 	'message': '',
			# },
			'process': {
				'taxes': self.get_default_process(),
				'manufacturers': self.get_default_process(),
				'categories': self.get_default_process(),
				'attributes': self.get_default_process(),
				'products': self.get_default_process(),
				'customers': self.get_default_process(),
				'quotes': self.get_default_process(),
				'newsletters': self.get_default_process(),
				'orders': self.get_default_process(),
				'reviews': self.get_default_process(),
				'pages': self.get_default_process(),
				'blogs': self.get_default_process(),
				'coupons': self.get_default_process(),
				'cartrules': self.get_default_process(),
			},
			'setting': self.get_setting(),
			# 'user': {
			# 	'id': None,
			# 	'email': None,
			# },
			'running': False,
			'finish': False,
			'resume': {
				'process': 'new',
				'type': '',
				'action': 'storage_data',
			},
			# 'start_msg': '',
			'limit': '',
			# 'curl': {
			# 	'useragent': True
			# },
			# 'step': None,
			'migration_id': self._migration_id,
			'demo': {
				'status': 'init',
				'clear': False,

			},
			'mode' : MIGRATION_DEMO,
			'log_start': False,
			'version': '2.0.0'
		}

	def get_default_process(self):
		return {
			'total': 0,
			'total_update': 0,
			'real_total': 0,
			'imported': 0,
			'id_src': 0,
			'error': 0,
			'point': 0,
			'time_start': 0,
			'time_resume': 0,
			'previous_imported': 0,
			'time_finish': 0,
			'finish': False
		}

	def get_setting(self):
		default_setting = {
			'storage': 200,
			'taxes': 4,
			'manufacturers': 4,
			'categories': 4,
			'attributes': 4,
			'products': 4,
			'customers': 4,
			'orders': 4,
			'reviews': 4,
			'blogs': 4,
			'coupons': 4,
			'pages': 4,
			'cartrules': 4,
			'delay': 0.01,
			'retry': 30,
			'src_prefix': '',
			'target_prefix': '',
		}
		# if self._migration_id:
		# 	setting = self.select_row(TABLE_SETTING, {'migration_id': self._migration_id})
		# 	if setting:
		# 		return json.loads(setting['setting'])
		return default_setting

	def before_save_migration(self, data):
		fields = ['user_id', 'pid', 'notice', 'src_cart_url', 'src_cart_type', 'src_token', 'target_cart_url', 'target_cart_type', 'target_token', 'status', 'mode']
		migration_data = dict()
		for field in fields:
			if field in data:
				migration_data[field] = data[field] if field != 'notice' else json_encode(data[field])
		if data.get('notice') and self._mode == self.MODE_LIVE:
			notice = data.get('notice')
			migration_data['src_cart_url'] = notice['src']['cart_url']
			migration_data['src_cart_type'] = notice['src']['cart_type']
			migration_data['src_token'] = notice['src']['config']['token']
			migration_data['target_cart_url'] = notice['target']['cart_url']
			migration_data['target_cart_type'] = notice['target']['cart_type']
			migration_data['target_token'] = notice['target']['config']['token']
		return migration_data

	def before_update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False, clear_entity_warning = False):
		if isinstance(notice, str):
			notice = json_decode(notice)

		update = dict()
		if finish:
			update['updated_at'] = get_current_time()
			if to_int(notice['mode']) == MIGRATION_DEMO:
				update['demo_updated_at'] = get_current_time()
				update['demo_status'] = self.DEMO_SUCCESS
		if pid:
			update['pid'] = pid
		if mode:
			update['mode'] = mode
		if status:
			update['status'] = status
			if notice:
				if isinstance(notice, str):
					notice = json_decode(notice)
				notice['resume']['process'] = self.get_status(None, status)
				if to_int(status) == STATUS_COMPLETED:
					update['last_full_mig_notice'] = json_encode(notice) if isinstance(notice, dict) else notice
		else:
			update['status'] = self.get_status(notice['resume']['process'] if isinstance(notice, dict) else json_decode(notice)['resume']['process'])
		if clear_entity_warning:
			update['entity_warning'] = None
		if notice:
			update['notice'] = json_encode(notice) if isinstance(notice, dict) else notice
		return update

	def log(self, msg, type_log = 'exceptions', is_log = True):
		if is_log:
			log(msg, self._migration_id, type_log)

	def get_status(self, process = None, status = None):
		process_status = {
			'new': STATUS_NEW,
			'configuring': STATUS_CONFIGURING,
			'demo_completed': STATUS_PAYMENT,
			'payment': STATUS_PAYMENT,
			'migrating': STATUS_RUN,
			'completed': STATUS_COMPLETED,
			# 'stopped': STATUS_STOP,
			# ''
		}
		if process:
			return process_status.get(process, STATUS_NEW)
		elif status:
			for key_status , value_status in process_status.items():
				if status == value_status:
					return key_status
		return ''

	def set_migration_id(self, migration_id):
		self._migration_id = migration_id

	def set_notice(self, notice):
		self._notice = notice

	def set_mode(self, mode):
		self._mode = mode

	def after_finish(self, migration_id):
		return response_success()