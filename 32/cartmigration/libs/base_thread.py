import threading
from cartmigration.libs.utils import *


class BaseThread(threading.Thread):
	def __init__(self, buffer = dict, conn = None):
		super().__init__()
		threading.Thread.__init__(self)
		self.conn = conn
		self.buffer = buffer
	#
	# def set_con(self, con):
	# 	self._con = con


	def run(self):
		if not self.buffer or not self.conn or not isinstance(self.buffer, dict):
			if self.conn:
				send_data_socket(response_error(), self.conn)
			return
		res = start_subprocess(None, self.buffer, True)
		if isinstance(res, dict) and 'next' in res:
			migration_id = res['next'].get('migration_id')
			start_subprocess(migration_id, res['next'])
			del(res['next'])
		send_data_socket(res, self.conn)
		return
		# controller_name = self.buffer.get('controller', None)
		# data = self.buffer['data']
		# action_name = self.buffer.get('action')
		# if not action_name:
		# 	if self.conn:
		# 		send_data_socket(response_error(), self.conn)
		# 	return
		# if not controller_name:
		# 	controller = BaseController(data)
		# else:
		# 	controller = get_controller(controller_name, data)
		# action_name = 'client_' + action_name
		# if controller and hasattr(controller, action_name):
		# 	try:
		# 		res = getattr(controller, action_name)(data)
		# 		send_data_socket(res, self.conn)
		# 	except Exception as e:
		# 		error = traceback.format_exc()
		# 		log(error)
		# 		send_data_socket(response_error(), self.conn)
		# 	return
		# if self.conn:
		# 	send_data_socket(response_error(), self.conn)

	def stop(self):
		pass

	def set_exit_flag(self, exit_flag):
		pass

	# def save_notice(self, cart = None):
	# 	if not cart:
	# 		cart = get_model('basecart')
	# 	notice = self._notice
	# 	demo = None
	# 	if 'demo' in notice and notice['demo']:
	# 		demo = 2
	# 	res = getattr(cart, 'save_user_notice')(self._migration_id, notice, demo)
	# 	return res
	#
	# def save_recent(self, cart = None):
	# 	if not cart:
	# 		cart = get_model('basecart')
	# 	return getattr(cart, 'save_recent')(self._migration_id, self._notice)
	#
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
	#
	# def get_source_cart(self, basecart = None):
	# 	base_controller = BaseController(self._migration_id, self._notice)
	# 	return base_controller.get_source_cart(basecart)
	#
	# def get_target_cart(self, basecart = None):
	# 	base_controller = BaseController(self._migration_id, self._notice)
	# 	return base_controller.get_target_cart(basecart)
	#
	# def get_notice(self, notice, con):
	# 	cart = get_model('basecart')
	# 	notice = getattr(cart, 'get_user_notice')(notice['setting']['migration_id'])
	# 	send_data_socket(notice, con)
	#
	# def display_config_source(self, notice, con):
	# 	result = response_success()
	# 	cart = get_model('basecart')
	# 	getattr(cart, 'set_migration_id')(notice['setting']['migration_id'])
	# 	getattr(cart, 'init_notice')()
	# 	source_cart = self.get_source_cart(cart)
	# 	if not source_cart:
	# 		send_data_socket(response_error())
	# 	getattr(source_cart, 'display_config_source')()
	# 	notice = getattr(source_cart, 'get_notice')()
	# 	getattr(cart, 'set_notice')(notice)
	# 	save_notice = self.save_notice(cart)
	# 	if not save_notice:
	# 		result['result'] = 'error'
	# 		result['msg'] = 'error save notice2'
	# 		send_data_socket(result, con)
	# 	send_data_socket(response_success(notice), con)