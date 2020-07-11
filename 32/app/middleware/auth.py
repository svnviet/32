import hmac

from werkzeug.wrappers import Request, Response, ResponseStream
from cartmigration.libs.utils import *
class Auth():
	def __init__(self, app):
		self.app = app

	def __call__(self, environ, start_response):
		request_ip = environ.get('REMOTE_ADDR')
		if request_ip != get_config_ini('server', 'ip'):
			res = Response(json_encode(response_error('403 Forbidden')), mimetype = 'text/plain', status = 403)
			return res(environ, start_response)
		authorization_key = environ.get('HTTP_AUTHORIZATION')
		if not authorization_key:
			res = Response(json_encode(response_error('Authorization failed. Please add AUTHORIZATION to header request')), mimetype = 'text/plain', status = 401)
			return res(environ, start_response)
		authorization_path = to_str(authorization_key).split(":")
		if len(authorization_path) != 2:
			res = Response(json_encode(response_error('Authorization failed.')), mimetype = 'text/plain', status = 401)
			return res(environ, start_response)
		time_request = to_str(authorization_path[0])
		# if to_int(time.time()) - to_int(time_request) > 30:
		# 	res = Response(json_encode(response_error('Authorization failed, request is expired')), mimetype = 'text/plain', status = 401)
		# 	return res(environ, start_response)
		hmac = authorization_path[1]
		if not self.authorize(time_request, hmac):
			res = Response(json_encode(response_error('Authorization failed.')), mimetype = 'text/plain', status = 401)
			return res(environ, start_response)
		return self.app(environ, start_response)

	def authorize(self, time_request, hmac):
		private_key = get_config_ini('local', 'private_key')
		if not private_key:
			return False
		return to_str(hmac) == hash_hmac('sha256', time_request, private_key)
