import urllib
import requests
from urllib.parse import urlencode
from cartmigration.libs.utils import *
from cartmigration.models.abs_migration.abstract_migration import LeAbstractMigration
class LeLiveMigration(LeAbstractMigration):
	TIMEOUT = 10

	def get_migration_notice(self, migration_id):
		notice_info = self.api('notice/' + to_str(migration_id), method = 'get')
		if notice_info and notice_info['data']:
			return notice_info['data']
		return self.get_default_notice()

	def delete_migration_notice(self, migration_id):
		if not migration_id:
			return True
		delete = self.api('notice/' + to_str(migration_id), method = 'delete')
		if delete and delete['result'] == 'success':
			return True
		return False

	def update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False, clear_entity_warning = False):
		update_data = self.before_update_notice(_migration_id, notice, pid, mode, status, finish, clear_entity_warning)
		update = self.api('migration/' + to_str(_migration_id), update_data, method = 'put')
		return update

	def save_migration(self, migration_id, data):
		if 'migration_id' in data:
			del data['migration_id']
		migration_data = self.before_save_migration(data)
		if migration_id:
			return self.api('migration/' + to_str(migration_id), migration_data, method = 'put')
		else:
			return self.api('migration', migration_data, method = 'post')

	def get_info_migration(self, migration_id):
		if not migration_id:
			return None
		info_migration = self.api('migration/' + to_str(migration_id), method = 'get')
		if info_migration['data']:
			return info_migration['data']
		return False

	def get_app_mode_limit(self):
		setting = self.api('migration/setting', {'key': 'app_mode'}, method = 'get')
		if setting['result'] == 'success':
			return setting['data']
		return False

	def api(self, path, data = None, method = 'post'):
		api_url = get_config_ini('server', 'api_url')
		url = to_str(api_url).strip('/') + '/' + to_str(path).strip('/')
		custom_header = self.get_custom_headers()
		res = self.request_by_method(method, url, data, custom_header)
		res = json_decode(res)
		if not res:
			return response_error()
		return res

	def get_custom_headers(self):
		time_request = to_str(to_int(time.time()))
		private_key = get_config_ini('server', 'private_key')
		hmac = hash_hmac('sha256', time_request, private_key)
		custom_headers = dict()
		custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		custom_headers['Authorization'] = to_str(time_request) + ":" + hmac
		return custom_headers

	def request_by_method(self, method, url, data, custom_headers = None, auth = None):
		if method == 'get':
			if data:
				if '?' not in url:
					url += '?'
				else:
					url += '&'
				if isinstance(data, dict):
					url += urllib.parse.urlencode(data)
				elif isinstance(data, str):
					url += data
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = self.USER_AGENT
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = self.USER_AGENT

		res = False
		r = None
		try:
			if method in ['get', 'delete']:
				r = getattr(requests, method)(url, headers = custom_headers, auth = auth, timeout = self.TIMEOUT)
			else:
				r = getattr(requests, method)(url, data, headers = custom_headers, auth = auth, timeout = self.TIMEOUT)
			res = r.text
			r.raise_for_status()
		except requests.exceptions.HTTPError as errh:
			msg = 'Url ' + url
			# msg += '\n Retry 5 times'
			msg += '\n Method: ' + method
			msg += '\n Status: ' + to_str(r.status_code) if r else ''
			msg += '\n Data: ' + to_str(data)
			msg += '\n Header: ' + to_str(r.headers)
			msg += '\n Response: ' + to_str(res)
			self.log(msg, 'live')
		except requests.exceptions.ConnectionError as errc:
			self.log("Error Connecting:" + to_str(errc) + " : " + to_str(res))
		except requests.exceptions.Timeout as errt:
			self.log("Timeout Error:" + to_str(errt) + " : " + to_str(res))
		except requests.exceptions.RequestException as err:
			self.log("OOps: Something Else" + to_str(err) + " : " + to_str(res))
		return res

	def request_by_post(self, url, data, custom_headers = None, auth = None):
		return self.request_by_method("post", url, data, custom_headers, auth)

	def request_by_get(self, url, data = None, custom_headers = None, auth = None):
		if data:
			if '?' not in url:
				url += '?'
			else:
				url += '&'
			if isinstance(data, dict):
				url += urllib.parse.urlencode(data)
			elif isinstance(data, str):
				url += data
		return self.request_by_method("get", url, data, custom_headers, auth)

	def request_by_put(self, url, data, custom_headers = None, auth = None):
		return self.request_by_method("put", url, data, custom_headers, auth)

	def request_by_delete(self, url, data, custom_headers = None, auth = None):
		return self.request_by_method("delete", url, data, custom_headers, auth)

	def after_finish(self, migration_id):
		return self.api('migration/email/' + to_str(migration_id))
