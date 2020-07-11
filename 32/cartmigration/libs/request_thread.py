import io
import pycurl
import threading
from urllib.parse import urlencode

class RequestThread(threading.Thread):
	def __init__(self, migration_id, url, data = None, method = 'get', headers = None, auth = None):
		super().__init__()
		threading.Thread.__init__(self)
		self.url = url
		self.method = method
		self.data = data
		self.headers = headers
		self.auth = auth
		self.migration_id = migration_id

	def run(self):
		response_head = io.BytesIO()
		c = pycurl.Curl()
		c.setopt(pycurl.WRITEFUNCTION, response_head.write)
		c.setopt(pycurl.URL, self.url)
		if self.headers:
			c.setopt(pycurl.HTTPHEADER, self.headers)
		c.setopt(pycurl.USERAGENT, "Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0")
		c.setopt(pycurl.FOLLOWLOCATION, 1)
		c.setopt(pycurl.SSL_VERIFYPEER, 0)
		c.setopt(pycurl.TIMEOUT, 300)
		c.setopt(pycurl.CONNECTTIMEOUT, 300)
		if self.auth and isinstance(self.auth, dict):
			auth_user = self.auth.get('user')
			auth_pass = self.auth.get('pass')
			if auth_user and auth_pass:
				c.setopt(pycurl.USERPWD, auth_user + ':' + auth_pass)

		if self.method == "post" and self.data:
			c.setopt(pycurl.POST, 1)
		if self.method == "put":
			c.setopt(pycurl.CUSTOMREQUEST, "PUT")
		if self.method == "delete":
			c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
		if self.data and self.method != "get":
			if isinstance(self.data, dict) or isinstance(self.data, list):
				self.data = urlencode(self.data)
			c.setopt(pycurl.POSTFIELDS, self.data)
		c.perform()
		return
