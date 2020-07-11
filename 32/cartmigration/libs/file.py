import urllib.parse
import re
class LeFile:

	def strip_domain_from_url(self, url):
		parse = urllib.parse.urlparse(url)
		path_url = parse.path
		query = parse.query
		fragment = parse.fragment
		if query:
			path_url += '?' + query
		if fragment:
			path_url += '#'+fragment
		return path_url

	def join_url_path(self, url, path_url):
		full_url = url.rstrip('/')
		if path_url:
			full_url += '/' + path_url.ltrip('/')
		return full_url

	def is_url_encode(self, path_url):
		pattern = '%[0-9A-F]{2}'
		matches = re.search(pattern, path_url)
		return True if matches else False

