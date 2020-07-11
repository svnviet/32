import copy
import csv
import html
import io
import math
import pycurl
import unicodedata
import zlib
from urllib.parse import urlencode
from xml.etree import ElementTree
import chardet
import pandas
# import grequests
import requests

from cartmigration.libs.request_thread import RequestThread
from cartmigration.libs.strip_html import StripHtml
from cartmigration.libs.strip_html import StripHtmlWix
from cartmigration.libs.utils import *
from cartmigration.models.migration import LeMigration
from cartmigration.models.setup import Setup

class LeBasecart(LeMigration):
	URL_PROXY = 'http://45.56.81.195/img_proxy.php?img='
	CONNECTOR_SUFFIX = '/le_connector/connector.php'
	TYPE_TAX = 'tax'
	TYPE_TAX_PRODUCT = 'tax_product'
	TYPE_TAX_CUSTOMER = 'tax_customer'
	TYPE_TAX_RATE = 'tax_rate'
	TYPE_TAX_CALCULATION = 'tax_calculation'
	TYPE_TAX_ZONE = 'tax_zone'
	TYPE_TAX_ZONE_COUNTRY = 'tax_zone_country'
	TYPE_TAX_ZONE_STATE = 'tax_zone_state'
	TYPE_TAX_ZONE_RATE = 'tax_zone_rate'
	TYPE_MANUFACTURER = 'manufacturer'
	TYPE_CATEGORY = 'category'
	TYPE_CATEGORY_BLOG = 'category_blog'
	TYPE_PRODUCT = 'product'
	TYPE_IMAGE = 'img'
	TYPE_PATH_IMAGE = 'path_img'
	TYPE_CHILD = 'product_child'
	TYPE_ATTR = 'attr'
	TYPE_ATTR_OPTION = 'attr_option'
	TYPE_ATTR_VALUE = 'attr_value'
	TYPE_OPTION = 'option'
	TYPE_OPTION_VALUE = 'option_value'
	TYPE_CUSTOMER = 'customer'
	TYPE_QUOTE = 'quote'
	TYPE_NEWSLETTER = 'newsletter'
	TYPE_ATTR_CUSTOMER = 'attr_customer'
	TYPE_ATTR_CUSTOMER_VALUE = 'attr_customer_value'
	TYPE_ADDRESS = 'address'
	TYPE_ORDER = 'order'
	TYPE_REVIEW = 'review'
	TYPE_SHIPPING = 'shipping'
	TYPE_PAGE = 'page'
	TYPE_BLOG = 'blog'
	TYPE_WIDGET = 'widget'
	TYPE_POLL = 'poll'
	TYPE_TRANSACTION = 'transaction'
	# TYPE_NEWSLETTER = 'newsletter'
	TYPE_USER = 'user'
	TYPE_RULE = 'rule'
	TYPE_COUPON = 'coupon'
	TYPE_CART_RULE = 'cart_rule'
	TYPE_POST = 'post'
	TYPE_FORMAT = 'format'
	TYPE_COMMENT = 'comment'
	TYPE_TAG = 'tag'
	TYPE_BUNDLE_OPTION = "bundle_option"
	TYPE_ORDER_ITEM = "sales_order_item"
	TYPE_CAT_URL = "category_url_key"
	TYPE_PRO_URL = "product_url_key"
	PRODUCT_SIMPLE = 'simple'
	PRODUCT_CONFIG = 'configurable'
	PRODUCT_VIRTUAL = 'virtual'
	PRODUCT_DOWNLOAD = 'download'
	PRODUCT_GROUP = 'grouped'
	PRODUCT_BUNDLE = 'bundle'
	PRODUCT_RELATE = 'relate'
	PRODUCT_UPSELL = 'upsell'
	PRODUCT_CROSS = 'cross'
	OPTION_FIELD = 'field'
	OPTION_TEXT = 'text'
	OPTION_SELECT = 'select'
	OPTION_DATE = 'date'
	OPTION_DATETIME = 'datetime'
	OPTION_RADIO = 'radio'
	OPTION_CHECKBOX = 'checkbox'
	OPTION_PRICE = 'price'
	OPTION_BOOLEAN = 'boolean'
	OPTION_FILE = 'file'
	OPTION_MULTISELECT = 'multiselect'
	OPTION_FRONTEND = 'frontend'
	OPTION_BACKEND = 'backend'
	GENDER_MALE = 'm'
	GENDER_FEMALE = 'f'
	GENDER_OTHER = 'o'
	PRICE_POSITIVE = '+'
	PRICE_NEGATIVE = '-'
	DEBUG_MODE = True
	SEO_DEFAULT = '1'
	SEO_301 = '301'
	HTTP_AUTH_TYPE = None  # src|target|both
	HTTP_AUTH_USER = None
	HTTP_AUTH_PASS = None
	LOG_QUERIES = False
	VARIANT_LIMIT = 1000
	LIMIT_CLEAR_DEMO = 100
	FIXED = 1
	PERCENT = 2
	COUNTRIES = '{"BD": "Bangladesh", "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BA": "Bosnia and Herzegovina", "BB": "Barbados", "WF": "Wallis and Futuna", "BL": "Saint Barthelemy", "BM": "Bermuda", "BN": "Brunei", "BO": "Bolivia", "BH": "Bahrain", "BI": "Burundi", "BJ": "Benin", "BT": "Bhutan", "JM": "Jamaica", "BV": "Bouvet Island", "BW": "Botswana", "WS": "Samoa", "BQ": "Bonaire, Saint Eustatius and Saba ", "BR": "Brazil", "BS": "Bahamas", "JE": "Jersey", "BY": "Belarus", "BZ": "Belize", "RU": "Russia", "RW": "Rwanda", "RS": "Serbia", "TL": "East Timor", "RE": "Reunion", "TM": "Turkmenistan", "TJ": "Tajikistan", "RO": "Romania", "TK": "Tokelau", "GW": "Guinea-Bissau", "GU": "Guam", "GT": "Guatemala", "GS": "South Georgia and the South Sandwich Islands", "GR": "Greece", "GQ": "Equatorial Guinea", "GP": "Guadeloupe", "JP": "Japan", "GY": "Guyana", "GG": "Guernsey", "GF": "French Guiana", "GE": "Georgia", "GD": "Grenada", "GB": "United Kingdom", "GA": "Gabon", "SV": "El Salvador", "GN": "Guinea", "GM": "Gambia", "GL": "Greenland", "GI": "Gibraltar", "GH": "Ghana", "OM": "Oman", "TN": "Tunisia", "JO": "Jordan", "HR": "Croatia", "HT": "Haiti", "HU": "Hungary", "HK": "Hong Kong", "HN": "Honduras", "HM": "Heard Island and McDonald Islands", "VE": "Venezuela", "PR": "Puerto Rico", "PS": "Palestinian Territory", "PW": "Palau", "PT": "Portugal", "SJ": "Svalbard and Jan Mayen", "PY": "Paraguay", "IQ": "Iraq", "PA": "Panama", "PF": "French Polynesia", "PG": "Papua New Guinea", "PE": "Peru", "PK": "Pakistan", "PH": "Philippines", "PN": "Pitcairn", "PL": "Poland", "PM": "Saint Pierre and Miquelon", "ZM": "Zambia", "EH": "Western Sahara", "EE": "Estonia", "EG": "Egypt", "ZA": "South Africa", "EC": "Ecuador", "IT": "Italy", "VN": "Vietnam", "SB": "Solomon Islands", "ET": "Ethiopia", "SO": "Somalia", "ZW": "Zimbabwe", "SA": "Saudi Arabia", "ES": "Spain", "ER": "Eritrea", "ME": "Montenegro", "MD": "Moldova", "MG": "Madagascar", "MF": "Saint Martin", "MA": "Morocco", "MC": "Monaco", "UZ": "Uzbekistan", "MM": "Myanmar", "ML": "Mali", "MO": "Macao", "MN": "Mongolia", "MH": "Marshall Islands", "MK": "Macedonia", "MU": "Mauritius", "MT": "Malta", "MW": "Malawi", "MV": "Maldives", "MQ": "Martinique", "MP": "Northern Mariana Islands", "MS": "Montserrat", "MR": "Mauritania", "IM": "Isle of Man", "UG": "Uganda", "TZ": "Tanzania", "MY": "Malaysia", "MX": "Mexico", "IL": "Israel", "FR": "France", "IO": "British Indian Ocean Territory", "SH": "Saint Helena", "FI": "Finland", "FJ": "Fiji", "FK": "Falkland Islands", "FM": "Micronesia", "FO": "Faroe Islands", "NI": "Nicaragua", "NL": "Netherlands", "NO": "Norway", "NA": "Namibia", "VU": "Vanuatu", "NC": "New Caledonia", "NE": "Niger", "NF": "Norfolk Island", "NG": "Nigeria", "NZ": "New Zealand", "NP": "Nepal", "NR": "Nauru", "NU": "Niue", "CK": "Cook Islands", "XK": "Kosovo", "CI": "Ivory Coast", "CH": "Switzerland", "CO": "Colombia", "CN": "China", "CM": "Cameroon", "CL": "Chile", "CC": "Cocos Islands", "CA": "Canada", "CG": "Republic of the Congo", "CF": "Central African Republic", "CD": "Democratic Republic of the Congo", "CZ": "Czech Republic", "CY": "Cyprus", "CX": "Christmas Island", "CR": "Costa Rica", "CW": "Curacao", "CV": "Cape Verde", "CU": "Cuba", "SZ": "Swaziland", "SY": "Syria", "SX": "Sint Maarten", "KG": "Kyrgyzstan", "KE": "Kenya", "SS": "South Sudan", "SR": "Suriname", "KI": "Kiribati", "KH": "Cambodia", "KN": "Saint Kitts and Nevis", "KM": "Comoros", "ST": "Sao Tome and Principe", "SK": "Slovakia", "KR": "South Korea", "SI": "Slovenia", "KP": "North Korea", "KW": "Kuwait", "SN": "Senegal", "SM": "San Marino", "SL": "Sierra Leone", "SC": "Seychelles", "KZ": "Kazakhstan", "KY": "Cayman Islands", "SG": "Singapore", "SE": "Sweden", "SD": "Sudan", "DO": "Dominican Republic", "DM": "Dominica", "DJ": "Djibouti", "DK": "Denmark", "VG": "British Virgin Islands", "DE": "Germany", "YE": "Yemen", "DZ": "Algeria", "US": "United States", "UY": "Uruguay", "YT": "Mayotte", "UM": "United States Minor Outlying Islands", "LB": "Lebanon", "LC": "Saint Lucia", "LA": "Laos", "TV": "Tuvalu", "TW": "Taiwan", "TT": "Trinidad and Tobago", "TR": "Turkey", "LK": "Sri Lanka", "LI": "Liechtenstein", "LV": "Latvia", "TO": "Tonga", "LT": "Lithuania", "LU": "Luxembourg", "LR": "Liberia", "LS": "Lesotho", "TH": "Thailand", "TF": "French Southern Territories", "TG": "Togo", "TD": "Chad", "TC": "Turks and Caicos Islands", "LY": "Libya", "VA": "Vatican", "VC": "Saint Vincent and the Grenadines", "AE": "United Arab Emirates", "AD": "Andorra", "AG": "Antigua and Barbuda", "AF": "Afghanistan", "AI": "Anguilla", "VI": "U.S. Virgin Islands", "IS": "Iceland", "IR": "Iran", "AM": "Armenia", "AL": "Albania", "AO": "Angola", "AQ": "Antarctica", "AS": "American Samoa", "AR": "Argentina", "AU": "Australia", "AT": "Austria", "AW": "Aruba", "IN": "India", "AX": "Aland Islands", "AZ": "Azerbaijan", "IE": "Ireland", "ID": "Indonesia", "UA": "Ukraine", "QA": "Qatar", "MZ": "Mozambique"}'
	CURRENCIES = '{"CAD":1.3289413903,"HKD":7.8269877328,"ISK":122.6715129487,"PHP":50.7950931395,"DKK":6.7891867333,"HUF":305.5429350295,"CZK":23.2385279418,"GBP":0.7740118128,"RON":4.3480236256,"SEK":9.5831894593,"IDR":14100.7996365289,"INR":71.6124488869,"BRL":4.2519763744,"RUB":64.0915947297,"HRK":6.7578373467,"JPY":109.4956837801,"THB":30.2253521127,"CHF":0.998727851,"EUR":0.9086778737,"MYR":4.1734666061,"BGN":1.7771921854,"TRY":5.7680145388,"CNY":7.034620627,"NOK":9.1894593367,"NZD":1.5571104044,"ZAR":14.7476601545,"USD":1.0,"MXN":19.6080872331,"SGD":1.3659245797,"AUD":1.4768741481,"ILS":3.4684234439,"KRW":1179.4638800545,"PLN":3.9265788278}'
	CURRENCIES_SYMBOL = '{"EUR": "\u20ac", "ILS": "\u20aa", "JPY": "JP\u00a5", "XOF": "CFA", "NZD": "NZ$", "CAD": "CA$", "THB": "\u0e3f", "MXN": "MX$", "GBP": "\u00a3", "AUD": "AU$", "XCD": "EC$", "USD": "US$", "VND": "\u20ab", "XAF": "FCFA", "INR": "\u20b9", "CNY": "CN\u00a5", "HKD": "HK$", "BRL": "R$", "XPF": "CFPF", "KRW": "\u20a9", "TWD": "NT$"}'
	PROXY_HOST = 'http://lum-customer-hl_9223efcc-zone-zone2:ycpetz9xq8lf@zproxy.lum-superproxy.io:22225'

	def __init__(self, data = None):
		super().__init__(data)
		self.cat_parent = list()
		self._notice = None
		self._previous_notice = None
		self._cart_url = None
		self._type = None
		self._has_retry = False
		self._clear_entity_warning = True
		self._user_id = None
		self.retry_503 = False
		self.currencies = dict()
		self._total_time_sleep = 0
		self._stop_action = False
		self.migrate_product_extend_config = None
		self.time_delay_503 = 0
		self.max_time_delay_503 = 0
		self._migration_id = data.get('migration_id') if isinstance(data, dict) else None
		self.image_proxy = None
		self._query_error = None
		self.connector_method = 'post'
		self.convert_data = dict()
		self.last_status = None
		self.last_header = None
		self.ssl_image = None

	# TODO: INIT
	# def get_db(self):
	# 	if not self._db:
	# 		db = Database()
	# 		conn = db.get_connect()
	# 		if not conn:
	# 			return None
	# 		self._db = db
	# 	return self._db

	def get_clear_entity_warning(self):
		return self._clear_entity_warning

	def set_convert_data(self, convert):
		self.convert_data = convert

	def get_stop_action(self):
		return self._stop_action

	# TODO: NOTICE
	def set_type(self, cart_type):
		self._type = cart_type

	def get_type(self):
		return self._type

	def get_user_id(self):
		return self._user_id

	def set_user_id(self, user_id):
		self._user_id = user_id

	def set_notice(self, notice):
		self._notice = notice
		cart_type = self.get_type()
		if cart_type:
			self._cart_url = notice[cart_type]['cart_url']
		return self

	def create_folder_upload(self, cart_url):
		cart_url = cart_url + to_str(time.time())
		return hashlib.md5(cart_url.encode('utf-8')).hexdigest()

	def get_file_info(self):
		return {

		}

	def get_api_info(self):
		return {

		}


	def log_primary(self, entity_type, error, entity_id = None, code = None, warning = None):
		msg = entity_type.capitalize()
		if entity_id:
			msg += ' id ' + to_str(entity_id) + ': '
		elif code:
			msg += ' code ' + to_str(code) + ': '
		else:
			msg += ': '
		msg += self.text_error_html(error)
		if warning:
			msg += '<br>'
			msg += self.text_warning_html(warning)
		self.log(msg, entity_type + '_errors')

	def get_notice(self):
		return self._notice

	def format_url(self, url):
		if self.CONNECTOR_SUFFIX in url:
			url = url.replace(self.CONNECTOR_SUFFIX, '')
		if "index.php" in url:
			url_split = url.split('index.php')
			url = url_split[0]
		url = url.rstrip('/')
		return url

	def is_init_notice(self, cart_type):
		setup_type = self.source_cart_setup(cart_type)
		return setup_type == 'file'

	def source_cart_setup(self, cart_type):
		setup_type = {
			'woocommerce': 'module_connector',
			'cart66': 'module_connector',
			'marketpress': 'module_connector',
			'jigoshop': 'module_connector',
			'joomshopping': 'module_connector',
			'shopp': 'module_connector',
			'wpecommerce': 'module_connector',
			'wpestore': 'module_connector',

			# api
			'shopify': 'api',
			'bigcommerce': 'api',
			'3dcart': 'api',
			'americommerce': 'api',
			'ecwid': 'api',
			'wix': 'api',
			'squarespace': 'api',
			'neto': 'api',
			'godaddy': 'api',
			'mystore': 'api',
			'smartweb': 'api',
			'customapi': 'api',

			# file
			'adobe': 'file',
			'ci_nopcommerce': 'file',
			'volusion': 'file',
			'lightspeed': 'file',
			'mivamerchant': 'file',
			'bigcartel': 'file',
			'amazonstore': 'file',
			'yahoostore': 'file',
			'weebly': 'api',
			'ekm': 'file',
			'corecommerce': 'file',

			'jshop': 'file',
			'zoey': 'file',
			'epages': 'file',
			'jimdo': 'file',
			'customfile': 'file',

			# database
			'nopcommerce': 'database',
			'aspdotnetstorefront': 'database',

		}
		return setup_type[cart_type] if cart_type in setup_type else 'connector'

	def target_cart_setup(self, cart_type):
		setup_type = {
			# moduel connector
			'woocommerce': 'module_connector',
			'cart66': 'module_connector',
			'marketpress': 'module_connector',
			'jigoshop': 'module_connector',
			'joomshopping': 'module_connector',
			'shopp': 'module_connector',
			'wpecommerce': 'module_connector',
			'wpestore': 'module_connector',

			# api
			'3dcart': 'api',
			'shopify': 'api',
			'bigcommerce': 'api',
			'volusion': 'api',
			'mivamerchant': 'api',
			'neto': 'api',
			'wix': 'api',
			'squarespace': 'api',
			'godaddy': 'api',
			'ecwid': 'api',
			'esty': 'api',
			# database
			'new_nopcommerce': 'database',
			'nopcommerce': 'database',

		}
		return setup_type[cart_type] if cart_type in setup_type else 'connector'

	def clear_previous_section(self, file_upload):
		return response_success()

	def get_upload_file_name(self, upload_name):
		return upload_name + '.csv'

	def storage_csv_by_type(self, type_storage, next_storage, success = False, finish = False, unset = list):
		if not success:
			success = next_storage
		if not self._notice['src']['config']['file'][type_storage]['upload'] or self._notice['src']['config']['file'][type_storage]['storage']:
			if finish:
				self._notice['src']['storage']['result'] = 'success'
				self._notice['src']['storage']['init'] = False

			else:
				self._notice['src']['storage']['result'] = 'process'
			self._notice['src']['storage']['function'] = 'storage_csv_' + next_storage
			self._notice['src']['storage']['msg'] = ''
			return self._notice['src']['storage']
		folder_upload = self.get_folder_upload(self._migration_id)

		file_upload = folder_upload + '/' + self.get_upload_file_name(type_storage)
		if not os.path.isfile(file_upload):
			if finish:
				self._notice['src']['storage']['result'] = 'success'
				self._notice['src']['storage']['init'] = False

			else:
				self._notice['src']['storage']['result'] = 'process'
			self._notice['src']['storage']['function'] = 'storage_csv_' + next_storage
			self._notice['src']['storage']['msg'] = ''
			return self._notice['src']['storage']
		allow_data = list()
		table = getattr(self, type_storage + '_table_construct')()
		validation = list()
		if not allow_data:
			rows = table['rows']
			validation = table.get('validation', list())
			if unset:
				rows = self.unset_list_in_dict(unset, rows)
			allow_data = list(rows.keys())
		data_csv = self.read_csv(file_upload)
		if not data_csv:
			if finish:
				self._notice['src']['storage']['result'] = 'success'
				self._notice['src']['storage']['init'] = False
			else:
				self._notice['src']['storage']['result'] = 'process'
			self._notice['src']['storage']['function'] = 'storage_csv_' + next_storage
			self._notice['src']['storage']['msg'] = ''
			return self._notice['src']['storage']
		for i, data in data_csv.items():
			if not data:
				continue
			data = self.sync_csv_title_row(data)
			check = False
			for key, value in data.items():
				if isinstance(value, float) and math.isnan(value):
					data[key] = None
			if validation:
				for column_name in validation:
					if (not (column_name in data)) or not data[column_name]:
						check = True
						break
				if check:
					continue

			data = self.add_config_to_dict(data)
			data = self.get_list_in_dict(allow_data, data)
			insert = self.insert_obj(table['table'], data)
			if not insert or insert['result'] != 'success':
				continue
		self._notice['src']['config']['file'][type_storage]['storage'] = True
		if finish:
			self._notice['src']['storage']['result'] = 'success'
			self._notice['src']['storage']['init'] = False
		else:
			self._notice['src']['storage']['result'] = 'process'
		self._notice['src']['storage']['function'] = 'storage_csv_' + success
		self._notice['src']['storage']['msg'] = self.console_success("Finish import " + type_storage)
		self._notice['src']['storage']['count'] = 0
		return self._notice['src']['storage']

	# def sync_data_csv_title(self, row, data):
	'''
	where: list with 2 value. example ['orderid', 123]
	filter file: orderid > 123
	'''
	def read_csv(self, file_upload, to_dict = True, delimiter = ",", has_reversed = False, where = list):
		if not os.path.isfile(file_upload):
			return None
		encoding = ['utf-8', 'utf-16', 'cp1252', 'latin1', 'iso-8859-1']

		for encode in encoding:
			try:
				data_csv = pandas.read_csv(file_upload, sep = delimiter, encoding = encode, low_memory = False, na_values = None, error_bad_lines = False)
				data_filter = False
				if where:
					try:
						data_filter = data_csv.loc[data_csv[where[0]] > where[1]]
					except Exception:
						data_filter = False
				if data_filter is False:
					data_filter = data_csv
				if to_dict:
					data_filter = data_filter.T.to_dict()
					if has_reversed:
						try:
							new_data = list(data_filter.values())
							new_data = list(reversed(new_data))
							new_data = self.list_to_dict(new_data)
						except:
							self.log_traceback('read_file')
							new_data = None
						if new_data:
							data_filter = new_data
				return data_filter
			except:
				self.log_traceback('read_file')
				continue
		return None

	def sync_csv_title_row(self, data):
		sync_data = dict()
		for key, value in data.items():
			new_key = key.replace(' ', '_').replace(':', '')
			sync_data[new_key] = value
		return sync_data

	def get_title_csv_file(self, file):
		if not os.path.isfile(file):
			return list()
		encoding = ['utf-8', 'utf-16', 'cp1252', 'latin1', 'iso-8859-1']
		for encode in encoding:
			try:
				f = open(file, 'r', encoding = encode)
				reader = csv.reader(f)
				header = next(reader)
				f.close()
				return header
			except Exception:
				continue
		return list()

	def add_config_to_dict(self, data):
		# data['folder'] = self._notice['src']['config']['folder']
		data['migration_id'] = self._migration_id
		# data['url_desc'] = self._notice['target']['cart_url']
		return data

	def unset_list_in_dict(self, need, haystack):
		if (not need) or (not isinstance(haystack, dict)) or (not isinstance(need, list)):
			return haystack
		for key in need:
			if key in haystack:
				del haystack[key]
		return haystack

	def get_list_in_dict(self, need, haystack):
		if (not need) or (not isinstance(haystack, dict)) or (not isinstance(need, list)):
			return haystack
		data = dict()
		for key in need:
			data[key] = haystack.get(key, None)
		return data

	def console_success(self, msg):
		result = '<p class="success"> - ' + msg + '</p>'
		return result

	def console_error(self, msg):
		result = '<p class="error"> - ' + msg + '</p>'
		return result

	def console_warning(self, msg):
		result = '<p class="warning"> - ' + msg + '</p>'
		return result

	def save_recent(self, _migration_id, notice):
		notice = json.dumps(notice)
		exist = False
		check = self.select_obj(TABLE_RECENT, {'migration_id': _migration_id})
		if check and check['result'] == 'success' and check['data']:
			exist = True
		result = False
		if exist:
			update = self.update_obj(TABLE_RECENT, {'notice': notice}, {'migration_id': _migration_id})
			if update and update['result'] == 'success':
				result = True
		else:
			insert = self.insert_obj(TABLE_RECENT, {'migration_id': _migration_id, 'notice': notice})
			if insert and insert['result'] == 'success':
				result = True
		return result

	def get_recent(self, _migration_id):
		if not _migration_id:
			return None
		result = self.select_obj(TABLE_RECENT, {'migration_id': _migration_id})
		if 'data' in result and to_len(result['data']) > 0:
			return json_decode(result['data'][0]['notice'])
		return None

	def delete_recent(self, _migration_id):
		if not _migration_id:
			return True
		delete = self.delete_obj(TABLE_RECENT, {'migration_id': _migration_id})
		if delete and delete['result'] == 'success':
			return delete['data']
		return False

	def get_migration_id(self):
		return self._migration_id

	def set_migration_id(self, _migration_id):
		# self._db_center.set_migration_id(_migration_id)
		# self._db_local.set_migration_id(_migration_id)
		self._migration_id = _migration_id

	def delete_dir(self, path):
		if not os.path.isdir(path):
			return response_error('Path is not directory.')
		try:
			path = path.rstrip('/\\')
			import shutil
			shutil.rmtree(path)
			return response_success()
		except Exception as e:
			return response_error(e)

	def combination_from_multi_dict(self, data = None):
		if data is None:
			data = dict()
		result = list()
		data = list(data.values())
		size_in = to_len(data)
		size = 1 if size_in > 0 else 0
		index_data = list()
		for index, row in enumerate(data):
			size = size * to_len(row)
			index_data.append(0)
		for index in range(size):
			result.append(list())
			for j in range(size_in):
				result[index].append(data[j][index_data[j]])
			for j in range(0, size_in, 1):
				if to_len(data[j]) - 1 > index_data[j]:
					index_data[j] += 1
					break
				else:
					index_data[j] = 0
		return result

	# TODO: Cart
	def get_cart(self, cart_type, cart_version = None, special_type = False):
		convert_version = parse_version(re.sub(r"[^0-9.]", '', to_str(cart_version)))
		if special_type and cart_type == 'magento':
			if not cart_version:
				return 'cart/magento/magento19'
			version = cart_version.split('.')
			if to_len(version) > 2 and to_int(version[0]) == 1 and to_int(version[1]) > 9:
				return 'cart/magento/magento19'
			# magento_version = parse_version(to_str(cart_version).replace('.ee', ''))
			if convert_version >= parse_version("2.0.0") or ('ee' in cart_version) :
				if 'ee' in cart_version:
					return 'cart/magento/magento2ee'

				return 'cart/magento/magento2'
			elif convert_version > parse_version('1.4.9'):
				return 'cart/magento/magento19'
			elif convert_version >= parse_version('1.4.1'):
				return 'cart/magento/magento14'
			else:
				return 'cart/magento/magento13'

		if special_type and cart_type == 'prestashop':
			if not cart_version:
				return 'basecart'
			# if convert_version > parse_version('1.6.9'):
			# 	return 'cart/prestashop_prestashop17'
			# elif convert_version > parse_version('1.4.9'):
			# 	return 'cart/prestashop_prestashop16'
			# elif convert_version > parse_version('1.3.9'):
			# 	return 'cart/prestashop_prestashop14'
			# else:
			# 	return 'cart/prestashop_prestashop13'
			prestashop_version = self.convert_version(cart_version, 2)
			if prestashop_version > 169:
				return 'cart/prestashop/prestashop17'
			elif prestashop_version > 149:
				return 'cart/prestashop/prestashop16'
			elif prestashop_version > 139:
				return 'cart/prestashop/prestashop14'
			else:
				return 'cart/prestashop/prestashop13'

		if cart_type == 'squirrelcart':
			return 'cart/squirrelcart'
		if cart_type == 'volusion':
			return 'cart/volusion'

		if cart_type == 'lightspeed':
			return 'cart/lightspeed'

		if cart_type == 'yahoostore':
			return 'cart/yahoostore'

		if cart_type == 'shopify':
			return 'cart/shopify'

		if cart_type == 'oscommerce':
			return 'cart/oscommerce'

		if cart_type == 'zencart':
			return 'cart/zencart'

		if cart_type == 'interspire':
			return 'cart/interspire'

		if cart_type == 'bigcommerce':
			return 'cart/bigcommerce'

		if cart_type == 'mivamerchant':
			return 'cart/mivamerchant'

		if cart_type == 'woocommerce':
			return 'cart/woocommerce'

		if cart_type == 'weebly':
			return 'cart/weebly'

		if cart_type == 'ekm':
			return 'cart/ekm'
		if cart_type == 'epage':
			return 'cart/epage'
		if cart_type == 'squarespace':
			return 'cart/squarespace'

		if cart_type == 'nopcommerce':
			return 'cart/nopcommerce'

		if cart_type == 'pinnaclecart':
			return 'cart/pinnaclecart'

		if cart_type == 'wpecommerce':
			if convert_version == parse_version('3.6'):
				return 'cart/wpecommerce36'
			if str(convert_version).find('3.8.1') >= 0 or convert_version == parse_version('3.14.0') or convert_version == parse_version('3.12.0'):
				return 'cart/wpecommerce3814'
			if convert_version > parse_version('2.9.9') and convert_version != parse_version('3.8.1'):
				return 'cart/wpecommerce38'

			return 'cart/wpecommerce'

		if cart_type == 'hikashop':
			return 'cart/hikashop'

		if cart_type == 'randshop':
			return 'cart/randshop'

		if cart_type == 'xcart':
			# if not cart_version:
			# 	return 'basecart'
			if parse_version('4.5.9') >= convert_version > parse_version('4.3.9'):
				return 'cart/xcart4'
			elif parse_version('5.0.0') > convert_version > parse_version('4.5.9'):
				return 'cart/xcart46'
			elif convert_version > parse_version('4.9.9'):
				return 'cart/xcart5'
			else:
				return 'cart/xcart'

		if cart_type == 'jshop':
			return 'cart/jshop'

		if cart_type == 'wix':
			return 'cart/wix'

		if cart_type == 'americommerce':
			return 'cart/americommerce'

		if cart_type == 'ecwid':
			return 'cart/ecwid'

		if cart_type == 'magento':
			if not cart_version:
				return 'cart/magento19'
			version = cart_version.split('.')
			if to_len(version) > 2 and to_int(version[0]) == 1 and to_int(version[1]) > 9:
				return 'cart/magento19'
			# magento_version = self.convert_version(cart_version, 2)
			if convert_version >= parse_version('2.0.0') or ('ee' in cart_version):
				if 'ee' in cart_version:
					return 'cart/magento2ee'
				return 'cart/magento2'
			elif convert_version > parse_version('1.4.9'):
				return 'cart/magento19'
			elif convert_version > parse_version('1.4.1'):
				return 'cart/magento14'
			else:
				return 'cart/magento13'
		if cart_type == 'opencart' or cart_type == 'mijoshop':
			# opencart_version = self.convert_version(cart_version, 2)
			if convert_version >= parse_version('3.0.0'):
				return 'cart/opencart3'
			elif parse_version('3.0.0') > convert_version > parse_version('1.9.9'):
				return 'cart/opencart2'
			elif parse_version('2.0.0') > convert_version > parse_version('1.5.1'):
				return 'cart/opencart15'
			elif parse_version('1.5.2') > convert_version > parse_version('1.2.9'):
				return 'cart/opencart14'
			else:
				return 'cart/opencart12'

		if cart_type == 'virtuemart':
			if not cart_version:
				return 'basecart'
			# virtuemart_version = self.convert_version(cart_version, 2)
			if convert_version == parse_version('2.0.22d'):
				return 'cart/virtuemart2'
			if convert_version < parse_version('2.0.0'):
				return 'cart/virtuemart1'
			else:
				return 'cart/virtuemart2'

		if cart_type == 'prestashop':
			# prestashop_version = self.convert_version(cart_version, 2)
			# if convert_version > parse_version('1.6.9'):
			# 	return 'cart/prestashop17'
			# elif convert_version > parse_version('1.4.9'):
			# 	return 'cart/prestashop16'
			# elif convert_version > parse_version('1.3.9'):
			# 	return 'cart/prestashop14'
			# else:
			# 	return 'cart/prestashop13'
			prestashop_version = self.convert_version(cart_version, 2)
			if prestashop_version > 169:
				return 'cart/prestashop17'
			elif prestashop_version > 149:
				return 'cart/prestashop16'
			elif prestashop_version > 139:
				return 'cart/prestashop14'
			else:
				return 'cart/prestashop13'

		if cart_type == 'jigoshop':
			# jigoshop_version = self.convert_version(cart_version, 2)
			if convert_version >= parse_version('2.0.0'):
				return 'cart/jigoshop2'
			else:
				return 'cart/jigoshop'

		if cart_type == 'oxideshop':
			# version_parse = self.convert_version(cart_version, 2)
			if convert_version > parse_version('4.6.9'):
				return 'cart/oxideshop49'
			else:
				return 'cart/oxideshop44'

		if cart_type == 'cscart':
			# cscart_version = self.convert_version(cart_version, 2)
			if convert_version < parse_version('3.0.0'):
				return 'cart/cscart2'
			elif convert_version >= parse_version('4.10.1'):
				return 'cart/cscart410'

			else:
				return 'cart/cscart'

		if cart_type == '3dcart':
			return 'cart/3dcart'

		if cart_type == 'neto':
			return 'cart/neto'

		if cart_type == 'cubecart':
			if not cart_version:
				return 'basecart'
			# cubecart_version = self.convert_version(cart_version, 2)
			if convert_version > parse_version('4.9.9'):
				return 'cart/cubecart6'
			else:
				return 'cart/cubecart4'
		if cart_type == 'ubercart':
			# ubercart_version = self.convert_version(cart_version, 2)
			if convert_version < parse_version('3.0.0'):
				return 'cart/ubercart2'
			else:
				return 'cart/ubercart3'
		if cart_type == 'cart66':
			return 'cart/cart66'

		if cart_type == 'amazonstore':
			return 'cart/amazonstore'

		if cart_type == 'xtcommerce':
			# xtcommerce_version = self.convert_version(cart_version, 2)
			if convert_version > parse_version('4.9.9'):
				return 'cart/xtcommerce5'
			elif convert_version > parse_version('3.9.9'):
				return 'cart/xtcommerce4'
			else:
				return 'cart/xtcommerce3'
		if cart_type == 'abantecart':
			return 'cart/abantecart'

		if cart_type == 'ubercart':
			# uber_version = self.convert_version(cart_version, 2)
			if convert_version < parse_version('3.0.0'):
				return 'cart/ubercart2'
			else:
				return 'cart/ubercart3'
		if cart_type == 'loaded':
			# loaded_version = self.convert_version(cart_version, 2)
			if convert_version < parse_version('7.0.0'):
				return 'cart/loadedcommerce6'
			else:
				return 'cart/loadedcommerce'
		if cart_type == 'bigcartel':
			return 'cart/bigcartel'
		if cart_type == 'adobe':
			return 'cart/adobebusinesscatalyst'
		if cart_type == 'custom':
			return 'cart/custom'
		if cart_type == 'drupal':
			return 'cart/drupalcommerce1x'
		if cart_type == 'wpestore':
			return 'cart/wpestore'
		if cart_type == 'shopp':
			return 'cart/shopp'
		if cart_type == 'aspdotnetstorefront':
			return 'cart/aspdotnetstorefront'
		if cart_type == 'ci_nopcommerce':
			return 'cart/nopcommerceci'
		if cart_type == 'marketpress':
			# marketpress_version = self.convert_version(cart_version, 2)
			if convert_version < parse_version('3.0.0'):
				return 'cart/marketpress'
			else:
				return 'cart/marketpress3'
		if cart_type == 'shopware':
			if not cart_version:
				return 'cart/shopware5'
			# virtuemart_version = self.convert_version(cart_version, 2)
			if convert_version >= parse_version('6.0.0'):
				return 'cart/shopware6'
			else:
				return 'cart/shopware5'
		if cart_type == 'joomshopping':
			return 'cart/joomshopping'
		if cart_type == 'gambio':
			return 'cart/gambio'
		if cart_type:
			return 'cart/' + cart_type
		return 'basecart'

	def old_get_cart(self, cart_type, cart_version = None, special_type = False):

		if special_type and cart_type == 'magento':
			if not cart_version:
				return 'basecart'
			version = cart_version.split('.')
			if to_len(version) > 2 and to_int(version[0]) == 1 and to_int(version[1]) > 9:
				return 'cart_magento_magento19'
			magento_version = self.convert_version(cart_version, 2)
			if magento_version > 200:
				if 'ee' in cart_version:
					return 'cart_magento_magento2ee'

				return 'cart_magento_magento2'
			elif magento_version > 149:
				return 'cart_magento_magento19'
			elif magento_version >= 141:
				return 'cart_magento_magento14'
			else:
				return 'cart_magento_magento13'

		if special_type and cart_type == 'prestashop':
			if not cart_version:
				return 'basecart'
			prestashop_version = self.convert_version(cart_version, 2)
			if prestashop_version > 169:
				return 'cart_prestashop_prestashop17'
			elif prestashop_version > 149:
				return 'cart_prestashop_prestashop16'
			elif prestashop_version > 139:
				return 'cart_prestashop_prestashop14'
			else:
				return 'cart_prestashop_prestashop13'

		if cart_type == 'volusion':
			return 'cart_volusion'

		if cart_type == 'lightspeed':
			return 'cart_lightspeed'

		if cart_type == 'yahoostore':
			return 'cart_yahoostore'

		if cart_type == 'shopify':
			return 'cart_shopify'

		if cart_type == 'oscommerce':
			return 'cart_oscommerce'

		if cart_type == 'zencart':
			return 'cart_zencart'

		if cart_type == 'interspire':
			return 'cart_interspire'

		if cart_type == 'bigcommerce':
			return 'cart_bigcommerce'

		if cart_type == 'mivamerchant':
			return 'cart_mivamerchant'

		if cart_type == 'woocommerce':
			return 'cart_woocommerce'

		if cart_type == 'weebly':
			return 'cart_weebly'

		if cart_type == 'ekm':
			return 'cart_ekm'

		if cart_type == 'epages':
			return 'cart_epages'

		if cart_type == 'summercart':
			return 'cart_summercart'

		if cart_type == 'squarespace':
			return 'cart_squarespace'

		if cart_type == 'nopcommerce':
			return 'cart_nopcommerce'
		if cart_type == 'aspdotnetstorefront':
			return 'cart_aspdotnetstorefront'
		if cart_type == 'pinnaclecart':
			return 'cart_pinnaclecart'

		if cart_type == 'wpecommerce':
			wpecommerce_version = self.convert_version(cart_version, 2)
			if wpecommerce_version > 299 and wpecommerce_version != 381:
				return 'cart_wpecommerce38'
			if wpecommerce_version == 381:
				return 'cart_wpecommerce3814'
			return 'cart_wpecommerce'

		if cart_type == 'hikashop':
			return 'cart_hikashop'

		if cart_type == 'randshop':
			return 'cart_randshop'

		if cart_type == 'xcart':
			# if not cart_version:
			# 	return 'basecart'
			xcart_version = self.convert_version(cart_version, 2)
			if xcart_version <= 459 and xcart_version > 439:
				return 'cart_xcart4'
			elif xcart_version < 500 and xcart_version > 459:
				return 'cart_xcart46'
			elif xcart_version > 499:
				return 'cart_xcart5'
			else:
				return 'cart_xcart'

		if cart_type == 'jshop':
			return 'cart_jshop'

		if cart_type == 'wix':
			return 'cart_wix'

		if cart_type == 'americommerce':
			return 'cart_americommerce'

		if cart_type == 'ecwid':
			return 'cart_ecwid'

		if cart_type == 'magento':
			if not cart_version:
				return 'basecart'
			version = cart_version.split('.')
			if to_len(version) > 2 and to_int(version[0]) == 1 and to_int(version[1]) > 9:
				return 'cart_magento19'
			magento_version = self.convert_version(cart_version, 2)
			if magento_version > 200:
				if 'ee' in cart_version:
					return 'cart_magento2ee'
				return 'cart_magento2'
			elif magento_version > 149:
				return 'cart_magento19'
			elif magento_version > 141:
				return 'cart_magento14'
			else:
				return 'cart_magento13'
		if cart_type == 'opencart' or cart_type == 'mijoshop':
			opencart_version = self.convert_version(cart_version, 2)
			if opencart_version >= 300:
				return 'cart_opencart3'
			elif opencart_version < 300 and opencart_version > 199:
				return 'cart_opencart2'
			elif opencart_version < 200 and opencart_version > 151:
				return 'cart_opencart15'
			elif opencart_version < 152 and opencart_version > 129:
				return 'cart_opencart14'
			else:
				return 'cart_opencart12'

		if cart_type == 'virtuemart':
			if not cart_version:
				return 'basecart'
			virtuemart_version = self.convert_version(cart_version, 2)
			if virtuemart_version < 200:
				return 'cart_virtuemart1'
			else:
				return 'cart_virtuemart2'

		if cart_type == 'prestashop':
			prestashop_version = self.convert_version(cart_version, 2)
			if prestashop_version > 169:
				return 'cart_prestashop17'
			elif prestashop_version > 149:
				return 'cart_prestashop16'
			elif prestashop_version > 139:
				return 'cart_prestashop14'
			else:
				return 'cart_prestashop13'

		if cart_type == 'jigoshop':
			jigoshop_version = self.convert_version(cart_version, 2)
			if jigoshop_version >= 200:
				return 'cart_jigoshop2'
			else:
				return 'cart_jigoshop'

		if cart_type == 'oxideshop':
			oxideshop_version = self.convert_version(cart_version, 2)
			if oxideshop_version > 469:
				return 'cart_oxideshop49'
			else:
				return 'cart_oxideshop44'

		if cart_type == 'cscart':
			cscart_version = self.convert_version(cart_version, 2)
			if cscart_version < 300:
				return 'cart_cscart2'
			else:
				return 'cart_cscart'

		if cart_type == '3dcart':
			return 'cart_3dcart'

		if cart_type == 'neto':
			return 'cart_neto'

		if cart_type == 'cubecart':
			if not cart_version:
				return 'basecart'
			cubecart_version = self.convert_version(cart_version, 2)
			if cubecart_version > 499:
				return 'cart_cubecart6'
			else:
				return 'cart_cubecart4'
		if cart_type == 'ubercart':
			ubercart_version = self.convert_version(cart_version, 2)
			if ubercart_version < 300:
				return 'cart_ubercart2'
			else:
				return 'cart_ubercart3'
		if cart_type == 'cart66':
			return 'cart_cart66'

		if cart_type == 'amazonstore':
			return 'cart_amazonstore'

		if cart_type == 'xtcommerce':
			xtcommerce_version = self.convert_version(cart_version, 2)
			if xtcommerce_version > 499:
				return 'cart_xtcommerce5'
			elif xtcommerce_version > 399:
				return 'cart_xtcommerce4'
			else:
				return 'cart_xtcommerce3'
		if cart_type == 'abantecart':
			return 'cart_abantecart'

		if cart_type == 'ubercart':
			uber_version = self.convert_version(cart_version, 2)
			if uber_version < 300:
				return 'cart_ubercart2'
			else:
				return 'cart_ubercart3'
		if cart_type == 'loaded':
			loaded_version = self.convert_version(cart_version, 2)
			if loaded_version < 700:
				return 'cart_loadedcommerce6'
			else:
				return 'cart_loadedcommerce'
		if cart_type == 'bigcartel':
			return 'cart_bigcartel'
		if cart_type == 'adobe':
			return 'cart_adobebusinesscatalyst'
		if cart_type == 'custom':
			return 'cart_custom'
		if cart_type == 'drupal':
			return 'cart_drupalcommerce1x'
		if cart_type == 'wpestore':
			return 'cart_wpestore'
		if cart_type == 'shopp':
			return 'cart_shopp'

		if cart_type == 'ci_nopcommerce':
			return 'cart_nopcommerceci'
		if cart_type == 'marketpress':
			marketpress_version = self.convert_version(cart_version, 2)
			if marketpress_version < 300:
				return 'cart_marketpress'
			else:
				return 'cart_marketpress3'
		return 'basecart'

	def get_seo(self, cart_type):
		return 'seo_' + cart_type.lower().strip() + '_default'

	def check_migration_id(self, customer_migration_id):
		cart_type = self._notice['src']['cart_type'] if self._notice else None
		target_type = self._notice['target']['cart_type'] if self._notice else None
		url = self._notice['src']['cart_url'] if self._notice else None
		check_migration_id = self.request_by_post('http://litextension.com/migration_id.php', {
			'user': "bGl0ZXg=",
			'pass': "YUExMjM0NTY=",
			'action': "Y2hlY2s=",
			'migration_id': string_to_base64(customer_migration_id),
			'cart_type': string_to_base64(cart_type),
			'target_type': string_to_base64(target_type),
			'url': string_to_base64(url),
			'save': 'MQ=='
		})
		if check_migration_id:
			check_migration_id = php_unserialize(base64_to_string(check_migration_id))
		return check_migration_id

	def get_source_custom_cart(self, _migration_id):
		return 'custom_' + _migration_id + '_source'

	def get_target_custom_cart(self, _migration_id):
		return 'custom_' + _migration_id + '_target'

	def get_source_cart(self):
		check = False
		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
			check = True
		elif (source_cart_type == 'prestashop') and (target_cart_type == 'prestashop'):
			check = True
		cart_version = self._notice['src']['config']['version']
		cart_name = self.get_cart(source_cart_type, cart_version, check)
		source_cart = get_model(cart_name)
		return source_cart

	def get_target_cart(self):
		check = False
		source_cart_type = self._notice['src']['cart_type']
		target_cart_type = self._notice['target']['cart_type']
		if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
			check = True
		elif (source_cart_type == 'prestashop') and (target_cart_type == 'prestashop'):
			check = True
		cart_version = self._notice['target']['config']['version']
		cart_name = self.get_cart(target_cart_type, cart_version, check)
		source_cart = get_model(cart_name)
		return source_cart

	def convert_version(self, v, num):
		if not v:
			return False
		digits = v.split('.')
		version = 0
		if isinstance(digits, list):
			for index, val in enumerate(digits):
				if index <= num:
					version += to_int(val[0]) * pow(10, max(0, num - index))
		return version

	def get_connector_url(self, action, token = None, cart_type = None):
		if not cart_type:
			cart_type = self.get_type()
		if not token:
			token = self._notice[cart_type]['config']['token']
		connector_suffix = self.CONNECTOR_SUFFIX
		if self._notice[cart_type]['config'].get('path_connector'):
			connector_suffix = self._notice[cart_type]['config'].get('path_connector') + connector_suffix
		url = self.get_url_suffix(connector_suffix)
		url += '?action=' + action + '&token=' + token
		url += '&cart_type=' + self._notice[self._type]['cart_type']
		url += "&time=" + to_str(to_int(time.time()))
		return url

	def get_url_suffix(self, suffix):
		url = self._cart_url.rstrip('/') + '/' + suffix.lstrip('/')
		return url

	def get_connector_data(self, url, data = None, proxies = ''):
		if not proxies and self._type and self._notice[self._type]['config'].get('proxy'):
			proxies = self._notice[self._type]['config'].get('proxy')
		origin_data = data
		if data:
			data = self.add_table_prefix(data)
			data = self.encode_connector_data(data)

		start_time = time.time()
		method = 'request_by_' + self.connector_method

		result = getattr(self, method)(url, data, proxies = proxies)
		time_str = to_str(time.time() - start_time) + 's'

		if self.LOG_QUERIES:
			self.log_time(time_str, url, origin_data)
		solve = self.solve_result_connector_data(result)
		if (not result or solve['result'] != 'success') and self.connector_method == 'post':
			get_result = self.request_by_get(url, data, proxies = proxies)
			get_solve = self.solve_result_connector_data(get_result)
			if get_solve['result'] == 'success':
				self.connector_method = 'get'
				return get_solve
		# if solve['result'] != 'success' and ('Your IP address was temporarily blocked by our IDS' in to_str(result) or 'Access Denied' in to_str(result)) and not proxies:
		# 	return self.get_connector_data(url, data, self.PROXY_HOST)
		retry = 0
		# retry if not decode data
		while (((not result or solve['result'] != 'success') and self.get_type() == 'src') or (solve and solve['result'] == 'decode')) and self._notice['running'] and not self._has_retry:
			if 'action=clearcache' in url:
				return response_success()
			msg_log = dict()
			msg_log['data'] = to_str(json_encode(origin_data))
			msg_log['data_encode'] = data
			msg_log['method'] = 'POST'
			msg_log['status'] = 200
			msg_log['response'] = to_str(result)
			retry += 1
			if not result and self.get_type() == 'src':
				msg = 'Src export failed'
			else:
				msg = 'Decode data failed'
			msg_log['error'] = msg + ', delay ' + to_str(3 * retry) + 'm and retry ' + to_str(retry) + " time"
			self.log_error(url, msg_log, to_str(self.get_type()) + '_delay')
			self.sleep_time(retry * 3 * 60)
			result = self.request_by_post(url, data)
			solve = self.solve_result_connector_data(result)
			if retry > 5:
				break
		if solve and solve['result'] != 'success':
			self.log(solve)
		return solve

	def solve_result_connector_data(self, result):
		if not result:
			return response_error()
		solve = self.gzinflate(result)
		solve = json_decode(solve)
		if not solve:
			solve = base64_to_string(result)
			solve = json_decode(solve)
		if not solve:
			return create_response('decode')
		return solve

	def gzinflate(self, base64_string):
		try:
			compressed_data = base64.b64decode(base64_string)
			res = zlib.decompress(compressed_data, -15)
			res = res.decode('utf-8')
			return res
		except:
			return False

	def gzdeflate(self, string):
		if isinstance(string, (list, dict)):
			string = json_encode(string)
		try:
			compressed_data = zlib.compress(string.encode())[2:-4]
			res = base64.b64encode(compressed_data)
			res = res.decode('utf-8')
			return res
		except:
			return False

	def uploadImageConnector(self, image_process, save_path, rename = None, override = None, is_proxy = False, check_map = True, ssl_image = False):
		if not image_process or ('download_image' in self._notice['target']['config'] and not self._notice['target']['config']['download_image']):
			return False
		param_rename = True
		param_override = False
		if self._notice['config'].get('ignore_existed_images') or self._notice['config'].get('reset') or self._notice['config'].get('remigrate'):
			param_rename = False
			param_override = False
		else:
			param_rename = True
		if override is not None:
			param_override = override

		if rename is not None:
			param_rename = rename
		url_image = self.get_connector_url('image')
		in_map = False
		if not is_proxy and check_map:
			path = self.get_map_field_by_src(self.TYPE_PATH_IMAGE, None, image_process['url'], field = 'code_desc')
			if path:
				in_map = True
				param_rename = False
				param_override = False
				save_path = path
			else:
				param_rename = True
				param_override = False
		image_process['url'] = image_process['url'].replace('http://', 'https://') if ssl_image or self.ssl_image else image_process['url']
		params = {
			'url': self.URL_PROXY + image_process['url'] if is_proxy or self.image_proxy else image_process['url'],
			'rename': param_rename
		}
		if self._notice['src']['config'].get('auth'):
			auth_user = to_str(self._notice['src']['config']['auth'].get('user'))
			auth_pass = to_str(self._notice['src']['config']['auth'].get('pass'))
			params['http_auth'] = {
				'user': auth_user,
				'pass': auth_pass,
			}
		if param_override:
			params['override'] = param_override
		time_start = time.time()
		image_import = self.get_connector_data(url_image, {
			'images': json.dumps({
				'ci': {
					'type': 'download',
					'path': save_path,
					'params': params
				}
			})
		})
		self.log(to_str(time.time() - time_start) + 's: ' + image_process['url'], 'time_images')
		image_import_path = False
		if image_import and image_import['result'] == 'success' and image_import.get('data', dict()).get('ci'):
			if is_proxy:
				self.image_proxy = True
			if ssl_image:
				self.ssl_image = True
			image_import_path = image_import['data']['ci']
			if not in_map:
				self.insert_map(self.TYPE_PATH_IMAGE, None, None, image_process['url'], image_import_path)
			return image_import_path
		if not is_proxy and self.image_proxy is None:
			return self.uploadImageConnector(image_process, save_path, rename, override, is_proxy = True, ssl_image = ssl_image)
		if not ssl_image and self.ssl_image is None and 'http://' in image_process['url']:
			return self.uploadImageConnector(image_process, save_path, rename, override, is_proxy = False, ssl_image = True)
		if ssl_image:
			self.ssl_image = False
		if is_proxy:
			self.image_proxy = False
		if image_import and image_import.get('error'):
			msg = 'Image Error '
			msg += '\n Url Image: ' + params['url']
			msg += '\n Save Path: ' + save_path
			msg += '\n Params: ' + to_str(params)
			msg += '\n Error: ' + to_str(image_import.get('error'))
			self.log(msg, 'image_error')

		return image_import_path

	def move_image_connector(self, image_process, save_path, old_path, rename = True):
		params = {
			'url': image_process['url'],
			'rename': rename,
			'override': False,
			'move': save_path,
		}
		image_import = self.get_connector_data(self.get_connector_url('image'), {
			'images': json.dumps({
				'ci': {
					'type': 'move',
					'path': old_path,
					'params': params
				}
			})
		})
		image_import_path = False
		if image_import and image_import['result'] == 'success' and image_import.get('data', dict()).get('ci'):
			image_import_path = image_import['data']['ci']
			self.insert_map(self.TYPE_PATH_IMAGE, None, None, image_process['url'], image_import_path)
			return image_import_path
		if image_import and image_import.get('error'):
			msg = 'Image Error '
			msg += '\n Url Image: ' + params['url']
			msg += '\n Save Path: ' + save_path
			msg += '\n Params: ' + to_str(params)
			msg += '\n Error: ' + to_str(image_import.get('error'))
			self.log(msg, 'image_error')

		return image_import_path

	def sync_connector_object(self, data, extra):
		if data['data'] and extra['data']:
			for k, v in extra['data'].items():
				if not (k in data['data']):
					data['data'][k] = v
		return data

	def encode_connector_data(self, data):
		encode_data = dict()
		for k, v in data.items():
			if parse_version(to_str(self._notice[self._type]['config'].get('connector_version'))) >= parse_version('1.0.1'):
				encode_data[k] = self.gzdeflate(v)
			else:
				encode_data[k] = string_to_base64(v)
		return encode_data

	def add_table_prefix(self, data):
		if 'query' in data:
			cart_type = self.get_type()
			prefix = to_str(self._notice[cart_type]['config']['table_prefix'])
			queries = json.loads(data['query'])
			if 'serialize' in data:
				add = dict()
				if isinstance(queries, list):
					for key, query in enumerate(queries):
						query['query'] = query['query'].replace('_DBPRF_', prefix)
						add[key] = query
				elif isinstance(queries, dict):
					for table, query in queries.items():
						query['query'] = query['query'].replace('_DBPRF_', prefix)
						add[table] = query
				data['query'] = json.dumps(add)
			else:
				query = queries
				query['query'] = query['query'].replace('_DBPRF_', prefix)
				data['query'] = json.dumps(query)
		return data

	def request_by_method(self, method, url, data, custom_headers = None, auth = None, proxies = ''):
		timeout = 300
		if not self._notice or not self._notice['running']:
			timeout = 30
		self._clear_entity_warning = True
		self._total_time_sleep = 0
		self._stop_action = False
		headers = custom_headers
		if custom_headers and isinstance(custom_headers, dict):
			headers = list()
			for key, value in custom_headers.items():
				headers.append(to_str(key) + ': ' + to_str(value))

		data_log = dict()
		origin_data = data

		# retry loop
		retry = 5
		res = False
		retry_503 = False
		while retry > 0:
			try:
				response_head = io.BytesIO()
				c = pycurl.Curl()
				c.setopt(pycurl.WRITEFUNCTION, response_head.write)
				c.setopt(pycurl.URL, url)
				c.setopt(pycurl.HTTPHEADER, headers)
				c.setopt(pycurl.USERAGENT, self.USER_AGENT)
				c.setopt(pycurl.FOLLOWLOCATION, 1)
				c.setopt(pycurl.SSL_VERIFYPEER, 0)
				c.setopt(pycurl.TIMEOUT, timeout)
				c.setopt(pycurl.CONNECTTIMEOUT, timeout)
				if proxies:
					c.setopt(pycurl.PROXY, proxies)
				if self.get_type() and self._notice[self.get_type()]['config'].get('auth'):
					auth_user = to_str(self._notice[self.get_type()]['config']['auth'].get('user'))
					auth_pass = to_str(self._notice[self.get_type()]['config']['auth'].get('pass'))
					c.setopt(pycurl.USERPWD, auth_user + ':' + auth_pass)

				if method == "post" and data:
					c.setopt(pycurl.POST, 1)
				if method == "put":
					c.setopt(pycurl.CUSTOMREQUEST, "PUT")
				if method == "delete":
					c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
				if data and method != "get":
					if isinstance(data, dict) or isinstance(data, list):
						data = urlencode(data)
					c.setopt(pycurl.POSTFIELDS, data)
				c.perform()
				if 'action=clearcache' in url:
					return string_to_base64(json_decode(response_success()))
				# if url == self._notice['config']:
				# 	return response_success()
				status = c.getinfo(pycurl.HTTP_CODE)
				# res = response_head.getvalue().decode('utf-8')
				self.last_status = to_int(status)
				try:
					res = response_head.getvalue().decode('utf-8')
				except Exception as e:
					try:
						res = response_head.getvalue().decode()
					except Exception as e:
						res = False
				if status in [403, 406] and to_str(method).lower() == 'post' and self._type and self._notice[self._type].get('setup_type') == 'connector':
					return self.request_by_get(url, data, custom_headers, auth, proxies)
				if (status in [403, 406] or status > 500) and not proxies:
					c.close()
					response_head.close()
					return self.request_by_method(method, url, data, custom_headers, auth, proxies = self.PROXY_HOST)
				# if 'An appropriate representation of the requested resource could not be found on this server. This error was generated by Mod_Security.' in res:
				# 	msg_log = dict()
				# 	msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
				# 	msg_log['data_encode'] = origin_data
				# 	msg_log['method'] = method
				# 	msg_log['status'] = status
				# 	msg_log['response'] = res
				# 	msg_log['error'] = ' Delay 1m: status = ' + to_str(status), 'delay'
				# 	self.log_error(url, msg_log, to_str(self.get_type()) + '_status')
				# 	self.sleep_time(60, status)

				# all status  >= 300, count down retry.
				if status >= 300:
					retry -= 1
					# for case status = 503: need to slow down migration speed
					if status == 503:
						if not self.retry_503:
							self.retry_503 = True
						if not self.max_time_delay_503:
							if self.time_delay_503 == 0:
								self.time_delay_503 = 0.25
							elif self.time_delay_503 < 2:
								self.time_delay_503 = self.time_delay_503 * 2
							self.sleep_time(self.time_delay_503, 503)
						else:
							self.sleep_time(self.max_time_delay_503, 503)
					elif status in [403, 406]:
						msg_log = dict()
						msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
						msg_log['data_encode'] = origin_data
						msg_log['method'] = method
						msg_log['status'] = status
						msg_log['response'] = res
						msg_log['error'] = ' Delay ' + to_str(retry) + 'm: status = ' + to_str(status), 'not Acceptable'
						self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
						self.sleep_time(60 * retry, status)
					elif status == 404 and self._notice[self._type]['setup_type'] == 'connector':
						if not proxies:
							c.close()
							response_head.close()
							return self.request_by_method(method, url, data, custom_headers, auth, proxies = self.PROXY_HOST)
						if retry <= 0:
							self._stop_action = True
							self.sleep_time(0, 404, warning = True, resume_action = True)
						else:
							msg_log = dict()
							msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
							msg_log['data_encode'] = origin_data
							msg_log['method'] = method
							msg_log['status'] = 404
							msg_log['response'] = res
							msg_log['error'] = ' Delay ' + to_str(5 * (5 - retry)) + 's: status = 404 not found'
							self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
							self.sleep_time(5 * (5 - retry), status)
					# sleep 10p for status >= 500 (!=503) and only in case migration is in step3 ( running )
					elif status >= 500 and self._notice['running']:
						msg_log = dict()
						msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
						msg_log['data_encode'] = origin_data
						msg_log['method'] = method
						msg_log['status'] = status
						msg_log['response'] = res
						msg_log['error'] = ' Delay 3m: status = ' + to_str(status), 'delay'
						self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
						self.sleep_time(3 * 60, status)

					if retry <= 0:
						if isinstance(data, dict):
							for key, value in origin_data.items():
								if self.gzinflate(value):
									data_log[key] = self.gzinflate(value)
								elif base64_to_string(value):
									data_log[key] = base64_to_string(value)
								else:
									data_log[key] = value
						msg_log = dict()
						msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
						msg_log['data_encode'] = origin_data
						msg_log['method'] = method
						msg_log['status'] = status
						msg_log['response'] = res
						msg_log['error'] = ''
						self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
				# status < 300: success
				else:
					# for case: need to slow down migration speed
					if self.retry_503 and not self.max_time_delay_503:
						self.max_time_delay_503 = self.time_delay_503
					# end 503
					break
				c.close()
				response_head.close()
			except pycurl.error as e:
				retry -= 1
				error_code = e.args[0]
				if error_code == pycurl.TIMEOUT:
					retry = 0
					error = traceback.format_exc()
					msg_log = dict()
					msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
					msg_log['data_encode'] = origin_data
					msg_log['method'] = method
					msg_log['error'] = to_str(error)
					self.log_error(url, msg_log, to_str(self.get_type()) + '_timeout', is_proxies = proxies)
					self.sleep_time(0.01, 'timeout', warning = True)
					break
				if retry <= 0:
					try:
						if isinstance(data, dict):
							for key, value in origin_data.items():
								if self.gzinflate(value):
									data_log[key] = self.gzinflate(value)
								elif base64_to_string(value):
									data_log[key] = base64_to_string(value)
								else:
									data_log[key] = value
					except Exception:
						pass
					error = e.args[1]
					msg_log = dict()
					msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
					msg_log['data_encode'] = origin_data
					msg_log['method'] = method
					msg_log['error'] = to_str(error)
					self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
			except Exception as e:
				retry -= 1
				res = False
				if retry <= 0:
					try:
						if isinstance(data, dict):
							for key, value in origin_data.items():
								if self.gzinflate(value):
									data_log[key] = self.gzinflate(value)
								elif base64_to_string(value):
									data_log[key] = base64_to_string(value)
								else:
									data_log[key] = value
					except Exception:
						pass
					error = traceback.format_exc()
					msg_log = dict()
					msg_log['data'] = to_str(json_encode(data_log) if data_log else data)
					msg_log['data_encode'] = origin_data
					msg_log['method'] = method
					msg_log['error'] = to_str(error)
					self.log_error(url, msg_log, to_str(self.get_type()) + '_status', is_proxies = proxies)
		self._has_retry = (retry < 5)
		return res

	def request_by_post(self, url, data, custom_headers = None, auth = None, proxies = ''):
		return self.request_by_method("post", url, data, custom_headers, auth, proxies = proxies)

	def request_by_get(self, url, data = None, custom_headers = None, auth = None, proxies = ''):
		if data:
			if '?' not in url:
				url += '?'
			else:
				url += '&'
			if isinstance(data, dict):
				url += urllib.parse.urlencode(data)
			elif isinstance(data, str):
				url += data
		return self.request_by_method("get", url, data, custom_headers, auth, proxies = proxies)

	def request_by_put(self, url, data, custom_headers = None, auth = None, proxies = ''):
		return self.request_by_method("put", url, data, custom_headers, auth, proxies = proxies)

	def request_by_delete(self, url, data, custom_headers = None, auth = None, proxies = ''):
		return self.request_by_method("delete", url, data, custom_headers, auth, proxies = proxies)

	def old_request_by_post(self, url, data, custom_headers = None, auth = None):
		# if 'pycurl' in self._notice[self._type]['config'] and self._notice[self._type]['config']['pycurl']:
		# 	return self.pycurl_request_by_post( url, data, custom_headers, auth)
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'

		res = False
		try:
			r = requests.post(url, data, headers = custom_headers, auth = auth)

			res = r.text
			r.raise_for_status()
		except requests.exceptions.HTTPError as errh:
			self.log("Http Error:" + to_str(errh) + " : " + to_str(res))
		except requests.exceptions.ConnectionError as errc:
			self.log("Error Connecting:" + to_str(errc) + " : " + to_str(res))
		except requests.exceptions.Timeout as errt:
			self._notice[self._type]['config']['pycurl'] = True
			self.log("Timeout Error:" + to_str(errt) + " : " + to_str(res))
		except requests.exceptions.RequestException as err:
			self.log("OOps: Something Else" + to_str(err) + " : " + to_str(res))
		return res

	def old_request_by_get(self, url, data = None, custom_headers = None, auth = None):
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'

		res = False
		if data:
			if isinstance(data, dict):
				url += '?' + urllib.parse.urlencode(data)
			elif isinstance(data, str):
				url += '?' + data
		try:
			r = requests.get(url, headers = custom_headers, auth = auth)
			res = r.text
			r.raise_for_status()
		except requests.exceptions.HTTPError as errh:
			self.log("Http Error:" + to_str(errh) + " : " + to_str(res))
		except requests.exceptions.ConnectionError as errc:
			self.log("Error Connecting:" + to_str(errc) + " : " + to_str(res))
		except requests.exceptions.Timeout as errt:
			self.log("Timeout Error:" + to_str(errt) + " : " + to_str(res))
		except requests.exceptions.RequestException as err:
			self.log("OOps: Something Else" + to_str(err) + " : " + to_str(res))
		return res

	def old_request_by_put(self, url, data, custom_headers = None, auth = None):
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'

		res = False
		try:
			r = requests.put(url, data, headers = custom_headers, auth = auth)
			res = r.text
			r.raise_for_status()
		except requests.exceptions.HTTPError as errh:
			self.log("Http Error:" + to_str(errh) + " : " + to_str(res))
		except requests.exceptions.ConnectionError as errc:
			self.log("Error Connecting:" + to_str(errc) + " : " + to_str(res))
		except requests.exceptions.Timeout as errt:
			self.log("Timeout Error:" + to_str(errt) + " : " + to_str(res))
		except requests.exceptions.RequestException as err:
			self.log("OOps: Something Else" + to_str(err) + " : " + to_str(res))
		return res

	def old_request_by_delete(self, url, data, custom_headers = None, auth = None):
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'

		res = False
		try:
			r = requests.delete(url, data = data, headers = custom_headers, auth = auth)
			res = r.text
			r.raise_for_status()
		except requests.exceptions.HTTPError as errh:
			self.log("Http Error:" + to_str(errh) + " : " + to_str(res))
		except requests.exceptions.ConnectionError as errc:
			self.log("Error Connecting:" + to_str(errc) + " : " + to_str(res))
		except requests.exceptions.Timeout as errt:
			self.log("Timeout Error:" + to_str(errt) + " : " + to_str(res))
		except requests.exceptions.RequestException as err:
			self.log("OOps: Something Else" + to_str(err) + " : " + to_str(res))
		return res

	def image_exist(self, url, path = ''):
		image_process = self.process_image_before_import(url, path)
		try:
			exist = requests.head(image_process['url'], headers={"User-Agent": self.USER_AGENT}, timeout = 10, verify = False)
		except requests.exceptions.Timeout as errt:
			self.log("image " + image_process['url'] + 'connection timeout', self._type + '_image')
			return False
		except Exception as e:
			self.log_traceback('img_exist')
			return False
		return exist.status_code == requests.codes.ok

	# process image
	def process_image_before_import(self, url, path):
		if not path:
			full_url = url
			path = strip_domain_from_url(url)
		else:
			full_url = join_url_path(url, path)
		path = re.sub(r"[^a-zA-Z0-9\.\-\_/]", '', path)
		full_url = self.parse_url(full_url)
		return {
			'url': full_url,
			'path': path
		}

	def parse_url(self, url):
		if not url:
			return url
		scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
		path = urllib.parse.quote(path, '/%')
		qs = urllib.parse.quote_plus(qs, ':&=')
		new_url = to_str(urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor)))
		if anchor and anchor.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.gif')):
			new_url = to_str(new_url).replace('#', '%23')
		return new_url

	def add_prefix_path(self, path, prefix = ''):
		join_path = ''
		if prefix:
			join_path += prefix.rstrip('\\/') + '/'
		join_path += path.lstrip('\\/').replace('?', '')
		return join_path

	def change_img_src_in_text(self, body_html, img_desc = False, prefix = ''):
		if not body_html or (not self._notice['config']['img_des'] and not img_desc):
			# if body_html:
			# 	body_html = re.compile(r"<img[^>]+>").sub('', body_html)
			# 	return body_html
			# else:
			return body_html
		match = re.findall(r"<img[^>]+>", body_html)
		# if match and self._notice['config']['strip_html']:
		# 	i = 0
		# 	replace_img = dict()
		# 	for html_img in match:
		# 		str_repl = 'replace_image_' + to_str(i)
		# 		i += 1
		# 		replace_img[str_repl] = html_img
		# 		body_html = body_html.replace(html_img, str_repl)
		# 	body_html = self.strip_html_tag(body_html)
		# 	if replace_img:
		# 		for un_repl_img, un_html_img in replace_img.items():
		# 			body_html = body_html.replace(un_repl_img, un_html_img)
		links = list()
		if match:
			for img in match:
				img_src = re.findall(r"(src=[\"'](.*?)[\"'])", img)
				if not img_src:
					continue
				img_src = img_src[0]
				links.append(img_src[1])
		save_path_target = self._notice['target']['config']['image'] if self._notice['target']['config'].get('image') else self._notice['target']['config']['image_product']
		for link in links:
			# download and replace
			url = link
			scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
			basename = os.path.basename(path)
			if not netloc:
				url = self._notice['src']['cart_url'].strip('/') + '/' + url.strip('/')
			image_process = self.process_image_before_import(url, '')
			image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(basename, save_path_target.rstrip('/') + '/content'))
			if image_import_path:
				new_image = self.get_url_suffix(image_import_path)
				body_html = body_html.replace(link, new_image)
		return body_html

	# map
	def select_map(self, _migration_id = None, map_type = None, id_src = None, id_desc = None, code_src = None, code_desc = None, value = None):
		where = dict()
		if _migration_id:
			where['migration_id'] = _migration_id
		if map_type:
			where['type'] = map_type
		if id_src:
			where['id_src'] = id_src
		if id_desc:
			where['id_desc'] = id_desc
		if code_src:
			where['code_src'] = code_src
		if code_desc:
			where['code_desc'] = code_desc
		if value:
			where['value'] = value
		if not where:
			return None
		result = self.select_obj(TABLE_MAP, where)
		try:
			data = result['data'][0]
		except Exception as e:
			data = None
		return data

	def update_map(self, map_type, id_src = None, code_src = None, id_desc = None, code_desc = None, value = None, additional_data = None):
		if not id_src and not code_src:
			return response_error()
		data = dict()
		if id_desc:
			data['id_desc'] = id_desc
		if code_desc:
			data['code_desc'] = code_desc
		if value:
			data['value'] = value
		if additional_data:
			data['additional_data'] = additional_data
		where = dict()
		where['type'] = map_type
		if id_src:
			where['id_src'] = id_src
		if code_src:
			where['code_src'] = code_src

		result = self.update_obj(TABLE_MAP, data, where)
		return result

	def update_order_number_into_map(self, convert, new_order_id, id_src = None, code_src = None):
		order = self.get_map_field_by_src(self.TYPE_ORDER, id_src, code_src, field = 'additional_data')

	def construct_additional_data_map(self, src, target):
		return {
			'map': {
				'src': src,
				'target': target,
			}
		}

	def select_image_map(self, id_src = None, id_desc = None, code_src = None, code_desc = None, value = None):
		where = dict()
		where['migration_id'] = self._migration_id
		where['type'] = self.TYPE_IMAGE
		if id_src:
			where['id_src'] = id_src
		if id_desc:
			where['id_desc'] = id_desc
		if code_src:
			where['code_src'] = code_src
		if code_desc:
			where['code_desc'] = code_desc
		if value:
			where['value'] = value
		if not where:
			return None
		result = self.select_obj(TABLE_MAP, where)
		if result['result'] == 'success':
			data = result['data']
		else:
			data = list()
		return data

	def select_category_map(self, id_src = None, id_desc = None, code_src = None, code_desc = None, value = None):
		where = dict()
		where['migration_id'] = self._migration_id
		where['type'] = self.TYPE_CATEGORY
		if id_src:
			where['id_src'] = id_src
		if id_desc:
			where['id_desc'] = id_desc
		if code_src:
			where['code_src'] = code_src
		if code_desc:
			where['code_desc'] = code_desc
		if value:
			where['value'] = value
		if not where:
			return None
		result = self.select_obj(TABLE_MAP, where)
		if result['result'] == 'success':
			data = result['data']
		else:
			data = list()
		return data

	def select_all_attribute_map(self):
		where = dict()
		where['migration_id'] = self._migration_id
		where['type'] = self.TYPE_ATTR
		result = self.select_obj(TABLE_MAP, where)
		data = list()
		if result['result'] == 'success' and result['data']:
			data = result['data']

		result_data = list()
		if data:
			for row in data:
				value = json_decode(row['value'])
				if value and value.get(self._type):
					row['value'] = json_encode(value[self._type])
				result_data.append(value)

		return data

	def get_map_field_by_src(self, map_type = None, id_src = None, code_src = None, field = 'id_desc'):
		if not id_src and not code_src:
			return False
		_migration_id = self._migration_id
		if id_src:
			code_src = None
		# else:
		# 	code_src = None
		map_data = self.select_map(_migration_id, map_type, id_src, None, code_src)
		if not map_data:
			return False
		return map_data.get(field, False)

	def insert_map(self, map_type = None, id_src = None, id_desc = None, code_src = None, code_desc = None, value = None, additional_data = None):
		if to_int(id_src) == 0 and to_str(id_src) != '0':
			id_src = None
		data_insert = {
			'migration_id': self._migration_id,
			'type': map_type,
			'id_src': id_src,
			'code_src': code_src,
			'id_desc': id_desc,
			'code_desc': code_desc,
			'value': value,
		}
		if to_int(self._notice['mode']) == MIGRATION_DEMO and parse_version(self._notice.get('version', '1.0.0')) >= parse_version('1.0.3'):
			if not additional_data:
				additional_data = self.get_mapping_info_by_type(map_type, id_desc)
			if additional_data:
				data_insert['additional_data'] = json_encode(additional_data)
		insert = self.insert_obj(TABLE_MAP, data_insert)
		if (not insert) or (insert['result'] != 'success'):
			return False
		return insert['data']

	def get_mapping_info_by_type(self, entity_type, id_desc):
		if not self.convert_data:
			return ''
		mapping = dict()
		if entity_type == self.TYPE_PRODUCT:
			src = self.convert_data['sku'] if self.convert_data.get('sku') else self.convert_data.get('name')
			target = self.convert_data['name'] if self.convert_data.get('name') else id_desc
			mapping = self.construct_additional_data_map(src, target)
		if entity_type == self.TYPE_CUSTOMER:
			name = get_value_by_key_in_dict(self.convert_data, 'first_name', '') + ' ' + get_value_by_key_in_dict(self.convert_data, 'middle_name', '') + ' ' + get_value_by_key_in_dict(self.convert_data, 'last_name', '')
			name = to_str(name).strip(' ')
			mapping = self.construct_additional_data_map(name, self.convert_data['email'])

		if entity_type == self.TYPE_ORDER:
			src_id = self.convert_data['order_number'] if self.convert_data.get('order_number') else self.convert_data['id']
			target_id = id_desc
			mapping = self.construct_additional_data_map(src_id, target_id)

		return mapping
	# query connector
	def create_insert_query_connector(self, table, data, params = False, prefix = True):
		table = '_DBPRF_' + table if prefix else table
		query = "INSERT INTO `" + table + "` " + self.dict_to_insert_condition(data)
		res = {
			'type': 'insert',
			'query': query
		}
		if params:
			res['params'] = {
				'insert_id': True,
			}
		return res

	def create_select_query_connector(self, table, where, fields = None, prefix = True):
		table = '_DBPRF_' + table if prefix else table
		select = '*'
		if fields:
			if isinstance(fields, list):
				select = ','.join(fields)
			if isinstance(fields, str):
				select = fields
		query = "SELECT " + select + " FROM " + table
		if where:
			if isinstance(where, str):
				query += " WHERE " + where
			elif isinstance(where, dict):
				query += " WHERE " + self.dict_to_where_condition(where)
		return {
			'type': 'select',
			'query': query
		}

	def create_update_query_connector(self, table, data_set, where, prefix = True):
		table = '_DBPRF_' + table if prefix else table
		query = "UPDATE `" + table + "` SET " + self.dict_to_set_condition(data_set) + " WHERE " + self.dict_to_where_condition(where)
		return {
			'type': 'update',
			'query': query,
		}

	def create_delete_query_connector(self, table, where, prefix = True):
		table = '_DBPRF_' + table if prefix else table
		query = "DELETE FROM `" + table + "` WHERE " + self.dict_to_where_condition(where)
		return {
			'type': 'delete',
			'query': query,
		}

	def import_data_connector(self, query, import_type = 'query', entity_id = '', primary = False, params = True):
		if params:
			query['params'] = {
				'insert_id': True,
			}
		result = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if (not result) or (result['result'] != 'success') or (not result['data']):
			query_log = query['query']
			query_log = import_type.capitalize() + ' ' + to_str(entity_id) + ': ' + query_log
			if result and result.get('error'):
				query_log += ': ' + self.text_error_html(result['error'])
			if primary:
				import_type = import_type + '_errors'
			log(query_log, self._migration_id, import_type)
			self._query_error = query_log
			return False
		if not params:
			return True
		return result['data']

	def get_query_error(self):
		error = self._query_error
		self._query_error = None
		return error

	def import_tax_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'taxes', entity_id, primary, params)

	def import_manufacturer_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'manufacturers', entity_id, primary, params)

	def import_category_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'categories', entity_id, primary, params)

	def import_product_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'products', entity_id, primary, params)

	def import_customer_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'customers', entity_id, primary, params)

	def import_order_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'orders', entity_id, primary, params)

	def import_review_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'reviews', entity_id, primary, params)

	def import_page_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'pages', entity_id, primary, params)

	def import_blog_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'blogs', entity_id, primary, params)

	def import_coupon_data_connector(self, query, primary = False, entity_id = '', params = True):
		return self.import_data_connector(query, 'coupon', entity_id, primary, params)

	def select_data_connector(self, query, _type = 'select', is_log = True):
		data = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps(query),
		})
		if data and data['data'] is False:
			query = query['query']
			if 'error' in data and data['error']:
				query += ': ' + self.text_error_html(data['error'])
			self.log(query, _type, is_log)
		return data

	def query_data_connector(self, query, _type = 'query', is_log = True):
		data = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps(query),
		})
		if data and data['data'] is False:
			query = query['query']
			if 'error' in data and data['error']:
				query += ': ' + self.text_error_html(data['error'])
			self.log(query, _type, is_log)
		return data

	def select_multiple_data_connector(self, queries, _type = 'select', is_log = True):
		all_data = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries),
		})
		if all_data and all_data['data']:
			for key, res in all_data['data'].items():
				if res is False:
					try:
						msg = queries[key]['query'] + ': ' + self.text_error_html(all_data['error'][key])
					except Exception:
						msg = queries[key]['query']
					self.log(msg, _type, is_log)
		return all_data

	def query_multiple_data_connector(self, queries, _type = 'select', is_log = True):
		if isinstance(queries, list):
			queries = self.list_to_dict(queries)
		all_data = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries),
		})
		if all_data and all_data['data']:
			if isinstance(all_data['data'], list):
				all_data['data'] = self.list_to_dict(all_data['data'])
			for key, res in all_data['data'].items():
				if res is False:
					try:
						msg = queries[key]['query'] + ': ' + self.text_error_html(all_data['error'][key])
					except Exception:
						msg = queries[key]['query']
					self.log(msg, _type, is_log)
		return all_data

	def import_multiple_data_connector(self, queries, import_type = 'query', is_log = True, insert_id = False):
		result = True
		all_import = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries),
		})
		if (not all_import) or (not all_import['data']):
			return False
		all_import_data = all_import['data']
		all_error = all_import['error']
		if isinstance(all_import_data, list):
			for key, value in enumerate(all_import_data):
				if not value:
					try:
						msg = queries[int(key)]['query'] + ': ' + self.text_error_html(str(all_error[int(key)]))
					except Exception:
						msg = queries[int(key)]['query']

					self.log(msg, import_type, is_log)
					# log err
					result = False
		elif isinstance(all_import_data, dict):
			for key, value in all_import['data'].items():
				if not value:
					try:
						msg = queries[str(key)]['query'] + ': ' + self.text_error_html(str(all_error[str(key)]))
					except Exception:
						msg = queries[str(key)]['query']
					self.log(msg, import_type, is_log)
					# log err
					result = False
		result = all_import if insert_id else result
		return result

	def text_error_html(self, error):
		if not error:
			return ''
		if isinstance(error, list):
			msg = ''
			for row in error:
				msg += '<span class="message-alert-error">' + to_str(row) + '</span>'
				msg += '<br>'
			return msg
		return '<span class="message-alert-error">' + to_str(error) + '</span>'

	def text_warning_html(self, warning):
		if not warning:
			return ''
		if isinstance(warning, list):
			msg = ''
			for row in warning:
				msg += '<span class="message-alert-warning">' + to_str(row) + '</span>'
				msg += '<br>'
			return msg
		return '<span class="message-alert-warning">' + to_str(warning) + '</span>'

	# Convert result of query get count to count
	def list_to_count_import(self, data, name = False):
		if not data:
			return 0
		count = 0
		try:
			if name:
				count = data[0][name]
			else:
				count = data[0][0]
		except Exception:
			count = 0
		return count

	def warning_import_entity(self, type_import, id_import = None, code = None, error_code = ''):
		msg = type_import.capitalize() + ' '
		if code:
			msg += 'code: ' + code
		elif id_import:
			msg += 'id ' + to_str(id_import)
		msg += ' import failed.'
		if error_code:
			msg += 'Error ' + error_code
		return msg

	def remove_prefix_path(self, path, prefix):
		path = to_str(path).strip('/')
		prefix = prefix.strip('/')
		if prefix:
			prefix_length = to_len(prefix)
			prefix_check = path[0:prefix_length]
			if prefix.strip('/') == prefix_check.strip('/'):
				path = path[prefix_length:]
		return path.strip('/')

	# CONSTRUCT

	# manufacturer
	def construct_manufacturer(self):
		return {
			'id': None,
			'code': None,
			'status': True,
			'sort_order': '',
			'name': '',
			'description': '',
			'meta_title': '',
			'meta_keyword': '',
			'meta_description': '',
			'url': '',
			'thumb_image': {
				'label': '',
				'url': '',
				'path': '',
			},
			'created_at': None,
			'updated_at': get_current_time(),
			'languages': dict(),
		}

	def construct_manufacturer_lang(self):
		return {
			'name': '',
			'description': ''
		}

	# end manufacturer

	# taxes
	def construct_tax(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'url': '',
			'languages': dict(),
			'tax_products': dict(),
			'tax_customers': dict(),
			'tax_zones': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_tax_product(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_tax_customer(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_tax_zone(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
			'country': dict(),
			'state': dict(),
			'rate': dict(),
		}

	def construct_tax_zone_country(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
			'country_code': ''
		}

	def construct_tax_zone_state(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
			'state_code': ''
		}

	def construct_tax_zone_rate(self):
		return {
			'id': None,
			'code': None,
			'name': '',
			'languages': dict(),
			'created_at': None,
			'updated_at': get_current_time(),
			'rate': '',
			'priority': 0
		}

	# end taxes

	# category
	def construct_category(self):
		return {
			'id': None,
			'code': None,
			'parent': dict(),
			'active': False,
			'thumb_image': {
				'label': '',
				'url': '',
				'path': '',
			},
			'images': list(),
			'name': '',
			'description': '',
			'short_description': '',
			'url_key': '',
			'meta_title': '',
			'meta_keyword': '',
			'meta_description': '',
			'sort_order': 0,
			'created_at': None,
			'updated_at': get_current_time(),
			'languages': dict(),
			'category': dict(),
			'categories_ext': dict(),
			'seo': list(),
			'category_group': list()
		}

	def construct_category_parent(self):
		return self.construct_category()

	def construct_seo_category(self):
		return {
			'request_path': '',
			'type': self.SEO_DEFAULT,  # self.SEO_DEFAULT or self.SEO_301
			'default': True,  # True or False
			'store_id': '',
		}

	def construct_category_lang(self):
		return {
			'name': '',
			'description': '',
			'short_description': '',
			'meta_title': '',
			'meta_keyword': '',
			'meta_description': '',
		}

	# end category

	# product
	def construct_product(self):
		return {
			'id': None,
			'code': None,
			'type': '',
			'is_child': False,
			'store_ids': list(),
			'thumb_image': {
				'label': '',
				'url': '',
				'path': '',
				'status': True,
			},
			'images': list(),
			'name': '',
			'sku': '',
			'url_key': '',
			'description': '',
			'short_description': '',
			'meta_title': '',
			'meta_keyword': '',
			'meta_description': '',
			'tags': '',
			'price': 0.0000,
			'cost': 0.0000,
			'special_price': {
				'price': 0.0000,
				'start_date': '',
				'end_date': '',
			},
			'group_prices': list(),
			'tier_prices': list(),
			'weight': 0.0000,
			'length': 0.0000,
			'width': 0.0000,
			'height': 0.0000,
			'status': True,
			'manage_stock': False,
			'qty': 0,
			'is_in_stock': True,
			'tax': {
				'id': None,
				'code': None,
			},
			'manufacturer': {
				'id': None,
				'code': None,
				'name': None
			},
			'created_at': None,
			'updated_at': get_current_time(),
			'categories': list(),
			'languages': dict(),
			'options': list(),
			'group_parent_ids': list(),
			'attributes': list(),
			'children': list(),
			'group_child_ids': list(),
			'relate': {
				'parent': list(),
				'children': list(),
			},
			'seo': list(),
			'downloadable': list()
		}

	def construct_product_downloadable(self):
		return {
			'path': '',
			'name': '',
			'limit': '',# 0 if unlimited
			'max_day': '',
			'date_expired': '',
			'price': 0.00,
			'languages': '',
			'sample': {
				'path': '',
				'name': '',
			}
		}

	def construct_product_downloadable_language(self):
		return {
			'name': ''
		}
	def construct_seo_product(self):
		return {
			'request_path': '',
			'type': self.SEO_DEFAULT,  # self.SEO_DEFAULT or self.SEO_301
			'default': True,  # True or False
			'store_id': '',
			'category_id': None,
		}

	def construct_product_relation(self):
		return {
			'id': '',
			'code': '',
			'type': '',
		}

	def construct_product_relation_value(self):
		return {
			'id': '',
			'code': '',
		}

	def construct_product_child(self):
		child = self.construct_product()
		child['type'] = self.PRODUCT_SIMPLE
		child['is_child'] = True
		return child

	def construct_product_image(self):
		return {
			'label': '',
			'url': '',
			'path': '',
			'status': True,
		}

	def construct_product_option(self):
		return {
			'id': None,
			'code': None,
			'option_set': '',
			'option_group': '',
			'option_mode': self.OPTION_BACKEND,
			'option_type': '',
			'option_code': None,
			'option_name': '',
			'option_languages': dict(),
			# 'option'
			'required': False,
			'values': list(),
		}

	def construct_product_option_lang(self):
		return {
			'option_name': '',
			'option_price': '0.0000'
		}

	def construct_product_option_value(self):
		return {
			'id': None,
			'code': None,
			'option_value_code': None,
			'option_value_name': '',
			'option_value_qty': False,
			'option_value_sku': False,
			'option_value_languages': dict(),
			'option_value_price': 0.0000,
			'price_prefix': '+',
			'option_value_disabled': False,
			'thumb_image': {
				'label': '',
				'url': '',
				'path': '',
			},
		}

	def construct_product_option_value_lang(self):
		return {
			'option_value_name': '',
		}

	def construct_product_lang(self):
		return {
			'name': '',
			'description': '',
			'short_description': '',
			'meta_title': '',
			'meta_keyword': '',
			'meta_description': '',
			'price': '',
		}

	def construct_product_category(self):
		return {
			'id': None,
			'code': None,
			'position': 0
		}

	def construct_product_tier_price(self):
		return {
			'id': None,
			'code': None,
			'sites': list(),
			'languages': list(),
			'name': '',
			'tier_code': '',
			'group': list(),
			'qty': 0,
			'price': 0.0000,
			'start_date': '',
			'end_date': '',
			'price_type': 'fixed',
			'customer_group_id': 0,
		}

	def construct_product_child_attribute(self):
		return self.construct_product_attribute()

	def construct_product_attribute(self):
		return {
			'option_id': None,
			'option_code_save': None,
			'option_set': '',
			'option_group': '',
			'option_mode': self.OPTION_BACKEND,
			'option_type': '',
			'option_code': '',
			'option_name': '',
			'option_languages': dict(),
			'option_value_id': None,
			'option_value_code_save': None,
			'option_value_code': '',
			'option_value_name': '',
			'option_value_languages': dict(),
			'price': 0.0000,
			'price_prefix': '+',
		}

	# end product

	# customer
	def construct_customer(self):
		return {
			'phone': '',
			'id': None,
			'code': None,
			'note': '',
			'group_id': '',
			'username': '',
			'email': '',
			'password': '',
			'first_name': '',
			'middle_name': '',
			'last_name': '',
			'gender': '',
			'dob': '',
			'is_subscribed': False,
			'active': True,
			'capabilities': list(),
			'created_at': None,
			'updated_at': get_current_time(),
			'address': list(),
			'groups': list(),
			'balance': 0.00
		}

	def construct_customer_address(self):
		return {
			'id': None,
			'code': None,
			'first_name': '',
			'middle_name': '',
			'last_name': '',
			'gender': '',
			'address_1': '',
			'address_2': '',
			'city': '',
			'country': {
				'id': None,
				'code': None,
				'country_code': '',
				'name': '',
			},
			'state': {
				'id': None,
				'code': None,
				'state_code': '',
				'name': '',
			},
			'postcode': '',
			'telephone': '',
			'company': '',
			'fax': '',
			'default': {
				'billing': False,
				'shipping': False,
			},
			'billing': False,
			'shipping': False,
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_customer_group(self):
		return {
			'id': None,
			'code': None
		}

	# end customer

	# order
	def construct_order(self):
		return {
			'id': None,
			'order_number': None,
			'code': None,
			'status': '',
			'tax': {
				'title': '',
				'amount': 0.0000,
				'percent': 0.0000,
			},
			'discount': {
				'code': '',
				'title': '',
				'amount': 0.0000,
				'percent': 0.0000,
			},
			'shipping': {
				'title': '',
				'amount': 0.0000,
				'percent': 0.0000,
			},
			'subtotal': {
				'title': '',
				'amount': 0.0000,
			},
			'total': {
				'title': '',
				'amount': 0.0000,
			},
			'currency': '',
			'created_at': None,
			'updated_at': get_current_time(),
			'customer': dict(),
			'customer_address': dict(),
			'billing_address': dict(),
			'shipping_address': dict(),
			'payment': {
				'id': None,
				'code': None,
				'method': '',
				'title': ''
			},
			'items': list(),
			'history': list(),
		}

	def construct_order_customer(self):
		return {
			'id': None,
			'code': None,
			'username': '',
			'email': '',
			'first_name': '',
			'middle_name': '',
			'last_name': '',
		}

	def construct_order_address(self):
		return {
			'id': None,
			'code': None,
			'first_name': '',
			'middle_name': '',
			'last_name': '',
			'address_1': '',
			'address_2': '',
			'city': '',
			'country': {
				'id': None,
				'code': None,
				'country_code': '',
				'name': '',
			},
			'state': {
				'id': None,
				'code': None,
				'state_code': '',
				'name': '',
			},
			'postcode': '',
			'telephone': '',
			'company': '',
			'fax': '',
		}

	def construct_order_item(self):
		return {
			'id': None,
			'code': None,
			'product': {
				'id': None,
				'code': None,
				'name': '',
				'sku': '',
			},
			'qty': 0,
			'price': 0.0000,
			'original_price': 0.0000,
			'tax_amount': 0.0000,
			'tax_percent': 0.0000,
			'discount_amount': 0.0000,
			'discount_percent': 0.0000,
			'subtotal': 0.0000,
			'total': 0.0000,
			'options': list(),
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_order_item_option(self):
		return {
			'option_id': '',
			'option_code_save': '',
			'option_set': '',
			'option_group': '',
			'option_type': '',
			'option_code': '',
			'option_name': '',
			'option_value_id': '',
			'option_value_code_save': '',
			'option_value_code': '',
			'option_value_name': '',
			'price': 0.0000,
			'price_prefix': '+',
		}

	def construct_order_history(self):
		return {
			'id': None,
			'code': None,
			'status': '',
			'comment': '',
			'notified': False,
			'created_at': None,
			'updated_at': get_current_time(),
		}

	def construct_order_payment(self):
		return {
			'id': None,
			'code': None,
			'method': '',
			'title': ''
		}

	# end order

	# review
	def construct_review(self):
		return {
			'id': None,
			'code': None,
			'product': {
				'id': None,
				'code': None,
				'name': ''
			},
			'customer': {
				'id': None,
				'code': None,
				'name': ''
			},
			'title': '',
			'content': '',
			'status': '',
			'created_at': '',
			'updated_at': '',
			'rating': list()
		}

	def construct_review_rating(self):
		return {
			'id': None,
			'code': None,
			'rate_code': '',
			'rate': ''
		}

	# end review

	# coupon
	def construct_coupon(self):
		return {
			'id': None,
			'code': None,
			'title': '',
			'description': '',
			'status': True,
			'min_spend': None,
			'max_spend': None,
			'short_description': '',
			'from_date': None,
			'to_date': None,
			'times_used': None,
			'usage_limit': None,
			'usage_per_customer': None,
			'type': '',  # fix|percent
			'coupon_usage': list(),
			'customer_group': list(),
			'products': list(),
			'categories': list(),
			'discount_amount': None,
			'languages': dict(),
			'created_at': None,
			'updated_at': None,
		}

	def construct_coupon_language(self):
		return {
			'title': '',
			'description': ''
		}

	def construct_coupon_usage(self):
		return {
			'customer_id': None,
			'timed_usage': None,
		}

	# blog category
	def construct_blog_category(self):
		construct = self.construct_category()
		construct['is_blog'] = True
		construct['url_key'] = ''
		return construct

	def construct_blog_category_lang(self):
		return {
			'title': '',
			'description': '',
		}

	# blog post
	def construct_blog_post(self):
		return {
			'id': None,
			'code': '',
			'title': '',
			'store_id': '',
			'content': '',
			'short_content': '',
			'short_description': '',
			'description': '',
			'meta_title': '',
			'meta_keywords': '',
			'meta_description': '',
			'images': list(),
			'url_key': '',
			'thumb_image': {
				'label': '',
				'url': '',
				'path': '',
			},
			'categories': list(),
			'tags': '',
			'author_id': None,
			'status': True,
			'languages': dict(),
			'created_at': '',
			'updated_at': get_current_time()
		}

	def construct_blog_post_lang(self):
		return {
			'title': '',
			'short_description': '',
			'description': '',
			'meta_title': '',
			'meta_description': '',
			'status': True,
		}

	# cms page
	def construct_cms_page(self):
		return {
			'id': None,
			'code': '',
			'title': '',
			'short_description': '',
			'description': '',
			'content': '',
			'meta_title': '',
			'meta_keywords': '',
			'meta_description': '',
			'images': list(),
			'url_key': '',
			'parent_id': None,
			'status': True,
			'languages': dict(),
			'created_at': '',
			'updated_at': ''
		}

	# END CONSTRUCT

	def join_text_to_key(self, text, length = False, char = '-', lower = True):
		text += " "
		if length:
			length = to_int(length)
			text = text[0:length]
			if text.find(' ') != -1:
				text = text[0:text.index(' ')]
		text = re.sub('[^0-9a-z]', '', text)
		text = re.sub('\s+', ' ', text)
		text = text.replace(' ', char)
		text = text.strip(char)
		if lower:
			text = text.lower()
		return text

	def get_country_name_by_code(self, iso_code):
		if not to_str(iso_code):
			return ''
		try:
			# countries = urllib.request.urlopen("http://country.io/names.json").read()
			# countries = countries.decode('utf-8')
			countries = json.loads(self.COUNTRIES)
		except Exception as e:
			return ''
		return countries.get(to_str(iso_code).upper(), '')

	def get_currency_rate(self, code):
		code = to_str(code).upper()
		if not code:
			return 1
		currencies = json_decode(self.CURRENCIES)
		return currencies.get(code, 1) if currencies else 1

	def get_currency_symbol(self, code):
		code = to_str(code).upper()
		if not code:
			return ''
		currencies_symbol = json_decode(self.CURRENCIES_SYMBOL)
		return currencies_symbol.get(code, code)

	def get_state_name_by_code(self, state_code, country_code = None):
		if not state_code:
			return ''
		where = dict()
		if country_code:
			where['country_id'] = country_code
		if state_code:
			where['code'] = state_code
		if not where:
			return None
		result = self.select_obj('directory_country_region', where)
		try:
			data = result['data'][0]['default_name']
		except Exception as e:
			data = None
		return data

	def get_map_customer_group(self, group_id = None):
		if not group_id:
			return 0
		res = 0
		if isinstance(self._notice['map']['customer_group'], list):
			try:
				res = self._notice['map']['customer_group'][int(group_id)]
			except Exception:
				res = 0
		if isinstance(self._notice['map']['customer_group'], dict):
			res = self._notice['map']['customer_group'].get(to_str(group_id), 0)
		return res

	# TODO: BEFORE MIGRATION

	def prepare_display_setup_source(self, request = None):
		cart_type = self._notice['src']['cart_type']
		setup_type = self.source_cart_setup(cart_type)
		self._notice['src']['setup_type'] = setup_type
		url_check = self._cart_url
		validate_url = self.validate_cart_url()
		if validate_url['result'] != 'success':
			msg = 'Url invalid. Please enter the correct url'
			if validate_url['msg']:
				msg = validate_url['msg']
			response = response_warning(msg)
			response['elm_error'] = "#source-cart-url"
			response['title'] = 'Url invalid'
			return response
		time_out = False
		if setup_type == 'connector':
			time_out = True
			url_check = self.get_url_suffix(self.CONNECTOR_SUFFIX)
		check_url = self.check_url(url_check, time_out)
		if check_url['result'] == 'auth':
			desc = 'Please enter Basic Auth User and Password to continue.'
			if self._notice['src']['config'].get('auth', dict()).get('user'):
				desc = 'Authentication failed, please check Auth User and Password and retry.'
			response = create_response('auth', desc)
			response['elm_error'] = "#source-cart-url"
			response['title'] = 'HTTP Basic Auth Required'
			return response
		if check_url['result'] == 'nginx':
			desc = 'We detect you are running Nginx. Please <a href="https://litextension.com/faq/docs/general-questions/connector-isssues/what-are-the-connector-files-and-how-to-download-them" target="_blank">follow this guide</a> to install connector on Nginx. If you have any difficulties, please contact us for assistance.'
			response = create_response('nginx', desc)
			response['elm_error'] = "#source-cart-url"
			response['title'] = 'Can not reach connector'
			return response
		if check_url['result'] != 'success':
			response = response_warning("Can’t connect to source store currently, please check and try again")
			response['elm_error'] = "#source-cart-url"
			response['title'] = 'Source Connection Error'
			if setup_type == 'connector':
				response['msg'] = "Please check and ensure corrector can be reachable at " + "<a target='_blank' href = '" + self.get_url_suffix(self.CONNECTOR_SUFFIX) + "'>" + self.get_url_suffix(self.CONNECTOR_SUFFIX) + "</a>"
				detect_cart = self.detect_cart_type(self._cart_url)
				if detect_cart:
					response['msg'] = "Can’t connect to source store currently. Your url looks like a " + detect_cart.capitalize() + " but your selected cart type is " + cart_type.capitalize() + ". Please check and try again"

			return response
		if setup_type == "module_connector":
			check_url = self.check_url(self.get_url_suffix(self.CONNECTOR_SUFFIX), time_out)
			if check_url['result'] != 'success':
				if request.get('src_type_upload') == 'api' and not self._notice['src']['config'].get('module_installed'):
					install = self.install_module_connector(request)
					if install['result'] != 'success':
						return install
					self._notice['src']['config']['type_upload'] = 'api'
					self._notice['src']['config']['module_installed'] = True
					self._notice['src']['config']['path_connector'] = self.get_path_connector()
			else:
				self._notice['src']['config']['type_upload'] = 'connector'

		# if
		if setup_type in ['connector', 'module_connector']:
			token = self._notice['src']['config']['token']
			# migration = self.get_info_migration(self._migration_id)
			# if migration:
			# 	token = migration['src_token']
			if not token:
				token = '123456'
			connector_url = self.get_connector_url('check', token)

			check = self.get_connector_data(connector_url)
			if not check:
				response = response_warning("Cannot reach connector! It should be uploaded at: " + self.get_url_suffix(self.CONNECTOR_SUFFIX))
				response['elm_error'] = "#source-cart-url"
				response['title'] = 'Source Url Error'
				if setup_type == 'module_connector' and self._notice['src']['config'].get('type_upload') == 'api':
					return response_error('Failed to automatically install the connector. You can <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), "#source-cart-url", 'Install failed.')
				return response
			if check['result'] == 'token':
				response = response_warning('Source token not correct! Probably you used the connector from another account. Please download and reupload connector from your account. <a href="https://litextension.com/faq/docs/unwanted-errors-fix/what-are-the-connector-files-and-how-to-download-them/" target="_blank">More details</a>')
				response['elm_error'] = "#source_connector"
				if setup_type == 'module_connector' and self._notice['src']['config'].get('type_upload') == 'api':
					response['msg'] = "Source token not correct! Please <a href='{}' target='_blank'>click</a> to change token and try again".format(self.get_link_change_token())
					response['elm_error'] = "#source-cart-url"
				response['title'] = "Source Token Error"

				return response
			if check['result'] != 'success':
				response = response_warning("Can’t connect to source store currently, please check and try again")
				response['elm_error'] = "#source-cart-url"
				response['title'] = 'Source Url Error'

				return response
			data = check['data']
			if not data:
				response = response_warning("Cannot reach database from source connector!")
				response['msg'] += ' <a href="https://litextension.com/faq/docs/unwanted-errors-fix/i-keep-getting-the-cannot-reach-database-from-source-target-connector-message-what-should-i-do/" target="_blank">More details!</a>'

				response['elm_error'] = "#source-cart-url"
				response['title'] = 'Source Url Error'
				return response
			if not self.check_cart_type_sync(data['cms'], self._notice['src']['cart_type']):
				response = response_warning("Source Cart type not correct! Your url looks like a " + to_str(data['cms']).capitalize()  + " but your selected cart type is " + cart_type.capitalize() + ". Please check and try again")
				response['elm_error'] = "#form-source-select"
				response['title'] = "Source Cart Type Error"
				return response
			connect = data['connect']
			if (not connect) or (connect['result'] != 'success'):
				response = response_warning("Cannot reach database from source connector!")
				response['msg'] += ' <a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">More details!</a>'

				response['elm_error'] = "#source-cart-url"
				response['title'] = 'Source Url Error'
				return response
			config_keys = ['cookie_key', 'version', 'table_prefix', 'charset', 'image', 'image_product', 'image_category', 'image_manufacturer', 'extend', 'connector_version']
			for config_key in config_keys:
				config_value = data[config_key] if config_key in data else ''
				if config_key == 'version' and self._notice['src']['config'].get('version') and 'ee' in self._notice['src']['config'].get('version'):
					config_value += '.ee'
				self._notice['src']['config'][config_key] = config_value
			if 'view' in data:
				self._notice['target']['config']['view'] = data.get('view')
			if 'site_id' in data:
				self._notice['src']['config']['site_id'] = data.get('site_id')
		# end if
		return response_success()

	def display_setup_source(self, request):
		configs = {
			'api': 'src_api',
			'database': 'src_database',
		}
		for key, config in configs.items():
			value = request.get(config, '')
			self._notice['src']['config'][key] = value
		if self._notice['src'].get('setup_type') in ['api', 'database']:
			method = 'validate_' + self._notice['src'].get('setup_type') + '_info'
			validate = getattr(self, method)()
			if validate['result'] != 'success':
				return validate

		return response_success()

	def prepare_display_setup_target(self, request = None):
		cart_type = self._notice['target']['cart_type']
		setup_type = self.target_cart_setup(cart_type)
		self._notice['target']['setup_type'] = setup_type
		validate_url = self.validate_cart_url()
		if validate_url['result'] != 'success':
			msg = 'Url invalid. Please enter the correct url'
			if validate_url['msg']:
				msg = validate_url['msg']
			response = response_warning(msg)
			response['elm_error'] = "#target-cart-url"
			response['title'] = 'Url invalid'
			return response
		url_check = self._cart_url
		time_out = False
		if setup_type == 'connector':
			time_out = True
			url_check = self.get_url_suffix(self.CONNECTOR_SUFFIX)
		check_url = self.check_url(url_check, time_out)
		if check_url['result'] == 'auth':
			desc = 'Please enter Basic Auth User and Password to continue.'
			if self._notice['target']['config'].get('auth', dict()).get('user'):
				desc = 'Authentication failed, please check Auth User and Password and retry.'
			response = create_response('auth', desc)
			response['elm_error'] = "#target-cart-url"
			response['title'] = 'HTTP Basic Auth Required'
			return response
		if check_url['result'] == 'nginx':
			desc = 'We detect you are running Nginx. Please <a href="https://litextension.com/faq/docs/general-questions/connector-isssues/what-are-the-connector-files-and-how-to-download-them" target="_blank">follow this guide</a> to install connector on Nginx. If you have any difficulties, please contact us for assistance.'
			response = create_response('nginx', desc)
			response['elm_error'] = "#target-cart-url"
			response['title'] = 'Can not reach connector'
			return response
		if check_url['result'] != 'success':
			response = response_warning("Can’t connect to target store currently, please check and try again. " + '<a href="https://litextension.com/faq/docs/unwanted-errors-fix/what-are-the-connector-files-and-how-to-download-them/" target="_blank">More details</a>')
			response['elm_error'] = "#target-cart-url"
			response['title'] = "Target Connection Error"
			if setup_type == 'connector':
				response['msg'] = "Please check and ensure corrector can be reachable at " + "<a target='_blank' href = '" + self.get_url_suffix(self.CONNECTOR_SUFFIX) + "'>" + self.get_url_suffix(self.CONNECTOR_SUFFIX) + "</a>"
				detect_cart = self.detect_cart_type(self._cart_url)
				if detect_cart:
					response['msg'] = "Can’t connect to target store currently. Your url looks like a " + detect_cart.capitalize() + " but your selected cart type is " + cart_type.capitalize() + ". Please check and try again"

			return response
		if setup_type == "module_connector":
			check_url = self.check_url(self.get_url_suffix(self.CONNECTOR_SUFFIX), time_out)
			if check_url['result'] != 'success':
				if request.get('target_type_upload') == 'api' and not self._notice['target']['config'].get('module_installed'):
					install = self.install_module_connector(request)
					if install['result'] != 'success':
						return install
					self._notice['target']['config']['type_upload'] = 'api'
					self._notice['target']['config']['module_installed'] = True
					self._notice['target']['config']['path_connector'] = self.get_path_connector()
			else:
				self._notice['target']['config']['type_upload'] = 'connector'

		# if
		if setup_type in ['connector', 'module_connector']:
			token = self._notice['target']['config']['token']

			# migration = self.get_info_migration(self._migration_id)
			# if migration:
			# 	token = migration['target_token']
			if not token:
				token = '123456'
			connector_url = self.get_connector_url('check', token)

			check = self.get_connector_data(connector_url)
			if not check:
				response = response_warning("Cannot reach connector! It should be uploaded at: " + self.get_url_suffix(self.CONNECTOR_SUFFIX))
				response['msg'] += ' <a href="https://litextension.com/faq/docs/general-questions/customer-support/why-is-your-migration-showing-error-there-is-an-error-when-reading-your-source-target-cart-database/" target="_blank">More details!</a>'

				response['elm_error'] = "#target-cart-url"
				response['title'] = "Target Connection Error"
				if setup_type == 'module_connector' and self._notice['target']['config'].get('type_upload') == 'api':
					return response_error('Failed to automatically install the connector. You can <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), "#target-cart-url", 'Install failed.')

				return response
			if check['result'] == 'token':
				response = response_warning('Target token not correct! Probably you used the connector from another account. Please download and reupload connector from your account. <a href="https://litextension.com/faq/docs/unwanted-errors-fix/what-are-the-connector-files-and-how-to-download-them/" target="_blank">More details</a>')
				response['elm_error'] = "#target_connector"
				response['title'] = "Target Token Error"
				if setup_type == 'module_connector' and self._notice['target']['config'].get('type_upload') == 'api':
					response['msg'] = "Target token not correct! Please <a href='{}' target='_blank'>click</a> to change token and try again".format(self.get_link_change_token())
					response['elm_error'] = "#target-cart-url"

				return response
			if check['result'] != 'success':
				response = response_warning("Can’t connect to target store currently, please check and try again")
				response['elm_error'] = "#target-cart-url"
				response['title'] = "Target Url Error"
				return response
			data = check['data']
			if not data:
				response = response_warning("Cannot reach database from target connector!")
				response['msg'] += ' <a href="https://litextension.com/faq/docs/unwanted-errors-fix/i-keep-getting-the-cannot-reach-database-from-source-target-connector-message-what-should-i-do/" target="_blank">More details!</a>'

				response['elm_error'] = "#target-cart-url"
				response['title'] = "Target Url Error"
				return response
			if not self.check_cart_type_sync(data['cms'], self._notice['target']['cart_type']):
				response = response_warning("Target Cart type not correct! Your url looks like a " + to_str(data['cms']).capitalize()  + " but your selected cart type is " + cart_type.capitalize() + ". Please check and try again")
				if self._notice['target']['cart_type'] == 'woocommerce:':
					response['msg'] = "Target Cart type not correct! Your url looks like a " + to_str(data['cms']).capitalize()  + " but your selected cart type is " + cart_type.capitalize() + ". Please check if Woocommerce is installed on your Target? If no, please install and try again."
				response['elm_error'] = "#form-target-select"
				response['title'] = "Target Cart Type Error"
				return response
			connect = data['connect']
			if (not connect) or (connect['result'] != 'success'):
				response = response_warning("Cannot reach database from target connector!")
				response['elm_error'] = "#target-cart-url"
				response['title'] = "Target Url Error"
				return response
			config_keys = ['cookie_key', 'version', 'table_prefix', 'charset', 'image', 'image_product', 'image_category', 'image_manufacturer', 'extend', 'connector_version']
			for config_key in config_keys:
				config_value = data[config_key] if config_key in data else ''
				if config_key == 'version' and self._notice['target']['config'].get('version') and 'ee' in self._notice['target']['config'].get('version'):
					config_value += '.ee'
				self._notice['target']['config'][config_key] = config_value
			if 'view' in data:
				self._notice['target']['config']['view'] = data.get('view')
			if 'download_image' in data and not data['download_image']:
				self.log('curl && file get content error', 'target_image')
				self._notice['target']['config']['download_image'] = data['download_image']
			if 'site_id' in data:
				self._notice['target']['config']['site_id'] = data.get('site_id')
		# end if
		return response_success()

	def prepare_display_storage(self):
		return response_success()

	def display_storage_source(self):
		return response_success()

	def display_storage_target(self):
		return response_success()

	def display_storage(self):
		supports = ['languages_select', 'site_map', 'language_map', 'category_map', 'attribute_map', 'order_status_map',
		            'currency_map', 'country_map', 'customer_group_map', 'taxes', 'manufacturers', 'categories',
		            'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules',
		            'add_new', 'clear_shop', 'img_des', 'ignore_image', 'pre_prd', 'pre_cus', 'pre_ord', 'seo', 'multi_languages_select']
		for support in supports:
			self._notice['support'][support] = self._notice['src']['support'][support] and self._notice['target']['support'][support]
		self._notice['step'] = 'storage'
		return response_success()

	def display_setup_target(self, request):
		configs = {
			'api': 'target_api',
			'database': 'target_database',
		}
		for key, config in configs.items():
			value = request.get(config, '')
			self._notice['target']['config'][key] = value
		if self._notice['target'].get('setup_type') in ['api', 'database']:
			method = 'validate_' + self._notice['target'].get('setup_type') + '_info'
			validate = getattr(self, method)()
			if validate['result'] != 'success':
				return validate
		return response_success()

	def prepare_display_upload(self, data):
		config_keys = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages',
		               'blogs', 'coupons', 'cartrules']
		str_config = []
		# for
		for config_key in config_keys:
			if config_key in str_config:
				value = data.get(config_key, '')
			else:
				value = True if (config_key in data) and data[config_key] else False

			self._notice['config'][config_key] = value
		# endfor
		self._notice['src']['storage']['init'] = False
		self._notice['src']['storage']['result'] = 'process'
		self._notice['step'] = 'config'

		return response_success()

	def display_upload(self, upload_msg):
		return response_success()

	def display_config_source(self):
		return response_success()

	def display_config_target(self):
		return response_success()

	def prepare_display_config(self):
		return response_success()

	def display_config(self):
		supports = ['languages_select', 'site_map', 'language_map', 'category_map', 'attribute_map', 'order_status_map', 'currency_map', 'country_map', 'customer_group_map', 'taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'add_new', 'clear_shop', 'img_des', 'ignore_image', 'pre_prd', 'pre_cus', 'pre_ord', 'seo', 'cus_pass', 'seo_301', 'strip_html', 'smart_collection', 'quotes', 'newsletters', 'multi_languages_select']
		for support in supports:
			self._notice['support'][support] = self._notice['src']['support'][support] and self._notice['target']['support'][support]
		if to_len(self._notice['src']['languages']) > 1:
			self._notice['support']['languages_select'] = True
		else:
			if to_len(self._notice['target']['languages']) > 0:
				self._notice['support']['languages_select'] = False
			else:
				self._notice['support']['languages_select'] = True
		if to_len(self._notice['src']['site']) > 1:
			self._notice['support']['site_select'] = True
		else:
			if to_len(self._notice['target']['site']) > 0:
				self._notice['support']['site_select'] = False
			else:
				self._notice['support']['site_select'] = True
		self._notice['resume']['process'] = 'configuring'
		self._notice['src']['languages'] = self.list_to_dict(self._notice['src']['languages'])
		self._notice['target']['languages'] = self.list_to_dict(self._notice['target']['languages'])
		if self._notice['migration_id'] and not self._notice['config'].get('recent') and not self._notice['config'].get('remigrate'):
			setup = Setup()
			check_setup = setup.setup_db_for_migration(self._notice['migration_id'])
			if not check_setup:
				return create_response('stop', "Can't setup db")
		if self._notice['src'].get('setup_type') == 'file':
			self._notice['target']['support']['update_latest_data'] = False
		return response_success()

	def prepare_display_confirm(self, data):
		map_keys = ['site', 'languages', 'attributes', 'order_status', 'customer_group']
		if self._notice['target']['cart_type'] == 'shopify':
			map_keys.append('order_status_shopify')
		# for
		for map_key in map_keys:
			map_data = data.get(map_key, dict())
			map_value = dict()
			map_data = self.list_to_dict(map_data)
			for index, value_map in map_data.items():
				map_value[to_str(index)] = value_map
			# if
			if map_key in data:
				self._notice['map'][map_key] = map_value
				# if
				if (map_key == 'languages') and map_value:
					self._notice['map'][map_key] = self.list_to_dict(map_value)
					map_root_cat = dict()
					for src_store_id, target_store_id in map_value.items():
						src_store_id = to_str(src_store_id)
						target_store_id = to_str(target_store_id)
						root_cat_src_id = to_str(self._notice['src']['store_category'].get(src_store_id))
						root_cat_target_id = to_str(self._notice['target']['store_category'].get(target_store_id))
						if root_cat_src_id and root_cat_target_id:
							if not (root_cat_src_id in map_root_cat):
								map_root_cat[root_cat_src_id] = list()
							if not (root_cat_target_id in map_root_cat[root_cat_src_id]):
								map_root_cat[root_cat_src_id].append(root_cat_target_id)
						site_src_id = self._notice['src']['site'].get(src_store_id)
						site_target_id = self._notice['target']['site'].get(target_store_id)
						if site_src_id and site_target_id and not data.get('site', dict()):
							if not (str(site_src_id) in self._notice['map']['site']):
								self._notice['map']['site'][str(site_src_id)] = list()
							if not (site_target_id in self._notice['map']['site'][str(site_src_id)]):
								self._notice['map']['site'][str(site_src_id)].append(site_target_id)
					self._notice['map']['category_data'] = map_root_cat

			# endif
		# endif
		# endfor
		config_keys = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'add_new', 'clear_shop', 'img_des', 'ignore_image', 'pre_cus', 'pre_ord', 'pre_prd', 'seo', 'seo_plugin', 'skip_demo', 'cus_pass', 'seo_301', 'strip_html', 'smart_collection', 'quotes', 'newsletters']
		str_config = ['seo_plugin']
		# for
		for config_key in config_keys:
			if config_key in str_config:
				value = data.get(config_key, '')
			else:
				value = True if config_key in data else False
			if (config_key == 'pre_prd') and (config_key in data):
				if self._notice['config']['add_new']:
					self._notice['config']['real_pre_prd'] = value
				else:
					self._notice['config']['real_pre_prd'] = (config_key in data) and (('clear_shop' in data) or (to_str(self._notice['target'].get('number_of_prd', 0)) == '0'))
			if (config_key == 'products') and (config_key in data) and self._notice['support']['attributes']:
				self._notice['config']['attributes'] = value
			if (config_key == 'add_new') and (self._notice['config']['recent']):
				value = True
			self._notice['config'][config_key] = value
			if config_key not in str_config and not self._notice['support'].get(config_key):
				self._notice['config'][config_key] = False
		# endfor
		language_select = data.get('languages_select', dict())
		self._notice['src']['languages_select'] = list()
		for language_id, value in language_select.items():
			self._notice['src']['languages_select'].append(language_id)

		site_select = data.get('site_select', dict())
		if isinstance(site_select, list):
			site_select = self.list_to_dict(site_select)
		self._notice['src']['site_select'] = list()
		for site_id, value in site_select.items():
			self._notice['src']['site_select'].append(site_id)
		return response_success()

	def display_confirm_source(self):
		return response_success()

	def display_confirm_target(self):
		return response_success()

	def display_confirm(self):
		if to_int(self._notice['mode']) == self.MIGRATION_FULL:
			self._notice['resume']['process'] = 'payment'
		else:
			self._notice['resume']['process'] = 'migrating'
			if self._notice['config']['skip_demo'] or self._notice['demo']['status'] == 'success' or self._notice['config'].get('remigrate'):
				self._notice['resume']['process'] = 'payment'
		self._notice['target']['clear_demo']['function'] = 'clear_target_products_demo'
		return response_success()

	def check_cart_type_sync(self, cart_type, cart_select):
		return cart_type in cart_select

	def prepare_import_source(self):
		return response_success()

	def prepare_import_target(self):
		# if not self._notice['config']['add_new'] and self._notice['demo'].get('status') == 'success' and not self._notice['config'].get('reset'):
		# 	if to_int(self._notice['mode']) == self.MIGRATION_DEMO:
		# 		key_total = 'total'
		# 	else:
		# 		key_total = 'real_total'
		# 	list_entity_check = ['customers']
		# 	check = True
		# 	for entity_check in list_entity_check:
		# 		if to_int(self._notice['process'][entity_check][key_total]) > to_int(self._notice['process'][entity_check]['imported']):
		# 			check = False
		# 			break
		# 	if not check and hasattr(self, 'clear_target_orders_demo'):
		# 		getattr(self, 'clear_target_orders_demo')()
		# 		self.delete_obj(TABLE_MAP, {'migration_id': self._migration_id, 'type': 'order'})
		return response_success()

	def prepare_import(self):
		check_db = self.select_page(TABLE_MAP, '', '*', 1)
		if check_db['result'] != 'success':
			setup = Setup()
			setup.setup_db_for_migration(self._notice['migration_id'])
		if (not self._notice['config']['add_new'] and self._notice['demo'].get('status') != 'success') or self._notice['config'].get('reset'):
			_migration_id = self._migration_id
			table_name = self.get_table_name(TABLE_MAP)
			no_delete = [self.TYPE_IMAGE, self.TYPE_PATH_IMAGE]
			cart_no_delete = list()
			if self._notice['src']['cart_type'] == 'magento' and self._notice['target']['cart_type'] == 'magento' and self._notice['config'].get('reset'):
				cart_no_delete = [self.TYPE_ATTR, self.TYPE_ATTR_OPTION]
			no_delete = list(set(no_delete + cart_no_delete))
			query = "DELETE FROM `" + table_name + "` WHERE migration_id = " + to_str(_migration_id) + " AND `type` NOT IN " + self.list_to_in_condition(no_delete)
			delete = self.query_raw(query)
			if not delete or delete['result'] != 'success':
				return error_database()
		if self._notice['config'].get('reset'):
			self.delete_obj(TABLE_RECENT, {'migration_id': self._migration_id})

		if self._notice['config'].get('update_latest_data') and not self._notice['config'].get('reset'):
			entities = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'quotes', 'newsletters']
			for entity_type in entities:
				if entity_type not in self._notice['process']:
					continue
				if self._notice['target']['config'].get('entity_update', dict()).get(entity_type):
					self._notice['process'][entity_type]['id_src'] = 0
					self._notice['process'][entity_type]['total'] = to_int(self._notice['process'][entity_type]['total']) + to_int(self._notice['process'][entity_type].get('total_update', 0))
		if not self._notice['src']['language_default'] or (self._notice['src']['language_default'] and to_str(self._notice['src']['language_default']) not in self._notice['map']['languages']):
			for src_lang_id, target_lang_id in self._notice['map']['languages'].items():
				self._notice['src']['language_default'] = src_lang_id
				break
		target_language_id = list(map(lambda x: to_str(x), list(self._notice['map']['languages'].values())))
		if to_str(self._notice['target']['language_default']) not in target_language_id and self._notice['map']['languages'] and self._notice['map']['languages'].get(to_str(self._notice['src']['language_default'])):
			self._notice['target']['language_default'] = self._notice['map']['languages'][to_str(self._notice['src']['language_default'])]
		return response_success()

	def prepare_display_import_source(self):
		if not self._notice['config'].get('recent'):
			entities = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'quotes', 'newsletters']
			self._previous_notice = copy.deepcopy(self._notice)
			for entity in entities:
				self._notice['process'][entity] = self.get_default_process()
		return response_success()

	def display_import_source(self):
		return response_success()

	def after_display_import_source(self):
		if not self._notice['config'].get('recent'):
			entities = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'quotes', 'newsletters']
			notice = copy.deepcopy(self._notice)
			for entity in entities:
				notice['process'][entity] = copy.deepcopy(self._previous_notice['process'][entity])
				notice['process'][entity]['total'] = self._notice['process'][entity]['total']
				notice['process'][entity]['real_total'] = self._notice['process'][entity]['real_total']
			self._notice = notice
			del self._previous_notice
		return response_success()


	def display_import_target(self):
		return response_success()

	def display_import(self):
		customer_migration_id = self._notice['migration_id']
		info = self.get_info_migration(customer_migration_id)
		limit = 'unlimit'
		if to_int(self._notice['mode']) == self.MIGRATION_DEMO:
			limit = get_value_by_key_in_dict(info, 'demo_limit', 20)
		if info and info['migration_group'] == self.GROUP_TEST:
			autotest = get_model('autotest')
			# limit = autotest.get_limit(customer_migration_id)
			limit = getattr(autotest, 'get_limit')(customer_migration_id)
		if not limit:
			limit = 0
		if limit == 'unlimit':
			limit = 'unlimited'
		self._notice['limit'] = limit
		limit_app_mode_data = None
		if to_int(self._notice['mode']) == self.MIGRATION_DEMO and self._notice['config'].get('app_mode'):
			limit_app_mode = self.get_app_mode_limit()
			if limit_app_mode and limit_app_mode['result'] == 'success':
				limit_app_mode_data = limit_app_mode['data']
		types = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'quotes', 'newsletters']
		for entity_type in types:
			if entity_type not in self._notice['process']:
				continue
			entity_limit = limit
			if limit_app_mode_data:
				if to_str(limit_app_mode_data['entities']) == 'all' or (isinstance(limit_app_mode_data['entities'], list) and entity_type in limit_app_mode_data['entities']):
					entity_limit = limit_app_mode_data['limit']
				else:
					entity_limit = 0
			total = self._notice['process'][entity_type]['total']
			if to_str(entity_limit) == 'unlimited' or entity_type == 'attributes':
				count = total
			else:
				count = total if to_int(total) < to_int(entity_limit) else entity_limit
			if entity_type in ['reviews', 'quotes', 'newsletters'] and self._notice['mode'] == self.MIGRATION_DEMO:
				count = 0
			self._notice['process'][entity_type]['total'] = count
			if self._notice['config']['recent']:
				self._notice['process'][entity_type]['real_total'] = to_int(total) + to_int(self._notice['process'][entity_type]['real_total'])
			else:
				self._notice['process'][entity_type]['real_total'] = total
		self._notice['step'] = 'import'

		return response_success()

	def display_update_source(self):
		return response_success()

	def display_update_target(self):
		return response_success()

	def display_update(self):
		return response_success()

	def get_query_display_import_source(self, update = False):
		return dict()

	# TODO: CLEAR TARGET DATA
	def clear_data(self):
		if not self._notice['config']['clear_shop'] and not self._notice['config'].get('reset_clear'):
			return response_success()
		if not hasattr(self, self._notice['target']['clear']['function']):
			return response_success()
		fn_clear = getattr(self, self._notice['target']['clear']['function'])
		clear = fn_clear()
		if clear['result'] == 'success':
			entities = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules']
			entity_select = list()
			for entity in entities:
				if self._notice['config'][entity]:
					entity_select.append(entity)
			if entity_select:
				msg = "Current " + ', '.join(entity_select) + ' cleared'
				if 'msg' not in clear:
					clear['msg'] = ''
				clear['msg'] += msg
		# clear['msg'] += self.get_msg_start_import('taxes')
		return clear

	def clear_demo(self):
		if to_int(self._notice['mode']) == self.MIGRATION_DEMO:
			return response_success()
		if self._notice['config']['recent']:
			return response_success()
		if self._notice['demo']['clear']:
			return response_success()
		types = ['products', 'orders']
		entities = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules', 'quotes', 'newsletters']
		if to_int(self._notice['mode']) == self.MIGRATION_DEMO and self._notice['demo']['status'] != 'skip':
			key_total = 'total'
		else:
			key_total = 'real_total'
		functions = ['clear_target_products_demo', 'clear_target_orders_demo']
		if self._notice['demo']['status'] != 'success' or not hasattr(self, self._notice['target']['clear_demo']['function']) or self._notice['target']['clear_demo']['function'] not in functions:
			self._notice['demo']['clear'] = True

			for entity_type in entities:
				if entity_type not in self._notice['process']:
					continue
				self._notice['process'][entity_type]['total'] = self._notice['process'][entity_type][key_total]
				if entity_type in types:
					self._notice['process'][entity_type]['time_finish'] = 0
					self._notice['process'][entity_type]['id_src'] = 0
					self._notice['process'][entity_type]['imported'] = 0
					self._notice['process'][entity_type]['error'] = 0

			return response_success()

		fn_clear = getattr(self, self._notice['target']['clear_demo']['function'])
		clear = fn_clear()
		if clear['result'] == 'success':
			self._notice['demo']['clear'] = True
			# types = ['taxes', 'manufacturers', 'categories','attributes', 'products', 'customers', 'orders', 'reviews', 'pages',
			# 		 'blogs', 'coupons', 'cartrules']
			for entity_type in entities:
				if entity_type not in self._notice['process']:
					continue
				self._notice['process'][entity_type]['total'] = self._notice['process'][entity_type][key_total]
				if entity_type in types:
					self._notice['process'][entity_type]['total'] = self._notice['process'][entity_type]['real_total']
					self._notice['process'][entity_type]['time_finish'] = 0
					self._notice['process'][entity_type]['id_src'] = 0
					self._notice['process'][entity_type]['imported'] = 0
					self._notice['process'][entity_type]['error'] = 0
		return clear

	def no_clear(self):
		return response_success()

	def no_clear_demo(self):
		return response_success()

	def storage_data(self):
		return response_success()

	def no_storage_data(self):
		return response_success()

	def finish_storage_data(self):
		file_info = self.get_file_info()
		folder_upload = self.get_folder_upload(self._migration_id)
		for file_name, info in file_info.items():
			file_upload = folder_upload + '/' + self.get_upload_file_name(file_name)
			if os.path.isfile(file_upload):
				os.remove(file_upload)
		return response_success()

	def get_folder_upload(self, migration_id):
		root_path = get_pub_path()
		migration = self.get_info_migration(migration_id)
		folder_upload = root_path + '/' + DIR_UPLOAD + '/' + to_str(migration_id)
		if self._is_test:
			folder_upload = root_path + '/' + DIR_UPLOAD + '/' + get_config_ini('src', 'file', file = 'test.ini')
			return folder_upload
		if migration and migration['migration_group'] == GROUP_TEST:
			migration_test = self.select_row(TABLE_MIGRATION_TEST, {'migration_id': migration_id})
			if migration_test:
				setup_info = json_decode(migration_test['setup_info'])
				if setup_info:
					file_info = setup_info.get('src_file') if isinstance(setup_info, dict) else ''
					if file_info:
						folder_upload = root_path + '/' + DIR_UPLOAD + '/' + file_info

		return folder_upload

	# Get message for next entity import
	def get_msg_start_import(self, type_import):
		result = ''
		if not type_import:
			result += "Finished migration!"
		types = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages',
		         'blogs', 'coupons', 'cartrules']
		index = types.index(type_import)
		for i, value in enumerate(types):
			if index <= i and self._notice['config'][value]:
				result += 'Importing ' + value + ' ... '
				break
		return result

	# TODO: TAX
	def prepare_taxes_import(self):
		return self

	def prepare_taxes_export(self):
		return self

	def get_taxes_main_export(self):
		return response_success()

	def get_taxes_ext_export(self, taxes):
		return response_success()

	def convert_tax_export(self, tax, taxes_ext):
		return response_success()

	def finish_tax_export(self):
		return response_success()

	def get_tax_id_import(self, convert, tax, taxes_ext):
		return convert['id']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id'], convert['code']) else False

	def router_tax_import(self, convert, tax, taxes_ext):
		return response_success('tax_import')

	def before_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def tax_import(self, convert, tax, taxes_ext):
		return response_success(0)

	def after_tax_import(self, tax_id, convert, tax, taxes_ext):
		return response_success()

	def addition_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def finish_tax_import(self):
		return response_success()

	# TODO: MANUFACTURER
	def prepare_manufacturers_import(self):
		return self

	def prepare_manufacturers_export(self):
		return self

	def get_manufacturers_main_export(self):
		return response_success()

	def get_manufacturers_ext_export(self, manufacturers):
		return response_success()

	def convert_manufacturer_export(self, manufacturer, manufacturers_ext):
		return response_success()

	def finish_manufacturer_export(self):
		return response_success()

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		return convert['id']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id'], convert['code']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success(0)

	def after_manufacturer_import(self, manufacturer_id, convert, manufacturer, manufacturers_ext):
		return response_success()

	def addition_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def finish_manufacturer_import(self):
		return response_success()

	# TODO: CATEGORY
	def prepare_categories_import(self):
		return self

	def prepare_categories_export(self):
		return self

	def get_categories_main_export(self):
		return response_success()

	def get_categories_ext_export(self, categories):
		return response_success()

	def prepare_convert_category_export(self):
		self.cat_parent = list()
		return response_success()

	def convert_category_export(self, category, categories_ext):
		return response_success()


	def get_category_id_import(self, convert, category, categories_ext):
		return convert['id']

	def check_category_import(self, convert, category, categories_ext):
		return True if self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code']) else False

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def category_import(self, convert, category, categories_ext):
		return response_success(0)

	def after_category_import(self, category_id, convert, category, categories_ext):
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	def finish_category_import(self):
		return response_success()

	# TODO: ATTRIBUTES
	def prepare_attributes_import(self):
		return self

	def prepare_attributes_export(self):
		return self

	def get_attributes_main_export(self):
		return response_success()

	def get_attributes_ext_export(self, attributes):
		return response_success()

	def convert_attribute_export(self, attribute, attributes_ext):
		return response_success()

	def get_attribute_id_import(self, convert, attribute, attributes_ext):
		return False

	def check_attribute_import(self, convert, attribute, attributes_ext):
		return False

	def router_attribute_import(self, convert, attribute, attributes_ext):
		return response_success('attribute_import')

	def before_attribute_import(self, convert, attribute, attributes_ext):
		return response_success()

	def attribute_import(self, convert, attribute, attributes_ext):
		return response_success(0)

	def after_attribute_import(self, attribute_id, convert, attribute, attributes_ext):
		return response_success()

	def addition_attribute_import(self, convert, attribute, attributes_ext):
		return response_success()

	def finish_attribute_import(self):
		return response_success()

	# TODO: PRODUCT
	def prepare_products_import(self):
		return self

	def prepare_products_export(self):
		return self

	def get_products_main_export(self):
		return response_success()

	def get_products_ext_export(self, products):
		return response_success()

	def convert_product_export(self, product, products_ext):
		return response_success()

	def get_product_id_import(self, convert, product, products_ext):
		return convert['id']

	def check_product_import(self, convert, product, products_ext):
		return self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'])

	def update_product_after_demo(self, product_id, convert, product, products_ext):
		return response_success()

	def update_latest_data_product(self, product_id, convert, product, products_ext):
		return response_success()

	def router_product_import(self, convert, product, products_ext):
		return response_success('product_import')

	def before_product_import(self, convert, product, products_ext):
		return response_success()

	def product_import(self, convert, product, products_ext):
		return response_success(0)

	def after_product_import(self, product_id, convert, product, products_ext):
		return response_success()

	def addition_product_import(self, convert, product, products_ext):
		return response_success()

	def finish_product_import(self):
		return response_success()

	# TODO: CUSTOMER
	def prepare_customers_import(self):
		return self

	def prepare_customers_export(self):
		return self

	def get_customers_main_export(self):
		return response_success()

	def get_customers_ext_export(self, customers):
		return response_success()

	def convert_customer_export(self, customer, customers_ext):
		return response_success()

	def finish_customer_export(self):
		return response_success()

	def get_customer_id_import(self, convert, customer, customers_ext):
		return convert['id']

	def check_customer_import(self, convert, customer, customers_ext):
		return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id'], convert['code']) else False

	def router_customer_import(self, convert, customer, customers_ext):
		return response_success('customer_import')

	def before_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def customer_import(self, convert, customer, customers_ext):
		return response_success(0)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
		return response_success()

	def addition_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def finish_customer_import(self):
		return response_success()

	# TODO: CUSTOMER QUOTE
	def prepare_quotes_import(self):
		return self

	def prepare_quotes_export(self):
		return self

	def get_quotes_main_export(self):
		return response_success()

	def get_quotes_ext_export(self, quotes):
		return response_success()

	def convert_quote_export(self, quote, quotes_ext):
		return response_success()

	def get_quote_id_import(self, convert, quote, quotes_ext):
		return convert['id']

	def check_quote_import(self, convert, quote, quotes_ext):
		return True if self.get_map_field_by_src(self.TYPE_QUOTE, convert['id'], convert['code']) else False

	def router_quote_import(self, convert, quote, quotes_ext):
		return response_success('quote_import')

	def before_quote_import(self, convert, quote, quotes_ext):
		return response_success()

	def quote_import(self, convert, quote, quotes_ext):
		return response_success(0)

	def after_quote_import(self, quote_id, convert, quote, quotes_ext):
		return response_success()

	def addition_quote_import(self, convert, quote, quotes_ext):
		return response_success()

	def finish_quote_import(self):
		return response_success()

	# TODO: CUSTOMER newsletter
	def prepare_newsletters_import(self):
		return self

	def prepare_newsletters_export(self):
		return self

	def get_newsletters_main_export(self):
		return response_success()

	def get_newsletters_ext_export(self, newsletters):
		return response_success()

	def convert_newsletter_export(self, newsletter, newsletters_ext):
		return response_success()

	def get_newsletter_id_import(self, convert, newsletter, newsletters_ext):
		return convert['id']

	def check_newsletter_import(self, convert, newsletter, newsletters_ext):
		return True if self.get_map_field_by_src(self.TYPE_NEWSLETTER, convert['id'], convert['code']) else False

	def router_newsletter_import(self, convert, newsletter, newsletters_ext):
		return response_success('newsletter_import')

	def before_newsletter_import(self, convert, newsletter, newsletters_ext):
		return response_success()

	def newsletter_import(self, convert, newsletter, newsletters_ext):
		return response_success(0)

	def after_newsletter_import(self, newsletter_id, convert, newsletter, newsletters_ext):
		return response_success()

	def addition_newsletter_import(self, convert, newsletter, newsletters_ext):
		return response_success()

	def finish_newsletter_import(self):
		return response_success()

	# TODO: ORDER
	def prepare_orders_import(self):
		return self

	def prepare_orders_export(self):
		return self

	def get_orders_main_export(self):
		return response_success()

	def get_orders_ext_export(self, orders):
		return response_success()

	def convert_order_export(self, order, orders_ext):
		return response_success()

	def finish_order_export(self):
		return response_success()

	def get_order_id_import(self, convert, order, orders_ext):
		return convert['id']

	def check_order_import(self, convert, order, orders_ext):
		return self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code'])

	def update_latest_data_order(self, order_id, convert, order, orders_ext):
		return response_success()

	def router_order_import(self, convert, order, orders_ext):
		return response_success('order_import')

	def before_order_import(self, convert, order, orders_ext):
		return response_success()

	def order_import(self, convert, order, orders_ext):
		return response_success(0)

	def update_order_after_demo(self, order_id, convert, order, orders_ext):
		return response_success()

	def after_order_import(self, order_id, convert, order, orders_ext):
		return response_success()

	def addition_order_import(self, convert, order, orders_ext):
		return response_success()

	def finish_order_import(self):
		return response_success()

	# TODO: REVIEW
	def prepare_reviews_import(self):
		return self

	def prepare_reviews_export(self):
		return self

	def get_reviews_main_export(self):
		return response_success()

	def get_reviews_ext_export(self, reviews):
		return response_success()

	def convert_review_export(self, review, reviews_ext):
		return response_success()

	def finish_review_export(self):
		return response_success()

	def get_review_id_import(self, convert, review, reviews_ext):
		return convert['id']

	def check_review_import(self, convert, review, reviews_ext):
		return True if self.get_map_field_by_src(self.TYPE_REVIEW, convert['id'], convert['code']) else False

	def router_review_import(self, convert, review, reviews_ext):
		return response_success('review_import')

	def before_review_import(self, convert, review, reviews_ext):
		return response_success()

	def review_import(self, convert, review, reviews_ext):
		return response_success(0)

	def after_review_import(self, review_id, convert, review, reviews_ext):
		return response_success()

	def addition_review_import(self, convert, review, reviews_ext):
		return response_success()

	def finish_review_import(self):
		return response_success()

	# TODO: PAGE
	def prepare_pages_import(self):
		return self

	def prepare_pages_export(self):
		return self

	def get_pages_main_export(self):
		return response_success()

	def get_pages_ext_export(self, pages):
		return response_success()

	def convert_page_export(self, page, pages_ext):
		return response_success()

	def finish_page_export(self):
		return response_success()

	def get_page_id_import(self, convert, page, pages_ext):
		return convert['id']

	def check_page_import(self, convert, page, pages_ext):
		return True if self.get_map_field_by_src(self.TYPE_PAGE, convert['id'], convert['code']) else False

	def router_page_import(self, convert, page, pages_ext):
		return response_success('page_import')

	def before_page_import(self, convert, page, pages_ext):
		return response_success()

	def page_import(self, convert, page, pages_ext):
		return response_success(0)

	def after_page_import(self, page_id, convert, page, pages_ext):
		return response_success()

	def addition_page_import(self, convert, page, pages_ext):
		return response_success()

	def finish_page_import(self):
		return response_success()

	# TODO: BLOCK
	def prepare_blogs_import(self):
		return response_success()

	def prepare_blogs_export(self):
		return self

	def get_blogs_main_export(self):
		return response_success()

	def get_blogs_ext_export(self, blocks):
		return response_success()

	def convert_blog_export(self, block, blocks_ext):
		return response_success()

	def get_blog_id_import(self, convert, block, blocks_ext):
		return convert['id']

	def check_blog_import(self, convert, block, blocks_ext):
		return True if self.get_map_field_by_src(self.TYPE_BLOG, convert['id'], convert['code']) else False

	def router_blog_import(self, convert, block, blocks_ext):
		return response_success('block_import')

	def before_blog_import(self, convert, block, blocks_ext):
		return response_success()

	def blog_import(self, convert, block, blocks_ext):
		return response_success(0)

	def after_blog_import(self, block_id, convert, block, blocks_ext):
		return response_success()

	def addition_blog_import(self, convert, block, blocks_ext):
		return response_success()

	def finish_blog_import(self):
		return response_success()

	def finish_blog_export(self):
		return response_success()
	# TODO: Coupon
	def prepare_coupons_import(self):
		return response_success()

	def prepare_coupons_export(self):
		return self

	def get_coupons_main_export(self):
		return response_success()

	def get_coupons_ext_export(self, coupons):
		return response_success()

	def convert_coupon_export(self, coupon, coupons_ext):
		return response_success()

	def finish_coupon_export(self):
		return response_success()

	def get_coupon_id_import(self, convert, coupon, coupons_ext):
		return convert['id']

	def check_coupon_import(self, convert, coupon, coupons_ext):
		return True if self.get_map_field_by_src(self.TYPE_COUPON, convert['id'], convert['code']) else False

	def router_coupon_import(self, convert, coupon, coupons_ext):
		return response_success('coupon_import')

	def before_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	def coupon_import(self, convert, coupon, coupons_ext):
		return response_success(0)

	def after_coupon_import(self, coupon_id, convert, coupon, coupons_ext):
		return response_success()

	def addition_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	def finish_coupon_import(self):
		return response_success()

	def prepare_display_finish(self):
		return response_success()

	def display_finish_source(self):
		return response_success()

	def display_finish_target(self):
		migration_id = self._migration_id
		recent_exist = self.select_row(TABLE_RECENT, {'migration_id': migration_id})
		notice = json.dumps(self._notice)
		if recent_exist:
			self.update_obj(TABLE_RECENT, {'notice': notice}, {'migration_id': migration_id})
		else:
			self.insert_obj(TABLE_RECENT, {'notice': notice, 'migration_id': migration_id})
		target_cart_type = self._notice['target']['cart_type']
		target_setup_type = self.target_cart_setup(target_cart_type)
		if target_setup_type == 'connector':
			token = self._notice['target']['config']['token']
			url = self.get_connector_url('clearcache', token)
			auth = dict()
			if self._notice[self.get_type()]['config'].get('auth'):
				auth['user'] = to_str(self._notice[self.get_type()]['config']['auth'].get('user'))
				auth['pass'] = to_str(self._notice[self.get_type()]['config']['auth'].get('pass'))
			new_thread = RequestThread(self._migration_id, url, auth = auth)
			new_thread.start()
		return response_success()

	def display_finish(self):
		if to_int(self._notice['mode']) == self.MIGRATION_FULL:
			self._notice['resume']['process'] = 'completed'
		else:
			self._notice['resume']['process'] = 'payment'
			self._notice['resume']['type'] = ''
			self._notice['resume']['action'] = 'clear_demo'
			self._notice['demo']['status'] = 'success'
		if self._notice.get('version'):
			self.save_migration_history()
		self._notice['config']['reset'] = False
		self._notice['config']['reset_clear'] = False
		self.after_finish()
		return response_success()

	def save_migration_history(self):
		migration_type = 'default'
		if to_int(self._notice['mode']) == MIGRATION_DEMO:
			migration_type = 'demo'
		if self._notice['config'].get('remigrate'):
			migration_type = 'remigrate'
		if self._notice['config'].get('recent'):
			migration_type = 'recent'
		if self._notice['config'].get('update_latest_data'):
			migration_type = 'smart update'
		if self._notice['config'].get('reset'):
			migration_type += '-reset'
		migration_history_data = {
			'migration_id': self._migration_id,
			'notice': json_encode(self._notice),
			'type': migration_type,
			'created_at': get_current_time()
		}
		self.insert_obj(TABLE_MIGRATION_HISTORY, migration_history_data)

	def check_url(self, url, timeout = False, proxies = None):
		if self._type and self._notice[self._type]['cart_type'] in ['wix','bigcommerce', 'squarespace', 'neto', 'shopify', 'smartweb', 'esty', 'customapi'] and self._notice[self._type]['cart_url'] == url:
			return response_success()
		if self._notice[self._type]['cart_type'] == 'bigcartel':
			url = to_str(url).replace('products.json', '')
		timeout = False  # disable
		response_head = io.BytesIO()
		h = io.BytesIO()
		c = pycurl.Curl()
		try:
			c.setopt(c.URL, url)
			c.setopt(c.WRITEFUNCTION, response_head.write)
			c.setopt(c.HEADERFUNCTION, h.write)
			c.setopt(c.SSL_VERIFYPEER, 0)
			c.setopt(c.USERAGENT, self.USER_AGENT)
			c.setopt(c.FOLLOWLOCATION, 1)
			if proxies:
				c.setopt(pycurl.PROXY, proxies)

			if self._notice[self.get_type()]['config'].get('auth'):
				auth_user = to_str(self._notice[self.get_type()]['config']['auth'].get('user'))
				auth_pass = to_str(self._notice[self.get_type()]['config']['auth'].get('pass'))
				c.setopt(pycurl.USERPWD, auth_user + ':' + auth_pass)
			if timeout:
				c.setopt(pycurl.TIMEOUT, 10)
				c.setopt(pycurl.CONNECTTIMEOUT, 10)
			else:
				c.setopt(pycurl.TIMEOUT, 30)
				c.setopt(pycurl.CONNECTTIMEOUT, 30)
			c.perform()
			head = h.getvalue().decode('utf-8')
			line_head = to_str(head).splitlines()
			location = ""

			for l in line_head:
				if "Location" in l:
					location = l.split(": ")[-1]
			if location:
				format_origin_url = self.format_url(url)
				format_redirect_url = self.format_url(location)
				if format_origin_url != format_redirect_url and format_origin_url.replace('https', '').replace('http', '') == format_redirect_url.replace('https', '').replace('http', ''):
					redirect_location = self.format_url(location)
					self._notice[self.get_type()]['cart_url'] = redirect_location
					return self.check_url(location)
			status = c.getinfo(pycurl.HTTP_CODE)

			if status == 200:
				if proxies:
					self._notice[self._type]['config']['proxy'] = proxies
				c.close()
				return response_success()
			c.close()
			try:
				res = response_head.getvalue().decode('utf-8')
			except:
				try:
					res = response_head.getvalue().decode('')
				except:
					res = ''
			if status == 403:
				if 'nginx' in to_str(res) and self.get_type() and self._notice[self.get_type()].get('setup_type') == 'connector' :
					return create_response('nginx')
				if not proxies:
					return self.check_url(url, timeout, self.PROXY_HOST)
			if status == 401 and self.get_type() and self._notice[self.get_type()].get('setup_type') == 'connector':
				return create_response('auth')
			msg_log = dict()
			msg_log['method'] = 'GET'
			msg_log['status'] = status
			msg_log['header'] = head
			msg_log['error'] = res
			self.log_error(url, msg_log)
		except Exception:
			self.log_traceback()
		return response_error()

	def get_list_seo(self):
		cart_type = self._notice[self.get_type()]['cart_type']
		path_seo = get_root_path() + '/cartmigration/models/seo/' + cart_type
		list_file = os.listdir(path_seo)
		list_seo = dict()
		for seo_file in list_file:
			seo_name = seo_file.replace('.py', '')
			list_seo['seo_' + cart_type + '_' + seo_name] = seo_name.upper() + ' Seo'
		result = dict()
		for model_name, label in list_seo.items():
			model = get_model(model_name)
			if model:
				result[model_name] = label
		return result

	def strip_html_tag(self, html, none_check = False):
		if not html:
			return ''
		if not self._notice['config'].get('strip_html') and not none_check:
			return html
		s = StripHtml()
		s.feed(to_str(html))
		return s.get_data()

	def strip_html_tag_wix(self, html):
		if not html:
			return ''
		s = StripHtmlWix()
		s.feed(to_str(html))
		return s.get_data()

	def create_sku_by_name(self, name, char = 32):
		sku = self.convert_attribute_code(name)
		if char and not isinstance(char, int):
			char = 32
		return sku[0:char]

	def remove_special_char(self, name):
		if not to_str(name).strip(' / - _'):
			return ''
		name = html.unescape(name)
		result = name.replace(' ', '-').replace('_', '-').replace('.', '-')
		result = result.replace('/', '')
		# result = re.sub('[\x21-\x2C]+', '', result)
		# result = re.sub('[\x3A-\x40]+', '', result)
		# result = re.sub('[\x5B-\x60]+', '', result)
		# result = re.sub('[\x7B-\x7E]+', '', result)
		result = ''.join(e for e in result if e.isalnum() or e == '-')
		result = result.strip(' -')
		while result.find('--') != -1:
			result = result.replace('--', '-')
		return result.strip(' -')

	def convert_attribute_code(self, name):
		if not to_str(name).strip(' / - _'):
			return ''
		str_convert = html.unescape(name)
		if isinstance(str_convert, bool):
			if str_convert:
				str_convert = 'yes'
			else:
				str_convert = 'no'
		result = self.generate_url(str_convert)
		if not result:
			return self.parse_url(str_convert).lower()
		try:
			check_encode = chardet.detect(result.encode())
			if check_encode['encoding'] != 'ascii' or not result:
				return self.parse_url(result).lower()
		except Exception:
			pass
		return result.strip('- ')

	def replace_url(self, url):
		result = url.strip(' -')
		result = result.replace(' ', '-').replace('_', '-')
		while result.find('--') != -1:
			result = result.replace('--', '-')
		result = result.replace(' ', '-').replace('_', '-')
		return result.strip(' -')

	def copy_data_from_parent_product(self, parent):
		children_data = self.construct_product_child()
		no_copy = [
			'languages',
			'options',
			'parent_configurable',
			'group_parent_ids',
			'attributes',
			'children',
			'group_child_ids',
			'relate',
			'seo', 'images', 'image', 'thumb_image', 'status',
			'description'

		]
		for code, value in parent.items():
			if code not in no_copy:
				children_data[code] = value
		return children_data

	def get_price_children(self, price, addition_price, price_prefix = '+'):
		if price_prefix == '-':
			new_price = to_decimal(price) - to_decimal(addition_price)
			return new_price if new_price > 0 else 0
		else:
			return to_decimal(price) + to_decimal(addition_price)

	def count_child_from_option(self, options):
		if not options:
			return 0
		child = 1
		for option in options:
			child *= to_len(option['values'])
		return child

	def convert_option_to_child(self, options, parent):
		variants = list()
		count = self.count_child_from_option(options)
		if count > self.VARIANT_LIMIT:
			msg = 'Product '
			if parent['id']:
				msg += 'id ' + to_str(parent['id'])
			elif parent['code']:
				msg += 'code: ' + parent['code']
			msg += ": too much variant (" + to_str(count) + ")"
			self.log(msg, 'variant')
			return variants
		options_src = dict()
		for option in options:
			if option['option_type'] not in ['select', 'drop_down', 'dropdown', self.OPTION_RADIO, self.OPTION_CHECKBOX, 'multiple', 'multiswatch', self.OPTION_MULTISELECT]:
				continue
			values = list()
			key_option = get_value_by_key_in_dict(option, 'id', option['option_code'])
			if option['values']:
				for value in option['values']:
					values.append(value['option_value_name'])
					opt_val = {
						'option_name': option['option_name'],
						'option_code': option['option_code'],
						'option_languages': option['option_languages'],
						'option_id': option['id'],
						'option_value_status': True if not value.get('option_value_disabled') else False,
						'qty': value['option_value_qty'],
						'option_value_id': value['id'],
						'sku': value['option_value_sku'],
						'option_value_name': value['option_value_name'],
						'option_value_code': value['option_value_code'],
						'price': value['option_value_price'],
						'price_prefix': value['price_prefix'],
						'option_value_languages': value['option_value_languages'],
						'thumb_image': get_value_by_key_in_dict(value, 'thumb_image', {'url': '', 'path': ''}),
						'weight': get_value_by_key_in_dict(value, 'option_value_weight', 0),
						'weight_prefix': get_value_by_key_in_dict(value, 'weight_prefix', '+')
					}
					if key_option not in options_src:
						options_src[key_option] = list()
					if opt_val['option_value_name']:
						options_src[key_option].append(opt_val)
		if not options_src:
			return variants
		combinations = self.combination_from_multi_dict(options_src)

		children_base_data = self.copy_data_from_parent_product(parent)

		for item, children in enumerate(combinations):
			children_data = copy.deepcopy(children_base_data)
			images = list()
			sku = ''
			name = ''
			min_qty = to_int(parent['qty'])
			status = children_data['status']
			for index, attribute in enumerate(children):
				if not attribute['option_value_status']:
					status = False
				attribute_data = self.construct_product_child_attribute()
				attribute_data['option_id'] = attribute['option_id']
				attribute_data['option_name'] = attribute['option_name']
				attribute_data['option_languages'] = attribute['option_languages']
				attribute_data['option_code'] = attribute['option_code']
				attribute_data['option_code_save'] = attribute['option_name']
				attribute_data['option_value_name'] = attribute['option_value_name']
				attribute_data['option_value_id'] = attribute['option_value_id']
				attribute_data['option_value_languages'] = attribute['option_value_languages']
				attribute_data['option_value_code'] = attribute['option_value_code']
				attribute_data['option_value_code_save'] = attribute['option_value_name']
				children_data['price'] = self.get_price_children(children_data['price'], attribute['price'], attribute['price_prefix'])
				children_data['weight'] = self.get_price_children(children_data['weight'], attribute['weight'], attribute['weight_prefix'])
				# children_data['weight'] = to_decimal(children_data['weight']) + to_decimal(attribute['weight_prefix'])
				if children_data['special_price']['price']:
					children_data['special_price']['price'] = self.get_price_children(children_data['special_price']['price'], attribute['price'], attribute['price_prefix'])
				attribute_data['option_value_price'] = to_decimal(attribute['price'])
				attribute_data['price_prefix'] = '+'
				children_data['attributes'].append(attribute_data)
				if sku:
					sku += '-'
				sku += attribute['sku'] if attribute['sku'] else attribute['option_value_name']
				if name:
					name += '-'
				name += attribute['option_value_name']
				if attribute['qty'] is not False and to_int(attribute['qty']) < min_qty:
					min_qty = to_int(attribute['qty'])
				if 'thumb_image' in attribute:
					if attribute['thumb_image']['url']:
						images.append(attribute['thumb_image'])
			if images:
				list_image = list()
				for index, image in enumerate(images):
					if index == 0:
						children_data['thumb_image']['url'] = image['url']
						children_data['thumb_image']['path'] = image['path']
					else:
						process_image = self.process_image_before_import(image['url'], image['path'])
						if process_image['url'] not in list_image:
							list_image.append(process_image['url'])
							children_data['images'].append(image)
			children_data['sku'] = to_str(children_data['sku']) + '-' + sku
			children_data['name'] += '-' + name
			children_data['qty'] = min_qty
			children_data['status'] = status
			children_data['code'] = children_data['sku'].lower()
			children_data['id'] = to_str(children_data['id']) + '' + to_str(item)
			variants.append(children_data)
		return variants

	def convert_child_to_option(self, childrens):
		max_attribute = 0
		option_src = list()
		for children in childrens:
			if max_attribute <= to_len(children['attributes']):
				max_attribute = to_len(children['attributes'])
				option_src = children['attributes']
		all_option_name = list()
		for option in option_src:
			if option['option_name'] in all_option_name:
				continue
			all_option_name.append(option['option_name'])
		options = dict()
		languages = dict()
		for children in childrens:
			for attribute in children['attributes']:
				if attribute['option_name'] not in all_option_name:
					continue
				if attribute['option_name'] not in options:
					options[attribute['option_name']] = dict()
				if not attribute['option_value_name'] or attribute['option_value_name'] in options[
					attribute['option_name']]:
					continue
				option_data = self.construct_product_option_value()
				option_data['option_code'] = attribute['option_code']
				option_data['option_value_name'] = attribute['option_value_name']
				option_data['option_value_code'] = attribute['option_value_code']
				option_data['option_value_languages'] = attribute['option_value_languages']
				option_data['option_value_price'] = attribute['price'] if attribute['price'] and to_int(attribute['price']) > 0 else (children['price'] if to_len(children['attributes']) < 2 and to_int(children['price']) > 0 else 0)
				option_data['price_prefix'] = attribute['price_prefix']
				languages[attribute['option_name']] = attribute['option_languages']
				options[attribute['option_name']][attribute['option_value_name']] = option_data
		all_options = list()
		for option_name, option in options.items():
			option_data = self.construct_product_option()
			option_data['id'] = None
			option_data['code'] = self.convert_attribute_code(option_name)
			option_data['option_type'] = 'select'
			option_data['option_name'] = option_name
			option_data['option_languages'] = languages[option_name] if option_name in languages else ''
			for option_value_name, option_value in option.items():
				if option_value['option_code'] and option_value['option_code'] != '':
					option_data['code'] = option_value['option_code']
					option_data['option_code'] = option_value['option_code']
				option_data['values'].append(option_value)
			all_options.append(option_data)
		return all_options

	def log_time(self, time_str, url, data):
		msg = "-------------------------"
		msg += '\n' + url
		msg += '\n Time: ' + time_str
		msg += '\n Data: ' + to_str(data)
		self.log(msg, to_str(self._type) + "_time_queries")

	# check app mode demo unlimited product
	def get_migrate_product_extend_config(self):
		if self.migrate_product_extend_config is not None:
			return self.migrate_product_extend_config
		if to_int(self._notice['mode']) == MIGRATION_FULL or not self._notice['config'].get('app_mode') or self._notice['target']['cart_type'] == 'prestashop':
			self.migrate_product_extend_config = True
			return True
		limit_app_mode = self.get_app_mode_limit()
		if not limit_app_mode or limit_app_mode['result'] != 'success':
			self.migrate_product_extend_config = True
			return True
		limit_app_mode_data = limit_app_mode['data']
		entity_limit = limit_app_mode_data['limit']
		if to_str(entity_limit) != 'unlimited':
			self.migrate_product_extend_config = True
			return True
		if to_str(limit_app_mode_data['entities']) == 'all' or (isinstance(limit_app_mode_data['entities'], list) and 'products' in limit_app_mode_data['entities']):
			self.migrate_product_extend_config = False
			return False
		self.migrate_product_extend_config = True
		return True

	def delete_map_demo(self, entity_type, entity_ids, field = 'id_desc'):
		where = {
			'migration_id': self._migration_id,
			'type': entity_type
		}
		query_delete = "DELETE FROM " + TABLE_MAP + " WHERE " + self.dict_to_where_condition(where) + ' AND ' + field + " IN " + self.list_to_in_condition(entity_ids)
		self.query_raw(query_delete)
		return response_success()

	def replace_url_src_to_url_target(self, content):
		if not content:
			return content
		parse_url_src = to_str(self._notice['src']['cart_url'])
		replace_str = ['https://www', 'http://www', 'https://', 'http://']
		for row in replace_str:
			parse_url_src = parse_url_src.replace(row, '')
		for row in replace_str:
			content = content.replace(row + parse_url_src, self._notice['target']['cart_url'])
		return content

	def display_recent_data(self):
		recent_data = self.get_recent(self._migration_id)
		if recent_data:
			for entity_type, data in recent_data['process'].items():
				if entity_type not in self._notice['process']:
					continue
				if self._notice['config'].get('update_latest_data') and self._notice['target']['config'].get('entity_update', dict()).get(entity_type):
					self._notice['process'][entity_type]['total_update'] = data['total']
				self._notice['process'][entity_type]['id_src'] = data['id_src']
				self._notice['process'][entity_type]['total'] = 0
				self._notice['process'][entity_type]['imported'] = 0
				self._notice['process'][entity_type]['error'] = 0
				self._notice['process'][entity_type]['time_finish'] = 0
				self._notice['process'][entity_type]['finish'] = False

	def create_async_request_data(self, url, data = None, method = 'POST'):
		if to_str(method).lower() not in ['post', 'delete', 'get', 'put']:
			method = 'get'
		return {
			'url': url,
			'data': json_encode(data) if data else None,
			'method': method.lower()
		}

	def async_request_by_method(self, data, custom_headers = None):
		if not data:
			return True
		if not custom_headers:
			custom_headers = dict()
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		elif isinstance(custom_headers, dict) and not custom_headers.get('User-Agent'):
			custom_headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) Gecko/20110619 Firefox/5.0'
		retry = 0
		requests_extend_data = list()
		retry_data = list()
		retry_index = list()
		all_data = data
		all_response = list()
		is_retry = False
		while to_len(all_data) or (to_len(retry_data) and retry < 5):
			if not all_data:
				retry += 1
				all_data = retry_data
				retry_data = list()
			all_request = [getattr(grequests, row['method'].lower())(row['url'], data = row['data'], headers = custom_headers) for row in all_data]
			all_import = grequests.map(all_request)
			status_code = [row.status_code for row in all_import]
			text = [json_decode(row.text) for row in all_import]
			if not is_retry:
				all_response = list(all_response + text)
			current_retry = list()
			if retry_index:
				current_retry = retry_index
				retry_index = list()
			for index, status in enumerate(status_code):
				if is_retry:
					all_response[current_retry[index]] = all_response[index]
				if status > 300 and status not in [400, 409]:
					retry_index.append(index if not current_retry else current_retry[index])
					retry_data.append(all_data[index])
				if status > 300:
					msg_log = dict()
					msg_log['data'] = to_str(all_data[index]['data'])
					msg_log['method'] = all_data[index]['method']
					msg_log['status'] = status
					msg_log['response'] = text[index]
					msg_log['error'] = ''
					self.log_error(all_data[index]['url'], msg_log, to_str(self.get_type()) + '_status')
			if not requests_extend_data and not retry_data:
				break
			if not requests_extend_data:
				is_retry = True
			if is_retry:
				self.sleep_time(retry * 60)
			all_data = requests_extend_data
			requests_extend_data = list()

		return all_response

	def get_param_from_content_xml(self, content, field):
		try:
			root = ElementTree.fromstring(content)
			value = root.find(field).text
			return value
		except Exception:
			self.log_traceback()
			return ''

	def sleep_time(self, value, status = 200, warning = False, resume_action = False, msg = ''):
		if self._notice and self._notice['running']:
			if status not in [200, 201, 204]:
				self._clear_entity_warning = False
				self._total_time_sleep += to_decimal(value)
				if self._total_time_sleep > 30 or warning:
					entity_warning = {
						'entity': self._notice['resume']['type'],
						'status': to_str(status),
						'type': self._type
					}
					if msg:
						entity_warning['msg'] = msg
					if resume_action:
						entity_warning['resume'] = True
					self.save_migration(self._migration_id, {'entity_warning': json_encode(entity_warning)})
			if to_int(value):
				time.sleep(to_int(value))

		return

	def generate_url(self, title):
		if not title:
			return ''
		title = self.remove_special_char(title).lower()
		title = title.strip(' -')
		special = {
			'Æ': 'AE',
			'Đ': 'd',
			'Ø': 'O',
			'Þ': 'TH',
			'ß': 'ss',
			'æ': 'ae',
			'ð': 'd',
			'ø': 'o',
			'þ': 'th',
			'Œ': 'OE',
			'œ': 'oe',
			'ƒ': 'f',
		}
		for index, val in special.items():
			title = title.replace(index, val)
		chars = list(title)
		res = list()
		for char in chars:
			text = unicodedata.normalize('NFD', char).encode('ascii', 'ignore')
			res.append(text.decode() if text.decode() else char)
		res = ''.join(res)
		res = self.replace_url(res)
		return res

	def nl2br(self, string, is_xhtml = True):
		string = to_str(string)
		if not string:
			return ''
		if is_xhtml:
			return string.replace('\n', '<br />\n')
		else:
			return string.replace('\n', '<br>\n')

	def validate_cart_url(self):
		return response_success()

	def validate_api_info(self):
		return response_success()

	def validate_database_info(self):
		return response_success()

	def detect_cart_type(self, url):
		cart = {
			'shopify': "myshopify.com",
			'wix': "wixsite.com",
			'3dcart': "3dcartstores.com",
		}
		for cart_type, domain in cart.items():
			if domain in url:
				return cart_type
		return False

	def install_module_connector(self, request):
		return response_success()

	def get_module_connector_info(self):
		return {
			'account': 'Admin Username',
			'password': 'Admin Password'
		}

	def get_path_connector(self):
		return ''

	def get_link_change_token(self):
		return ''
	def get_summary_demo_by_type(self, entity_type):
		summary = self.select_page(TABLE_MAP, {'type': entity_type, 'migration_id': self._migration_id}, limit = 20, order_by = 'id_src')
		if summary['result'] != 'success' or not summary['data']:
			return list()
		return summary['data']
