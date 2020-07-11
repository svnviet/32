from cartmigration.models.basecart import LeBasecart
from cartmigration.libs.utils import *

class LeCartPrestashop13(LeBasecart):

	# TODO: DISPLAY
	def display_config_source(self):
		parent = super().display_config_source()
		if parent['result'] != 'success':
			return parent
		default_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT cfg.*, lg.* FROM _DBPRF_lang AS lg LEFT JOIN _DBPRF_configuration AS cfg ON lg.id_lang = cfg.value WHERE cfg.name = 'PS_LANG_DEFAULT'"
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT cfg.*, cur.* FROM _DBPRF_currency AS cur LEFT JOIN _DBPRF_configuration AS cfg ON cur.id_currency = cfg.value WHERE cfg.name = 'PS_CURRENCY_DEFAULT'"
			}
		}
		# default_config = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(default_queries)
		# })
		default_config = self.select_multiple_data_connector(default_queries, 'config')
		if (not default_config) or (default_config['result'] != 'success'):
			return response_error()
		default_config_data = default_config['data']
		if default_config_data and default_config_data['languages'] and default_config_data['currencies']:
			self._notice['src']['language_default'] = default_config_data['languages'][0]['id_lang'] if default_config_data['languages'][0]['id_lang'] else 1
			self._notice['src']['currency_default'] = default_config_data['currencies'][0]['id_currency'] if default_config_data['currencies'][0]['id_currency'] else 1
		self._notice['src']['site'] = {
			1: 'Default Shop',
		}
		self._notice['src']['category_root'] = 1
		self._notice['src']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['src']['attributes'] = {
			1: 'Default Attribute',
		}
		config_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_lang WHERE active = 1",
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_currency",
			},
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_order_state_lang WHERE id_lang = '" + self._notice['src']['language_default'] + "'",
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_group_lang where id_lang = '" + self._notice['src']['language_default'] + "'"
			},
		}
		# config = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(config_queries)
		# })
		config = self.select_multiple_data_connector(config_queries, 'config')
		if (not config) or (config['result'] != 'success'):
			return response_error("can't display config source")
		config_data = config['data']
		language_data = dict()
		storage_cat_data = dict()
		currency_data = dict()
		order_status_data = dict()
		for language_row in config_data['languages']:
			language_data[language_row['id_lang']] = to_str(language_row['name']) + "(" + to_str(language_row['iso_code']) + ")"
		for currency_row in config_data['currencies']:
			currency_data[currency_row['id_currency']] = currency_row['name']
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['id_order_state']] = order_status_row['name']
		customer_group_data = dict()
		if config_data['customer_group'] and to_len(config_data['customer_group']) > 0:
			for customer_group in config_data['customer_group']:
				customer_group_data[customer_group['id_group']] = customer_group['name']
		else:
			customer_group_data['1'] = 'Default'
		self._notice['src']['languages_select'] = language_data
		self._notice['src']['languages'] = language_data
		self._notice['src']['store_category'] = storage_cat_data
		self._notice['src']['currencies'] = currency_data
		self._notice['src']['order_status'] = order_status_data
		self._notice['src']['customer_group'] = customer_group_data
		self._notice['src']['support']['country_map'] = False
		self._notice['src']['support']['languages_select'] = True
		self._notice['src']['support']['site_map'] = False
		self._notice['src']['support']['customer_group_map'] = True
		self._notice['src']['support']['order_status_map'] = True
		self._notice['src']['support']['reviews'] = False
		self._notice['src']['support']['seo'] = True
		self._notice['src']['support']['seo_301'] = True
		self._notice['src']['support']['cus_pass'] = True
		self._notice['src']['support']['coupons'] = True
		self._notice['src']['support']['pages'] = True

		return response_success()

	def display_config_target(self):
		parent = super().display_config_source()
		if parent['result'] != 'success':
			return parent
		return response_success()

	def display_confirm_source(self):
		return response_success()

	def display_confirm_target(self):
		self._notice['target']['clear']['function'] = 'clear_target_taxes'
		return response_success()

	def get_query_display_import_source(self, update = False):
		compare_condition = ' > '
		if update:
			compare_condition = ' <= '
		queries = {
			'taxes': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_tax WHERE id_tax " + compare_condition + to_str(self._notice['process']['taxes']['id_src']),
			},
			'manufacturers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count  FROM _DBPRF_manufacturer WHERE id_manufacturer " + compare_condition + to_str(self._notice['process']['manufacturers']['id_src']),
			},
			'categories': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_category WHERE id_parent > 0 AND id_category " + compare_condition + to_str(self._notice['process']['categories']['id_src']),
			},
			'products': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_product WHERE id_product " + compare_condition + to_str(self._notice['process']['products']['id_src']),
			},
			'customers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_customer WHERE id_customer " + compare_condition + to_str(self._notice['process']['customers']['id_src']),
			},
			'orders': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_orders WHERE id_order " + compare_condition + to_str(self._notice['process']['orders']['id_src']),
			},
			'coupons': {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_discount WHERE id_discount ' + compare_condition + to_str(self._notice['process']['coupons']['id_src']),
			},
			'pages': {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_cms WHERE id_cms ' + compare_condition + to_str(self._notice['process']['pages']['id_src']),
			},
		}
		return queries

	def display_import_source(self):
		parent = super().display_import_source()
		if parent['result'] != 'success':
			return parent
		if self._notice['config']['add_new']:
			self.display_recent_data()

		queries = self.get_query_display_import_source()
		count = self.select_multiple_data_connector(queries, 'config')
		if (not count) or (count['result'] != 'success'):
			return response_error()
		real_totals = dict()
		for key, row in count['data'].items():
			total = self.list_to_count_import(row, 'count')
			real_totals[key] = total
		for key, total in real_totals.items():
			self._notice['process'][key]['total'] = total
		return response_success()

	def display_update_source(self):
		queries = self.get_query_display_import_source(True)
		count = self.select_multiple_data_connector(queries, 'count')

		if (not count) or (count['result'] != 'success'):
			return response_error()
		real_totals = dict()
		for key, row in count['data'].items():
			total = self.list_to_count_import(row, 'count')
			real_totals[key] = total
		for key, total in real_totals.items():
			self._notice['process'][key]['total_update'] = total
		return response_success()

	def display_import_target(self):
		return response_success()

	def display_finish_source(self):
		return response_success()

	def display_finish_target(self):
		return response_success()

	def prepare_import_source(self):
		return response_success()

	def prepare_import_target(self):
		return response_success()

	# TODO: CLEAR

	def clear_target_taxes(self):
		return self._notice['target']['clear']

	def clear_target_manufacturers(self):
		return self._notice['target']['clear']

	def clear_target_categories(self):
		return self._notice['target']['clear']

	def clear_target_products(self):
		return self._notice['target']['clear']

	def clear_target_customers(self):
		return self._notice['target']['clear']

	def clear_target_orders(self):
		return self._notice['target']['clear']

	def clear_target_reviews(self):
		return self._notice['target']['clear']

	# TODO: TAX

	def prepare_taxes_import(self):
		return self

	def prepare_taxes_export(self):
		return self

	def get_taxes_main_export(self):
		id_src = self._notice['process']['taxes']['id_src']
		limit = self._notice['setting']['taxes']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_tax WHERE id_tax > " + to_str(id_src) + " ORDER BY id_tax ASC LIMIT " + to_str(limit)
		}
		# taxes = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		taxes = self.select_data_connector(query, 'taxes')
		if not taxes or taxes['result'] != 'success':
			return response_error('could not get taxes main to export')
		return taxes

	def get_taxes_ext_export(self, taxes):
		tax_ids = duplicate_field_value_from_list(taxes['data'], 'id_tax')
		taxes_ext_queries = {
			'tax_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_lang WHERE id_tax IN " + self.list_to_in_condition(tax_ids)
			},
			'tax_state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_state WHERE id_tax IN " + self.list_to_in_condition(tax_ids)
			},
			'tax_zone': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_zone WHERE id_tax IN " + self.list_to_in_condition(tax_ids)
			},
		}
		# taxes_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(taxes_ext_queries)
		# })
		taxes_ext = self.select_multiple_data_connector(taxes_ext_queries, 'taxes')
		if not taxes_ext or taxes_ext['result'] != 'success':
			return response_error('err taxes ext data')

		state_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_state'], 'id_state')
		zone_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_zone'], 'id_zone')
		taxes_ext_rel_queries = {
			'state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_state WHERE id_state IN " + self.list_to_in_condition(state_ids)
			},
			'zone': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_zone WHERE id_zone IN " + self.list_to_in_condition(zone_ids)
			},
		}
		# taxes_ext_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(taxes_ext_rel_queries),
		# })
		taxes_ext_rel = self.select_multiple_data_connector(taxes_ext_rel_queries, 'taxes')
		if not taxes_ext_rel or taxes_ext_rel['result'] != 'success':
			return response_error('err taxes ext rel data')
		taxes_ext = self.sync_connector_object(taxes_ext, taxes_ext_rel)

		country_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_state'], 'id_state')
		taxes_ext_rel_rel_queries = {
			'country': {
				'type': 'select',
				'query': "SELECT c.*, cl.* FROM _DBPRF_country AS c LEFT JOIN _DBPRF_country_lang AS cl ON c.id_country = cl.id_country WHERE c.id_country IN " + self.list_to_in_condition(country_ids) + " OR c.id_zone IN " + self.list_to_in_condition(zone_ids)
			},
		}
		# taxes_ext_rel_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(taxes_ext_rel_rel_queries),
		# })
		taxes_ext_rel_rel = self.select_multiple_data_connector(taxes_ext_rel_rel_queries, 'taxes')
		if not taxes_ext_rel_rel or taxes_ext_rel_rel['result'] != 'success':
			return response_error('err taxes ext rel data')
		taxes_ext = self.sync_connector_object(taxes_ext, taxes_ext_rel_rel)

		return taxes_ext

	def convert_tax_export(self, tax, taxes_ext):
		tax_zone_data = list()
		tax_product_data = list()

		tax_product = self.construct_tax_product()
		tax_lang = get_list_from_list_by_field(taxes_ext['data']['tax_lang'], 'id_tax', tax['id_tax'])
		tax_name = get_row_value_from_list_by_field(tax_lang, 'id_lang', to_str(self._notice['src']['language_default']), 'name')
		tax_product['id'] = tax['id_tax']
		tax_product['code'] = None
		tax_product['name'] = tax_name
		tax_product_data.append(tax_product)

		src_tax_states = get_list_from_list_by_field(taxes_ext['data']['tax_state'], 'id_tax', tax['id_tax'])
		if src_tax_states:
			for src_tax_state in src_tax_states:
				tax_zone_rate = self.construct_tax_zone_rate()
				tax_zone_rate['id'] = ''
				tax_zone_rate['name'] = tax_name
				tax_zone_rate['rate'] = tax['rate']

				tax_zone_state = self.construct_tax_zone_state()
				tax_zone_country = self.construct_tax_zone_country()

				state_row = get_row_from_list_by_field(taxes_ext['data']['state'], 'id_state', src_tax_state['id_state'])
				if state_row:
					tax_zone_state['id'] = state_row['id_state']
					tax_zone_state['name'] = state_row['name']
					tax_zone_state['state_code'] = state_row['iso_code']

					countries = get_list_from_list_by_field(taxes_ext['data']['country'], 'id_country', state_row['id_country'])
					country_row = get_row_from_list_by_field(countries, 'id_lang', to_str(self._notice['src']['language_default']))
					if country_row:
						tax_zone_country['id'] = country_row['id_country']
						tax_zone_country['name'] = country_row['name']
						tax_zone_country['country_code'] = country_row['iso_code']

				tax_zone = self.construct_tax_zone()
				tax_zone['id'] = tax['id_tax']
				tax_zone['name'] = tax_name
				tax_zone['country'] = tax_zone_country
				tax_zone['state'] = tax_zone_state
				tax_zone['rate'] = tax_zone_rate
				tax_zone_data.append(tax_zone)

		tax_data = self.construct_tax()
		tax_data['id'] = tax['id_tax']
		tax_data['name'] = tax_name
		tax_data['created_at'] = ''
		tax_data['updated_at'] = ''
		tax_data['tax_products'] = tax_product_data
		tax_data['tax_zones'] = tax_zone_data

		return response_success(tax_data)

	def get_tax_id_import(self, convert, tax, taxes_ext):
		return tax['id_tax']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id']) else False

	def router_tax_import(self, convert, tax, taxes_ext):
		return response_success('tax_import')

	def before_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def tax_import(self, convert, tax, taxes_ext):
		return response_success(1)

	def after_tax_import(self, tax_id, convert, tax, taxes_ext):
		return response_success()

	def addition_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	# TODO: MANUFACTURER

	def prepare_manufacturers_import(self):
		return self

	def prepare_manufacturers_export(self):
		return self

	def get_manufacturers_main_export(self):
		id_src = self._notice['process']['manufacturers']['id_src']
		limit = self._notice['setting']['manufacturers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_manufacturer WHERE id_manufacturer > " + to_str(id_src) + " ORDER BY id_manufacturer ASC LIMIT " + to_str(limit),
		}
		# manufacturers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		manufacturers = self.select_data_connector(query, 'manufacturers')
		if not manufacturers or manufacturers['result'] != 'success':
			return response_error('could not get manufacturers main to export')
		return manufacturers

	def get_manufacturers_ext_export(self, manufacturers):
		manufacturer_ids = duplicate_field_value_from_list(manufacturers['data'], 'id_manufacturer')
		manufacturers_ext_queries = {
			'manufacturer_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_manufacturer_lang WHERE id_manufacturer IN " + self.list_to_in_condition(manufacturer_ids)
			}
		}
		# manufacturers_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(manufacturers_ext_queries),
		# })
		manufacturers_ext = self.select_multiple_data_connector(manufacturers_ext_queries, 'manufacturers')
		if not manufacturers_ext or manufacturers_ext['result'] != 'success':
			return response_error('err manufacturers ext data')

		return manufacturers_ext

	def convert_manufacturer_export(self, manufacturer, manufacturers_ext):
		manufacturer_data = self.construct_manufacturer()
		manufacturer_data['id'] = manufacturer['id_manufacturer']
		manufacturer_data['name'] = manufacturer['name']
		manufacturer_data['url'] = ''
		manufacturer_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_manufacturer'])
		manufacturer_data['thumb_image']['path'] = 'm/' + to_str(manufacturer['id_manufacturer']) + '.jpg'
		manufacturer_lang = get_list_from_list_by_field(manufacturers_ext['data']['manufacturer_lang'], 'id_manufacturer', manufacturer['id_manufacturer'])
		manufacturer_url = get_row_from_list_by_field(manufacturer_lang, 'id_lang', self._notice['src']['language_default'])
		manufacturer_data['url'] = manufacturer_url
		manufacturer_data['created_at'] = convert_format_time(manufacturer['date_add'])
		manufacturer_data['updated_at'] = convert_format_time(manufacturer['date_upd'])

		return response_success(manufacturer_data)

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		return manufacturer['id_manufacturer']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def after_manufacturer_import(self, manufacturer_id, convert, manufacturer, manufacturers_ext):
		return response_success()

	def addition_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	# TODO: CATEGORY

	def prepare_categories_export(self):
		return self

	def prepare_categories_import(self):
		return self

	def get_categories_main_export(self):
		id_src = self._notice['process']['categories']['id_src']
		limit = self._notice['setting']['categories']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_category WHERE id_parent > 0 AND  id_category > 1 AND id_category > " + to_str(id_src) + " ORDER BY id_category ASC LIMIT " + to_str(limit)
		}
		# categories = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		categories = self.select_data_connector(query, 'categories')
		if not categories or categories['result'] != 'success':
			return response_error('could not get categories main to export')
		return categories

	def get_categories_ext_export(self, categories):
		category_ids = duplicate_field_value_from_list(categories['data'], 'id_category')
		categories_ext_queries = {
			'category_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category_lang WHERE id_category IN " + self.list_to_in_condition(category_ids),
			},
		}
		# categories_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(categories_ext_queries),
		# })
		categories_ext = self.select_multiple_data_connector(categories_ext_queries, 'categories')
		if not categories_ext or categories_ext['result'] != 'success':
			return response_error('err categories ext data')
		return categories_ext

	def convert_category_export(self, category, categories_ext):
		category_data = self.construct_category()
		parent = self.construct_category_parent()
		parent_ids = list()
		if category['id_parent'] and to_decimal(category['id_parent']) != to_decimal(category['id_category']) and to_int(category['id_parent']) > 2:
			parent_ids.append(category['id_parent'])
			parent_data = self.get_category_parent(category['id_parent'])
			if parent_data['result'] == 'success':
				parent = parent_data['data']
			else:
				return response_error()
		else:
			parent['id'] = 0

		category_data['id'] = category['id_category']
		category_data['parent'] = parent
		category_data['active'] = True if to_int(category['active']) == 1 else False
		category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_category'])
		category_data['thumb_image']['path'] = 'c/' + to_str(category['id_category']) + '.jpg'
		category_data['thumb_image']['label'] = category['id_category']

		category_data['sort_order'] = 1
		category_data['created_at'] = convert_format_time(category['date_add'])
		category_data['updated_at'] = convert_format_time(category['date_upd'])
		category_data['category'] = category
		category_data['categories_ext'] = categories_ext
		category_description = get_list_from_list_by_field(categories_ext['data']['category_lang'], 'id_category', category['id_category'])
		category_description_def = get_row_from_list_by_field(category_description, 'id_lang', self._notice['src']['language_default'])
		if not category_description_def:
			category_description_def = category_description[0]
		category_data['name'] = category_description_def['name']
		category_data['description'] = category_description_def['description']
		category_data['meta_title'] = category_description_def['meta_title']
		category_data['meta_keyword'] = category_description_def['meta_keywords']
		category_data['meta_description'] = category_description_def['meta_description']
		for language_id, language_label in self._notice['src']['languages'].items():
			category_language_data = self.construct_category_lang()
			category_description_lang = get_row_from_list_by_field(category_description, 'id_lang', language_id)
			if not category_description_lang:
				category_description_lang = category_description_def
			category_language_data['name'] = category_description_lang['name']
			category_language_data['description'] = category_description_lang['description']
			category_language_data['meta_title'] = category_description_lang['meta_title']
			category_language_data['meta_keyword'] = category_description_lang['meta_keywords']
			category_language_data['meta_description'] = category_description_lang['meta_description']
			category_data['languages'][language_id] = category_language_data
		# if self._notice['config']['seo']:
		detect_seo = self.detect_seo()
		category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)
		return response_success(category_data)

	def get_category_id_import(self, convert, category, categories_ext):
		return category['id_category']

	def check_category_import(self, convert, category, categories_ext):
		return True if self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id']) else False

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def category_import(self, convert, category, categories_ext):
		return response_success(1)

	def after_category_import(self, category_id, convert, category, categories_ext):
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	# TODO: PRODUCT

	def prepare_products_export(self):
		return self

	def prepare_products_import(self):
		return self

	def get_products_main_export(self):
		id_src = self._notice['process']['products']['id_src']
		limit = self._notice['setting']['products']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_product WHERE id_product > " + to_str(id_src) + " ORDER BY id_product ASC LIMIT " + to_str(limit)
		}
		# products = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		products = self.select_data_connector(query, 'products')
		if not products or products['result'] != 'success':
			return response_error('could not get products main to export')
		return products

	def get_products_ext_export(self, products):
		product_ids = duplicate_field_value_from_list(products['data'], 'id_product')
		manu_ids = duplicate_field_value_from_list(products['data'], 'id_manufacturer')
		product_ext_queries = {
			'product_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_lang WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'category_product': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category_product WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'product_attribute': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_attribute WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'feature_product': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_feature_product WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'product_tag': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_tag WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'image': {
				'type': 'select',
				'query': "SELECT img.*, imgl.* FROM _DBPRF_image AS img LEFT JOIN _DBPRF_image_lang AS imgl ON imgl.id_image = img.id_image WHERE img.id_product IN " + self.list_to_in_condition(product_ids) + " AND imgl.id_lang = " + self._notice['src']['language_default']
			},
			'manufacturer': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_manufacturer WHERE id_manufacturer IN " + self.list_to_in_condition(manu_ids)
			},
			'accessory': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_accessory  WHERE id_product_1 IN " + self.list_to_in_condition(product_ids) + " OR id_product_2 IN " + self.list_to_in_condition(product_ids),
			},
		}
		# product_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(product_ext_queries),
		# })
		product_ext = self.select_multiple_data_connector(product_ext_queries, 'products')
		if not product_ext or product_ext['result'] != 'success':
			return response_error('err product ext data')

		product_attribute_ids = duplicate_field_value_from_list(product_ext['data']['product_attribute'], 'id_product_attribute')
		feature_ids = duplicate_field_value_from_list(product_ext['data']['feature_product'], 'id_feature')
		feature_value_ids = duplicate_field_value_from_list(product_ext['data']['feature_product'], 'id_feature_value')
		tag_ids = duplicate_field_value_from_list(product_ext['data']['product_tag'], 'id_tag')
		product_ext_rel_queries = {
			'product_attribute_combination': {
				'type': 'select',
				'query': "SELECT pac.*, a.* FROM _DBPRF_product_attribute_combination AS pac LEFT JOIN _DBPRF_attribute AS a ON a.id_attribute = pac.id_attribute WHERE pac.id_product_attribute IN " + self.list_to_in_condition(product_attribute_ids)
			},
			'feature_lang': {
				'type': 'select',
				'query': "SELECT f.*, fl.* FROM _DBPRF_feature AS f LEFT JOIN _DBPRF_feature_lang AS fl ON fl.id_feature = f.id_feature WHERE f.id_feature IN " + self.list_to_in_condition(feature_ids)
			},
			'feature_value_lang': {
				'type': 'select',
				'query': "SELECT fv.*, fvl.* FROM _DBPRF_feature_value AS fv LEFT JOIN _DBPRF_feature_value_lang AS fvl ON fvl.id_feature_value = fv.id_feature_value WHERE fv.id_feature_value IN " + self.list_to_in_condition(feature_value_ids)
			},
			'feature_value': {
				'type': 'select',
				'query': "SELECT * FROM  _DBPRF_feature_value  WHERE id_feature IN " + self.list_to_in_condition(
					feature_ids)
			},
			'tags': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tag WHERE id_tag IN " + self.list_to_in_condition(tag_ids)
			},
		}
		# product_ext_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(product_ext_rel_queries),
		# })
		product_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, 'products')
		if not product_ext_rel or product_ext_rel['result'] != 'success':
			return response_error('err product ext rel data')
		product_ext = self.sync_connector_object(product_ext, product_ext_rel)

		attribute_group_ids = duplicate_field_value_from_list(product_ext_rel['data']['product_attribute_combination'], 'id_attribute_group')
		attribute_ids = duplicate_field_value_from_list(product_ext_rel['data']['product_attribute_combination'], 'id_attribute')
		product_ext_rel_rel_queries = {
			'attribute_group_lang': {
				'type': 'select',
				'query': "SELECT ag.*, agl.* FROM _DBPRF_attribute_group AS ag LEFT JOIN _DBPRF_attribute_group_lang AS agl ON agl.id_attribute_group = ag.id_attribute_group WHERE ag.id_attribute_group IN " + self.list_to_in_condition(attribute_group_ids),
			},
			'attribute_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_attribute_lang WHERE id_attribute IN " + self.list_to_in_condition(attribute_ids),
			},
		}
		# product_ext_rel_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(product_ext_rel_rel_queries),
		# })
		product_ext_rel_rel = self.select_multiple_data_connector(product_ext_rel_rel_queries, 'products')
		if not product_ext_rel_rel or product_ext_rel_rel['result'] != 'success':
			return response_error('err product ext rel rel data')
		product_ext = self.sync_connector_object(product_ext, product_ext_rel_rel)

		return product_ext

	def convert_product_export(self, product, products_ext):
		product_data = self.construct_product()
		product_data['id'] = product['id_product']
		product_data['sku'] = product['reference']
		product_data['price'] = to_decimal(product['price']) if product['price'] else 0
		product_data['weight'] = product['weight']
		product_data['status'] = True if to_int(product['active']) == 1 else False
		product_data['qty'] = product['quantity']
		product_data['manage_stock'] = True if to_int(product['quantity']) > 0 else False
		product_data['created_at'] = convert_format_time(product['date_add'])
		product_data['updated_at'] = convert_format_time(product['date_upd'])

		special_price = False
		if to_decimal(product['reduction_price']) > 0:
			special_price = to_decimal(product['price']) - to_decimal(product['reduction_price'])
		elif to_decimal(product['reduction_percent']) > 0:
			special_price = to_decimal(product['price']) - to_decimal(to_decimal(product['reduction_percent']) * to_decimal(product['price']))
		if special_price and to_decimal(special_price) < to_decimal(product_data['price']):
			product_data['special_price']['price'] = special_price
			product_data['special_price']['start_date'] = convert_format_time(product['reduction_from'])
			product_data['special_price']['end_date'] = convert_format_time(product['reduction_to'])

		product_descriptions = get_list_from_list_by_field(products_ext['data']['product_lang'], 'id_product', product['id_product'])
		if not product_descriptions:
			msg = 'Product id ' + to_str(product['id_product']) + ' not description.'
			return response_error(msg)
		product_description_def = get_row_from_list_by_field(product_descriptions, 'id_lang', self._notice['src']['language_default'])
		if not product_description_def:
			product_description_def = product_descriptions[0]
		product_data['name'] = product_description_def['name']
		product_data['description'] = product_description_def['description']
		product_data['short_description'] = product_description_def['description_short']
		product_data['meta_title'] = product_description_def['meta_title']
		product_data['meta_keyword'] = product_description_def['meta_keywords']
		product_data['meta_description'] = product_description_def['meta_description']
		for product_description in product_descriptions:
			product_language_data = self.construct_product_lang()
			product_language_data['name'] = product_description['name']
			product_language_data['description'] = product_description['description']
			product_language_data['short_description'] = product_description['description_short']
			product_language_data['meta_title'] = product_description['meta_title']
			product_language_data['meta_keyword'] = product_description['meta_keywords']
			product_language_data['meta_description'] = product_description['meta_description']
			language_id = product_description['id_lang']
			product_data['languages'][language_id] = product_language_data

		product_images = get_list_from_list_by_field(products_ext['data']['image'], 'id_product', product['id_product'])
		product_image_main = get_row_from_list_by_field(product_images, 'cover', '1')
		url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
		if product_image_main:
			product_data['thumb_image']['url'] = url_product_image
			product_data['thumb_image']['path'] = self._get_image_path(product_image_main['id_image'], product['id_product'])
		if product_images:
			for product_image in product_images:
				if product_image['cover'] == 1:
					continue
				product_image_data = self.construct_product_image()
				product_image_data['label'] = product_image['legend']
				product_image_data['url'] = url_product_image
				product_image_data['path'] = self._get_image_path(product_image['id_image'], product['id_product'])
				product_data['images'].append(product_image_data)

		product_data['tax']['id'] = product['id_tax']

		product_data['manufacturer']['id'] = product['id_manufacturer']
		manufacturer_desc = get_row_from_list_by_field(products_ext['data']['manufacturer'], 'id_manufacturer', product['id_manufacturer'])
		if manufacturer_desc:
			product_data['manufacturer']['name'] = manufacturer_desc['name']

		product_categories = get_list_from_list_by_field(products_ext['data']['category_product'], 'id_product', product['id_product'])
		if product_categories:
			for product_category in product_categories:
				product_category_data = self.construct_product_category()
				product_category_data['id'] = product_category['id_category']
				product_data['categories'].append(product_category_data)

		product_feature = get_list_from_list_by_field(products_ext['data']['feature_product'], 'id_product', product['id_product'])
		if product_feature:
			feature_ids = duplicate_field_value_from_list(product_feature, 'id_feature')
			for feature_id in feature_ids:
				option_data = self.construct_product_option()
				option_data['id'] = feature_id
				feature_langs = get_list_from_list_by_field(products_ext['data']['feature_lang'], 'id_feature', feature_id)
				feature_lang_def = get_row_from_list_by_field(feature_langs, 'id_lang', self._notice['src']['language_default'])
				if not feature_lang_def:
					feature_lang_def = feature_langs[0]
				option_data['option_name'] = feature_lang_def['name']
				for feature_lang in feature_langs:
					option_language_data = self.construct_product_option_lang()
					option_language_data['option_name'] = feature_lang['name']
					language_id = feature_lang['id_lang']
					option_data['option_languages'][language_id] = option_language_data

				product_feature_value = get_list_from_list_by_field(product_feature, 'id_feature', feature_id)
				feature_value_ids = duplicate_field_value_from_list(product_feature_value, 'id_feature_value')
				for feature_value_id in feature_value_ids:
					option_value_data = self.construct_product_option_value()
					option_value_data['id'] = feature_value_id
					feature_value_langs = get_list_from_list_by_field(products_ext['data']['feature_value_lang'], 'id_feature_value', feature_value_id)
					feature_value_lang_def = get_row_from_list_by_field(feature_value_langs, 'id_lang', self._notice['src']['language_default'])
					if not feature_value_lang_def:
						feature_value_lang_def = feature_value_langs[0]
					option_value_data['option_value_name'] = feature_value_lang_def['value']

					for feature_value_lang in feature_value_langs:
						option_value_language_data = self.construct_product_option_value_lang()
						option_value_language_data['option_value_name'] = feature_value_lang['value']
						language_id = feature_value_lang['id_lang']
						option_value_data['option_value_languages'][language_id] = option_value_language_data
					option_data['values'].append(option_value_data)
				product_data['options'].append(option_data)

		childen_products = get_list_from_list_by_field(products_ext['data']['product_attribute'], 'id_product', product['id_product'])
		if childen_products:
			product_data['type'] = self.PRODUCT_CONFIG
			for childen_product in childen_products:
				childen_options = get_list_from_list_by_field(products_ext['data']['product_attribute_combination'], 'id_product_attribute', childen_product['id_product_attribute'])
				childen_name = product_description_def['name']
				for childen_option_name in childen_options:
					attribute_lang = get_list_from_list_by_field(products_ext['data']['attribute_lang'], 'id_attribute', childen_option_name['id_attribute'])
					option_value_name = get_row_value_from_list_by_field(attribute_lang, 'id_lang', self._notice['src']['language_default'], 'name')
					childen_name = to_str(childen_name) + ' - ' + to_str(option_value_name)
				childen_data = self.construct_product_child()
				childen_data['id'] = childen_product['id_product_attribute']
				childen_data['name'] = childen_name
				childen_data['status'] = True
				childen_data['sku'] = childen_product['reference'] if childen_product['reference'] else product['reference']
				childen_data['price'] = to_decimal(product['price']) + to_decimal(childen_product['price'])
				childen_data['weight'] = to_decimal(product['weight']) + to_decimal(childen_product['weight'])
				childen_data['qty'] = childen_product['quantity']
				childen_data['created_at'] = ''
				childen_data['update_at'] = ''

				for product_description in product_descriptions:
					childen_language_data = {}
					childen_language_data['name'] = childen_name
					id_lang = product_description['id_lang']
					childen_data['languages'][id_lang] = childen_language_data

				for childen_option in childen_options:
					childen_product_option_data = self.construct_product_child_attribute()
					attribute_group_langs = get_list_from_list_by_field(products_ext['data']['attribute_group_lang'], 'id_attribute_group', childen_option['id_attribute_group'])
					attribute_group_lang_def = get_row_from_list_by_field(attribute_group_langs, 'id_lang', self._notice['src']['language_default'])
					childen_product_option_data['option_id'] = childen_option['id_attribute_group']
					childen_product_option_data['option_type'] = 'select'
					childen_product_option_data['option_code'] = to_str(attribute_group_lang_def.get('name')).lower()
					childen_product_option_data['option_name'] = attribute_group_lang_def.get('name')
					for attribute_group_lang in attribute_group_langs:
						product_attribute_lang_data = self.construct_product_option_lang()
						product_attribute_lang_data['option_name'] = attribute_group_lang['name']
						language_id = attribute_group_lang['id_lang']
						childen_product_option_data['option_languages'][language_id] = product_attribute_lang_data

					attribute_langs = get_list_from_list_by_field(products_ext['data']['attribute_lang'], 'id_attribute', childen_option['id_attribute'])
					attribue_lang_def = get_row_from_list_by_field(attribute_langs, 'id_lang', self._notice['src']['language_default'])
					childen_product_option_data['option_value_id'] = childen_option['id_attribute']
					childen_product_option_data['option_value_code'] = to_str(attribue_lang_def.get('name')).lower()
					childen_product_option_data['option_value_name'] = attribue_lang_def.get('name')
					for attribute_lang in attribute_langs:
						product_attribute_value_lang_data = self.construct_product_option_value_lang()
						product_attribute_value_lang_data['option_value_name'] = attribute_lang['name']
						language_id = attribute_lang['id_lang']
						childen_product_option_data['option_value_languages'][language_id] = product_attribute_value_lang_data
					childen_data['attributes'].append(childen_product_option_data)
				product_data['children'].append(childen_data)
		# related
		parent = get_list_from_list_by_field(products_ext['data']['accessory'], 'id_product_1', product_data['id'])
		childrend = get_list_from_list_by_field(products_ext['data']['accessory'], 'id_product_2', product_data['id'])
		if parent:
			for row in parent:
				key = self.PRODUCT_RELATE,
				relation = self.construct_product_relation()
				relation['id'] = row['id_product_2']
				relation['type'] = key
				product_data['relate']['children'].append(relation)
		if childrend:
			for row in childrend:
				key = self.PRODUCT_RELATE,
				relation = self.construct_product_relation()
				relation['id'] = row['id_product_1']
				relation['type'] = key
				product_data['relate']['parent'].append(relation)
		product_tags = get_list_from_list_by_field(products_ext['data']['product_tag'], 'id_product', product['id_product'])
		if product_tags:
			tags = list()
			for product_tag in product_tags:
				tag = get_row_from_list_by_field(products_ext['data']['tags'], 'id_tag', product_tag['id_tag'])
				if tag:
					tags.append(tag['name'])
			product_data['tags'] = (','.join(tags))

		# if self._notice['config']['seo']:
		detect_seo = self.detect_seo()
		product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)
		return response_success(product_data)

	def get_product_id_import(self, convert, product, products_ext):
		return product['id_product']

	def check_product_import(self, convert, product, products_ext):
		return True if self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id']) else False

	def router_product_import(self, convert, product, products_ext):
		return response_success('product_import')

	def before_product_import(self, convert, product, products_ext):
		return response_success()

	def product_import(self, convert, product, products_ext):
		return response_success(1)

	def after_product_import(self, product_id, convert, product, products_ext):
		return response_success()

	def addition_product_import(self, convert, product, products_ext):
		return response_success()

	# TODO: CUSTOMER

	def prepare_customers_export(self):
		return self

	def prepare_customers_import(self):
		return self

	def get_customers_main_export(self):
		id_src = self._notice['process']['customers']['id_src']
		limit = self._notice['setting']['customers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_customer WHERE id_customer > " + to_str(id_src) + " ORDER BY id_customer ASC LIMIT " + to_str(limit)
		}
		# customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		customers = self.select_data_connector(query, 'customers')
		if not customers or customers['result'] != 'success':
			return response_error('could not get customers main to export')
		return customers

	def get_customers_ext_export(self, customers):
		customer_ids = duplicate_field_value_from_list(customers['data'], 'id_customer')
		customer_ext_queries = {
			'address': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_address WHERE id_customer IN " + self.list_to_in_condition(customer_ids)
			},
			'group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group WHERE id_customer IN " + self.list_to_in_condition(customer_ids)
			}
		}
		# customers_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(customer_ext_queries),
		# })
		customers_ext = self.select_multiple_data_connector(customer_ext_queries, 'customers')
		if not customers_ext or customers_ext['result'] != 'success':
			return response_error('err customers ext data')

		country_ids = duplicate_field_value_from_list(customers_ext['data']['address'], 'id_country')
		state_ids = duplicate_field_value_from_list(customers_ext['data']['address'], 'id_state')
		customers_ext_rel_queries = {
			'country': {
				'type': 'select',
				'query': "SELECT c.*, cl.* FROM _DBPRF_country AS c LEFT JOIN _DBPRF_country_lang AS cl ON cl.id_country = c.id_country WHERE c.id_country IN " + self.list_to_in_condition(country_ids) + " AND cl.id_lang = " + self._notice['src']['language_default']
			},
			'state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_state WHERE id_state IN " + self.list_to_in_condition(state_ids)
			},

		}
		# customers_ext_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(customers_ext_rel_queries),
		# })
		customers_ext_rel = self.select_multiple_data_connector(customers_ext_rel_queries, 'customers')
		if not customers_ext_rel or customers_ext_rel['result'] != 'success':
			return response_error('err customers ext rel data')
		customers_ext = self.sync_connector_object(customers_ext, customers_ext_rel)
		return customers_ext

	def convert_customer_export(self, customer, customers_ext):
		customer_data = self.construct_customer()
		customer_data['id'] = customer['id_customer']
		customer_data['username'] = customer['email']
		customer_data['email'] = customer['email']
		customer_data['password'] = customer['passwd'] + ":" + self._notice['src']['config']['cookie_key']
		customer_data['first_name'] = customer['firstname']
		customer_data['last_name'] = customer['lastname']
		customer_data['gender'] = customer['id_gender']
		customer_data['dob'] = convert_format_time(customer['birthday'])
		customer_data['is_subscribed'] = True if to_int(customer['newsletter']) == 1 else False
		customer_data['active'] = True if to_int(customer['active']) == 1 else False
		customer_data['created_at'] = convert_format_time(customer['date_add'])
		customer_data['updated_at'] = convert_format_time(customer['date_upd'])
		group_id = get_row_value_from_list_by_field(customers_ext['data']['group'], 'id_customer', customer['id_customer'], 'id_group')
		if group_id:
			customer_data['group_id'] = group_id
		address_books = get_list_from_list_by_field(customers_ext['data']['address'], 'id_customer', customer['id_customer'])
		if address_books:
			for key, address_book in enumerate(address_books):
				address_data = self.construct_customer_address()
				address_data['id'] = address_book['id_address']
				address_data['first_name'] = address_book['firstname']
				address_data['last_name'] = address_book['lastname']
				address_data['address_1'] = address_book['address1']
				address_data['address_2'] = address_book['address2']
				address_data['city'] = address_book['city']
				address_data['postcode'] = address_book['postcode']
				address_data['telephone'] = address_book['phone'] if address_book['phone'] else address_book['phone_mobile']
				address_data['company'] = address_book['company']
				address_data['fax'] = ''

				country = get_row_from_list_by_field(customers_ext['data']['country'], 'id_country', address_book['id_country'])
				if country:
					address_data['country']['id'] = country['id_country']
					address_data['country']['country_code'] = country['iso_code']
					address_data['country']['name'] = country['name']
				else:
					address_data['country']['country_code'] = 'US'
					address_data['country']['name'] = 'United States'

				state = get_row_from_list_by_field(customers_ext['data']['state'], 'id_state', address_book['id_state'])
				if state:
					address_data['state']['id'] = state['id_state']
					address_data['state']['state_code'] = state['iso_code']
					address_data['state']['name'] = state['name']
				else:
					address_data['state']['state_code'] = 'AL'
					address_data['state']['name'] = 'Alabama'

				if key == 0:
					address_data['default']['shipping'] = True
					address_data['default']['billing'] = True

				customer_data['address'].append(address_data)
		return response_success(customer_data)

	def get_customer_id_import(self, convert, customer, customers_ext):
		return customer['id_customer']

	def check_customer_import(self, convert, customer, customers_ext):
		return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id']) else False

	def router_customer_import(self, convert, customer, customers_ext):
		return response_success('customer_import')

	def before_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def customer_import(self, convert, customer, customers_ext):
		return response_success(1)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
		return response_success()

	def addition_customer_import(self, convert, customer, customers_ext):
		return response_success()

	# TODO: ORDER

	def prepare_orders_export(self):
		return self

	def prepare_orders_import(self):
		return self

	def get_orders_main_export(self):
		id_src = self._notice['process']['orders']['id_src']
		limit = self._notice['setting']['orders']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_orders WHERE id_order > " + to_str(id_src) + " ORDER BY id_order ASC LIMIT " + to_str(limit)
		}
		# orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		orders = self.select_data_connector(query, 'orders')
		if not orders or orders['result'] != 'success':
			return response_error('could not get orders main to export')
		return orders

	def get_orders_ext_export(self, orders):
		order_ids = duplicate_field_value_from_list(orders['data'], 'id_order')
		delivery_address_ids = duplicate_field_value_from_list(orders['data'], 'id_address_delivery')
		invoice_address_ids = duplicate_field_value_from_list(orders['data'], 'id_address_invoice')
		address_ids = list(set(delivery_address_ids + invoice_address_ids))
		currency_ids = duplicate_field_value_from_list(orders['data'], 'id_currency')
		customer_ids = duplicate_field_value_from_list(orders['data'], 'id_customer')
		orders_ext_queries = {
			'order_detail': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_order_detail WHERE id_order IN " + self.list_to_in_condition(order_ids)
			},
			'order_history': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_order_history WHERE id_order IN " + self.list_to_in_condition(order_ids)
			},
			'address': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_address WHERE id_address IN " + self.list_to_in_condition(address_ids)
			},
			'currency': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_currency WHERE id_currency IN " + self.list_to_in_condition(currency_ids)
			},
			'customer': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer WHERE id_customer IN " + self.list_to_in_condition(customer_ids)
			},
			'message': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_message WHERE id_order IN " + self.list_to_in_condition(order_ids)
			}
		}
		# orders_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(orders_ext_queries),
		# })
		orders_ext = self.select_multiple_data_connector(orders_ext_queries, 'orders')
		if not orders_ext or orders_ext['result'] != 'success':
			return response_error('err orders ext data')

		country_ids = duplicate_field_value_from_list(orders_ext['data']['address'], 'id_country')
		state_ids = duplicate_field_value_from_list(orders_ext['data']['address'], 'id_state')
		orders_ext_rel_queries = {
			'country': {
				'type': 'select',
				'query': "SELECT c.*, cl.* FROM _DBPRF_country AS c LEFT JOIN _DBPRF_country_lang AS cl ON cl.id_country = c.id_country WHERE c.id_country IN " + self.list_to_in_condition(country_ids)
			},
			'state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_state WHERE id_state IN " + self.list_to_in_condition(state_ids)
			},
		}
		# orders_ext_rel = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(orders_ext_rel_queries),
		# })
		orders_ext_rel = self.select_multiple_data_connector(orders_ext_rel_queries, 'orders')
		if not orders_ext_rel or orders_ext_rel['result'] != 'success':
			return response_error('err orders ext data')
		orders_ext = self.sync_connector_object(orders_ext, orders_ext_rel)

		return orders_ext

	def convert_order_export(self, order, orders_ext):
		order_data = self.construct_order()
		order_status = get_list_from_list_by_field(orders_ext['data']['order_history'], 'id_order', order['id_order'])
		order_status_id = order_status[0]['id_order_state']
		order_data['id'] = order['id_order']
		order_data['status'] = order_status_id
		order_data['tax']['title'] = 'Taxes'
		order_data['tax']['amount'] = to_decimal(order['total_paid']) - to_decimal(order['total_products']) - to_decimal(order['total_shipping']) + to_decimal(order['total_discounts'])
		order_data['shipping']['title'] = 'Shipping'
		order_data['shipping']['amount'] = order['total_shipping']
		order_data['discount']['title'] = 'Total products'
		order_data['discount']['amount'] = order['total_discounts']
		order_data['subtotal']['title'] = 'Total products'
		order_data['subtotal']['amount'] = order['total_products']
		order_data['total']['title'] = 'Total'
		order_data['total']['amount'] = order['total_paid']

		currency_ps = get_row_from_list_by_field(orders_ext['data']['currency'], 'id_currency', order['id_currency'])
		order_data['currency'] = currency_ps.get('name')
		order_data['created_at'] = convert_format_time(order['date_add'])
		order_data['updated_at'] = convert_format_time(order['date_upd'])

		order_customer = self.construct_order_customer()
		customer_ps = get_row_from_list_by_field(orders_ext['data']['customer'], 'id_customer', order['id_customer'])
		order_customer['id'] = order['id_customer']
		if customer_ps:
			order_customer['email'] = customer_ps['email']
			order_customer['first_name'] = customer_ps['firstname']
			order_customer['last_name'] = customer_ps['lastname']
		order_data['customer'] = order_customer

		customer_address = self.construct_order_address()
		order_data['customer_address'] = customer_address

		billing_address = get_row_from_list_by_field(orders_ext['data']['address'], 'id_address', order['id_address_invoice'])
		order_billing = self.construct_order_address()
		if billing_address:
			order_billing['id'] = order['id_address_invoice']
			order_billing['first_name'] = billing_address['firstname']
			order_billing['last_name'] = billing_address['lastname']
			order_billing['address_1'] = billing_address['address1']
			order_billing['address_2'] = billing_address['address2']
			order_billing['city'] = billing_address['city']
			order_billing['postcode'] = billing_address['postcode']
			order_billing['telephone'] = billing_address['phone'] if billing_address['phone'] else billing_address['phone_mobile']
			order_billing['company'] = billing_address['company']
			billing_country = get_row_from_list_by_field(orders_ext['data']['country'], 'id_country', billing_address['id_country'])
			if billing_country:
				order_billing['country']['id'] = billing_country['id_country']
				order_billing['country']['country_code'] = billing_country['iso_code']
				order_billing['country']['name'] = billing_country['name']
			billing_state = get_row_from_list_by_field(orders_ext['data']['state'], 'id_state', billing_address['id_state'])
			if billing_state:
				order_billing['state']['id'] = billing_state['id_state']
				order_billing['state']['state_code'] = billing_state['iso_code']
				order_billing['state']['name'] = billing_state['name']
		order_data['billing_address'] = order_billing
		delivery_address = get_row_from_list_by_field(orders_ext['data']['address'], 'id_address', order['id_address_delivery'])
		order_delivery = self.construct_order_address()
		if delivery_address:
			order_delivery['id'] = order['id_address_delivery']
			order_delivery['first_name'] = delivery_address['firstname']
			order_delivery['last_name'] = delivery_address['lastname']
			order_delivery['address_1'] = delivery_address['address1']
			order_delivery['address_2'] = delivery_address['address2']
			order_delivery['city'] = delivery_address['city']
			order_delivery['postcode'] = delivery_address['postcode']
			order_delivery['telephone'] = delivery_address['phone']
			order_delivery['company'] = delivery_address['company']
			delivery_country = get_row_from_list_by_field(orders_ext['data']['country'], 'id_country', delivery_address['id_country'])
			if delivery_country:
				order_delivery['country']['id'] = delivery_country['id_country']
				order_delivery['country']['country_code'] = delivery_country['iso_code']
				order_delivery['country']['name'] = delivery_country['name']
			delivery_state = get_row_from_list_by_field(orders_ext['data']['state'], 'id_state', delivery_address['id_state'])
			if delivery_state:
				order_delivery['state']['id'] = delivery_state['id_state']
				order_delivery['state']['state_code'] = delivery_state['iso_code']
				order_delivery['state']['name'] = delivery_state['name']
		order_data['shipping_address'] = order_delivery
		order_payment = self.construct_order_payment()
		order_payment['title'] = order['payment']
		order_data['payment'] = order_payment

		order_products = get_list_from_list_by_field(orders_ext['data']['order_detail'], 'id_order', order['id_order'])
		order_items = list()
		for order_product in order_products:
			order_item = self.construct_order_item()
			product_name = to_str(order_product['product_name']).split(" - ")
			order_item['id'] = order_product['id_order_detail']
			order_item['product']['id'] = order_product['product_id']
			order_item['product']['name'] = product_name[0] if product_name[0] else ''
			order_item['product']['sku'] = order_product['product_reference']
			order_item['qty'] = order_product['product_quantity']
			order_item['price'] = to_decimal(order_product['product_price'])
			order_item['original_price'] = to_decimal(order_product['product_price'])
			order_item['tax_amount'] = (to_decimal(order_product['product_price']) * to_decimal(order_product['product_quantity']) * to_decimal(order_product['tax_rate'])) / 100
			order_item['tax_percent'] = ''
			order_item['discount_amount'] = '0.0000'
			order_item['discount_percent'] = '0.0000'
			order_item['subtotal'] = to_decimal(order_product['product_price']) * to_decimal(order_product['product_quantity'])
			order_item['total'] = to_decimal(order_item['subtotal']) + to_decimal(order_item['tax_amount'])
			if product_name and to_len(product_name) > 1:
				order_item_options = list()
				order_product_attributes = to_str(product_name[1]).split(",")
				for order_product_attribute in order_product_attributes:
					order_item_option = self.construct_order_item_option()
					option_prod = to_str(order_product_attribute).split(":")
					order_item_option['option_name'] = option_prod[0] if option_prod and to_len(option_prod) > 0 else ''
					order_item_option['option_value_name'] = option_prod[1] if option_prod and to_len(option_prod) > 1 else ''
					order_item_option['price'] = ''
					order_item_option['price_prefix'] = ''
					order_item_options.append(order_item_option)
				order_item['options'] = order_item_options
			order_items.append(order_item)
		order_data['items'] = order_items

		message_ps = get_list_from_list_by_field(orders_ext['data']['message'], 'id_order', order['id_order'])
		if message_ps:
			for message in message_ps:
				order_history = self.construct_order_history()
				order_history['id'] = message['id_message']
				order_history['comment'] = message['message']
				order_history['created_at'] = convert_format_time(message['date_add'])
				order_data['history'].append(order_history)

		return response_success(order_data)

	def get_order_id_import(self, convert, order, orders_ext):
		return order['id_order']

	def check_order_import(self, convert, order, orders_ext):
		return True if self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code']) else False

	def router_order_import(self, convert, order, orders_ext):
		return response_success('order_import')

	def before_order_import(self, convert, order, orders_ext):
		return response_success()

	def order_import(self, convert, order, orders_ext):
		return response_success(1)

	def after_order_import(self, order_id, convert, order, orders_ext):
		return response_success()

	def addition_order_import(self, convert, order, orders_ext):
		return response_success()

	# TODO: REVIEW

	def prepare_reviews_export(self):
		return self

	def prepare_reviews_import(self):
		return self

	def get_reviews_main_export(self):
		id_src = self._notice['process']['reviews']['id_src']
		limit = self._notice['setting']['reviews']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_product_reviews WHERE review_id > " + to_str(
				id_src) + " ORDER BY review_id ASC LIMIT " + to_str(limit)
		}
		# reviews = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		reviews = self.select_data_connector(query, 'reviews')
		if not reviews or reviews['result'] != 'success':
			return response_error('could not get reviews main to export')
		return reviews

	def get_reviews_ext_export(self, reviews):
		return response_success(list())

	def convert_review_export(self, review, reviews_ext):
		review_data = self.construct_review()
		review_data['id'] = review['review_id']

		review_data['language_id'] = self._notice['src']['language_default']
		review_data['product']['id'] = review['productid']
		review_data['product']['name'] = ''
		review_data['customer']['id'] = ''
		review_data['customer']['name'] = review['email']
		review_data['title'] = ''
		review_data['content'] = review['message']
		review_data['status'] = 1
		review_data['created_at'] = get_current_time()
		review_data['updated_at'] = get_current_time()

		rating = self.construct_review_rating()
		rating['rate_code'] = 'default'
		rating['rate'] = 5
		review_data['rating'].append(rating)

		return response_success(review_data)

	def get_review_id_import(self, convert, review, reviews_ext):
		return review['review_id']

	def check_review_import(self, convert, review, reviews_ext):
		return True if self.get_map_field_by_src(self.TYPE_REVIEW, convert['id'], convert['code']) else False

	def router_review_import(self, convert, review, reviews_ext):
		return response_success('review_import')

	def before_review_import(self, convert, review, reviews_ext):
		return response_success()

	def review_import(self, convert, review, reviews_ext):
		return response_success(1)

	def after_review_import(self, review_id, convert, review, reviews_ext):
		return response_success()

	def addition_review_import(self, convert, review, reviews_ext):
		return response_success()

	# TODO: PAGE

	def prepare_pages_import(self):
		return self

	def prepare_pages_export(self):
		return self

	def get_pages_main_export(self):
		id_src = self._notice['process']['pages']['id_src']
		limit = self._notice['setting']['pages']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_cms WHERE id_cms > " + to_str(id_src) + " ORDER BY id_cms ASC LIMIT " + to_str(limit),
		}
		pages = self.select_data_connector(query, 'pages')
		if not pages or pages['result'] != 'success':
			return response_error()
		return pages

	def get_pages_ext_export(self, pages):
		page_ids = duplicate_field_value_from_list(pages['data'], 'id_cms')
		page_id_con = self.list_to_in_condition(page_ids)
		page_ext_queries = {
			'cms_lang': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_cms_lang WHERE id_cms IN " + page_id_con
			},
		}
		pages_ext = self.select_multiple_data_connector(page_ext_queries, 'products')
		if (not pages_ext) or pages_ext['result'] != 'success':
			return response_error()
		return pages_ext

	def convert_page_export(self, page, pages_ext):
		page_data = self.construct_cms_page()
		page_data['id'] = page['id_cms']
		page_data['status'] = True
		cms_lang = get_list_from_list_by_field(pages_ext['data']['cms_lang'], 'id_cms', page['id_cms'])
		cms_lang_def = get_row_from_list_by_field(cms_lang, 'id_lang', self._notice['src']['language_default'])
		if not cms_lang_def and cms_lang:
			cms_lang_def = cms_lang[0]
		page_data['title'] = cms_lang_def['meta_title']
		page_data['meta_title'] = cms_lang_def['meta_title']
		page_data['content'] = cms_lang_def['content']
		page_data['meta_keywords'] = cms_lang_def['meta_keywords']
		page_data['meta_description'] = cms_lang_def['meta_description']
		page_data['url_key'] = cms_lang_def['link_rewrite']
		for cms_lang_lang in cms_lang:
			cms_lang_data = dict()
			cms_lang_data['title'] = cms_lang_lang['meta_title']
			cms_lang_data['meta_title'] = cms_lang_lang['meta_title']
			cms_lang_data['content'] = cms_lang_lang['content']
			cms_lang_data['meta_keywords'] = cms_lang_lang['meta_keywords']
			cms_lang_data['meta_description'] = cms_lang_lang['meta_description']
			cms_lang_data['url_key'] = cms_lang_lang['link_rewrite']
			page_data['languages'][cms_lang_lang['id_lang']] = cms_lang_data
		return response_success(page_data)

	def get_page_id_import(self, convert, page, pages_ext):
		return page['id_cms']

	def check_page_import(self, convert, page, pages_ext):
		return False

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

	# TODO: BLOCK

	def prepare_blogs_import(self):
		return response_success()

	def prepare_blogs_export(self):
		return self

	def get_blogs_main_export(self):
		return self

	def get_blogs_ext_export(self, blocks):
		return response_success()

	def convert_blog_export(self, block, blocks_ext):
		return response_success()

	def get_blog_id_import(self, convert, block, blocks_ext):
		return False

	def check_blog_import(self, convert, block, blocks_ext):
		return False

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

	# TODO: Coupon
	def prepare_coupons_import(self):
		return response_success()

	def prepare_coupons_export(self):
		return self

	def get_coupons_main_export(self):
		id_src = self._notice['process']['coupons']['id_src']
		limit = self._notice['setting']['coupons']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_discount WHERE id_discount > " + to_str(id_src) + " ORDER BY id_discount ASC LIMIT " + to_str(limit),
		}
		coupons = self.select_data_connector(query, 'coupons')
		if not coupons or coupons['result'] != 'success':
			return response_error()
		return coupons

	def get_coupons_ext_export(self, coupons):
		coupon_ids = duplicate_field_value_from_list(coupons['data'], 'id_discount')
		coupon_id_con = self.list_to_in_condition(coupon_ids)
		coupon_ext_queries = {
			'discount_lang': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_discount_lang WHERE id_discount IN " + coupon_id_con
			},
			'discount_category': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_discount_category WHERE id_discount IN " + coupon_id_con
			},
		}
		coupons_ext = self.select_multiple_data_connector(coupon_ext_queries, 'products')
		if (not coupons_ext) or coupons_ext['result'] != 'success':
			return response_error()
		return coupons_ext

	def convert_coupon_export(self, coupon, coupons_ext):
		coupon_data = self.construct_coupon()
		coupon_data['id'] = coupon['id_discount']
		coupon_data['code'] = coupon['name']
		coupon_data['title'] = coupon['name']
		discount_lang = get_list_from_list_by_field(coupons_ext['data']['discount_lang'], 'id_discount', coupon['id_discount'])
		coupon_data['description'] = get_row_value_from_list_by_field(discount_lang, 'id_lang', self._notice['src']['language_default'], 'description')
		coupon_data['status'] = True if to_int(coupon['active']) == 1 else False
		coupon_data['from_date'] = convert_format_time(coupon['date_from'])
		coupon_data['to_date'] = convert_format_time(coupon['date_to'])
		coupon_data['min_spend'] = coupon['minimal']
		coupon_data['times_used'] = coupon['quantity']
		coupon_data['usage_limit'] = coupon['quantity']
		coupon_data['discount_amount'] = coupon['value']
		coupon_data['usage_per_customer'] = coupon['quantity_per_user']
		coupon_data['type'] = self.PERCENT if to_int(to_decimal(coupon['id_discount_type'])) == 1 else self.FIXED
		coupon_data['coupon_type'] = 2 if coupon['name'] else 1
		coupon_data['customer_group'].append(coupon['id_group'])
		category_ids = get_list_from_list_by_field(coupons_ext['data']['discount_category'], 'id_discount', coupon['id_discount'])
		if category_ids:
			for category_id in category_ids:
				coupon_data['categories'].append(category_id['id_category'])

		return response_success(coupon_data)

	def get_coupon_id_import(self, convert, coupon, coupons_ext):
		return coupon['id_discount']

	def check_coupon_import(self, convert, coupon, coupons_ext):
		return True if self.get_map_field_by_src(self.TYPE_COUPON, convert['id'], convert['code']) else False

	def router_coupon_import(self, convert, coupon, coupons_ext):
		return response_success('coupon_import')

	def before_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	def coupon_import(self, convert, coupon, coupons_ext):
		coupon_data = {
			'id_customer': 0,
			'date_from': convert['from_date'] if convert['from_date'] else get_current_time(),
			'date_to': convert['to_date'] if convert['to_date'] else get_current_time(),
			'description': convert['description'],
			'quantity': convert['usage_limit'],
			'quantity_per_user': convert['usage_per_customer'],
			'priority': 1,
			'partial_use': 1,
			'code': convert['code'] if convert['code'] else convert['id'],
			'minimum_amount': convert['min_spend'],
			'minimum_amount_currency': 1,
			'reduction_percent': convert['discount_amount'] if convert['type'] == self.PERCENT else 0.00,
			'reduction_amount': convert['discount_amount'] if convert['type'] == self.FIXED else 0.00,
			'reduction_currency': 1,
			'active': 1 if convert['active'] else 0,
			'date_add': convert_format_time(convert['created_at']) if convert['created_at'] else get_current_time(),
			'date_upd': convert_format_time(convert['updated_at']) if convert['updated_at'] else get_current_time(),
		}
		coupon_query = self.create_insert_query_connector('cart_rule', coupon_data)
		coupon_import = self.import_data_connector(coupon_query, 'coupons', convert['id'])
		if not coupon_import:
			return response_error()
		self.insert_map(self.TYPE_COUPON, convert['id'], coupon_import, convert['code'])
		return response_success(coupon_import)

	def after_coupon_import(self, coupon_id, convert, coupon, coupons_ext):
		all_queries = list()
		coupon_category_data = {
			'id_cart_rule': coupon_id,
			'id_lang': self._notice['target']['language_default'],
			'name': convert['title']
		}
		all_queries.append(self.create_insert_query_connector("cart_rule_lang", coupon_category_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'coupons')
		return response_success()

	def addition_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	# TODO: CODE PRESTASHOP13

	def get_category_parent(self, category_id):
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_category WHERE id_category = " + to_str(category_id),
		}
		# categories = self.get_connector_data(url_query, {'query': json.dumps(query)})
		categories = self.select_data_connector(query, 'categories')
		if not categories or categories['result'] != 'success':
			return response_warning()
		categories_ext = self.get_categories_ext_export(categories)
		category = categories['data'][0]
		category_convert = self.convert_category_export(category, categories_ext)
		return category_convert

	def _get_image_path(self, id_image, id_product):
		path_img = 'p/' + to_str(id_product) + '-' + to_str(id_image) + '.jpg'
		return path_img

	def detect_seo(self):
		return 'default_seo'

	def categories_default_seo(self, category, categories_ext):
		result = list()
		key_entity = 'id_category'
		cat_desc = get_list_from_list_by_field(categories_ext['data']['category_lang'], key_entity, category['id_category'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			seo_cate = self.construct_seo_category()
			seo_cate['request_path'] = seo['link_rewrite']
			seo_cate['store_id'] = seo['id_lang']
			seo_cate['type'] = type_seo
			result.append(seo_cate)
		return result

	def products_default_seo(self, product, products_ext):
		result = list()
		key_entity = 'id_product'
		prd_desc = get_list_from_list_by_field(products_ext['data']['product_lang'], key_entity, product['id_product'])
		for seo in prd_desc:
			type_seo = self.SEO_DEFAULT
			seo_product = self.construct_seo_product()
			seo_product['request_path'] = seo['link_rewrite']
			seo_product['store_id'] = seo['id_lang']
			seo_product['type'] = type_seo
			result.append(seo_product)
		return result
