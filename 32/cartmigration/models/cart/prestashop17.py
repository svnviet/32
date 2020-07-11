from cartmigration.models.basecart import LeBasecart
from cartmigration.libs.utils import *
import requests

class LeCartPrestashop17(LeBasecart):

	def __init__(self, data = None):
		super().__init__(data)
		self.product_relates = list()

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
			},
			'shops': {
				'type': 'select',
				'query': "SELECT cfg.*, shop.* FROM _DBPRF_shop AS shop LEFT JOIN _DBPRF_configuration AS cfg ON shop.id_shop = cfg.value WHERE cfg.name = 'PS_SHOP_DEFAULT'"
			},
			'root_category': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category WHERE is_root_category = 1"
			},
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
		if default_config_data and default_config_data['shops']:
			self._notice['src']['shop_default'] = default_config_data['shops'][0]['id_shop'] if default_config_data['shops'][0]['id_shop'] else 1
		if default_config_data and default_config_data['root_category']:
			self._notice['src']['category_root'] = default_config_data['root_category'][0]['id_category'] if default_config_data['root_category'][0]['id_category'] else 2
		else:
			self._notice['src']['category_root'] = 2
		self._notice['src']['category_data'] = {
			self._notice['src']['category_root']: 'Default Category',
		}
		self._notice['src']['site'] = {
			1: 'Default Shop',
		}
		self._notice['src']['category_data'] = {
			self._notice['src']['category_root']: 'Default Category',
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
				'query': "SELECT * FROM _DBPRF_currency WHERE active = 1",
			},
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_order_state_lang WHERE id_lang = '" + self._notice['src']['language_default'] + "'",
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_group_lang where id_lang = '" + self._notice['src'][
					'language_default'] + "'"
			},
			'stores': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_shop WHERE active = 1"
			},
			'module': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_module WHERE name LIKE 'productcomments' AND active = 1"
			}
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
		site_data = dict()
		for language_row in config_data['languages']:
			language_data[language_row['id_lang']] = to_str(language_row['name']) + "(" + to_str(language_row['iso_code']) + ")"
		for currency_row in config_data['currencies']:
			currency_data[currency_row['id_currency']] = currency_row['name']
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['id_order_state']] = order_status_row['name']
		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['id_group']] = customer_group['name']
		if config_data and config_data['stores']:
			for store_row in config_data['stores']:
				site_data[to_int(store_row['id_shop'])] = store_row['name']
		else:
			site_data = {
				1: 'Default Shop',
			}
		if config_data['module'] and to_len(config_data['module']) > 0:
			self._notice['src']['support']['reviews'] = True
		else:
			self._notice['src']['support']['reviews'] = False

		self._notice['src']['site'] = site_data
		self._notice['src']['languages_select'] = language_data
		self._notice['src']['languages'] = language_data
		self._notice['src']['store_category'] = storage_cat_data
		self._notice['src']['currencies'] = currency_data
		self._notice['src']['order_status'] = order_status_data
		self._notice['src']['customer_group'] = customer_group_data
		self._notice['src']['support']['country_map'] = False
		self._notice['src']['support']['languages_select'] = True
		self._notice['src']['support']['site_map'] = True
		self._notice['src']['support']['customer_group_map'] = True
		self._notice['src']['support']['order_status_map'] = True
		self._notice['src']['support']['seo_301'] = True
		self._notice['src']['support']['seo'] = True
		self._notice['src']['support']['cus_pass'] = True
		self._notice['src']['support']['coupons'] = True
		self._notice['src']['support']['pages'] = True
		return response_success()

	def display_config_target(self):
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
			},
			'shop_default': {
				'type': 'select',
				'query': "SELECT cfg.* FROM _DBPRF_configuration AS cfg WHERE cfg.name = 'PS_SHOP_DEFAULT'"
			},
			'image_resize': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_image_type WHERE `products` = '1' OR `categories` = '1' OR `manufacturers` = '1'"
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
			self._notice['target']['language_default'] = default_config_data['languages'][0]['id_lang'] if default_config_data['languages'][0]['id_lang'] else 1
			self._notice['target']['currency_default'] = default_config_data['currencies'][0]['id_currency'] if default_config_data['currencies'][0]['id_currency'] else 1
			self._notice['target']['shop_default'] = default_config_data['shop_default'][0]['value'] if default_config_data['shop_default'] else 1
		else:
			self._notice['target']['language_default'] = 1
			self._notice['target']['currency_default'] = 1
			self._notice['target']['shop_default'] = 1

		self._notice['target']['images_resize'] = default_config_data['image_resize']

		self._notice['target']['site'] = {
			1: 'Default Shop',
		}
		self._notice['target']['category_root'] = 1
		self._notice['target']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['target']['attributes'] = {
			1: 'Default Attribute',
		}
		module_check = ['lecmprepass', 'leurlrewrite', 'productcomments', 'iqitreviews', 'revws']
		config_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_lang WHERE active = 1",
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_currency WHERE active = 1",
			},
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_order_state_lang WHERE id_lang = '" + self._notice['target']['language_default'] + "'",
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_group_lang where id_lang = '" + self._notice['target'][
					'language_default'] + "'"
			},
			# 'module_seo': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_module where name = 'leurlrewrite' AND active = 1"
			# },
			'stores': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_shop WHERE active = 1"
			},
			'module': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_module WHERE active = 1 AND name IN " + self.list_to_in_condition(module_check)
			}
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
		site_data = dict()
		for language_row in config_data['languages']:
			language_data[language_row['id_lang']] = to_str(language_row['name']) + "(" + to_str(language_row['iso_code']) + ")"
		for currency_row in config_data['currencies']:
			currency_data[currency_row['id_currency']] = currency_row['name']
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['id_order_state']] = order_status_row['name']
		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['id_group']] = customer_group['name']
		if config_data and config_data['stores']:
			for store_row in config_data['stores']:
				site_data[to_int(store_row['id_shop'])] = store_row['name']
		else:
			site_data = {
				1: 'Default Shop',
			}
		self._notice['target']['support']['reviews'] = False
		self._notice['target']['support']['plugin_seo'] = False
		self._notice['target']['support']['plugin_seo_301'] = False
		if config_data['module'] and to_len(config_data['module']) > 0:
			for module in config_data['module']:
				if module['name'] in ['productcomments', 'iqitreviews', 'revws']:
					if to_str(module['active']) == '1':
						self._notice['target']['support']['reviews'] = True
						self._notice['target']['support'][module['name']] = True
				if module['name'] == 'leurlrewrite':
					self._notice['target']['support']['plugin_seo'] = True
					self._notice['target']['support']['plugin_seo_301'] = True
				if module['name'] == 'lecmprepass':
					self._notice['target']['support']['plugin_cus_pass'] = True
		self._notice['target']['site'] = site_data
		self._notice['target']['customer_group'] = customer_group_data
		self._notice['target']['languages_select'] = language_data
		self._notice['target']['languages'] = language_data
		self._notice['target']['store_category'] = storage_cat_data
		self._notice['target']['currencies'] = currency_data
		self._notice['target']['order_status'] = order_status_data
		self._notice['target']['support']['country_map'] = False
		self._notice['target']['support']['pre_ord'] = True
		self._notice['target']['support']['pre_cus'] = True
		self._notice['target']['support']['img_des'] = True
		self._notice['target']['support']['cus_pass'] = True
		self._notice['target']['support']['check_cus_pass'] = True
		self._notice['target']['support']['languages_select'] = True
		self._notice['target']['support']['site_map'] = True
		self._notice['target']['support']['customer_group_map'] = True
		self._notice['target']['support']['order_status_map'] = True
		self._notice['target']['support']['coupons'] = True
		self._notice['target']['support']['pages'] = True
		self._notice['target']['support']['seo'] = True
		self._notice['target']['support']['check_seo'] = True
		self._notice['target']['support']['seo_301'] = True
		self._notice['target']['support']['check_seo_301'] = True
		#self._notice['target']['support']['reviews'] = True
		self._notice['target']['support']['update_latest_data'] = True
		self._notice['target']['config']['entity_update']['products'] = True
		return response_success()

	def display_confirm_source(self):
		return response_success()

	def display_confirm_target(self):
		self._notice['target']['clear']['function'] = 'clear_target_taxes'
		self._notice['target']['clear_demo']['function'] = 'clear_target_products_demo'
		return response_success()

	def get_query_display_import_source(self, update = False):
		compare_condition = ' > '
		if update:
			compare_condition = ' <= '

		queries = {
			'taxes': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_tax_rules_group WHERE id_tax_rules_group " + compare_condition + to_str(self._notice['process']['taxes']['id_src']),
			},
			'manufacturers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count  FROM _DBPRF_manufacturer WHERE id_manufacturer " + compare_condition + to_str(self._notice['process']['manufacturers']['id_src']),
			},
			'categories': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_category WHERE is_root_category != 1 AND id_category > 1 AND id_category " + compare_condition + to_str(self._notice['process']['categories']['id_src']),
			},
			'products': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_product WHERE id_product " + compare_condition + to_str(self._notice['process']['products']['id_src']),
			},
			'customers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_customer WHERE deleted = 0 AND id_customer " + compare_condition + to_str(self._notice['process']['customers']['id_src']),
			},
			'orders': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_orders WHERE id_order " + compare_condition + to_str(self._notice['process']['orders']['id_src']),
			},
			# 'reviews': {
			# 	'type': 'select',
			# 	'query': "SELECT COUNT(1) AS count FROM _DBPRF_product_comment WHERE id_product_comment " + compare_condition + to_str(self._notice['process']['reviews']['id_src']),
			# },
			'coupons': {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_cart_rule WHERE id_cart_rule ' + compare_condition + to_str(self._notice['process']['coupons']['id_src']),
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

	def prepare_import_source(self):
		return response_success()

	def prepare_import_target(self):
		return response_success()

	# TODO: CLEAR

	def clear_target_taxes(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_manufacturers',
		}
		if not self._notice['config']['taxes']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'tax',
			'tax_lang',
			'tax_rule',
			'tax_rules_group',
			'tax_rules_group_shop',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_manufacturers(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_categories',
		}
		if not self._notice['config']['manufacturers']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'manufacturer',
			'manufacturer_lang',
			'manufacturer_shop',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_categories(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_products',
		}
		if not self._notice['config']['categories']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'category',
			'category_group',
			'category_lang',
			'category_shop',
			'category_product',
			'lecm_rewrite'
		]
		queries = {
			'root': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_configuration WHERE name = 'PS_ROOT_CATEGORY'"
			},
			'home': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_configuration WHERE name = 'PS_HOME_CATEGORY'"
			}
		}
		config = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries)
		})
		id_home = config['data']['home'][0]['value']
		id_root = config['data']['root'][0]['value']
		for table in tables:
			where = " WHERE id_category NOT IN (" + to_str(id_root) + "," + to_str(id_home) + ") "
			if table == 'lecm_rewrite':
				where = " WHERE type = 'category'"
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "` " + where
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_products(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_customers',
		}
		if not self._notice['config']['products']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_ATTR
		}
		attrs = self.select_obj(TABLE_MAP, where)
		attr_ids = list()
		if attrs['result'] == 'success':
			attr_ids = duplicate_field_value_from_list(attrs['data'], 'id_desc')
		tables = [
			'accessory',
			'product',
			'product_shop',
			'product_lang',
			'product_tag',
			'product_carrier',
			'product_attribute',
			'product_attribute_shop',
			'product_attribute_combination',
			'product_attribute_image',
			'feature',
			'feature_lang',
			'feature_shop',
			'feature_product',
			'feature_value',
			'feature_value_lang',
			'category_product',
			'tag',
			'image',
			'image_lang',
			'image_shop',
			'specific_price',
			'specific_price_priority',
			'specific_price_rule',
			'specific_price_rule_condition',
			'specific_price_rule_condition_group',
			'cart_product',
			'product_attachment',
			'product_country_tax',
			'product_download',
			'product_group_reduction_cache',
			'product_sale',
			'product_supplier',
			'warehouse_product_location',
			'stock',
			'stock_available',
			'stock_mvt',
			'customization',
			'customization_field',
			'customization_field_lang',
			'supply_order_detail',
			'attribute_impact',
			'attribute',
			'attribute_lang',
			'attribute_group',
			'attribute_group_lang',
			'attribute_group_shop',
			'attribute_shop',
			'customized_data',
			'pack',
			'search_index',
			'search_word',
			'layered_product_attribute',
			'lecm_rewrite'
		]

		for table in tables:
			where = ''
			if table == 'lecm_rewrite':
				where = " WHERE type = 'product'"
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		table_attrs = [
			'feature',
			'feature_lang',
			'feature_shop',
		]
		attr_id_con = self.list_to_in_condition(attr_ids)
		where = ' WHERE id_feature NOT IN ' + attr_id_con
		for table_attr in table_attrs:
			clear_table_attr = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table_attr + "`" + where
				})
			})
			if not clear_table_attr or clear_table_attr['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table_attr, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_customers(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_orders',
		}
		if not self._notice['config']['customers']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'customer',
			'customer_group',
			'customer_message',
			'customer_message_sync_imap',
			'customer_thread',
			'message',
			'message_readed',
			'address'
		]

		for table in tables:
			where = " WHERE id_customer > 0"
			if table == 'customer_message' or table == 'customer_message_sync_imap' or table == 'message_readed':
				clear_table = self.get_connector_data(self.get_connector_url('query'), {
					'query': json.dumps({
						'type': 'query',
						'query': "DELETE FROM `_DBPRF_" + table + "` "
					})
				})
			else:
				clear_table = self.get_connector_data(self.get_connector_url('query'), {
					'query': json.dumps({
						'type': 'query',
						'query': "DELETE FROM `_DBPRF_" + table + "` " + where
					})
				})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_orders(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_reviews',
		}
		if not self._notice['config']['orders']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'orders',
			'order_carrier',
			'order_cart_rule',
			'order_detail',
			'order_detail_tax',
			'order_history',
			'order_invoice',
			'order_invoice_payment',
			'order_invoice_tax',
			'order_payment',
			'order_return',
			'order_return_detail',
			'order_slip',
			'order_slip_detail',
		]

		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "` "
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_reviews(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_pages',
		}
		if not self._notice['config']['reviews']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'product_comment',
			'product_comment_grade',
		]

		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "` "
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_pages(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_coupons',
		}
		if not self._notice['config']['pages']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'cms',
			'cms_lang',
		]

		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "` "
				})
			})
			if not clear_table or clear_table['result'] != 'success':
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_coupons(self):
		next_clear = {
			'result': 'success',
			'function': 'clear_target_coupons',
			'msg': ''
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['coupons']:
			return next_clear
		tables = [
			'cart_rule',
			'cart_rule_lang',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		return next_clear

	# TODO: CLEAR DEMO

	def clear_target_taxes_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_manufacturers_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['taxes']:
			return self._notice['target']['clear_demo']
		tax_ids = list()
		geo_zone_ids = list()
		rate_ids = list()
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_TAX
		}
		taxes = self.select_obj(TABLE_MAP, where)
		if taxes['result'] == 'success':
			tax_ids = duplicate_field_value_from_list(taxes['data'], 'id_desc')

		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_TAX_ZONE
		}
		taxes_geozone = self.select_obj(TABLE_MAP, where)
		if taxes_geozone['result'] == 'success':
			geo_zone_ids = duplicate_field_value_from_list(taxes_geozone['data'], 'id_desc')

		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_TAX_ZONE_RATE
		}
		taxes_rate = self.select_obj(TABLE_MAP, where)
		if taxes_geozone['result'] == 'success':
			rate_ids = duplicate_field_value_from_list(taxes_rate['data'], 'id_desc')

		if not tax_ids:
			return next_clear
		tax_id_con = self.list_to_in_condition(tax_ids)
		tables = [
			'tax',
			'tax_lang',
			'tax_rule',
			'tax_rules_group',
			'tax_rules_group_shop',
		]

		for table in tables:
			where = ''
			if table == 'tax_rules_group' or table == 'tax_rules_group_shop':
				where = ' WHERE id_tax_rules_group IN ' + tax_id_con
			if table == 'tax_rule':
				where = ' WHERE id_tax_rule+ IN ' + self.list_to_in_condition(geo_zone_ids)
			if table == 'tax' or table == 'tax_lang':
				where = ' WHERE id_tax IN ' + self.list_to_in_condition(rate_ids)
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_manufacturers_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_categories_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['manufacturers']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_MANUFACTURER
		}
		manufacturers = self.select_obj(TABLE_MAP, where)
		manufacturer_ids = list()
		if manufacturers['result'] == 'success':
			manufacturer_ids = duplicate_field_value_from_list(manufacturers['data'], 'id_desc')

		if not manufacturer_ids:
			return next_clear
		manufacturer_id_con = self.list_to_in_condition(manufacturer_ids)
		tables = [
			'manufacturer',
			'manufacturer_lang',
			'manufacturer_shop',
		]

		for table in tables:
			where = ' WHERE id_manufacturer IN ' + manufacturer_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue

		return self._notice['target']['clear_demo']

	def clear_target_categories_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_products_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['categories']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_CATEGORY
		}
		categories = self.select_obj(TABLE_MAP, where)
		category_ids = list()
		if categories['result'] == 'success':
			category_ids = duplicate_field_value_from_list(categories['data'], 'id_desc')

		if not category_ids:
			return next_clear
		category_id_con = self.list_to_in_condition(category_ids)
		tables = [
			'category',
			'category_group',
			'category_lang',
			'category_shop',
			'lecm_rewrite'
		]

		for table in tables:
			where = " WHERE id_category IN " + category_id_con
			if table == 'lecm_rewrite':
				where = " WHERE type = 'category' AND id_desc IN " + category_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
			self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_products_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_orders_demo',
		}
		if not self._notice['config']['products']:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_PRODUCT
		}
		products = self.select_page(TABLE_MAP, where, self.LIMIT_CLEAR_DEMO)
		product_ids = list()
		if products['result'] == 'success':
			product_ids = duplicate_field_value_from_list(products['data'], 'id_desc')
		if not product_ids:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		product_id_con = self.list_to_in_condition(product_ids)
		tables = [
			'product',
			'product_shop',
			'product_lang',
			'stock_available',
			'product_carrier',
			'category_product',
			'image',
			'image_shop',
			'specific_price',
			'feature_product',
			'product_attribute',
			'product_attribute_shop',
			'layered_product_attribute',
		]

		for table in tables:
			where = ' WHERE id_product IN ' + product_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self.delete_map_demo(self.TYPE_PRODUCT, product_ids)
		if product_ids and to_len(product_ids) < self.LIMIT_CLEAR_DEMO:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		return self._notice['target']['clear_demo']

	def clear_target_customers_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_orders_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['customers']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_CUSTOMER
		}
		customers = self.select_obj(TABLE_MAP, where)
		customer_ids = list()
		if customers['result'] == 'success':
			customer_ids = duplicate_field_value_from_list(customers['data'], 'id_desc')
		if not customer_ids:
			return next_clear
		customer_id_con = self.list_to_in_condition(customer_ids)
		tables = [
			'customer',
			'customer_group',
			'address'
		]
		for table in tables:
			where = " WHERE id_customer IN " + customer_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_orders_demo(self):
		next_clear = {
			'result': 'success',
			'function': 'clear_target_reviews_demo',
		}
		if not self._notice['config']['orders']:
			self._notice['target']['clear_demo'] = next_clear

			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_ORDER
		}
		orders = self.select_page(TABLE_MAP, where, self.LIMIT_CLEAR_DEMO)
		order_ids = list()
		if orders['result'] == 'success':
			order_ids = duplicate_field_value_from_list(orders['data'], 'id_desc')
		if not order_ids:
			self._notice['target']['clear_demo'] = next_clear

			return next_clear
		tables = [
			'orders',
			'order_carrier',
			'order_detail',
			'order_history',
		]

		for table in tables:
			where = " WHERE order_id IN " + self.list_to_in_condition(order_ids)
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self.delete_map_demo(self.TYPE_ORDER, order_ids)
		if order_ids and to_len(order_ids) < self.LIMIT_CLEAR_DEMO:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		return self._notice['target']['clear_demo']

	def clear_target_reviews_demo(self):
		next_clear = {
			'result': 'success',
			'function': 'clear_target_pages_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['reviews']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_REVIEW
		}
		reviews = self.select_obj(TABLE_MAP, where)
		review_ids = list()
		if reviews['result'] == 'success':
			review_ids = duplicate_field_value_from_list(reviews['data'], 'id_desc')
		if not review_ids:
			return next_clear
		tables = [
			'product_comment',
			'product_comment_grade',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "` WHERE id_product_comment IN " + self.list_to_in_condition(review_ids)
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue

	def clear_target_pages_demo(self):
		next_clear = {
			'result': 'success',
			'function': '',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['pages']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_PAGE
		}
		pages = self.select_obj(TABLE_MAP, where)
		page_ids = list()
		if pages['result'] == 'success':
			page_ids = duplicate_field_value_from_list(pages['data'], 'id_desc')
		if not page_ids:
			return next_clear
		tables = [
			'cms',
			'cms_lang',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "` WHERE id_cms IN " + self.list_to_in_condition(page_ids)
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

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
			'query': "SELECT * FROM _DBPRF_tax_rules_group WHERE id_tax_rules_group > " + to_str(id_src) + " ORDER BY id_tax_rules_group ASC LIMIT " + to_str(limit)
		}
		# taxes = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		taxes = self.select_data_connector(query, 'taxes')
		if not taxes or taxes['result'] != 'success':
			return response_error('could not get taxes main to export')
		return taxes

	def get_taxes_ext_export(self, taxes):
		tax_rule_ids = duplicate_field_value_from_list(taxes['data'], 'id_tax_rules_group')
		taxes_ext_queries = {
			'tax_rule': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_rule WHERE id_tax_rules_group IN " + self.list_to_in_condition(tax_rule_ids)
			},
		}
		# taxes_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(taxes_ext_queries)
		# })
		taxes_ext = self.select_multiple_data_connector(taxes_ext_queries, 'taxes')
		if not taxes_ext or taxes_ext['result'] != 'success':
			return response_error('err taxes ext data')

		tax_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_rule'], 'id_tax')
		country_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_rule'], 'id_country')
		state_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_rule'], 'id_state')
		taxes_ext_rel_queries = {
			'tax_and_tax_lang': {
				'type': 'select',
				'query': "SELECT t.*, tl.* FROM _DBPRF_tax AS t LEFT JOIN _DBPRF_tax_lang AS tl ON tl.id_tax = t.id_tax WHERE t.id_tax IN " + self.list_to_in_condition(tax_ids) + " AND tl.id_lang = " + self._notice['src']['language_default']
			},
			'country': {
				'type': 'select',
				'query': "SELECT c.*, cl.* FROM _DBPRF_country AS c LEFT JOIN _DBPRF_country_lang AS cl ON cl.id_country = c.id_country WHERE c.id_country IN " + self.list_to_in_condition(country_ids) + " AND cl.id_lang = " + self._notice['src']['language_default']
			},
			'state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_state WHERE id_state IN " + self.list_to_in_condition(state_ids)
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

		return taxes_ext

	def convert_tax_export(self, tax, taxes_ext):
		tax_zone_data = list()
		tax_product_data = list()

		tax_product = self.construct_tax_product()
		tax_product['id'] = tax['id_tax_rules_group']
		tax_product['code'] = None
		tax_product['name'] = tax['name']
		tax_product_data.append(tax_product)

		src_tax_rates = get_list_from_list_by_field(taxes_ext['data']['tax_rule'], 'id_tax_rules_group', tax['id_tax_rules_group'])
		if src_tax_rates:
			for src_tax_rate in src_tax_rates:
				tax_lang = get_row_from_list_by_field(taxes_ext['data']['tax_and_tax_lang'], 'id_tax', src_tax_rate['id_tax'])
				tax_zone_rate = self.construct_tax_zone_rate()
				if tax_lang:
					tax_zone_rate['id'] = src_tax_rate['id_tax']
					tax_zone_rate['name'] = tax_lang['name']
					tax_zone_rate['rate'] = tax_lang['rate']
					tax_zone_rate['priority'] = 1

				tax_zone_state = self.construct_tax_zone_state()
				src_tax_state = get_row_from_list_by_field(taxes_ext['data']['state'], 'id_state', src_tax_rate['id_state'])
				if src_tax_state:
					tax_zone_state['id'] = src_tax_state['id_state']
					tax_zone_state['name'] = src_tax_state['name']
					tax_zone_state['state_code'] = src_tax_state['iso_code']
				tax_zone_country = self.construct_tax_zone_country()
				src_tax_country = get_row_from_list_by_field(taxes_ext['data']['country'], 'id_country', src_tax_rate['id_country'])
				if src_tax_country:
					tax_zone_country['id'] = src_tax_country['id_country']
					tax_zone_country['name'] = src_tax_country['name']
					tax_zone_country['country_code'] = src_tax_country['iso_code']
				tax_zone = self.construct_tax_zone()
				tax_zone['id'] = tax['id_tax_rules_group']
				tax_zone['name'] = tax['name']
				tax_zone['country'] = tax_zone_country
				tax_zone['state'] = tax_zone_state
				tax_zone['rate'] = tax_zone_rate
				tax_zone_data.append(tax_zone)

		tax_data = self.construct_tax()
		tax_data['id'] = tax['id_tax_rules_group']
		tax_data['name'] = tax['name']
		tax_data['created_at'] = convert_format_time(tax['date_add'])
		tax_data['updated_at'] = convert_format_time(tax['date_upd'])
		tax_data['tax_products'] = tax_product_data
		tax_data['tax_zones'] = tax_zone_data

		return response_success(tax_data)

	def get_tax_id_import(self, convert, tax, taxes_ext):
		return tax['id_tax_rules_group']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id'], convert['code']) else False

	def router_tax_import(self, convert, tax, taxes_ext):
		return response_success('tax_import')

	def before_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def tax_import(self, convert, tax, taxes_ext):
		tax_status = 1
		if 'status' in convert:
			if not convert['status']:
				tax_status = 0
		tax_rules_group_data = {
			'name': convert['name'],
			'active': tax_status,
			'deleted': 0,
			'date_add': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'date_upd': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
		}
		id_tax_rules_group = self.import_data_connector(self.create_insert_query_connector('tax_rules_group', tax_rules_group_data), 'tax')
		if id_tax_rules_group:
			self.insert_map(self.TYPE_TAX, convert['id'], id_tax_rules_group, convert['code'])
		else:
			return response_error(self.warning_import_entity(self.TYPE_TAX, convert['id'], convert['code']))
		return response_success(id_tax_rules_group)

	def after_tax_import(self, tax_id, convert, tax, taxes_ext):
		all_query = list()
		language_def = self._notice['target']['language_default']

		tax_rules_group_shop_data = {
			'id_tax_rules_group': tax_id,
			'id_shop': self._notice['target']['shop_default'],
		}
		all_query.append(self.create_insert_query_connector('tax_rules_group_shop', tax_rules_group_shop_data))
		if not self._notice['support']['site_map']:
			for src_shop, target_shop in self._notice['map']['site'].items():
				if target_shop == self._notice['target']['shop_default']:
					continue
				tax_rules_group_shop_data = {
					'id_tax_rules_group': tax_id,
					'id_shop': target_shop,
				}
				all_query.append(self.create_insert_query_connector('tax_rules_group_shop', tax_rules_group_shop_data))
		tax_zones_data = convert['tax_zones']
		countries_active = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': 'SELECT id_country FROM _DBPRF_country WHERE active = 1'
			})
		})
		all_countries = list()
		if countries_active and countries_active['data']:
			all_countries = duplicate_field_value_from_list(countries_active['data'], 'id_country')
		if tax_zones_data:
			for tax_zone_data in tax_zones_data:
				tax_zone_rate = tax_zone_data['rate']
				id_tax = 0
				if tax_zone_rate:

					id_tax = self.get_map_field_by_src(self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate['code'])
					if not id_tax:
						tax_data = {
							'rate': tax_zone_rate['rate'],
							'active': convert.get('status', '1'),
							'deleted': 0,
						}
						id_tax = self.import_data_connector(self.create_insert_query_connector('tax', tax_data), 'tax')
						if id_tax:
							self.insert_map(self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], id_tax, tax_zone_rate['code'])
						tax_lang_data = {
							'id_tax': id_tax,
							'id_lang': language_def,
							'name': tax_zone_rate['name'],
						}
						all_query.append(self.create_insert_query_connector('tax_lang', tax_lang_data))

				tax_zone_country = tax_zone_data['country']
				if tax_zone_country['id'] or tax_zone_country['code']:
					country = self.get_connector_data(self.get_connector_url('query'), {
						'query': json.dumps({
							'type': 'select',
							'query': 'SELECT id_country FROM _DBPRF_country WHERE iso_code = "' + to_str(tax_zone_country['code']) + '"'
						})
					})
					if country and country['data']:
						id_country = country['data'][0]['id_country']
					else:
						if not tax_zone_country['id']:
							id_country = 0
						else:
							id_country = tax_zone_country['id']
				else:
					id_country = 0
				if not id_country and all_countries:
					id_country = all_countries[0]
				tax_zone_state = tax_zone_data['state']
				if id_country and (tax_zone_state['id'] or tax_zone_state['state_code']):
					state = self.get_connector_data(self.get_connector_url('query'), {
						'query': json.dumps({
							'type': 'select',
							'query': 'SELECT id_state FROM _DBPRF_state WHERE id_country = ' + to_str(id_country) + ' and iso_code = "' + to_str(tax_zone_state['state_code']) + '"'
						})
					})
					if state and state['data']:
						id_state = state['data'][0]['id_state']
					else:
						if not tax_zone_state['id']:
							id_state = 0
						else:
							id_state = tax_zone_state['id']
				else:
					id_state = 0

				tax_rule_data = {
					'id_tax_rules_group': tax_id,
					'id_country': id_country,
					'id_state': id_state,
					'zipcode_from': get_value_by_key_in_dict(tax_zone_data, 'postcode', 0),
					'zipcode_to': 0,
					'id_tax': id_tax,
					'behavior': 1,
					'description': tax_zone_rate.get('description', tax_zone_rate.get('name', '')) if tax_zone_rate else '',
				}
				id_tax_zone = self.import_data_connector(self.create_insert_query_connector('tax_rule', tax_rule_data), 'tax')
				self.insert_map(self.TYPE_TAX_ZONE, tax_zone_data['id'], id_tax_zone, tax_zone_data['code'])
		if all_query:
			self.import_multiple_data_connector(all_query, 'tax')
		del all_query
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
		manufacturer_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_manufacturer']).rstrip('/')
		manufacturer_data['thumb_image']['path'] = 'm/' + to_str(manufacturer['id_manufacturer']) + '.jpg'
		manufacturer_lang = get_list_from_list_by_field(manufacturers_ext['data']['manufacturer_lang'], 'id_manufacturer', manufacturer['id_manufacturer'])
		manufacturer_url = get_row_from_list_by_field(manufacturer_lang, 'id_lang', self._notice['src']['language_default'])
		# manufacturer_data['url'] = manufacturer_url
		if manufacturer_url:
			manufacturer_data['description'] = get_value_by_key_in_dict(manufacturer_url, 'description', '')
			manufacturer_data['meta_title'] = get_value_by_key_in_dict(manufacturer_url, 'meta_title', '')
			manufacturer_data['meta_description'] = get_value_by_key_in_dict(manufacturer_url, 'meta_description', '')
		manufacturer_data['created_at'] = convert_format_time(manufacturer['date_add'])
		manufacturer_data['updated_at'] = convert_format_time(manufacturer['date_upd'])

		return response_success(manufacturer_data)

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		return manufacturer['id_manufacturer']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id'], convert['code']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		manufacturer_data = {
			'name': convert['name'],
			'date_add': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'date_upd': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'active': 1 if convert['status'] else 0,
		}
		id_manufacturer = self.import_manufacturer_data_connector(self.create_insert_query_connector('manufacturer', manufacturer_data), True, convert['id'])
		if id_manufacturer:
			self.insert_map(self.TYPE_MANUFACTURER, convert['id'], id_manufacturer, convert['code'])
		else:
			return response_error(self.warning_import_entity(self.TYPE_MANUFACTURER, convert['id'], convert['code']))
		return response_success(id_manufacturer)

	def after_manufacturer_import(self, manufacturer_id, convert, manufacturer, manufacturers_ext):
		all_query = list()
		if not self._notice['support']['site_map']:
			manufacturer_shop_data = {
				'id_manufacturer': manufacturer_id,
				'id_shop': self._notice['target']['shop_default'],
			}
			all_query.append(self.create_insert_query_connector('manufacturer_shop', manufacturer_shop_data))
		else:
			for src_shop, target_shop in self._notice['map']['site'].items():
				manufacturer_shop_data = {
					'id_manufacturer': manufacturer_id,
					'id_shop': target_shop,
				}
				all_query.append(self.create_insert_query_connector('manufacturer_shop', manufacturer_shop_data))
		if convert['thumb_image']['url'] or convert['thumb_image']['path']:
			image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
			# image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path('img/' + to_str(image_process['path']), self._notice['target']['config']['image_category']))
			image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path('m/' + to_str(manufacturer_id) + '.jpg', self._notice['target']['config']['image_category']))

			if image_import_path:
				image_name = self.remove_prefix_path(image_import_path, self._notice['target']['config']['image_category'])
		manufacturer_lang_data = {
			'id_manufacturer': manufacturer_id,
			'id_lang': self._notice['target']['language_default'],
			'description': to_str(convert.get('description', '')),
			'short_description': '',
			'meta_title': to_str(convert.get('meta_title', '')),
			'meta_keywords': to_str(convert.get('meta_keyword', '')),
			'meta_description': to_str(convert.get('meta_description', '')),
		}
		all_query.append(self.create_insert_query_connector('manufacturer_lang', manufacturer_lang_data))
		if all_query:
			self.import_multiple_data_connector(all_query, 'manufacturer')
		del all_query
		return response_success()

	def addition_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	# TODO: CATEGORY

	def prepare_categories_export(self):
		return self

	def prepare_categories_import(self):
		parent = super().prepare_categories_import()
		if self._notice['config']['seo'] or self._notice['config']['seo_301']:
			query = self.dict_to_create_table_sql(self.lecm_rewrite_table_construct())
			self.query_data_connector({'type': 'query', 'query': query['query']})
		return self

	def get_categories_main_export(self):
		id_src = self._notice['process']['categories']['id_src']
		limit = self._notice['setting']['categories']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_category WHERE is_root_category != 1 AND id_category > 1 AND id_category > " + to_str(id_src) + " ORDER BY id_category ASC LIMIT " + to_str(limit)
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
			'category_shop': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category_shop WHERE id_category IN " + self.list_to_in_condition(category_ids),
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
			if parent_data['result'] == 'success' and parent_data['data']:
				parent = parent_data['data']
			elif parent_data['result'] == 'success' and not parent_data['data']:
				parent['id'] = 0
			else:
				return response_error()
		else:
			parent['id'] = 0

		category_data['id'] = category['id_category']
		category_data['parent'] = parent
		category_data['active'] = True if to_int(category['active']) == 1 else False

		if self.image_exist(self.get_url_suffix(self._notice['src']['config']['image_category']), 'c/' + to_str(category['id_category']) + '.jpg'):
			category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_category'])
			category_data['thumb_image']['path'] = 'c/' + to_str(category['id_category']) + '.jpg'
			category_data['thumb_image']['label'] = category['id_category']
		shops = get_list_from_list_by_field(categories_ext['data']['category_shop'], 'id_category',
		                                    category['id_category'])
		category_data['store_ids'] = duplicate_field_value_from_list(shops, 'id_shop')
		category_data['sort_order'] = category['position']
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
		detect_seo = self.detect_seo()
		category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)
		return response_success(category_data)

	def get_category_id_import(self, convert, category, categories_ext):
		return category['id_category']

	def check_category_import(self, convert, category, categories_ext):
		return True if self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code']) else False

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def category_import(self, convert, category, categories_ext):
		id_parent = 2
		parent = convert['parent']
		if parent and parent['id'] != 0 and (parent['id'] is not None or parent['code'] is not None):
			parent_import = self.import_category_parent(parent)
			if parent_import['result'] == 'success' and parent_import['data']:
				id_parent = parent_import['data']
		depth = 0
		if id_parent:
			query = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category WHERE id_category = " + to_str(id_parent),
			}
			category_path = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps(query),
			})
			if category_path and category_path['result'] == 'success' and category_path['data']:
				depth = to_int(category_path['data'][0]['level_depth']) + 1
		category_data = {
			'id_parent': id_parent,
			'level_depth': depth,
			'active': 1 if convert['active'] else 0,
			'date_add': convert['created_at'] if convert['created_at'] else get_current_time(),
			'date_upd': convert['updated_at'] if convert['updated_at'] else get_current_time(),
			'position': convert.get('sort_order', 0),
		}
		id_category = self.import_category_data_connector(self.create_insert_query_connector('category', category_data), True, convert['id'])
		if id_category:
			self.insert_map(self.TYPE_CATEGORY, convert['id'], id_category, convert['code'])
		else:
			return response_error(self.warning_import_entity(self.TYPE_CATEGORY, convert['id'], convert['code']))
		return response_success(id_category)

	def after_category_import(self, category_id, convert, category, categories_ext):
		all_query = list()
		if self._notice['config']['seo'] or self._notice['config']['seo_301']:
			for seo_url in convert['seo']:
				leurlrewrite = {
					'link_rewrite': seo_url['request_path'],
					'id_desc': category_id,
					'type': 'category',
					'lang_code': self._notice['target']['language_default']
				}
				seourl_query = self.create_insert_query_connector("lecm_rewrite", leurlrewrite)
				all_query.append(seourl_query)

		if self._notice['support']['site_map']:
			for src_store, target_store in self._notice['map']['site'].items():
				# if target_store == self._notice['target']['shop_default']:
				# 	continue
				category_shop_data = {
					'id_category': category_id,
					'id_shop': target_store,
					'position': convert.get('sort_order', 0),
				}
				all_query.append(self.create_insert_query_connector('category_shop', category_shop_data))
		else:
			category_shop_data = {
				'id_category': category_id,
				'id_shop': self._notice['target']['shop_default'],
				'position': convert.get('sort_order', 0),
			}
			all_query.append(self.create_insert_query_connector('category_shop', category_shop_data))
		url_image = self.get_connector_url('image')
		for src_language, target_language in self._notice['map']['languages'].items():
			category_lang = convert['languages'].get(to_str(src_language)) if convert['languages'].get(to_str(src_language)) else convert
			if self._notice['support']['site_map'] and convert.get('store_ids'):
				shop_imported = list()
				for src_store in convert['store_ids']:
					id_shop = get_value_by_key_in_dict(self._notice['map']['site'], src_store, self._notice['target']['shop_default'])
					if id_shop in shop_imported:
						continue
					shop_imported.append(id_shop)
					category_lang_data = {
						'id_category': category_id,
						'id_shop': id_shop,
						'id_lang': target_language,
						'name': self.strip_html_tag(category_lang['name'] if category_lang['name'] else convert['name']),
						'description': self.change_img_src_in_text(self.strip_html_tag(to_str(category_lang['description']))),
						'link_rewrite': convert.get('url_key') if convert.get('url_key') else self.convert_attribute_code(to_str(category_lang['name'] if category_lang['name'] else convert['name'])),
						'meta_title': category_lang['meta_title'],
						'meta_keywords': category_lang['meta_keyword'],
						'meta_description': category_lang['meta_description'],
					}
					category_shop_data = {
						'id_category': category_id,
						'id_shop': id_shop,
						'position': convert.get('sort_order', 0),
					}
					all_query.append(self.create_insert_query_connector('category_shop', category_shop_data))
					all_query.append(self.create_insert_query_connector('category_lang', category_lang_data))
			else:
				id_shop = self._notice['target']['shop_default']
				category_lang_data = {
					'id_category': category_id,
					'id_shop': id_shop,
					'id_lang': target_language,
					'name': self.strip_html_tag(category_lang['name'] if category_lang['name'] else convert['name']),
					'description': self.change_img_src_in_text(self.strip_html_tag(to_str(category_lang['description']))),
					'link_rewrite': convert['url_key'] if convert.get('url_key') else self.convert_attribute_code(to_str(category_lang['name'])),
					'meta_title': category_lang['meta_title'],
					'meta_keywords': category_lang['meta_keyword'],
					'meta_description': category_lang['meta_description'],
				}
				category_shop_data = {
					'id_category': category_id,
					'id_shop': id_shop,
					'position': convert.get('sort_order', 0),
				}
				all_query.append(self.create_insert_query_connector('category_shop', category_shop_data))
				all_query.append(self.create_insert_query_connector('category_lang', category_lang_data))

		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_group",
		}
		groups = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps(query),
		})
		if groups and 'data' in groups:
			for group in groups['data']:
				category_group_data = {
					'id_category': category_id,
					'id_group': group['id_group'],
				}
				all_query.append(self.create_insert_query_connector('category_group', category_group_data))

		if convert['thumb_image']['url'] or convert['thumb_image']['path']:
			image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
			# image_import = self.get_connector_data(url_image, {
			# 	'images': json.dumps({
			# 		'img': {
			# 			'type': 'download',
			# 			'path': self.add_prefix_path('c/' + to_str(category_id) + '.jpg', self._notice['target']['config']['image_category']),
			# 			'params': {
			# 				'url': image_process['url'],
			# 				'rename': True
			# 			}
			# 		}
			# 	})
			# })
			image_import_path = None
			if self.image_exist(image_process['url']):
				image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path('c/' + to_str(category_id) + '.jpg', self._notice['target']['config']['image_category']))
			# if image_import and image_import['result'] == 'success':
			#   image_import_path = image_import['data']['img']
			if image_import_path:
				image_name = self.remove_prefix_path(image_import_path, self._notice['target']['config']['image_category'])
		if all_query:
			self.import_multiple_data_connector(all_query)
		del all_query
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	# TODO: PRODUCT

	def prepare_products_export(self):
		return self

	def prepare_products_import(self):
		parent = super().prepare_products_import()
		if self._notice['config']['seo'] or self._notice['config']['seo_301']:
			query = self.dict_to_create_table_sql(self.lecm_rewrite_table_construct())
			self.query_data_connector({'type': 'query', 'query': query['query']})
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
				'query': "SELECT * FROM _DBPRF_product_attribute WHERE id_product IN " + self.list_to_in_condition(product_ids) + " ORDER BY id_product_attribute ASC"
			},
			'feature_product': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_feature_product WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'product_tag': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_tag WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'specific_price': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_specific_price WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'stock_available': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_stock_available WHERE id_product IN " + self.list_to_in_condition(product_ids)
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
		category_ids = duplicate_field_value_from_list(product_ext['data']['category_product'], 'id_category')
		product_ext_rel_queries = {
			'product_attribute_combination': {
				'type': 'select',
				'query': "SELECT pac.*, a.* FROM _DBPRF_product_attribute_combination AS pac LEFT JOIN _DBPRF_attribute AS a ON a.id_attribute = pac.id_attribute WHERE pac.id_product_attribute IN " + self.list_to_in_condition(product_attribute_ids)
			},
			'product_attribute_image': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_attribute_image WHERE id_product_attribute IN " + self.list_to_in_condition(product_attribute_ids)
			},
			'feature_lang': {
				'type': 'select',
				'query': "SELECT f.*, fl.* FROM _DBPRF_feature AS f LEFT JOIN _DBPRF_feature_lang AS fl ON fl.id_feature = f.id_feature WHERE f.id_feature IN " + self.list_to_in_condition(feature_ids)
			},
			'feature_value': {
				'type': 'select',
				'query': "SELECT * FROM  _DBPRF_feature_value  WHERE id_feature IN " + self.list_to_in_condition(
					feature_ids)
			},
			'feature_value_lang': {
				'type': 'select',
				'query': "SELECT fv.*, fvl.* FROM _DBPRF_feature_value AS fv LEFT JOIN _DBPRF_feature_value_lang AS fvl ON fvl.id_feature_value = fv.id_feature_value WHERE fv.id_feature_value IN " + self.list_to_in_condition(feature_value_ids)
			},
			'tags': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tag WHERE id_tag IN " + self.list_to_in_condition(tag_ids)
			},
			'category_lang': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_category_lang WHERE id_category IN " + self.list_to_in_condition(category_ids)
			},
			'product_shop': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_product_shop WHERE id_product IN " + self.list_to_in_condition(product_ids)
			},
			'stock_available': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_stock_available WHERE id_product IN " + self.list_to_in_condition(product_ids)
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
		stock_data_all = get_list_from_list_by_field(products_ext['data']['stock_available'], 'id_product', product['id_product'])
		stock_data_main = get_row_from_list_by_field(stock_data_all, 'id_product_attribute', 0)
		product_data = self.construct_product()
		product_data['id'] = product['id_product']

		product_data['price'] = to_decimal(product['price']) if product['price'] else 0
		if to_decimal(product['wholesale_price']) > 0:
			product_data['cost'] = to_decimal(product['wholesale_price'])
		product_data['width'] = product['width']
		product_data['height'] = product['height']
		product_data['length'] = product['depth']
		product_data['weight'] = product['weight']
		product_data['status'] = True if to_int(product['active']) == 1 else False
		product_data['qty'] = stock_data_main['quantity'] if stock_data_main and 'quantity' in stock_data_main else 0
		product_data['manage_stock'] = True
		product_data['is_in_stock'] = True if to_int(product_data['qty']) > 0 else False
		product_data['created_at'] = convert_format_time(product['date_add'])
		product_data['updated_at'] = convert_format_time(product['date_upd'])
		product_data['ean'] = product['ean13']
		product_data['upc'] = product['upc']
		shops = get_list_from_list_by_field(products_ext['data']['product_shop'], 'id_product', product['id_product'])
		product_data['store_ids'] = duplicate_field_value_from_list(shops, 'id_shop')
		special_price = False
		specific_price = get_list_from_list_by_field(products_ext['data']['specific_price'], 'id_product', product['id_product'])
		list_special = get_list_from_list_by_field(specific_price, 'from_quantity', 1)
		special = self.get_special_price(list_special)
		if special:
			if special['reduction_type'] == 'amount':
				special_price = to_decimal(product['price']) - to_decimal(special['reduction'])
			else:
				special_price = to_decimal(product['price']) - to_decimal(to_decimal(special['reduction']) * to_decimal(product['price']))
			if special_price and to_decimal(special_price) > to_decimal(special['price']):
				product_data['special_price']['price'] = special_price if special_price <= to_int(special['price']) else to_int(special['price'])
				product_data['special_price']['start_date'] = convert_format_time(special['from'])
				product_data['special_price']['end_date'] = convert_format_time(special['to'])

		product_descriptions = get_list_from_list_by_field(products_ext['data']['product_lang'], 'id_product', product['id_product'])
		if not product_descriptions:
			msg = 'Product id ' + to_str(product['id_product']) + ' not description.'
			return response_error(msg)
		product_description_def = get_row_from_list_by_field(product_descriptions, 'id_lang', self._notice['src']['language_default'])
		if not product_description_def:
			product_description_def = product_descriptions[0]
		product_data['name'] = product_description_def['name']
		sku = product['reference']
		if not sku:
			sku = self.create_sku_by_name(product_data['name'])
		product_data['sku'] = sku
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
			product_data['thumb_image']['path'] = self._get_image_path(product_image_main['id_image'])
		if product_images:
			for product_image in product_images:
				if to_int(product_image['cover']) == 1:
					continue
				product_image_data = self.construct_product_image()
				product_image_data['label'] = product_image['legend']
				product_image_data['url'] = url_product_image
				product_image_data['path'] = self._get_image_path(product_image['id_image'])
				product_data['images'].append(product_image_data)

		product_data['tax']['id'] = product['id_tax_rules_group']

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

		product_features = get_list_from_list_by_field(products_ext['data']['feature_product'], 'id_product', product['id_product'])
		# if product_feature:
		# 	feature_ids = duplicate_field_value_from_list(product_feature, 'id_feature')
		# 	for feature_id in feature_ids:
		# 		option_data = self.construct_product_option()
		# 		option_data['id'] = feature_id
		# 		feature_langs = get_list_from_list_by_field(products_ext['data']['feature_lang'], 'id_feature', feature_id)
		# 		feature_lang_def = get_row_from_list_by_field(feature_langs, 'id_lang', self._notice['src']['language_default'])
		# 		if not feature_lang_def:
		# 			feature_lang_def = feature_langs[0]
		# 		option_data['option_name'] = feature_lang_def['name']
		# 		for feature_lang in feature_langs:
		# 			option_language_data = self.construct_product_option_lang()
		# 			option_language_data['option_name'] = feature_lang['name']
		# 			language_id = feature_lang['id_lang']
		# 			option_data['option_languages'][language_id] = option_language_data
		#
		# 		product_feature_value = get_list_from_list_by_field(product_feature, 'id_feature', feature_id)
		# 		feature_value_ids = duplicate_field_value_from_list(product_feature_value, 'id_feature_value')
		# 		for feature_value_id in feature_value_ids:
		# 			option_value_data = self.construct_product_option_value()
		# 			option_value_data['id'] = feature_value_id
		# 			feature_value_langs = get_list_from_list_by_field(products_ext['data']['feature_value_lang'], 'id_feature_value', feature_value_id)
		# 			feature_value_lang_def = get_row_from_list_by_field(feature_value_langs, 'id_lang', self._notice['src']['language_default'])
		# 			if not feature_value_lang_def:
		# 				feature_value_lang_def = feature_value_langs[0]
		# 			option_value_data['option_value_name'] = feature_value_lang_def['value']
		#
		# 			for feature_value_lang in feature_value_langs:
		# 				option_value_language_data = self.construct_product_option_value_lang()
		# 				option_value_language_data['option_value_name'] = feature_value_lang['value']
		# 				language_id = feature_value_lang['id_lang']
		# 				option_value_data['option_value_languages'][language_id] = option_value_language_data
		# 			option_data['values'].append(option_value_data)
		# 		product_data['options'].append(option_data)

		if product_features:
			for product_feature in product_features:
				product_attribute_data = self.construct_product_attribute()
				feature_langs = get_list_from_list_by_field(products_ext['data']['feature_lang'], 'id_feature', product_feature['id_feature'])
				feature_lang_def = get_row_from_list_by_field(feature_langs, 'id_lang', self._notice['src']['language_default'])
				if not feature_lang_def and feature_langs:
					feature_lang_def = feature_langs[0]
				product_attribute_data['option_id'] = product_feature['id_feature']
				product_attribute_data['option_type'] = 'select'
				feature_value = get_row_from_list_by_field(products_ext['data']['feature_value'], 'id_feature_value', product_feature['id_feature_value'])
				if feature_value:
					if to_int(feature_value['custom']) == 1:
						product_attribute_data['option_type'] = 'text'
				product_attribute_data['option_code'] = to_str(feature_lang_def['name']).lower() if feature_lang_def else ""
				product_attribute_data['option_name'] = feature_lang_def['name'] if feature_lang_def else ""
				for feature_lang in feature_langs:
					option_language_data = self.construct_product_option_lang()
					option_language_data['option_name'] = feature_lang['name']
					language_id = feature_lang['id_lang']
					product_attribute_data['option_languages'][language_id] = option_language_data

				feature_value_langs = get_list_from_list_by_field(products_ext['data']['feature_value_lang'], 'id_feature_value', product_feature['id_feature_value'])
				feature_value_lang_def = get_row_from_list_by_field(feature_value_langs, 'id_lang', self._notice['src']['language_default'])
				if not feature_value_lang_def and feature_value_langs:
					feature_value_lang_def = feature_value_langs[0]
				product_attribute_data['option_value_id'] = product_feature['id_feature_value']
				product_attribute_data['option_value_code'] = to_str(feature_value_lang_def['value']).lower() if feature_value_lang_def else ""
				product_attribute_data['option_value_name'] = feature_value_lang_def['value'] if feature_value_lang_def else ""
				for feature_value_lang in feature_value_langs:
					option_value_language_data = self.construct_product_option_value_lang()
					option_value_language_data['option_value_name'] = feature_value_lang['value']
					language_id = feature_value_lang['id_lang']
					product_attribute_data['option_value_languages'][language_id] = option_value_language_data
				product_data['attributes'].append(product_attribute_data)
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
		# export utc , barcode , ean ...
		utcs = {'ean13', 'upc', 'jan'}
		for utc in utcs:
			if utc in product:
				utc_import = self.construct_product_attribute()
				utc_import['option_code'] = utc
				utc_import['option_mode'] = 'backend'
				utc_import['option_name'] = utc
				utc_import['option_type'] = 'text'
				utc_import['option_value_code'] = product[utc]
				utc_import['option_value_name'] = product[utc]
				product_data['attributes'].append(utc_import)
		children_products = get_list_from_list_by_field(products_ext['data']['product_attribute'], 'id_product', product['id_product'])
		combinations = list()
		if children_products:
			product_data['type'] = self.PRODUCT_CONFIG
			list_id_product_attribute = duplicate_field_value_from_list(children_products, 'id_product_attribute')
			product_attribute_combination = get_list_from_list_by_field(products_ext['data']['product_attribute_combination'], 'id_product_attribute', list_id_product_attribute)
			list_id_attribute_group = duplicate_field_value_from_list(product_attribute_combination, 'id_attribute_group')
			id_attribute_dup = duplicate_field_value_from_list(product_attribute_combination, 'id_attribute')
			options_src = dict()
			for id_attribute_group in list_id_attribute_group:
				children_options = get_list_from_list_by_field(product_attribute_combination, 'id_attribute_group', id_attribute_group)
				children_product_option_data = self.construct_product_option()
				attribute_group_langs = get_list_from_list_by_field(products_ext['data']['attribute_group_lang'], 'id_attribute_group', id_attribute_group)
				attribute_group_lang_def = get_row_from_list_by_field(attribute_group_langs, 'id_lang', self._notice['src']['language_default'])
				children_product_option_data['id'] = id_attribute_group
				option_type = 'checkbox'
				if attribute_group_lang_def.get('group_type') == 'radio':
					option_type = 'radio'
				children_product_option_data['option_type'] = option_type if to_int(id_attribute_group) != 14 else 'text'
				children_product_option_data['option_name'] = attribute_group_lang_def['public_name'] if attribute_group_lang_def['public_name'] else attribute_group_lang_def['name']
				children_product_option_data['sort_order'] = attribute_group_lang_def.get('position')
				for attribute_group_lang in attribute_group_langs:
					product_attribute_lang_data = self.construct_product_option_lang()
					product_attribute_lang_data['option_name'] = attribute_group_lang['public_name'] if attribute_group_lang['public_name'] else attribute_group_lang['name']
					language_id = attribute_group_lang['id_lang']
					children_product_option_data['option_languages'][language_id] = product_attribute_lang_data
				id_attribute_dup = duplicate_field_value_from_list(children_options, 'id_attribute')
				for children_option in id_attribute_dup:
					attribute_langs = get_list_from_list_by_field(products_ext['data']['attribute_lang'], 'id_attribute', children_option)
					attribue_lang_def = get_row_from_list_by_field(attribute_langs, 'id_lang', self._notice['src']['language_default'])
					children_options_cmb = get_list_from_list_by_field(products_ext['data']['product_attribute_combination'], 'id_attribute', children_option)
					if not to_str(attribue_lang_def.get('name')).strip():
						continue
					for children_cmb in children_options_cmb:
						children_product = get_row_from_list_by_field(children_products, 'id_product_attribute', children_cmb['id_product_attribute'])
						if children_product:
							break
					option_value = self.construct_product_option_value()
					option_value['id'] = attribue_lang_def['id_attribute']
					option_value['option_value_name'] = attribue_lang_def['name']
					for children_cmb in children_options_cmb:
						product_value = get_row_from_list_by_field(products_ext['data']['stock_available'], 'id_product_attribute', children_cmb['id_product_attribute'])
						if product_value:
							option_value['option_value_qty'] = product_value.get('quantity')
					for attribute_lang in attribute_langs:
						product_attribute_value_lang_data = self.construct_product_option_value_lang()
						product_attribute_value_lang_data['option_value_name'] = attribute_lang['name']
						language_id = attribute_lang['id_lang']
						option_value['option_value_languages'][language_id] = product_attribute_value_lang_data
					children_product_option_data['values'].append(option_value)

				if not children_product_option_data['values']:
					continue

				combinations.append(children_product_option_data)

		product_tags = get_list_from_list_by_field(products_ext['data']['product_tag'], 'id_product', product['id_product'])
		if product_tags:
			tags = list()
			for product_tag in product_tags:
				tag = get_row_from_list_by_field(products_ext['data']['tags'], 'id_tag', product_tag['id_tag'])
				if tag:
					tags.append(tag['name'])
			product_data['tags'] = (','.join(tags))[0:254]
		children = self.convert_option_to_child(combinations, product_data)
		for child in children:
			child['type'] = self.PRODUCT_SIMPLE
			child['is_child'] = True
			id_attribute = list()
			for attribute in child['attributes']:
				if attribute['option_value_id']:
					id_attribute.append(attribute['option_value_id'])
			child_com = self.get_children_from_list(id_attribute, children_products, products_ext)
			if not child_com:
				continue
			children_image = get_row_from_list_by_field(products_ext['data']['product_attribute_image'], 'id_product_attribute', child_com['id_product_attribute'])
			if children_image:
				url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
				child['thumb_image']['url'] = url_product_image
				child['thumb_image']['path'] = self._get_image_path(children_image['id_image'])
			if child_com['weight']:
				child['weight'] = to_decimal(child['weight']) + to_decimal(child_com['weight'])
			child['ean'] = child_com['ean13']
			child['isbn'] = child_com.get('isbn')
			child['upc'] = child_com.get('upc')
			if child_com.get('reference'):
				child['sku'] = child_com.get('reference')
			if child_com.get('price'):
				child['price'] = to_decimal(child['price']) + to_decimal(child_com['price'])
			product_data['children'].append(child)

		# if children:
		detect_seo = self.detect_seo()
		product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)

		return response_success(product_data)

	def get_product_id_import(self, convert, product, products_ext):
		return product['id_product']

	def check_product_import(self, convert, product, products_ext):
		return self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'])

	def update_latest_data_product(self, product_id, convert, product, products_ext):
		all_query = list()
		all_query.append(self.create_delete_query_connector('category_product', {'id_product': product_id}))
		category_desc = list()
		for value in convert['categories']:
			category_ids = list()
			category_list = self.select_category_map(value['id'])
			if category_list:
				for category_map in category_list:
					category_ids.append(category_map['id_desc'])
			if not category_ids:
				continue
			for category_id in category_ids:
				if category_id in category_desc:
					continue
				category_desc.append(category_id)
				category_product_data = {
					'id_category': category_id,
					'id_product': product_id,
				}
				all_query.append(self.create_insert_query_connector('category_product', category_product_data))

		id_category_default = 1
		if convert['categories']:
			id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['categories'][0]['id'], convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, None, convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = 1

		product_data = {
			'id_category_default': id_category_default,
			'price': convert['price'],
			'reference': get_value_by_key_in_dict(convert, 'sku', '')[0:32],
			'active': 1 if convert['status'] else 0,
		}
		all_query.append(self.create_update_query_connector('product', product_data, {'id_product': product_id}))

		product_shop_data = {
			'id_category_default': id_category_default,
			'active': 1 if convert['status'] else 0,
			'price': convert['price'],
		}
		all_query.append(self.create_update_query_connector('product_shop', product_shop_data, {'id_product': product_id}))

		if convert['special_price']['price']:
			all_query.append(self.create_delete_query_connector('specific_price', {'id_product': product_id}))
			specific_price_data = {
				'id_specific_price_rule': 0,
				'id_cart': 0,
				'id_product': product_id,
				'id_shop_group': 0,
				'id_currency': 0,
				'id_country': 0,
				'id_group': 0,
				'id_customer': 0,
				'id_product_attribute': 0,
				'price': '-1.000000',
				'from_quantity': 1,
				'reduction_type': 'amount',
				'reduction': to_decimal(convert['price']) - to_decimal(convert['special_price']['price']),
				'from': convert['special_price']['start_date'] if convert['special_price']['start_date'] else '0000-00-00 00:00:00',
				'to': convert['special_price']['end_date'] if convert['special_price']['end_date'] else '0000-00-00 00:00:00',
			}
			all_query.append(self.create_update_query_connector('specific_price', specific_price_data, {'id_product': product_id}))

		qty = convert['qty']
		if 'manage_stock' in convert and convert['manage_stock']:
			qty = convert['qty']
		else:
			qty = 9999
		stock_available_data = {
			'quantity': qty,
		}
		all_query.append(self.create_update_query_connector('stock_available', stock_available_data, {'id_product': product_id}))

		for src_language, target_language in self._notice['map']['languages'].items():
			product_lang = convert['languages'][src_language] if (src_language in convert['languages'] and convert['languages'][src_language]) else convert

			if self._notice['support']['site_map']:
				shop_imported = list()
				for src_store in convert['store_ids']:
					id_shop = get_value_by_key_in_dict(self._notice['map']['site'], src_store, self._notice['target']['shop_default'])
					if id_shop in shop_imported:
						continue
					shop_imported.append(id_shop)
					product_lang_data = {
						'name': product_lang['name'],
						'link_rewrite': to_str(re.sub(r"[^a-zA-Z0-9- ]", '', product_lang['name'])).lower().replace(' ', '-'),
						'meta_title': to_str(product_lang['meta_title'])[0:128],
						'meta_keywords': to_str(product_lang['meta_keyword'])[0:255],
						'meta_description': to_str(product_lang['meta_description'])[0:255],
					}
					where_lang_data = {
						'id_product': product_id,
						'id_shop': id_shop,
						'id_lang': target_language,
					}
					all_query.append(self.create_update_query_connector('product_lang', product_lang_data, where_lang_data))
			else:
				id_shop = self._notice['target']['shop_default']
				product_lang_data = {
					'name': product_lang['name'][0:128],
					'link_rewrite': to_str(re.sub(r"[^a-zA-Z0-9- ]", '', product_lang['name'])).lower().replace(' ', '-'),
					'meta_title': to_str(product_lang['meta_title'])[0:128],
					'meta_keywords': to_str(product_lang['meta_keyword'])[0:255],
					'meta_description': to_str(product_lang['meta_description'])[0:255],
				}
				where_lang_data = {
					'id_product': product_id,
					'id_shop': id_shop,
					'id_lang': target_language,
				}
				all_query.append(self.create_update_query_connector('product_lang', product_lang_data, where_lang_data))

		self.import_multiple_data_connector(all_query, 'update_demo_product')
		return response_success()

	def update_product_after_demo(self, product_id, convert, product, products_ext):
		all_queries = list()
		id_manufacturer = 0
		if convert['manufacturer']['id'] or convert['manufacturer']['code']:
			id_manufacturer = self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['manufacturer']['id'], convert['manufacturer']['code'])
			if not id_manufacturer:
				id_manufacturer = self.get_map_field_by_src(self.TYPE_MANUFACTURER, None, convert['manufacturer']['code'])
			if not id_manufacturer:
				id_manufacturer = 0
		id_tax_rules_group = 0
		if convert['tax']['id'] or convert['tax']['code']:
			id_tax_rules_group = self.get_map_field_by_src(self.TYPE_TAX, convert['tax']['id'], convert['tax']['code'])
			if not id_tax_rules_group:
				id_tax_rules_group = 0
		id_category_default = 1
		if convert['categories']:
			id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['categories'][0]['id'], convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, None, convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = 1
		all_queries.append(self.create_update_query_connector('product', {'id_manufacturer': id_manufacturer, 'id_category_default': id_category_default, 'id_tax_rules_group': id_tax_rules_group}, {'id_product': product_id}))
		all_queries.append(self.create_update_query_connector('product_shop', {'id_category_default': id_category_default, 'id_tax_rules_group': id_tax_rules_group}, {'id_product': product_id}))
		if convert['categories']:
			all_queries.append(self.create_delete_query_connector('category_product', {'id_product': product_id}))
			for category in convert['categories']:
				id_category = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], category['code'])
				if not id_category:
					id_category = 0
				category_product_data = {
					'id_category': id_category,
					'id_product': product_id,
				}
				all_queries.append(self.create_insert_query_connector('category_product', category_product_data))
		self.import_multiple_data_connector(all_queries, 'update_demo_product')
		return response_success()

	def router_product_import(self, convert, product, products_ext):
		return response_success('product_import')

	def before_product_import(self, convert, product, products_ext):
		return response_success()

	def product_import(self, convert, product, products_ext):
		all_query = list()
		id_manufacturer = 0
		if convert['manufacturer']['id'] or convert['manufacturer']['code']:
			id_manufacturer = self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['manufacturer']['id'], convert['manufacturer']['code'])
			if not id_manufacturer:
				id_manufacturer = self.get_map_field_by_src(self.TYPE_MANUFACTURER, None, convert['manufacturer']['code'])
			if not id_manufacturer:
				id_manufacturer = 0
		id_tax_rules_group = 0
		if convert['tax']['id'] or convert['tax']['code']:
			id_tax_rules_group = self.get_map_field_by_src(self.TYPE_TAX, convert['tax']['id'], convert['tax']['code'])
			if not id_tax_rules_group:
				id_tax_rules_group = 0
		id_category_default = 1
		if convert['categories']:
			id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['categories'][0]['id'], convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = self.get_map_field_by_src(self.TYPE_CATEGORY, None, convert['categories'][0]['code'])
			if not id_category_default:
				id_category_default = 1
		is_virtual = 0
		cache_is_pack = 0
		if convert['type'] == self.PRODUCT_VIRTUAL:
			is_virtual = 1
		if convert['type'] == self.PRODUCT_GROUP:
			cache_is_pack = 1

		product_data = {
			'id_supplier': 0,
			'id_manufacturer': id_manufacturer,
			'id_category_default': id_category_default,
			'id_tax_rules_group': id_tax_rules_group,
			'price': convert['price'] if convert['price'] else 0.00,
			'supplier_reference': '',
			'location': convert.get('location'),
			'reference': get_value_by_key_in_dict(convert, 'sku', '')[0:32],
			'width': convert['width'] if convert['width'] else 0.0000,
			'height': convert['height'] if convert['height'] else 0.0000,
			'weight': convert['weight'] if convert['weight'] else 0.0000,
			'depth': convert['length'] if convert['length'] else 0.0000,
			'active': 1 if convert['status'] else 0,
			'redirect_type': '404',
			'is_virtual': is_virtual,
			'date_add': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'date_upd': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'cache_is_pack': cache_is_pack,
			'upc': convert.get('upc', convert.get('barcode')),
			'ean13': convert.get('ean', convert.get('barcode')),
			'isbn': convert.get('isbn', convert.get('barcode')),
			'out_of_stock': 1 if convert['is_in_stock'] else 2,
		}
		if self._notice['config']['pre_prd']:
			product_data['id_product'] = convert['id']
		id_product = self.import_product_data_connector(self.create_insert_query_connector('product', product_data), True, convert['id'])
		if id_product:
			self.insert_map(self.TYPE_PRODUCT, convert['id'], id_product, convert['code'])
		else:
			return response_error(self.warning_import_entity(self.TYPE_PRODUCT, convert['id'], convert['code']))

		product_shop_data = {
			'id_product': id_product,
			'id_shop': self._notice['target']['shop_default'],
			'id_category_default': id_category_default,
			'id_tax_rules_group': id_tax_rules_group,
			'active': 1 if convert['status'] else 0,
			'price': convert['price'],
			'redirect_type': '404',
			'date_add': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
			'date_upd': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
		}
		all_query.append(self.create_insert_query_connector('product_shop', product_shop_data))
		if self._notice['support']['site_map']:
			for src_store, target_store in self._notice['map']['site'].items():
				if target_store == self._notice['target']['shop_default']:
					continue
				product_shop_data = {
					'id_product': id_product,
					'id_shop': target_store,
					'id_category_default': id_category_default,
					'id_tax_rules_group': id_tax_rules_group,
					'active': 1 if convert['status'] else 0,
					'price': convert['price'],
					'redirect_type': '404',
					'date_add': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
					'date_upd': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
				}
				all_query.append(self.create_insert_query_connector('product_shop', product_shop_data))

		if all_query:
			self.import_multiple_data_connector(all_query, 'product')
		del all_query
		return response_success(id_product)

	def after_product_import(self, product_id, convert, product, products_ext):
		all_queries = list()
		# if convert['type'] == self.PRODUCT_GROUP:
		# 	list_pack = list()
		# 	if 'group_child_ids' in convert and convert['group_child_ids']:
		# 		for child_src in convert['group_child_ids']:
		# 			child_target = self.get_map_field_by_src(self.TYPE_PRODUCT, child_src['id'])
		# 			if child_target:
		# 				list_pack.append({
		# 					'id': child_target,
		# 					'qty': get_value_by_key_in_dict(child_src, 'qty', 1)
		# 				})
		# 	for pack in list_pack:
		# 		pack_data = {
		# 			'id_product_pack': product_id,
		# 			'id_product_item': pack['id'],
		# 			'id_product_attribute_item': 0,
		# 			'quantity': pack['qty'],
		# 		}
		# 		pack_query = self.create_insert_query_connector("pack", pack_data)
		# 		all_queries.append(pack_query)
		# if 'group_parent_ids' in convert:
		# 	for parent_src in convert['group_parent_ids']:
		# 		parent_target = self.get_map_field_by_src(self.TYPE_PRODUCT, parent_src)
		# 		if parent_target:
		# 			pack_data = {
		# 				'id_product_pack': parent_target,
		# 				'id_product_item': product_id,
		# 				'id_product_attribute_item': 0,
		# 				'quantity': 1,
		# 			}
		# 			pack_query = self.create_insert_query_connector("pack", pack_data)
		# 			all_queries.append(pack_query)

		all_query = list()
		url_image = self.get_connector_url('image')
		qty = convert['qty']
		if 'manage_stock' in convert and convert['manage_stock']:
			qty = convert['qty']
		else:
			qty = 9999
		stock_available_data = {
			'id_product': product_id,
			'id_product_attribute': 0,
			'id_shop': self._notice['target']['shop_default'],
			'id_shop_group': 0,
			'quantity': qty,
		}
		all_query.append(self.create_insert_query_connector('stock_available', stock_available_data))

		for src_language, target_language in self._notice['map']['languages'].items():
			product_lang = convert['languages'][src_language] if (src_language in convert['languages'] and convert['languages'][src_language]) else convert
			product_name_lang = product_lang['name'] if product_lang and product_lang.get('name') else convert['name']
			if self._notice['support']['site_map']:
				shop_imported = list()
				for src_store in convert['store_ids']:
					id_shop = get_value_by_key_in_dict(self._notice['map']['site'], src_store, self._notice['target']['shop_default'])
					if id_shop in shop_imported:
						continue
					shop_imported.append(id_shop)
					product_lang_data = {
						'id_product': product_id,
						'id_shop': id_shop,
						'id_lang': target_language,
						'name': self.strip_html_tag(to_str(product_name_lang)[0:128]),
						'description': self.change_img_src_in_text(to_str(product_lang['description']).replace('\\\\', '\\')),
						'description_short': self.change_img_src_in_text(to_str(product_lang['short_description'])),
						'link_rewrite': self.convert_attribute_code(product_name_lang),
						'meta_title': to_str(product_lang['meta_title'])[0:128],
						'meta_keywords': to_str(product_lang['meta_keyword'])[0:255],
						'meta_description': to_str(product_lang['meta_description'])[0:255],
					}
					all_query.append(self.create_insert_query_connector('product_lang', product_lang_data))
			else:
				id_shop = self._notice['target']['shop_default']
				product_lang_data = {
					'id_product': product_id,
					'id_shop': id_shop,
					'id_lang': target_language,
					'name': self.strip_html_tag(to_str(product_lang['name'])[0:128] if product_lang['name'] else convert['name']),
					'description': self.change_img_src_in_text(to_str(product_lang['description']).replace('\\\\', '\\')),
					'description_short': self.strip_html_tag(to_str(product_lang['short_description'])),
					'link_rewrite': self.convert_attribute_code(product_name_lang),
					'meta_title': to_str(product_lang['meta_title'])[0:128],
					'meta_keywords': to_str(product_lang['meta_keyword'])[0:255],
					'meta_description': to_str(product_lang['meta_description'])[0:255],
				}
				all_query.append(self.create_insert_query_connector('product_lang', product_lang_data))

		if convert['tags']:
			product_tags = to_str(convert['tags']).split(',')
			for product_tag in product_tags:
				tag_data = {
					'id_lang': self._notice['target']['language_default'],
					'name': to_str(product_tag).replace('_', '-'),
				}
				id_tag = self.import_data_connector(self.create_insert_query_connector('tag', tag_data), 'product')
				product_tag_data = {
					'id_product': product_id,
					'id_tag': id_tag,
					'id_lang': self._notice['target']['language_default'],
				}
				all_query.append(self.create_insert_query_connector('product_tag', product_tag_data))
		image_order = 1
		image_imported = list()
		resize_images = get_list_from_list_by_field(self._notice['target']['images_resize'], 'products', '1')

		previous_image = list()
		# if self._notice['config']['ignore_existed_images']:
		previous_image = self.select_image_map(convert['id'], None, convert['code'])
		if convert['thumb_image']['path'] or convert['thumb_image']['url']:
			image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
			image_data = {
				'id_product': product_id,
				'position': image_order,
				'cover': 1,
			}
			id_image = self.import_data_connector(self.create_insert_query_connector('image', image_data), 'product')
			id_image_exist = self.get_id_image(image_process['url'], previous_image)
			# if id_image_exist:
			# 	id_image = id_image_exist
			if id_image:
				if id_image_exist:
					all_queries.append(self.create_delete_query_connector('image_shop', {'id_image': id_image_exist}))
					all_queries.append(self.create_delete_query_connector('image_lang', {'id_image': id_image_exist}))
				image_shop_data = {
					'id_product': product_id,
					'id_image': id_image,
					'id_shop': self._notice['target']['shop_default'],
					'cover': 1,
				}
				all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
				if self._notice['support']['site_map'] and 'store_ids' in convert:
					for src_store in convert['store_ids']:
						# for src_store, target_store in self._notice['map']['site']:
						if src_store in self._notice['map']['site']:
							if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
								continue
							image_shop_data = {
								'id_product': product_id,
								'id_image': id_image,
								'id_shop': self._notice['map']['site'][src_store],
								'cover': 1,
							}
							all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
				for src_language, target_language in self._notice['map']['languages'].items():
					image_lang_data = {
						'id_image': id_image,
						'id_lang': target_language,
					}
					all_query.append(self.create_insert_query_connector('image_lang', image_lang_data))

				image_imported.append({
					'id_image': id_image,
					'url': image_process['url']
				})
				if not id_image_exist:
					image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(self._get_image_path(id_image), self._notice['target']['config']['image_product']))
				else:
					image_import_path = self.move_image_connector(image_process, self.add_prefix_path(self._get_image_path(id_image), self._notice['target']['config']['image_product']), self.add_prefix_path(self._get_image_path(id_image_exist), self._notice['target']['config']['image_product']))
				if image_import_path:
					if not id_image_exist:
						self.insert_map(self.TYPE_IMAGE, convert['id'], id_image, convert['code'], None, image_process['url'])
					else:
						self.update_map(self.TYPE_IMAGE, convert['id'], convert['code'], id_image)
					resize_data = dict()
					for image_resize in resize_images:
						resize_data[image_resize['name']] = {
							'type': 'resize',
							'path': image_import_path,
							'params': {
								'desc': self.add_prefix_path(self._get_image_path_generate(id_image, image_resize['name']), self._notice['target']['config']['image_product']),
								'width': image_resize['width'],
								'height': image_resize['height']
							}
						}
						if image_resize['name'] == "small_default":
							resize_data['product_mini'] = {
								'type': 'resize',
								'path': image_import_path,
								'params': {
									'desc': '/img/tmp/product_mini_' + to_str(id_image) + '.jpg',
									'width': image_resize['width'],
									'height': image_resize['height']
								}
							}
					if resize_data:
						import_image_resize = self.get_connector_data(url_image, {
							'images': json.dumps(
								resize_data
							)
						})

		if convert['images']:
			for key, image in enumerate(convert['images']):
				if image['path'] or image['url']:
					image_process = self.process_image_before_import(image['url'], image['path'])
					if get_row_from_list_by_field(image_imported, 'url', image_process['url']):
						continue
					image_order += 1
					id_image_exist = self.get_id_image(image_process['url'], previous_image)
					# if id_image_exist:
					# 	id_image = id_image_exist
					# else:
					image_data = {
						'id_product': product_id,
						'position': image_order,
					}
					id_image = self.import_data_connector(self.create_insert_query_connector('image', image_data), 'product')
					if id_image:
						if id_image_exist:
							all_queries.append(self.create_delete_query_connector('image_shop', {'id_image': id_image}))
							all_queries.append(self.create_delete_query_connector('image_lang', {'id_image': id_image}))
						image_imported.append({
							'id_image': id_image,
							'url': image_process['url']
						})
						image_shop_data = {
							'id_product': product_id,
							'id_image': id_image,
							'id_shop': self._notice['target']['shop_default'],
							'cover': None,
						}
						all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
						if self._notice['support']['site_map'] and 'store_ids' in convert:
							for src_store in convert['store_ids']:
								# for src_store, target_store in self._notice['map']['site']:
								if src_store in self._notice['map']['site']:
									if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
										continue
									image_shop_data = {
										'id_product': product_id,
										'id_image': id_image,
										'id_shop': self._notice['map']['site'][src_store],
										'cover': None,
									}
									all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
						for src_language, target_language in self._notice['map']['languages'].items():
							image_lang_data = {
								'id_image': id_image,
								'id_lang': target_language,
							}
							all_query.append(self.create_insert_query_connector('image_lang', image_lang_data))
						if not id_image_exist:
							img_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(self._get_image_path(id_image), self._notice['target']['config']['image_product']))
						else:
							img_import_path = self.move_image_connector(image_process, self.add_prefix_path(self._get_image_path(id_image), self._notice['target']['config']['image_product']), self.add_prefix_path(self._get_image_path(id_image_exist), self._notice['target']['config']['image_product']))

						if img_import_path:
							if not id_image_exist:
								self.insert_map(self.TYPE_IMAGE, convert['id'], id_image, convert['code'], None, image_process['url'])
							else:
								self.update_map(self.TYPE_IMAGE, convert['id'], convert['code'], id_image)

							resize_img_data = dict()
							for image_resize in resize_images:
								resize_img_data[image_resize['name']] = {
									'type': 'resize',
									'path': img_import_path,
									'params': {
										'desc': self.add_prefix_path(self._get_image_path_generate(id_image, image_resize['name']), self._notice['target']['config']['image_product']),
										'width': image_resize['width'],
										'height': image_resize['height']
									}
								}
							if resize_img_data:
								import_images_resize = self.get_connector_data(url_image, {
									'images': json.dumps(resize_img_data)
								})
							del resize_img_data

		if convert['categories']:
			for category in convert['categories']:
				id_category = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], category['code'])
				if not id_category:
					id_category = 0
				category_product_data = {
					'id_category': id_category,
					'id_product': product_id,
				}
				all_query.append(self.create_insert_query_connector('category_product', category_product_data))

		if convert['special_price']['price'] and (convert['special_price']['end_date'] == '0000-00-00' or convert['special_price']['end_date'] == '0000-00-00 00:00:00' or convert['special_price']['end_date'] == '' or convert['special_price']['end_date'] == None or convert['special_price']['end_date'] > get_current_time()):
			specific_price_data = {
				'id_specific_price_rule': 0,
				'id_cart': 0,
				'id_product': product_id,
				'id_shop_group': 0,
				'id_currency': 0,
				'id_country': 0,
				'id_group': 0,
				'id_customer': 0,
				'id_product_attribute': 0,
				'price': '-1.000000',
				'from_quantity': 1,
				'reduction': to_decimal(convert['price']) - to_decimal(convert['special_price']['price']),
				'reduction_type': 'amount',
				'from': convert['special_price']['start_date'] if convert['special_price']['start_date'] else '0000-00-00 00:00:00',
				'to': convert['special_price']['end_date'] if convert['special_price']['end_date'] else '0000-00-00 00:00:00',
			}
			all_query.append(self.create_insert_query_connector('specific_price', specific_price_data))

		if convert['tier_prices']:
			for tier_prices in convert['tier_prices']:
				specific_price_data = {
					'id_specific_price_rule': 0,
					'id_cart': 0,
					'id_product': product_id,
					'id_shop_group': 0,
					'id_currency': 0,
					'id_country': 0,
					'id_group': 0,
					'id_customer': 0,
					'id_product_attribute': 0,
					'price': '-1.000000',
					'from_quantity': tier_prices['qty'] if tier_prices['qty'] else 1,
					'reduction': to_decimal(convert['price']) - to_decimal(tier_prices['price']),
					'reduction_type': 'amount',
					'from': tier_prices['start_date'] if tier_prices['start_date'] else '0000-00-00 00:00:00',
					'to': tier_prices['end_date'] if tier_prices['end_date'] else '0000-00-00 00:00:00',
				}
				all_query.append(self.create_insert_query_connector('specific_price', specific_price_data))

		if convert['group_prices']:
			for group_price in convert['group_prices']:
				if group_price['price']:
					specific_price_data = {
						'id_specific_price_rule': 0,
						'id_cart': 0,
						'id_product': product_id,
						'id_shop_group': 0,
						'id_currency': 0,
						'id_country': 0,
						'id_group': self.get_map_customer_group(group_price['customer_group_id']),
						'id_customer': 0,
						'id_product_attribute': 0,
						'price': '-1.000000',
						'from_quantity': 1,
						'reduction': to_decimal(convert['price']) - to_decimal(group_price['price']),
						'reduction_type': 'amount',
						'from': convert['special_price']['start_date'] if convert['special_price']['start_date'] else '0000-00-00 00:00:00',
						'to': convert['special_price']['end_date'] if convert['special_price']['end_date'] else '0000-00-00 00:00:00',
					}
					all_query.append(self.create_insert_query_connector('specific_price', specific_price_data))

		if convert['attributes']:
			for key, product_attribute_data in enumerate(convert['attributes']):
				if not product_attribute_data['option_value_name'] or product_attribute_data['option_value_name'] == '':
					continue
				option_code = product_attribute_data['option_code'] if product_attribute_data['option_code'] and product_attribute_data['option_code'] != '' else self.convert_option_name_to_code(product_attribute_data['option_name'])
				id_feature = self.get_map_field_by_src(self.TYPE_ATTR, product_attribute_data['option_id'], option_code)
				if not id_feature:
					feature_data = {
						'position': key,
					}
					id_feature = self.import_data_connector(self.create_insert_query_connector('feature', feature_data), 'product')
					self.insert_map(self.TYPE_ATTR, product_attribute_data['option_id'], id_feature, option_code)
					feature_shop_data = {
						'id_feature': id_feature,
						'id_shop': self._notice['target']['shop_default']
					}
					all_query.append(self.create_insert_query_connector('feature_shop', feature_shop_data))
					if self._notice['support']['site_map'] and 'store_ids' in convert:
						for src_store in convert['store_ids']:
							# for src_store, target_store in self._notice['map']['site']:
							if src_store in self._notice['map']['site']:
								if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
									continue
								feature_shop_data = {
									'id_feature': id_feature,
									'id_shop': self._notice['map']['site'][src_store]
								}
								all_query.append(self.create_insert_query_connector('feature_shop', feature_shop_data))
					for src_language, target_language in self._notice['map']['languages'].items():
						feature_lang = product_attribute_data['option_languages'][src_language] if src_language in product_attribute_data['option_languages'] and product_attribute_data['option_languages'][src_language] else product_attribute_data
						feature_lang_data = {
							'id_feature': id_feature,
							'id_lang': target_language,
							'name': feature_lang['option_name'],
						}
						all_query.append(self.create_insert_query_connector('feature_lang', feature_lang_data))
				option_value_code = product_attribute_data['option_value_code'] if product_attribute_data['option_value_code'] else self.convert_option_name_to_code(product_attribute_data['option_value_name'])
				id_feature_value = self.get_map_field_by_src(self.TYPE_ATTR_VALUE, product_attribute_data['option_value_id'], option_value_code)
				if not id_feature_value:
					feature_value_data = {
						'id_feature': id_feature,
						'custom': 0,
					}
					if product_attribute_data['option_type'] == 'text':
						feature_value_data = {
							'id_feature': id_feature,
							'custom': 1,
						}
					id_feature_value = self.import_data_connector(self.create_insert_query_connector('feature_value', feature_value_data), 'product')
					self.insert_map(self.TYPE_ATTR_VALUE, product_attribute_data['option_value_id'], id_feature_value, option_value_code)
					for src_language, target_language in self._notice['map']['languages'].items():
						feature_value_lang = product_attribute_data['option_value_languages'][src_language] if src_language in product_attribute_data['option_value_languages'] and product_attribute_data['option_value_languages'][src_language] else product_attribute_data
						feature_value_lang_data = {
							'id_feature_value': id_feature_value,
							'id_lang': target_language,
							'value': feature_value_lang['option_value_name'],
						}
						all_query.append(self.create_insert_query_connector('feature_value_lang', feature_value_lang_data))
				feature_product_data = {
					'id_feature': id_feature,
					'id_product': product_id,
					'id_feature_value': id_feature_value,
				}
				all_query.append(self.create_insert_query_connector('feature_product', feature_product_data))

		if self.get_migrate_product_extend_config():
			children_list = list()
			if convert['children']:
				children_list = convert['children']
			elif convert['options']:
				if self.count_child_from_option(convert['options']) <= self.VARIANT_LIMIT:
					children_list = self.convert_option_to_child(convert['options'], convert)
				else:
					self.log('product id ' + to_str(convert['id']) + 'too much variant')
			if children_list and to_len(children_list) > self.VARIANT_LIMIT:
				self.log('product id ' + to_str(convert['id']) + 'too much variant')
				children_list = list()

			if children_list:
				for key, children_data in enumerate(children_list):
					id_product_attribute = self.get_map_field_by_src(self.TYPE_CHILD, children_data['id'], children_data['code'])
					if not id_product_attribute:
						product_attribute_data = {
							'id_product': product_id,
							'reference': children_data['sku'],
							'price': to_decimal(children_data['price']) - to_decimal(convert['price']),
							'quantity': children_data['qty'] if 'manage_stock' in children_data and children_data['manage_stock'] else 9999,
							'weight': to_decimal(children_data['weight']) - to_decimal(convert['weight']),
							'available_date': get_value_by_key_in_dict(children_data, 'date_available', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
						}
						if children_data.get('default'):
							product_attribute_data['default_on'] = 1
						id_product_attribute = self.import_data_connector(self.create_insert_query_connector('product_attribute', product_attribute_data), 'product')
						self.insert_map(self.TYPE_CHILD, children_data['id'], id_product_attribute, children_data['code'])
						product_attribute_shop_data = {
							'id_product': product_id,
							'id_product_attribute': id_product_attribute,
							'id_shop': self._notice['target']['shop_default'],
							'price': to_decimal(children_data['price']) - to_decimal(convert['price']),
							'weight': to_decimal(children_data['weight']) - to_decimal(convert['weight']),
							'available_date': get_value_by_key_in_dict(children_data, 'date_available', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
						}
						if children_data.get('default'):
							product_attribute_shop_data['default_on'] = 1
						all_query.append(self.create_insert_query_connector('product_attribute_shop', product_attribute_shop_data))
						if self._notice['support']['site_map'] and 'store_ids' in convert:
							for src_store in convert['store_ids']:
								# for src_store, target_store in self._notice['map']['site']:
								if src_store in self._notice['map']['site']:
									# if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
									# 	continue
									product_attribute_shop_data = {
										'id_product': product_id,
										'id_product_attribute': id_product_attribute,
										'id_shop': self._notice['map']['site'][src_store],
										'price': to_decimal(children_data['price']) - to_decimal(convert['price']),
										'weight': to_decimal(children_data['weight']) - to_decimal(convert['weight']),
										'available_date': get_value_by_key_in_dict(children_data, 'date_available', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
									}
									if children_data.get('default'):
										product_attribute_shop_data['default_on'] = 1
									all_query.append(self.create_insert_query_connector('product_attribute_shop', product_attribute_shop_data))
						if children_data['special_price']['price'] and (children_data['special_price']['end_date'] == '0000-00-00' or children_data['special_price']['end_date'] == '0000-00-00 00:00:00' or children_data['special_price']['end_date'] == '' or children_data['special_price']['end_date'] == None or children_data['special_price']['end_date'] > get_current_time()):
							specific_price_data = {
								'id_specific_price_rule': 0,
								'id_cart': 0,
								'id_product': product_id,
								'id_shop_group': 0,
								'id_currency': 0,
								'id_country': 0,
								'id_group': 0,
								'id_customer': 0,
								'id_product_attribute': id_product_attribute,
								'price': '-1.000000',
								'from_quantity': 1,
								'reduction': to_decimal(children_data['price']) - to_decimal(children_data['special_price']['price']),
								'reduction_type': 'amount',
								'from': children_data['special_price']['start_date'] if children_data['special_price']['start_date'] else get_current_time(),
								'to': children_data['special_price']['end_date'] if children_data['special_price']['end_date'] else get_current_time(),
							}
							all_query.append(self.create_insert_query_connector('specific_price', specific_price_data))
						stock_available_data = {
							'id_product': product_id,
							'id_product_attribute': id_product_attribute,
							'id_shop': self._notice['target']['shop_default'],
							'id_shop_group': 0,
							'quantity': children_data['qty'] if 'manage_stock' in children_data and children_data['manage_stock'] else 9999,
						}
						all_query.append(self.create_insert_query_connector('stock_available', stock_available_data))
						if self._notice['support']['site_map'] and 'store_ids' in convert:
							for src_store in children_data['store_ids']:
								# for src_store, target_store in self._notice['map']['site']:
								if src_store in self._notice['map']['site']:
									if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
										continue
									stock_available_data = {
										'id_product': product_id,
										'id_product_attribute': id_product_attribute,
										'id_shop': self._notice['map']['site'][src_store],
										'id_shop_group': 0,
										'quantity': children_data['qty'] if 'manage_stock' in children_data and children_data['manage_stock'] else 9999,
									}
									all_query.append(
										self.create_insert_query_connector('stock_available', stock_available_data))

						if children_data['thumb_image']['path'] or children_data['thumb_image']['url']:
							image_process = self.process_image_before_import(children_data['thumb_image']['url'], children_data['thumb_image']['path'])
							id_image = get_row_value_from_list_by_field(image_imported, 'url', image_process['url'], 'id_image')
							if not id_image:
								image_order += 1
								image_data = {
									'id_product': product_id,
									'position': image_order,
									'cover': None,
								}
								id_image = self.import_data_connector(self.create_insert_query_connector('image', image_data), 'product')
								image_imported.append({
									'id_image': id_image,
									'url': image_process['url']
								})
								image_shop_data = {
									'id_product': product_id,
									'id_image': id_image,
									'id_shop': self._notice['target']['shop_default'],
									'cover': None,
								}
								all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
								if self._notice['support']['site_map'] and 'store_ids' in convert:
									for src_store in children_data['store_ids']:
										# for src_store, target_store in self._notice['map']['site']:
										if src_store in self._notice['map']['site']:
											if self._notice['map']['site'][src_store] == self._notice['target'][
												'shop_default']:
												continue
											image_shop_data = {
												'id_product': product_id,
												'id_image': id_image,
												'id_shop': self._notice['map']['site'][src_store],
												'cover': None,
											}
											all_query.append(self.create_insert_query_connector('image_shop', image_shop_data))
								for src_language, target_language in self._notice['map']['languages'].items():
									image_lang_data = {
										'id_image': id_image,
										'id_lang': target_language,
									}
									all_query.append(self.create_insert_query_connector('image_lang', image_lang_data))
								image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(self._get_image_path(id_image), self._notice['target']['config']['image_product']))

								resize_data = dict()
								for image_resize in resize_images:
									resize_data[image_resize['name']] = {
										'type': 'resize',
										'path': image_import_path,
										'params': {
											'desc': self.add_prefix_path(self._get_image_path_generate(id_image, image_resize['name']), self._notice['target']['config']['image_product']),
											'width': image_resize['width'],
											'height': image_resize['height']
										}
									}
								if resize_data:
									import_image_resize = self.get_connector_data(url_image, {
										'images': json.dumps(resize_data)
									})

							if id_image:
								product_attribute_image_data = {
									'id_product_attribute': id_product_attribute,
									'id_image': id_image
								}
								all_query.append(self.create_insert_query_connector('product_attribute_image', product_attribute_image_data))
					if children_data['attributes']:
						for key, childen_product_option_data in enumerate(children_data['attributes']):
							childen_option_code = childen_product_option_data['option_code'] if childen_product_option_data['option_code'] else self.convert_option_name_to_code(childen_product_option_data['option_name'])
							id_attribute_group = self.get_map_field_by_src(self.TYPE_OPTION, childen_product_option_data['option_id'], childen_option_code)
							if not id_attribute_group:
								option_type = 'select'
								if childen_product_option_data['option_type'] and childen_product_option_data['option_type'] == self.OPTION_RADIO:
									option_type = 'radio'
								elif childen_product_option_data['option_type'] and childen_product_option_data['option_type'] == 'color':
									option_type = 'color'
								else:
									option_type = 'select'
								attribute_group_data = {
									'is_color_group': 1 if childen_product_option_data['option_type'] == 'color' else 0,
									'group_type': option_type,
									'position': self.get_position('attribute_group'),
								}
								id_attribute_group = self.import_data_connector(self.create_insert_query_connector('attribute_group', attribute_group_data), 'product')
								self.insert_map(self.TYPE_OPTION, childen_product_option_data['option_id'], id_attribute_group, childen_option_code)
								attribute_group_shop_data = {
									'id_attribute_group': id_attribute_group,
									'id_shop': self._notice['target']['shop_default'],
								}
								all_query.append(self.create_insert_query_connector('attribute_group_shop', attribute_group_shop_data))
								if self._notice['support']['site_map'] and 'store_ids' in convert:
									for src_store in children_data['store_ids']:
										# for src_store, target_store in self._notice['map']['site']:
										if src_store in self._notice['map']['site']:
											if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
												continue
											attribute_group_shop_data = {
												'id_attribute_group': id_attribute_group,
												'id_shop': self._notice['map']['site'][src_store],
											}
											all_query.append(self.create_insert_query_connector('attribute_group_shop', attribute_group_shop_data))

								for src_language, target_language in self._notice['map']['languages'].items():
									if childen_product_option_data['option_languages']:
										product_attribute_lang_data = childen_product_option_data['option_languages'][src_language] if src_language in childen_product_option_data['option_languages'] and childen_product_option_data['option_languages'][src_language] else childen_product_option_data
									else:
										product_attribute_lang_data = childen_product_option_data
									attribute_group_lang_data = {
										'id_attribute_group': id_attribute_group,
										'id_lang': target_language,
										'name': product_attribute_lang_data['option_name'],
										'public_name': product_attribute_lang_data['option_name'],
									}
									all_query.append(self.create_insert_query_connector('attribute_group_lang', attribute_group_lang_data))
							option_value_code = childen_product_option_data['option_value_code'] + childen_option_code if childen_product_option_data['option_value_code'] else self.convert_option_name_to_code(childen_product_option_data['option_value_name']) + childen_option_code
							id_attribute = self.get_map_field_by_src(self.TYPE_OPTION_VALUE, childen_product_option_data['option_value_id'], option_value_code)
							if not id_attribute:
								attribute_data = {
									'id_attribute_group': id_attribute_group,
									'color': '',
									'position': self.get_position('attribute'),
								}
								id_attribute = self.import_data_connector(self.create_insert_query_connector('attribute', attribute_data), 'product')
								self.insert_map(self.TYPE_OPTION_VALUE, childen_product_option_data['option_value_id'], id_attribute, option_value_code)
								attribute_shop_data = {
									'id_attribute': id_attribute,
									'id_shop': self._notice['target']['shop_default'],
								}
								all_query.append(self.create_insert_query_connector('attribute_shop', attribute_shop_data))
								if self._notice['support']['site_map'] and 'store_ids' in convert:
									for src_store in children_data['store_ids']:
										# for src_store, target_store in self._notice['map']['site']:
										if src_store in self._notice['map']['site']:
											if self._notice['map']['site'][src_store] == self._notice['target']['shop_default']:
												continue
											attribute_shop_data = {
												'id_attribute': id_attribute,
												'id_shop': self._notice['map']['site'][src_store],
											}
											all_query.append(self.create_insert_query_connector('attribute_shop', attribute_shop_data))

								for src_language, target_language in self._notice['map']['languages'].items():
									if childen_product_option_data['option_value_languages']:
										product_attribute_value_lang_data = childen_product_option_data['option_value_languages'][src_language] if src_language in childen_product_option_data['option_value_languages'] and childen_product_option_data['option_value_languages'][src_language] else childen_product_option_data
									else:
										product_attribute_value_lang_data = childen_product_option_data
									attribute_lang_data = {
										'id_attribute': id_attribute,
										'id_lang': target_language,
										'name': product_attribute_value_lang_data['option_value_name'],
									}
									all_query.append(self.create_insert_query_connector('attribute_lang', attribute_lang_data))
							product_attribute_combination_data = {
								'id_attribute': id_attribute,
								'id_product_attribute': id_product_attribute,
							}
							all_query.append(self.create_insert_query_connector('product_attribute_combination', product_attribute_combination_data))
							layered_product_attribute_data = {
								'id_attribute': id_attribute,
								'id_product': product_id,
								'id_attribute_group': id_attribute_group,
								'id_shop': self._notice['target']['shop_default']
							}
							all_query.append(self.create_insert_query_connector('layered_product_attribute', layered_product_attribute_data))
							if self._notice['support']['site_map']:
								for src_store in convert['store_ids']:
									# for src_store, target_store in self._notice['map']['site']:
									if src_store in self._notice['map']['site']:
										if self._notice['map']['site'][src_store] == self._notice['target'][
											'shop_default']:
											continue
										layered_product_attribute_data = {
											'id_attribute': id_attribute,
											'id_product': product_id,
											'id_attribute_group': id_attribute_group,
											'id_shop': self._notice['map']['site'][src_store]
										}
										all_query.append(self.create_insert_query_connector('layered_product_attribute', layered_product_attribute_data))
			# todo: group product
			if 'group_child_ids' in convert and convert['group_child_ids']:
				for group_prd in convert['group_child_ids']:
					id_prd_group = self.get_map_field_by_src(self.TYPE_PRODUCT, group_prd['id'])
					if id_prd_group:
						pack_data = {
							'id_product_pack': product_id,
							'id_product_item': id_prd_group,
							'id_product_attribute_item': 0,
							'quantity': get_value_by_key_in_dict(group_prd, 'qty', 1),
						}
						all_query.append(self.create_insert_query_connector('pack', pack_data))

			if 'group_parent_ids' in convert and convert['group_parent_ids']:
				for parent_group in convert['group_parent_ids']:
					id_parent_group = self.get_map_field_by_src(self.TYPE_PRODUCT, parent_group)
					if id_parent_group:
						pack_data = {
							'id_product_pack': id_parent_group,
							'id_product_item': product_id,
							'id_product_attribute_item': 0,
							'quantity': get_value_by_key_in_dict(parent_group, 'qty', 1),
						}
						all_query.append(self.create_insert_query_connector('pack', pack_data))

		if 'children' in convert['relate'] and convert['relate']['children']:
			for relate_child in convert['relate']['children']:
				if relate_child['type'] in [self.PRODUCT_CROSS, self.PRODUCT_RELATE]:
					id_child_relate = self.get_map_field_by_src(self.TYPE_PRODUCT, relate_child['id'], relate_child['code'])
					if not id_child_relate:
						product_relate_child_data = {
							'product_id': product_id,
							'product_relate_code': relate_child['code'],
							'product_relate_id': relate_child['id'],
						}
						self.product_relates.append(product_relate_child_data)
					if id_child_relate:
						accessory_data = {
							'id_product_1': product_id,
							'id_product_2': id_child_relate,
						}
						all_query.append(self.create_insert_query_connector('accessory', accessory_data))

		if 'parent' in convert['relate'] and convert['relate']['parent']:
			for relate_parent in convert['relate']['parent']:
				if relate_parent['type'] in [self.PRODUCT_CROSS, self.PRODUCT_RELATE]:
					id_parent_relate = self.get_map_field_by_src(self.TYPE_PRODUCT, relate_parent['id'], relate_parent['code'])
					if not id_parent_relate:
						product_relate_parent_data = {
							'product_id': product_id,
							'product_relate_code': relate_parent['code'],
							'product_relate_id': relate_parent['id'],
						}
						self.product_relates.append(product_relate_parent_data)
					if id_parent_relate:
						accessory_data = {
							'id_product_1': id_parent_relate,
							'id_product_2': product_id,
						}
						all_query.append(self.create_insert_query_connector('accessory', accessory_data))

		for product_rela in self.product_relates:
			if to_int(convert['id']) == to_int(product_rela['product_relate_id']):
				accessory_data = {
					'id_product_1': product_rela['product_id'],
					'id_product_2': product_id
				}
				all_queries.append(self.create_insert_query_connector('accessory', accessory_data))

		if self._notice['config']['seo'] or self._notice['config']['seo_301']:
			for seo_url in convert['seo']:
				leurlrewrite = {
					'link_rewrite': seo_url['request_path'],
					'id_desc': product_id,
					'type': 'product',
					'lang_code': self._notice['target']['language_default']
				}
				seourl_query = self.create_insert_query_connector("lecm_rewrite", leurlrewrite)
				all_query.append(seourl_query)

		if all_query:
			self.import_multiple_data_connector(all_query, 'product')
		del all_query
		return response_success()

	def addition_product_import(self, convert, product, products_ext):
		return response_success()

	# TODO: CUSTOMER

	def prepare_customers_export(self):
		return self

	def prepare_customers_import(self):
		delete_query = {
			'type': 'query',
			'query': "DELETE FROM `_DBPRF_configuration` WHERE name = 'LEPP_TYPE' OR name = 'LEPP_URL'"
		}
		config_delete = self.import_data_connector(delete_query)
		all_queries = list()
		config_data = {
			'name': 'LEPP_URL',
			'date_add': get_current_time(),
			'date_upd': get_current_time(),
			'value': self._notice['src']['cart_url']
		}
		url_query = self.create_insert_query_connector('configuration', config_data)
		all_queries.append(url_query)
		config_data = {
			'name': 'LEPP_TYPE',
			'date_add': get_current_time(),
			'date_upd': get_current_time(),
			'value': self._notice['src']['cart_type']
		}
		type_query = self.create_insert_query_connector('configuration', config_data)
		all_queries.append(type_query)
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'customer')
		return self

	def get_customers_main_export(self):
		id_src = self._notice['process']['customers']['id_src']
		limit = self._notice['setting']['customers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_customer WHERE deleted = 0 AND id_customer > " + to_str(id_src) + " ORDER BY id_customer ASC LIMIT " + to_str(limit)
			# 'query': "SELECT * FROM _DBPRF_customer WHERE id_customer = 1326 "
		}
		# customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		customers = self.select_data_connector(query, 'customers')
		if not customers or customers['result'] != 'success':
			return response_error('could not get customers main to export')
		return customers

	def get_customers_ext_export(self, customers):
		url_query = self.get_connector_url('query')
		customer_ids = duplicate_field_value_from_list(customers['data'], 'id_customer')
		customer_ext_queries = {
			'address': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_address WHERE deleted = 0 AND id_customer IN " + self.list_to_in_condition(customer_ids)
			},
			'group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group WHERE id_customer IN " + self.list_to_in_condition(
					customer_ids)
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
		customer_data['store_id'] = customer['id_shop']
		customer_data['username'] = customer['email']
		customer_data['email'] = customer['email']
		customer_data['password'] = customer['passwd'] + ":" + self._notice['src']['config']['cookie_key']
		customer_data['first_name'] = customer['firstname']
		customer_data['last_name'] = customer['lastname']
		customer_data['gender'] = customer['id_gender']
		customer_data['dob'] = convert_format_time(customer['birthday'], '%Y-%m-%d')
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
				address_data['telephone'] = address_book['phone_mobile'] if address_book['phone'] == '' or address_book['phone'] == ' ' else address_book['phone']
				address_data['company'] = address_book['company']
				address_data['fax'] = ''

				country = get_row_from_list_by_field(customers_ext['data']['country'], 'id_country', address_book['id_country'])
				if country:
					address_data['country']['id'] = country['id_country']
					address_data['country']['country_code'] = country['iso_code']
					address_data['country']['name'] = country['name']
				# else:
				# 	address_data['country']['country_code'] = 'US'
				# 	address_data['country']['name'] = 'United States'

				state = get_row_from_list_by_field(customers_ext['data']['state'], 'id_state', address_book['id_state'])
				if state:
					address_data['state']['id'] = state['id_state']
					address_data['state']['state_code'] = state['iso_code']
					address_data['state']['name'] = state['name']
				# else:
				# 	address_data['state']['state_code'] = 'AL'
				# 	address_data['state']['name'] = 'Alabama'

				if address_data['telephone'] and address_data['telephone'] != '' and address_data['telephone'] != ' ':
					address_data['default']['shipping'] = True
					address_data['default']['billing'] = True

				customer_data['address'].append(address_data)
		return response_success(customer_data)

	def get_customer_id_import(self, convert, customer, customers_ext):
		return customer['id_customer']

	def check_customer_import(self, convert, customer, customers_ext):
		return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id'], convert['code']) else False

	def router_customer_import(self, convert, customer, customers_ext):
		return response_success('customer_import')

	def before_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def customer_import(self, convert, customer, customers_ext):
		id_group = None
		if 'group_id' in convert and convert['group_id']:
			id_group = self.get_map_customer_group(convert['group_id'])
		if not id_group:
			group = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'select',
					'query': "SELECT * FROM _DBPRF_group_lang WHERE name LIKE 'Customer'",
				})
			})
			if group and group['data']:
				id_group = group['data'][0]['id_group']
			else:
				id_group = 1
		if convert['gender'] == self.GENDER_MALE:
			gender = 1
		elif convert['gender'] == self.GENDER_FEMALE:
			gender = 2
		else:
			gender = 0
		customer_data = {
			'id_default_group': id_group,
			'id_gender': gender,
			'id_lang': self._notice['target']['language_default'],
			'firstname': convert['first_name'] if convert['first_name'] else '',
			'lastname': convert['last_name'] if convert['last_name'] else '',
			'email': convert['email'],
			'passwd': convert['password'] if convert['password'] else '',
			'last_passwd_gen': get_current_time(),
			'birthday': convert['dob'] if convert['dob'] else None,
			'newsletter': 1 if convert['is_subscribed'] else 0,
			'date_add': convert['created_at'] if to_str(convert['created_at']) != '' else get_current_time(),
			'date_upd': convert['updated_at'] if to_str(convert['updated_at']) != '' else get_current_time(),
			'active': 1 if convert['active'] else 0,
		}
		if self._notice['config']['pre_cus']:
			self.delete_target_customer(convert['id'])
			customer_data['id_customer'] = convert['id']
		id_customer = self.import_customer_data_connector(self.create_insert_query_connector('customer', customer_data), True, convert['id'])
		if id_customer:
			self.insert_map(self.TYPE_CUSTOMER, convert['id'], id_customer, convert['code'])
			self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "UPDATE _DBPRF_customer SET secure_key = md5(date_format(date_add( sysdate(), INTERVAL FLOOR( 1 + (RAND() * 998)) MICROSECOND),'%Y%m%d%H%i%s%f')) WHERE secure_key = -1"
				})
			})
		else:
			return response_error(self.warning_import_entity(self.TYPE_CUSTOMER, convert['id'], convert['code']))
		all_query = list()
		customer_group_data = {
			'id_customer': id_customer,
			'id_group': id_group,
		}
		all_query.append(self.create_insert_query_connector('customer_group', customer_group_data))
		if all_query:
			self.import_multiple_data_connector(all_query, 'customer')
		del all_query
		return response_success(id_customer)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
		if convert['address']:
			for key, address_data in enumerate(convert['address']):
				id_address = self.get_map_field_by_src(self.TYPE_ADDRESS, to_int(address_data['id']), address_data['code'])
				if not id_address:
					# 	zone_state = address_data['state']
					# 	if zone_state['id'] or zone_state['state_code']:
					# 		state = self.get_connector_data(self.get_connector_url('query'), {
					# 			'query': json.dumps({
					# 				'type': 'select',
					# 				'query': "SELECT id_state FROM _DBPRF_state WHERE iso_code LIKE '" + to_str(zone_state['state_code']) + "'"
					# 			})
					# 		})
					# 		if state and state['data']:
					# 			id_state = state['data'][0]['id_state']
					# 		else:
					# 			if not zone_state['id']:
					# 				id_state = 0
					# 			else:
					# 				id_state = zone_state['id']
					# 	else:
					# 		id_state = 0
					#
					# 	zone_country = address_data['country']
					# 	if zone_country['id'] or zone_country['country_code']:
					# 		country = self.get_connector_data(self.get_connector_url('query'), {
					# 			'query': json.dumps({
					# 				'type': 'select',
					# 				'query': "SELECT id_country FROM _DBPRF_country WHERE iso_code LIKE '" + to_str(zone_country['country_code']) + "'"
					# 			})
					# 		})
					# 		if country and country['data']:
					# 			id_country = country['data'][0]['id_country']
					# 		else:
					# 			if not zone_country['id']:
					# 				id_country = 0
					# 			else:
					# 				id_country = zone_country['id']
					# 	else:
					# 		id_country = 0
					# 	address_data_insert = {
					# 		'id_country': id_country,
					# 		'id_state': id_state,
					# 		'id_customer': customer_id,
					# 		'alias': 'My address',
					# 		'company': address_data['company'],
					# 		'lastname': address_data['last_name'] if address_data['last_name'] else '',
					# 		'firstname': address_data['first_name'] if address_data['first_name'] else '',
					# 		'address1': address_data['address_1'],
					# 		'address2': address_data['address_2'],
					# 		'postcode': address_data['postcode'],
					# 		'city': address_data['city'],
					# 		'phone': address_data['telephone'],
					# 		'phone_mobile': address_data['telephone'],
					# 		'vat_number': address_data['vat_number'] if 'vat_number' in address_data else '',
					# 		'date_add': address_data['created_at'] if address_data['created_at'] else get_current_time(),
					# 		'date_upd': address_data['updated_at'] if address_data['updated_at'] else get_current_time(),
					# 		'active': 1,
					# 	}
					# 	id_address = self.import_data_connector(self.create_insert_query_connector('address', address_data_insert), 'customer')
					# 	if id_address:
					# 		self.insert_map(self.TYPE_ADDRESS, address_data['id'], id_address, address_data['code'])
					self.insert_address(customer_id, address_data)

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
			'customer_thread': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_thread WHERE id_order IN " + self.list_to_in_condition(order_ids)
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
		customer_thread_ids = duplicate_field_value_from_list(orders_ext['data']['customer_thread'], 'id_customer_thread')
		orders_ext_rel_queries = {
			'country': {
				'type': 'select',
				'query': "SELECT c.*, cl.* FROM _DBPRF_country AS c LEFT JOIN _DBPRF_country_lang AS cl ON cl.id_country = c.id_country WHERE c.id_country IN " + self.list_to_in_condition(country_ids)
			},
			'state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_state WHERE id_state IN " + self.list_to_in_condition(state_ids)
			},
			'customer_message': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_message WHERE id_customer_thread IN " + self.list_to_in_condition(customer_thread_ids)
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
		if order_status:
			for order_stt in order_status:
				order_status_id = order_stt['id_order_state']
		else:
			order_status_id = 1
		order_data['id'] = order['id_order']
		order_data['order_number'] = order['reference'] if 'reference' in order else None
		order_data['status'] = order_status_id
		order_data['store_id'] = order['id_shop']
		order_data['tax']['title'] = 'Taxes'
		order_data['tax']['amount'] = to_decimal(order['total_paid_tax_incl']) - to_decimal(order['total_paid_tax_excl'])
		order_data['shipping']['title'] = 'Shipping'
		order_data['shipping']['amount'] = order['total_shipping']
		order_data['discount']['title'] = 'Discount products'
		order_data['discount']['amount'] = order['total_discounts']
		order_data['subtotal']['title'] = 'Total products'
		order_data['subtotal']['amount'] = order['total_products']
		order_data['total']['title'] = 'Total'
		order_data['total']['amount'] = order['total_paid']

		currency_ps = get_row_from_list_by_field(orders_ext['data']['currency'], 'id_currency', order['id_currency'])
		order_data['currency'] = currency_ps.get('iso_code')
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
			order_billing['telephone'] = billing_address['phone'] if billing_address['phone'] and billing_address['phone'] != '' and billing_address['phone'] != ' ' else billing_address['phone_mobile']
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
			order_delivery['telephone'] = delivery_address['phone'] if delivery_address['phone'] and delivery_address['phone'] != '' and delivery_address['phone'] != ' ' else delivery_address['phone_mobile']
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
			order_item['price'] = order_product['unit_price_tax_incl']
			order_item['original_price'] = order_product['unit_price_tax_excl']
			order_item['tax_amount'] = (to_decimal(order_product['total_price_tax_incl']) - to_decimal(order_product['total_price_tax_excl']))
			order_item['tax_percent'] = ''
			order_item['discount_amount'] = '0.0000'
			order_item['discount_percent'] = '0.0000'
			order_item['subtotal'] = to_decimal(order_product['total_price_tax_excl'])
			order_item['total'] = to_decimal(order_product['total_price_tax_excl'])
			if product_name and to_len(product_name) > 1:
				order_item_options = list()
				order_product_attributes = to_str(product_name[1]).split(",")
				for order_product_attribute in order_product_attributes:
					order_item_option = self.construct_order_item_option()
					option_prod = to_str(order_product_attribute).split(":")
					order_item_option['option_name'] = option_prod[0] if option_prod[0] else ''
					order_item_option['option_value_name'] = option_prod[1] if option_prod and to_len(option_prod) > 1 else ''
					order_item_option['price'] = ''
					order_item_option['price_prefix'] = ''
					order_item_options.append(order_item_option)
				order_item['options'] = order_item_options
			order_items.append(order_item)
		order_data['items'] = order_items

		# message_ps = get_list_from_list_by_field(orders_ext['data']['message'], 'id_order', order['id_order'])
		# order_history = list()
		# if message_ps:
		# 	for message in message_ps:
		# 		order_history = self.construct_order_history()
		# 		order_history['id'] = message['id_message']
		# 		order_history['comment'] = message['message']
		# 		order_history['created_at'] = message['date_add']
		# 		order_data['history'].append(order_history)
		#

		id_customer_thread = get_row_value_from_list_by_field(orders_ext['data']['customer_thread'], 'id_order', order['id_order'], 'id_customer_thread')
		customer_msg = get_list_from_list_by_field(orders_ext['data']['customer_message'], 'id_customer_thread', id_customer_thread)
		order_history = list()
		for customer_m in customer_msg:
			order_history = self.construct_order_history()
			order_history['id'] = customer_m['id_customer_message']
			order_history['status'] = ''
			order_history['comment'] = customer_m['message']
			order_history['notified'] = ''
			order_history['created_at'] = convert_format_time(customer_m['date_add'])
			order_history['updated_at'] = convert_format_time(customer_m['date_upd'])
			order_data['history'].append(order_history)

		return response_success(order_data)

	def get_order_id_import(self, convert, order, orders_ext):
		return order['id_order']

	def check_order_import(self, convert, order, orders_ext):
		return self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code'])

	def update_order_after_demo(self, order_id, convert, order, orders_ext):
		all_queries = list()
		if convert['customer']['id']:
			id_customer = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not id_customer:
				id_customer = 1
		else:
			id_customer = 1
		all_queries.append(self.create_update_query_connector('orders', {'id_customer': id_customer}, {'id_order': order_id}))
		if convert['items']:
			self.import_data_connector(self.create_delete_query_connector('order_detail', {'id_order': order_id}))
			id_shop = self._notice['target']['shop_default']
			if self._notice['support']['site_map'] and 'store_id' in convert and convert['store_id'] in self._notice['map']['site']:
				id_shop = self._notice['map']['site'][convert['store_id']]
			for key, order_item in enumerate(convert['items']):
				if order_item['product']['id'] or order_item['product']['code']:
					id_product = self.get_map_field_by_src(self.TYPE_PRODUCT, order_item['product']['id'], order_item['product']['code'])
					if not id_product:
						id_product = 0
				else:
					id_product = 0
				product_name = ''
				if order_item['options']:
					for order_item_option in order_item['options']:
						product_name = to_str(order_item_option['option_name']) + ' : ' + to_str(order_item_option['option_value_name']) + ', '
				order_detail_data = {
					'id_order': order_id,
					'id_shop': id_shop,
					'product_id': id_product,
					'product_name': to_str(order_item['product']['name']) + ' - ' + to_str(product_name),
					'product_quantity': order_item['qty'],
					'product_quantity_in_stock': order_item['qty'],
					'product_price': to_decimal(order_item['price']),
					'product_reference': order_item['product']['sku'],
					'product_weight': 0.000000,
					'tax_name': '',
					'total_price_tax_incl': to_decimal(order_item['price']) + to_decimal(order_item['tax_amount']),
					'total_price_tax_excl': order_item['price'],
					'unit_price_tax_incl': order_item['price'],
					'unit_price_tax_excl': order_item['price'],
					'original_product_price': order_item['price'],
				}
				all_queries.append(self.create_insert_query_connector('order_detail', order_detail_data))
		self.import_multiple_data_connector(all_queries, 'update_demo_order')
		return response_success()

	def router_order_import(self, convert, order, orders_ext):
		return response_success('order_import')

	def before_order_import(self, convert, order, orders_ext):
		return response_success()

	def order_import(self, convert, order, orders_ext):
		if convert['customer']['id'] or convert['customer']['code']:
			id_customer = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not id_customer:
				id_customer = 1
		else:
			id_customer = 1

		secure_key = False
		if to_int(id_customer) != 1:
			query = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer WHERE id_customer = " + to_str(id_customer)
			}
			customer_data = self.select_data_connector(query, 'order')
			if customer_data and customer_data['data']:
				secure_key = customer_data['data'][0]['secure_key']

		if convert['shipping_address']['id'] or convert['shipping_address']['code']:
			id_address_delivery = self.get_map_field_by_src(self.TYPE_ADDRESS, convert['shipping_address']['id'], convert['shipping_address']['code'])
			if not id_address_delivery:
				id_address_delivery = self.insert_address(id_customer, convert['shipping_address'])
		else:
			id_address_delivery = self.insert_address(id_customer, convert['shipping_address'])
		if convert['billing_address']['id'] or convert['billing_address']['code']:
			id_address_invoice = self.get_map_field_by_src(self.TYPE_ADDRESS, convert['billing_address']['id'], convert['billing_address']['code'])
			if not id_address_invoice:
				id_address_invoice = self.insert_address(id_customer, convert['billing_address'])
		else:
			id_address_invoice = self.insert_address(id_customer, convert['billing_address'])
		updated_at =  convert['updated_at'] if convert['updated_at'] else get_current_time()
		orders_data = {
			'id_carrier': 2,
			'id_lang': self._notice['target']['language_default'],
			'id_customer': id_customer,
			'id_cart': self._notice['target']['language_default'],
			'id_currency': self._notice['target']['currency_default'],
			'id_address_delivery': id_address_delivery,
			'id_address_invoice': id_address_invoice,
			'current_state': self._notice['map']['order_status'].get(convert['status'], 1),
			'payment': convert['payment']['title'] if convert['payment']['title'] else 'cashondelivery',
			'module': 'ps_checkpayment',
			'secure_key': secure_key if secure_key else '-1',
			'total_discounts': convert['discount']['amount'],
			'total_discounts_tax_incl': convert['discount']['amount'],
			'total_discounts_tax_excl': convert['discount']['amount'],
			'total_paid': convert['total']['amount'],
			'total_paid_tax_incl': convert['total']['amount'],
			'total_paid_tax_excl': to_decimal(convert['total']['amount']) - to_decimal(convert['tax']['amount']),
			'total_products': convert['subtotal']['amount'],
			'total_products_wt': convert['subtotal']['amount'],
			'total_shipping': convert['shipping']['amount'],
			'total_shipping_tax_incl': convert['shipping']['amount'],
			'total_shipping_tax_excl': convert['shipping']['amount'],
			'round_mode': 0,
			'round_type': 0,
			'invoice_number': 0,
			'delivery_number': 0,
			'date_add': convert['created_at'] if convert['created_at'] else get_current_time(),
			'date_upd': updated_at,
			'invoice_date': updated_at,
			'delivery_date': updated_at,
			'reference': convert['id'],
		}
		if self._notice['config']['pre_ord'] and self._notice['src'].get('setup_type') != 'api':
			self.delete_target_order(convert['id'])
			orders_data['id_order'] = convert['id']
		id_order = self.import_order_data_connector(self.create_insert_query_connector('orders', orders_data), True, convert['id'])
		if id_order:
			self.insert_map(self.TYPE_ORDER, convert['id'], id_order, convert['code'])
		else:
			return response_error(self.warning_import_entity(self.TYPE_ORDER, convert['id'], convert['code']))
		return response_success(id_order)

	def after_order_import(self, order_id, convert, order, orders_ext):
		if convert['customer']['id']:
			id_customer = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not id_customer:
				id_customer = 1
		else:
			id_customer = 1
		all_query = list()
		order_carrier_data = {
			'id_order': order_id,
			'id_carrier': 2,
			'id_order_invoice': 0,
			'date_add': convert['updated_at'] if convert['updated_at'] else get_current_time(),
		}
		all_query.append(self.create_insert_query_connector('order_carrier', order_carrier_data))
		payment = {
			'order_reference': convert['id'],
			'id_currency': 1,
			'amount': convert['total']['amount'],
			'payment_method': convert['payment']['title'],
			'conversion_rate': 1,
			'date_add': convert['created_at'],
		}
		all_query.append(self.create_insert_query_connector('order_payment', payment))
		id_shop = self._notice['target']['shop_default']
		if self._notice['support']['site_map'] and 'store_id' in convert and convert['store_id'] in self._notice['map']['site']:
			id_shop = self._notice['map']['site'][convert['store_id']]
		if convert['items']:
			for key, order_item in enumerate(convert['items']):
				if order_item['product']['id'] or order_item['product']['code']:
					id_product = self.get_map_field_by_src(self.TYPE_PRODUCT, order_item['product']['id'], order_item['product']['code'])
					if not id_product:
						id_product = 0
				else:
					id_product = 0
				product_name = ''
				if order_item['options']:
					for order_item_option in order_item['options']:
						product_name = to_str(order_item_option['option_name']) + ' : ' + to_str(order_item_option['option_value_name']) + ', '
				order_detail_data = {
					'id_order': order_id,
					'id_shop': id_shop,
					'product_id': id_product,
					'product_name': to_str(order_item['product']['name']) + ' - ' + to_str(product_name),
					'product_quantity': order_item['qty'],
					'product_quantity_in_stock': order_item['qty'],
					'product_price': to_decimal(order_item['price']),
					'product_reference': order_item['product']['sku'],
					'product_weight': 0.000000,
					'tax_name': '',
					'total_price_tax_incl': to_decimal(order_item['price']) + to_decimal(order_item['tax_amount']),
					'total_price_tax_excl': order_item['price'],
					'unit_price_tax_incl': order_item['price'],
					'unit_price_tax_excl': order_item['price'],
					'original_product_price': order_item['price'],
				}
				all_query.append(self.create_insert_query_connector('order_detail', order_detail_data))
		order_history_data = {
			'id_employee': 0,
			'id_order': order_id,
			'id_order_state': self._notice['map']['order_status'].get(convert['status'], 1),
			'date_add': convert['updated_at'] if convert['updated_at'] else get_current_time(),
		}
		all_query.append(self.create_insert_query_connector('order_history', order_history_data))

		if convert['history']:
			customer_thread = {
				'id_shop': id_shop,
				'id_lang': self._notice['target']['language_default'],
				'id_contact': 0,
				'id_customer': id_customer,
				'id_order': order_id,
				'id_product': 0,
				'status': 'open',
				'email': convert['customer']['email'],
				'date_add': get_current_time(),
				'date_upd': get_current_time(),
			}
			id_customer_thread = self.import_order_data_connector(self.create_insert_query_connector('customer_thread', customer_thread))
			for history in convert['history']:
				customer_message = {
					'id_customer_thread': id_customer_thread,
					'message': history.get('comment', ''),
					'date_add': get_current_time(),
					'date_upd': get_current_time(),
					'private': 0 if history.get('notified') else 1,
					'read': 0,
				}
				all_query.append(self.create_insert_query_connector('customer_message', customer_message))
		if all_query:
			self.import_multiple_data_connector(all_query, 'order')
		del all_query
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
			'query': "SELECT * FROM _DBPRF_product_comment WHERE id_product_comment > " + to_str(id_src) + " ORDER BY id_product_comment ASC LIMIT " + to_str(limit)
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
		review_data['id'] = review['id_product_comment']

		review_data['language_id'] = self._notice['src']['language_default']
		review_data['product']['id'] = review['id_product']
		review_data['product']['name'] = ''
		review_data['customer']['id'] = review['id_customer']
		review_data['customer']['name'] = review['customer_name']
		review_data['title'] = review['title']
		review_data['content'] = review['content']
		review_data['status'] = review['validate']
		review_data['created_at'] = convert_format_time(review['date_add'])
		review_data['updated_at'] = convert_format_time(review['date_add'])

		rating = self.construct_review_rating()
		rating['rate_code'] = 'default'
		rating['rate'] = review['grade']
		review_data['rating'].append(rating)

		return response_success(review_data)

	def get_review_id_import(self, convert, review, reviews_ext):
		return review['id_product_comment']

	def check_review_import(self, convert, review, reviews_ext):
		return True if self.get_map_field_by_src(self.TYPE_REVIEW, convert['id'], convert['code']) else False

	def router_review_import(self, convert, review, reviews_ext):
		return response_success('review_import')

	def before_review_import(self, convert, review, reviews_ext):
		return response_success()

	def review_import(self, convert, review, reviews_ext):
		for module in ['productcomments', 'iqitreviews', 'revws']:
			if self._notice['target']['support'].get(module):
				return getattr(self, 'review_import_' + module)(convert, review, reviews_ext)
		return self.review_import_productcomments(convert, review, reviews_ext)

	def review_import_revws(self, convert, review, reviews_ext):
		product_id = False
		if convert['product']['id'] or convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
		if not product_id:
			product_id = 0
		customer_id = 0
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not customer_id:
				customer_id = 0
		review_data = {
			'entity_type': 'product',
			'id_entity': product_id,
			'id_customer': customer_id,
			'id_guest': 0,
			'title': convert['title'],
			'content': convert['content'],
			'display_name': convert['customer']['name'],
			'verified_buyer': 1,
			'email': '',
			'validated': 1 if convert['status'] and to_str(convert['status']) == '1' else 0,
			'deleted': 0,
			'date_add': convert['created_at'],
			'date_upd': convert['updated_at'] if convert['updated_at'] else convert['created_at'],
			'id_lang': 1
		}
		review_id = self.import_review_data_connector(self.create_insert_query_connector('revws_review', review_data), True, convert['id'])
		if not review_id:
			return response_error('review id ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		rating_data = {
			'id_review': review_id,
			'id_criterion': 1,
			'grade': convert['rating'][0]['rate'] if convert['rating'] and len(convert['rating']) > 0 else 0,
		}
		self.import_review_data_connector(self.create_insert_query_connector('revws_review_grade', rating_data), True, convert['id'])
		return response_success(review_id)

	def review_import_iqitreviews(self, convert, review, reviews_ext):
		product_id = False
		if convert['product']['id'] or convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
		if not product_id:
			product_id = 0
		customer_id = 0
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not customer_id:
				customer_id = 0
		review_data = {
			'id_product': product_id,
			'id_customer': customer_id,
			'id_guest': 0,
			'title': convert['title'],
			'comment': convert['content'],
			'customer_name': convert['customer']['name'],
			'rating': convert['rating'][0]['rate'],
			'status': 1 if convert['status'] and to_str(convert['status']) == '1' else 0,
			'date_add': convert['created_at'],
		}
		review_id = self.import_review_data_connector(self.create_insert_query_connector('iqitreviews_products', review_data), True, convert['id'])
		if not review_id:
			return response_error('review id ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		return response_success(review_id)

	def review_import_productcomments(self, convert, review, reviews_ext):
		product_id = False
		if convert['product']['id'] or convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
		if not product_id:
			product_id = 0
		# response_warning('Review ' + to_str(convert['id']) + ' import false. Product does not exist!')
		customer_id = 0
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
			if not customer_id:
				customer_id = 0
		review_data = {
			'id_product': product_id,
			'id_customer': customer_id,
			'id_guest': 0,
			'title': convert['title'],
			'content': convert['content'],
			'customer_name': convert['customer']['name'],
			'grade': convert['rating'][0]['rate'] if convert['rating'] and to_len(convert['rating']) > 0 else 0,
			'validate': 1 if convert['status'] and convert['status'] == '1' else 0,
			'deleted': 0,
			'date_add': convert['updated_at'],
			# 'id_shop': self._notice['target']['shop_default']
		}
		review_id = self.import_review_data_connector(self.create_insert_query_connector('product_comment', review_data), True, convert['id'])
		if not review_id:
			return response_error('review id ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		rating_data = {
			'id_product_comment': review_id,
			'id_product_comment_criterion': 1,
			'grade': convert['rating'][0]['rate'] if convert['rating'] and to_len(convert['rating']) > 0 else 0,
		}
		self.import_review_data_connector(self.create_insert_query_connector('product_comment_grade', rating_data), True, convert['id'])
		return response_success(review_id)

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
		page_data['status'] = True if to_int(page['active']) == 1 else False
		cms_lang = get_list_from_list_by_field(pages_ext['data']['cms_lang'], 'id_cms', page['id_cms'])
		cms_lang_def = get_row_from_list_by_field(cms_lang, 'id_lang', self._notice['src']['language_default'])
		if not cms_lang_def:
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
		return True if self.get_map_field_by_src(self.TYPE_PAGE, convert['id'], convert['code']) else False

	def router_page_import(self, convert, page, pages_ext):
		return response_success('page_import')

	def before_page_import(self, convert, page, pages_ext):
		return response_success()

	def page_import(self, convert, page, pages_ext):
		page_data = {
			'id_cms_category': 1,
			'position': 0,
			'active': 1 if convert['status'] else 0,
			'indexation': 0,
		}
		page_query = self.create_insert_query_connector('cms', page_data)
		page_import = self.import_data_connector(page_query, 'pages', convert['id'])
		if not page_import:
			return response_error()
		self.insert_map(self.TYPE_PAGE, convert['id'], page_import, convert['code'])
		return response_success(page_import)

	def after_page_import(self, page_id, convert, page, pages_ext):
		all_queries = list()
		page_lang_data = {
			'id_cms': page_id,
			'id_lang': self._notice['target']['language_default'],
			'id_shop': self._notice['target']['shop_default'],
			'head_seo_title': convert['meta_title'],
			'meta_title': convert['title'],
			'meta_description': convert['meta_description'],
			'meta_keywords': convert['meta_keywords'],
			'content': convert['content'],
			'link_rewrite': convert['url_key'],
		}
		all_queries.append(self.create_insert_query_connector("cms_lang", page_lang_data))
		page_shop_data = {
			'id_cms': page_id,
			'id_shop': self._notice['target']['shop_default'],
		}
		all_queries.append(self.create_insert_query_connector("cms_shop", page_shop_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'pages')
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
			'query': "SELECT * FROM _DBPRF_cart_rule WHERE id_cart_rule > " + to_str(id_src) + " ORDER BY id_cart_rule ASC LIMIT " + to_str(limit),
		}
		coupons = self.select_data_connector(query, 'coupons')
		if not coupons or coupons['result'] != 'success':
			return response_error()
		return coupons

	def get_coupons_ext_export(self, coupons):
		coupon_ids = duplicate_field_value_from_list(coupons['data'], 'id_cart_rule')
		coupon_id_con = self.list_to_in_condition(coupon_ids)
		coupon_ext_queries = {
			'cart_rule_lang': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_cart_rule_lang WHERE id_cart_rule IN " + coupon_id_con
			},
			'cart_rule_group': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_cart_rule_group WHERE id_cart_rule IN " + coupon_id_con
			},
		}
		coupons_ext = self.select_multiple_data_connector(coupon_ext_queries, 'products')
		if (not coupons_ext) or coupons_ext['result'] != 'success':
			return response_error()
		return coupons_ext

	def convert_coupon_export(self, coupon, coupons_ext):
		coupon_data = self.construct_coupon()
		coupon_data['id'] = coupon['id_cart_rule']
		coupon_data['code'] = coupon['code']
		cart_rule_lang = get_list_from_list_by_field(coupons_ext['data']['cart_rule_lang'], 'id_cart_rule', coupon['id_cart_rule'])
		coupon_data['title'] = get_row_value_from_list_by_field(cart_rule_lang, 'id_lang', self._notice['src']['language_default'], 'name')
		coupon_data['description'] = coupon['description']
		coupon_data['status'] = True if to_int(coupon['active']) == 1 else False
		coupon_data['created_at'] = convert_format_time(coupon['date_add'])
		coupon_data['updated_at'] = convert_format_time(coupon['date_upd'])
		coupon_data['from_date'] = convert_format_time(coupon['date_from'])
		coupon_data['to_date'] = convert_format_time(coupon['date_to'])
		coupon_data['min_spend'] = coupon['minimum_amount']
		coupon_data['times_used'] = coupon['quantity']
		coupon_data['usage_limit'] = coupon['quantity']
		coupon_data['discount_amount'] = coupon['reduction_percent'] if to_int(to_decimal(coupon['reduction_percent'])) > 0 else coupon['reduction_amount']
		coupon_data['usage_per_customer'] = coupon['quantity_per_user']
		coupon_data['sort_order'] = coupon['priority']
		coupon_data['type'] = self.PERCENT if to_int(to_decimal(coupon['reduction_percent'])) > 0 else self.FIXED
		coupon_data['coupon_type'] = 2 if coupon['code'] else 1

		cart_rule_group = get_list_from_list_by_field(coupons_ext['data']['cart_rule_group'], 'id_cart_rule', coupon['id_cart_rule'])
		if cart_rule_group:
			for cart_rule_group_child in cart_rule_group:
				coupon_data['customer_group'].append(cart_rule_group_child['id_group'])
		return response_success(coupon_data)

	def get_coupon_id_import(self, convert, coupon, coupons_ext):
		return coupon['id_cart_rule']

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
			'quantity': convert['usage_limit'] if convert['usage_limit'] else 0,
			'quantity_per_user': convert['usage_per_customer'] if convert['usage_per_customer'] is not None else 0,
			'priority': 1,
			'partial_use': 1,
			'code': convert['code'] if convert['code'] else convert['id'],
			'minimum_amount': convert['discount_amount'],
			'minimum_amount_currency': 1,
			'reduction_percent': convert['discount_amount'] if convert['type'] == self.PERCENT else 0.00,
			'reduction_amount': convert['discount_amount'] if convert['type'] == self.FIXED else 0.00,
			'reduction_currency': 1,
			'free_shipping': 1 if convert.get('simple_free_shipping') and to_int(convert.get('simple_free_shipping')) == 1 else 0,
			'active': 1 if convert['status'] else 0,
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

	# TODO: CODE PRESTASHOP17

	def get_category_parent(self, category_id):
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_category WHERE id_category = " + to_str(category_id),
		}
		# categories = self.get_connector_data(url_query, {'query': json.dumps(query)})
		categories = self.select_data_connector(query, 'categories')
		if not categories or categories['result'] != 'success':
			return response_error('could not get category parent to export')
		if categories and categories['data']:
			category = categories['data'][0]
			categories_ext = self.get_categories_ext_export(categories)
			category_convert = self.convert_category_export(category, categories_ext)
			return category_convert
		if categories and len(categories['data']) == 0:
			return response_success()
		return response_error('could not get category parent to export')

	def import_category_parent(self, convert_parent):
		parent_exists = self.get_map_field_by_src(self.TYPE_CATEGORY, convert_parent['id'], convert_parent['code'])
		if parent_exists:
			return response_success(parent_exists)
		category = convert_parent['category']
		categories_ext = convert_parent['categories_ext']
		category_parent_import = self.category_import(convert_parent, category, categories_ext)
		self.after_category_import(category_parent_import['data'], convert_parent, category, categories_ext)
		return category_parent_import

	def _get_image_path(self, id_image):
		path = self._get_img_folder_static(id_image)
		path_img = 'p/' + to_str(path) + to_str(id_image) + '.jpg'
		return path_img

	def _get_image_path_generate(self, id_image, type_image = ''):
		path = self._get_img_folder_static(id_image)
		path_img = 'p/' + to_str(path) + to_str(id_image) + '.jpg'
		if type_image:
			path_img = 'p/' + to_str(path) + to_str(id_image) + '-' + type_image + '.jpg'
		return path_img

	# def _get_image_path_default(self, id_image):
	# 	path = self._get_img_folder_static(id_image)
	# 	path_img = 'p/' + to_str(path) + to_str(id_image) + '-home_default.jpg'
	# 	return path_img
	#
	# def _get_image_path_small(self, id_image):
	# 	path = self._get_img_folder_static(id_image)
	# 	path_img = 'p/' + to_str(path) + to_str(id_image) + '-small_default.jpg'
	# 	return path_img

	def _get_img_folder_static(self, id_image):
		folders = list(str(id_image))
		path = to_str(('/'.join(folders))) + '/'
		return path

	def insert_address(self, customer_id, convert):
		zone_state = convert['state']
		if zone_state['id'] or zone_state['state_code']:
			state = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'select',
					'query': "SELECT id_state FROM _DBPRF_state WHERE iso_code LIKE '" + to_str(zone_state['state_code']) + "'"
				})
			})
			if state and state['data']:
				id_state = state['data'][0]['id_state']
			else:
				if zone_state['id']:
					id_state = zone_state['id']
				else:
					id_state = 0
		else:
			id_state = 0

		zone_country = convert['country']
		if zone_country['id'] or zone_country['country_code']:
			country = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'select',
					'query': "SELECT id_country FROM _DBPRF_country WHERE iso_code LIKE '" + to_str(zone_country['country_code']) + "'"
				})
			})
			if country and country['data']:
				id_country = country['data'][0]['id_country']
			else:
				if zone_country['id']:
					id_country = zone_country['id']
				else:
					id_country = 0
		else:
			id_country = 0
		address_data = {
			'id_country': id_country if to_int(id_country) > 0 else '1',
			'id_state': id_state if to_int(id_state) > 0 else '1',
			'id_customer': customer_id,
			'alias': 'My address',
			'company': convert.get('company', ''),
			'lastname': convert.get('last_name', ''),
			'firstname': convert.get('first_name', ''),
			'address1': convert['address_1'] if convert['address_1'] else '',
			'address2': convert['address_2'] if convert['address_2'] else '',
			'postcode': convert['postcode'],
			'city': convert.get('city', ''),
			'phone': convert['telephone'],
			'phone_mobile': convert['telephone'],
			'vat_number': convert['vat_number'] if 'vat_number' in convert else '',
			'date_add': get_current_time(),
			'date_upd': get_current_time(),
			'active': 1,
		}
		id_address = self.import_data_connector(self.create_insert_query_connector('address', address_data), 'customer')
		if id_address:
			self.insert_map(self.TYPE_ADDRESS, convert['id'], id_address, convert['code'])
		return id_address

	def delete_target_customer(self, customer_id):
		if not customer_id:
			return True
		url_query = self.get_connector_url('query')
		customer_ext_queries = {
			'customer_thread': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_customer_thread WHERE id_customer = ' + customer_id,
			},
			'message': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_message WHERE id_customer = ' + customer_id,
			},
		}
		# customer_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(customer_ext_queries)
		# })
		customer_ext = self.select_multiple_data_connector(customer_ext_queries, 'customers')
		if not customer_ext or customer_ext['result'] != 'success':
			return True
		customer_thread = duplicate_field_value_from_list(customer_ext['data']['customer_thread'], 'id_customer_thread')
		message = duplicate_field_value_from_list(customer_ext['data']['message'], 'id_message')
		queries_delete = {
			'customer': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer WHERE id_customer = " + to_str(customer_id)
			},
			'customer_group': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_group WHERE id_customer = " + to_str(customer_id)
			},
			'customer_thread': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_thread WHERE id_customer = " + to_str(customer_id)
			},
			'message': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_message WHERE id_customer = " + to_str(customer_id)
			},
			'address': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_address WHERE id_customer = " + to_str(customer_id)
			},
			'customer_entity_int': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity_int WHERE entity_id = " + to_str(customer_id)
			},
			'customer_entity_datetime': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity_datetime WHERE entity_id = " + to_str(customer_id)
			},
			'customer_entity': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity WHERE entity_id = " + to_str(customer_id)
			},
		}
		if customer_thread:
			queries_delete['customer_message'] = {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_message WHERE id_customer_thread IN " + self.list_to_in_condition(customer_thread)
			}

		if message:
			queries_delete['message_readed'] = {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_message_readed WHERE id_message IN " + self.list_to_in_condition(message)
			}
		self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(queries_delete)
		})
		return True

	def delete_target_order(self, order_id):
		if not order_id:
			return True
		url_query = self.get_connector_url('query')

		delete_queries = {
			'orders': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_orders WHERE id_order = " + to_str(order_id),
			},
			'order_carrier': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_carrier WHERE id_order = " + to_str(order_id),
			},
			'order_cart_rule': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_cart_rule WHERE id_order = " + to_str(order_id),
			},
			'order_detail': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_detail WHERE id_order = " + to_str(order_id),
			},
			'order_history': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_history WHERE id_order = " + to_str(order_id),
			},
			'order_invoice': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_invoice WHERE id_order = " + to_str(order_id),
			},
			'order_return': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_return WHERE id_order = " + to_str(order_id),
			},
			'order_slip': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_order_slip WHERE id_order = " + to_str(order_id),
			},
		}
		self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(delete_queries)
		})
		return True

	def detect_seo(self):
		return 'default_seo'

	def categories_default_seo(self, category, categories_ext):
		result = list()
		key_entity = 'id_category'
		cat_desc = get_list_from_list_by_field(categories_ext['data']['category_lang'], key_entity, category['id_category'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			seo_cate = self.construct_seo_category()
			seo_cate['request_path'] = to_str(category['id_category']) + '-' + seo['link_rewrite']
			seo_cate['store_id'] = seo['id_lang']
			seo_cate['type'] = type_seo
			result.append(seo_cate)
		return result

	def products_default_seo(self, product, products_ext):
		result = list()
		key_entity = 'id_product'
		prd_desc = get_list_from_list_by_field(products_ext['data']['product_lang'], key_entity, product['id_product'])
		childen_products = get_list_from_list_by_field(products_ext['data']['product_attribute'], 'id_product', product['id_product'])
		add_seo = ''
		if childen_products:
			product_attribute = get_row_value_from_list_by_field(childen_products, 'default_on', 1, 'id_product_attribute')
			if product_attribute:
				add_seo = to_str(product_attribute) + '-'
		for seo in prd_desc:
			type_seo = self.SEO_DEFAULT
			seo_product = self.construct_seo_product()
			seo_product['request_path'] = to_str(product['id_product']) + '-' + add_seo + seo['link_rewrite']
			seo_product['store_id'] = seo['id_lang']
			seo_product['type'] = type_seo
			result.append(seo_product)
		return result

	# def image_exists(site, url):
	# 	r = requests.head(url)
	# 	return r.status_code == requests.codes.ok

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

	def get_id_image(self, url_image, image_previous):
		if not image_previous:
			return None
		for image in image_previous:
			if url_image == image['value']:
				return image['id_desc']
		return None

	def get_position(self, table):
		get_postion = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': "SELECT MAX(position) FROM _DBPRF_" + table,
			})
		})
		if get_postion and get_postion['data'][0]['MAX(position)'] != None:
			position = to_int(get_postion['data'][0]['MAX(position)']) + 1
		else:
			position = 1

		return position

	def get_special_price(self, list_special):
		res = None
		unlimited = None
		for special in list_special:
			if special['to'] == '0000-00-00 00:00:00':
				unlimited = special
				continue
			start = to_timestamp(special['from']) if special['from'] != '0000-00-00 00:00:00' else 0
			end = to_timestamp(special['to'])
			if start <= time.time() <= end:
				res = special
		if not res:
			res = unlimited
		return res

	def convert_option_name_to_code(self, name = ''):
		if isinstance(name, bool):
			if name:
				name = 'yes'
			else:
				name = 'no'
		result = to_str(name).strip(' ')
		result = result.lower()
		result = result.replace('ö', 'o').replace('ő', 'o').replace('ű', 'u').replace('.', '_').replace('/', '_').replace(',', '_')
		result = re.sub('[^A-Za-z0-9_ - \'\"/,:;]+', '', result)
		result = re.sub('[^A-Za-z0-9_ -]+', '-', result)
		result = result.strip(' -')
		result = result.replace(' ', '-').replace('_', '-')
		while result.find('--') != -1:
			result = result.replace('--', '-')
		return result

	def lecm_rewrite_table_construct(self):
		return {
			'table': '_DBPRF_lecm_rewrite',
			'rows': {
				'id_url': 'INTEGER UNSIGNED NOT NULL AUTO_INCREMENT',
				'id_src': 'INT(11)',
				'link_rewrite': 'VARCHAR(255)',
				'id_desc': 'INT(11)',
				'type': 'VARCHAR(8)',
				'lang_code': 'VARCHAR(2)',
			},
			'index': [
				['id_src', 'id_desc', 'type', 'link_rewrite', 'lang_code']
			]
		}

	def uploadImageConnector(self, image_process, save_path, rename = None, override = None, is_proxy = False):
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
		# if not is_proxy:
		# 	path = self.get_map_field_by_src(self.TYPE_PATH_IMAGE, None, image_process['url'], field = 'code_desc')
		# 	if path:
		# 		in_map = True
		# 		param_rename = False
		# 		param_override = False
		# 		save_path = path
		# 	else:
		# 		param_rename = True
		# 		param_override = False
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
			image_import_path = image_import['data']['ci']
			# if not in_map:
			# 	self.insert_map(self.TYPE_PATH_IMAGE, None, None, image_process['url'], image_import_path)
			return image_import_path
		if not is_proxy and not self.image_proxy:
			return self.uploadImageConnector(image_process, save_path, rename, override, True)
		if image_import and image_import.get('error'):
			msg = 'Image Error '
			msg += '\n Url Image: ' + params['url']
			msg += '\n Save Path: ' + save_path
			msg += '\n Params: ' + to_str(params)
			msg += '\n Error: ' + to_str(image_import.get('error'))
			self.log(msg, 'image_error')

		return image_import_path

	def get_children_from_list(self, id_attribute, list_child, products_ext):
		for child in list_child:
			product_attribute_combination = get_list_from_list_by_field(products_ext['data']['product_attribute_combination'], 'id_product_attribute', child['id_product_attribute'])
			list_attribute_id = duplicate_field_value_from_list(product_attribute_combination, 'id_attribute')
			diff_ids = list((list(set(id_attribute) - set(list_attribute_id)) + list(set(list_attribute_id) - set(id_attribute))))
			if not diff_ids:
				return child
		return dict()
