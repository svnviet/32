import copy
import re
import unicodedata
from cartmigration.libs.utils import *

try:
	import chardet
except:
	pass

from cartmigration.models.basecart import LeBasecart

class LeCartMagento2(LeBasecart):
	def __init__(self, data = None):
		super().__init__(data)
		self.tax_customer = None
		self.tax_rule = None
		self.eav_attribute_product = None
		self.catalog_eav_attribute = None

	def display_config_source(self):
		parent = super().display_config_target()
		if parent['result'] != 'success':
			return parent
		url_query = self.get_connector_url('query')
		default_query = {
			'languages': {
				'type': 'select',
				'query': "select st.store_id, st.name, st.code, st.sort_order, sw.website_id,sw.name as website_name,st.group_id,sg.root_category_id, sg.name as group_name "
				         " from _DBPRF_store as st "
				         "JOIN _DBPRF_store_website as sw on st.website_id = sw.website_id "
				         "JOIN _DBPRF_store_group as sg on st.group_id = sg.group_id "
				         "WHERE st.code != 'admin'"
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'currency/options/default'"
			},
			'eav_entity_type': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_eav_entity_type",
			},
			"category_root": {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_category_entity "
				         "WHERE level = 1"
			},
		}
		default_config = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(default_query)
		})
		if (not default_config) or (default_config['result'] != 'success'):
			return response_error()
		default_config_data = default_config['data']
		if default_config_data and default_config_data['languages'] and default_config_data['eav_entity_type']:
			self._notice['src']['language_default'] = self.get_default_language(default_config_data['languages'])
			self._notice['src']['currency_default'] = default_config_data['currencies'][0]['value'] if to_len(
				default_config_data['currencies']) > 0 else 'USD'
			for eav_entity_type in default_config_data['eav_entity_type']:
				self._notice['src']['extends'][eav_entity_type['entity_type_code']] = eav_entity_type[
					'entity_type_id']
		else:
			return response_error('err default data')
		self._notice['src']['category_root'] = 1
		self._notice['src']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['target']['attributes'] = {
			1: 'Default Attribute',
		}

		category_root_data = dict()
		category_row_data = dict()
		category_row_root = dict()
		check_magento_ee = False
		for category_root_row in default_config_data['category_root']:
			category_root_data[category_root_row['entity_id']] = category_root_row['entity_id']
			if category_root_row.get('row_id'):
				check_magento_ee = True
				category_row_data[category_root_row['row_id']] = category_root_row['row_id']
				category_row_root[category_root_row['row_id']] = category_root_row['entity_id']

		self._notice['src']['category_data'] = category_root_data
		self._notice['src']['category_row_data'] = category_row_data
		self._notice['src']['category_row_root'] = category_row_root

		if check_magento_ee:
			self._notice['src']['config']['version'] += '.ee'

		config_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'general/locale/code'"
			},
			# 'currencies': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'currency/options/allow'"
			# },
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_status"
			},
			'orders_state': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_status_state"
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group"
			},
		}

		config = self.select_multiple_data_connector(config_queries)
		if (not config) or (config['result'] != 'success'):
			return response_error("can't display config target")
		config_data = config['data']
		language_data = dict()
		storage_cat_data = dict()
		self._notice['src']['store_site'] = dict()
		for language_row in default_config_data['languages']:
			lang_id = language_row['store_id']
			self._notice['src']['site'][lang_id] = language_row['website_id']
			self._notice['src']['store_site'][lang_id] = language_row['website_id']
			language_data[lang_id] = language_row['website_name'] + ' > ' + language_row['group_name'] + ' > ' + language_row['name']
			storage_cat_data[lang_id] = language_row['root_category_id']
		order_status_data = dict()
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['status']] = order_status_row['label']
		order_state_data = dict()
		for order_state_row in config_data['orders_state']:
			order_state_data[order_state_row['status']] = order_state_row['state']
		self._notice['src']['order_state'] = order_state_data
		self._notice['src']['support']['order_state_map'] = True
		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['customer_group_id']] = customer_group['customer_group_code']

		self._notice['src']['languages'] = language_data
		self._notice['src']['store_category'] = storage_cat_data
		self._notice['src']['order_status'] = order_status_data
		self._notice['src']['customer_group'] = customer_group_data
		self._notice['src']['config']['seo_module'] = self.get_list_seo()

		self._notice['src']['support']['country_map'] = False
		self._notice['src']['support']['languages_select'] = True
		self._notice['src']['support']['site_map'] = False
		self._notice['src']['support']['customer_group_map'] = True
		self._notice['src']['support']['attributes'] = False
		self._notice['src']['support']['add_new'] = True
		self._notice['src']['support']['pre_prd'] = True
		self._notice['src']['support']['pre_cus'] = True
		self._notice['src']['support']['cus_pass'] = True
		self._notice['src']['support']['pre_ord'] = True
		self._notice['src']['support']['ignore_image'] = False
		self._notice['src']['support']['img_des'] = True
		self._notice['src']['support']['coupons'] = True
		self._notice['src']['support']['seo'] = True
		self._notice['src']['support']['seo_301'] = True
		# self._notice['src']['support']['blogs'] = True
		self._notice['src']['support']['pages'] = True
		if self._notice['target']['cart_type'] == 'shopify':
			self._notice['src']['support']['multi_languages_select'] = True
		return response_success()

	def display_config_target(self):
		parent = super().display_config_target()
		if parent['result'] != 'success':
			return parent
		url_query = self.get_connector_url('query')
		default_query = {
			'languages': {
				'type': 'select',
				'query': "SELECT st.store_id, st.name, st.code, st.sort_order, sw.website_id,sw.name AS website_name,st.group_id,sg.root_category_id, sg.name AS group_name FROM _DBPRF_store as st JOIN _DBPRF_store_website AS sw ON st.website_id = sw.website_id JOIN _DBPRF_store_group AS sg ON st.group_id = sg.group_id WHERE st.code != 'admin'"
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'currency/options/default'"
			},
			'eav_entity_type': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_eav_entity_type",
			},
			"category_root": {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_category_entity "
				         "WHERE level = 1"
			},
			"customer_pwd": {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_setup_module "
				         "WHERE `module` = 'LitExtension_CustomerPassword'"
			},
		}
		default_config = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(default_query)
		})
		if (not default_config) or (default_config['result'] != 'success'):
			return response_error()
		default_config_data = default_config['data']
		if default_config_data and default_config_data['languages'] and \
				default_config_data['eav_entity_type']:
			self._notice['target']['language_default'] = self.get_default_language(default_config_data['languages'])
			self._notice['target']['currency_default'] = default_config_data['currencies'][0]['value'] if to_len(
				default_config_data['currencies']) > 0 else 'USD'
			for eav_entity_type in default_config_data['eav_entity_type']:
				self._notice['target']['extends'][eav_entity_type['entity_type_code']] = eav_entity_type[
					'entity_type_id']
		else:
			return response_error('err default data')
		self._notice['target']['category_root'] = 1
		self._notice['target']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['target']['attributes'] = {
			1: 'Default Attribute',
		}
		if default_config_data['customer_pwd']:
			self._notice['target']['support']['plugin_cus_pass'] = True
		category_root_data = dict()
		category_row_data = dict()
		category_row_root = dict()
		check_magento_ee = False
		for category_root_row in default_config_data['category_root']:
			category_root_data[category_root_row['entity_id']] = category_root_row['entity_id']
			if category_root_row.get('row_id'):
				check_magento_ee = True
				category_row_data[category_root_row['row_id']] = category_root_row['row_id']
				category_row_root[category_root_row['row_id']] = category_root_row['entity_id']

		self._notice['target']['category_data'] = category_root_data
		self._notice['target']['category_row_data'] = category_row_data
		self._notice['target']['category_row_root'] = category_row_root

		if check_magento_ee:
			self._notice['target']['config']['version'] += '.ee'

		config_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'general/locale/code'"
			},
			# 'currencies': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'currency/options/allow'"
			# },
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_status"
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group"
			},
			'stores': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_store WHERE code != 'admin' AND is_active = 1"
			}
		}

		config = self.select_multiple_data_connector(config_queries)
		if (not config) or (config['result'] != 'success'):
			return response_error("can't display config target")
		config_data = config['data']
		language_data = dict()
		storage_cat_data = dict()
		site_data = dict()
		self._notice['target']['store_site'] = dict()
		for language_row in default_config_data['languages']:
			lang_id = language_row['store_id']
			self._notice['target']['site'][lang_id] = language_row['website_id']
			self._notice['target']['store_site'][lang_id] = language_row['website_id']
			language_data[lang_id] = language_row['website_name'] + ' > ' + language_row['group_name'] + ' > ' + language_row['name']
			storage_cat_data[lang_id] = language_row['root_category_id']
		order_status_data = dict()
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['status']] = order_status_row['label']
		if config_data and config_data['stores']:
			for store_row in config_data['stores']:
				site_data[store_row['store_id']] = store_row['name']
		else:
			site_data = {
				1: 'Default Shop',
			}
		# currency_data = dict()
		# currencies = config_data['currencies'][0]['value']
		# if currencies:
		# 	currencies_list = currencies.split(',')
		# 	for currency_row in currencies_list:
		# 		currency_data[currency_row] = currency_row
		# else:
		# 	currency_data['USD'] = 'USD'

		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['customer_group_id']] = customer_group['customer_group_code']
		self._notice['target']['website'] = config_data['stores']
		self._notice['target']['site'] = site_data
		self._notice['target']['languages'] = language_data
		self._notice['target']['store_category'] = storage_cat_data
		self._notice['target']['order_status'] = order_status_data
		self._notice['target']['customer_group'] = customer_group_data
		self._notice['target']['support']['country_map'] = False
		self._notice['target']['support']['languages_select'] = True
		self._notice['target']['support']['site_map'] = True
		self._notice['target']['support']['customer_group_map'] = True
		self._notice['target']['support']['attributes'] = True
		self._notice['target']['support']['pages'] = True
		self._notice['target']['support']['blogs'] = True
		# self._notice['target']['support']['rules'] = True
		# self._notice['target']['support']['cartrules'] = True
		self._notice['target']['support']['add_new'] = True
		self._notice['target']['support']['reviews'] = True
		self._notice['target']['support']['pre_prd'] = False
		self._notice['target']['support']['pre_cus'] = True
		self._notice['target']['support']['pre_ord'] = True
		self._notice['target']['support']['ignore_image'] = False
		self._notice['target']['support']['img_des'] = True
		self._notice['target']['support']['seo'] = True
		self._notice['target']['support']['coupons'] = True
		self._notice['target']['support']['seo_301'] = True
		self._notice['target']['support']['cus_pass'] = True
		self._notice['target']['support']['check_cus_pass'] = True
		self._notice['target']['support']['update_latest_data'] = True
		self._notice['target']['config']['entity_update']['products'] = True
		return response_success()

	def display_confirm_target(self):
		self._notice['target']['clear']['function'] = 'clear_target_taxes'
		self._notice['target']['clear_demo']['function'] = 'clear_target_products_demo'
		return response_success()
	def get_query_display_import_source(self, update = False):
		compare_condition = ' > '
		if update:
			compare_condition = ' <= '
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = " WHERE " + to_str(store_id_con) + " AND "
		else:
			store_id_con = " WHERE "

		queries = {
			'taxes': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_tax_class WHERE class_type = 'PRODUCT' AND class_id " + compare_condition + to_str(
					self._notice['process']['taxes']['id_src']),
			},
			'manufacturers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_eav_attribute as ea LEFT JOIN "
				         "_DBPRF_eav_attribute_option as eao ON ea.attribute_id = eao.attribute_id WHERE "
				         "ea.attribute_code = 'manufacturer' AND eao.option_id " + compare_condition + to_str(
					self._notice['process']['manufacturers']['id_src']),
			},
			'categories': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_catalog_category_entity WHERE level > 1 AND entity_id " + compare_condition + to_str(
					self._notice['process']['categories']['id_src']),
			},
			'attributes': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_eav_attribute WHERE entity_type_id = '" +
				         self._notice['src']['extends']['catalog_product'] + "' AND attribute_id " + compare_condition + to_str(
					self._notice['process']['attributes']['id_src']),
			},
			'products': {
				'type': 'select',
				'query': "SELECT COUNT(1) as count FROM _DBPRF_catalog_product_entity WHERE entity_id NOT IN (SELECT product_id FROM _DBPRF_catalog_product_super_link) AND entity_id IN (select product_id from _DBPRF_catalog_product_website where" + self.get_con_website_select_count() + ") AND entity_id " + compare_condition + to_str(
					self._notice['process']['products']['id_src']),
			},
			'customers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_customer_entity WHERE " + self.get_con_website_select_count() + " AND entity_id " + compare_condition + to_str(
					self._notice['process']['customers']['id_src']),
			},
			'orders': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_sales_order WHERE" + self.get_con_store_select_count() + " AND entity_id " + compare_condition + to_str(
					self._notice['process']['orders']['id_src']),
			},
			'reviews': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_review WHERE review_id IN (select review_id from _DBPRF_review_store where" + self.get_con_store_select_count() + ") AND review_id " + compare_condition + to_str(
					self._notice['process']['reviews']['id_src']),
			},
			'pages': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_cms_page WHERE page_id IN (select page_id from _DBPRF_cms_page_store where" + self.get_con_store_select_count() + ") AND page_id " + compare_condition + to_str(
					self._notice['process']['pages']['id_src']),
			},
			'blogs': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_cms_block WHERE block_id IN (select block_id from _DBPRF_cms_block_store where" + self.get_con_store_select_count() + ") AND block_id " + compare_condition + to_str(
					self._notice['process']['blogs']['id_src']),
			},
			'coupons': {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_salesrule as s inner join _DBPRF_salesrule_coupon as sc on s.rule_id = sc.rule_id WHERE s.rule_id ' + compare_condition + to_str(
					self._notice['process']['coupons']['id_src']),
			},
		}
		return queries

	def display_import_source(self):
		if self._notice['config']['add_new']:
			self.display_recent_data()

		queries = self.get_query_display_import_source()
		count = self.select_multiple_data_connector(queries, 'count')

		if (not count) or (count['result'] != 'success'):
			return response_error()
		real_totals = dict()
		for key, row in count['data'].items():
			total = self.list_to_count_import(row, 'count')
			real_totals[key] = total
		real_totals['manufacturers'] = real_totals['manufacturers'] if to_int(real_totals['manufacturers']) > 1 else 0
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
		real_totals['manufacturers'] = real_totals['manufacturers'] if to_int(real_totals['manufacturers']) > 1 else 0
		for key, total in real_totals.items():
			self._notice['process'][key]['total_update'] = total
		return response_success()

	def display_import_target(self):
		return response_success()

	# TODO: CLEAR DEMO
	def clear_target_taxes_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_manufacturers_demo',
		}

		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['taxes']:
			return next_clear
		tax_ids = list()
		tax_product_ids = list()
		tax_customer_ids = list()
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_TAX_PRODUCT
		}
		taxes = self.select_obj(TABLE_MAP, where)
		if taxes['result'] == 'success':
			tax_product_ids = duplicate_field_value_from_list(taxes['data'], 'id_desc')
			tax_ids = list(set(tax_ids + tax_product_ids))
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_TAX_CUSTOMER
		}
		taxes = self.select_obj(TABLE_MAP, where)
		if taxes['result'] == 'success':
			tax_customer_ids = duplicate_field_value_from_list(taxes['data'], 'id_desc')
			tax_ids = list(set(tax_ids + tax_customer_ids))
		if not tax_ids:
			return next_clear
		tax_id_con = self.list_to_in_condition(tax_ids)
		tables = [
			'tax_calculation_rate',
			'tax_calculation_rule',
			'tax_calculation',
			'tax_class',
		]
		for table in tables:
			where = ''
			if table == 'tax_class':
				where = ' WHERE class_type = "PRODUCT" AND class_id IN ' + tax_id_con
			if table == 'tax_calculation':
				where = ' WHERE product_tax_class_id IN ' + self.list_to_in_condition(tax_product_ids)
			if table == 'tax_calculation_rate':
				where = ' WHERE tax_calculation_rate_id IN (SELECT tax_calculation_rate_id FROM `_DBPRF_tax_calculation`  WHERE product_tax_class_id IN ' + self.list_to_in_condition(
					tax_product_ids) + ' )'
			if table == 'tax_calculation_rule':
				where = ' WHERE tax_calculation_rute_id IN (SELECT tax_calculation_rute_id FROM `_DBPRF_tax_calculation`  WHERE product_tax_class_id IN ' + self.list_to_in_condition(
					tax_product_ids) + ' )'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear_demo'] = next_clear
		return next_clear

	def clear_target_manufacturers_demo(self):
		self._notice['target']['clear_demo']['result'] = 'process'
		self._notice['target']['clear_demo']['function'] = 'clear_target_categories_demo'
		self._notice['target']['clear_demo']['table_index'] = 0

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
			'catalog_category_entity_datetime',
			'catalog_category_entity_decimal',
			'catalog_category_entity_int',
			'catalog_category_entity_text',
			'catalog_category_entity_varchar',
			'catalog_category_entity',
			'catalog_url_rewrite_product_category',
			'url_rewrite',
			'catalog_category_product'
		]

		for table in tables:
			where = ' WHERE entity_id IN ' + category_id_con

			if table == 'url_rewrite':
				where = ' WHERE entity_type like "category" AND entity_id IN ' + category_id_con
			if table == 'catalog_url_rewrite_product_category' or table == 'catalog_category_product':
				where = ' WHERE category_id IN ' + category_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear_demo'] = next_clear

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
			product_id_map = duplicate_field_value_from_list(products['data'], 'id_desc')
			product_ids = list(set(product_ids + product_id_map))
		if not product_ids:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		product_id_con = self.list_to_in_condition(product_ids)
		tables = [
			'catalog_category_product',
			'cataloginventory_stock_status',
			'cataloginventory_stock_item',
			'catalog_product_website',
			'catalog_product_super_attribute_label',
			'catalog_product_super_link',
			'catalog_product_relation',
			'catalog_product_super_attribute',
			'catalog_product_option_price',
			'catalog_product_option_title',
			'catalog_product_option_type_price',
			'catalog_product_option_type_title',
			'catalog_product_option_type_value',
			'catalog_product_option',
			'catalog_product_entity_media_gallery_value_to_entity',
			'catalog_product_entity_media_gallery_value',
			'catalog_product_entity_tier_price',
			'catalog_product_entity_varchar',
			'catalog_product_entity_datetime',
			'catalog_product_entity_decimal',
			'catalog_product_entity_text',
			'catalog_product_entity_int',
			'catalog_product_link',
			'catalog_product_bundle_selection',
			'catalog_product_bundle_option_value',
			'catalog_product_bundle_option',
			'catalog_url_rewrite_product_category',
			'url_rewrite',
			'catalog_product_entity',
			'sequence_product'
		]
		table_key_product_id = ['cataloginventory_stock_status', 'cataloginventory_stock_item',
		                        'catalog_product_website', 'catalog_product_link',
		                        'catalog_url_rewrite_product_category', 'catalog_product_option',
		                        'catalog_category_product']
		if self.convert_version(self._notice['target']['config']['version'], 2) >= 230:
			tables.insert(0, 'inventory_stock_1')
			table_key_product_id.insert(0, 'inventory_stock_1')
		table_option = ['catalog_product_option_price',
		                'catalog_product_option_title',
		                'catalog_product_option_type_price',
		                'catalog_product_option_type_title',
		                'catalog_product_option_type_value', ]
		for table in tables:
			where = ' WHERE entity_id IN ' + product_id_con

			if table == 'url_rewrite':
				where = ' WHERE entity_type like "product" AND entity_id IN ' + product_id_con

			if table in table_key_product_id:
				where = ' WHERE product_id IN ' + product_id_con
			if table == 'catalog_product_relation':
				where = ' WHERE parent_id IN ' + product_id_con + ' OR child_id IN ' + product_id_con

			if table == 'catalog_product_bundle_option':
				where = ' WHERE parent_id IN ' + product_id_con

			if table in ['catalog_product_bundle_option_value', 'catalog_product_bundle_selection']:
				where = ' WHERE option_id IN (SELECT option_id FROM _DBPRF_catalog_product_bundle_option WHERE parent_id IN' + product_id_con + ')'

			if table in table_option:
				option_id_con = " (SELECT option_id FROM _DBPRF_catalog_product_option WHERE product_id IN " + product_id_con + ")"
				where = ' WHERE option_id IN ' + option_id_con
				if table in ['catalog_product_option_type_title', 'catalog_product_option_type_price']:
					where = " WHERE option_type_id IN (SELECT option_type_id FROM _DBPRF_catalog_product_option_type_value " + where + ")"
			if table == 'sequence_product':
				where = 'WHERE sequence_value IN ' + product_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		self.delete_map_demo(self.TYPE_PRODUCT, product_ids)
		if to_len(product_ids) < self.LIMIT_CLEAR_DEMO:
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
			customer_id_map = duplicate_field_value_from_list(customers['data'], 'id_desc')
			customer_ids = list(set(customer_ids + customer_id_map))
		if not customer_ids:
			return next_clear
		customer_id_con = self.list_to_in_condition(customer_ids)
		tables = [
			'customer_entity_datetime',
			'customer_entity_int',
			'customer_entity_varchar',
			'customer_entity_text',
			'customer_entity_decimal',
			'customer_grid_flat',
			'customer_address_entity_datetime',
			'customer_address_entity_int',
			'customer_address_entity_varchar',
			'customer_address_entity_text',
			'customer_address_entity_decimal',
			'customer_address_entity',
			'newsletter_subscriber',
			'customer_entity',
		]
		table_address = ['customer_address_entity_datetime',
		                 'customer_address_entity_int',
		                 'customer_address_entity_varchar',
		                 'customer_address_entity_text',
		                 'customer_address_entity_decimal']
		for table in tables:
			where = ' WHERE entity_id IN ' + customer_id_con
			if table == 'newsletter_subscriber':
				where = ' WHERE customer_id IN ' + customer_id_con
			if table == 'customer_address_entity':
				where = ' WHERE parent_id IN ' + customer_id_con
			if table in table_address:
				where = ' WHERE entity_id IN (SELECT entity_id FROM _DBPRF_customer_address_entity WHERE parent_id IN ' + customer_id_con + ')'

			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "` " + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
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
			order_id_map = duplicate_field_value_from_list(orders['data'], 'id_desc')
			order_ids = list(set(order_ids + order_id_map))
		if not order_ids:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		self.delete_target_order(order_ids)
		self.delete_map_demo(self.TYPE_ORDER, order_ids)
		if to_len(order_ids) < self.LIMIT_CLEAR_DEMO:
			self._notice['target']['clear_demo'] = next_clear
			return next_clear
		return self._notice['target']['clear_demo']

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
			'tax_class',
			'tax_calculation_rate',
			'tax_calculation_rule',
			'tax_calculation',
		]
		for table in tables:
			where = ''
			if table == 'tax_class':
				where = ' WHERE class_type = "PRODUCT"'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
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
		if not self._notice['config']['categories']:
			self._notice['target']['clear']['result'] = 'process'
			self._notice['target']['clear']['function'] = ''
			self._notice['target']['clear']['table_index'] = 0
			self._notice['target']['clear']['msg'] = ''
			return self._notice['target']['clear']
		tables = [
			'catalog_category_entity_datetime',
			'catalog_category_entity_decimal',
			'catalog_category_entity_int',
			'catalog_category_entity_text',
			'catalog_category_entity_varchar',
			'catalog_category_entity',
			'url_rewrite',
		]

		root_category_ids = list()

		root_category_data = self._notice['target']['category_data']
		for key, value in root_category_data.items():
			root_category_ids.append(key)
		root_category_ids.append(1)
		root_ids_in_condition = self.list_to_in_condition(root_category_ids)
		for table in tables:
			where = ' WHERE entity_id NOT IN ' + root_ids_in_condition
			if table == 'url_rewrite':
				where = ' WHERE entity_type like "category"'
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

	def clear_target_products(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_customers',
		}
		if not self._notice['config']['products']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'catalog_category_product_index',
			'catalog_category_product',
			'cataloginventory_stock_status_idx',
			'cataloginventory_stock_status',
			'cataloginventory_stock_item',
			'catalog_product_website',
			'catalog_product_super_attribute_label',
			'catalog_product_super_link',
			'catalog_product_relation',
			'catalog_product_super_attribute',
			# 'eav_attribute_option',
			# 'eav_attribute_option_value',
			# 'eav_attribute',
			# 'eav_entity_attribute',
			# 'catalog_eav_attribute',
			'catalog_product_option_price',
			'catalog_product_option_title',
			'catalog_product_option_type_price',
			'catalog_product_option_type_title',
			'catalog_product_option_type_value',
			'catalog_product_option',
			'url_rewrite',
			'catalog_product_entity_media_gallery_value_to_entity',
			'catalog_product_entity_media_gallery_value',
			'catalog_product_entity_media_gallery',
			'catalog_product_entity_tier_price',
			'catalog_product_entity_varchar',
			'catalog_product_entity_datetime',
			'catalog_product_entity_decimal',
			'catalog_product_entity_text',
			'catalog_product_entity_int',
			'catalog_product_link',
			'catalog_product_bundle_selection',
			'catalog_product_bundle_option_value',
			'catalog_product_bundle_option',
			'catalog_product_entity',
		]
		if self.convert_version(self._notice['target']['config']['version'], 2) >= 230:
			tables.insert(0, 'inventory_stock_1')
		for table in tables:
			where = ''
			if table == 'url_rewrite':
				where = ' WHERE entity_type like "product"'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	def clear_target_customers(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_orders',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['customers']:
			return next_clear
		tables = [
			'customer_entity_datetime',
			'customer_entity_int',
			'customer_entity_varchar',
			'customer_entity_text',
			'customer_entity_decimal',
			'customer_grid_flat',
			'customer_address_entity',
			'customer_address_entity_datetime',
			'customer_address_entity_int',
			'customer_address_entity_varchar',
			'customer_address_entity_text',
			'customer_address_entity_decimal',
			'newsletter_subscriber',
			'customer_entity',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_orders(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_reviews',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['orders']:
			return next_clear
		tables = [
			'sales_creditmemo',
			'sales_creditmemo_grid',
			'sales_creditmemo_item',
			'sales_order_grid',
			'sales_shipment',
			'sales_shipment_grid',
			'sales_shipment_item',
			'sales_order_address',
			'sales_order_payment',
			'sales_order_status_history',
			'sales_order_item',
			'sales_invoice_item',
			'sales_invoice_grid',
			'sales_invoice',
			'sales_order',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_reviews(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_pages',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['reviews']:
			return next_clear
		tables = [
			'review',
			'review_detail',
			'review_store',
			'review_entity_summary'
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_pages(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_blogs',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['pages']:
			return next_clear
		tables = [
			'url_rewrite',
			'cms_page_store',
			'cms_page',
		]
		for table in tables:
			where = ''
			if table == 'url_rewrite':
				where = ' WHERE entity_type like "cms-page" '
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_blogs(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_coupons',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['blogs']:
			return next_clear
		tables = [
			'cms_blog_store',
			'cms_block',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_coupons(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_cartrules',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['coupons']:
			return next_clear
		tables = [
			'salesrule_label',
			'salesrule_customer_group',
			'salesrule_customer',
			'salesrule_coupon_usage',
			'salesrule_coupon',
			'salesrule',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_cartrules(self):
		next_clear = {
			'result': 'success',
			'function': '',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config'].get('cartrules'):
			return next_clear
		tables = [
			'catalogrule_website',
			'catalogrule_customer_group',
			'catalogrule_product_price',
			'catalogrule_product',
			'catalogrule_group_website',
			'catalogrule',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
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
			'query': "SELECT * FROM _DBPRF_tax_class WHERE class_type = 'PRODUCT' AND class_id > " + to_str(
				id_src) + " ORDER BY class_id ASC LIMIT " + to_str(limit)
		}
		taxes = self.select_data_connector(query, 'taxes_primary')

		if not taxes or taxes['result'] != 'success':
			return response_error('could not get taxes main to export')
		return taxes

	def get_taxes_ext_export(self, taxes):
		url_query = self.get_connector_url('query')
		tax_product_class_ids = duplicate_field_value_from_list(taxes['data'], 'class_id')
		taxes_ext_queries = {
			'tax_calculation': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_calculation WHERE product_tax_class_id IN " +
				         self.list_to_in_condition(
					         tax_product_class_ids),
			}

		}
		taxes_ext = self.select_multiple_data_connector(taxes_ext_queries, 'taxes')

		if not taxes_ext or taxes_ext['result'] != 'success':
			return response_error()
		tax_customer_class_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'],
		                                                         'customer_tax_class_id')
		tax_rule_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'], 'tax_calculation_rule_id')
		tax_rate_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'], 'tax_calculation_rate_id')
		taxes_ext_rel_queries = {
			'tax_class': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_class WHERE class_id IN " + self.list_to_in_condition(
					tax_customer_class_ids),
			},
			'tax_calculation_rate': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_calculation_rate WHERE tax_calculation_rate_id IN " +
				         self.list_to_in_condition(
					         tax_rate_ids),
			},
			'tax_calculation_rule': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_calculation_rule WHERE tax_calculation_rule_id IN " +
				         self.list_to_in_condition(
					         tax_rule_ids),
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group WHERE tax_class_id IN " + self.list_to_in_condition(
					tax_customer_class_ids)
			},
		}

		taxes_ext_rel = self.select_multiple_data_connector(taxes_ext_rel_queries, 'taxes')
		if not taxes_ext_rel or taxes_ext_rel['result'] != 'success':
			return response_error()
		taxes_ext = self.sync_connector_object(taxes_ext, taxes_ext_rel)
		return taxes_ext

	def convert_tax_export(self, tax, taxes_ext):
		tax_zones = list()
		tax_customers = list()
		tax_rules = list()
		tax_product = self.construct_tax_product()

		tax_data = self.construct_tax()
		tax_data['id'] = tax['class_id']
		tax_data['name'] = tax['class_name']
		tax_data['type'] = tax['class_type']
		tax_data['created_at'] = get_current_time()
		tax_data['updated_at'] = get_current_time()
		tax_products = list()
		tax_product['id'] = tax['class_id']
		tax_product['code'] = None
		tax_product['name'] = tax['class_name']
		tax_products.append(tax_product)
		tax_calculation_rate_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'], 'tax_calculation_rate_id')
		if tax_calculation_rate_ids:
			for tax_calculation_rate_id in tax_calculation_rate_ids:
				tax_zone_country = self.construct_tax_zone_country()
				tax_zone_state = self.construct_tax_zone_state()
				tax_zone_rate = self.construct_tax_zone_rate()
				tax_calculation_rate = get_row_from_list_by_field(taxes_ext['data']['tax_calculation_rate'], 'tax_calculation_rate_id', tax_calculation_rate_id)
				if tax_calculation_rate:
					tax_zone_rate['id'] = tax_calculation_rate_id
					tax_zone_rate['name'] = tax_calculation_rate['code']
					tax_zone_rate['rate'] = tax_calculation_rate['rate']
					tax_zone_rate['priority'] = 1

					tax_zone_state['id'] = tax_calculation_rate[
						'tax_region_id'] if 'tax_region_id' in tax_calculation_rate else 0
					tax_zone_state['state_code'] = tax_calculation_rate[
						'code'] if 'code' in tax_calculation_rate else ''

					tax_zone_country['country_code'] = tax_calculation_rate['tax_country_id']

					tax_zone = self.construct_tax_zone()
					tax_zone['id'] = tax_calculation_rate_id
					tax_zone['name'] = tax['class_name']
					tax_zone['country'] = tax_zone_country
					tax_zone['state'] = tax_zone_state
					tax_zone['rate'] = tax_zone_rate
					tax_zones.append(tax_zone)
		tax_calculation_rule_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'], 'tax_calculation_rule_id')
		if tax_calculation_rule_ids:
			for tax_calculation_rule_id in tax_calculation_rule_ids:
				tax_calculation_rate = get_row_from_list_by_field(taxes_ext['data']['tax_calculation_rule'], 'tax_calculation_rule_id', tax_calculation_rule_id)
				if tax_calculation_rate:
					tax_rule = dict()
					tax_rule['id'] = tax_calculation_rate['tax_calculation_rule_id']
					tax_rule['code'] = tax_calculation_rate['code']
					tax_rules.append(tax_rule)

		tax_customer_class_ids = duplicate_field_value_from_list(taxes_ext['data']['tax_calculation'], 'customer_tax_class_id')
		if tax_customer_class_ids:
			for tax_customer_class_id in tax_customer_class_ids:
				tax_customer = self.construct_tax_customer()
				tax_customer_data = get_row_from_list_by_field(taxes_ext['data']['tax_class'], 'class_id', tax_customer_class_id)
				if tax_customer_data:
					tax_customer['id'] = tax_customer_data['class_id']
					tax_customer['code'] = tax_customer_data['class_name']
					tax_customers.append(tax_customer)

		tax_data['tax_products'] = tax_products
		tax_data['tax_customers'] = tax_customers
		tax_data['tax_zones'] = tax_zones
		tax_data['tax_rules'] = tax_rules
		tax_data['tax_calculation'] = get_list_from_list_by_field(taxes_ext['data']['tax_calculation'], 'product_tax_class_id', tax['class_id'])
		return response_success(tax_data)

	def get_tax_id_import(self, convert, tax, taxes_ext):
		return tax['class_id']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX_PRODUCT, convert['id'], convert['code']) else False

	def router_tax_import(self, convert, tax, taxes_ext):
		return response_success('tax_import')

	def before_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def tax_import(self, convert, tax, taxes_ext):
		tax_class_data = {
			'class_name': convert['name'],
			'class_type': convert['type'] if 'type' in convert else 'PRODUCT',
		}
		tax_class_id = self.import_tax_data_connector(self.create_insert_query_connector('tax_class', tax_class_data), True, convert['id'])
		if not tax_class_id:
			return response_error()
		self.insert_map(self.TYPE_TAX_PRODUCT, convert['id'], tax_class_id, convert['code'])
		return response_success(tax_class_id)

	def after_tax_import(self, tax_id, convert, tax, taxes_ext):
		customer_class_ids = list()
		tax_calculation_rate_ids = list()
		tax_calculation_rule_ids = list()
		for tax_zone in convert['tax_zones']:
			tax_calculation_rate_id = self.get_map_field_by_src(self.TYPE_TAX_RATE, tax_zone['id'])
			if tax_calculation_rate_id:
				tax_calculation_rate_ids.append(tax_calculation_rate_id)
				continue
			tax_region_id = self.get_region_id_from_state_code(tax_zone['state']['code'], tax_zone['country']['country_code'])
			tax_calculation_rate_data = {
				'tax_country_id': tax_zone['country']['country_code'],
				'tax_region_id': tax_region_id,
				'code': convert['name'],  # tax_zone['state']['state_code'],
				'rate': tax_zone['rate']['rate'],
				'tax_postcode': get_value_by_key_in_dict(tax_zone, 'postcode', '*')
			}
			tax_calculation_rate_id = self.import_data_connector(self.create_insert_query_connector('tax_calculation_rate', tax_calculation_rate_data), 'tax')
			if not tax_calculation_rate_id:
				continue
			tax_calculation_rate_ids.append(tax_calculation_rate_id)
			self.insert_map(self.TYPE_TAX_RATE, tax_zone['id'], tax_calculation_rate_id, tax_zone['code'])
		if convert['tax_customers']:
			for tax_customer in convert['tax_customers']:
				customer_tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_CUSTOMER, tax_customer['id'])
				if customer_tax_class_id:
					customer_class_ids.append(customer_tax_class_id)
					continue
				tax_class_data = {
					'class_name': tax_customer['code'],
					'class_type': 'CUSTOMER',
				}
				customer_tax_class_id = self.import_data_connector(self.create_insert_query_connector('tax_class', tax_class_data), 'tax')
				if not customer_tax_class_id:
					continue
				customer_class_ids.append(customer_tax_class_id)
				self.insert_map(self.TYPE_TAX_CUSTOMER, tax_customer['id'], customer_tax_class_id, tax_customer['code'])
		else:
			customer_tax_id = self.get_tax_customer()
			if customer_tax_id:
				customer_class_ids.append(customer_tax_id)
		if 'tax_rules' in convert:
			for tax_rule in convert['tax_rules']:
				if not tax_rule.get('code'):
					continue
				tax_calculation_rule_id = self.get_map_field_by_src('tax_rule', tax_rule['id'])
				if tax_calculation_rule_id:
					tax_calculation_rule_ids.append(tax_calculation_rule_id)
					continue
				tax_calculation_rule_data = {
					'code': tax_rule.get('code'),
					'priority': 0,
					'position': 0,
					'calculate_subtotal': 0
				}
				tax_calculation_rule_id = self.import_data_connector(self.create_insert_query_connector('tax_calculation_rule', tax_calculation_rule_data), 'tax')
				if not tax_calculation_rule_id:
					continue
				tax_calculation_rule_ids.append(tax_calculation_rule_id)
				self.insert_map('tax_rule', tax_rule['id'], tax_calculation_rule_id, tax_rule['code'])
		else:
			tax_rule_id = self.get_tax_rule()
			if tax_rule_id:
				tax_calculation_rule_ids.append(tax_rule_id)
		# if 'tax_calculation' in convert and isinstance(convert['tax_calculation'], list):
		# 	for item in convert['tax_calculation']:
		# 		if (item['customer_tax_class_id'] in customer_class_ids) and (
		# 				item['tax_calculation_rate_id'] in tax_calculation_rate_ids) and (
		# 				item['tax_calculation_rule_id'] in tax_calculation_rule_ids):
		# 			tax_calculation_data = {
		# 				'tax_calculation_rate_id': tax_calculation_rate_ids[item['tax_calculation_rate_id']],
		# 				'tax_calculation_rule_id': tax_calculation_rule_ids[item['tax_calculation_rule_id']],
		# 				'customer_tax_class_id': customer_class_ids[item['customer_tax_class_id']],
		# 				'product_tax_class_id': tax_id,
		# 			}
		# 			self.import_data_connector(
		# 				self.create_insert_query_connector('tax_calculation', tax_calculation_data), 'tax')
		# else:
		all_queries = list()
		for customer_class_id in customer_class_ids:
			for tax_calculation_rate_id in tax_calculation_rate_ids:
				for tax_calculation_rule_id in tax_calculation_rule_ids:
					tax_calculation_data = {
						'tax_calculation_rate_id': tax_calculation_rate_id,
						'tax_calculation_rule_id': tax_calculation_rule_id,
						'customer_tax_class_id': customer_class_id,
						'product_tax_class_id': tax_id,
					}
					all_queries.append(self.create_insert_query_connector('tax_calculation', tax_calculation_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'tax')
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
			'query': "SELECT eao.* FROM _DBPRF_eav_attribute as ea LEFT JOIN _DBPRF_eav_attribute_option as eao ON "
			         "ea.attribute_id = eao.attribute_id WHERE ea.attribute_code = 'manufacturer' AND eao.option_id > " + to_str(id_src) + " ORDER BY eao.option_id ASC LIMIT " + to_str(limit)
		}
		manufacturers = self.select_data_connector(query, 'manufacturers_primary')

		if not manufacturers or manufacturers['result'] != 'success':
			return response_error()
		return manufacturers

	def get_manufacturers_ext_export(self, manufacturers):
		option_ids = duplicate_field_value_from_list(manufacturers['data'], 'option_id')
		manufacturers_ext_query = {
			'eav_attribute_option_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute_option_value WHERE option_id IN " +
				         self.list_to_in_condition(
					         option_ids)
			}
		}
		manufacturers_ext = self.select_multiple_data_connector(manufacturers_ext_query, 'manufacturers')

		if not manufacturers_ext or (manufacturers_ext['result'] != 'success'):
			return response_error()
		manufacturers_ext_rel_queries = {
		}
		if manufacturers_ext_rel_queries:
			manufacturers_ext_rel = self.select_multiple_data_connector(manufacturers_ext_rel_queries, 'manufacturers')

			if not manufacturers_ext_rel or manufacturers_ext_rel['result'] != 'success':
				return response_error()
			manufacturers_ext = self.sync_connector_object(manufacturers_ext, manufacturers_ext_rel)
		return manufacturers_ext

	def convert_manufacturer_export(self, manufacturer, manufacturers_ext):
		manufacturer_data = self.construct_manufacturer()
		manufacturer_data['id'] = manufacturer['option_id']
		manufacturer_desc = get_list_from_list_by_field(manufacturers_ext['data']['eav_attribute_option_value'],
		                                                'option_id', manufacturer['option_id'])
		manufacturer_data['name'] = get_row_value_from_list_by_field(manufacturer_desc, 'store_id', 0, 'value')
		return response_success(manufacturer_data)

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		return manufacturer['option_id']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id'], convert['code']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		url_query = self.get_connector_url('query')
		product_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = 4 and attribute_code = 'manufacturer'"
			},
		}
		product_eav_attribute = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(product_eav_attribute_queries)
		})
		try:
			attribute_id = product_eav_attribute['data']['eav_attribute'][0]['attribute_id']
		except Exception:
			attribute_id = self.create_attribute('manufacturer', 'int', 'select', 4, 'Manufacturer')
		if not attribute_id:
			response_error('can not get attribute code manufacturer')
		option_id = self.check_option_exist(convert['name'], 'manufacturer')
		if not option_id:
			eav_attribute_option_data = {
				'attribute_id': attribute_id,
				'sort_order': 0
			}
			option_id = self.import_manufacturer_data_connector(
				self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data), True,
				convert['id'])
			if not option_id:
				return response_error('Error import manufacturer')
			eav_attribute_option_value_data = {
				'option_id': option_id,
				'store_id': 0,
				'value': convert['name'],
			}
			self.import_manufacturer_data_connector(
				self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
		self.insert_map(self.TYPE_MANUFACTURER, convert['id'], option_id, convert['code'])
		return response_success(option_id)

	def after_manufacturer_import(self, manufacturer_id, convert, manufacturer, manufacturers_ext):

		return response_success()

	def addition_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	# TODO: CATEGORY
	def prepare_categories_import(self):
		return self

	def prepare_categories_export(self):
		return self

	def get_categories_main_export(self):
		id_src = self._notice['process']['categories']['id_src']
		limit = self._notice['setting']['categories']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_catalog_category_entity WHERE level > 1 AND entity_id > " + to_str(id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit)
		}
		categories = self.select_data_connector(query, 'categories')

		if not categories or categories['result'] != 'success':
			return response_error()
		return categories

	def get_categories_ext_export(self, categories):
		url_query = self.get_connector_url('query')
		category_ids = duplicate_field_value_from_list(categories['data'], 'entity_id')
		category_id_query = self.list_to_in_condition(category_ids)
		store_ids = self.get_all_store_select()
		store_id_con = self.list_to_in_condition(store_ids)
		categories_ext_queries = {
			'catalog_category_entity_varchar': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_varchar WHERE entity_id IN " +
				         category_id_query + " AND store_id IN " + store_id_con
			},
			'catalog_category_entity_text': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_text WHERE entity_id IN " + category_id_query +
				         " AND store_id IN " + store_id_con,
			},
			'catalog_category_entity_int': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_int WHERE entity_id IN " + category_id_query +
				         " AND store_id IN " + store_id_con,
			},
			'catalog_category_entity_decimal': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_decimal WHERE entity_id IN " +
				         category_id_query + " AND store_id IN " + store_id_con,
			},
			'catalog_category_entity_datetime': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_datetime WHERE entity_id IN " +
				         category_id_query + " AND store_id IN " + store_id_con,
			},
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['src']['extends'][
					'catalog_category']
			},
			'url_rewrite': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_url_rewrite WHERE entity_id IN " +
				         category_id_query + " AND is_autogenerated = 1 AND entity_type = 'category'"
				# + " AND store_id IN " + store_id_con
			}
		}
		categories_ext = self.select_multiple_data_connector(categories_ext_queries, 'categories_primary')

		if not categories_ext or (categories_ext['result'] != 'success'):
			return response_error()
		categories_ext_rel_queries = {
		}
		if categories_ext_rel_queries:
			categories_ext_rel = self.get_connector_data(self.get_connector_url('query'), {
				'serialize': True,
				'query': json.dumps(categories_ext_rel_queries)
			})
			if not categories_ext_rel or categories_ext_rel['result'] != 'success':
				return response_error()
			categories_ext = self.sync_connector_object(categories_ext, categories_ext_rel)
		return categories_ext

	def convert_category_export(self, category, categories_ext):
		category['level'] = to_len(category['path'].split('/'))-1
		category_data = self.construct_category()
		parent = self.construct_category_parent()
		category_data = self.add_construct_default(category_data)
		parent = self.add_construct_default(parent)
		code_parent = ''
		parent['level'] = 1
		if category['parent_id'] and to_int(category['level']) > 2:
			parent['id'] = category['parent_id']
			parent_data = self.get_categories_parent(category['parent_id'])
			if parent_data['result'] == 'success' and parent_data['data']:
				parent = parent_data['data']
				code_parent = parent_data['url_key'] if 'url_key' in parent_data else ''
		eav_attribute = dict()
		for row in categories_ext['data']['eav_attribute']:
			eav_attribute[row['attribute_code']] = row['attribute_id']
		entity_varchar = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_varchar'],'entity_id', category['entity_id'])
		entity_text = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_text'], 'entity_id',category['entity_id'])
		entity_int = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_int'], 'entity_id',category['entity_id'])

		language_default = self._notice['src']['language_default']
		is_active = get_list_from_list_by_field(entity_int, 'attribute_id', eav_attribute['is_active'])
		is_active_def = self.get_data_default(is_active, 'store_id', language_default, 'value')
		include_in_menu = get_list_from_list_by_field(entity_int, 'attribute_id', eav_attribute['include_in_menu'])
		include_in_menu_def = self.get_data_default(include_in_menu, 'store_id', language_default, 'value')
		images = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['image'])
		image_def_path = self.get_data_default(images, 'store_id', language_default, 'value')
		category_data['id'] = category['entity_id']
		url_key = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_key'], 'value')
		category_data['code'] = url_key
		category_data['level'] = to_int(parent['level']) + 1
		category_data['parent'] = parent
		category_data['active'] = True if to_int(is_active_def) == 1 else False
		if image_def_path:
			category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_category'])
			category_data['thumb_image']['path'] = image_def_path
		category_data['sort_order'] = 1
		category_data['created_at'] = category['created_at']
		category_data['updated_at'] = category['updated_at']
		category_data['category'] = category
		category_data['categories_ext'] = categories_ext
		category_data['include_in_menu'] = to_int(include_in_menu_def)

		names = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['name'])
		name_def = self.get_data_default(names, 'store_id', language_default, 'value')
		descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['description'])
		description_def = self.get_data_default(descriptions, 'store_id', language_default, 'value')
		meta_titles = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['meta_title'])
		meta_title_def = self.get_data_default(meta_titles, 'store_id', language_default, 'value')
		meta_keywords = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['meta_keywords'])
		meta_keywords_def = self.get_data_default(meta_keywords, 'store_id', language_default, 'value')
		meta_descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['meta_description'])
		meta_description_def = self.get_data_default(meta_descriptions, 'store_id', language_default, 'value')
		is_anchor = get_list_from_list_by_field(entity_int, 'attribute_id', eav_attribute['is_anchor'])
		is_anchor_def = self.get_data_default(is_anchor, 'store_id', language_default, 'value')
		display_mode = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['display_mode'])
		display_mode_def = self.get_data_default(display_mode, 'store_id', language_default, 'value')
		url_key = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_key'])
		url_key_def = self.get_data_default(url_key, 'store_id', language_default, 'value')
		url_path = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_path'])
		url_path_def = self.get_data_default(url_path, 'store_id', language_default, 'value')
		category_data['is_anchor'] = is_anchor_def if is_anchor_def else 0
		category_data['url_key'] = url_key_def if url_key_def else ''
		category_data['code'] = code_parent + '/' + url_key_def if code_parent else url_key_def
		category_data['url_path'] = url_path_def if url_path_def else ''
		category_data['display_mode'] = display_mode_def if display_mode_def else ''
		category_data['name'] = self.convert_image_in_description(name_def if name_def else '')
		category_data['description'] = self.convert_image_in_description(description_def if description_def else '')
		category_data['meta_title'] = meta_title_def if meta_title_def else ''
		category_data['meta_keywords'] = meta_keywords_def if meta_keywords_def else ''
		category_data['meta_description'] = meta_description_def if meta_description_def else ''
		# Maximum 255 chars
		category_data['meta_description'] = category_data['meta_description'][:255] if category_data['meta_description'] else ''

		# for
		for language_id, label in self._notice['src']['languages'].items():
			category_language_data = self.construct_category_lang()
			name_lang = get_row_value_from_list_by_field(names, 'store_id', language_id, 'value')
			description_lang = get_row_value_from_list_by_field(descriptions, 'store_id', language_id, 'value')
			meta_title_lang = get_row_value_from_list_by_field(meta_titles, 'store_id', language_id, 'value')
			meta_keyword_lang = get_row_value_from_list_by_field(meta_keywords, 'store_id', language_id, 'value')
			meta_description_lang = get_row_value_from_list_by_field(meta_descriptions, 'store_id', language_id,
			                                                         'value')
			display_mode_lang = get_row_value_from_list_by_field(display_mode, 'store_id', language_id, 'value')
			url_key_lang = get_row_value_from_list_by_field(url_key, 'store_id', language_id, 'value')
			url_path_lang = get_row_value_from_list_by_field(url_path, 'store_id', language_id, 'value')
			is_anchor_lang = get_row_value_from_list_by_field(is_anchor, 'store_id', language_id, 'value')
			category_language_data['name'] = name_lang if name_lang else category_data['name']
			category_language_data['description'] = self.convert_image_in_description(description_lang if description_lang else category_data['description'])
			category_language_data['meta_title'] = meta_title_lang if meta_title_lang else category_data['meta_title']
			category_language_data['meta_keywords'] = meta_keyword_lang if meta_keyword_lang else category_data['meta_keywords']
			category_language_data['meta_description'] = meta_description_lang if meta_description_lang else category_data['meta_description']
			# Maximum 255 chars
			category_language_data['meta_description'] = category_language_data['meta_description'][:255] if category_language_data['meta_description'] else ''
			category_language_data['display_mode'] = display_mode_lang
			category_language_data['url_key'] = url_key_lang if url_key_lang else category_data['url_key']

			category_language_data['url_path'] = url_path_lang
			category_language_data['is_anchor'] = is_anchor_lang
			category_data['languages'][language_id] = category_language_data
		# endfor

		url_rewrite = get_list_from_list_by_field(categories_ext['data']['url_rewrite'], 'entity_id',
		                                          category['entity_id'])
		category_data['url_rewrite'] = list()
		for rewrite in url_rewrite:
			rewrite_data = dict()
			rewrite_data['store_id'] = rewrite['store_id']
			rewrite_data['request_path'] = rewrite['request_path']
			rewrite_data['description'] = rewrite['description']
			category_data['url_rewrite'].append(rewrite_data)

		detect_seo = self.detect_seo()
		category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)
		return response_success(category_data)

	def get_category_id_import(self, convert, category, categories_ext):
		return category['entity_id']

	def check_category_import(self, convert, category, categories_ext):
		return self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code'])

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def category_import(self, convert, category, categories_ext):
		parent_data = list()
		if convert['parent'] and (convert['parent']['id'] != convert['id']) and (
				convert['parent']['id'] or convert['parent']['code']):
			parent_import = self.import_category_parent(convert['parent'])
			if parent_import['result'] != 'success':
				return response_warning('Category id: ' + to_str(convert['id']) + 'Could not import parent')
			parent_data = parent_import['data']
		elif (convert['parent']['id'] != convert['id']) and (convert['parent']['id'] or convert['parent']['code']) and \
				convert['parent']['id'] in self._notice['map']['category_data']:
			parent_ids = self._notice['map']['category_data'][convert['parent']['id']]
			for parent_id in parent_ids:
				row = {
					'parent_id': parent_id,
					'cate_path': '1/' + parent_id,
					'value': '',
				}
				parent_data.append(row)

		else:
			if not self._notice['support']['site_map']:
				parent_id = self._notice['target']['store_category'][to_str(self.get_map_store_view(self._notice['src']['language_default']))] if to_str(self.get_map_store_view(self._notice['src']['language_default'])) in self._notice['target']['store_category'] else self._notice['target']['store_category'][str(self._notice['target']['language_default'])]
				row = {
					'data': parent_id,
					'cate_path': '1/' + to_str(parent_id),
					'value': '',
				}
				parent_data.append(row)
			else:
				# for key, root_cate_id in self._notice['target']['category_data'].items():
				parent_id = self._notice['target']['store_category'].get(to_str(self._notice['target']['language_default']))
				if not parent_id:
					for store_id, root_cate_id in self._notice['target']['store_category'].items():
						parent_id = root_cate_id
						break
				row = {
					'data': parent_id,
					'cate_path': '1/' + to_str(parent_id),
					'value': '',
				}
				parent_data.append(row)
		response = list()
		for parent_row in parent_data:
			parent_id = parent_row['data']
			cate_path = parent_row['cate_path']
			category_data = {
				'attribute_set_id': 3,
				'parent_id': parent_id,
				'created_at': convert['created_at'] if convert['created_at'] and convert['created_at'] != '0000-00-00 00:00:00' else get_current_time(),
				'updated_at': convert['updated_at'] if convert['updated_at'] and convert['updated_at'] != '0000-00-00 00:00:00' else get_current_time(),
				'path': cate_path,
				'position': convert.get('sort_order', 0),
				'level': to_len(cate_path.split('/')),
				'children_count': convert.get('category', {}).get('children_count', 0)
			}
			category_id = self.import_category_data_connector(self.create_insert_query_connector('catalog_category_entity', category_data), True, convert['id'])
			if not category_id:
				return response_warning(self.warning_import_entity(self.TYPE_CATEGORY, convert['id']))
			url_key = self.convert_attribute_code(convert['name'].replace('&',''))
			if parent_row['value']:
				url_key = parent_row['value'] + '/' + url_key
			self.insert_map(self.TYPE_CATEGORY, convert['id'], category_id, convert['code'], cate_path + '/' + to_str(category_id), url_key)
			update_path = self.import_category_data_connector(
				self.create_update_query_connector('catalog_category_entity', {'path': cate_path + '/' + to_str(category_id)}, {'entity_id': category_id}), False)
			if not update_path:
				return response_error(self.warning_import_entity(self.TYPE_CATEGORY, convert['id']))

			response_row = response_success(category_id)
			response_row['cate_path'] = cate_path + '/' + to_str(category_id)
			response_row['value'] = url_key
			response.append(response_row)
		return response_success(response)

	def after_category_import(self, category_id, convert, category, categories_ext):
		all_queries = list()
		category_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = 3",
			}
		}
		category_eav_attribute = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(category_eav_attribute_queries)
		})
		category_eav_attribute_data = dict()
		for attribute in category_eav_attribute['data']['eav_attribute']:
			if attribute['backend_type'] != 'static':
				if not attribute['attribute_code'] in category_eav_attribute_data:
					category_eav_attribute_data[attribute['attribute_code']] = dict()
				category_eav_attribute_data[attribute['attribute_code']]['attribute_id'] = attribute['attribute_id']
				category_eav_attribute_data[attribute['attribute_code']]['backend_type'] = attribute['backend_type']
		image_name = None
		if convert['thumb_image']['url'] or convert['thumb_image']['path'] and self.image_exist(convert['thumb_image']['url'], convert['thumb_image']['path']):
			if (not ('ignore_image' in self._notice['config'])) or (not self._notice['config']['ignore_image']):
				image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
				image_name = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_magento_image_path(image_process['path']) + os.path.basename(image_process['path']), self._notice['target']['config']['image_category']))
				if image_name:
					image_name = self.remove_prefix_path(image_name, self._notice['target']['config']['image_category'])
			else:
				image_name = convert['thumb_image']['path']

		cate_url_key = self.get_category_url_key(convert.get('url_key'), 0, self.convert_attribute_code(convert['name'].replace('&','')), category_id = category_id)
		cate_url_path = self.get_category_url_path(cate_url_key, 0, cate_url_key)
		insert_attribute_data = {
			'all_children': None,
			'available_sort_by': None,
			'children': None,
			'children_count': None,
			'custom_apply_to_products': 0,
			'custom_design': None,
			'custom_design_from': None,
			'custom_design_to': None,
			'custom_layout_update': None,
			'custom_use_parent_settings': 0,
			'default_sort_by': None,
			'filter_price_range': None,
			'image': image_name if image_name else None,
			'include_in_menu': convert.get('include_in_menu', 1),
			'is_active': 1 if convert['active'] == True else 0,
			'landing_page': None,
			'level': None,
			'meta_description': convert['meta_description'],
			'meta_keywords': convert['meta_keyword'],
			'meta_title': convert['meta_title'],
			'name': self.strip_html_tag(convert['name']),
			'description': self.change_img_src_in_text(convert['description']),
			'display_mode': "PRODUCTS",
			'page_layout': None,
			'path': None,
			'path_in_store': None,
			'position': None,
			'url_key': cate_url_key.lower() if cate_url_key else None,
			'url_path': cate_url_path,
			'is_anchor': 1,
		}

		# for
		for key1, value1 in category_eav_attribute_data.items():

			# for
			for key2, value2 in insert_attribute_data.items():
				if key1 == key2:
					if key2 != 'include_in_menu' and value2 != 0:
						if not value2:
							continue
					value3 = value2
					category_attr_data = {
						'attribute_id': value1['attribute_id'],
						'store_id': 0,
						'entity_id': category_id,
						'value': value2,
					}
					all_queries.append(
						self.create_insert_query_connector('catalog_category_entity_' + value1['backend_type'], category_attr_data))
		# endfor

		# endfor

		# if
		if convert['languages']:

			# for
			for language_id, language_data in convert['languages'].items():
				insert_attribute_data = {
					'meta_description': language_data['meta_description'],
					'meta_keywords': language_data['meta_keyword'],
					'meta_title': language_data['meta_title'],
					'name': self.strip_html_tag(language_data['name']),
					'description': self.change_img_src_in_text(language_data['description']),
					'display_mode': "PRODUCTS",
					'url_key': get_value_by_key_in_dict(insert_attribute_data, 'url_key', '').lower(),
					# language_data['url_key'],
					'url_path': get_value_by_key_in_dict(insert_attribute_data, 'url_path', ''),
					# language_data['url_path'],
					'is_anchor': 1,
				}

				# for
				for key1, value1 in category_eav_attribute_data.items():

					# for
					for key2, value2 in insert_attribute_data.items():
						if key1 == key2:
							store_id = self.get_map_store_view(language_id)
							if to_int(store_id) == 0:
								continue
							if key2 == 'url_key':
								value2 = self.get_category_url_key(value2, store_id, language_data['name'], category_id = category_id)
							if key2 == 'url_path':
								value2 = self.get_category_url_path(value2, store_id)
							if not value2:
								continue
							category_attr_data = {
								'attribute_id': value1['attribute_id'],
								'store_id': store_id,
								'entity_id': category_id,
								'value': value2,
							}
							all_queries.append(
								self.create_insert_query_connector('catalog_category_entity_' + value1['backend_type'], category_attr_data))
		# endfor

		# endfor

		# endfor
		# endif
		seo_301 = self._notice['config']['seo_301']
		seo_default = ''
		if not self._notice['config']['seo'] or self._notice['config']['seo_301']:
			seo_default = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code'], 'value')
			if seo_default:
				store_target = list(self._notice['map']['languages'].values())
				store_target = list(map(lambda x: to_int(x), store_target))
				if 0 not in store_target:
					store_target.append(0)
				for store_id in store_target:
					url_rewrite_data = {
						'entity_type': 'category',
						'entity_id': category_id,
						'request_path': seo_default,
						'target_path': 'catalog/category/view/id/' + to_str(category_id),
						'redirect_type': 0,
						'store_id': store_id,
						'description': None,
						'is_autogenerated': 1,
						'metadata': None,
					}
					self.import_category_data_connector(self.create_insert_query_connector('url_rewrite', url_rewrite_data))

		is_default = False
		if (self._notice['config']['seo'] or seo_301) and 'seo' in convert:
			if seo_301:
				is_default = True
			for rewrite in convert['seo']:
				path = rewrite['request_path']
				if not path or path == seo_default:
					continue
				default = True if rewrite['default'] and not is_default else False
				if default:
					is_default = True
				store_id = self.get_map_store_view(rewrite.get('store_id', 0))
				path = self.get_request_path(path, store_id, 'category')
				url_rewrite_data = {
					'entity_type': 'category',
					'entity_id': category_id,
					'request_path': path,
					'target_path': 'catalog/category/view/id/' + to_str(category_id) if not seo_301 else seo_default,
					'redirect_type': 301 if seo_301 else 0,
					'store_id': store_id,
					'description': None,
					'is_autogenerated': 1 if default else 0,
					'metadata': None,
				}
				self.import_category_data_connector(self.create_insert_query_connector('url_rewrite', url_rewrite_data))

		if all_queries:
			self.import_multiple_data_connector(all_queries, 'category')
		del all_queries
		del category_eav_attribute_data
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	# TODO: PRODUCT
	def prepare_products_import(self):
		parent = super().prepare_products_import()
		product_eav_attribute_queries = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['catalog_product']),
		}
		product_eav_attribute = self.select_data_connector(product_eav_attribute_queries)
		if product_eav_attribute and product_eav_attribute['result'] == 'success' and product_eav_attribute['data']:
			for eav_attribute in product_eav_attribute['data']:
				self.insert_map(self.TYPE_ATTR, None, eav_attribute['attribute_id'], None, eav_attribute['attribute_code'], json_encode(eav_attribute))
		return self

	def prepare_products_export(self):
		return self

	def get_products_main_export(self):
		id_src = self._notice['process']['products']['id_src']
		limit = self._notice['setting']['products']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_catalog_product_entity WHERE entity_id IN (select product_id from _DBPRF_catalog_product_website where" + self.get_con_website_select_count() + ") AND "
			         "entity_id > " + to_str(id_src) + " AND entity_id NOT IN (SELECT product_id FROM _DBPRF_catalog_product_super_link) ORDER BY entity_id ASC LIMIT " + to_str(limit)
		}
		products = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})

		if not products or products['result'] != 'success':
			return response_error()
		return products

	def get_products_ext_export(self, products):
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = ' AND ' + to_str(store_id_con)
		url_query = self.get_connector_url('query')
		product_ids = duplicate_field_value_from_list(products['data'], 'entity_id')
		product_id_con = self.list_to_in_condition(product_ids)
		product_ext_queries = {
			'catalog_product_website': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_website WHERE product_id IN " + product_id_con,
			},
			'catalog_product_super_link': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_super_link WHERE parent_id IN " + product_id_con,
			},
			# 'catalog_product_relation': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_catalog_product_relation WHERE child_id IN " + product_id_con,
			# },
			# 'eav_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['src']['extends'][
			# 		'catalog_product']
			# },
			# 'eav_entity_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_eav_entity_attribute WHERE entity_type_id = " +
			# 	         self._notice['src']['extends']['catalog_product']
			# },
			'eav_attribute_option_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute_option_value"
			},
			'eav_attribute_option': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute_option"
			},
			'catalog_product_link': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_link WHERE product_id IN " + product_id_con + " OR linked_product_id IN" + product_id_con,
			},
			'catalog_product_link_grouped_product': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_link WHERE link_type_id = 3 and linked_product_id IN " + product_id_con,
			},
			'catalog_product_option': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_option WHERE product_id IN " + product_id_con
			},

			'downloadable_link': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_downloadable_link WHERE product_id IN " + product_id_con
			},
			'downloadable_sample': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_downloadable_sample WHERE product_id IN " + product_id_con
			},
			'catalog_product_bundle_option': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_bundle_option WHERE parent_id IN " + product_id_con
			},
			'catalog_product_entity_media_gallery_value_to_entity': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_catalog_product_entity_media_gallery_value_to_entity WHERE entity_id IN ' + product_id_con
			}
		}

		product_ext_queries['url_rewrite'] = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_url_rewrite WHERE entity_type = 'product' and entity_id IN " + product_id_con + store_id_con
		}
		product_ext = self.select_multiple_data_connector(product_ext_queries, 'products')

		if (not product_ext) or product_ext['result'] != 'success':
			return response_error()
		download_able_link_ids = duplicate_field_value_from_list(product_ext['data']['downloadable_link'], 'link_id')
		download_able_link_id_con = self.list_to_in_condition(download_able_link_ids)
		download_sample_link_ids = duplicate_field_value_from_list(product_ext['data']['downloadable_sample'],
		                                                           'sample_id')
		# parent_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_link'], 'parent_id')
		children_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_link'], 'product_id')
		allproduct_id_query = self.list_to_in_condition(list(set(product_ids + children_ids)))
		option_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_option'], 'option_id')
		option_id_query = self.list_to_in_condition(option_ids)
		link_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_link'], 'link_id')
		bundle_option_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_bundle_option'],
		                                                    "option_id")
		bundle_option_id_con = self.list_to_in_condition(bundle_option_ids)
		media_value_ids = duplicate_field_value_from_list(
			product_ext['data']['catalog_product_entity_media_gallery_value_to_entity'],
			"value_id")
		media_value_id_con = self.list_to_in_condition(media_value_ids)
		# attribute_ids = duplicate_field_value_from_list(product_ext['data']['eav_attribute'], "attribute_id")
		# attribute_ids_con = self.list_to_in_condition(attribute_ids)
		product_ext_rel_queries = {
			# 'catalog_eav_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_catalog_eav_attribute WHERE attribute_id IN " + attribute_ids_con +" and is_visible_on_front =1 "
			# },
			'catalog_product_link_attribute_decimal': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_link_attribute_decimal WHERE product_link_attribute_id = 3 and link_id IN " + self.list_to_in_condition(
					link_ids),
			},
			'catalog_product_super_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_super_attribute WHERE product_id IN " + allproduct_id_query,
			},
			'catalog_product_entity': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_product_entity WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_datetime': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_datetime WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_decimal': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_decimal WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_gallery': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_gallery WHERE entity_id IN " +
				         allproduct_id_query,
			},
			'catalog_product_entity_int': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_int WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_text': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_text WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_varchar': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_varchar WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_media_gallery': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_media_gallery WHERE value_id IN " +
				         media_value_id_con,
			},
			'catalog_product_entity_tier_price': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_tier_price WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_category_product': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_product WHERE product_id IN " + allproduct_id_query,
			},
			'cataloginventory_stock_item': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_cataloginventory_stock_item WHERE product_id IN " + allproduct_id_query,
			},
			'catalog_product_bundle_parent': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_bundle_selection WHERE product_id IN " + allproduct_id_query,
			},
			'catalog_product_option_title': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_option_title WHERE option_id IN " + option_id_query,
			},
			'catalog_product_option_price': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_option_price WHERE option_id IN " + option_id_query,
			},
			'catalog_product_option_type_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_option_type_value as cpotv WHERE cpotv.option_id IN " + option_id_query,
			},
			'catalog_product_bundle_option_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_bundle_option_value WHERE option_id IN " + bundle_option_id_con,
			},
			'catalog_product_bundle_selection': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_bundle_selection WHERE option_id IN " + bundle_option_id_con,
			},
			'downloadable_link_title': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_downloadable_link_title WHERE link_id IN " + download_able_link_id_con
			},
			'downloadable_link_price': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_downloadable_link_price WHERE link_id IN " + download_able_link_id_con
			},
			'downloadable_sample_title': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_downloadable_sample_title WHERE sample_id IN " + self.list_to_in_condition(
					download_sample_link_ids)
			},
		}

		product_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, 'products')

		if (not product_ext_rel) or (product_ext_rel['result'] != 'success'):
			return response_error()
		product_ext = self.sync_connector_object(product_ext, product_ext_rel)
		option_type_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_option_type_value'],
		                                                  'option_type_id')
		option_type_id_con = self.list_to_in_condition(option_type_ids)
		value_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_entity_media_gallery'],
		                                            'value_id')
		value_id_con = self.list_to_in_condition(value_ids)
		option_attr_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_entity_int'], 'value')
		option_attr_id_con = self.list_to_in_condition(option_attr_ids)
		eav_attribute = self.get_eav_attribute_product()

		multi = get_list_from_list_by_field(eav_attribute, 'frontend_input', 'multiselect')
		multi_ids = self.list_to_in_condition(multi)
		all_option = list()
		if multi:
			multi_option = get_list_from_list_by_field(product_ext['data']['catalog_product_entity_varchar'],
			                                           'attribute_id', multi_ids)
			for row in multi_option:
				if row['value']:
					new_option = row['value'].split(',')
					all_option = set(all_option + new_option)
		all_option_query = self.list_to_in_condition(all_option)
		super_attribute_id = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_attribute'],
		                                                     'product_super_attribute_id')
		super_attribute_id_query = self.list_to_in_condition(super_attribute_id)

		product_ext_rel_rel_queries = {
			'catalog_product_super_attribute_label': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_super_attribute_label WHERE store_id = 0 AND product_super_attribute_id IN " + super_attribute_id_query
			},
			'catalog_product_super_attribute_pricing': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_product_super_attribute_pricing WHERE product_super_attribute_id IN " + super_attribute_id_query
			},
			'eav_attribute_option_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute_option_value WHERE option_id IN " + option_attr_id_con + " OR option_id IN " + all_option_query,
			},
			'catalog_product_entity_media_gallery_value': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_media_gallery_value WHERE value_id IN " +
				         media_value_id_con,
			},
			# 'all_option': {
			# 	'type': "select",
			# 	'query': "SELECT a.option_id,a.attribute_id,b.value FROM _DBPRF_eav_attribute_option as a, _DBPRF_eav_attribute_option_value as b WHERE a.option_id = b.option_id and b.store_id = 0"
			# },
			'catalog_product_option_type_title': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_catalog_product_option_type_title WHERE option_type_id IN ' + option_type_id_con,
			},
			'catalog_product_option_type_price': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_catalog_product_option_type_price WHERE option_type_id IN ' + option_type_id_con,
			},

		}
		product_ext_rel_rel = self.select_multiple_data_connector(product_ext_rel_rel_queries, 'products')

		if (not product_ext_rel_rel) or (product_ext_rel_rel['result'] != 'success'):
			return response_error()
		product_ext = self.sync_connector_object(product_ext, product_ext_rel_rel)
		return product_ext

	def convert_product_export(self, product, products_ext):
		products_ext_data = products_ext['data']
		product_data = self.construct_product()
		entity_decimal = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_decimal'], 'entity_id', product['entity_id'])
		entity_int = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_int'], 'entity_id', product['entity_id'])
		entity_text = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_text'], 'entity_id', product['entity_id'])
		entity_varchar = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_varchar'], 'entity_id', product['entity_id'])
		entity_datetime = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_datetime'], 'entity_id', product['entity_id'])
		product_eav_attributes = self.get_eav_attribute_product()
		catalog_eav_attribute = self.get_catalog_eav_attribute()
		eav_attribute = dict()
		for row in product_eav_attributes:
			eav_attribute[row['attribute_code']] = row['attribute_id']

		price = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['price'], 'value')
		cost = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['cost'], 'value')
		weight = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['weight'], 'value')
		status = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute['status'], 'value')
		visibility = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute['visibility'], 'value')
		quantity = get_row_from_list_by_field(products_ext['data']['cataloginventory_stock_item'], 'product_id', product['entity_id'])

		product_data['id'] = product['entity_id']
		product_data['sku'] = product['sku']
		product_data['price'] = price if price else 0.0000
		product_data['cost'] = cost if cost else 0.0000
		product_data['weight'] = weight if weight else 0.0000
		product_data['status'] = True if to_int(status) == 1 and to_int(visibility) != 1 else False
		product_data['qty'] = to_decimal(get_value_by_key_in_dict(quantity, 'qty', 0.00)) if isinstance(quantity, dict) else 0.00
		if quantity:
			if to_int(quantity['use_config_manage_stock']) == 1:
				product_data['manage_stock'] = not self._notice['src']['config'].get('no_manage_stock')
			else:
				product_data['manage_stock'] = True if to_int(quantity['manage_stock']) == 1 else False
			product_data['is_in_stock'] = True if to_int(quantity['is_in_stock']) == 1 else False
		else:
			product_data['manage_stock'] = False
			product_data['is_in_stock'] = False

		product_data['created_at'] = product['created_at']
		product_data['updated_at'] = product['updated_at']
		language_default = self._notice['src']['language_default']
		names = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['name'])
		name_def = self.get_data_default(names, 'store_id', language_default, 'value')
		descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['description'])
		description_def = self.get_data_default(descriptions, 'store_id', language_default, 'value')
		short_descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['short_description'])
		short_description_def = self.get_data_default(short_descriptions, 'store_id', language_default, 'value')
		meta_titles = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['meta_title'])
		meta_title_def = self.get_data_default(meta_titles, 'store_id', language_default, 'value')
		meta_keywords = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['meta_keyword'])
		meta_keyword_def = self.get_data_default(meta_keywords, 'store_id', language_default, 'value')
		meta_descriptions = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['meta_description'])
		meta_description_def = self.get_data_default(meta_descriptions, 'store_id', language_default, 'value')

		url_keys = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_key'])
		url_key_def = self.get_data_default(url_keys, 'store_id', language_default, 'value')

		product_data['url_key'] = url_key_def if url_key_def else get_row_value_from_list_by_field(url_keys, 'store_id', 0, 'value')
		product_data['name'] = name_def if name_def else get_row_value_from_list_by_field(names, 'store_id', 0, 'value')

		product_data['description'] = self.convert_image_in_description(description_def if description_def else '')
		product_data['short_description'] = self.convert_image_in_description(short_description_def if short_description_def else '')
		product_data['meta_title'] = meta_title_def
		product_data['meta_keyword'] = meta_keyword_def
		product_data['meta_description'] = meta_description_def
		# Maximum 255 chars
		product_data['meta_description'] = to_str(product_data['meta_description'])[:255]

		images = list()
		image = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['image'], 'value')
		image_label = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['image_label'], 'value')
		url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
		if image and image != 'no_selection':
			product_data['thumb_image']['url'] = url_product_image
			product_data['thumb_image']['path'] = image
			product_data['thumb_image']['label'] = image_label
			images.append(image)
		product_media = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value_to_entity'], 'entity_id', product['entity_id'])
		product_media_ids = duplicate_field_value_from_list(product_media, 'value_id')
		product_images = get_list_from_list_by_field(products_ext_data['catalog_product_entity_media_gallery'], 'value_id', product_media_ids)

		product_data['images'] = list()
		if product_images:
			for product_image in product_images:
				if product_image['value'] in images:
					continue
				product_image_data = self.construct_product_image()
				product_image_data['label'] = get_row_value_from_list_by_field(products_ext_data['catalog_product_entity_media_gallery_value'], 'value_id', product_image['value_id'], 'label')
				product_image_data['position'] = get_row_value_from_list_by_field(products_ext_data['catalog_product_entity_media_gallery_value'], 'value_id', product_image['value_id'], 'position')
				product_image_data['url'] = url_product_image
				product_image_data['path'] = product_image['value']
				images.append(product_image['value'])
				product_data['images'].append(product_image_data)

		special_price = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['special_price'], 'value')
		special_from_date = get_row_value_from_list_by_field(entity_datetime, 'attribute_id', eav_attribute['special_from_date'], 'value')
		special_to_date = get_row_value_from_list_by_field(entity_datetime, 'attribute_id', eav_attribute['special_to_date'], 'value')
		if special_price:
			product_data['special_price']['price'] = special_price
			product_data['special_price']['start_date'] = special_from_date if special_from_date else ''
			product_data['special_price']['end_date'] = special_to_date if special_to_date else ''

		tiers_price = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_tier_price'], 'entity_id', product['entity_id'])
		if tiers_price:
			for tier_price in tiers_price:
				tier_price_data = self.construct_product_tier_price()
				tier_price_data['id'] = tier_price['value_id']
				tier_price_data['qty'] = tier_price['qty']
				tier_price_data['price'] = tier_price['value']
				tier_price_data['customer_group_id'] = tier_price['customer_group_id']
				product_data['tier_prices'].append(tier_price_data)

		product_data['tax']['id'] = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute['tax_class_id'], 'value')
		product_data['manufacturer']['id'] = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute.get('manufacturer'), 'value')
		manu_name = get_row_value_from_list_by_field(products_ext['data']['eav_attribute_option_value'], 'option_id', product_data['manufacturer']['id'], 'value')
		product_data['manufacturer']['name'] = manu_name if manu_name else ""

		product_categories = get_list_from_list_by_field(products_ext['data']['catalog_category_product'], 'product_id', product['entity_id'])
		if product_categories:
			for product_category in product_categories:
				product_category_data = self.construct_product_category()
				product_category_data['id'] = product_category['category_id']
				product_data['categories'].append(product_category_data)

		for lang_id, lang_name in self._notice['src']['languages'].items():
			product_language_data = self.construct_product_lang()
			name_lang = self.get_data_default(names, 'store_id', lang_id, 'value')
			description_lang = self.get_data_default(descriptions, 'store_id', lang_id, 'value')
			short_description_lang = self.get_data_default(short_descriptions, 'store_id', lang_id, 'value')
			meta_title_lang = self.get_data_default(meta_titles, 'store_id', lang_id, 'value')
			meta_keyword_lang = self.get_data_default(meta_keywords, 'store_id', lang_id, 'value')
			meta_description_lang = self.get_data_default(meta_descriptions, 'store_id', lang_id, 'value')
			url_key_lang = self.get_data_default(url_keys, 'store_id', lang_id, 'value')
			product_language_data['url_key'] = url_key_lang if url_key_lang else product_data['url_key']
			product_language_data['name'] = name_lang if name_lang else product_data['name']
			product_language_data['description'] = self.convert_image_in_description(description_lang) if description_lang else product_data['description']
			product_language_data['short_description'] = short_description_lang if short_description_lang else product_data['short_description']
			product_language_data['meta_title'] = meta_title_lang if meta_title_lang else (product_data['meta_title'] if product_data['meta_title'] else '')
			product_language_data['meta_keyword'] = meta_keyword_lang if meta_keyword_lang else (product_data['meta_keyword'] if product_data['meta_keyword'] else '')
			product_language_data['meta_description'] = meta_description_lang if meta_description_lang else (
				product_data['meta_description'] if product_data['meta_description'] else '')
			# Maximum 255 chars
			product_language_data['meta_description'] = product_language_data['meta_description'][:255] if product_language_data['meta_description'] else ''
			product_data['languages'][lang_id] = product_language_data

		if product['type_id'] == 'grouped':
			product_data['type'] = self.PRODUCT_GROUP
			links = get_list_from_list_by_field(products_ext['data']['catalog_product_link'], 'product_id', product['entity_id'])
			childs = get_list_from_list_by_field(links, 'link_type_id', 3)
			if childs:
				# child_ids = duplicate_field_value_from_list(childs, 'linked_product_id')
				for child_link in childs:
					product_data['group_child_ids'].append({
						'id': child_link['linked_product_id'],
						'qty': get_row_value_from_list_by_field(products_ext['data']['catalog_product_link_attribute_decimal'], 'link_id', child_link['link_id'], 'value')
					})
		# Get option product simple
		if product['type_id'] == 'simple':
			links = get_list_from_list_by_field(products_ext['data']['catalog_product_link'], 'linked_product_id', product['entity_id'])
			parents = get_list_from_list_by_field(links, 'link_type_id', 3)
			if parents:
				for parent_link in parents:
					product_data['group_parent_ids'].append({
						'id': parent_link['product_id'],
						'qty': get_row_value_from_list_by_field(products_ext['data']['catalog_product_link_attribute_decimal'], 'link_id', parent_link['link_id'], 'value')
					})
		product_options = get_list_from_list_by_field(products_ext['data']['catalog_product_option'], 'product_id', product['entity_id'])
		if product_options:
			for product_option in product_options:
				option_data = self.construct_product_option()
				option_data['id'] = product_option['option_id']
				option_data['option_type'] = product_option['type']
				option_data['required'] = True if to_int(product_option['is_require']) > 0 else False
				option_title = get_list_from_list_by_field(products_ext['data']['catalog_product_option_title'], 'option_id', product_option['option_id'])
				option_name_def = get_row_value_from_list_by_field(option_title, 'store_id', self._notice['src']['language_default'], 'title') if get_row_value_from_list_by_field(option_title, 'store_id', self._notice['src']['language_default'], 'title') else get_row_value_from_list_by_field(option_title, 'store_id', 0, 'title')
				option_data['option_name'] = option_name_def
				for id, name in self._notice['src']['languages'].items():
					option_language_data = self.construct_product_option_lang()
					option_name_lang = get_row_value_from_list_by_field(option_title, 'store_id', id, 'title')
					option_language_data['option_name'] = option_name_lang if option_name_lang else option_name_def
					option_data['option_languages'][id] = option_language_data
				if product_option['type'] not in ['drop_down', 'radio', 'checkbox', 'multiple']:
					product_data['options'].append(option_data)
					continue
				product_option_type_value = get_list_from_list_by_field(
					products_ext['data']['catalog_product_option_type_value'], 'option_id',
					product_option['option_id'])
				option_type_ids = duplicate_field_value_from_list(product_option_type_value, 'option_type_id')

				for option_type_id in option_type_ids:
					option_value_data = self.construct_product_option_value()
					option_value_data['id'] = option_type_id
					option_type_value = get_row_from_list_by_field(products_ext['data']['catalog_product_option_type_value'], 'option_type_id', option_type_id)
					catalog_product_option_type_title = get_list_from_list_by_field(products_ext['data']['catalog_product_option_type_title'], 'option_type_id', option_type_id)
					catalog_product_option_type_price = get_row_from_list_by_field(products_ext['data']['catalog_product_option_type_price'], 'option_type_id', option_type_id)
					option_type_source_def = get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', self._notice['src']['language_default']) if get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', self._notice['src']['language_default']) else get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', 0)
					if option_type_value.get('sku'):
						option_value_data['option_value_sku'] = option_type_value['sku']
					option_value_data['option_value_name'] = option_type_source_def['title']

					for id, name in self._notice['src']['languages'].items():
						option_value_language_data = self.construct_product_option_value_lang()
						option_value_name_lang = get_row_value_from_list_by_field(catalog_product_option_type_title, 'store_id', id, 'title')
						option_value_language_data['option_value_name'] = option_value_name_lang if option_value_name_lang else option_type_source_def['title']
						option_value_data['option_value_languages'][id] = option_value_language_data

					if catalog_product_option_type_price and catalog_product_option_type_price['price_type'] == 'fixed':
						price_option = catalog_product_option_type_price['price']
					elif catalog_product_option_type_price and catalog_product_option_type_price['price_type'] == 'percent':
						price_option = to_decimal(catalog_product_option_type_price['price']) * to_decimal(price) / 100
					else:
						price_option = 0
					option_value_data['option_value_price'] = price_option
					option_data['values'].append(option_value_data)
				product_data['options'].append(option_data)
		# Get children product config
		if product['type_id'] == 'configurable':
			product_data['type'] = self.PRODUCT_CONFIG
			children_products = get_list_from_list_by_field(products_ext['data']['catalog_product_super_link'], 'parent_id', product['entity_id'])
			super_attributes = get_list_from_list_by_field(products_ext['data']['catalog_product_super_attribute'], 'product_id', product['entity_id'])
			if children_products and super_attributes:
				for children in children_products:
					children_source = get_row_from_list_by_field(products_ext['data']['catalog_product_entity'],'entity_id', children['product_id'])
					children_datetime = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_datetime'], 'entity_id', children_source['entity_id'])
					children_decimal = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_decimal'], 'entity_id',children_source['entity_id'])
					children_int = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_int'],'entity_id', children_source['entity_id'])
					children_varchar = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_varchar'], 'entity_id',children_source['entity_id'])
					children_text = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_text'],'entity_id', children_source['entity_id'])

					children_names = get_list_from_list_by_field(children_varchar, 'attribute_id',eav_attribute['name'])
					children_name_def = self.get_data_default(children_names, 'store_id', language_default, 'value')
					children_prices = get_list_from_list_by_field(children_decimal, 'attribute_id',eav_attribute['price'])
					children_price_def = self.get_data_default(children_prices, 'store_id', language_default, 'value')
					children_cost = get_list_from_list_by_field(children_decimal, 'attribute_id',eav_attribute['cost'])
					children_cost_def = self.get_data_default(children_cost, 'store_id', language_default, 'value')
					children_weights = get_list_from_list_by_field(children_decimal, 'attribute_id',eav_attribute['weight'])
					children_weight_def = self.get_data_default(children_weights, 'store_id', language_default, 'value')
					stock_item = get_row_from_list_by_field(products_ext['data']['cataloginventory_stock_item'],'product_id', children_source['entity_id'])
					children_descriptions = get_list_from_list_by_field(children_text, 'attribute_id',eav_attribute['description'])
					children_description_def = self.get_data_default(children_descriptions, 'store_id', language_default,'value')
					children_short_descriptions = get_list_from_list_by_field(children_text, 'attribute_id',eav_attribute['short_description'])
					children_short_description_def = self.get_data_default(children_short_descriptions,'store_id', language_default, 'value')
					children_meta_titles = get_list_from_list_by_field(children_varchar, 'attribute_id',eav_attribute['meta_title'])
					children_meta_title_def = self.get_data_default(children_meta_titles, 'store_id', language_default,'value')
					children_meta_keywords = get_list_from_list_by_field(children_text, 'attribute_id',eav_attribute['meta_keyword'])
					children_meta_keyword_def = self.get_data_default(children_meta_keywords, 'store_id', language_default,'value')
					children_meta_descriptions = get_list_from_list_by_field(children_varchar, 'attribute_id',eav_attribute['meta_description'])
					children_meta_description_def = self.get_data_default(children_meta_descriptions,'store_id', language_default, 'value')

					childen_data = self.construct_product_child()
					childen_data['id'] = children_source['entity_id']
					status = get_row_value_from_list_by_field(children_int, 'attribute_id', eav_attribute['status'],'value')
					childen_data['status'] = True if to_int(status) == 1 else False
					childen_data['name'] = children_name_def
					childen_data['description'] = children_description_def if children_description_def else ''
					childen_data['short_description'] = children_short_description_def if children_short_description_def else ''
					childen_data['meta_title'] = children_meta_title_def if children_meta_title_def else ''
					childen_data['meta_keyword'] = children_meta_keyword_def if children_meta_keyword_def else ''
					childen_data['meta_description'] = children_meta_description_def if children_meta_description_def else ''
					# Maximum 255 chars
					childen_data['meta_description'] = childen_data['meta_description'][:255] if childen_data['meta_description'] else ''
					childen_data['sku'] = children_source['sku']
					childen_data['price'] = to_decimal(children_price_def)
					childen_data['cost'] = to_decimal(children_cost_def)
					childen_data['weight'] = to_decimal(children_weight_def) if to_decimal(children_weight_def) else 0.0000
					childen_data['qty'] = to_int(stock_item['qty']) if stock_item else 0
					if stock_item:
						if to_int(stock_item['use_config_manage_stock']) == 1:
							childen_data['manage_stock'] = not self._notice['src']['config'].get('no_manage_stock')
						else:
							childen_data['manage_stock'] = True if to_int(stock_item['manage_stock']) == 1 else False
							childen_data['is_in_stock'] = True if to_int(stock_item['is_in_stock']) == 1 else False
					else:
						childen_data['manage_stock'] = False
						childen_data['is_in_stock'] = False
					# childen_data['manage_stock'] = True if stock_item and to_decimal(stock_item['qty']) > 0 else False
					# childen_data['is_in_stock'] = True if stock_item and to_int(stock_item['is_in_stock']) == 1 else False

					childen_data['created_at'] = children_source['created_at']
					childen_data['update_at'] = children_source['updated_at']
					child_special_price = get_row_value_from_list_by_field(children_decimal, 'attribute_id',eav_attribute['special_price'], 'value')
					child_special_from_date = get_row_value_from_list_by_field(children_datetime, 'attribute_id',eav_attribute['special_from_date'], 'value')
					child_special_to_date = get_row_value_from_list_by_field(children_datetime, 'attribute_id',eav_attribute['special_to_date'], 'value')
					if child_special_price:
						childen_data['special_price']['price'] = child_special_price
						childen_data['special_price']['start_date'] = child_special_from_date
						childen_data['special_price']['end_date'] = child_special_to_date
					image = get_row_value_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['image'],'value')
					image_label = get_row_value_from_list_by_field(children_varchar, 'attribute_id', eav_attribute[
						'image_label'], 'value')
					url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
					if image and image != 'no_selection':
						childen_data['thumb_image']['url'] = url_product_image
						childen_data['thumb_image']['path'] = image
						childen_data['thumb_image']['label'] = image_label
					product_image_chil = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery'], 'entity_id',children_source['entity_id'])
					if product_image_chil:
						for product_image in product_image_chil:
							if product_image['value'] == image and product_image['value'] != 'no_selection':
								continue
							product_image_data = self.construct_product_image()
							product_image_data['label'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id',product_image['value_id'], 'label')
							product_image_data['url'] = url_product_image
							product_image_data['path'] = product_image['value']
							childen_data['images'].append(product_image_data)

					for lang_id, lang_name in self._notice['src']['languages'].items():
						childen_language_data = self.construct_product_lang()
						childen_name_lang = self.get_data_default(children_names, 'store_id', lang_id,'value')
						childen_description_lang = self.get_data_default(children_descriptions, 'store_id',lang_id, 'value')
						childen_short_description_lang = self.get_data_default(children_short_descriptions,'store_id', lang_id, 'value')
						childen_meta_title_lang = self.get_data_default(children_meta_titles, 'store_id',lang_id, 'value')
						childen_meta_keyword_lang = self.get_data_default(children_meta_keywords, 'store_id',lang_id, 'value')
						childen_meta_description_lang = self.get_data_default(children_meta_descriptions,'store_id', lang_id, 'value')
						childen_language_data['name'] = childen_name_lang if childen_name_lang else (children_name_def if children_name_def else '')
						childen_language_data['description'] = childen_description_lang if childen_description_lang else (children_description_def if children_description_def else '')
						childen_language_data['short_description'] = childen_short_description_lang if childen_short_description_lang else (children_short_description_def if children_short_description_def else '')
						childen_language_data['meta_title'] = childen_meta_title_lang if childen_meta_title_lang else (children_meta_title_def if children_meta_title_def else '')
						childen_language_data['meta_keyword'] = childen_meta_keyword_lang if childen_meta_keyword_lang else (children_meta_keyword_def if children_meta_keyword_def else '')
						childen_language_data['meta_description'] = childen_meta_description_lang if childen_meta_description_lang else (children_meta_description_def if children_meta_description_def else '')
						# Maximum 255 chars
						childen_language_data['meta_description'] = childen_language_data['meta_description'][:255] if childen_language_data['meta_description'] else ''
						childen_data['languages'][lang_id] = childen_language_data

					for super_attribute in super_attributes:
						childen_product_option_data = self.construct_product_child_attribute()
						eav_attribute_desc = get_row_from_list_by_field(product_eav_attributes,'attribute_id', super_attribute['attribute_id'])
						super_attribute_label = get_list_from_list_by_field(
							products_ext['data']['catalog_product_super_attribute_label'], 'product_super_attribute_id',
							super_attribute['product_super_attribute_id'])
						childen_product_option_data['option_id'] = super_attribute['product_super_attribute_id']
						childen_product_option_data['option_code'] = eav_attribute_desc['attribute_code']
						childen_product_option_data['option_name'] = eav_attribute_desc['frontend_label']
						for id_lang, nameLang in self._notice['src']['languages'].items():
							product_attribute_lang_data = self.construct_product_option_lang()
							option_name_lang_sa = get_row_value_from_list_by_field(super_attribute_label, 'store_id', id_lang, 'value')
							product_attribute_lang_data['option_name'] = option_name_lang_sa if option_name_lang_sa else eav_attribute_desc['frontend_label']
							childen_product_option_data['option_languages'][id_lang] = product_attribute_lang_data

						option_id = get_row_value_from_list_by_field(children_int, 'attribute_id', super_attribute['attribute_id'], 'value')
						eav_attribute_option_value = get_list_from_list_by_field(
							products_ext['data']['eav_attribute_option_value'], 'option_id', option_id)
						option_value_def = get_row_value_from_list_by_field(eav_attribute_option_value, 'store_id', 0, 'value')
						if not option_value_def:
							for vlua in eav_attribute_option_value:
								if vlua['value']:
									option_value_def = vlua
									break
						childen_product_option_data['option_value_id'] = option_id
						childen_product_option_data['option_value_code'] = to_str(option_value_def).lower()
						childen_product_option_data['option_value_name'] = option_value_def
						super_attribute_pricing = get_list_from_list_by_field(products_ext['data']['catalog_product_super_attribute_pricing'],'product_super_attribute_id', super_attribute['product_super_attribute_id'])
						super_attribute_pricing_row = get_row_from_list_by_field(super_attribute_pricing, 'value_index',option_id)
						super_attribute_price = 0
						if super_attribute_pricing_row:
							if super_attribute_pricing_row['is_percent'] and super_attribute_pricing_row['is_percent'] != '0':
								super_attribute_price = to_decimal(
									super_attribute_pricing_row['pricing_value']) * to_decimal(price) / 100
							else:
								super_attribute_price = to_decimal(super_attribute_pricing_row['pricing_value'])
						childen_product_option_data['price'] = to_decimal(super_attribute_price)
						# childen_data['price'] = to_decimal(childen_data['price']) + to_decimal(super_attribute_price)
						childen_product_option_data['price'] = super_attribute_price
						for id_lang, name_lang in self._notice['src']['languages'].items():
							product_attribute_value_lang_data = self.construct_product_option_value_lang()
							option_value_lang = get_row_value_from_list_by_field(eav_attribute_option_value, 'store_id',id_lang, 'value')
							product_attribute_value_lang_data['option_value_name'] = option_value_lang if option_value_lang else option_value_def
							childen_product_option_data['option_value_languages'][id_lang] = product_attribute_value_lang_data
						childen_data['attributes'].append(childen_product_option_data)
					product_data['children'].append(childen_data)

		# Attributes
		attribute_values = {
			'decimal': entity_decimal,
			'int': entity_int,
			'text': entity_text,
			'varchar': entity_varchar,
			'datetime': entity_datetime,
		}
		for index, eav_attribute_row in enumerate(product_eav_attributes):
			if len(product_eav_attributes) > 250 and to_int(index) % 30 == 0:
				time.sleep(0.01)
			if not attribute_values.get(eav_attribute_row['backend_type'] or eav_attribute_row['frontend_input'] not in ['select', 'text', 'textarea', 'boolean', 'multiselect']) or (eav_attribute_row['attribute_code'] in ['manufacturer']):
				continue
			product_attribute_data = self.construct_product_attribute()
			product_attribute_data['option_id'] = eav_attribute_row['attribute_id']

			product_attribute_data['option_type'] = 'select'
			if eav_attribute_row['frontend_input'] == 'text':
				product_attribute_data['option_type'] = 'text'
			elif eav_attribute_row['frontend_input'] == 'multiselect':
				product_attribute_data['option_type'] = self.OPTION_MULTISELECT
			product_attribute_data['option_code'] = eav_attribute_row['attribute_code']
			product_attribute_data['option_name'] = eav_attribute_row['frontend_label']
			catalog_eav_attribute_data = get_row_from_list_by_field(catalog_eav_attribute, 'attribute_id', eav_attribute_row['attribute_id'])
			product_attribute_data['is_visible'] = True
			if catalog_eav_attribute_data and to_int(catalog_eav_attribute_data['is_visible_on_front']) == 0:
				product_attribute_data['is_visible'] = False
			for id_lang, name_lang in self._notice['src']['languages'].items():
				option_language_data = self.construct_product_option_lang()
				option_language_data['option_name'] = eav_attribute_row['frontend_label']
				product_attribute_data['option_languages'][id_lang] = option_language_data
			option_value = get_list_from_list_by_field(attribute_values[eav_attribute_row['backend_type']], 'attribute_id', eav_attribute_row['attribute_id'])
			if not option_value:
				continue
			option_value_default = get_row_from_list_by_field(option_value, 'store_id', 0)
			if not option_value_default:
				option_value_default = option_value[0]
			if eav_attribute_row['frontend_input'] == 'text':
				product_attribute_data['option_value_name'] = option_value_default['value']
				for id_lang, name_lang in self._notice['src']['languages'].items():
					option_value_language_data = self.construct_product_option_value_lang()
					option_value_name_lang = get_row_from_list_by_field(option_value, 'store_id', id_lang)
					if not option_value_name_lang:
						continue
					option_value_language_data['option_value_name'] = option_value_name_lang['value']
					product_attribute_data['option_value_languages'][id_lang] = option_value_language_data
			if eav_attribute_row['frontend_input'] == 'select':
				option_v_name = get_list_from_list_by_field(products_ext['data']['eav_attribute_option_value'], 'option_id', option_value_default['value'])
				if not option_v_name:
					continue
				option_v_name_def = get_row_from_list_by_field(option_v_name, 'store_id', 0)
				if not option_v_name_def:
					option_v_name_def = option_v_name[0]
				product_attribute_data['option_value_name'] = option_v_name_def['value']
				for id_lang, name_lang in self._notice['src']['languages'].items():
					option_value_language_data = self.construct_product_option_value_lang()
					option_value_name_lang = get_row_from_list_by_field(option_v_name, 'store_id', id_lang)
					if not option_value_name_lang:
						continue
					option_value_language_data['option_value_name'] = option_value_name_lang['value']
					product_attribute_data['option_value_languages'][id_lang] = option_value_language_data
			if eav_attribute_row['frontend_input'] == 'multiselect':
				if not option_value_default['value']:
					continue
				option_value_id_srcs = option_value_default['value'].split(',')
				option_value_name = list()
				for option_value_id_src in option_value_id_srcs:
					option_v_name = get_row_value_from_list_by_field(products_ext['data']['eav_attribute_option_value'], 'option_id', option_value_id_src, 'value') if eav_attribute_row['frontend_input'] in {'multiselect', 'select'} else option_value['value']
					if not option_v_name:
						continue
					option_value_name.append(option_v_name)

				if not option_value_name:
					continue
				product_attribute_data['option_value_name'] = ';'.join(option_value_name)
			product_data['attributes'].append(product_attribute_data)

		if product['type_id'] == 'virtual':
			product_data['type'] = self.PRODUCT_VIRTUAL

		# tags
		# tag_relation = get_list_from_list_by_field(products_ext['data']['tag_relation'], 'product_id',
		#                                            product['entity_id'])
		# if tag_relation:
		# 	tags = list()
		# 	for product_tag in tag_relation:
		# 		tag = get_row_from_list_by_field(products_ext['data']['tag'], 'tag_id', product_tag['tag_id'])
		# 		tags.append(tag['name'])
		# 	product_data['tags'] = ','.join(tags)
		# related
		relation_type = {
			'1': self.PRODUCT_RELATE,
			'4': self.PRODUCT_UPSELL,
			'5': self.PRODUCT_CROSS
		}
		catalog_product_link_parent = get_list_from_list_by_field(products_ext['data']['catalog_product_link'],'product_id', product['entity_id'])
		catalog_product_link_children = get_list_from_list_by_field(products_ext['data']['catalog_product_link'],'linked_product_id', product['entity_id'])
		if catalog_product_link_parent:
			for row in catalog_product_link_parent:
				key = get_value_by_key_in_dict(relation_type, to_str(row['link_type_id']), self.PRODUCT_RELATE)
				relation = self.construct_product_relation()
				relation['id'] = row['linked_product_id']
				relation['type'] = key
				product_data['relate']['children'].append(relation)
		if catalog_product_link_children:
			for row in catalog_product_link_children:
				key = get_value_by_key_in_dict(relation_type, to_str(row['link_type_id']), 'relate')
				relation = self.construct_product_relation()
				relation['id'] = row['product_id']
				relation['type'] = key
				product_data['relate']['parent'].append(relation)
		# if self._notice['config']['seo']:
		detect_seo = self.detect_seo()
		product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)
		return response_success(product_data)

	def finish_product_export(self):
		del self.eav_attribute_product
		del self.catalog_eav_attribute
		return response_success()

	def get_product_id_import(self, convert, product, products_ext):
		return product['entity_id']

	def check_product_import(self, convert, product, products_ext):
		return self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'])

	def update_product_after_demo(self, product_id, convert, order, orders_ext):
		all_query = list()
		all_query.append(self.create_delete_query_connector('catalog_category_product', {'product_id': product_id}))
		category_desc = list()
		for value in convert['categories']:
			category_ids = list()
			try:
				category_ids = self._notice['map']['category_data'][value['id']]
			except KeyError:
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
				catalog_category_product_data = {
					'category_id': category_id,
					'product_id': product_id,
					'position': get_value_by_key_in_dict(value, 'position', 1),
				}
				all_query.append(
					self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))
		tax_class_id_src = convert.get('tax', {}).get('id', None)
		tax_class_id = 0
		if tax_class_id_src:
			tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_PRODUCT, tax_class_id_src)
		product_eav_attribute = self.select_all_attribute_map()
		product_eav_attribute_data = dict()
		for attribute_data in product_eav_attribute:
			attribute = json.loads(attribute_data['value'])
			if attribute['backend_type'] != 'static':
				if not attribute_data['code_src'] in product_eav_attribute_data:
					product_eav_attribute_data[attribute_data['code_src']] = dict()
				product_eav_attribute_data[attribute_data['code_src']]['attribute_id'] = attribute['attribute_id']
				product_eav_attribute_data[attribute_data['code_src']]['backend_type'] = attribute['backend_type']
				product_eav_attribute_data[attribute_data['code_src']]['frontend_input'] = attribute['frontend_input']
		data_attribute_insert = {
			'tax_class_id': tax_class_id if tax_class_id else 0,
			# 'url_path': pro_url_path,
		}
		manufacturer_src_id = convert.get('manufacturer', {}).get('id')
		if manufacturer_src_id:
			manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, manufacturer_src_id)
			if manufacturer_id and 'manufacturer' in product_eav_attribute_data:
				data_attribute_insert['manufacturer'] = manufacturer_id
		for key, value in data_attribute_insert.items():
			if key not in product_eav_attribute_data:
				continue
			if not value:
				continue
			all_query.append(self.create_delete_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'], {'entity_id': product_id, 'attribute_id':product_eav_attribute_data[key]['attribute_id']}))
			product_attr_data = {
				'attribute_id': product_eav_attribute_data[key]['attribute_id'],
				'store_id': 0,
				'entity_id': product_id,
				'value': value,
			}
			all_query.append(self.create_insert_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'],product_attr_data))
		self.import_multiple_data_connector(all_query, 'update_demo_product')
		return response_success()

	def router_product_import(self, convert, product, products_ext):
		return response_success('product_import')

	def before_product_import(self, convert, product, products_ext):
		return response_success()

	def import_parent_product(self, parent):
		parent_id = self.get_map_field_by_src(self.TYPE_PRODUCT, parent['id'], parent['code'])
		if parent_id:
			return response_success(parent_id)
		parent_import = self.product_import(parent, None, None)
		if parent_import['result'] != 'success':
			return parent_import
		parent_id = parent_import['data']
		self.after_product_import(parent_id, parent, None, None)
		return parent_import

	def import_children_product(self, children):
		children_id = self.get_map_field_by_src(self.TYPE_PRODUCT, children['id'], children['code'])
		if children_id:
			return response_success(children_id)
		children_import = self.product_import(children, None, None)
		if children_import['result'] != 'success':
			return children_import
		children_id = children_import['data']
		self.after_product_import(children_id, children, None, None)
		return children_import

	def product_import(self, convert, product, products_ext):
		response = response_success()
		try:
			attribute_set_id = self._notice['map']['attributes'][convert['attribute_set_id']]
		except Exception:
			attribute_set_id = 4

		index = None
		sku = convert['sku']
		if not sku:
			sku = self.convert_attribute_code(convert['name'])
		# if not sku:
		# 	return response_error(self.warning_import_entity(self.TYPE_PRODUCT, convert['id'], convert['code'], 'sku is empty'))
		new_sku = sku
		if sku:
			while self.check_sku_exist(new_sku):
				index = time.time()
				new_sku = sku + to_str(index)
		product_type = convert['type'] if convert['type'] else 'simple'
		if convert['is_child']:
			product_type = 'simple'
		catalog_product_entity_data = {
			'attribute_set_id': attribute_set_id,
			'type_id': product_type,
			'sku': new_sku[:60],
			'has_options': 0,
			'required_options': 0,
			'created_at': convert_format_time(convert.get('created_at')) if convert.get('created_at') else get_current_time(),
			'updated_at': convert_format_time(convert.get('updated_at')) if convert.get('updated_at') else get_current_time(),
		}
		# if not convert['type'] and to_len(convert['children']) > 0:
		# 	catalog_product_entity_data['type_id'] = self.PRODUCT_CONFIG
		if to_len(convert['options']) > 0:
			catalog_product_entity_data['has_options'] = 1
		if self._notice['config']['pre_prd']:
			catalog_product_entity_data['entity_id'] = convert['id']

		product_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity', catalog_product_entity_data), True, convert['id'])
		if not product_id:
			response['result'] = 'warning'
			response['msg'] = self.warning_import_entity('product', convert['id'])
			return response
		if not sku:
			new_sku = product_id
			self.import_data_connector(self.create_update_query_connector('catalog_product_entity', {'sku': product_id}, {'entity_id': product_id}))
		if index:
			new_sku = to_str(sku) + '-' + to_str(product_id)
			self.import_data_connector(self.create_update_query_connector('catalog_product_entity', {'sku': new_sku}, {'entity_id': product_id}))
		map_type = self.TYPE_PRODUCT
		if convert.get('is_child'):
			map_type = self.TYPE_CHILD
		self.insert_map(map_type, convert['id'], product_id, convert['code'], new_sku, catalog_product_entity_data['type_id'])
		return response_success(product_id)

	def after_product_import(self, product_id, convert, product, products_ext):
		new_sku = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'], 'value')
		if not new_sku:
			sku = convert['sku']
			if not sku:
				sku = self.convert_attribute_code(convert['name'])
			new_sku = sku
			index = None
			if sku:
				while self.check_sku_exist(new_sku):
					index = time.time()
					new_sku = sku + to_str(index)
			if not sku:
				sku = product_id
			if index:
				new_sku = to_str(sku) + '-' + to_str(product_id)
		url_query = self.get_connector_url('query')
		url_image = self.get_connector_url('image')
		all_query = list()
		all_attribute = self.select_all_attribute_map()
		product_eav_attribute_data = dict()
		attribute_id_media = None
		for attribute_row in all_attribute:
			attribute = json_decode(attribute_row['value'])
			if not attribute:
				value_att = to_str(attribute_row['value']).replace('\\', '\\\\')
				list_re = re.findall('".*?".*?"', to_str(value_att))
				for reg in list_re:
					if to_str(reg).find(':') > 0 or to_str(reg).find(',') > 0:
						pass
					else:
						value_att = to_str(value_att).replace(reg[1:-1], reg[1:-1].replace('"', '\\"'))
				attribute = json_decode(value_att)
			if attribute['backend_type'] != 'static':
				if not attribute['attribute_code'] in product_eav_attribute_data:
					product_eav_attribute_data[attribute['attribute_code']] = dict()
				product_eav_attribute_data[attribute['attribute_code']]['attribute_id'] = attribute['attribute_id']
				product_eav_attribute_data[attribute['attribute_code']]['backend_type'] = attribute['backend_type']
				product_eav_attribute_data[attribute['attribute_code']]['frontend_input'] = attribute['frontend_input']
			if attribute['attribute_code'] == 'media_gallery':
				attribute_id_media = attribute['attribute_id']

		# ------------------------------------------------------------------------------------------------------------------------
		# todo: image
		# image begin
		image_name = None
		if 'images' in convert:
			for item in convert['images']:
				if item['url'] or item['path']:
					item_image_name = None
					image_process = self.process_image_before_import(item['url'], item['path'])
					if (not ('ignore_image' in self._notice['config'])) or (not self._notice['config']['ignore_image']):
						item_image_name = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_magento_image_path(image_process['path']) + os.path.basename(image_process['path']), self._notice['target']['config']['image_product']))
						if item_image_name:
							item_image_name = self.remove_prefix_path(item_image_name, self._notice['target']['config']['image_product'])
					else:
						item_image_name = item['path']
					if item_image_name:
						if convert['thumb_image']['url']:
							main_process = self.process_image_before_import(convert['thumb_image']['url'],convert['thumb_image']['path'])
							if main_process['url'] == image_process['url']:
								image_name = item_image_name
						catalog_product_entity_media_gallery_data = {
							'attribute_id': attribute_id_media,
							'value': item_image_name,
							'media_type': 'image',
							'disabled': 0,
						}
						value_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity_media_gallery', catalog_product_entity_media_gallery_data))
						if value_id:
							catalog_product_entity_media_gallery_value_to_entity_data = {
								'value_id': value_id,
								'entity_id': product_id,
							}
							catalog_product_entity_media_gallery_value_to_entity_query = self.create_insert_query_connector('catalog_product_entity_media_gallery_value_to_entity', catalog_product_entity_media_gallery_value_to_entity_data)
							all_query.append(catalog_product_entity_media_gallery_value_to_entity_query)
							catalog_product_entity_media_gallery_value_data = {
								'value_id': value_id,
								'store_id': 0,
								'entity_id': product_id,
								'label': item.get('label', ''),
								'position': item.get('position', 1),
								'disabled': 0,
							}
							all_query.append(self.create_insert_query_connector('catalog_product_entity_media_gallery_value', catalog_product_entity_media_gallery_value_data))
		if (not image_name) and convert['thumb_image']['url']:
			if not self._notice['config'].get('ignore_image'):
				image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
				image_name = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_magento_image_path(image_process['path']) + os.path.basename(image_process['path']), self._notice['target']['config']['image_product']))
				if image_name:
					image_name = self.remove_prefix_path(image_name, self._notice['target']['config']['image_product'])
			else:
				image_name = convert['thumb_image']['path']

			if image_name:
				catalog_product_entity_media_gallery_data = {
					'attribute_id': attribute_id_media,
					'value': image_name,
					'media_type': 'image',
					'disabled': 0,
				}
				value_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity_media_gallery', catalog_product_entity_media_gallery_data))
				if value_id:
					catalog_product_entity_media_gallery_value_to_entity_data = {
						'value_id': value_id,
						'entity_id': product_id,
					}
					catalog_product_entity_media_gallery_value_to_entity_query = self.create_insert_query_connector('catalog_product_entity_media_gallery_value_to_entity', catalog_product_entity_media_gallery_value_to_entity_data)
					all_query.append(catalog_product_entity_media_gallery_value_to_entity_query)
					catalog_product_entity_media_gallery_value_data = {
						'value_id': value_id,
						'store_id': 0,
						'entity_id': product_id,
						'label': convert['thumb_image'].get('label', ''),
						'position': convert['thumb_image'].get('position', 1),
						'disabled': 0,
					}
					all_query.append(self.create_insert_query_connector('catalog_product_entity_media_gallery_value', catalog_product_entity_media_gallery_value_data))
		# image end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: link category and product
		# begin
		category_desc = list()
		for value in convert['categories']:
			# try:
			#	category_id = self._notice['map']['category_data'][int(value['id'])]
			# except KeyError:
			if not self._notice['support']['site_map']:
				category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, value['id'])

				if not category_id:
					category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, None, value['code'])
				if not category_id or category_id in category_desc:
					continue
				category_desc.append(category_id)
				catalog_category_product_data = {
					'category_id': category_id,
					'product_id': product_id,
					'position': 1,
				}
				all_query.append(self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))
			else:
				parent_exist = self.select_category_map(value['id'])
				if parent_exist:
					for parent_row in parent_exist:
						category_desc.append(parent_row['id_desc'])
						catalog_category_product_data = {
							'category_id': parent_row['id_desc'],
							'product_id': product_id,
							'position': 1,
						}
						all_query.append(self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: cataloginventory_stock
		# begin
		if self.convert_version(self._notice['target']['config']['version'], 2) >= 230:
			inventory_source_item_data = {
				'source_code': 'default',
				'sku': new_sku,
				'quantity': convert['qty'],
				'status': 1,
			}
			all_query.append(self.create_insert_query_connector('inventory_source_item', inventory_source_item_data))

			if 'view' in self._notice['target']['config'] and not self._notice['target']['config']['view']:
				inventory_stock_1_data = {
					'product_id': product_id,
					'website_id': 0,
					'stock_id': 1,
					'qty': convert['qty'],
					'is_salable': 1,
					'sku': new_sku,
				}
				all_query.append(self.create_insert_query_connector('inventory_stock_1', inventory_stock_1_data))
		cataloginventory_stock_item_data = {
			'product_id': product_id,
			'stock_id': 1,
			'website_id': 0,
			'min_qty': 1,
			'use_config_min_qty': 1,
			'is_qty_decimal': 0,
			'qty': convert['qty'],
			'backorders': 0,
			'is_in_stock': 1 if (convert['qty'] and convert['manage_stock']) or not convert['manage_stock'] or convert.get('is_in_stock') else 0,
			'manage_stock': 1 if convert['manage_stock'] else 0,
			'use_config_manage_stock': 0
		}
		all_query.append(self.create_insert_query_connector('cataloginventory_stock_item', cataloginventory_stock_item_data))

		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: tier price
		# begin
		if 'tier_prices' in convert:
			for value in convert['tier_prices']:
				tier_price_website_id = [0]
				if get_value_by_key_in_dict(value, 'website_id', None):
					tier_price_website_id = self.get_website_ids_target_by_id_src([value['website_id']])
					if not tier_price_website_id:
						tier_price_website_id = [0]
				for website_id in tier_price_website_id:
					catalog_product_entity_tier_price_data = {
						'entity_id': product_id,
						'all_groups': value.get('all_groups', 1),
						'customer_group_id': self._notice['map']['customer_group'].get(to_str(value.get('customer_group_id', -1)), 0),
						'qty': value['qty'],
						'value': value['price'],
						'website_id': website_id,
					}
					if self.convert_version(self._notice['target']['config']['version'], 2) >= 220:
						catalog_product_entity_tier_price_data['percentage_value'] = None

					if get_value_by_key_in_dict(value, 'percentage_value', None):
						catalog_product_entity_tier_price_data['percentage_value'] = get_value_by_key_in_dict(value, 'percentage_value', None)

					all_query.append(self.create_insert_query_connector('catalog_product_entity_tier_price', catalog_product_entity_tier_price_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: product website
		# begin
		product_website_ids = list()
		if 'store_ids' in convert and self._notice['support']['site_map']:
			for src_store_id in convert['store_ids']:
				if src_store_id in self._notice['map']['site']:
					target_store = self._notice['map']['site'][src_store_id]
					website_target_id = get_row_value_from_list_by_field(self._notice['target']['website'], 'store_id', target_store, 'website_id')
					if website_target_id and website_target_id not in product_website_ids:
						product_website_ids.append(website_target_id)
		if not product_website_ids:
			target_store_id = self._notice['map']['languages'].get(to_str(self._notice['src']['language_default']), 0)
			website_id = self.get_website_id_by_store_id(target_store_id)
			if website_id:
				product_website_ids.append(website_id)
		for website_id in product_website_ids:
			catalog_product_website_data = {
				'product_id': product_id,
				'website_id': website_id,
			}
			all_query.append(
				self.create_insert_query_connector('catalog_product_website', catalog_product_website_data))
		# end

		# ------------------------------------------------------------------------------------------------------------------------

		# todo: product attribute
		# begin: attribute product
		# begin map
		tax_class_id = 0
		if convert['tax']['id'] or convert['tax']['code']:
			tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_PRODUCT, convert['tax']['id'], convert['tax']['code'])
		pro_url_key = self.get_product_url_key(convert.get('url_key'), 0, convert.get('name', ''), product_id = product_id)
		pro_url_path = self.get_product_url_path(convert.get('url_path'), 0, pro_url_key)

		data_attribute_insert = {
			'name': self.strip_html_tag(get_value_by_key_in_dict(convert, 'name', '')),
			'meta_title': get_value_by_key_in_dict(convert, 'meta_title', ''),
			'meta_description': get_value_by_key_in_dict(convert, 'meta_description', ''),
			'image': image_name if image_name else None,
			'small_image': image_name if image_name else None,
			'thumbnail': image_name if image_name else None,
			'description': self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'description', '')),
			'short_description': self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'short_description', '')),
			'meta_keyword': get_value_by_key_in_dict(convert, 'meta_keyword', ''),
			'special_price': convert.get('special_price', {}).get('price'),
			'special_from_date': convert.get('special_price', {}).get('start_date'),
			'special_to_date': convert.get('special_price', {}).get('end_date'),
			'length': get_value_by_key_in_dict(convert, 'length', 0.0000),
			'width': get_value_by_key_in_dict(convert, 'width', 0.0000),
			'height': get_value_by_key_in_dict(convert, 'height', 0.0000),
			'ts_dimensions_length': get_value_by_key_in_dict(convert, 'length', 0.0000),
			'ts_dimensions_width': get_value_by_key_in_dict(convert, 'width', 0.0000),
			'ts_dimensions_height': get_value_by_key_in_dict(convert, 'height', 0.0000),
			'price': get_value_by_key_in_dict(convert, 'price', 0.0000),
			'weight': get_value_by_key_in_dict(convert, 'weight', 0.0000),
			'cost': get_value_by_key_in_dict(convert, 'cost', 0.0000),
			'visibility': convert.get('visibility', 4),
			'quantity_and_stock_status': 1 if convert.get('quantity_and_stock_status') else 0,
			'tax_class_id': tax_class_id if tax_class_id else 0,
			'status': 1 if convert['status'] else 2,
			'url_key': pro_url_key.lower(),
			'url_path': pro_url_path,
		}
		if get_value_by_key_in_dict(convert, 'available_date', '') != '':
			data_attribute_insert['news_from_date'] = get_value_by_key_in_dict(convert, 'available_date')
			if 'news_from_date' not in product_eav_attribute_data:
				for attribute_row in all_attribute:
					attribute = json_decode(attribute_row['value'])
					if 'frontend_label' in attribute and attribute['frontend_label'] == 'Set Product as New from Date':
						product_eav_attribute_data['news_from_date'] = dict()
						product_eav_attribute_data['news_from_date']['attribute_id'] = attribute['attribute_id']
						product_eav_attribute_data['news_from_date']['backend_type'] = attribute['backend_type']
						product_eav_attribute_data['news_from_date']['frontend_input'] = attribute['frontend_input']

		list_attr = dict()
		if 'attributes' in convert:
			for attribute_src in convert['attributes']:
				attr_src_code = self.create_attribute_code(attribute_src['option_code']) if attribute_src['option_code'] else self.create_attribute_code(attribute_src['option_name'])
				if attr_src_code == 'options':
					attr_src_code += 'options_lecm'  # bug magento not accept attribute code options
				backend_type = 'varchar'

				if not attribute_src['option_type']:
					attribute_src['option_type'] = 'select'

				if attribute_src['option_type'] in {'select'}:
					backend_type = 'int'
				if attribute_src['option_type'] == 'multiselect':
					backend_type = 'text'
				if attribute_src['option_type'] == self.OPTION_DATETIME:
					backend_type = 'datetime'
				if attribute_src['option_type'] == self.OPTION_RADIO:
					attribute_src['option_type'] = self.OPTION_SELECT
					backend_type = 'int'
				attribute_src_data = {
					'attribute_code': attr_src_code,
					'frontend_input': attribute_src['option_type'],
					'backend_type': backend_type
				}
				index = 2
				new_attr_src_code = attr_src_code
				while new_attr_src_code in product_eav_attribute_data:
					attribute_target_data = {
						'attribute_code': new_attr_src_code,
						'frontend_input': product_eav_attribute_data[new_attr_src_code]['frontend_input'],
						'backend_type': product_eav_attribute_data[new_attr_src_code]['backend_type']
					}
					check_sync = self.check_attribute_sync(attribute_target_data, attribute_src_data)
					if check_sync:
						break
					new_attr_src_code = attr_src_code + '_' + to_str(index) + 'nd'
					attribute_src_data['attribute_code'] = new_attr_src_code
					index += 1
				attr_src_code = new_attr_src_code
				if attr_src_code not in product_eav_attribute_data:
					attribute_set_id = self._notice['map']['attributes'].get(convert.get('attribute_set_id', -1), 4)

					attribute_id = self.create_attribute(attr_src_code, backend_type, attribute_src['option_type'], attribute_set_id, attribute_src['option_name'])
					if attribute_id:
						attribute_target_data = {
							'attribute_id': attribute_id,
							'attribute_code': attr_src_code,
							'frontend_input': attribute_src['option_type'],
							'backend_type': backend_type
						}
						if attr_src_code not in product_eav_attribute_data:
							self.insert_map(self.TYPE_ATTR, None, attribute_id, None, attr_src_code, json_encode(attribute_target_data))
							product_eav_attribute_data[attr_src_code] = dict()
							product_eav_attribute_data[attr_src_code]['attribute_id'] = attribute_id
							product_eav_attribute_data[attr_src_code]['backend_type'] = backend_type
							product_eav_attribute_data[attr_src_code]['frontend_input'] = attribute_src['option_type']
				else:
					attribute_id = product_eav_attribute_data[attr_src_code]['attribute_id']
				if not attribute_id:
					continue
				list_attribute_label_store = list()
				for language_id, attribute_language_data in attribute_src['option_languages'].items():
					if attribute_language_data['option_name'] == attribute_src['option_name']:
						continue
					store_id = self.get_map_store_view(language_id)
					if store_id in list_attribute_label_store:
						continue
					list_attribute_label_store.append(store_id)
					check_label_in_map = self.get_map_field_by_src('att_label', attribute_id, store_id, 'code_desc')
					if check_label_in_map:
						continue
					check_label_in_target = self.check_attribute_label_store_exist(attribute_id, store_id)
					if check_label_in_target:
						self.insert_map('att_label', attribute_id, None, store_id, check_label_in_target)
						continue
					eav_attribute_label_data = {
						'attribute_id': attribute_id,
						'store_id': store_id,
						'value': attribute_language_data['option_name']
					}
					all_query.append(self.create_insert_query_connector('eav_attribute_label', eav_attribute_label_data))
					self.insert_map('att_label', attribute_id, None, store_id, attribute_language_data['option_name'])

				if not attribute_src['option_type']:
					attribute_src['option_type'] = 'select'
				if attribute_src['option_type'] == 'select':

					# option_data_configurable = attribute_configurable.get('option_data', list())
					# for option_data in option_data_configurable:
					option_id = self.check_option_exist(attribute_src['option_value_name'], attr_src_code)
					if not option_id:
						eav_attribute_option_data = {
							'attribute_id': attribute_id,
							'sort_order': 0,
						}
						option_id = self.import_product_data_connector(self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data))
						if not option_id:
							continue
						eav_attribute_option_value_data = {
							'option_id': option_id,
							'store_id': 0,
							'value': attribute_src['option_value_name']
						}
						self.import_product_data_connector(self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
					if option_id:
						data_attribute_insert[attr_src_code] = option_id
						list_attr[attribute_src['option_name']] = attribute_id
						list_attribute_value_store = list()
						for language_id, attribute_value_language_data in attribute_src['option_value_languages'].items():
							if attribute_value_language_data['option_value_name'] == attribute_src['option_value_name']:
								continue
							store_id = self.get_map_store_view(language_id)
							if store_id in list_attribute_value_store:
								continue
							list_attribute_value_store.append(store_id)
							check_value_in_map = self.get_map_field_by_src('att_value_store', attribute_id, store_id, 'code_desc')
							if check_value_in_map:
								continue
							check_value_in_target = self.check_option_value_store_exist(option_id, store_id)
							if check_value_in_target:
								self.insert_map('att_value_store', option_id, None, store_id, check_value_in_target)
								continue
							eav_attribute_option_value_data = {
								'option_id': option_id,
								'store_id': store_id,
								'value': attribute_value_language_data['option_value_name']
							}
							all_query.append(self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
							self.insert_map('att_value_store', attribute_id, None, store_id, attribute_value_language_data['option_value_name'])
				elif attribute_src['option_type'] == 'multiselect':

					# option_data_configurable = attribute_configurable.get('option_data', list())
					# for option_data in option_data_configurable:
					attribute_values = to_str(attribute_src['option_value_name']).split(';')
					list_options = list()
					for attribute_value in attribute_values:
						option_id = self.check_option_exist(attribute_value, attr_src_code)
						if not option_id:
							eav_attribute_option_data = {
								'attribute_id': attribute_id,
								'sort_order': 0,
							}
							option_id = self.import_product_data_connector(
								self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data))
							if not option_id:
								continue
							eav_attribute_option_value_data = {
								'option_id': option_id,
								'store_id': 0,
								'value': attribute_value
							}
							self.import_product_data_connector(
								self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
						if option_id:
							list_options.append(to_str(option_id))
					list_options = list(set(list_options))
					data_attribute_insert[attr_src_code] = ','.join(list_options)
				else:
					data_attribute_insert[attr_src_code] = attribute_src['option_value_name']
		if image_name:
			data_attribute_insert['image_label'] = convert.get('image', {}).get('label', None)
		if convert['downloadable']:
			data_attribute_insert['links_title'] = 'Links'
			data_attribute_insert['links_purchased_separately'] = '1'
			data_attribute_insert['samples_title'] = 'Samples'
		if convert['type'] == 'bundle':
			data_attribute_insert['price_type'] = convert.get('price_type')
			data_attribute_insert['weight_type'] = convert.get('weight_type')
			data_attribute_insert['sku_type'] = convert.get('sku_type')
		manufacturer_src_id = convert.get('manufacturer', {}).get('id')
		manufacturer_id = False
		if manufacturer_src_id:
			manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, manufacturer_src_id)
			if not manufacturer_id:
				manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, None, manufacturer_src_id)
			if manufacturer_id and 'manufacturer' in product_eav_attribute_data:
				data_attribute_insert['manufacturer'] = manufacturer_id
		if not manufacturer_id and convert.get('manufacturer', {}).get('code'):
			manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, None, convert.get('manufacturer', {}).get('code'))
			if manufacturer_id and 'manufacturer' in product_eav_attribute_data:
				data_attribute_insert['manufacturer'] = manufacturer_id

		if not manufacturer_id and convert['manufacturer']['name']:
			if 'manufacturer' not in product_eav_attribute_data:
				product_eav_attribute_queries = {
					'eav_attribute': {
						'type': "select",
						'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = 4 and attribute_code = 'manufacturer'"
					},
				}
				product_eav_attribute = self.get_connector_data(url_query, {
					'serialize': True,
					'query': json.dumps(product_eav_attribute_queries)
				})
				try:
					attribute_id = product_eav_attribute['data']['eav_attribute'][0]['attribute_id']
				except Exception:
					attribute_id = self.create_attribute('manufacturer', 'int', 'select', 4, 'Manufacturer')
				if not attribute_id:
					response_error('can not get attribute code manufacturer')
			else:
				attribute_id = product_eav_attribute_data['manufacturer']['attribute_id']
			manufacturer_id = self.check_option_exist(convert['manufacturer']['name'], 'manufacturer')
			if not manufacturer_id:
				eav_attribute_option_data = {
					'attribute_id': attribute_id,
					'sort_order': 0
				}
				manufacturer_id = self.import_manufacturer_data_connector(
					self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data), True,
					convert['id'])
				if manufacturer_id:
					# return response_error('Error import manufacturer')
					eav_attribute_option_value_data = {
						'option_id': manufacturer_id,
						'store_id': 0,
						'value': convert['manufacturer']['name'],
					}
					self.import_manufacturer_data_connector(
						self.create_insert_query_connector('eav_attribute_option_value',
						                                   eav_attribute_option_value_data))
					self.insert_map(self.TYPE_MANUFACTURER, convert['id'], manufacturer_id)
					data_attribute_insert['manufacturer'] = manufacturer_id
			else:
				data_attribute_insert['manufacturer'] = manufacturer_id
		special_attribute = ['price_type', 'weight_type', 'sku_type']
		for key1, value1 in product_eav_attribute_data.items():
			for key2, value2 in data_attribute_insert.items():
				if key2 != key1:
					continue
				if key2 in special_attribute:
					value2 = value2 if value2 else 0
				elif not value2:
					continue
				product_attr_data = {
					'attribute_id': value1['attribute_id'],
					'store_id': 0,
					'entity_id': product_id,
					'value': value2,
				}
				all_query.append(self.create_insert_query_connector('catalog_product_entity_' + value1['backend_type'], product_attr_data))

		# begin: product attribute multi language
		if 'languages' in convert:
			for language_id, language_data in convert['languages'].items():
				if (language_id not in self._notice['map']['languages']) or (language_id in self._notice['map']['languages'] and self._notice['map']['languages'][language_id] == self._notice['target']['language_default']):
					continue
				target_language_id = self._notice['map']['languages'][language_id]
				data_attribute_insert = {
					'name': self.strip_html_tag(language_data.get('name')),
					'meta_title': language_data.get('meta_title'),
					'meta_description': language_data.get('meta_description'),
					'description': self.change_img_src_in_text(language_data.get('description')),
					'short_description': self.change_img_src_in_text(language_data.get('short_description')),
					'status': language_data.get('status'),
					'url_key': language_data.get('url_key').lower() if language_data.get('url_key') else language_data.get('url_key'),
					'url_path': language_data.get('url_path'),
				}
				for key1, value1 in product_eav_attribute_data.items():
					for key2, value2 in data_attribute_insert.items():
						if key2 != key1:
							continue
						store_id = self.get_map_store_view(language_id)
						if key2 == 'url_key':
							value2 = self.get_product_url_key(value2, store_id, language_data.get('name'), product_id = product_id)
						if key2 == 'url_path':
							value2 = self.get_product_url_path(value2, store_id, None)
						if not value2:
							continue
						product_attr_data = {
							'attribute_id': value1['attribute_id'],
							'store_id': store_id,
							'entity_id': product_id,
							'value': value2,
						}
						all_query.append(
							self.create_insert_query_connector('catalog_product_entity_' + value1['backend_type'],
							                                   product_attr_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------
				# ------------------------------------------------------------------------------------------------------------------------
		if self.get_migrate_product_extend_config():
			# todo: custom option  product
			# begin
			option_list = list()
			children_list = list()
			option_from_child = list()
			if convert['children']:
				if to_len(children_list) <= self.VARIANT_LIMIT:
					children_list = convert['children']
				else:
					option_from_child = self.convert_child_to_option(children_list)
			if convert['options'] and not convert['children']:
				if children_list or convert['type'] != self.PRODUCT_CONFIG or self.count_child_from_option(convert['options']) > self.VARIANT_LIMIT:
					option_list = convert['options']
				else:
					children_list = self.convert_option_to_child(convert['options'], convert)
			option_list = list(option_list + option_from_child)

			if option_list:
				for item in option_list:
					# option_value = item.get('value')
					catalog_product_option_data = {
						'product_id': product_id,
						'type': self.get_option_type_by_src_type(item.get('option_type')),
						'is_require': 1 if item['required'] else 0,
						'sku': get_value_by_key_in_dict(item, 'option_code', None),
						# 'max_characters': item.get('option_max_characters'),
						# 'file_extension': item.get('option_max_characters'),
						# 'image_size_x': item.get('option_image_size_x'),
						# 'image_size_y': item.get('option_image_size_y'),
						'sort_order': get_value_by_key_in_dict(item, 'option_sort_order', 0),
					}
					catalog_product_option_id = self.import_product_data_connector(
						self.create_insert_query_connector('catalog_product_option', catalog_product_option_data))
					if not catalog_product_option_id:
						continue
					catalog_product_option_title_data = {
						'option_id': catalog_product_option_id,
						'store_id': 0,
						'title': item.get('option_name'),
					}
					all_query.append(self.create_insert_query_connector('catalog_product_option_title', catalog_product_option_title_data))
					option_languages = item.get('option_languages')
					if option_languages:
						for option_language_id, option_language_data in option_languages.items():
							if self.get_map_store_view(option_language_id) == 0:
								continue
							catalog_product_option_title_data = {
								'option_id': catalog_product_option_id,
								'store_id': self.get_map_store_view(option_language_id),
								'title': option_language_data.get('option_name'),
							}
							all_query.append(self.create_insert_query_connector('catalog_product_option_title', catalog_product_option_title_data))

							if to_len(item['values']) == 0:
								catalog_product_option_price_data = {
									'option_id': catalog_product_option_id,
									'store_id': self.get_map_store_view(option_language_id),
									'price': option_language_data.get('option_name_price', '0.0000'),
									'price_type': option_language_data.get('price_type', 'fixed'),
								}
								all_query.append(self.create_insert_query_connector('catalog_product_option_price', catalog_product_option_price_data))

					if item['values']:
						for option_type in item['values']:
							catalog_product_option_type_value_data = {
								'option_id': catalog_product_option_id,
								'sku': option_type.get('option_value_code'),
								'sort_order': option_type.get('sort_order', 0),
							}
							catalog_product_option_type_value_id = self.import_product_data_connector(
								self.create_insert_query_connector('catalog_product_option_type_value', catalog_product_option_type_value_data))
							if not catalog_product_option_type_value_id:
								continue
							option_value_languages = option_type.get('option_value_languages')
							catalog_product_option_type_title_data = {
								'option_type_id': catalog_product_option_type_value_id,
								'store_id': 0,
								'title': option_type.get('option_value_name'),
							}
							all_query.append(self.create_insert_query_connector('catalog_product_option_type_title', catalog_product_option_type_title_data))

							catalog_product_option_type_price_data = {
								'option_type_id': catalog_product_option_type_value_id,
								'store_id': 0,
								# 'price': option_type.get('price_prefix', '') + option_type.get('option_value_price', '0.0000'),
								'price': to_str(option_type.get('option_value_price', '')),
								'price_type': option_type.get('option_value_price_type', 'fixed'),
							}
							all_query.append(self.create_insert_query_connector('catalog_product_option_type_price', catalog_product_option_type_price_data))
							if option_value_languages:
								for option_value_language_id, option_value_language_data in option_value_languages.items():
									if self.get_map_store_view(option_value_language_id) == 0:
										continue
									catalog_product_option_type_title_data = {
										'option_type_id': catalog_product_option_type_value_id,
										'store_id': self.get_map_store_view(option_value_language_id),
										'title': option_value_language_data.get('option_value_name'),
									}
									all_query.append(self.create_insert_query_connector('catalog_product_option_type_title', catalog_product_option_type_title_data))

									catalog_product_option_type_price_data = {
										'option_type_id': catalog_product_option_type_value_id,
										'store_id': self.get_map_store_view(option_value_language_id),
										'price': item.get('price_prefix', '') + to_str(option_value_language_data.get('option_value_price', '0.0000')),
										'price_type': option_value_language_data.get('option_value_price_type', 'fixed'),
									}
									all_query.append(self.create_insert_query_connector('catalog_product_option_type_price', catalog_product_option_type_price_data))
			# end
			# todo: assign children to configurable product
			# begin
			all_attributes = dict()
			if children_list:
				child_ids = list()
				for product_child in children_list:
					product_child['visibility'] = 1
					child_import = self.product_import(product_child, {}, {})
					if child_import['result'] == 'success' and child_import['data']:
						after_import = self.after_product_import(child_import['data'], product_child, {}, {})
						if after_import['result'] == 'success' and not all_attributes:
							all_attributes = after_import['data']
						child_id = child_import['data']
						child_ids.append(child_id)
					else:
						continue
				for attribute_label, attribute_id in all_attributes.items():
					catalog_product_super_attribute_data = {
						'product_id': product_id,
						'attribute_id': attribute_id,
						'position': 0,
					}
					product_super_attribute_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_super_attribute',catalog_product_super_attribute_data))
					if not product_super_attribute_id:
						continue
					if attribute_label:
						catalog_product_super_attribute_label_data = {
							'product_super_attribute_id': product_super_attribute_id,
							'store_id': 0,
							'use_default': 1,
							'value': attribute_label
						}
						all_query.append(self.create_insert_query_connector('catalog_product_super_attribute_label',catalog_product_super_attribute_label_data))
				if child_ids:
					for child_id_imported in child_ids:
						catalog_product_relation_data = {
							'parent_id': product_id,
							'child_id': child_id_imported
						}
						all_query.append(self.create_insert_query_connector('catalog_product_relation', catalog_product_relation_data))

						catalog_product_super_link_data = {
							'product_id': child_id_imported,
							'parent_id': product_id
						}
						all_query.append(self.create_insert_query_connector('catalog_product_super_link',catalog_product_super_link_data))

			# end
			# ------------------------------------------------------------------------------------------------------------------------

			# todo: downloadable product
			# begin
			if convert['downloadable']:
				for downloadable_link in convert['downloadable']:
					downloadable_link_data = {
						'product_id': product_id,
						'sort_order': 0,
						'number_of_downloads': downloadable_link['limit'] if downloadable_link['limit'] else 0,
						'is_shareable': 1,
						'link_url': downloadable_link['path'],
						'link_file': downloadable_link['path'],
						'link_type': 'url' if 'http' in downloadable_link['path'] else 'file',
						'sample_url': downloadable_link['sample']['path'],
						'sample_file': downloadable_link['sample']['path'],
						'sample_type': 'url' if 'http' in downloadable_link['sample']['path'] else 'file',
					}
					downloadable_link_id = self.import_product_data_connector(self.create_insert_query_connector('downloadable_link', downloadable_link_data))
					if not downloadable_link_id:
						continue
					downloadable_link_title_data = {
						'link_id': downloadable_link_id,
						'store_id': 0,
						'title': downloadable_link['name']
					}
					all_query.append(self.create_insert_query_connector('downloadable_link_title', downloadable_link_title_data))

					downloadable_link_price_data = {
						'link_id': downloadable_link_id,
						'website_id': 0,
						'price': downloadable_link['price']
					}
					all_query.append(self.create_insert_query_connector('downloadable_link_price', downloadable_link_price_data))

			# downloadable = convert.get('downloadable', dict())
			# if 'link' in downloadable:
			# 	for downloadable_link in downloadable['link']:
			# 		link = downloadable_link['link']
			# 		title = downloadable_link['title']
			# 		price = downloadable_link['price']
			#
			# 		downloadable_link_data = {
			# 			'product_id': product_id,
			# 			'sort_order': 0,
			# 			'number_of_downloads': link.get('number_of_download', 0),
			# 			'is_shareable': link.get('is_shareable'),
			# 			'link_url': link.get('link_url'),
			# 			'link_file': link.get('link_file'),
			# 			'link_type': link.get('link_type'),
			# 			'sample_url': link.get('sample_url'),
			# 			'sample_file': link.get('sample_file'),
			# 			'sample_type': link.get('sample_type'),
			# 		}
			# 		downloadable_link_id = self.import_product_data_connector(self.create_insert_query_connector('downloadable_link', downloadable_link_data))
			# 		if not downloadable_link_id:
			# 			continue
			# 		downloadable_link_title_data = {
			# 			'link_id': downloadable_link_id,
			# 			'store_id': 0,
			# 			'title': title.get('title')
			# 		}
			# 		all_query.append(self.create_insert_query_connector('downloadable_link_title', downloadable_link_title_data))
			#
			# 		downloadable_link_price_data = {
			# 			'link_id': downloadable_link_id,
			# 			'website_id': 0,
			# 			'price': price.get('price')
			# 		}
			# 		all_query.append(self.create_insert_query_connector('downloadable_link_price', downloadable_link_price_data))
			#
			# if 'samples' in downloadable:
			# 	for downloadable_sample in downloadable['samples']:
			# 		sample = downloadable_sample['sample']
			# 		title = downloadable_sample.get('title', {})
			#
			# 		downloadable_sample_data = {
			# 			'product_id': product_id,
			# 			'sample_url': sample.get('sample_url'),
			# 			'sample_file': sample.get('sample_file'),
			# 			'sample_type': sample.get('sample_type'),
			# 			'sort_order': sample.get('sort_order', 0),
			# 		}
			# 		downloadable_sample_id = self.import_product_data_connector(
			# 			self.create_insert_query_connector('downloadable_sample', downloadable_sample_data))
			# 		if not downloadable_sample_id:
			# 			continue
			# 		downloadable_sample_title_data = {
			# 			'sample_id': downloadable_sample_id,
			# 			'store_id': 0,
			# 			'title': title.get('title')
			# 		}
			# 		all_query.append(self.create_insert_query_connector('downloadable_sample_title', downloadable_sample_title_data))
			# end
			# ------------------------------------------------------------------------------------------------------------------------
			# todo: parent grouped product
			# begin
			if convert.get('parent_grouped'):
				for grouped_parent_id in convert['parent_grouped']:
					parent_id_desc = self.get_map_field_by_src(self.TYPE_PRODUCT, grouped_parent_id)
					if not parent_id_desc:
						continue
					catalog_product_relation_data = {
						'parent_id': parent_id_desc,
						'child_id': product_id,
					}
					all_query.append(
						self.create_insert_query_connector('catalog_product_relation', catalog_product_relation_data))

					catalog_product_link_data = {
						'product_id': parent_id_desc,
						'linked_product_id': product_id,
						'link_type_id': 3
					}
					all_query.append(self.create_insert_query_connector('catalog_product_link', catalog_product_link_data))
			if convert.get('group_products'):
				for grouped_child_id in convert['group_products']:
					child_id_desc = self.get_map_field_by_src(self.TYPE_PRODUCT, grouped_child_id)
					if not child_id_desc:
						continue
					catalog_product_relation_data = {
						'parent_id': product_id,
						'child_id': child_id_desc,
					}
					all_query.append(
						self.create_insert_query_connector('catalog_product_relation', catalog_product_relation_data))

					catalog_product_link_data = {
						'product_id': product_id,
						'linked_product_id': child_id_desc,
						'link_type_id': 3
					}
					all_query.append(self.create_insert_query_connector('catalog_product_link', catalog_product_link_data))
			# end
			# ------------------------------------------------------------------------------------------------------------------------
			# todo:parent bundle product
			# begin

			if 'parent_bundle' in convert:
				for parent_bundle in convert['parent_bundle']:
					bundle_product_parent_import = self.import_parent_product(parent_bundle)
					if bundle_product_parent_import['result'] != 'success':
						continue
					bundle_product_parent_id = bundle_product_parent_import['data']
					if not bundle_product_parent_id:
						continue
					catalog_product_relation_data = {
						'parent_id': bundle_product_parent_id,
						'child_id': product_id,
					}
					all_query.append(
						self.create_insert_query_connector('catalog_product_relation', catalog_product_relation_data))

					if 'parent_bundle_selection' in convert:
						for parent_bundle_selection in convert['parent_bundle_selection']:
							if parent_bundle_selection['parent_product_id'] != parent_bundle['id']:
								continue
							bundle_option_id = self.get_map_field_by_src(self.TYPE_BUNDLE_OPTION,
							                                             parent_bundle_selection['option_id'])
							if not bundle_option_id:
								continue
							catalog_product_bundle_selection_data = {
								'product_id': product_id,
								'option_id': bundle_option_id,
								'parent_product_id': bundle_product_parent_id,
								'position': parent_bundle_selection.get('position', 0),
								'is_default': parent_bundle_selection.get('is_default', 0),
								'selection_price_type': parent_bundle_selection.get('selection_price_type', 0),
								'selection_price_value': parent_bundle_selection.get('selection_price_value', '0.0000'),
								'selection_qty': parent_bundle_selection.get('selection_qty'),
								'selection_can_change_qty': parent_bundle_selection.get('selection_can_change_qty', 0),
							}
							all_query.append(self.create_insert_query_connector('catalog_product_bundle_selection',
							                                                    catalog_product_bundle_selection_data))

			# begin: bundle option

			if 'bundle_option' in convert:
				for bundle_option in convert['bundle_option']:
					catalog_product_bundle_option_data = {
						'parent_id': product_id,
						'required': bundle_option.get('option', {}).get('required', 0),
						'position': bundle_option.get('option', {}).get('position', 0),
						'type': bundle_option.get('option', {}).get('type'),
					}
					bundle_option_id = self.import_product_data_connector(
						self.create_insert_query_connector('catalog_product_bundle_option',
						                                   catalog_product_bundle_option_data))
					if not bundle_option_id:
						continue
					self.insert_map(self.TYPE_BUNDLE_OPTION, bundle_option['option']['option_id'], bundle_option_id)

					for bundle_option_value in bundle_option['value']:
						catalog_product_bundle_option_value_data = {
							'option_id': bundle_option_id,
							'store_id': self.get_map_store_view(bundle_option_value.get('store_id', 0)),
							'title': bundle_option_value.get('title')
						}
						if self.convert_version(self._notice['target']['config']['version'], 2) >= 220:
							catalog_product_bundle_option_value_data['parent_product_id'] = product_id
						all_query.append(self.create_insert_query_connector('catalog_product_bundle_option_value',
						                                                    catalog_product_bundle_option_value_data))

			# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: product relate(1), up-sell(4), cross-sell(5)
		# begin
		if convert['relate']['children']:
			index = 1
			for relate_id in convert['relate']['children']:
				relate_desc_id = self.get_map_field_by_src(self.TYPE_PRODUCT, relate_id['id'])
				if not relate_desc_id:
					relate_desc_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, relate_id['id'])
				if relate_desc_id:
					catalog_product_link_relate_data = {
						"product_id": product_id,
						"linked_product_id": relate_desc_id,
						"link_type_id": self.get_type_relation_product(relate_id['type']),
					}
					link_id = self.import_product_data_connector(
						self.create_insert_query_connector('catalog_product_link', catalog_product_link_relate_data))

					if link_id:
						query = {
							'type': 'select',
							'query': "SELECT * FROM _DBPRF_catalog_product_link_attribute WHERE link_type_id = " + to_str(self.get_type_relation_product(relate_id['type']))
						}
						product_link_attribute_id_data = self.get_connector_data(self.get_connector_url('query'),
						                                                         {'query': json.dumps(query)})
						product_link_attribute_id = product_link_attribute_id_data['data'][0][
							'product_link_attribute_id']
						catalog_product_link_attribute_int = {
							"product_link_attribute_id": product_link_attribute_id,
							"link_id": link_id,
							"value": index,
						}
						index += 1
						all_query.append(self.create_insert_query_connector('catalog_product_link_attribute_int',
						                                                    catalog_product_link_attribute_int))
		if convert['relate']['parent']:
			for relate_id2 in convert['relate']['parent']:
				relate_desc_id = self.get_map_field_by_src(self.TYPE_PRODUCT, relate_id2['id'])
				if not relate_desc_id:
					relate_desc_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, relate_id2['id'])
				if relate_desc_id:
					link_type_id = self.get_type_relation_product(relate_id2['type'])
					catalog_product_link_relate_data = {
						"product_id": relate_desc_id,
						"linked_product_id": product_id,
						"link_type_id": link_type_id,
					}
					link_id = self.import_product_data_connector(
						self.create_insert_query_connector('catalog_product_link', catalog_product_link_relate_data))
					if link_id:
						# get product_link_attribute_id
						product_link_attribute_id = self.get_type_relation_product(relate_id2['type'])
						query = {
							'type': 'select',
							'query': "SELECT * FROM _DBPRF_catalog_product_link_attribute WHERE link_type_id = " + to_str(link_type_id)
						}
						product_link_attribute_id_data = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
						product_link_attribute_id = product_link_attribute_id_data['data'][0]['product_link_attribute_id']
						query = {
							'type': 'select',
							'query': " SELECT " + to_str(product_link_attribute_id) + ", " + to_str(link_id) + " ," \
							                                                                                   " MAX(`value`)+1 FROM _DBPRF_catalog_product_link_attribute_int WHERE `link_id` IN " \
							                                                                                   " (SELECT link_id FROM _DBPRF_catalog_product_link WHERE product_id = " + to_str(
								relate_desc_id) + " and link_type_id = " + to_str(link_type_id) + ")"
						}
						catalog_product_link_attribute_check = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
						if not catalog_product_link_attribute_check['data'][0]['MAX(`value`)+1']:
							query = "INSERT INTO _DBPRF_catalog_product_link_attribute_int (`product_link_attribute_id`,`link_id`, `value`) VALUES (" + to_str(link_type_id) + ", " + to_str(link_id) + " , 0" + ")"
						else:
							query = "INSERT INTO _DBPRF_catalog_product_link_attribute_int (`product_link_attribute_id`,`link_id`, `value`)" \
							        " SELECT " + to_str(product_link_attribute_id) + ", " + to_str(link_id) + " ," \
							                                                                                  " MAX(`value`)+1 FROM _DBPRF_catalog_product_link_attribute_int WHERE `link_id` IN " \
							                                                                                  " (SELECT link_id FROM _DBPRF_catalog_product_link WHERE product_id = " + to_str(
								relate_desc_id) + " and link_type_id = " + to_str(link_type_id) + ")"

						all_query.append({
							'type': 'insert',
							'query': query
						})

		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: attribute remain
		# begin

		# ------------------------------------------------------------------------------------------------------------------------
		# todo: seo
		# begin
		if not convert['is_child']:
			seo_default = dict()

			# if not self._notice['config']['seo'] or self._notice['config']['seo_301']:
				# seo_default = self.generate_url_key(convert['name'])
			store_target = list(self._notice['map']['languages'].values())
			store_target = list(map(lambda x: to_int(x), store_target))
			if 0 not in store_target:
				store_target.append(0)
			for store_id in store_target:
				try:
					name = convert['languages'][to_str(store_id)]['name']
				except:
					name = convert['name']
				if not name:
					name = convert['name']
				seo = self.generate_url_key(name)
				seo = self.get_request_path(seo, store_id)
				seo_default[store_id] = seo
				url_rewrite_data = {
					'entity_type': 'product',
					'entity_id': product_id,
					'request_path': seo,
					'target_path': 'catalog/product/view/id/' + to_str(product_id),
					'redirect_type': 0,
					'store_id': store_id,
					'description': None,
					'is_autogenerated': 1,
					'metadata': None,
				}
				self.import_category_data_connector(
					self.create_insert_query_connector('url_rewrite', url_rewrite_data))

			is_default = False
			seo_301 = self._notice['config']['seo_301']
			if (self._notice['config']['seo'] or self._notice['config']['seo_301']) and 'seo' in convert:
				for url_rewrite_product in convert['seo']:
					store_id = self.get_map_store_view(url_rewrite_product['store_id'])
					if url_rewrite_product['request_path'] == seo_default.get(to_int(store_id)):
						continue
					if seo_301:
						is_default = True
					default = True if url_rewrite_product['default'] and not is_default else False
					if default:
						is_default = True
					if 'category_id' not in url_rewrite_product or not url_rewrite_product['category_id']:
						url_rewrite_product_data = {
							'entity_type': 'product',
							'entity_id': product_id,
							'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
							'target_path': seo_default.get(to_int(store_id)) if seo_301 else 'catalog/product/view/id/' + to_str(product_id),
							'redirect_type': self.SEO_301 if seo_301 else 0,
							'store_id': store_id,
							'description': None,
							'is_autogenerated': 0,
							'metadata': None,
						}
						self.import_product_data_connector(
							self.create_insert_query_connector('url_rewrite', url_rewrite_product_data))
					else:
						category_id = url_rewrite_product['category_id']
						if not category_id:
							continue
						category_desc_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category_id)
						if not category_desc_id:
							continue
						metadata = {
							'category_id': category_desc_id
						}
						url_rewrite_data = {
							'entity_type': 'product',
							'entity_id': product_id,
							'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
							'target_path': seo_default.get(to_int(store_id)) if seo_301 else 'catalog/product/view/id/' + to_str(product_id) + '/category/' + to_str(category_desc_id),
							'redirect_type': self.SEO_301 if seo_301 else 0,
							'store_id': store_id,
							'description': None,
							'is_autogenerated': 1 ,
							'metadata': self.magento_serialize(metadata),
						}
						url_rewrite_id = self.import_product_data_connector(
							self.create_insert_query_connector('url_rewrite', url_rewrite_data))
						if not url_rewrite_id:
							continue
						if not seo_301:
							catalog_product_url_rewrite_data = {
								'url_rewrite_id': url_rewrite_id,
								'category_id': category_id,
								'product_id': product_id,
							}
							all_query.append(self.create_insert_query_connector('catalog_url_rewrite_product_category',catalog_product_url_rewrite_data))

		# end
		# ------------------------------------------------------------------------------------------------------------------------

		self.import_multiple_data_connector(all_query, 'products')
		return response_success(list_attr)

	def update_latest_data_product(self, product_id, convert, product, products_ext):
		old_url_key = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'], 'code_desc')
		all_query = list()
		all_query.append(self.create_delete_query_connector('catalog_category_product', {'product_id': product_id}))
		category_desc = list()
		for value in convert['categories']:
			category_ids = list()
			try:
				category_ids = self._notice['map']['category_data'][value['id']]
			except KeyError:
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
				catalog_category_product_data = {
					'category_id': category_id,
					'product_id': product_id,
					'position': get_value_by_key_in_dict(value, 'position', 1),
				}
				all_query.append(
					self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))

		# sotck
		manage_stock_data = convert.get('manage_stock_data')
		if manage_stock_data:
			low_stock_date = manage_stock_data.get('low_stock_date')
			if low_stock_date and low_stock_date == '0000-00-00 00:00:00':
				low_stock_date = None

			cataloginventory_stock_item_data = {
				'qty': convert.get('qty'),
				'min_qty': manage_stock_data.get('min_qty', '0.0000'),
				'use_config_min_qty': manage_stock_data.get('use_config_min_qty', 1),
				'is_qty_decimal': manage_stock_data.get('is_qty_decimal', 0),
				'backorders': manage_stock_data.get('backorders', 0),
				'use_config_backorders': manage_stock_data.get('use_config_backorders', 1),
				'min_sale_qty': manage_stock_data.get('min_sale_qty', '1.0000'),
				'use_config_min_sale_qty': manage_stock_data.get('use_config_min_sale_qty', 1),
				'max_sale_qty': manage_stock_data.get('max_sale_qty', '0.0000'),
				'use_config_max_sale_qty': manage_stock_data.get('use_config_max_sale_qty', 1),
				'is_in_stock': manage_stock_data.get('is_in_stock', 0),
				'low_stock_date': low_stock_date,
				'notify_stock_qty': manage_stock_data.get('notify_stock_qty'),
				'use_config_notify_stock_qty': manage_stock_data.get('use_config_notify_stock_qty', 1),
				'manage_stock': manage_stock_data.get('manage_stock', 0),
				'use_config_manage_stock': manage_stock_data.get('use_config_manage_stock', 1),
				'stock_status_changed_auto': manage_stock_data.get('stock_status_changed_auto', 0),
				'use_config_qty_increments': manage_stock_data.get('use_config_qty_increments', 0),
				'qty_increments': manage_stock_data.get('qty_increments', '0.0000'),
				'use_config_enable_qty_inc': manage_stock_data.get('use_config_enable_qty_inc', 1),
				'enable_qty_increments': manage_stock_data.get('enable_qty_increments', 0),
				'is_decimal_divided': manage_stock_data.get('is_decimal_divided', 0),
			}
			all_query.append(self.create_update_query_connector('cataloginventory_stock_item', cataloginventory_stock_item_data, {'product_id': product_id}))
		tax_class_id_src = convert.get('tax', {}).get('id', None)
		tax_class_id = 0
		if tax_class_id_src:
			tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_PRODUCT, tax_class_id_src)
		product_eav_attribute = self.select_all_attribute_map()
		product_eav_attribute_data = dict()
		for attribute_data in product_eav_attribute:
			attribute = json.loads(attribute_data['value'])
			if attribute['backend_type'] != 'static':
				if not attribute_data['code_src'] in product_eav_attribute_data:
					product_eav_attribute_data[attribute_data['code_src']] = dict()
				product_eav_attribute_data[attribute_data['code_src']]['attribute_id'] = attribute['attribute_id']
				product_eav_attribute_data[attribute_data['code_src']]['backend_type'] = attribute['backend_type']
				product_eav_attribute_data[attribute_data['code_src']]['frontend_input'] = attribute['frontend_input']
		data_attribute_insert = {
			'name': self.strip_html_tag(convert.get('name')),
			'meta_keyword': convert.get('meta_keyword'),
			'special_price': convert.get('special_price', {}).get('price'),
			'special_from_date': convert.get('special_price', {}).get('start_date'),
			'special_to_date': convert.get('special_price', {}).get('end_date'),
			'price': to_decimal(convert.get('price')),
			'tax_class_id': tax_class_id if tax_class_id else 0,
			'status': 1 if convert['status'] else 2,
		}
		is_change_url = False
		if convert['url_key'] != old_url_key:
			is_change_url = True
			pro_url_key = self.get_product_url_key(convert.get('url_key'), 0, convert.get('name', ''), product_id = product_id)
			data_attribute_insert['url_key'] = pro_url_key
		for key, value in data_attribute_insert.items():
			if key not in product_eav_attribute_data:
				continue
			if not value:
				continue
			all_query.append(self.create_delete_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'], {'entity_id': product_id, 'attribute_id': product_eav_attribute_data[key]['attribute_id']}))
			product_attr_data = {
				'attribute_id': product_eav_attribute_data[key]['attribute_id'],
				'store_id': 0,
				'entity_id': product_id,
				'value': value,
			}
			all_query.append(self.create_insert_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'], product_attr_data))
		if is_change_url and not convert.get('is_child'):
			# todo: seo
			# begin
			seo_queries = list()
			set_default = dict()
			delete_query = list()
			delete_query.append(self.create_delete_query_connector('url_rewrite', {'entity_type': 'product', 'entity_id': product_id}))
			delete_query.append(self.create_delete_query_connector('catalog_url_rewrite_product_category', {'product_id': product_id}))
			self.query_multiple_data_connector(delete_query)
			seo_default = dict()

			# if not self._notice['config']['seo'] or self._notice['config']['seo_301']:
			# seo_default = self.generate_url_key(convert['name'])
			store_target = list(self._notice['map']['languages'].values())
			store_target = list(map(lambda x: to_int(x), store_target))
			if 0 not in store_target:
				store_target.append(0)
			for store_id in store_target:
				try:
					name = convert['languages'][to_str(store_id)]['name']
				except:
					name = convert['name']
				if not name:
					name = convert['name']
				seo = self.generate_url_key(name)
				seo = self.get_request_path(seo, store_id)
				seo_default[store_id] = seo
				url_rewrite_data = {
					'entity_type': 'product',
					'entity_id': product_id,
					'request_path': seo,
					'target_path': 'catalog/product/view/id/' + to_str(product_id),
					'redirect_type': 0,
					'store_id': store_id,
					'description': None,
					'is_autogenerated': 1,
					'metadata': None,
				}
				self.import_category_data_connector(
					self.create_insert_query_connector('url_rewrite', url_rewrite_data))

			is_default = False
			seo_301 = self._notice['config']['seo_301']
			if (self._notice['config']['seo'] or self._notice['config']['seo_301']) and 'seo' in convert:
				for url_rewrite_product in convert['seo']:
					store_id = self.get_map_store_view(url_rewrite_product['store_id'])
					if url_rewrite_product['request_path'] == seo_default.get(to_int(store_id)):
						continue
					if seo_301:
						is_default = True
					default = True if url_rewrite_product['default'] and not is_default else False
					if default:
						is_default = True
					if 'category_id' not in url_rewrite_product or not url_rewrite_product['category_id']:
						url_rewrite_product_data = {
							'entity_type': 'product',
							'entity_id': product_id,
							'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
							'target_path': seo_default.get(to_int(store_id)) if seo_301 else 'catalog/product/view/id/' + to_str(product_id),
							'redirect_type': self.SEO_301 if seo_301 else 0,
							'store_id': store_id,
							'description': None,
							'is_autogenerated': 0,
							'metadata': None,
						}
						self.import_product_data_connector(
							self.create_insert_query_connector('url_rewrite', url_rewrite_product_data))
					else:
						category_id = url_rewrite_product['category_id']
						if not category_id:
							continue
						category_desc_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category_id)
						if not category_desc_id:
							continue
						metadata = {
							'category_id': category_desc_id
						}
						url_rewrite_data = {
							'entity_type': 'product',
							'entity_id': product_id,
							'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
							'target_path': seo_default.get(to_int(store_id)) if seo_301 else 'catalog/product/view/id/' + to_str(product_id) + '/category/' + to_str(category_desc_id),
							'redirect_type': self.SEO_301 if seo_301 else 0,
							'store_id': store_id,
							'description': None,
							'is_autogenerated': 1,
							'metadata': self.magento_serialize(metadata),
						}
						url_rewrite_id = self.import_product_data_connector(
							self.create_insert_query_connector('url_rewrite', url_rewrite_data))
						if not url_rewrite_id:
							continue
						if not seo_301:
							catalog_product_url_rewrite_data = {
								'url_rewrite_id': url_rewrite_id,
								'category_id': category_id,
								'product_id': product_id,
							}
							all_query.append(self.create_insert_query_connector('catalog_url_rewrite_product_category', catalog_product_url_rewrite_data))
		self.import_multiple_data_connector(all_query, 'update_demo_product')
		return response_success()

	def addition_product_import(self, convert, product, products_ext):
		return response_success()

	def finish_product_import(self):
		convert_version = parse_version(to_str(self._notice['target']['config']['version']).replace('.ee', ''))
		if convert_version >= parse_version("2.3.0"):
			query = 'CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW _DBPRF_inventory_stock_1 AS SELECT DISTINCT `legacy_stock_status`.`product_id` AS `product_id`,`legacy_stock_status`.`website_id` AS `website_id`,`legacy_stock_status`.`stock_id` AS `stock_id`,`legacy_stock_status`.`qty` AS `quantity`,`legacy_stock_status`.`stock_status` AS `is_salable`,`product`.`sku` AS `sku` FROM (`_DBPRF_cataloginventory_stock_status` `legacy_stock_status` JOIN `_DBPRF_catalog_product_entity` `product` on((`legacy_stock_status`.`product_id` = `product`.`entity_id`)))'
			self.query_data_connector({
				'type': 'query',
				'query': query
			})
		return response_success()
	# TODO: CUSTOMER
	def prepare_customers_import(self):
		if self._notice['config'].get('cus_pass'):
			all_queries = list()
			all_queries.append(self.create_delete_query_connector('core_config_data', {
				'path': 'lecupd/general/type'
			}))
			config_data = {
				'scope': 'default',
				'scope_id': '0',
				'path': 'lecupd/general/type',
				'value': self._notice['src']['cart_type']
			}
			option_query = self.create_insert_query_connector('core_config_data', config_data)
			all_queries.append(option_query)
			self.import_multiple_data_connector(all_queries)
		return self

	def prepare_customers_export(self):
		return self

	def get_customers_main_export(self):
		id_src = self._notice['process']['customers']['id_src']
		limit = self._notice['setting']['customers']
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = to_str(store_id_con) + ' AND '
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_customer_entity WHERE " + self.get_con_website_select_count() + " AND entity_id > " + to_str(
				id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit)
		}
		customers = self.select_data_connector(query, 'customers')

		if not customers or customers['result'] != 'success':
			return response_error()
		return customers

	def get_customers_ext_export(self, customers):
		url_query = self.get_connector_url('query')
		customer_ids = duplicate_field_value_from_list(customers['data'], 'entity_id')
		customer_id_con = self.list_to_in_condition(customer_ids)
		customer_ext_queries = {
			'customer_entity_datetime': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_entity_datetime WHERE entity_id IN " + customer_id_con
			},
			'customer_entity_decimal': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_entity_decimal WHERE entity_id IN " + customer_id_con
			},
			'customer_entity_int': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_entity_int WHERE entity_id IN " + customer_id_con
			},
			'customer_entity_text': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_entity_text WHERE entity_id IN " + customer_id_con
			},
			'customer_entity_varchar': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_entity_varchar WHERE entity_id IN " + customer_id_con
			},
			'customer_address_entity': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_address_entity WHERE parent_id IN " + customer_id_con
			},
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(
					self._notice['src']['extends']['customer']) + " OR entity_type_id = " + to_str(
					self._notice['src']['extends']['customer_address'])
			},
			'newsletter_subscriber': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_newsletter_subscriber WHERE customer_id IN " + customer_id_con
			},
		}
		customer_ext = self.select_multiple_data_connector(customer_ext_queries, 'customers_errors')

		if not customer_ext or customer_ext['result'] != 'success':
			return response_error()

		address_ids = duplicate_field_value_from_list(customer_ext['data']['customer_address_entity'], 'entity_id')
		address_id_con = self.list_to_in_condition(address_ids)
		region_ids = duplicate_field_value_from_list(customer_ext['data']['customer_address_entity'], 'region_id')
		region_ids_con = self.list_to_in_condition(region_ids)
		customer_ext_rel_queries = {
			'customer_address_entity_datetime': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_address_entity_datetime WHERE entity_id IN " + address_id_con
			},
			'customer_address_entity_decimal': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_address_entity_decimal WHERE entity_id IN " + address_id_con
			},
			'customer_address_entity_int': {
				'type': 'select',
				'query': "SELECT caei.*, dcr.* FROM _DBPRF_customer_address_entity_int as caei LEFT JOIN "
				         "_DBPRF_directory_country_region as dcr ON caei.value = dcr.region_id  WHERE caei.entity_id "
				         "IN " + address_id_con
			},
			'customer_address_entity_text': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_address_entity_text WHERE entity_id IN " + address_id_con
			},
			'customer_address_entity_varchar': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_address_entity_varchar WHERE entity_id IN " + address_id_con
			},
			'regions': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_directory_country_region WHERE region_id IN " + region_ids_con
			}
		}
		customer_ext_rel = self.select_multiple_data_connector(customer_ext_rel_queries, 'customers_errors')

		if not customer_ext_rel or customer_ext_rel['result'] != 'success':
			return response_error()
		customer_ext = self.sync_connector_object(customer_ext, customer_ext_rel)
		return customer_ext

	def convert_customer_export(self, customer, customers_ext):
		eav_attribute_cus = dict()
		eav_attribute_cusadd = dict()

		# for
		for attribute in customers_ext['data']['eav_attribute']:
			if attribute['entity_type_id'] == self._notice['src']['extends']['customer']:
				eav_attribute_cus[attribute['attribute_code']] = attribute['attribute_id']
			else:
				eav_attribute_cusadd[attribute['attribute_code']] = attribute['attribute_id']
		# endfor

		entity_int = get_list_from_list_by_field(customers_ext['data']['customer_entity_int'], 'entity_id',
		                                         customer['entity_id'])
		entity_varchar = get_list_from_list_by_field(customers_ext['data']['customer_entity_varchar'], 'entity_id',
		                                             customer['entity_id'])
		entity_datetime = get_list_from_list_by_field(customers_ext['data']['customer_entity_datetime'], 'entity_id',
		                                              customer['entity_id'])

		middle_name = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute_cus['middlename'],
		                                               'value')
		subscriber = get_list_from_list_by_field(customers_ext['data']['newsletter_subscriber'], 'customer_id',
		                                         customer['entity_id'])
		customer_data = self.construct_customer()
		customer_data = self.add_construct_default(customer_data)
		customer_data['id'] = customer['entity_id']
		customer_data['increment_id'] = customer['increment_id']
		customer_data['email'] = customer['email'].strip()
		customer_data['username'] = customer['email'].strip()
		customer_data['password'] = customer['password_hash']
		customer_data['first_name'] = customer['firstname'] if customer['firstname'] else ''
		customer_data['middle_name'] = middle_name if middle_name else ''
		customer_data['last_name'] = customer['lastname'] if customer['lastname'] else ''
		customer_group_ids = list()
		customer_group_ids.append(customer['group_id'])
		customer_data['group_id'] = customer['group_id']
		gender_id = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute_cus.get('gender'), 'value')
		customer_data['gender'] = gender_id if gender_id else 3
		customer_data['dob'] = customer['dob'] if customer['dob'] else ''
		customer_data['is_subscribed'] = subscriber
		customer_data['active'] = customer['is_active']
		customer_data['created_at'] = customer['created_at']
		customer_data['updated_at'] = customer['updated_at']
		customer_data['telephone'] = ''
		address_entity = get_list_from_list_by_field(customers_ext['data']['customer_address_entity'], 'parent_id', customer['entity_id'])
		if address_entity:
			for address in address_entity:
				address_entity_int = get_list_from_list_by_field(customers_ext['data']['customer_address_entity_int'],
				                                                 'entity_id', address['entity_id'])
				address_entity_text = get_list_from_list_by_field(customers_ext['data']['customer_address_entity_text'],
				                                                  'entity_id', address['entity_id'])
				address_entity_varchar = get_list_from_list_by_field(
					customers_ext['data']['customer_address_entity_varchar'], 'entity_id', address['entity_id'])
				address_data = self.construct_customer_address()
				address_data = self.add_construct_default(address_data)
				address_data['id'] = address['entity_id']
				if address['entity_id'] == customer['default_billing']:
					address_data['default']['billing'] = True
				if address['entity_id'] == customer['default_shipping']:
					address_data['default']['shipping'] = True
				address_data['first_name'] = address['firstname']
				address_data['last_name'] = address['lastname']
				street = address['street']
				if street:
					street_line = street.splitlines()
					address_data['address_1'] = street_line[0] if to_len(street_line) > 0 else ''
					address_data['address_2'] = street_line[1] if to_len(street_line) > 1 else ''
				address_data['city'] = address['city'] if address['city'] else ''
				address_data['postcode'] = address['postcode'] if address['postcode'] else ''
				address_data['telephone'] = address['telephone'] if address['telephone'] else ''
				customer_data['telephone'] = address['telephone'] if (not customer_data['telephone']) and (
					address['telephone']) else ''
				address_data['company'] = address['company'] if address['company'] else ''
				address_data['fax'] = address['fax'] if address['fax'] else ''
				country_code = address['country_id']
				if country_code:
					address_data['country']['country_code'] = country_code
					address_data['country']['name'] = self.get_country_name_by_code(country_code)
				else:
					address_data['country']['country_code'] = 'US'
					address_data['country']['name'] = 'United States'

				state_id = address['region_id']
				state = get_row_from_list_by_field(customers_ext['data']['regions'], 'region_id', state_id)
				if state and state['code'] and state['default_name']:
					address_data['state']['state_code'] = state['code']
					address_data['state']['name'] = state['default_name']
				else:
					address_data['state']['state_code'] = ''
					address_data['state']['name'] = address['region']
				customer_data['address'].append(address_data)
		return response_success(customer_data)

	def get_customer_id_import(self, convert, customer, customers_ext):
		return customer['entity_id']

	def check_customer_import(self, convert, customer, customers_ext):
		return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id']) else False

	def router_customer_import(self, convert, customer, customers_ext):
		return response_success('customer_import')

	def before_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def customer_import(self, convert, customer, customers_ext):
		# store_id = self.get_map_store_view(customer['store_id'])
		if convert.get('store_id'):
			store_src_id = convert['store_id']
		else:
			store_src_id = 0
			if self._notice['src']['language_default']:
				store_src_id = self._notice['src']['language_default']
			else:
				for language_id, language_label in self._notice['src']['languages'].items():
					store_src_id = language_id
					break
		store_id = self.get_map_store_view(store_src_id)
		gender_id = 0
		# store_id = 0
		if self._notice['support']['site_map'] and 'store_id' in convert and convert['store_id'] in self._notice['map']['site']:
			store_id = self._notice['map']['site'][convert['store_id']]
		if convert.get('gender'):
			if convert.get('gender') == 'Male' or to_str(convert.get('gender')) == '1':
				gender_id = 1
			if convert.get('gender') == 'Female' or to_str(convert.get('gender')) == '0':
				gender_id = 2

		customer_entity_data = {
			'website_id': self.get_website_id_by_store_id(store_id),
			'email': convert['email'],
			'group_id': self.get_map_customer_group(convert['group_id']),
			'increment_id': None,
			'store_id': store_id,
			'created_at': to_str(convert.get('created_at', get_current_time()) if convert['created_at'] else get_current_time())[0:19],
			'updated_at': to_str(convert.get('updated_at', get_current_time()) if convert['updated_at'] else get_current_time())[0:19],
			'is_active': convert.get('is_active', 1),
			'disable_auto_group_change': convert.get('disable_auto_group_change', 0),
			'created_in': self._notice['target']['languages'].get(to_str(store_id), 'DEFAULT'),
			'prefix': convert.get('prefix'),
			'firstname': convert.get('first_name'),
			'middlename': convert.get('middle_name'),
			'lastname': convert.get('last_name'),
			'suffix': convert.get('suffix', ''),
			'dob': to_str(convert['dob'])[0:10] if convert_format_time(to_str(convert['dob'])[0:19]) else None,
			'password_hash': convert.get('password'),
			'rp_token': '',
			'rp_token_created_at': get_current_time(),
			'default_billing': None,
			'default_shipping': None,
			'taxvat': convert.get('taxvat', ''),
			'confirmation': None,
			'gender': gender_id,
		}

		if self._notice['config']['pre_cus']:
			# self.delete_target_customer(convert['id'])
			customer_entity_data['entity_id'] = convert['id']
		customer_id = self.import_customer_data_connector(
			self.create_insert_query_connector('customer_entity', customer_entity_data), True, convert['id'])
		if not customer_id:
			return response_error(self.warning_import_entity(self.TYPE_CUSTOMER, convert['id']))
		self.insert_map(self.TYPE_CUSTOMER, convert['id'], customer_id, convert['email'])
		return response_success(customer_id)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
		all_queries = list()
		billing_default = None
		shipping_default = None
		billing_full = None
		shipping_full = None
		increment_id = None
		customer_update_data = dict()
		new_customer_increment_id = '' + to_str(customer_id)
		while to_len(new_customer_increment_id) < 8:
			new_customer_increment_id = '0' + new_customer_increment_id
		new_customer_increment_id = '1' + new_customer_increment_id
		old_customer_increment_id = convert.get('increment_id', new_customer_increment_id)
		if self._notice['config']['pre_cus']:
			increment_id = old_customer_increment_id
		else:
			increment_id = new_customer_increment_id
		customer_update_data['increment_id'] = increment_id
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: customer address
		# customer address begin
		# for
		for customer_address in convert['address']:
			street = ''
			if customer_address['state']['name']:
				region_id = self.get_region_id_by_state_name(customer_address['state']['name'])
			else:
				if customer_address['state']['state_code']:
					region_id = self.get_region_id_from_state_code(customer_address['state']['state_code'], customer_address['country']['country_code'])
				else:
					region_id = 0

			street = get_value_by_key_in_dict(customer_address, 'address_1', '')
			if not street:
				street = get_value_by_key_in_dict(customer_address, 'address_2', '')
			else:
				if get_value_by_key_in_dict(customer_address, 'address_2', ''):
					street += '\n' + customer_address['address_2']
			customer_address_entity_data = {
				'increment_id': increment_id,
				'parent_id': customer_id,
				'created_at': get_value_by_key_in_dict(customer_address, 'created_at', get_current_time()),
				'updated_at': get_value_by_key_in_dict(customer_address, 'updated_at',
				                                       get_current_time()) if get_value_by_key_in_dict(customer_address,
				                                                                                       'updated_at',
				                                                                                       get_current_time()) else get_current_time(),
				'is_active': get_value_by_key_in_dict(customer_address, 'is_active', 1),
				'city': get_value_by_key_in_dict(customer_address, 'city', ''),
				'company': get_value_by_key_in_dict(customer_address, 'company'),
				'country_id': get_value_by_key_in_dict(get_value_by_key_in_dict(customer_address, 'country', dict()),
				                                       'country_code', ''),
				'fax': get_value_by_key_in_dict(customer_address, 'fax'),
				'firstname': get_value_by_key_in_dict(customer_address, 'first_name', ''),
				'lastname': get_value_by_key_in_dict(customer_address, 'last_name', ''),
				'middlename': get_value_by_key_in_dict(customer_address, 'middle_name'),
				'postcode': get_value_by_key_in_dict(customer_address, 'postcode'),
				'prefix': get_value_by_key_in_dict(customer_address, 'prefix'),
				'region': customer_address['state']['name'] if customer_address['state']['name'] else '',
				'region_id': region_id,
				'street': street,
				'suffix': get_value_by_key_in_dict(convert, 'suffix'),
				'telephone': get_value_by_key_in_dict(customer_address, 'telephone', ''),
				'vat_id': get_value_by_key_in_dict(customer_address, 'vat_id'),
				'vat_is_valid': None,
				'vat_request_date': None,
				'vat_request_id': None,
				'vat_request_success': None,
			}
			if (not customer_address['default']['billing']) and (not customer_address['default']['shipping']):
				all_queries.append(
					self.create_insert_query_connector('customer_address_entity', customer_address_entity_data))
			else:
				customer_address_id = self.import_customer_data_connector(
					self.create_insert_query_connector('customer_address_entity', customer_address_entity_data))
				if not customer_address_id:
					continue
				address_street = to_str(customer_address['address_1']) + " " + to_str(customer_address['city']) + " " + \
				                 to_str(customer_address['state']['name']) + " " + to_str(customer_address['postcode'])
				if customer_address['default']['billing']:
					billing_default = customer_address_id
					billing_full = address_street
				if customer_address['default']['shipping']:
					shipping_default = customer_address_id
					shipping_full = address_street

		# endfor
		# customer address end
		# ------------------------------------------------------------------------------------------------------------------------

		if billing_default:
			customer_update_data['default_billing'] = billing_default
		if shipping_default:
			customer_update_data['default_shipping'] = shipping_default

		# ------------------------------------------------------------------------------------------------------------------------
		# todo: customer grid
		# begin
		store_id = 0  # self.get_map_store_view(customer['store_id'])
		address_default = convert['address'][0] if to_len(convert['address']) > 1 else dict()
		gender_id = 3
		if convert.get('gender'):
			if convert.get('gender') == 'Male' or to_str(convert.get('gender')) == '1':
				gender_id = 1
			else:
				gender_id = 2
		customer_grid_flat_data = {
			'entity_id': customer_id,
			'name': get_value_by_key_in_dict(convert, 'first_name', '') + ' ' + get_value_by_key_in_dict(convert, 'middle_name', '') + ' ' + get_value_by_key_in_dict(convert, 'last_name', ''),
			'email': convert['email'] if convert['email'] else '',
			'group_id': self.get_map_customer_group(convert['group_id']),
			'created_at': convert['created_at'] if convert['created_at'] else get_current_time(),
			'website_id': self.get_website_id_by_store_id(store_id),
			'confirmation': '',
			'created_in': self._notice['target']['languages'].get(str(store_id), 'DEFAULT'),
			'dob': convert['dob'] if convert_format_time(convert['dob']) else None,
			'gender': gender_id,
			'taxvat': get_value_by_key_in_dict(convert, 'taxvat'),
			'lock_expires': get_current_time(),
			'billing_full': billing_full,
			'billing_firstname': convert['first_name'] if convert['first_name'] else '',
			'billing_lastname': convert['last_name'] if convert['last_name'] else '',
			'billing_telephone': address_default.get('telephone'),
			'billing_postcode': address_default.get('postcode'),
			'billing_country_id': address_default.get('country', dict()).get('country_code'),
			'billing_region': address_default.get('state', dict()).get('name'),
			'billing_street': address_default.get('address_1'),
			'billing_city': address_default.get('city'),
			'billing_fax': address_default.get('fax'),
			'billing_vat_id': '',
			'billing_company': address_default.get('company'),
			'shipping_full': shipping_full,
		}
		all_queries.append(self.create_insert_query_connector('customer_grid_flat', customer_grid_flat_data))
		# customer grid end
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: customer subscribed
		# begin
		# if
		try:
			store_id = self._notice['map']['languages'][to_str(self._notice['src']['language_default'])]
		except Exception:
			store_id = 1
		if 'is_subscribed' in convert and convert['is_subscribed']:
			newsletter_subscriber_data = {
				'customer_id': customer_id,
				'subscriber_email': convert.get('email'),
				'subscriber_status': 1,
				'store_id': store_id
			}
			all_queries.append(
				self.create_insert_query_connector('newsletter_subscriber', newsletter_subscriber_data))

		# endif
		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: customer update
		if customer_update_data:
			all_queries.append(
				self.create_update_query_connector('customer_entity', customer_update_data, {'entity_id': customer_id}))

		# ------------------------------------------------------------------------------------------------------------------------

		if all_queries:
			self.import_multiple_data_connector(all_queries, 'customer')
		return response_success()

	def addition_customer_import(self, convert, customer, customers_ext):
		return response_success()

	# TODO: ORDER
	def prepare_orders_import(self):
		return self

	def prepare_orders_export(self):
		return self

	def get_orders_main_export(self):
		id_src = self._notice['process']['orders']['id_src']
		limit = self._notice['setting']['orders']
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = to_str(store_id_con) + ' AND '
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_sales_order WHERE " + self.get_con_store_select_count() + " AND entity_id > " + to_str(
				id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit)
		}
		orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})

		if not orders or orders['result'] != 'success':
			return response_error()
		return orders

	def get_orders_ext_export(self, orders):
		order_ids = duplicate_field_value_from_list(orders['data'], 'entity_id')
		order_id_con = self.list_to_in_condition(order_ids)
		url_query = self.get_connector_url('query')
		order_ext_quries = {
			'sales_flat_order_item': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_item WHERE product_id not in (SELECT product_id FROM `_DBPRF_catalog_product_super_link`) AND order_id IN " + order_id_con
			},
			'sales_flat_order_address': {
				'type': 'select',
				'query': "SELECT sfoa.*,sdcr.code,sdcr.default_name FROM _DBPRF_sales_order_address as sfoa LEFT "
				         "JOIN _DBPRF_directory_country_region as sdcr ON sfoa.region_id = sdcr.region_id WHERE "
				         "sfoa.parent_id IN " + order_id_con,
			},
			'sales_flat_order_status_history': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_status_history WHERE parent_id IN " + order_id_con
			},
			'sales_flat_order_payment': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_payment WHERE parent_id IN " + order_id_con
			},
			'sales_flat_invoice': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_invoice WHERE order_id IN " + order_id_con
			},
			'sales_flat_shipment': {
				'type': 'select',
				'query': "SELECT entity_id, store_id, base_amount, amount, total_weight, total_qty, email_sent, send_email, order_id, customer_id, shipping_address_id, billing_address_id, shipment_status, increment_id, created_at, updated_at, packages, customer_note, customer_note_notify, shipment_cost, is_first_shipment, warehouse_id FROM _DBPRF_sales_shipment WHERE order_id IN " + order_id_con
			},
			'sales_flat_creditmemo': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_creditmemo WHERE order_id IN " + order_id_con
			},
		}
		order_ext = self.select_multiple_data_connector(order_ext_quries, 'orders')

		if not order_ext or order_ext['result'] != 'success':
			return response_error()
		order_ext_rel_queries = dict()
		invoice_ids = duplicate_field_value_from_list(order_ext['data']['sales_flat_invoice'], 'entity_id')
		if invoice_ids:
			invoice_item_condition = self.list_to_in_condition(invoice_ids)
			order_ext_rel_queries['sales_flat_invoice_item'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_invoice_item WHERE parent_id IN " + invoice_item_condition,
			}
			order_ext_rel_queries['sales_flat_invoice_comment'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_invoice_comment WHERE parent_id IN " +
				         invoice_item_condition,
			}
		shipment_ids = duplicate_field_value_from_list(order_ext['data']['sales_flat_shipment'], 'entity_id')
		if shipment_ids:
			shipment_item_condition = self.list_to_in_condition(shipment_ids)
			order_ext_rel_queries['sales_flat_shipment_item'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_shipment_item WHERE parent_id IN " + shipment_item_condition,
			}
			order_ext_rel_queries['sales_flat_shipment_comment'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_shipment_comment WHERE parent_id IN " +
				         shipment_item_condition,
			}
		creditmemo_ids = duplicate_field_value_from_list(order_ext['data']['sales_flat_creditmemo'], 'entity_id')
		if creditmemo_ids:
			creditmemo_item_condition = self.list_to_in_condition(creditmemo_ids)
			order_ext_rel_queries['sales_flat_creditmemo_item'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_creditmemo_item WHERE parent_id IN " +
				         creditmemo_item_condition,
			}
			order_ext_rel_queries['sales_flat_creditmemo_comment'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_creditmemo_comment WHERE parent_id IN " +
				         creditmemo_item_condition,
			}

		if order_ext_rel_queries:
			order_ext_rel = self.select_multiple_data_connector(order_ext_rel_queries, 'orders')

			if not order_ext_rel or order_ext_rel['result'] != 'success':
				return response_error()
			order_ext = self.sync_connector_object(order_ext, order_ext_rel)
		return order_ext

	def convert_order_export(self, order, orders_ext):
		order_data = self.construct_order()
		order_data = self.add_construct_default(order_data)
		order_data['id'] = order['entity_id']
		order_data['status'] = order['status']
		order_data['tax']['title'] = "Taxes"
		order_data['increment_id'] = order['increment_id']
		order_data['tax']['amount'] = order['base_tax_amount']
		order_data['shipping']['title'] = "Shipping"
		order_data['shipping']['amount'] = order['shipping_amount']
		order_data['subtotal']['title'] = 'Total products'
		order_data['subtotal']['amount'] = order['subtotal']
		order_data['total']['title'] = 'Total'
		order_data['total']['amount'] = order['grand_total']
		order_data['currency'] = order['store_currency_code']
		order_data['created_at'] = order['created_at']
		order_data['updated_at'] = order['updated_at']
		order_data['base_subtotal_incl_tax'] = order['base_subtotal_incl_tax']
		order_data['subtotal_incl_tax'] = order['subtotal_incl_tax']

		order_data['order_number'] = order['increment_id']
		order_data['discount']['amount'] = order['discount_amount']
		order_data['discount']['code'] = get_value_by_key_in_dict(order, 'coupon_code')
		order_data['order_number'] = order['increment_id']

		order_customer = self.construct_order_customer()
		order_customer = self.add_construct_default(order_customer)
		order_customer['id'] = order['customer_id']
		order_customer['email'] = get_value_by_key_in_dict(order, 'customer_email', '').strip()
		order_customer['first_name'] = order['customer_firstname']
		order_customer['last_name'] = order['customer_lastname']
		order_customer['middle_name'] = order['customer_middlename']
		order_customer['group_id'] = order['customer_group_id']

		order_data['customer'] = order_customer

		customer_address = self.construct_order_address()
		customer_address = self.add_construct_default(customer_address)

		order_address = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_address'], 'parent_id',
		                                            order['entity_id'])
		billing_address = get_row_from_list_by_field(order_address, 'address_type', 'billing')
		order_billing = self.construct_order_address()
		order_billing = self.add_construct_default(order_billing)
		# if
		if billing_address:
			order_billing['first_name'] = billing_address['firstname']
			order_billing['last_name'] = billing_address['lastname']
			street = get_value_by_key_in_dict(billing_address, 'street', '').splitlines()
			order_billing['address_1'] = street[0] if to_len(street) > 0 else ''
			order_billing['address_2'] = street[1] if to_len(street) > 1 else ''
			order_billing['city'] = billing_address['city'] if billing_address['city'] else ''
			order_billing['postcode'] = billing_address['postcode'] if billing_address['postcode'] else ''
			order_billing['telephone'] = billing_address['telephone'] if billing_address['telephone'] else ''
			order_billing['company'] = billing_address['company'] if billing_address['company'] else ''
			if billing_address['country_id']:
				order_billing['country']['country_code'] = billing_address['country_id']
				order_billing['country']['name'] = self.get_country_name_by_code(billing_address['country_id'])
			else:
				order_billing['country']['country_code'] = 'US'
				order_billing['country']['name'] = 'United States'

			if billing_address['region_id']:
				order_billing['state']['name'] = billing_address['default_name'] if billing_address[
					'default_name'] else ''
				order_billing['state']['state_code'] = billing_address['code'] if billing_address['code'] else ''
			else:
				order_billing['state']['name'] = billing_address['region'] if billing_address['region'] else ''
				order_billing['state']['state_code'] = ''
		# endif

		order_data['billing_address'] = order_billing
		delivery_address = get_row_from_list_by_field(order_address, 'address_type', 'shipping')
		order_delivery = self.construct_order_address()
		order_delivery = self.add_construct_default(order_delivery)

		# if
		if delivery_address:
			order_delivery['first_name'] = delivery_address['firstname']
			order_delivery['last_name'] = delivery_address['lastname']
			if delivery_address['street'] :
				street_deli = delivery_address['street'].splitlines()
				order_delivery['address_1'] = street_deli[0]
				order_delivery['address_2'] = street_deli[1] if to_len(street_deli) > 1 else ''
			order_delivery['city'] = delivery_address['city'] if delivery_address['city'] else ''
			order_delivery['postcode'] = delivery_address['postcode'] if delivery_address['postcode'] else ''
			order_delivery['telephone'] = delivery_address['telephone'] if delivery_address['telephone'] else ''
			order_delivery['company'] = delivery_address['company'] if delivery_address['company'] else ''
			if delivery_address['country_id']:
				order_delivery['country']['country_code'] = delivery_address['country_id']
				order_delivery['country']['name'] = self.get_country_name_by_code(delivery_address['country_id'])
			else:
				order_delivery['country']['country_code'] = 'US'
				order_delivery['country']['name'] = 'United States'

			if delivery_address['region_id']:
				order_delivery['state']['name'] = delivery_address['default_name'] if delivery_address[
					'default_name'] else ''
				order_delivery['state']['state_code'] = delivery_address['code'] if delivery_address['code'] else ''
			else:
				order_delivery['state']['name'] = delivery_address['region'] if delivery_address['region'] else ''
				order_delivery['state']['state_code'] = ''
		# endif

		order_data['shipping_address'] = order_delivery
		order_payment = get_row_from_list_by_field(orders_ext['data']['sales_flat_order_payment'], 'parent_id',
		                                           order['entity_id'])
		payment_data = self.construct_order_payment()
		payment_data['title'] = order_payment.get('method', '')
		payment_data['method'] = order_payment.get('method', '')
		order_data['payment'] = payment_data
		order_data['payment_method'] = order_payment.get('method')

		# todo: get product in order
		order_products = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_item'], 'order_id',
		                                             order['entity_id'])

		order_items = list()
		order_child_items = list()

		# for
		for order_product in order_products:
			order_item = self.construct_order_item()
			order_item = self.add_construct_default(order_item)
			order_item['id'] = get_value_by_key_in_dict(order_product, 'item_id')
			if get_value_by_key_in_dict(order_product, 'parent_item_id'):
				map_parent = dict()
				map_parent['parent_id'] = get_value_by_key_in_dict(order_product, 'parent_item_id')
				map_parent['children_id'] = get_value_by_key_in_dict(order_product, 'item_id')
				order_child_items.append(map_parent)
			order_item['created_at'] = get_value_by_key_in_dict(order_product, 'created_at', get_current_time())
			order_item['updated_at'] = get_value_by_key_in_dict(order_product, 'updated_at', get_current_time())
			order_item['weight'] = get_value_by_key_in_dict(order_product, 'weight', '0.0000')
			order_item['is_qty_decimal'] = get_value_by_key_in_dict(order_product, 'is_qty_decimal', False)
			order_item['no_discount'] = get_value_by_key_in_dict(order_product, 'no_discount', True)
			order_item['product_type'] = get_value_by_key_in_dict(order_product, 'product_type', 'simple')
			order_item['quote_item_id'] = get_value_by_key_in_dict(order_product, 'quote_item_id')

			order_item['parent_item_id'] = get_value_by_key_in_dict(order_product, 'parent_item_id')
			order_item['qty_canceled'] = get_value_by_key_in_dict(order_product, 'qty_canceled', 0)
			order_item['qty_ordered'] = get_value_by_key_in_dict(order_product, 'qty_ordered', 0)
			order_item['qty_backordered'] = get_value_by_key_in_dict(order_product, 'qty_backordered', 0)
			order_item['qty_invoiced'] = get_value_by_key_in_dict(order_product, 'qty_invoiced', 0)
			order_item['qty_refunded'] = get_value_by_key_in_dict(order_product, 'qty_refunded', 0)
			order_item['qty_shipped'] = get_value_by_key_in_dict(order_product, 'qty_shipped', 0)
			order_item['base_cost'] = get_value_by_key_in_dict(order_product, 'base_cost', 0.0000)
			order_item['base_discount_amount'] = get_value_by_key_in_dict(order_product, 'base_discount_amount', 0.0000)
			order_item['base_tax_before_discount'] = get_value_by_key_in_dict(order_product, 'base_tax_before_discount',
			                                                                  0.0000)
			order_item['tax_before_discount'] = get_value_by_key_in_dict(order_product, 'tax_before_discount', 0.0000)
			order_item['locked_do_invoice'] = get_value_by_key_in_dict(order_product, 'locked_do_invoice', 0.0000)
			order_item['locked_do_ship'] = get_value_by_key_in_dict(order_product, 'locked_do_ship', False)
			order_item['base_price_incl_tax'] = get_value_by_key_in_dict(order_product, 'base_price_incl_tax', 0.0000)
			order_item['base_row_total_incl_tax'] = get_value_by_key_in_dict(order_product, 'base_row_total_incl_tax',
			                                                                 0.0000)
			# order_item['product']['code'] = get_value_by_key_in_dict(order_product, 'sku', '')
			order_item['product']['id'] = get_value_by_key_in_dict(order_product, 'product_id')
			order_item['product']['name'] = get_value_by_key_in_dict(order_product, 'name', '')
			order_item['product']['sku'] = get_value_by_key_in_dict(order_product, 'sku', '')
			order_item['qty'] = to_int(to_decimal(get_value_by_key_in_dict(order_product, 'qty_ordered', 0)))
			order_item['price'] = get_value_by_key_in_dict(order_product, 'price', 0.0000)
			order_item['original_price'] = get_value_by_key_in_dict(order_product, 'original_price', 0.0000)
			order_item['tax_amount'] = get_value_by_key_in_dict(order_product, 'tax_amount', 0.0000)
			order_item['tax_percent'] = get_value_by_key_in_dict(order_product, 'tax_percent', 0.0000)
			order_item['tax_invoiced'] = get_value_by_key_in_dict(order_product, 'tax_invoiced', 0.0000)
			order_item['discount_amount'] = get_value_by_key_in_dict(order_product, 'discount_amount', 0.0000)
			order_item['discount_percent'] = get_value_by_key_in_dict(order_product, 'discount_percent', 0.0000)
			order_item['discount_invoiced'] = get_value_by_key_in_dict(order_product, 'discount_invoiced', 0.0000)
			order_item['total'] = get_value_by_key_in_dict(order_product, 'row_total', 0.0000)
			order_item['row_total'] = get_value_by_key_in_dict(order_product, 'row_total', 0.0000)
			order_item['row_invoiced'] = get_value_by_key_in_dict(order_product, 'row_invoiced', 0.0000)
			order_item['row_weight'] = get_value_by_key_in_dict(order_product, 'row_weight', 0.0000)
			order_item['price_incl_tax'] = get_value_by_key_in_dict(order_product, 'price_incl_tax', 0.0000)
			order_item['row_total_incl_tax'] = get_value_by_key_in_dict(order_product, 'row_total_incl_tax', 0.0000)
			order_item['free_shipping'] = get_value_by_key_in_dict(order_product, 'free_shipping')
			order_item['tax_canceled'] = get_value_by_key_in_dict(order_product, 'tax_canceled', 0.0000)
			order_item['tax_refunded'] = get_value_by_key_in_dict(order_product, 'tax_refunded', 0.0000)
			order_item['discount_refunded'] = get_value_by_key_in_dict(order_product, 'discount_refunded', 0.0000)

			# if
			if get_value_by_key_in_dict(order_product, 'product_options'):
				cart_version = self._notice['src']['config']['version']
				magento_version = self.convert_version(cart_version, 2)
				option_products_str = get_value_by_key_in_dict(order_product, 'product_options', '')
				while re.search('";$', option_products_str):
					option_products_str = re.sub('";$', '', to_str(option_products_str), 1)
				option_products_str = re.sub('^s.*:"a:', 'a:', to_str(option_products_str), 1)

				if magento_version < 220:
					options = php_unserialize(option_products_str)
				else:
					options = json_decode(option_products_str)
				item_options = list()

				# if
				if not options or not isinstance(options, dict):
					continue
				if options and 'options' in options:
					order_item_options = list()
					for option in options['options']:
						if not isinstance(option, dict):
							continue
						if option and 'label' in option and 'value' in option:
							order_item_option = self.construct_order_item_option()
							order_item_option['option_name'] = option['label']
							order_item_option['option_value_name'] = option['value']
							item_options.append(order_item_option)
				# item_options.append(order_item_options)
				# endif

				# if
				if options and 'attributes_info' in options:
					order_item_options = list()
					options_info = options['attributes_info']
					if isinstance(options['attributes_info'], dict):
						options_info = options['attributes_info'].values()
					for attr in options_info:
						order_item_option = self.construct_order_item_option()
						order_item_option['option_name'] = attr['label']
						order_item_option['option_value_name'] = attr['value']
						item_options.append(order_item_option)
				# item_options['attributes_info'] = order_item_options
				# endif

				order_item['options'] = item_options
			order_items.append(order_item)
		# endif
		# endfor
		order_data['items'] = order_items
		if order_child_items:
			order_data['order_child_item'] = order_child_items

		# todo: Get order history
		order_status_history = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_status_history'],
		                                                   'parent_id', order['entity_id'])
		order_histories = list()
		for status_history in order_status_history:
			order_history = self.construct_order_history()
			order_history = self.add_construct_default(order_history)
			order_history['id'] = status_history['entity_id']
			order_history['status'] = status_history['status']
			order_history['comment'] = status_history['comment']
			order_history['created_at'] = status_history['created_at']
			order_history['notified'] = to_int(status_history['is_customer_notified']) == 1
			order_histories.append(order_history)
		order_data['history'] = order_histories

		shipment = get_row_from_list_by_field(orders_ext['data']['sales_flat_shipment'], 'order_id',
		                                      order['entity_id'])
		if shipment:
			order_data['shipment'] = dict()
			order_data['shipment']['data'] = shipment
			shipment_item = get_list_from_list_by_field(orders_ext['data']['sales_flat_shipment_item'], 'parent_id',
			                                            shipment['entity_id'])
			order_data['shipment']['item'] = shipment_item
			shipment_comment = get_list_from_list_by_field(orders_ext['data']['sales_flat_shipment_comment'],
			                                               'parent_id', shipment['entity_id'])
			order_data['shipment']['comment'] = shipment_comment

		invoice = get_row_from_list_by_field(orders_ext['data']['sales_flat_invoice'], 'order_id', order['entity_id'])
		if invoice:
			order_data['invoice'] = dict()
			order_data['invoice']['data'] = invoice
			invoice_item = get_list_from_list_by_field(orders_ext['data']['sales_flat_invoice_item'], 'parent_id',
			                                           invoice['entity_id'])
			order_data['invoice']['item'] = invoice_item
			invoice_comment = get_list_from_list_by_field(orders_ext['data']['sales_flat_invoice_comment'],
			                                              'parent_id',
			                                              invoice['entity_id'])
			order_data['invoice']['comment'] = invoice_comment
		creditmemo = get_row_from_list_by_field(orders_ext['data']['sales_flat_creditmemo'], 'order_id',
		                                        order['entity_id'])
		if creditmemo:
			order_data['creditmemo'] = dict()
			order_data['creditmemo']['data'] = creditmemo
			creditmemo_item = get_list_from_list_by_field(orders_ext['data']['sales_flat_creditmemo_item'],
			                                              'parent_id',
			                                              creditmemo['entity_id'])
			order_data['creditmemo']['item'] = creditmemo_item
			creditmemo_comment = get_list_from_list_by_field(orders_ext['data']['sales_flat_creditmemo_comment'],
			                                                 'parent_id', creditmemo['entity_id'])
			order_data['creditmemo']['comment'] = creditmemo_comment
		return response_success(order_data)

	def get_order_id_import(self, convert, order, orders_ext):
		return order['entity_id']

	def check_order_import(self, convert, order, orders_ext):
		return self.get_map_field_by_src(self.TYPE_ORDER, convert['id'])

	def update_order_after_demo(self, order_id, convert, order, orders_ext):
		order = get_value_by_key_in_dict(convert, 'order', order)
		all_queries = list()
		new_order_increment_id = '' + to_str(order_id)
		while to_len(new_order_increment_id) < 8:
			new_order_increment_id = '0' + new_order_increment_id
		new_order_increment_id = '1' + new_order_increment_id
		old_order_increment_id = convert.get('increment_id', new_order_increment_id)
		if self._notice['config']['pre_ord']:
			order_increment_id = old_order_increment_id
		else:
			order_increment_id = new_order_increment_id
		customer_id = None
		if convert['customer']['id']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'])
		if not customer_id:
			customer_id = None
		if customer_id:
			all_queries.append(self.create_update_query_connector('sales_order', {'customer_id': customer_id}, {'entity_id': order_id}))
			all_queries.append(self.create_update_query_connector('sales_order_grid', {'customer_id': customer_id}, {'entity_id': order_id}))
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: order item
		# begin
		items_order = convert['items']
		item_queries = list()
		self.import_data_connector(self.create_delete_query_connector('sales_order_item',  {'order_id': order_id}))

		item_ids = list()
		map_item = dict()
		# for
		store_id = self.get_map_store_view(order['store_id'])
		for item_order in items_order:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item_order['product']['id'])
			if not product_id:
				product_id = None
			product_options = ''
			options = item_order.get('options')

			# if
			if options:
				product_options = dict()
				if 'options' in options:
					product_option = list()
					for option in options['options']:
						item_option = {
							'label': option['option_name'],
							'value': option['option_value_name']
						}
						product_option.append(item_option)
					product_options['options'] = product_option
				if 'attributes_info' in options:
					product_option = list()
					for option in options['attributes_info']:
						item_option = {
							'label': option['option_name'],
							'value': option['option_value_name']
						}
						product_option.append(item_option)
					product_options['attributes_info'] = product_option
				product_options = self.magento_serialize(product_options)
			# endif
			sales_order_item_data = {
				'order_id': order_id,
				'parent_item_id': None,
				'quote_item_id': None,
				'store_id': store_id,
				'created_at': get_value_by_key_in_dict(item_order, 'created_at', get_current_time()),
				'updated_at': get_value_by_key_in_dict(item_order, 'updated_at', get_current_time()),
				'product_id': product_id,
				'product_type': get_value_by_key_in_dict(item_order, 'product_type', 'simple'),
				'product_options': product_options,
				'weight': get_value_by_key_in_dict(item_order, 'weight'),
				'is_virtual': get_value_by_key_in_dict(item_order, 'is_virtual'),
				'sku': get_value_by_key_in_dict(get_value_by_key_in_dict(item_order, 'product', dict()), 'sku'),
				'name': get_value_by_key_in_dict(get_value_by_key_in_dict(item_order, 'product', dict()), 'name'),
				'description': get_value_by_key_in_dict(item_order, 'description', ''),
				'applied_rule_ids': '',
				'additional_data': '',
				'is_qty_decimal': get_value_by_key_in_dict(item_order, 'is_qty_decimal'),
				'no_discount': get_value_by_key_in_dict(item_order, 'no_discount', 0),
				'qty_backordered': get_value_by_key_in_dict(item_order, 'qty_backordered'),
				'qty_canceled': get_value_by_key_in_dict(item_order, 'qty_canceled'),
				'qty_invoiced': get_value_by_key_in_dict(item_order, 'qty_invoiced'),
				'qty_ordered': get_value_by_key_in_dict(item_order, 'qty_ordered'),
				'qty_refunded': get_value_by_key_in_dict(item_order, 'qty_refunded'),
				'qty_shipped': get_value_by_key_in_dict(item_order, 'qty_shipped'),
				'base_cost': get_value_by_key_in_dict(item_order, 'base_cost'),
				'price': get_value_by_key_in_dict(item_order, 'price', '0.0000'),
				'base_price': get_value_by_key_in_dict(item_order, 'price', '0.0000'),
				'original_price': get_value_by_key_in_dict(item_order, 'original_price', '0.0000'),
				'base_original_price': get_value_by_key_in_dict(item_order, 'original_price', '0.0000'),
				'tax_percent': get_value_by_key_in_dict(item_order, 'tax_percent', 0),
				'tax_amount': get_value_by_key_in_dict(item_order, 'tax_amount', 0),
				'base_tax_amount': get_value_by_key_in_dict(item_order, 'tax_amount', 0),
				'tax_invoiced': get_value_by_key_in_dict(item_order, 'tax_invoiced', 0),
				'base_tax_invoiced': get_value_by_key_in_dict(item_order, 'tax_invoiced', 0),
				'discount_percent': get_value_by_key_in_dict(item_order, 'discount_percent', 0),
				'discount_amount': get_value_by_key_in_dict(item_order, 'discount_amount', 0),
				'base_discount_amount': get_value_by_key_in_dict(item_order, 'base_discount_amount', 0),
				'discount_invoiced': get_value_by_key_in_dict(item_order, 'discount_invoiced', 0),
				'base_discount_invoiced': get_value_by_key_in_dict(item_order, 'discount_invoiced', 0),
				'amount_refunded': '0.0000',
				'base_amount_refunded': '0.0000',
				'row_total': get_value_by_key_in_dict(item_order, 'row_total', 0),
				'base_row_total': get_value_by_key_in_dict(item_order, 'base_row_total', 0),
				'row_invoiced': get_value_by_key_in_dict(item_order, 'row_invoiced', 0),
				'base_row_invoiced': get_value_by_key_in_dict(item_order, 'row_invoiced', 0),
				'row_weight': get_value_by_key_in_dict(item_order, 'row_weight', 0),
				'base_tax_before_discount': get_value_by_key_in_dict(item_order, 'base_tax_before_discount', 0),
				'tax_before_discount': get_value_by_key_in_dict(item_order, 'tax_before_discount', 0),
				'ext_order_item_id': '',
				'locked_do_invoice': get_value_by_key_in_dict(item_order, 'locked_do_invoice'),
				'locked_do_ship': get_value_by_key_in_dict(item_order, 'locked_do_ship'),
				'price_incl_tax': get_value_by_key_in_dict(item_order, 'price_incl_tax', 0),
				'base_price_incl_tax': get_value_by_key_in_dict(item_order, 'price_incl_tax', 0),
				'row_total_incl_tax': get_value_by_key_in_dict(item_order, 'row_total_incl_tax', 0),
				'base_row_total_incl_tax': get_value_by_key_in_dict(item_order, 'row_total_incl_tax', 0),
				'discount_tax_compensation_amount': '0.0000',
				'base_discount_tax_compensation_amount': '0.0000',
				'discount_tax_compensation_invoiced': '0.0000',
				'base_discount_tax_compensation_invoiced': '0.0000',
				'discount_tax_compensation_refunded': '0.0000',
				'base_discount_tax_compensation_refunded': '0.0000',
				'tax_canceled': get_value_by_key_in_dict(item_order, 'tax_canceled', 0),
				'discount_tax_compensation_canceled': '0.0000',
				'tax_refunded': get_value_by_key_in_dict(item_order, 'tax_refunded', 0),
				'base_tax_refunded': get_value_by_key_in_dict(item_order, 'tax_refunded', 0),
				'discount_refunded': get_value_by_key_in_dict(item_order, 'discount_refunded', 0),
				'base_discount_refunded': get_value_by_key_in_dict(item_order, 'discount_refunded', 0),
				'free_shipping': get_value_by_key_in_dict(item_order, 'free_shipping', 0),
				'gift_message_id': None,
				'gift_message_available': None,
				'weee_tax_applied': None,
				'weee_tax_applied_amount': None,
				'weee_tax_applied_row_amount': None,
				'weee_tax_disposition': None,
				'weee_tax_row_disposition': None,
				'base_weee_tax_applied_amount': None,
				'base_weee_tax_applied_row_amnt': None,
				'base_weee_tax_disposition': None,
				'base_weee_tax_row_disposition': None,
			}
			item_queries.append(self.create_insert_query_connector('sales_order_item', sales_order_item_data, True))
			item_ids.append(item_order['id'])
		# endfor
		items_import = self.import_multiple_data_connector(item_queries, 'order_item', True, True)
		if items_import and items_import.get('data'):
			for index, order_item_id in enumerate(items_import['data']):
				if order_item_id:
					map_item[item_ids[index]] = order_item_id

		if 'order_child_item' in convert:
			for order_child_item in convert['order_child_item']:
				parent_id = map_item.get(str(order_child_item['parent_id']))
				child_id = map_item.get(str(order_child_item['children_id']))
				if (not parent_id) or (not child_id):
					continue
				all_queries.append(self.create_update_query_connector('sales_order_item', {'parent_item_id': parent_id}, {'item_id': child_id}))
		# end
		if 'link_purchased_data' in convert:
			del_queries = list()
			delete_query = {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_downloadable_link_purchased_item WHERE purchased_id IN (SELECT purchased_id FROM _DBPFF_downloadable_link_purchased WHERE order_id = ' + to_str(order_id) + ')'
			}
			del_queries.append(delete_query)
			del_queries.append(self.create_delete_query_connector('downloadable_link_purchased', {'order_id': order_id}))
			self.import_multiple_data_connector(del_queries)
			for link_purchased in convert['link_purchased_data']:
				data = {
					'order_id': order_id,
					'order_increment_id': order_increment_id,
					'order_item_id': map_item.get(str(link_purchased['order_item_id'])),
					'created_at': link_purchased['created_at'],
					'updated_at': link_purchased['updated_at'],
					'customer_id': customer_id,
					'product_name': link_purchased['product_name'],
					'product_sku': link_purchased['product_sku'],
					'link_section_title': link_purchased['link_section_title']
				}
				purchased_id = self.import_order_data_connector(self.create_insert_query_connector('downloadable_link_purchased', data))
				if link_purchased['purchased_item']:
					for purchased_item in link_purchased['purchased_item']:
						product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, purchased_item['product_id'])
						if not product_id:
							product_id = None
						item_data = {
							'purchased_id': purchased_id,
							'order_item_id': map_item.get(str(link_purchased['order_item_id'])),
							'product_id': product_id,
							'link_hash': purchased_item['link_hash'],
							'number_of_downloads_bought': purchased_item['number_of_downloads_bought'],
							'number_of_downloads_used': purchased_item['number_of_downloads_used'],
							'link_id': purchased_item['link_id'],
							'link_title': purchased_item['link_title'],
							'is_shareable': purchased_item['is_shareable'],
							'link_url': purchased_item['link_url'],
							'link_file': purchased_item['link_file'],
							'link_type': purchased_item['link_type'],
							'status': purchased_item['status'],
							'created_at': purchased_item['created_at'],
							'updated_at': purchased_item['updated_at']
						}
						all_queries.append(self.create_insert_query_connector('downloadable_link_purchased_item', item_data))
		self.import_multiple_data_connector(all_queries, 'update_order')
		return response_success()

	def router_order_import(self, convert, order, orders_ext):
		return response_success('order_import')

	def before_order_import(self, convert, order, orders_ext):
		return response_success()

	def order_import(self, convert, order, orders_ext):
		customer_id = None
		if 'id' in convert['customer'] and convert['customer']['id']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'])
		if not customer_id:
			customer_id = None
		customer_group = None
		if 'group_id' in convert['customer']:
			customer_group = self.get_map_customer_group(convert['customer']['group_id'])
			if not customer_group:
				customer_group = None
		try:
			order_status = self._notice['map']['order_status'][convert['status']]
		except Exception:
			order_status = 'canceled'
		if convert.get('store_id'):
			store_src_id = convert['store_id']
		else:
			store_src_id = 0
			if self._notice['src']['language_default']:
				store_src_id = self._notice['src']['language_default']
			else:
				for language_id, language_label in self._notice['src']['languages'].items():
					store_src_id = language_id
					break
		store_id = self.get_map_store_view(store_src_id)
		if self._notice['support']['site_map'] and 'store_id' in convert and convert['store_id'] in self._notice['map']['site']:
			store_id = self._notice['map']['site'][convert['store_id']]
		store_name = self.get_name_store_view(store_id)
		store_name = store_name[0:32]
		total_qty_ordered = 0
		for item in convert['items']:
			if item['qty']:
				# total_qty_ordered = to_int(total_qty_ordered) + to_int(item['qty'])
				total_qty_ordered += to_int(to_decimal(item['qty'])) if item['qty'] else 0
		# subtotal += to_decimal(value['subtotal']) if value['subtotal'] else 0
		# subtotal = convert['subtotal']
		shipping_tax_amount = to_decimal(get_value_by_key_in_dict(convert, 'shipping_tax_amount', 0))
		shipping_incl = to_decimal(convert['shipping']['amount']) + shipping_tax_amount
		order_entity_data = {
			'state': self.get_order_state_by_order_status(order_status),
			'status': order_status,
			'coupon_code': get_value_by_key_in_dict(order, 'coupon_code'),
			'protect_code': get_value_by_key_in_dict(order, 'protect_code'),
			'shipping_description': get_value_by_key_in_dict(convert['shipping'], 'title', ''),
			'is_virtual': get_value_by_key_in_dict(order, 'is_virtual', 0),
			'store_id': store_id,
			'customer_id': customer_id,
			'base_discount_amount': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'base_discount_invoiced': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'base_discount_canceled': get_value_by_key_in_dict(order, 'base_discount_canceled'),
			'base_discount_refunded': get_value_by_key_in_dict(order, 'base_discount_refunded'),
			'base_grand_total': convert['total']['amount'] if convert['total']['amount'] else None,
			'base_shipping_amount': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'base_shipping_invoiced': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'base_shipping_canceled': get_value_by_key_in_dict(order, 'base_shipping_canceled'),
			'base_shipping_refunded': get_value_by_key_in_dict(order, 'base_shipping_refunded'),
			'base_shipping_tax_amount': shipping_tax_amount,
			'base_shipping_tax_refunded': get_value_by_key_in_dict(order, 'base_shipping_tax_refunded'),
			'base_subtotal': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'base_subtotal_invoiced': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'base_subtotal_canceled': get_value_by_key_in_dict(order, 'base_subtotal_canceled'),
			'base_subtotal_refunded': get_value_by_key_in_dict(order, 'base_subtotal_refunded'),
			'base_tax_amount': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'base_tax_invoiced': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'base_tax_canceled': get_value_by_key_in_dict(order, 'base_tax_canceled'),
			'base_tax_refunded': get_value_by_key_in_dict(order, 'base_tax_refunded'),
			'base_to_global_rate': '1.0000',
			'base_to_order_rate': '1.0000',
			'base_total_canceled': get_value_by_key_in_dict(order, 'base_total_canceled'),
			'base_total_invoiced_cost': get_value_by_key_in_dict(order, 'base_total_invoiced_cost'),
			'base_total_offline_refunded': get_value_by_key_in_dict(order, 'base_total_offline_refunded'),
			'base_total_online_refunded': get_value_by_key_in_dict(order, 'base_total_online_refunded'),
			'base_total_paid': convert['total']['amount'] if order_status == 'complete' else None,
			'base_total_invoiced': convert['total']['amount'] if order_status == 'complete' else None,
			'base_total_qty_ordered': get_value_by_key_in_dict(order, 'base_total_qty_ordered'),
			'base_total_refunded': get_value_by_key_in_dict(order, 'base_total_refunded'),
			'discount_amount': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'discount_invoiced': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'discount_canceled': get_value_by_key_in_dict(order, 'discount_canceled'),
			'discount_refunded': get_value_by_key_in_dict(order, 'discount_refunded'),
			'grand_total': convert['total']['amount'] if convert['total']['amount'] else None,
			'shipping_amount': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'shipping_invoiced': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'shipping_canceled': get_value_by_key_in_dict(order, 'shipping_canceled'),
			'shipping_refunded': get_value_by_key_in_dict(order, 'shipping_refunded'),
			'shipping_tax_amount': shipping_tax_amount,
			'shipping_tax_refunded': get_value_by_key_in_dict(order, 'shipping_tax_refunded'),
			'store_to_base_rate': 0.0,
			'store_to_order_rate': 0.0,
			'subtotal': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'subtotal_invoiced': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'subtotal_canceled': get_value_by_key_in_dict(order, 'subtotal_canceled'),
			'subtotal_refunded': get_value_by_key_in_dict(order, 'subtotal_refunded'),
			'tax_amount': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'tax_invoiced': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'tax_canceled': get_value_by_key_in_dict(order, 'tax_canceled'),
			'tax_refunded': get_value_by_key_in_dict(order, 'tax_refunded'),
			'total_canceled': get_value_by_key_in_dict(order, 'total_canceled'),
			'total_offline_refunded': get_value_by_key_in_dict(order, 'total_offline_refunded'),
			'total_online_refunded': get_value_by_key_in_dict(order, 'total_online_refunded'),
			'total_paid': convert['total']['amount'] if order_status == 'complete' else None,
			'total_invoiced': convert['total']['amount'] if order_status == 'complete' else None,
			'total_qty_ordered': total_qty_ordered if total_qty_ordered else None,
			'total_refunded': get_value_by_key_in_dict(order, 'total_refunded'),
			'can_ship_partially': get_value_by_key_in_dict(order, 'can_ship_partially'),
			'can_ship_partially_item': get_value_by_key_in_dict(order, 'can_ship_partially_item'),
			'customer_is_guest': get_value_by_key_in_dict(order, 'customer_is_guest'),
			'customer_note_notify': get_value_by_key_in_dict(order, 'customer_note_notify'),
			'billing_address_id': get_value_by_key_in_dict(order, 'billing_address_id'),
			'customer_group_id': customer_group,
			'edit_increment': get_value_by_key_in_dict(order, 'edit_increment'),
			'email_sent': get_value_by_key_in_dict(order, 'email_sent'),
			'send_email': get_value_by_key_in_dict(order, 'send_email'),
			'forced_shipment_with_invoice': get_value_by_key_in_dict(order, 'forced_shipment_with_invoice'),
			'payment_auth_expiration': get_value_by_key_in_dict(order, 'payment_auth_expiration'),
			'quote_address_id': None,
			'quote_id': None,
			'shipping_address_id': None,
			'adjustment_negative': order.get('adjustment_negative', 0.000),
			'adjustment_positive': order.get('adjustment_positive', 0.000),
			'base_adjustment_negative': order.get('base_adjustment_negative', 0.000),
			'base_adjustment_positive': order.get('base_adjustment_positive', 0.000),
			'base_shipping_discount_amount': get_value_by_key_in_dict(order, 'base_shipping_discount_amount', '0.0000'),
			'base_subtotal_incl_tax': to_decimal(convert['subtotal']['amount']) + to_decimal(convert['tax']['amount']),
			'base_total_due': convert['total']['amount'] if convert['total']['amount'] else None,
			'payment_authorization_amount': get_value_by_key_in_dict(order, 'payment_authorization_amount'),
			'shipping_discount_amount': get_value_by_key_in_dict(order, 'shipping_discount_amount', '0.0000'),
			'subtotal_incl_tax': to_decimal(convert['subtotal']['amount']) + to_decimal(convert['tax']['amount']),
			'total_due': convert['total']['amount'] if convert['total']['amount'] and order_status != 'complete' else None,
			'weight': get_value_by_key_in_dict(order, 'weight'),
			'customer_dob': get_value_by_key_in_dict(order, 'customer_dob'),
			'increment_id': None,
			'applied_rule_ids': get_value_by_key_in_dict(order, 'applied_rule_ids'),
			'base_currency_code': self._notice['target']['currency_default'],
			'customer_email': get_value_by_key_in_dict(convert['customer'], 'email'),
			'customer_firstname': get_value_by_key_in_dict(convert['customer'], 'first_name'),
			'customer_lastname': get_value_by_key_in_dict(convert['customer'], 'last_name'),
			'customer_middlename': get_value_by_key_in_dict(convert['customer'], 'middle_name'),
			'customer_prefix': get_value_by_key_in_dict(order, 'customer_prefix'),
			'customer_suffix': get_value_by_key_in_dict(order, 'customer_suffix'),
			'customer_taxvat': get_value_by_key_in_dict(order, 'customer_taxvat'),
			'discount_description': get_value_by_key_in_dict(order, 'discount_description'),
			'ext_customer_id': None,
			'ext_order_id': None,
			'global_currency_code': self._notice['target']['currency_default'],
			'hold_before_state': get_value_by_key_in_dict(order, 'hold_before_state'),
			'hold_before_status': get_value_by_key_in_dict(order, 'hold_before_status'),
			'order_currency_code': self._notice['target']['currency_default'],
			'original_increment_id': get_value_by_key_in_dict(order, 'original_increment_id'),
			'relation_child_id': None,
			'relation_child_real_id': None,
			'relation_parent_id': None,
			'remote_ip': get_value_by_key_in_dict(order, 'remote_ip'),
			# 'shipping_method': get_value_by_key_in_dict(order, 'shipping_method', 'no_shippingmethod'),
			'shipping_method': 'flatrate_flatrate',
			'store_currency_code': get_value_by_key_in_dict(convert, 'currency', self._notice['target']['currency_default']),
			'store_name': store_name if store_name else None,
			'x_forwarded_for': str(get_value_by_key_in_dict(order, 'x_forwarded_for'))[:32],
			'customer_note': get_value_by_key_in_dict(order, 'customer_note'),
			'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),
			'total_item_count': to_len(convert['items']),
			'customer_gender': get_value_by_key_in_dict(order, 'customer_gender'),
			'discount_tax_compensation_amount': get_value_by_key_in_dict(order, 'discount_tax_compensation_amount'),
			'base_discount_tax_compensation_amount': get_value_by_key_in_dict(order, 'base_discount_tax_compensation_amount'),
			'shipping_discount_tax_compensation_amount': get_value_by_key_in_dict(order, 'shipping_discount_tax_compensation_amount'),
			'base_shipping_discount_tax_compensation_amnt': get_value_by_key_in_dict(order, 'base_shipping_discount_tax_compensation_amnt'),
			'discount_tax_compensation_invoiced': get_value_by_key_in_dict(order, 'discount_tax_compensation_invoiced'),
			'base_discount_tax_compensation_invoiced': get_value_by_key_in_dict(order, 'base_discount_tax_compensation_invoiced'),
			'discount_tax_compensation_refunded': get_value_by_key_in_dict(order, 'discount_tax_compensation_refunded'),
			'base_discount_tax_compensation_refunded': get_value_by_key_in_dict(order, 'base_discount_tax_compensation_refunded'),
			'shipping_incl_tax': shipping_incl,
			'base_shipping_incl_tax': shipping_incl,
			'coupon_rule_name': get_value_by_key_in_dict(order, 'coupon_rule_name'),
			'gift_message_id': get_value_by_key_in_dict(order, 'gift_message_id'),
			'paypal_ipn_customer_notified': get_value_by_key_in_dict(order, 'paypal_ipn_customer_notified'),
		}

		# if self._notice['config']['pre_ord'] and self._notice['src'].get('setup_type') != 'api':
			# self.delete_target_order(convert['id'])
			# order_entity_data['entity_id'] = convert['id']
		#order_id = self.import_order_data_connector(self.create_insert_query_connector('sales_order', order_entity_data), True, convert['id'])
		order_id = self.import_order_data_connector(self.create_insert_query_connector('sales_order', order_entity_data), True, convert['id'])
		if not order_id:
			return response_error(self.warning_import_entity(self.TYPE_ORDER, convert['id']))
		new_order_increment_id = '' + to_str(order_id)
		while to_len(new_order_increment_id) < 8:
			new_order_increment_id = '0' + new_order_increment_id
		new_order_increment_id = '1' + new_order_increment_id
		order_number = convert.get('order_number')
		if not order_number:
			order_number = convert['id']

		old_order_increment_id = order_number if order_number else new_order_increment_id
		if self._notice['config']['pre_ord']:
			order_increment_id = old_order_increment_id
		else:
			order_increment_id = new_order_increment_id
		self.insert_map(self.TYPE_ORDER, convert['id'], order_id, additional_data = self.construct_additional_data_map(order_number, order_increment_id))
		return response_success(order_id)

	def after_order_import(self, order_id, convert, order, orders_ext):
		all_queries = list()
		new_order_increment_id = '' + to_str(order_id)
		while to_len(new_order_increment_id) < 8:
			new_order_increment_id = '0' + new_order_increment_id
		new_order_increment_id = '1' + new_order_increment_id
		order_number = convert.get('order_number')
		if not order_number:
			order_number = convert['id']

		old_order_increment_id = order_number if order_number else new_order_increment_id
		if self._notice['config']['pre_ord']:
			order_increment_id = old_order_increment_id
		else:
			order_increment_id = new_order_increment_id
		customer = convert['customer']
		customer_id = None
		if customer.get('id'):
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, customer['id'])
		if not customer_id:
			customer_id = None

		if 'group_id' in customer:
			customer_group = self.get_map_customer_group(customer['group_id'])
			if not customer_group:
				customer_group = None
		else:
			customer_group = None

		try:
			order_status = self._notice['map']['order_status'][convert['status']]
		except Exception:
			order_status = 'canceled'
		# try:
		# 	store_id = self._notice['map']['languages'][to_str(self._notice['src']['language_default'])]
		# except Exception:
		# 	store_id = 1
		if convert.get('store_id'):
			store_src_id = convert['store_id']
		else:
			store_src_id = 0
			if self._notice['src']['language_default']:
				store_src_id = self._notice['src']['language_default']
			else:
				for language_id, language_label in self._notice['src']['languages'].items():
					store_src_id = language_id
					break
		store_id = self.get_map_store_view(store_src_id)
		store_name = self.get_name_store_view(store_id)
		customer_fullname = to_str(customer.get('first_name')) + ' ' + to_str(customer.get('middle_name')) + (
			' ' if customer.get('middle_name') else '') + to_str(customer.get('last_name'))
		bill_address = convert['billing_address']
		ship_address = convert['shipping_address']
		billing_full_name = to_str(bill_address.get('first_name')) + ' ' + to_str(bill_address.get('middle_name')) + (
			' ' if customer.get('middle_name') else '') + to_str(bill_address.get('last_name'))
		shipping_full_name = to_str(ship_address.get('first_name')) + ' ' + to_str(ship_address.get('middle_name')) + (
			' ' if customer.get('middle_name') else '') + to_str(ship_address.get('last_name'))
		sales_order_grid_data = {
			'entity_id': order_id,
			'status': order_status,
			'store_id': store_id,
			'store_name': store_name,
			'customer_id': customer_id,
			'base_grand_total': convert['total']['amount'],
			'base_total_paid': None,
			'grand_total': convert['total']['amount'],
			'total_paid': convert['total']['amount'],
			'increment_id': order_increment_id,
			'base_currency_code': self._notice['target']['currency_default'],
			'order_currency_code': self._notice['target']['currency_default'],
			'shipping_name': shipping_full_name,
			'billing_name': billing_full_name,
			'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),
			'billing_address': bill_address['address_1'],
			'shipping_address': ship_address['address_1'],
			'shipping_information': convert['shipping']['title'],
			'customer_email': customer['email'],
			'customer_group': customer_group,
			'subtotal': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'shipping_and_handling': None,
			'customer_name': customer_fullname,
			'payment_method': get_value_by_key_in_dict(convert, 'payment_method', None),
			'total_refunded': None,
		}
		all_queries.append(self.create_insert_query_connector('sales_order_grid', sales_order_grid_data))

		# todo: order address
		# billing
		sales_order_address_billing_data = {
			'parent_id': order_id,
			'customer_address_id': None,
			'quote_address_id': None,
			'region_id': None,
			'customer_id': customer_id,
			'fax': bill_address['fax'],
			'region': bill_address['state']['name'],
			'postcode': bill_address['postcode'],
			'lastname': bill_address['last_name'],
			'street': to_str(bill_address['address_1']) + '\n' + to_str(bill_address['address_2']),
			'city': bill_address['city'],
			'email': customer['email'],
			'telephone': bill_address['telephone'],
			'country_id': bill_address['country']['country_code'] if bill_address['country']['country_code'] else 'US',
			'firstname': bill_address['first_name'],
			'address_type': 'billing',
			'prefix': None,
			'middlename': bill_address['middle_name'],
			'suffix': None,
			'company': bill_address['company'],
			'vat_id': None,
			'vat_is_valid': None,
			'vat_request_id': None,
			'vat_request_date': None,
			'vat_request_success': None,
		}
		sales_address_billing_id = self.import_order_data_connector(
			self.create_insert_query_connector('sales_order_address', sales_order_address_billing_data))
		if not sales_address_billing_id:
			sales_address_billing_id = None
		# shipping
		sales_order_address_shipping_data = {
			'parent_id': order_id,
			'customer_address_id': None,
			'quote_address_id': None,
			'region_id': None,
			'customer_id': customer_id,
			'fax': ship_address['fax'],
			'region': ship_address['state']['name'],
			'postcode': ship_address['postcode'],
			'lastname': ship_address['last_name'],
			'street': to_str(ship_address['address_1']) + '\n' + to_str(ship_address['address_2']),
			'city': ship_address['city'],
			'email': customer['email'],
			'telephone': ship_address['telephone'],
			'country_id': ship_address['country']['country_code'] if ship_address['country']['country_code'] else 'US',
			'firstname': ship_address['first_name'],
			'address_type': 'shipping',
			'prefix': None,
			'middlename': ship_address['middle_name'],
			'suffix': None,
			'company': ship_address['company'],
			'vat_id': None,
			'vat_is_valid': None,
			'vat_request_id': None,
			'vat_request_date': None,
			'vat_request_success': None,
		}
		sales_address_shipping_id = self.import_order_data_connector(
			self.create_insert_query_connector('sales_order_address', sales_order_address_shipping_data))
		if not sales_address_shipping_id:
			sales_address_shipping_id = None
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: status histories
		# begin
		for order_history in convert['history']:
			sales_order_status_history_data = {
				'parent_id': order_id,
				'is_customer_notified': 1,
				'is_visible_on_front': 0,
				'comment': order_history['comment'] if order_history['comment'] else '',
				'status': order_status,
				'created_at': convert_format_time(order_history['created_at']),
				'entity_name': 'order',
			}
			all_queries.append(
				self.create_insert_query_connector('sales_order_status_history', sales_order_status_history_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------
		# shipment
		order_shipment_id = False
		if convert.get('check_fulfill', False):
			total_qty_ordered = 0
			for item in convert['items']:
				if item['qty']:
					# total_qty_ordered = to_int(total_qty_ordered) + to_int(item['qty'])
					total_qty_ordered += to_int(to_decimal(item['qty'])) if item['qty'] else 0
			shipment_data = {
				'store_id': 3,
				'total_qty': total_qty_ordered,
				'order_id': order_id,
				'customer_id': customer_id,
				'shipping_address_id': sales_address_billing_id,
				'billing_address_id': sales_address_shipping_id,
				'increment_id': order_increment_id[1:] + "-shipment",
				'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
				'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),
			}
			order_shipment_id = self.import_order_data_connector(self.create_insert_query_connector('sales_shipment', shipment_data, True), True, convert['id'])

			if order_shipment_id:
				sales_shipment_grid_data = {
					'entity_id': order_shipment_id,
					'increment_id': order_increment_id[1:] + "-shipment",
					'store_id': store_id,
					'total_qty': total_qty_ordered,
					'order_increment_id': order_increment_id,
					'order_id': order_id,
					'order_created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
					'customer_name': customer_fullname,
					'order_status': order_status,
					'billing_address': bill_address['address_1'],
					'shipping_address': ship_address['address_1'],
					'billing_name': billing_full_name,
					'shipping_name': shipping_full_name,
					'customer_email': customer['email'],
					'customer_group_id': customer_group,
					'shipping_information': get_value_by_key_in_dict(convert['shipping'], 'title', ''),
					'payment_method': get_value_by_key_in_dict(convert, 'payment_method', None),
					'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
					'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),

				}
				all_queries.append(self.create_insert_query_connector('sales_shipment_grid', sales_shipment_grid_data))

		# todo: order item
		# begin
		items_order = convert['items']
		item_queries = list()
		item_ids = list()
		map_item = dict()
		for item_order in items_order:
			sku = item_order['product']['sku']
			product = self.select_map(self._migration_id, self.TYPE_PRODUCT, item_order['product']['id'], None, item_order['product']['code'])
			if product:
				product_id = product['id_desc']
				sku = product['code_desc']
			else:
				product_id = None
			# product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item_order['product']['id'],
			#                                        item_order['product']['code'])
			# if not product_id:
			# 	product_id = None
			product_options = ''
			options = item_order.get('options')
			_product_options = dict()
			# if
			if options:

				product_option = list()
				for option in options:
					item_option = {
						'label': self.strip_html_tag(option['option_name'], True),
						'value': self.strip_html_tag(option['option_value_name'])
					}
					product_option.append(item_option)
				_product_options['options'] = product_option

				product_options = self.magento_serialize(_product_options)
			# endif
			total = item_order['subtotal']
			total_incl = item_order['total']
			if total == total_incl:
				total_incl = to_decimal(total) + to_decimal(item_order['tax_amount'])
			tax_percent = to_decimal(item_order['tax_amount'])/to_decimal(total) * 100 if total and to_decimal(total) > 0 else 0
			sales_order_item_data = {
				'order_id': order_id,
				'parent_item_id': None,
				'quote_item_id': None,
				'store_id': store_id,
				'created_at': get_value_by_key_in_dict(item_order, 'created_at', get_current_time()),
				'updated_at': get_value_by_key_in_dict(item_order, 'updated_at', get_current_time()),
				'product_id': product_id,
				'product_type': get_value_by_key_in_dict(item_order, 'product_type', get_value_by_key_in_dict(product, 'value', 'simple') if product else 'simple'),
				'product_options': product_options,
				'weight': get_value_by_key_in_dict(item_order, 'weight'),
				'is_virtual': get_value_by_key_in_dict(item_order, 'is_virtual'),
				'sku': sku,
				'name': get_value_by_key_in_dict(get_value_by_key_in_dict(item_order, 'product', dict()), 'name'),
				'description': get_value_by_key_in_dict(item_order, 'description', ''),
				'applied_rule_ids': '',
				'additional_data': '',
				'is_qty_decimal': get_value_by_key_in_dict(item_order, 'is_qty_decimal', 0),
				'no_discount': get_value_by_key_in_dict(item_order, 'no_discount', 0),
				'qty_backordered': get_value_by_key_in_dict(item_order, 'qty_backordered', 0),
				'qty_canceled': get_value_by_key_in_dict(item_order, 'qty_canceled', 0),
				'qty_invoiced': get_value_by_key_in_dict(item_order, 'qty_invoiced', 0),
				'qty_ordered': get_value_by_key_in_dict(item_order, 'qty', 1),
				'qty_refunded': get_value_by_key_in_dict(item_order, 'qty_refunded', 0),
				'qty_shipped': get_value_by_key_in_dict(item_order, 'qty_shipped', 0),
				'base_cost': get_value_by_key_in_dict(item_order, 'base_cost', 0),
				'price': get_value_by_key_in_dict(item_order, 'price', '0.0000'),
				'base_price': get_value_by_key_in_dict(item_order, 'price', '0.0000'),
				'original_price': get_value_by_key_in_dict(item_order, 'original_price', '0.0000'),
				'base_original_price': get_value_by_key_in_dict(item_order, 'original_price', '0.0000'),
				'tax_percent': tax_percent,
				'tax_amount': get_value_by_key_in_dict(item_order, 'tax_amount', 0),
				'base_tax_amount': get_value_by_key_in_dict(item_order, 'tax_amount', 0),
				'tax_invoiced': get_value_by_key_in_dict(item_order, 'tax_invoiced', 0),
				'base_tax_invoiced': get_value_by_key_in_dict(item_order, 'tax_invoiced', 0),
				'discount_percent': get_value_by_key_in_dict(item_order, 'discount_percent', 0),
				'discount_amount': get_value_by_key_in_dict(item_order, 'discount_amount', 0),
				'base_discount_amount': get_value_by_key_in_dict(item_order, 'base_discount_amount', 0),
				'discount_invoiced': get_value_by_key_in_dict(item_order, 'discount_invoiced', 0),
				'base_discount_invoiced': get_value_by_key_in_dict(item_order, 'discount_invoiced', 0),
				'amount_refunded': '0.0000',
				'base_amount_refunded': '0.0000',
				'row_total': total,
				'base_row_total': total,
				'row_invoiced': get_value_by_key_in_dict(item_order, 'row_invoiced', 0),
				'base_row_invoiced': get_value_by_key_in_dict(item_order, 'row_invoiced', 0),
				'row_weight': get_value_by_key_in_dict(item_order, 'row_weight', 0),
				'base_tax_before_discount': get_value_by_key_in_dict(item_order, 'base_tax_before_discount', 0),
				'tax_before_discount': get_value_by_key_in_dict(item_order, 'tax_before_discount', 0),
				'ext_order_item_id': '',
				'locked_do_invoice': get_value_by_key_in_dict(item_order, 'locked_do_invoice', 0),
				'locked_do_ship': get_value_by_key_in_dict(item_order, 'locked_do_ship', None),
				'price_incl_tax': to_decimal(get_value_by_key_in_dict(item_order, 'price', '0.0000')) + to_decimal(get_value_by_key_in_dict(item_order, 'tax_amount', 0)),
				'base_price_incl_tax': to_decimal(get_value_by_key_in_dict(item_order, 'price', '0.0000')) + to_decimal(get_value_by_key_in_dict(item_order, 'tax_amount', 0)),
				'row_total_incl_tax': total_incl,
				'base_row_total_incl_tax': total_incl,
				'discount_tax_compensation_amount': '0.0000',
				'base_discount_tax_compensation_amount': '0.0000',
				'discount_tax_compensation_invoiced': '0.0000',
				'base_discount_tax_compensation_invoiced': '0.0000',
				'discount_tax_compensation_refunded': '0.0000',
				'base_discount_tax_compensation_refunded': '0.0000',
				'tax_canceled': get_value_by_key_in_dict(item_order, 'tax_canceled', 0),
				'discount_tax_compensation_canceled': '0.0000',
				'tax_refunded': get_value_by_key_in_dict(item_order, 'tax_refunded', 0),
				'base_tax_refunded': get_value_by_key_in_dict(item_order, 'tax_refunded', 0),
				'discount_refunded': get_value_by_key_in_dict(item_order, 'discount_refunded', 0),
				'base_discount_refunded': get_value_by_key_in_dict(item_order, 'discount_refunded', 0),
				'free_shipping': get_value_by_key_in_dict(item_order, 'free_shipping', 0),
				'gift_message_id': None,
				'gift_message_available': None,
				'weee_tax_applied': None,
				'weee_tax_applied_amount': None,
				'weee_tax_applied_row_amount': None,
				'weee_tax_disposition': None,
				'weee_tax_row_disposition': None,
				'base_weee_tax_applied_amount': None,
				'base_weee_tax_applied_row_amnt': None,
				'base_weee_tax_disposition': None,
				'base_weee_tax_row_disposition': None,
			}
			order_item_id = self.import_order_data_connector(self.create_insert_query_connector('sales_order_item', sales_order_item_data, True), True, convert['id'])
			if convert.get('check_fulfill', False) and order_shipment_id:
				sales_shipment_item = {
					'parent_id': order_shipment_id,
					'price': get_value_by_key_in_dict(item_order, 'price', '0.0000'),
					'qty': get_value_by_key_in_dict(item_order, 'qty', 1),
					'product_id': product_id,
					'order_item_id': order_item_id,
					'name': get_value_by_key_in_dict(get_value_by_key_in_dict(item_order, 'product', dict()), 'name'),
					'sku': sku,
				}
				all_queries.append(self.create_insert_query_connector('sales_shipment_item', sales_shipment_item, True))
		# endfor
		# items_import = self.import_multiple_data_connector(item_queries, 'order')
		# if 'data' in items_import:
		# 	for index, order_item_id in enumerate(items_import['data']):
		# 		if order_item_id:
		# 			map_item[item_ids[index]] = order_item_id
		#
		# if 'order_child_item' in convert:
		# 	for order_child_item in convert['order_child_item']:
		# 		parent_id = map_item.get(str(order_child_item['parent_id']))
		# 		child_id = map_item.get(str(order_child_item['children_id']))
		# 		if (not parent_id) or (not child_id):
		# 			continue
		# 		all_queries.append(self.create_update_query_connector('sales_order_item', {'parent_item_id': parent_id},
		# 		                                                      {'item_id': child_id}))
		# end
		# todo: order payment method
		# begin
		if convert['payment']:
			sales_order_payment_data = {
				'parent_id': order_id,
				'shipping_amount': convert['shipping']['amount'],
				'method': get_value_by_key_in_dict(convert['payment'], 'title', 'cod'),
				'additional_information': self.magento_serialize({"method_title": convert['payment']['title']}),
			}
			all_queries.append(
				self.create_insert_query_connector('sales_order_payment', sales_order_payment_data, True))
		# end

		# todo: order shipment
		# begin
		# copy from magento/magento2

		order_update_data = {
			'increment_id': order_increment_id,
			'billing_address_id': sales_address_billing_id,
			'shipping_address_id': sales_address_shipping_id,
		}
		all_queries.append(self.create_update_query_connector('sales_order', order_update_data, {
			'entity_id': order_id}))

		# end
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'order')
		return response_success()

	def addition_order_import(self, convert, order, orders_ext):
		return response_success()

	def finish_order_import(self):
		queries = {
			'order': {
				'type': 'select',
				'query': 'SELECT MAX(cast(increment_id as UNSIGNED)) AS max_increment_id, store_id FROM _DBPRF_sales_order group by store_id'
			},
			'invoice': {
				'type': 'select',
				'query': 'SELECT MAX(increment_id) AS max_increment_id, store_id FROM _DBPRF_sales_invoice group by store_id'
			},
			'shipment': {
				'type': 'select',
				'query': 'SELECT MAX(increment_id) AS max_increment_id, store_id FROM _DBPRF_sales_shipment group by store_id'
			},
			'creditmemo': {
				'type': 'select',
				'query': 'SELECT MAX(increment_id) AS max_increment_id, store_id FROM _DBPRF_sales_creditmemo group by store_id'
			},
			'sales_sequence_profile': {
				'type': 'select',
				'query': 'SELECT tblprofile.*, tblmeta.sequence_table FROM _DBPRF_sales_sequence_profile AS tblprofile LEFT JOIN _DBPRF_sales_sequence_meta AS tblmeta ON tblprofile.meta_id = tblmeta.meta_id'
			}
		}
		all_queries = list()
		res = self.select_multiple_data_connector(queries, 'finish_order')
		if not res or res['result'] != 'success':
			return response_error()
		entities = ['order', 'invoice', 'shipment', 'creditmemo']
		for entity in entities:
			if not res['data'][entity]:
				continue
			for row in res['data'][entity]:
				max_increment_id = row['max_increment_id']
				if not max_increment_id:
					continue
				if max_increment_id[0] == '#':
					max_increment_id = max_increment_id[1:]
				if max_increment_id[0:to_len(row['store_id'])] == row['store_id']:
					max_increment_id = max_increment_id[to_len(row['store_id']):]
				table = '_DBPRF_sequence_' + entity + '_' + to_str(row['store_id'])
				prefix = ''
				suffix = ''
				meta_data = get_row_from_list_by_field(res['data']['sales_sequence_profile'], 'sequence_table', table)
				if meta_data:
					prefix = meta_data['prefix']
					suffix = meta_data['suffix']
				if prefix and max_increment_id[0:to_len(prefix)] == prefix:
					max_increment_id = max_increment_id[to_len(prefix):]
				if suffix and max_increment_id[0-len(suffix)] == suffix:
					max_increment_id = max_increment_id[0:0-len(suffix)]
				max_increment_id = re.sub('[^0-9]', '', to_str(max_increment_id))
				max_increment_id = to_int(max_increment_id)
				if max_increment_id:
					all_queries.append({
						'type': 'query',
						'query': 'ALTER TABLE ' + table + ' AUTO_INCREMENT = ' + to_str(max_increment_id + 1)
					})
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'finish_order')
		return response_success()
	# TODO: REVIEW
	def prepare_reviews_import(self):
		return self

	def prepare_reviews_export(self):
		return self

	def get_reviews_main_export(self):
		id_src = self._notice['process']['reviews']['id_src']
		limit = self._notice['setting']['reviews']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_review WHERE review_id IN (select review_id from _DBPRF_review_store where" + self.get_con_store_select_count() + ") AND review_id > " + to_str(
				id_src) + " ORDER BY review_id ASC LIMIT " + to_str(limit)
		}
		reviews = self.select_data_connector(query, 'orders')

		if not reviews or reviews['result'] != 'success':
			return response_error()
		return reviews

	def get_reviews_ext_export(self, reviews):
		review_ids = duplicate_field_value_from_list(reviews['data'], 'review_id')
		review_id_con = self.list_to_in_condition(review_ids)
		url_query = self.get_connector_url('query')
		review_ext_queries = {
			'review_detail': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_review_detail WHERE review_id IN " + review_id_con
			},
			'rating_option_vote': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_rating_option_vote WHERE review_id IN " + review_id_con,
			}
		}
		reviews_ext = self.select_multiple_data_connector(review_ext_queries, 'orders')

		if not reviews_ext or reviews_ext['result'] != 'success':
			return response_error()
		return reviews_ext

	def convert_review_export(self, review, reviews_ext):
		review_data = self.construct_review()
		review_data = self.add_construct_default(review_data)
		review_detail = get_row_from_list_by_field(reviews_ext['data']['review_detail'], 'review_id',
		                                           review['review_id'])
		review_data['id'] = review['review_id']
		review_data['product']['id'] = review['entity_pk_value']
		if review_detail:
			review_data['customer']['id'] = review_detail['customer_id']
			review_data['customer']['name'] = review_detail['nickname']
			review_data['title'] = review_detail['title']
			review_data['content'] = review_detail['detail']
		review_data['status'] = review['status_id']
		review_data['created_at'] = review['created_at']
		review_data['updated_at'] = review['created_at']
		rating = self.construct_review_rating()
		rating_option_vote = get_list_from_list_by_field(reviews_ext['data']['rating_option_vote'], 'review_id', review['review_id'])
		rate_value = 0
		rating['rate_code'] = 'default'
		if rating_option_vote:
			for rating_value in rating_option_vote:
				rate_value += to_decimal(rating_value['value'])
			rating['rate'] = to_decimal(rate_value) / to_len(rating_option_vote)
		else:
			rating['rate'] = rate_value
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
		product_id = False
		if convert['product']['id']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
			if not product_id:
				product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, convert['product']['code'])
		if (not product_id) and convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, convert['product']['code'])
		if not product_id:
			return response_error('Review ' + to_str(convert['id']) + ' import false. Product does not exist!')

		review_data = {
			'entity_pk_value': product_id,
			'created_at': convert['created_at'],
			'status_id': to_int(convert['status']),
			'entity_id': 1,  # id type review in review_entity
		}
		review_import_query = self.create_insert_query_connector('review', review_data)
		review_id = self.import_review_data_connector(review_import_query, True, convert['id'])
		if not review_id:
			response_warning('review id ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		return response_success(review_id)

	def after_review_import(self, review_id, convert, review, reviews_ext):
		all_query = list()
		customer_id = 0
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'])
			if not customer_id and convert['customer']['code']:
				customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, None, convert['customer']['code'])
				if not customer_id:
					customer_id = 0

		review__detail_data = {
			'review_id': review_id,
			'store_id': 1,
			'title': convert['title'],
			'detail': convert['content'],
			'nickname': convert['customer']['name'] if convert['customer']['name'] else '',
			'customer_id': customer_id if customer_id else None
		}

		product_id = 0
		if convert['product']['id']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
		if (not product_id) and convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, convert['product']['code'])
		if not product_id:
			product_id = 0
		# review__detail_data2 = {
		# 	'review_id': review_id,
		# 	'store_id': 1,
		# 	'title': convert['title'],
		# 	'detail': convert['content'],
		# 	'nickname': convert['customer']['name'] if convert['customer']['name'] else '',
		# 	'customer_id': customer_id if customer_id else None
		# }
		review_store_data = {
			'review_id': review_id,
			'store_id': 0,
		}
		review_store_data2 = {
			'review_id': review_id,
			'store_id': 1,
		}
		if convert['rating']:
			option = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'select',
					'query': "SELECT * FROM _DBPRF_rating_option WHERE rating_id = (SELECT rating_id FROM _DBPRF_rating WHERE rating_code LIKE 'Value') AND value = (SELECT rating_id FROM _DBPRF_rating_entity WHERE entity_code LIKE 'product_review')",
				})
			})
			id_option = 0
			if option and option['data']:
				id_option = option['data'][0]['option_id']
			else:
				id_option = 0

			rating_option_vote_data = {
				'customer_id': customer_id if customer_id else None,
				'value': to_str(convert['rating'][0]['rate']).replace('\\', ''),
				'review_id': review_id,
				'entity_pk_value': product_id,
				'option_id': id_option,
			}
			all_query.append(self.create_insert_query_connector('rating_option_vote', rating_option_vote_data))

			review_entity_summary_data = {
				'entity_pk_value': product_id,
				'entity_type': 1,
				'rating_summary': to_str(convert['rating'][0]['rate']).replace('\\', ''),
				'store_id': 0
			}
			all_query.append(self.create_insert_query_connector('review_entity_summary', review_entity_summary_data))

		all_query.append(self.create_insert_query_connector('review_store', review_store_data))
		all_query.append(self.create_insert_query_connector('review_store', review_store_data2))
		all_query.append(self.create_insert_query_connector('review_detail', review__detail_data))
		# all_query.append(self.create_insert_query_connector('review_detail', review__detail_data2))
		self.import_multiple_data_connector(all_query, 'review')
		return response_success()

	def addition_review_import(self, convert, review, reviews_ext):
		return response_success()

	# TODO: PAGE
	def get_pages_main_export(self):
		id_src = self._notice['process']['pages']['id_src']
		limit = self._notice['setting']['pages']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_cms_page WHERE page_id IN (select page_id from _DBPRF_cms_page_store where" + self.get_con_store_select_count() + ") AND "
			         "page_id > " + to_str(id_src) + " ORDER BY page_id ASC LIMIT " + to_str(limit)
		}
		pages = self.select_data_connector(query, 'pages')
		if not pages or pages['result'] != 'success':
			return response_error()
		return pages

	def get_pages_ext_export(self, pages):
		page_ids = duplicate_field_value_from_list(pages['data'], 'page_id')
		page_id_con = self.list_to_in_condition(page_ids)
		pages_ext_queries = {
			'cms_page_store': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_cms_page_store WHERE page_id IN " + page_id_con
			},
		}
		pages_ext = self.select_multiple_data_connector(pages_ext_queries, 'pages')
		if not pages_ext or pages_ext['result'] != 'success':
			return response_error()
		return pages_ext

	def convert_page_export(self, page, pages_ext):
		page_data = self.construct_cms_page()
		page_data['id'] = page['page_id']
		page_data['title'] = page['title']
		page_data['short_description'] = ''
		page_data['content'] = self.convert_image_in_description(page['content'])
		page_data['short_content'] = page['content_heading']
		page_data['url_key'] = page['identifier']
		page_data['status'] = True if to_int(page['is_active']) == 1 else False
		page_data['created_at'] = page['creation_time']
		page_data['updated_at'] = page['update_time']
		page_data['sort_order'] = to_int(page['sort_order'])
		return response_success(page_data)

	def get_page_id_import(self, convert, page, pages_ext):
		return page['page_id']

	def check_page_import(self, convert, page, pages_ext):
		return True if self.get_map_field_by_src(self.TYPE_PAGE, convert['id']) else False

	def router_page_import(self, convert, page, pages_ext):
		return response_success('page_import')

	def before_page_import(self, convert, page, pages_ext):
		return response_success()

	def page_import(self, convert, page, pages_ext):
		router = get_value_by_key_in_dict(convert, 'identifier')
		if not router:
			router = self.generate_url_key(get_value_by_key_in_dict(convert, 'title'))
		index = 1
		new_router = router
		while self.check_exist_url_cms(self.TYPE_PAGE, new_router):
			new_router = router + '-' + to_str(index)
			index += 1
		router = new_router
		page_entity_data = {
			'title': get_value_by_key_in_dict(convert, 'title'),
			'page_layout': self.get_page_layout(get_value_by_key_in_dict(convert, 'page_layout')),
			'meta_keywords': get_value_by_key_in_dict(convert, 'meta_keywords'),
			'meta_description': get_value_by_key_in_dict(convert, 'meta_description'),
			'identifier': router,
			'content_heading': get_value_by_key_in_dict(convert, 'content_heading'),
			'content': self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'content'), True),
			'creation_time': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'update_time': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'is_active': 1 if convert['status'] else 0,
			'sort_order': get_value_by_key_in_dict(convert, 'sort_order', 0),
			'layout_update_xml': self.get_layout_update_xml(get_value_by_key_in_dict(convert, 'layout_update_xml')),
			'custom_theme': None,
			'custom_root_template': self.get_page_layout(get_value_by_key_in_dict(convert, 'custom_root_template')),
			'custom_layout_update_xml': self.get_layout_update_xml(get_value_by_key_in_dict(convert, 'custom_layout_update_xml')),
			'custom_theme_from': get_value_by_key_in_dict(convert, 'custom_theme_from'),
			'custom_theme_to': get_value_by_key_in_dict(convert, 'custom_theme_to'),
			'meta_title': get_value_by_key_in_dict(convert, 'meta_title'),
		}
		if self.convert_version(self._notice['target']['config']['version'], 2) >= 234:
			page_entity_data['layout_update_selected'] = '_no_update_'
		page_id = self.import_page_data_connector(self.create_insert_query_connector('cms_page', page_entity_data), True, convert['id'])
		if not page_id:
			return response_error()
		self.insert_map(self.TYPE_PAGE, convert['id'], page_id, None, router)
		return response_success(page_id)

	def after_page_import(self, page_id, convert, page, pages_ext):
		url = self.select_map(self._migration_id, self.TYPE_PAGE, convert['id'])
		all_queries = list()
		if convert.get('stores'):
			list_stores = list()
			for store_id in convert['stores']:
				store_desc_id = self.get_map_store_view(store_id)
				if to_str(store_desc_id) in list_stores:
					continue
				list_stores.append(to_str(store_desc_id))
				page_store_data = {
					'page_id': page_id,
					'store_id': store_desc_id,
				}
				all_queries.append(self.create_insert_query_connector('cms_page_store', page_store_data))
				page_url_data = {
					'entity_type': 'cms-page',
					'entity_id': page_id,
					'request_path': url['code_desc'],
					'target_path': 'cms/page/view/page_id/' + to_str(page_id),
					'redirect_type': 0,
					'store_id': store_id,
					'description': None,
					'is_autogenerated': 1,
					'metadata': None,
				}
				all_queries.append(self.create_insert_query_connector('url_rewrite', page_url_data))

		if all_queries:
			self.import_multiple_data_connector(all_queries, 'pages')
		return response_success()

	def addition_page_import(self, convert, page, pages_ext):
		return response_success()

	# TODO: BLOCK
	def prepare_blogs_import(self):
		parent = super().prepare_blogs_export()
		attribute_queries = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(
				self._notice['target']['extends']['catalog_category']) + " AND attribute_code = 'landing_page'",
		}
		product_eav_attribute = self.select_data_connector(attribute_queries)
		if product_eav_attribute and product_eav_attribute['result'] == 'success' and product_eav_attribute['data']:
			attribute_landing_page = product_eav_attribute['data'][0]
			self.update_map('attr_landing_page', None, None, attribute_landing_page['attribute_id'])
		return self

	def prepare_blogs_export(self):
		parent = super().prepare_blogs_export()
		attribute_queries = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(
				self._notice['src']['extends']['catalog_category']) + " AND attribute_code = 'landing_page'",
		}
		product_eav_attribute = self.select_data_connector(attribute_queries)
		if product_eav_attribute and product_eav_attribute['result'] == 'success' and product_eav_attribute['data']:
			attribute_landing_page = product_eav_attribute['data'][0]
			self.insert_map('attr_landing_page', attribute_landing_page['attribute_id'], None, 'landing_page')
		return self

	def get_blogs_main_export(self):
		id_src = self._notice['process']['blogs']['id_src']
		limit = self._notice['setting'].get('blogs', 4)
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_cms_block WHERE block_id IN (select block_id from _DBPRF_cms_block_store where" + self.get_con_store_select_count() + ") AND "
					 "block_id > " + to_str(id_src) + " ORDER BY block_id ASC LIMIT " + to_str(limit)
		}
		blocks = self.select_data_connector(query, 'blogs')
		if not blocks or blocks['result'] != 'success':
			return response_error()
		return blocks

	def get_blogs_ext_export(self, blocks):
		block_ids = duplicate_field_value_from_list(blocks['data'], 'block_id')
		block_id_con = self.list_to_in_condition(block_ids)
		attr_landing_page = self.select_map(self._migration_id, 'attr_landing_page')
		attr_landing_page_id = attr_landing_page['id_src']
		blocks_ext_queries = {
			'cms_blog_store': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_cms_blog_store WHERE block_id IN " + block_id_con
			},
			'category_block': {
				'type': 'select',
				'query': "SELECT value as block_id,entity_id as category_id,store_id FROM _DBPRF_catalog_category_entity_int WHERE attribute_id = '" + to_str(
					attr_landing_page_id) + "' AND value IN " + block_id_con,
			}
		}
		blocks_ext = self.select_multiple_data_connector(blocks_ext_queries, 'pages')
		if not blocks_ext or blocks_ext['result'] != 'success':
			return response_error()
		return blocks_ext

	def convert_blog_export(self, block, blocks_ext):
		block_data = block
		block_data['id'] = block['block_id']
		stores = get_list_from_list_by_field(blocks_ext['data']['cms_blog_store'], 'block_id', block_data['id'])
		block_data['stores'] = list()
		for store in stores:
			block_data['stores'].append(store['store_id'])
		block_data['category_block'] = get_list_from_list_by_field(blocks_ext['data']['category_block'], 'block_id', block['block_id'])

		del (block_data['block_id'])
		return response_success(block_data)

	def get_blog_id_import(self, convert, block, blocks_ext):
		return block['block_id']

	def check_blog_import(self, convert, block, blocks_ext):
		return True if self.get_map_field_by_src(self.TYPE_BLOG, convert['id']) else False

	def router_blog_import(self, convert, block, blocks_ext):
		return response_success('block_import')

	def before_blog_import(self, convert, block, blocks_ext):
		return response_success()

	def blog_import(self, convert, block, blocks_ext):
		router = get_value_by_key_in_dict(convert, 'identifier')
		if not router:
			router = self.generate_url_key(get_value_by_key_in_dict(convert, 'title'))
		index = 1
		new_router = router
		while self.check_exist_url_cms(self.TYPE_BLOG, new_router):
			new_router = router + '-' + to_str(index)
			index += 1
		router = new_router
		block_entity_data = {
			'title': get_value_by_key_in_dict(convert, 'title'),
			'identifier': router,
			'content': get_value_by_key_in_dict(convert, 'content'),
			'creation_time': get_value_by_key_in_dict(convert, 'creation_time', get_current_time()),
			'update_time': get_value_by_key_in_dict(convert, 'update_time', get_current_time()),
			'is_active': get_value_by_key_in_dict(convert, 'is_active', 1),
		}
		block_id = self.import_page_data_connector(self.create_insert_query_connector('cms_block', block_entity_data), True, convert['id'])
		if not block_id:
			return response_error()
		self.insert_map(self.TYPE_BLOG, convert['id'], block_id, None, router)
		return response_success(block_id)

	def after_blog_import(self, block_id, convert, block, blocks_ext):
		url = self.select_map(self._migration_id, self.TYPE_BLOG, convert['id'])
		all_queries = list()
		if convert.get('stores'):
			list_stores = list()
			for store_id in convert['stores']:
				store_desc_id = self.get_map_store_view(store_id)
				if to_str(store_desc_id) in list_stores:
					continue
				list_stores.append(to_str(store_desc_id))
				block_store_data = {
					'block_id': block_id,
					'store_id': store_desc_id,
				}
				all_queries.append(self.create_insert_query_connector('cms_blog_store', block_store_data))
				block_url_data = {
					'entity_type': 'cms-block',
					'entity_id': block_id,
					'request_path': url['code_desc'],
					'target_path': 'cms/block/view/block_id/' + to_str(block_id),
					'redirect_type': 0,
					'store_id': store_id,
					'description': None,
					'is_autogenerated': 1,
					'metadata': None,
				}
				all_queries.append(self.create_insert_query_connector('url_rewrite', block_url_data))
		if convert.get('category_block'):
			attr_landing_page = self.select_map(self._migration_id, 'attr_landing_page')
			attr_landing_page_id = attr_landing_page['id_desc']
			for item in convert['category_block']:
				category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, item['category_id'])
				if category_id:
					category_blog_data = {
						'attribute_id': attr_landing_page_id,
						'store_id': self.get_map_store_view(item['store_id']),
						'entity_id': category_id,
						'value': block_id,
					}
					all_queries.append(self.create_insert_query_connector('catalog_category_entity_int', category_blog_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'blogs')
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
		limit = self._notice['setting'].get('coupons', 4)
		query = {
			'type': 'select',
			'query': "SELECT s.* FROM _DBPRF_salesrule as s inner join _DBPRF_salesrule_coupon as sc on s.rule_id = sc.rule_id WHERE "
			         "s.rule_id > " + to_str(id_src) + " ORDER BY s.rule_id ASC LIMIT " + to_str(limit)
		}
		coupons = self.select_data_connector(query, 'coupons')
		if not coupons or coupons['result'] != 'success':
			return response_error()
		return coupons

	def get_coupons_ext_export(self, coupons):
		rule_ids = duplicate_field_value_from_list(coupons['data'], 'rule_id')
		rule_id_con = self.list_to_in_condition(rule_ids)
		coupons_ext_queries = {
			'salesrule_coupon': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_coupon WHERE rule_id IN " + rule_id_con
			},
			'salesrule_customer': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_customer WHERE rule_id IN " + rule_id_con,
			},
			# 'salesrule_customer_group': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_salesrule_customer_group WHERE rule_id IN " + rule_id_con,
			# },
			'salesrule_label': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_label WHERE rule_id IN " + rule_id_con,
			},
			'salesrule_product_attribute': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_product_attribute WHERE rule_id IN " + rule_id_con,
			}
		}
		coupons_ext = self.select_multiple_data_connector(coupons_ext_queries, 'coupons')
		if not coupons_ext or coupons_ext['result'] != 'success':
			return response_error()
		coupon_ids = duplicate_field_value_from_list(coupons_ext['data']['salesrule_coupon'], 'coupon_id')
		coupon_id_con = self.list_to_in_condition(coupon_ids)
		coupons_ext_queries_rel = {
			'salesrule_coupon_usage': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_coupon_usage WHERE coupon_id IN " + coupon_id_con
			},
		}
		if coupons['data'] and 'customer_group_ids' not in coupons['data'][0]:
			coupons_ext_queries_rel['salesrule_customer_group'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_customer_group WHERE rule_id IN " + rule_id_con,
			}

		if coupons['data'] and 'website_ids' not in coupons['data'][0]:
			coupons_ext_queries_rel['salesrule_website'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_salesrule_website WHERE rule_id IN " + rule_id_con,
			}
		coupons_ext_rel = self.select_multiple_data_connector(coupons_ext_queries_rel, 'coupons')

		if not coupons_ext_rel or coupons_ext_rel['result'] != 'success':
			return response_error()
		coupons_ext = self.sync_connector_object(coupons_ext, coupons_ext_rel)
		return coupons_ext

	def convert_coupon_export(self, coupon, coupons_ext):
		coupon_data = self.construct_coupon()
		coupon_data['id'] = coupon['rule_id']
		salesrule_coupon = get_row_from_list_by_field(coupons_ext['data']['salesrule_coupon'], 'rule_id', coupon['rule_id'])
		if not salesrule_coupon:
			self.log(coupon['rule_id'] + 'coupon code empty')
			return response_error()
		coupon_label = get_list_from_list_by_field(coupons_ext['data']['salesrule_label'], 'rule_id', coupon['rule_id'])
		coupon_label_default = get_row_value_from_list_by_field(coupon_label, 'store_id', 0, 'label')

		coupon_data['code'] = salesrule_coupon['code']
		coupon_data['title'] = coupon['name'] if not coupon_label_default else coupon_label_default
		coupon_data['description'] = coupon['description']
		coupon_data['status'] = True if to_int(coupon['is_active']) == 1 else False
		coupon_data['created_at'] = salesrule_coupon['created_at']
		coupon_data['from_date'] = coupon['from_date']
		coupon_data['to_date'] = convert_format_time(salesrule_coupon['expiration_date']) if salesrule_coupon['expiration_date'] else convert_format_time(coupon['to_date'], '%Y-%m-%d')
		coupon_data['times_used'] = salesrule_coupon['times_used']
		coupon_data['usage_limit'] = salesrule_coupon['usage_limit']
		coupon_data['discount_amount'] = coupon['discount_amount']
		coupon_data['usage_per_customer'] = salesrule_coupon['usage_per_customer']
		coupon_data['type'] = self.PERCENT if coupon['simple_action'] == 'by_percent' else self.FIXED
		if 'customer_group_ids' in coupon:
			customer_group = to_str(coupon['customer_group_ids']).split(',')
			coupon_data['customer_group'] = customer_group
		elif coupons_ext['data'] and 'salesrule_customer_group' in coupons_ext['data']:
			customer_groups = get_list_from_list_by_field(coupons_ext['data']['salesrule_customer_group'], 'rule_id', coupon['rule_id'])
			coupon_data['customer_group'] = duplicate_field_value_from_list(customer_groups, 'customer_group_id')
		for lang_id, lang_name in self._notice['src']['languages'].items():
			coupon_language_data = self.construct_coupon_language()
			coupon_label_language = get_row_value_from_list_by_field(coupon_label, 'store_id', lang_id, 'value')
			if coupon_label_language:
				coupon_language_data['title'] = coupon_label_language
			coupon_data['languages'][lang_id] = coupon_language_data
		coupon_usages = get_list_from_list_by_field(coupons_ext['data']['salesrule_coupon_usage'], 'coupon_id', salesrule_coupon['coupon_id'])
		if coupon_usages:
			for coupon_usage in coupon_usages:
				coupon_usage_data = self.construct_coupon_usage()
				coupon_usage_data['customer_id'] = coupon_usage['customer_id']
				coupon_usage_data['timed_usage'] = coupon_usage.get('timed_usage', 0)
				coupon_data['coupon_usage'].append(coupon_usage_data)
		return response_success(coupon_data)

	def get_coupon_id_import(self, convert, coupon, coupons_ext):
		return coupon['rule_id']

	def check_coupon_import(self, convert, coupon, coupons_ext):
		return True if self.get_map_field_by_src(self.TYPE_COUPON, convert['id'], convert['code']) else False

	def router_coupon_import(self, convert, coupon, coupons_ext):
		return response_success('coupon_import')

	def before_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	def coupon_import(self, convert, coupon, coupons_ext):
		product_ids = convert.get('products')
		if product_ids:
			product_id_map_arr = list()
			for product_id in product_ids:
				map_product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, product_id)
				if map_product_id and map_product_id not in product_id_map_arr:
					product_id_map_arr.append(to_str(map_product_id))
			if product_id_map_arr:
				product_ids = ','.join(product_id_map_arr)
			else:
				product_ids = None
		rule_data = {
			'name': get_value_by_key_in_dict(convert, 'title'),
			'description': get_value_by_key_in_dict(convert, 'description'),
			'from_date': get_value_by_key_in_dict(convert, 'from_date'),
			'to_date': get_value_by_key_in_dict(convert, 'to_date'),
			'uses_per_customer': get_value_by_key_in_dict(convert, 'usage_per_customer', 0),
			'is_active': 1 if convert['status'] else 0,
			'conditions_serialized': None,

			'actions_serialized': '',
		#	'product_ids': product_id if to_len(product_ids) > 0 else None,
			'is_advanced': get_value_by_key_in_dict(convert, 'is_advanced', 1),
			'stop_rules_processing': get_value_by_key_in_dict(convert, 'stop_rules_processing', 1),
			'sort_order': get_value_by_key_in_dict(convert, 'sort_order', 0),
			'simple_action': 'by_percent' if convert['type'] == self.PERCENT else 'by_fixed',
			'discount_qty': get_value_by_key_in_dict(convert, 'discount_qty'),
			'discount_amount': get_value_by_key_in_dict(convert, 'discount_amount', 0),
			'discount_step': get_value_by_key_in_dict(convert, 'discount_step', 0),
			'apply_to_shipping': get_value_by_key_in_dict(convert, 'apply_to_shipping', 0),
			'times_used': get_value_by_key_in_dict(convert, 'times_used', 0),
			'is_rss': get_value_by_key_in_dict(convert, 'is_rss', 0),
			#'coupon_type': get_value_by_key_in_dict(convert, 'type', 2),
			'coupon_type': get_value_by_key_in_dict(convert, 'coupon_type', 2),
			'use_auto_generation': get_value_by_key_in_dict(convert, 'use_auto_generation', 0),
			'uses_per_coupon': get_value_by_key_in_dict(convert, 'usage_limit', 0),
			'simple_free_shipping': get_value_by_key_in_dict(convert, 'simple_free_shipping', 0),

		}
		coupon_id = self.import_coupon_data_connector(self.create_insert_query_connector('salesrule', rule_data), True, convert['id'])
		if not coupon_id:
			return response_error()
		self.insert_map(self.TYPE_COUPON, convert['id'], coupon_id)
		return response_success(coupon_id)

	def after_coupon_import(self, coupon_id, convert, coupon, coupons_ext):
		all_queries = list()
		website_ids = list()
		if 'store_ids' in convert and self._notice['support']['site_map']:
			for src_store_id in convert['store_ids']:
				if src_store_id in self._notice['map']['site']:
					target_store = self._notice['map']['site'][src_store_id]
					website_target_id = get_row_value_from_list_by_field(self._notice['target']['website'], 'store_id', target_store, 'website_id')
					if website_target_id and website_target_id not in website_ids:
						website_ids.append(website_target_id)
		if not website_ids:
			target_store_id = self._notice['map']['languages'].get(to_str(self._notice['src']['language_default']), 0)
			website_id = self.get_website_id_by_store_id(target_store_id)
			if website_id:
				website_ids.append(website_id)
		for website_id in website_ids:
			rule_website_data = {
				'rule_id': coupon_id,
				'website_id': website_id
			}
			all_queries.append(self.create_insert_query_connector('salesrule_website', rule_website_data))

		# for customer in convert['customer']:
		# 	customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, customer['customer_id'])
		# 	if not customer_id:
		# 		continue
		# 	rule_customer_data = {
		# 		'rule_id': coupon_id,
		# 		'customer_id': customer_id,
		# 		'times_used': get_value_by_key_in_dict(customer, 'times_used', 0)
		# 	}
		# 	all_queries.append(self.create_insert_query_connector('salesrule_customer', rule_customer_data))
		for customer_group in convert['customer_group']:
			group_id = self._notice['map']['customer_group'].get(to_str(customer_group))
			if not group_id:
				continue
			rule_customer_group_data = {
				'rule_id': coupon_id,
				'customer_group_id': group_id,
			}
			all_queries.append(self.create_insert_query_connector('salesrule_customer_group', rule_customer_group_data))
		if convert['languages']:
			# for
			list_stores = list()
			for language_id, language_data in convert['languages'].items():
				store_id = self.get_map_store_view(language_id)
				if store_id in list_stores:
					continue
				list_stores.append(store_id)
				rule_label_data = {
					'rule_id': coupon_id,
					'store_id': store_id,
					'label': language_data['title']
				}
				all_queries.append(self.create_insert_query_connector('salesrule_label', rule_label_data))

		rule_coupon_data = {
			'rule_id': coupon_id,
			'code': convert['code'],
			'usage_limit': convert['usage_limit'],
			'usage_per_customer': convert['usage_per_customer'],
			'times_used': convert['times_used'] if convert['times_used'] else 0,
			'expiration_date': None,
			'is_primary': 1,
			'created_at': convert['created_at'],
			'type': 0,
		}
		coupon_row_id = self.import_coupon_data_connector(self.create_insert_query_connector('salesrule_coupon', rule_coupon_data))
		if coupon_row_id and convert['coupon_usage']:
			if type(convert['coupon_usage']) == list:
				for usage in convert['coupon_usage']:
					if not usage['timed_usage']:
						continue
					customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, usage['customer_id'])
					if not customer_id:
						continue
					coupon_usage_data = {
						'coupon_id': coupon_row_id,
						'customer_id': customer_id,
						'times_used': usage['timed_usage']
					}
					all_queries.append(self.create_insert_query_connector('salesrule_coupon_usage', coupon_usage_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'coupons')
		return response_success()

	def addition_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	# todo: code magento
	def make_magento_image_path(self, image):
		image_name = os.path.basename(image)
		image_tmp = image_name.split('.')
		del image_tmp[-1]
		image = '.'.join(image_tmp)
		path = ''
		if to_len(image) >= 1:
			path = image[0]
			if to_len(image) >= 2:
				path = path + '/' + image[1]
			else:
				path = path + '/_'
		return path + '/'

	def get_website_ids_target_by_id_src(self, website_ids):
		target_website_ids = list()
		for website_id_src in website_ids:
			try:
				website_id_tar = self._notice['map']['site'][website_id_src]
				if website_id_tar:
					for website_id in website_id_tar:
						if not (website_id in target_website_ids):
							target_website_ids.append(website_id)
			except KeyError:
				pass
		return target_website_ids

	def generate_product_url_key(self, name):
		if not name:
			return ''
		url_key = re.sub('[^0-9a-z]', '-', to_str(name))
		url_key = to_str(url_key).strip(' -')
		while to_str(url_key).find('--') != -1:
			url_key = to_str(url_key).replace('--', '-')
		return url_key

	def check_exist_url_key(self, url_key, store_id = 0, type_url = 'product'):
		query = {
			'type': 'select',
			'query': "SELECT cp.* FROM _DBPRF_catalog_" + type_url + "_entity_varchar AS cp JOIN _DBPRF_eav_attribute AS ea ON ea.attribute_id = cp.attribute_id WHERE ea.attribute_code = 'url_key' AND cp.value = '" + url_key + "' AND cp.store_id = '" + to_str(
				store_id) + "'"
		}
		res = self.select_data_connector(query, type_url + '_errors')
		if (not res) or (res['result'] != 'success') or (not ('data' in res)) or (not res['data']):
			return False
		return to_len(res['data']) > 0

	def get_request_path(self, request_path, store_id = 0, seo_type = 'product'):
		has_suffix = False
		no_suffix = ''
		suffix = ''
		if to_str(request_path).find('.') != -1:
			list_request_path = request_path.split('.')
			suffix = list_request_path[to_len(list_request_path) - 1]
			has_suffix = True
			no_suffix = to_str(request_path).replace('.' + suffix, '')
		cur_request_path = request_path
		while self.check_exist_request_path(cur_request_path, store_id, seo_type):
			index = to_str(to_int(time.time()))
			if has_suffix:
				cur_request_path = no_suffix + '-' + to_str(index) + '.' + suffix
			else:
				cur_request_path = request_path + '-' + to_str(index)
		return cur_request_path

	def check_exist_request_path(self, request_path, store_id = 0, seo_type = 'product'):
		where = {
			'request_path': request_path,
			'store_id': store_id
		}
		url_data = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_url_rewrite WHERE " + self.dict_to_where_condition(where)
			})
		})
		if (not url_data) or (url_data['result'] != 'success') or (not ('data' in url_data)):
			return False
		return to_len(url_data['data']) > 0

	def get_product_url_path(self, url_path, store_id, default):
		if not url_path:
			url_path = default
		if not url_path:
			return ''
		has_suffix = False
		no_suffix = ''
		suffix = ''
		if to_str(url_path).find('.') != -1:
			list_url_path = url_path.split('.')
			suffix = list_url_path[to_len(list_url_path) - 1]
			has_suffix = True
			no_suffix = to_str(url_path).replace('.' + suffix, '')
		cur_url_path = url_path
		index = 0
		while self.check_exist_url_path(cur_url_path, store_id):
			index += 1
			if has_suffix:
				cur_url_path = no_suffix + '-' + to_str(index) + '.' + suffix
			else:
				cur_url_path = url_path + '-' + to_str(index)
		return cur_url_path

	def check_exist_url_path(self, url_path, store_id, type_url = 'product'):
		query = "SELECT cp.* FROM _DBPRF_catalog_" + type_url + "_entity_varchar AS cp JOIN eav_attribute AS ea ON ea.attribute_id = cp.attribute_id WHERE ea.attribute_code = 'url_path' AND cp.value = '" + url_path + "' AND cp.store_id = '" + to_str(
			store_id) + "'"
		res = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select', 'query': query
			})
		})
		if (not res) or (res['result'] != 'success') or (not ('data' in res)) or (not res['data']):
			return False
		return to_len(res['data']) > 0

	def create_attribute(self, attribute_code, backend_type, frontend_input, attribute_set_id = 4, frontend_label = None, entity_type_id = 4):
		all_query = list()
		eav_attribute_data = {
			'entity_type_id': 4,
			'attribute_code': attribute_code,
			'attribute_model': None,
			'backend_model': 'Magento\Eav\Model\Entity\Attribute\Backend\ArrayBackend' if frontend_input == self.OPTION_MULTISELECT else None,
			'backend_type': backend_type,
			'backend_table': None,
			'frontend_model': None,
			'frontend_input': frontend_input,
			'frontend_label': frontend_label,
			'frontend_class': None,
			'source_model': None,
			'is_required': 0,
			'is_user_defined': 1,
			'default_value': None,
			'is_unique': 0,
			'note': None,
		}
		eav_attribute_id = self.import_data_connector(self.create_insert_query_connector('eav_attribute', eav_attribute_data), 'attribute')
		if not eav_attribute_id:
			return False
		attribute_group_id = self.get_attribute_group_id(attribute_set_id)
		eav_entity_attribute_data = {
			'entity_type_id': entity_type_id,
			'attribute_set_id': attribute_set_id,
			'attribute_group_id': attribute_group_id,
			'attribute_id': eav_attribute_id,
			'sort_order': 0

		}
		all_query.append(self.create_insert_query_connector('eav_entity_attribute', eav_entity_attribute_data))

		catalog_eav_attribute_data = {
			'attribute_id': eav_attribute_id,
			'frontend_input_renderer': None,
			'is_global': 1,
			'is_visible': 1,
			'is_searchable': 1,
			'is_filterable': 1,
			'is_comparable': 1,
			'is_visible_on_front': 1,
			'is_html_allowed_on_front': 1,
			'is_used_for_price_rules': 0,
			'is_filterable_in_search': 1,
			'used_in_product_listing': 0,
			'used_for_sort_by': 0,
			'apply_to': None,
			'is_visible_in_advanced_search': 1,
			'position': 0,
			'is_wysiwyg_enabled': 0,
			'is_used_for_promo_rules': 0,
			'is_required_in_admin_store': 0,
			'is_used_in_grid': 0,
			'is_visible_in_grid': 0,
			'is_filterable_in_grid': 0,
			'search_weight': 1,
			'additional_data': None,
		}
		all_query.append(self.create_insert_query_connector('catalog_eav_attribute', catalog_eav_attribute_data))

		self.import_multiple_data_connector(all_query, 'attribute')
		return eav_attribute_id

	def get_attribute_group_id(self, attribute_set_id):
		if not attribute_set_id:
			return 7
		query = "SELECT * FROM _DBPRF_eav_attribute_group WHERE " + self.dict_to_where_condition(
			{'attribute_set_id': attribute_set_id, 'default_id': 1})
		res = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': query
			})
		})
		data = res.get('data', list())
		if to_len(data) > 0:
			return data[0]['attribute_group_id']
		return 7

	def check_option_exist(self, option_value, attribute_code):
		# option_value = self.replace_special_word(option_value)
		query = {
			'type': 'select',
			'query': "SELECT a.option_id FROM _DBPRF_eav_attribute_option_value as a, _DBPRF_eav_attribute_option as b, _DBPRF_eav_attribute as c  WHERE a.value = " + self.escape(
				option_value) + " AND a.option_id = b.option_id AND b.attribute_id = c.attribute_id AND c.attribute_code = " + self.escape(
				attribute_code) + ""
		}
		res = self.select_data_connector(query, 'check_option')
		try:
			option_id = res['data'][0]['option_id']
		except Exception:
			option_id = False
		return option_id

	def replace_special_word(self, value):
		value = to_str(value).replace('"', '\\"')
		value = to_str(value).replace("'", "\\'")
		return value

	def magento_serialize(self, obj):
		res = False
		try:
			if self.convert_version(self._notice['target']['config']['version'], 2) < 220:
				res = php_serialize(obj)
			else:
				res = json.dumps(obj)
		except Exception:
			res = False
		return res

	def get_default_language(self, stores):
		sort_order = 0
		if to_len(stores) > 0 and 'sort_order' in stores[0]:
			sort_order = stores[0]['sort_order']
		else:
			return 1
		default_lang = None
		for store in stores:
			if store['code'] == 'default':
				return store['store_id']
			if to_str(store['store_id']) != '0' and not default_lang:
				default_lang = store['store_id']
			if to_int(store['sort_order']) < to_int(sort_order):
				sort_order = store['sort_order']
		for store in stores:
			if to_int(store['sort_order'] == to_int(sort_order)):
				default_lang = store['store_id']
		return default_lang

	def get_all_store_select(self):
		select_store = self._notice['src']['languages_select'].copy()
		if '0' not in select_store:
			# select_store = list()
			select_store.append(0)
		return select_store

	def get_all_website_select(self):
		select_store = self._notice['src']['languages_select']
		all_website = list()
		for store_id in select_store:
			website_id = self._notice['src']['site'].get(store_id)
			if website_id:
				all_website.append(website_id)
		return all_website

	def add_construct_default(self, construct):
		construct['site_id'] = 1
		construct['language_id'] = self._notice['src']['language_default']
		return construct

	def get_categories_parent(self, parent_id):
		categories = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_category_entity WHERE entity_id = " + to_str(parent_id)
			})
		})
		if (not categories) or (categories['result'] != 'success'):
			return response_warning()
		categories_ext = self.get_categories_ext_export(categories)
		if (not categories_ext) or (categories_ext['result'] != 'success'):
			return response_warning()
		category = categories['data'][0]
		return self.convert_category_export(category, categories_ext)

	def import_category_parent(self, parent):
		parent_data = list()
		parent_exist = self.select_category_map(parent['id'])
		if parent_exist:
			for parent_row in parent_exist:
				res = response_success(parent_row['id_desc'])
				res['cate_path'] = parent_row['code_desc']
				res['value'] = parent_row['value']
				parent_data.append(res)
			return response_success(parent_data)
		parent_import = self.category_import(parent, None, None)
		if parent_import['result'] != 'success':
			return parent_import
		parent_import_data = parent_import['data']
		for parent_row_import in parent_import_data:
			if parent_row_import['result'] == 'success':
				self.after_category_import(parent_row_import['data'], parent, None, None)
		return parent_import

	def get_website_id_by_store_id_src(self, store_id):
		if to_int(store_id) == 0:
			return 0
		return self._notice['src'].get('store_site', dict()).get(to_str(store_id), 0)

	def get_website_id_by_store_id(self, store_id):
		if to_int(store_id) == 0:
			return 0
		return self._notice['target'].get('store_site', dict()).get(to_str(store_id), 0)

	def get_category_url_key(self, url_key, store_id, name, category_id = ''):
		if not url_key:
			url_key = self.generate_url_key(name)
		cur_url_key = url_key
		exist = False
		while self.check_exist_url_key(cur_url_key, store_id, 'category'):
			if category_id and not exist:
				cur_url_key = url_key + '-' + to_str(category_id)
			else:
				return url_key + '-' + to_str(to_int(time.time()))

			exist = True
		return cur_url_key

	def get_category_url_path(self, url_path, store_id, default = None):
		if not url_path:
			url_path = default
		if not url_path:
			return ''
		has_suffix = False
		no_suffix = ''
		suffix = ''
		if to_str(url_path).find('.') != -1:
			list_url_path = to_str(url_path).split('.')
			suffix = list_url_path[to_len(list_url_path) - 1]
			has_suffix = True
			no_suffix = to_str(url_path).replace('.' + suffix, '')
		cur_url_path = url_path
		index = 0
		while self.check_exist_url_path(cur_url_path, store_id, 'category'):
			index += 1
			if has_suffix:
				cur_url_path = no_suffix + '-' + to_str(index) + '.' + suffix
			else:
				cur_url_path = url_path + '-' + to_str(index)
		return cur_url_path

	def get_product_url_key(self, url_key, store_id, name, product_id = ''):
		if not url_key:
			url_key = self.generate_url_key(name)
		cur_url_key = url_key

		exist = False
		while self.check_exist_url_key(cur_url_key, store_id):
			if product_id and not exist:
				cur_url_key = url_key + '-' + to_str(product_id)
			else:
				return url_key + '-' + to_str(to_int(time.time()))
			exist = True
		return cur_url_key

	def generate_url_key(self, name):
		return self.convert_attribute_code(name)
		if not name:
			return ''
		name = to_str(name).lower()
		url_key = re.sub('[\'\"]', '', to_str(name))
		# url_key = re.sub('[^0-9a-z]', '-', url_key)
		url_key = self.generate_url(url_key)
		url_key = to_str(url_key).replace('å', 'a').replace('ö', 'o').replace('ő', 'o').replace('ű', 'u').replace('.', '_').replace('/', '_').replace(',', '_').replace(' ', '_')
		try:
			check_encode = chardet.detect(url_key.encode())
			if check_encode['encoding'] != 'ascii':
				result = self.parse_url(url_key)
		except Exception:
			self.log(name, 'generate_url')
			pass
		url_key = re.sub('[^A-Za-z0-9_\-\'\"/,:;]+', '', to_str(url_key))
		url_key = re.sub('[^A-Za-z0-9_ -]+', '-', to_str(url_key))
		url_key = to_str(url_key).strip(' -')
		url_key = to_str(url_key).replace(' ', '-').replace('_', '-')
		while to_str(url_key).find('--') != -1:
			url_key = to_str(url_key).replace('--', '-')
		return url_key

	def get_product_parent(self, parent_id):
		url_query = self.get_connector_url('query')
		result = response_success()
		product = self.get_connector_data(url_query, {
			'query': json.dumps({
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_product_entity WHERE entity_id = " + parent_id
			})
		})
		if (not product) or (product['result'] != 'success'):
			result['result'] = 'error'
			return result
		product_ext = self.get_products_ext_export(product)
		if (not product_ext) or (product_ext['result'] != 'success'):
			result['result'] = 'error'
			return result
		product = product['data'][0]
		return self.convert_product_export(product, product_ext)

	def delete_target_customer(self, customer_id):
		if not customer_id:
			return True
		queries_delete = {
			'customer_address_entity': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_address_entity WHERE parent_id = " + to_str(customer_id)
			},
			'customer_grid_flat': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_grid_flat WHERE entity_id = " + to_str(customer_id)
			},
			'customer_entity_decimal': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity_decimal WHERE entity_id = " + to_str(customer_id)
			},
			'customer_entity_text': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity_text WHERE entity_id = " + to_str(customer_id)
			},
			'customer_entity_varchar': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_entity_varchar WHERE entity_id = " + to_str(customer_id)
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
		self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries_delete)
		})
		return True

	def get_region_id_by_state_name(self, state_name):
		if not state_name:
			return 0
		query = "SELECT dcr.region_id FROM _DBPRF_directory_country_region as dcr WHERE dcr.default_name = " + self.escape(
			state_name)
		regions = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': query
			})
		})
		if not regions or regions['result'] != 'success':
			return 0
		if regions['data'] and to_len(regions['data']) > 0:
			return regions['data'][0]['region_id']
		return 0

	def get_order_state_by_order_status(self, order_status):
		status_state_data = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps({
				'status_state': {
					'type': 'select',
					'query': "SELECT * FROM _DBPRF_sales_order_status_state WHERE " + self.dict_to_where_condition(
						{'status': order_status})
				}
			})
		})
		if not status_state_data or status_state_data['result'] != 'success':
			return 'canceled'
		try:
			res = status_state_data['data']['status_state'][0]['state']
		except Exception:
			res = 'canceled'
		return res

	def get_name_store_view(self, store_id):
		if to_int(store_id) == 0:
			return 'Admin \n Default \n Admin'
		store_name = self._notice['target']['languages'].get(store_id)
		if not store_name:
			store_name = self._notice['target']['languages'].get(to_str(store_id))
		store_name = to_str(store_name).replace(' > ', '\n')
		if not store_name:
			return 'Admin \n Default \n Admin'
		if to_len(store_name) > 32:
			store_name = 'STORE VIEW #' + to_str(store_id)
		return store_name

	def delete_target_order(self, order_id):
		if not order_id:
			return True
		url_query = self.get_connector_url('query')
		order_ext_queries = {
			'invoice': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_sales_invoice WHERE order_id = ' + to_str(order_id),
			},
			'creditmemo': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_sales_creditmemo WHERE order_id = ' + to_str(order_id),
			},
			'shipment': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_sales_shipment WHERE order_id = ' + to_str(order_id),
			},
		}
		order_ext = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(order_ext_queries)
		})
		if not order_ext or order_ext['result'] != 'success':
			return True
		invoice_ids = duplicate_field_value_from_list(order_ext['data']['invoice'], 'entity_id')
		shipment_ids = duplicate_field_value_from_list(order_ext['data']['shipment'], 'entity_id')
		creditmemo_ids = duplicate_field_value_from_list(order_ext['data']['creditmemo'], 'entity_id')
		delete_queries = {
			'sales_order_item': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order_item WHERE order_id = " + to_str(order_id),
			},
			'sales_order_status_history': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order_status_history WHERE parent_id = " + to_str(order_id),
			},

			'sales_order_payment': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order_payment WHERE parent_id = " + to_str(order_id),
			},
			'sales_order_address': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order_address WHERE parent_id = " + to_str(order_id),
			},
			'sales_order_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order_grid WHERE entity_id = " + to_str(order_id),
			},
		}
		if shipment_ids:
			delete_queries['sales_shipment_item'] = {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_shipment_item WHERE parent_id IN " + self.list_to_in_condition(
					shipment_ids)
			}
		if creditmemo_ids:
			delete_queries['sales_creditmemo_item'] = {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_creditmemo_item WHERE parent_id IN " + self.list_to_in_condition(
					creditmemo_ids)
			}
		if invoice_ids:
			delete_queries['sales_invoice_item'] = {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_invoice_item WHERE parent_id IN " + self.list_to_in_condition(
					invoice_ids)
			}
		delete_queries_ext = {
			'sales_invoice_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_invoice_grid WHERE order_id = " + to_str(order_id),
			},
			'sales_invoice': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_invoice WHERE order_id = " + to_str(order_id),
			},
			'sales_shipment_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_shipment_grid WHERE order_id = " + to_str(order_id),
			},
			'sales_shipment': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_shipment WHERE order_id = " + to_str(order_id),
			},
			'sales_creditmemo_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_creditmemo_grid WHERE order_id = " + to_str(order_id),
			},
			'sales_creditmemo': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_creditmemo WHERE order_id = " + to_str(order_id),
			},
			'sales_order': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_order WHERE entity_id = " + to_str(order_id),
			},
		}
		delete_queries.update(delete_queries_ext)
		self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(delete_queries)
		})
		return True

	def get_map_store_view(self, src_store):
		if src_store is None:
			return 0
		# if to_int(src_store) == 0:
		# 	return 0
		return self._notice['map']['languages'].get(to_str(src_store), self._notice['map']['languages'].get(
			self._notice['src']['language_default'], 0))

	def create_attribute_code(self, attribute_name):
		attribute_name = self.convert_attribute_code(attribute_name)
		attribute_name = self.remove_special_char(attribute_name)
		attribute_name = to_str(attribute_name).replace('-', '_')
		return attribute_name

	def get_type_relation_product(self, src_type):
		target_type = {
			self.PRODUCT_RELATE: 1,
			self.PRODUCT_UPSELL: 4,
			self.PRODUCT_CROSS: 5,
		}
		return target_type.get(src_type, 1) if src_type else 1

	def get_option_type_by_src_type(self, src_type):
		types = {
			'select': 'drop_down',
			'text': 'field',
			'radio': 'radio',
			'checkbox': 'checkbox',
			'file': 'file',
			'textarea': 'area',
			'date': 'date',
			'time': 'time',
			'datetime': 'date_time',
			'textfield': 'field',
			'files': 'file'
		}
		return types[src_type] if src_type in types else 'drop_down'

	def get_region_id_from_state_code(self, state_code_src, country_code):
		region_id = 0
		if not state_code_src or not country_code:
			return region_id
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_directory_country_region WHERE code = '" + to_str(
				state_code_src) + "' AND country_id = '" + country_code + "'"
		}
		regions = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if regions and regions['data']:
			region_id = regions['data'][0]['region_id']
		return region_id

	def get_con_store_select_count(self):
		if not self._notice['support']['languages_select']:
			return ' 1'
		select_store = self._notice['src']['languages_select'].copy()
		src_store = self._notice['src']['languages'].copy()
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		if not unselect_store:
			return ' 1'
		select_store.append(0)
		where = ' store_id IN ' + self.list_to_in_condition(select_store) + ' '
		return where

	def get_con_website_select_count(self):
		if not self._notice['support']['languages_select']:
			return ' 1'
		select_store = self._notice['src']['languages_select'].copy()
		select_website = [self.get_website_id_by_store_id_src(item) for item in select_store]
		src_store = self._notice['src']['languages'].copy()
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		# unselect_website = [self.get_website_id_by_store_id(item,'src') for item in unselect_store]
		if not unselect_store:
			return ' 1'
		# select_store.append(0)
		where = ' website_id IN ' + self.list_to_in_condition(select_website) + ' '
		return where
	def get_con_store_select(self):
		if not self._notice['support']['languages_select']:
			return 1
		select_store = self._notice['src']['languages_select'].copy()
		src_store = self._notice['src']['languages'].copy()
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		if not unselect_store:
			return 1
		select_store.append(0)
		if to_len(select_store) >= to_len(unselect_store):
			where = ' store_id IN ' + self.list_to_in_condition(select_store) + ' '
		else:
			where = ' store_id NOT IN ' + self.list_to_in_condition(unselect_store) + ' '
		return where

	def detect_seo(self):
		return 'default_seo'

	def categories_default_seo(self, category, categories_ext):
		result = list()
		key_entity = 'entity_id'
		key_system = 'is_system'
		cat_desc = get_list_from_list_by_field(categories_ext['data']['url_rewrite'], key_entity, category['entity_id'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			if self._notice['support'].get('seo_301') and self._notice['config'].get('seo_301'):
				type_seo = self.SEO_301

			seo_cate = self.construct_seo_category()
			seo_cate['request_path'] = seo['request_path']
			seo_cate['store_id'] = seo['store_id']
			seo_cate['type'] = type_seo
			result.append(seo_cate)
		return result

	def products_default_seo(self, product, products_ext):
		result = list()
		key_entity = 'entity_id'
		key_system = 'is_system'
		cat_desc = get_list_from_list_by_field(products_ext['data']['url_rewrite'], key_entity, product['entity_id'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			if self._notice['support'].get('seo_301') and self._notice['config'].get('seo_301'):
				type_seo = self.SEO_301
			seo_product = self.construct_seo_product()
			seo_product['request_path'] = seo['request_path']
			seo_product['store_id'] = seo['store_id']
			seo_product['type'] = type_seo
			result.append(seo_product)
		return result

	def check_attribute_sync(self, src_attr, target_attr):
		if (src_attr.get('backend_type') == 'static') or (to_str(src_attr.get('is_user_defined')) == '0'):
			return True
		field_check = ['frontend_input', 'backend_type']
		for field in field_check:
			if (not (field in src_attr)) or (not (field in target_attr)) or (src_attr[field] != target_attr[field]):
				return False
		return True

	def check_sku_exist(self, sku):
		select = {
			'sku': sku,
		}
		product_data = self.select_data_connector(self.create_select_query_connector('catalog_product_entity', select))

		try:
			entity_id = product_data['data'][0]['entity_id']
		except Exception:
			entity_id = False
		return entity_id

	def construct_product(self):
		construct = super().construct_product()
		construct['thumbnail'] = self.construct_product_image()
		construct['group_prices'] = list()
		construct['small_image'] = self.construct_product_image()
		return construct

	def convert_image_in_description(self, description):
		if not description:
			return description
		match = re.findall(r"<img[^>]+>", to_str(description))
		links = list()
		if match:
			for img in match:
				img_src = re.findall(r"(src=[\"' ]{{(.*?)}}[\"' ])", to_str(img))
				if not img_src:
					continue
				img_src = img_src[0]
				links.append(img_src)
		for link in links:
			src = re.findall(r"((?:media|skin) url=[\"' ](.*?)[\"'])", link[1])
			if not src:
				continue
			src = src[0]
			new_src = self._cart_url.strip('/') + '/media/' + src[1].strip('/')
			description = to_str(description).replace(link[0], 'src="' + new_src + '"')
		return description

	def check_attribute_label_store_exist(self, attr_id, store_id = 0):
		query = self.create_select_query_connector('eav_attribute_label', {'attribute_id': attr_id, 'store_id': store_id})
		res = self.select_data_connector(query, 'attr_label')
		try:
			label = res['data'][0]['value']
		except Exception:
			label = False
		return label

	def check_option_value_store_exist(self, option_id, store_id = 0):
		query = self.create_select_query_connector('eav_attribute_option_value', {'option_id': option_id, 'store_id': store_id})
		res = self.select_data_connector(query, 'attr_label')
		try:
			label = res['data'][0]['value']
		except Exception:
			label = False
		return label

	def check_exist_url_cms(self, cms_type, url):
		if cms_type == self.TYPE_BLOG:
			cms_type = 'block'
		query = 'select * from _DBPRF_cms_' + cms_type + ' where identifier = ' + self.escape(url)
		url_data = self.select_data_connector({
			'type': 'select',
			'query': query,
		})

		if not url_data or url_data['result'] != 'success' or not url_data['data']:
			return False
		if to_len(url_data['data']) > 0:
			return self.check_exist_request_path(url)
		return False

	def get_page_layout(self, page_layout):
		if not page_layout:
			return ''
		layouts = {
			'1': 'one_',
			'2': 'two_',
			'3': 'three_',
		}
		for key, layout in layouts.items():
			page_layout = to_str(page_layout).replace(layout, key)
		page_layout = to_str(page_layout).replace('_', '-')
		return page_layout

	def get_layout_update_xml(self, layout_xml):
		if not layout_xml:
			return ''
		find_block = re.findall('\<block\>(.+?)\<\/block\>', to_str(layout_xml))
		if find_block:
			for block in find_block:
				layout_xml = to_str(layout_xml).replace('<block>' + block + '</block>', '<block>' + to_str(block).replace('_', '/') + '</block>')
		return layout_xml

	def get_data_default(self, data, field, store_id, need, store_default=0):
		data_def = get_row_value_from_list_by_field(data, field, store_id, need)
		if data_def:
			return data_def
		return get_row_value_from_list_by_field(data, field, store_default, need)

	def get_tax_customer(self):
		if self.tax_customer:
			return self.tax_customer
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_tax_class WHERE class_type = 'CUSTOMER' LIMIT 1 "
		}
		result = self.select_data_connector(query)
		if result['result'] == 'success' and result['data']:
			self.tax_customer = result['data'][0]['class_id']
			return self.tax_customer
		tax_class_data = {
			'class_name': 'default',
			'class_type': 'CUSTOMER',
		}
		self.tax_customer = self.import_data_connector(self.create_insert_query_connector('tax_class', tax_class_data), 'tax')
		return self.tax_customer

	def get_tax_rule(self):
		if self.tax_rule:
			return self.tax_rule
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_tax_calculation_rule WHERE code = 'default'"
		}
		result = self.select_data_connector(query)
		if result['result'] == 'success' and result['data']:
			self.tax_rule = result['data'][0]['tax_calculation_rule_id']
			return self.tax_rule
		tax_calculation_rule_data = {
			'code': 'default',
			'priority': 1,
			'position': 0,
			'calculate_subtotal': 0
		}
		self.tax_rule = self.import_data_connector(self.create_insert_query_connector('tax_calculation_rule', tax_calculation_rule_data), 'tax')
		return self.tax_rule

	def get_eav_attribute_product(self):
		if self.eav_attribute_product:
			return self.eav_attribute_product
		self.eav_attribute_product = list()
		eav_attribute = self.select_data_connector(self.create_select_query_connector('eav_attribute', {'entity_type_id': self._notice['src']['extends']['catalog_product']}))
		if eav_attribute['result'] == 'success' and eav_attribute['data']:
			self.eav_attribute_product = eav_attribute['data']
		return self.eav_attribute_product

	def get_catalog_eav_attribute(self):
		if self.catalog_eav_attribute:
			return self.catalog_eav_attribute
		eav_attribute_product = self.get_eav_attribute_product()
		eav_attribute_ids = duplicate_field_value_from_list(eav_attribute_product, 'attribute_id')
		eav_attribute_con = self.list_to_in_condition(eav_attribute_ids)
		query = {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_eav_attribute WHERE attribute_id IN " + eav_attribute_con,
			}
		catalog_eav_attribute = self.select_data_connector(query)
		if catalog_eav_attribute['result'] == 'success' and catalog_eav_attribute['data']:
			self.catalog_eav_attribute = catalog_eav_attribute['data']
		return self.catalog_eav_attribute
