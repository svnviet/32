import json
import copy
from xml.etree import ElementTree
from operator import itemgetter
from cartmigration.models.basecart import LeBasecart
from cartmigration.libs.utils import *

class LeCartMagento13(LeBasecart):
	TYPE_MAX_RELATE = 'relate'
	TYPE_MAX_GROUP = 'group'

	def __init__(self, data = None):
		super().__init__(data)
		self.attribute_mst_brands = None
		self.attribute_landing_page = None
		self.attribute_manufacturer = None
		self.attribute_category = None
		self.attribute_customer = None
		self.attribute_customer_address = None
		self.eav_attribute_product = None
		self.catalog_eav_attribute = None
		self.all_tax_rate = dict()

	def display_config_source(self):
		parent = super().display_config_source()
		if parent['result'] != 'success':
			return parent
		url_query = self.get_connector_url('query')
		path_config = ['shipping/origin/country_id', 'advanced/modules_disable_output/MST_Brands', 'tax/calculation/price_includes_tax', 'advanced/modules_disable_output/MageWorx_CustomOptions', 'cataloginventory/item_options/manage_stock']
		default_query = {
			'languages': {
				'type': 'select',
				'query': "select st.store_id, st.name, st.code, st.sort_order, sw.website_id,sw.name as website_name,"
				         "st.group_id,sg.root_category_id, sg.name as group_name "
				         " from _DBPRF_core_store as st "
				         "JOIN _DBPRF_core_website as sw on st.website_id = sw.website_id "
				         "JOIN _DBPRF_core_store_group as sg on st.group_id = sg.group_id "
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
			'config': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path IN " + self.list_to_in_condition(path_config)
			}
		}
		default_config = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(default_query)
		})
		if (not default_config) or (default_config['result'] != 'success'):
			return response_error()
		default_config_data = default_config['data']

		if default_config_data and default_config_data['languages'] and default_config_data['currencies'] and default_config_data['eav_entity_type']:
			self._notice['src']['language_default'] = self.get_default_language(default_config_data['languages'])
			self._notice['src']['currency_default'] = default_config_data['currencies'][0]['value'] if to_len(
				default_config_data) > 0 else 'USD'
			for eav_entity_type in default_config_data['eav_entity_type']:
				self._notice['src']['extends'][eav_entity_type['entity_type_code']] = eav_entity_type[
					'entity_type_id']
		else:
			return response_error('err default data')
		if default_config_data['config']:
			for config in default_config_data['config']:
				if config['path'] == 'advanced/modules_disable_output/MST_Brands' and to_str(config['value']) == '0':
					self._notice['src']['support']['mst_brands'] = True
				if config['path'] == 'advanced/modules_disable_output/MageWorx_CustomOptions' and to_str(config['value']) == '0':
					self._notice['src']['support']['custom_options'] = True
				if config['path'] == 'tax/calculation/price_includes_tax' and to_str(config['value']) == '1':
					self._notice['src']['support']['price_includes_tax'] = True
				if config['path'] == 'shipping/origin/country_id' and to_str(config['value']):
					self._notice['src']['config']['shipping_country'] = config['value']
				if config['path'] == 'cataloginventory/item_options/manage_stock' and to_str(config['value']) == '0':
					self._notice['src']['config']['no_manage_stock'] = True
		if self._notice['src']['support'].get('price_includes_tax') and not self._notice['src']['config'].get('shipping_country'):
			shipping_config = self.get_connector_data(self.get_connector_url('file'), {
				'files': json_encode(
					{
						'config': {
							'type': 'content',
							'path': 'app/code/core/Mage/Shipping/etc/config.xml'
						}
					}
				)
			})
			if shipping_config['result'] == 'success' and shipping_config['data'] and shipping_config['data'].get('config'):
				xml_data = shipping_config['data'].get('config')
				default_country = self.get_param_from_content_xml(xml_data, 'default/shipping/origin/country_id')
				self._notice['src']['config']['shipping_country'] = default_country if default_country else 'US'
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
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'general/locale/code'"
			},
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
			'stores': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_store WHERE code != 'admin' AND is_active = 1"
			}
		}
		config = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(config_queries)
		})
		if (not config) or (config['result'] != 'success'):
			return response_error("can't display config source")
		config_data = config['data']
		language_data = dict()
		storage_cat_data = dict()
		order_status_data = dict()
		site_data = dict()
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['status']] = order_status_row['label']
		order_state_data = dict()
		for order_state_row in config_data['orders_state']:
			order_state_data[order_state_row['status']] = order_state_row['state']
		self._notice['src']['order_state'] = order_state_data
		self._notice['src']['support']['order_state_map'] = True
		self._notice['src']['store_site'] = dict()
		for language_row in default_config_data['languages']:
			lang_id = language_row['store_id']
			self._notice['src']['site'][lang_id] = language_row['website_id']
			self._notice['src']['store_site'][lang_id] = language_row['website_id']
			if language_row['name']:
				language_data[lang_id] = language_row['website_name'] + ' > ' + language_row['group_name'] + ' > ' + language_row['name']
			else:
				language_data[lang_id] = language_row['website_name'] + ' > ' + language_row['group_name'] + ' > '
		if config_data and config_data['stores']:
			for store_row in config_data['stores']:
				site_data[store_row['store_id']] = store_row['name']
		else:
			site_data = {
				1: 'Default Shop',
			}
		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['customer_group_id']] = customer_group['customer_group_code']
		# self._notice['src']['site'] = site_data
		self._notice['src']['languages'] = language_data
		self._notice['src']['store_category'] = storage_cat_data
		self._notice['src']['order_status'] = order_status_data
		self._notice['src']['customer_group'] = customer_group_data
		self._notice['src']['config']['seo_module'] = self.get_list_seo()
		self._notice['src']['support']['country_map'] = False
		self._notice['src']['support']['languages_select'] = True
		self._notice['src']['support']['site_map'] = True
		self._notice['src']['support']['customer_group_map'] = True
		self._notice['src']['support']['attributes'] = True
		self._notice['src']['support']['pages'] = True
		# self._notice['src']['support']['blogs'] = True
		self._notice['src']['support']['coupons'] = True
		# self._notice['src']['support']['cartrules'] = True
		self._notice['src']['support']['pre_prd'] = True
		self._notice['src']['support']['add_new'] = True
		self._notice['src']['support']['pre_cus'] = True
		self._notice['src']['support']['pre_ord'] = True
		self._notice['src']['support']['ignore_image'] = True
		self._notice['src']['support']['img_des'] = True
		self._notice['src']['support']['seo'] = True
		self._notice['src']['support']['cus_pass'] = True
		self._notice['src']['support']['seo_301'] = True
		if self._notice['target']['cart_type'] == 'shopify':
			self._notice['src']['support']['multi_languages_select'] = True
		return response_success()

	def display_config_target(self):
		parent = super().display_config_source()
		if parent['result'] != 'success':
			return parent
		url_query = self.get_connector_url('query')
		default_query = {
			'languages': {
				'type': 'select',
				'query': "select st.store_id, st.name, st.code, st.sort_order, sw.website_id,sw.name as website_name,"
				         "st.group_id,sg.root_category_id, sg.name as group_name "
				         " from _DBPRF_core_store as st "
				         "JOIN _DBPRF_core_website as sw on st.website_id = sw.website_id "
				         "JOIN _DBPRF_core_store_group as sg on st.group_id = sg.group_id "
				         "WHERE st.code != 'admin'"
			},
			'currencies': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'currency/options/default'"
			},
			'eav_entity_type': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_eav_entity_type",
			}
		}
		default_config = self.select_multiple_data_connector(default_query, 'config_query')
		if (not default_config) or (default_config['result'] != 'success'):
			return response_error("Cannot get target configuration!", '#target-cart-url', 'Target Connector Error')
		default_config_data = default_config['data']
		if default_config_data and default_config_data['languages'] and default_config_data['currencies'] and default_config_data['eav_entity_type']:
			self._notice['target']['language_default'] = self.get_default_language(default_config_data['languages'])
			self._notice['target']['currency_default'] = default_config_data['currencies'][0]['value'] if to_len(
				default_config_data) > 0 else 'USD'
			for eav_entity_type in default_config_data['eav_entity_type']:
				self._notice['target']['extends'][eav_entity_type['entity_type_code']] = eav_entity_type['entity_type_id']
		else:
			return response_error('err default data')
		self._notice['target']['category_root'] = 1
		self._notice['target']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['target']['attributes'] = {
			1: 'Default Attribute',
		}
		config_queries = {
			'languages': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_config_data WHERE path = 'general/locale/code'"
			},
			'orders_status': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_order_status"
			},
			'customer_group': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_customer_group"
			},
			"category_root": {
				'type': 'select',
				'query': "SELECT a.entity_id, b.value FROM _DBPRF_catalog_category_entity a, "
				         "_DBPRF_catalog_category_entity_varchar b, _DBPRF_eav_attribute c "
				         "WHERE a.level = '1' AND "
				         "b.entity_id = a.entity_id AND "
				         "b.attribute_id = c.attribute_id AND "
				         "b.store_id = 0 AND "
				         "c.attribute_code = 'name' AND "
				         "c.entity_type_id = '" + self._notice['target']['extends']['catalog_category'] + "'"
			},
		}

		config = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(config_queries)
		})
		if (not config) or (config['result'] != 'success'):
			return response_error("Cannot get target configuration!", '#target-cart-url', 'Target Connector Error')

		config_data = config['data']
		language_data = dict()
		store_cat_data = dict()
		for language_row in default_config_data['languages']:
			lang_id = language_row['store_id']
			self._notice['target']['site'][lang_id] = language_row['website_id']
			language_data[lang_id] = language_row['website_name'] + ' > ' + language_row['group_name'] + ' > ' + language_row['name']
			store_cat_data[lang_id] = language_row['root_category_id']
		order_status_data = dict()
		for order_status_row in config_data['orders_status']:
			order_status_data[order_status_row['status']] = order_status_row['label']

		customer_group_data = dict()
		for customer_group in config_data['customer_group']:
			customer_group_data[customer_group['customer_group_id']] = customer_group['customer_group_code']

		category_root_data = dict()
		for category_root_row in config_data['category_root']:
			category_root_data[category_root_row['entity_id']] = category_root_row['value']

		if category_root_data:
			self._notice['target']['category_data'] = category_root_data

		self._notice['target']['languages'] = language_data
		self._notice['target']['store_category'] = store_cat_data
		self._notice['target']['order_status'] = order_status_data
		self._notice['target']['customer_group'] = customer_group_data
		self._notice['target']['support']['site_map'] = False
		self._notice['target']['support']['languages_select'] = True
		self._notice['target']['support']['country_map'] = False
		self._notice['target']['support']['customer_group_map'] = True
		self._notice['target']['support']['attributes'] = True
		self._notice['target']['support']['ignore_image'] = True
		self._notice['target']['support']['img_des'] = True
		self._notice['target']['support']['seo'] = True
		self._notice['target']['support']['cus_pass'] = True
		self._notice['target']['support']['pages'] = True
		self._notice['target']['support']['blocks'] = True
		self._notice['target']['support']['coupons'] = True
		self._notice['target']['support']['cartrules'] = True
		return response_success()

	def display_confirm_target(self):
		self._notice['target']['clear']['function'] = 'clear_target_taxes'
		self._notice['target']['clear_demo']['function'] = 'clear_target_products_demo'
		return response_success()

	def get_query_display_import_source(self, update = False):
		compare_condition = ' > '
		if update:
			compare_condition = ' <= '
		select_store = self._notice['src']['languages_select']
		website_id = 0
		if select_store:
			website_id = self._notice['src']['site'].get(select_store[0], 0)
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = store_id_con + ' AND '
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

			'products': {
				'type': 'select',
				'query': "SELECT COUNT(1) as count FROM _DBPRF_catalog_product_entity WHERE entity_id NOT IN (SELECT product_id FROM _DBPRF_catalog_product_super_link WHERE parent_id NOT IN (SELECT entity_id FROM _DBPRF_catalog_product_entity WHERE type_id ='grouped' ))"
				         "AND entity_id IN (SELECT product_id FROM _DBPRF_catalog_product_website where" + self.get_con_website_select_count() + ") AND entity_id " + compare_condition + to_str(self._notice['process']['products']['id_src']),
			},
			'customers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_customer_entity WHERE " + self.get_con_website_select_count() + " AND entity_id " + compare_condition + to_str(
					self._notice['process']['customers']['id_src']),
			},
			'orders': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_sales_flat_order WHERE " + self.get_con_store_select_count() + " AND entity_id " + compare_condition + to_str(
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
			# 'blogs': {
			# 	'type': 'select',
			# 	'query': 'SELECT COUNT(1) AS count FROM _DBPRF_blog WHERE post_id ' + compare_condition + to_str(
			# 		self._notice['process']['blogs']['id_src']),
			# },
			'coupons': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM _DBPRF_salesrule as s left join _DBPRF_salesrule_coupon as sc on s.rule_id = sc.rule_id and sc.is_primary = 1 WHERE "
				         "(exists (select 1 from _DBPRF_salesrule_website as website where (website.website_id in ('" + to_str(website_id) + "')) and (s.rule_id = website.rule_id))) AND s.rule_id " + compare_condition + to_str(
					self._notice['process']['coupons']['id_src']),
			},

		}
		if self._notice['src']['support'].get('mst_brands'):
			queries['manufacturers'] = {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_mst_brands WHERE entity_id ' + compare_condition + to_str(self._notice['process']['manufacturers']['id_src'])
			}
		if self._notice['src']['support']['blogs']:
			queries['blogs'] = {
				'type': 'select',
				'query': 'SELECT COUNT(1) AS count FROM _DBPRF_aw_blog WHERE post_id ' + compare_condition + to_str(
					self._notice['process']['blogs']['id_src']),
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

	# TODO: CLEAR
	def clear_target_taxes(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_manufacturers',
		}
		if not self._notice['config']['taxes']:
			self._notice['target']['clear'] = next_clear
			return self._notice['target']['clear']
		tables = [
			'tax_class',
			'tax_calculation_rate',
			'tax_calculation_rule',
			'tax_calculation',
		]
		for table in tables:
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query',
					'query': "DELETE FROM `_DBPRF_" + table + "`"
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_manufacturers(self):
		self._notice['target']['clear']['result'] = 'process'
		self._notice['target']['clear']['function'] = 'clear_target_categories'
		self._notice['target']['clear']['table_index'] = 0

		return self._notice['target']['clear']

	def clear_target_categories(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_products',
		}
		if not self._notice['config']['categories']:
			self._notice['target']['clear'] = next_clear
			return next_clear
		tables = [
			'catalog_category_entity_datetime',
			'catalog_category_entity_decimal',
			'catalog_category_entity_int',
			'catalog_category_entity_text',
			'catalog_category_entity_varchar',
			'catalog_category_entity',
			'core_url_rewrite',
		]

		root_category_ids = list()

		root_category_data = self._notice['target']['category_data']
		for key, value in root_category_data.items():
			root_category_ids.append(key)
		root_category_ids.append(1)

		root_ids_in_condition = self.list_to_in_condition(root_category_ids)
		for table in tables:
			where = ' WHERE entity_id NOT IN ' + root_ids_in_condition

			if table == 'core_url_rewrite':
				where = ' WHERE category_id IS NOT NULL'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		self._notice['target']['clear'] = next_clear

		return next_clear

	def clear_target_products(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_customers',
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['products']:
			return next_clear
		tables = [
			'catalog_category_product',
			'cataloginventory_stock_status',
			'cataloginventory_stock_item',
			'catalog_product_website',
			'catalog_product_super_attribute_label',
			'catalog_product_super_link',
			'catalog_product_relation',
			'catalog_product_super_attribute',
			'catalog_product_super_attribute_pricing'
			'catalog_product_option_price',
			'catalog_product_option_title',
			'catalog_product_option_type_price',
			'catalog_product_option_type_title',
			'catalog_product_option_type_value',
			'catalog_product_option',
			'catalog_product_entity_media_gallery',
			'catalog_product_entity_media_gallery_value',
			'catalog_product_entity_tier_price',
			'catalog_product_entity_group_price',
			'catalog_product_entity_varchar',
			'catalog_product_entity_datetime',
			'catalog_product_entity_decimal',
			'catalog_product_entity_text',
			'catalog_product_entity_int',
			'catalog_product_link',
			'catalog_product_bundle_selection',
			'catalog_product_bundle_option_value',
			'catalog_product_bundle_option',
			'core_url_rewrite',
			'catalog_product_entity',
		]
		for table in tables:
			where = ''

			if table == 'core_url_rewrite':
				where = ' WHERE product_id IS NOT NULL'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

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
			'sales_flat_creditmemo',
			'sales_flat_creditmemo_grid',
			'sales_flat_creditmemo_item',
			'sales_flat_order_grid',
			'sales_flat_shipment',
			'sales_flat_shipment_grid',
			'sales_flat_shipment_item',
			'sales_flat_order_address',
			'sales_flat_order_payment',
			'sales_flat_order_status_history',
			'sales_flat_order_item',
			'sales_flat_invoice_item',
			'sales_flat_invoice_grid',
			'sales_flat_invoice',
			'sales_flat_order',
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
			'review_store',
			'review_detail',
			'rating_option_vote',
			'review_entity_summary',
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
		return next_clear
		if not self._notice['config'].get('blogs'):
			return next_clear
		tables = [
			'cms_block_store',
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
				where = ' WHERE class_id IN ' + tax_id_con
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
			category_id_map = duplicate_field_value_from_list(categories['data'], 'id_desc')
			category_ids = list(set(category_ids + category_id_map))
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
			'core_url_rewrite',
			'catalog_category_product'
		]

		for table in tables:
			where = ' WHERE entity_id IN ' + category_id_con

			if table == 'core_url_rewrite':
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
			'catalog_product_super_attribute_pricing',
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
			'core_url_rewrite',
			'catalog_product_entity',
		]
		table_key_product_id = ['cataloginventory_stock_status', 'cataloginventory_stock_item',
		                        'catalog_product_website', 'catalog_product_link',
		                        'catalog_url_rewrite_product_category', 'catalog_product_option',
		                        'catalog_category_product', 'core_url_rewrite']
		table_option = ['catalog_product_option_price',
		                'catalog_product_option_title',
		                'catalog_product_option_type_price',
		                'catalog_product_option_type_title',
		                'catalog_product_option_type_value', ]
		for table in tables:
			where = ' WHERE entity_id IN ' + product_id_con

			if table in table_key_product_id:
				where = ' WHERE product_id IN ' + product_id_con
			if table == 'catalog_product_relation':
				where = ' WHERE parent_id IN ' + product_id_con + ' OR child_id IN ' + product_id_con

			if table == 'catalog_product_bundle_option':
				where = ' WHERE parent_id IN ' + product_id_con

			if table == 'catalog_product_super_attribute_pricing':
				where = ' WHERE product_super_attribute_id IN (SELECT product_super_attribute_id FROM _DBPRF_catalog_product_super_attribute WHERE product_id IN' + product_id_con + ')'

			if table in ['catalog_product_bundle_option_value', 'catalog_product_bundle_selection']:
				where = ' WHERE option_id IN (SELECT option_id FROM _DBPRF_catalog_product_bundle_option WHERE parent_id IN' + product_id_con + ')'

			if table in table_option:
				option_id_con = " (SELECT option_id FROM _DBPRF_catalog_product_option WHERE product_id IN " + product_id_con + ")"
				where = ' WHERE option_id IN ' + option_id_con
				if table in ['catalog_product_option_type_title', 'catalog_product_option_type_price']:
					where = " WHERE option_type_id IN (SELECT option_type_id FROM _DBPRF_catalog_product_option_type_value " + where + ")"
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

	def clear_target_reviews_demo(self):
		next_clear = {
			'result': 'process',
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
		review_id_con = self.list_to_in_condition(review_ids)
		tables = [
			'review_store',
			'review_detail',
			'rating_option_vote',
			'review_entity_summary',
			'review',
		]
		for table in tables:
			where = ' WHERE review_id IN ' + review_id_con
			if table == 'review_entity_summary':
				where = ' entity_pk_value IN (SELECT entity_pk_value FROM _DBPRF_review WHERE review_id IN ' + review_id_con + ')'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_pages_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_blocks_demo',
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
		page_id_con = self.list_to_in_condition(page_ids)
		tables = [
			'url_rewrite',
			'cms_page_store',
			'cms_page',
		]
		for table in tables:
			where = ' WHERE page_id IN ' + page_id_con
			if table == 'url_rewrite':
				where = ' WHERE entity_type like "cms-page" AND entity_id IN ' + page_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_blocks_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_coupons_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['blocks']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_BLOCK
		}
		blocks = self.select_obj(TABLE_MAP, where)
		block_ids = list()
		if blocks['result'] == 'success':
			block_ids = duplicate_field_value_from_list(blocks['data'], 'id_desc')
		if not block_ids:
			return next_clear
		block_id_con = self.list_to_in_condition(block_ids)
		tables = [
			'cms_block_store',
			'cms_block',
		]
		for table in tables:
			where = " WHERE block_id IN " + block_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_coupons_demo(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_cartrules_demo',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config']['coupons']:
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_COUPON
		}
		rules = self.select_obj(TABLE_MAP, where)
		rule_ids = list()
		if rules['result'] == 'success':
			rule_ids = duplicate_field_value_from_list(rules['data'], 'id_desc')
		if not rule_ids:
			return next_clear
		rule_id_con = self.list_to_in_condition(rule_ids)
		tables = [
			'salesrule_label',
			'salesrule_customer_group',
			'salesrule_customer',
			'salesrule_coupon_usage',
			'salesrule_coupon',
			'salesrule',
		]
		for table in tables:
			where = " WHERE rule_id IN " + rule_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_cartrules_demo(self):
		next_clear = {
			'result': 'success',
			'function': '',
		}
		self._notice['target']['clear_demo'] = next_clear
		if not self._notice['config'].get('cartrules'):
			return next_clear
		where = {
			'migration_id': self._migration_id,
			'type': self.TYPE_CART_RULE
		}
		cartrules = self.select_obj(TABLE_MAP, where)
		cartrule_ids = list()
		if cartrules['result'] == 'success':
			cartrule_ids = duplicate_field_value_from_list(cartrules['data'], 'id_desc')
		if not cartrule_ids:
			return next_clear
		cartrule_id_con = self.list_to_in_condition(cartrule_ids)
		tables = [
			'catalogrule_website',
			'catalogrule_customer_group',
			'catalogrule_product_price',
			'catalogrule_product',
			'catalogrule_group_website',
			'catalogrule',
		]
		for table in tables:
			where = " WHERE rule_id IN " + cartrule_id_con
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Could not empty table " + table, 'clear')
				continue
		return next_clear

	def prepare_import_source(self):
		return response_success()

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
			'query': "SELECT * FROM _DBPRF_tax_class WHERE class_type = 'PRODUCT' AND class_id > " + to_str(id_src) + " ORDER BY class_id ASC LIMIT " + to_str(limit)
		}
		taxes = self.select_data_connector(query, 'taxes')
		if not taxes or taxes['result'] != 'success':
			return response_error()
		return taxes

	def get_taxes_ext_export(self, taxes):
		url_query = self.get_connector_url('query')
		class_ids = duplicate_field_value_from_list(taxes['data'], 'class_id')
		classid_in_query = self.list_to_in_condition(class_ids)
		taxes_ext_queries = {
			'tax_calculation': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_calculation WHERE product_tax_class_id IN " + classid_in_query
			}
		}
		taxes_ext = self.select_multiple_data_connector(taxes_ext_queries, 'taxes')

		if not taxes_ext or taxes_ext['result'] != 'success':
			return response_error('can not get taxes ext')

		tax_calculation_rate_ids = duplicate_field_value_from_list(taxes_ext['data'][
			                                                           'tax_calculation'], 'tax_calculation_rate_id')
		tax_calculation_rate_id_query = self.list_to_in_condition(tax_calculation_rate_ids)

		tax_calculation_rule_ids = duplicate_field_value_from_list(taxes_ext['data'][
			                                                           'tax_calculation'], 'tax_calculation_rule_id')
		tax_calculation_rule_id_query = self.list_to_in_condition(tax_calculation_rule_ids)
		taxes_ext_rel_queries = {
			'tax_calculation_rate': {
				'type': 'select',
				'query': "SELECT tcr.*, dcr.*, tcr.code as tcr_code FROM _DBPRF_tax_calculation_rate as tcr LEFT JOIN _DBPRF_directory_country_region as dcr ON tcr.tax_region_id = dcr.region_id WHERE tax_calculation_rate_id IN " + tax_calculation_rate_id_query
			},
			'tax_calculation_rule': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_tax_calculation_rule WHERE tax_calculation_rule_id IN " + tax_calculation_rule_id_query
			},
		}

		taxes_ext_rel = self.select_multiple_data_connector(taxes_ext_rel_queries, 'taxes')

		if not taxes_ext_rel or taxes_ext_rel['result'] != 'success':
			return response_error('can not get taxes_ext_rel')
		taxes_ext = self.sync_connector_object(taxes_ext, taxes_ext_rel)
		return taxes_ext

	def convert_tax_export(self, tax, taxes_ext):
		tax_products = list()
		tax_customers = list()
		tax_zones = list()

		tax_product = self.construct_tax_product()
		tax_product['id'] = tax['class_id']
		tax_product['code'] = None
		tax_product['name'] = tax['class_name']
		tax_products.append(tax_product)

		tax_calculations = get_list_from_list_by_field(taxes_ext['data']['tax_calculation'], 'product_tax_class_id', tax['class_id'])
		if tax_calculations:
			for tax_calculation in tax_calculations:
				tax_zone_rate = self.construct_tax_zone_rate()
				tax_calculation_rate = get_row_from_list_by_field(taxes_ext['data']['tax_calculation_rate'], 'tax_calculation_rate_id', tax_calculation['tax_calculation_rate_id'])
				tax_calculation_rule = get_row_from_list_by_field(taxes_ext['data']['tax_calculation_rule'], 'tax_calculation_rule_id', tax_calculation['tax_calculation_rule_id'])
				tax_zone_rate['id'] = tax_calculation['tax_calculation_rate_id']
				tax_zone_rate['region_id'] = tax_calculation_rate['tax_region_id']
				tax_zone_rate['name'] = get_value_by_key_in_dict(tax_calculation_rate, 'tcr_code', tax['class_name'])
				tax_zone_rate['rate'] = tax_calculation_rate['rate']
				tax_zone_rate['priority'] = tax_calculation_rule.get('priority', 1)

				tax_zone_state = self.construct_tax_zone_state()

				tax_zone_state['state_code'] = get_value_by_key_in_dict(tax_calculation_rate, 'code', '')

				tax_zone_country = self.construct_tax_zone_country()

				tax_zone_country['country_code'] = tax_calculation_rate['tax_country_id']

				tax_zone = self.construct_tax_zone()

				tax_zone['id'] = tax_calculation['tax_calculation_rate_id']
				tax_zone['name'] = tax['class_name']
				tax_zone['country'] = tax_zone_country
				tax_zone['state'] = tax_zone_state
				tax_zone['rate'] = tax_zone_rate
				tax_zones.append(tax_zone)
		tax_data = self.construct_tax()
		tax_data['id'] = tax['class_id']
		tax_data['name'] = tax['class_name']
		tax_data['tax_products'] = tax_products
		tax_data['tax_zones'] = tax_zones
		return response_success(tax_data)

	def get_tax_id_import(self, convert, tax, taxes_ext):
		return tax['class_id']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id']) else False

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
		customer_class_ids = dict()
		tax_calculation_rate_ids = dict()
		tax_calculation_rule_ids = dict()
		for tax_zone in convert['tax_zones']:
			tax_calculation_rate_id = self.get_map_field_by_src(self.TYPE_TAX_RATE, tax_zone['id'])
			if tax_calculation_rate_id:
				tax_calculation_rate_ids[tax_zone['id']] = tax_calculation_rate_id
				continue
			tax_region_id = self.get_region_id_from_state_code(tax_zone['state']['code'], tax_zone['country']['country_code'])
			tax_calculation_rate_data = {
				'tax_country_id': tax_zone['country']['country_code'],
				'tax_region_id': tax_region_id,
				'tax_postcode': get_value_by_key_in_dict(tax_zone, 'postcode', '*'),
				'code': convert['name'],
				'rate': tax_zone['rate']['rate'],
			}
			tax_calculation_rate_id = self.import_data_connector(
				self.create_insert_query_connector('tax_calculation_rate', tax_calculation_rate_data), 'tax')
			if not tax_calculation_rate_id:
				continue
			tax_calculation_rate_ids[tax_zone['id']] = tax_calculation_rate_id
			self.insert_map(self.TYPE_TAX_RATE, tax_zone['id'], tax_calculation_rate_id, tax_zone['code'])

		for tax_customer in convert['tax_customers']:
			customer_tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_CUSTOMER, tax_customer['id'])
			if customer_tax_class_id:
				customer_class_ids[tax_customer['id']] = customer_tax_class_id
				continue
			tax_class_data = {
				'class_name': tax_customer['code'],
				'class_type': 'CUSTOMER',
			}
			customer_tax_class_id = self.import_data_connector(
				self.create_insert_query_connector('tax_class', tax_class_data), 'tax')
			if not customer_tax_class_id:
				continue
			customer_class_ids[tax_customer['id']] = customer_tax_class_id
			self.insert_map(self.TYPE_TAX_CUSTOMER, tax_customer['id'], customer_tax_class_id, tax_customer['code'])

		if 'tax_rules' in convert:
			for tax_rule in convert['tax_rules']:
				tax_calculation_rule_id = self.get_map_field_by_src('tax_rule', tax_rule['id'])
				if tax_calculation_rule_id:
					tax_calculation_rule_ids[tax_rule['id']] = tax_calculation_rule_id
					continue
				tax_calculation_rule_data = {
					'code': tax_rule.get('code'),
					'priority': 0,
					'position': 0,
					'calculate_subtotal': 0
				}
				tax_calculation_rule_id = self.import_data_connector(
					self.create_insert_query_connector('tax_calculation_rule', tax_calculation_rule_data), 'tax')
				if not tax_calculation_rule_id:
					continue
				tax_calculation_rule_ids[tax_rule['id']] = tax_calculation_rule_id
				self.insert_map('tax_rule', tax_rule['id'], tax_calculation_rule_id, tax_rule['code'])
		else:
			tax_calculation_rule_id = self.get_map_field_by_src('tax_rule', 1)
			if tax_calculation_rule_id:
				tax_calculation_rule_ids[1] = tax_calculation_rule_id
			else:
				tax_calculation_rule_data = {
					'code': "Default",
					'priority': 0,
					'position': 0,
					'calculate_subtotal': 0
				}
				tax_calculation_rule_id = self.import_data_connector(
					self.create_insert_query_connector('tax_calculation_rule', tax_calculation_rule_data), 'tax')

				tax_calculation_rule_ids[1] = tax_calculation_rule_id
				self.insert_map('tax_rule', 1, tax_calculation_rule_id)
		if 'tax_calculation' in convert and isinstance(convert['tax_calculation'], list):
			for item in convert['tax_calculation']:
				if (item['customer_tax_class_id'] in customer_class_ids) and (
						item['tax_calculation_rate_id'] in tax_calculation_rate_ids) and (
						item['tax_calculation_rule_id'] in tax_calculation_rule_ids):
					tax_calculation_data = {
						'tax_calculation_rate_id': tax_calculation_rate_ids[item['tax_calculation_rate_id']],
						'tax_calculation_rule_id': tax_calculation_rule_ids[item['tax_calculation_rule_id']],
						'customer_tax_class_id': customer_class_ids[item['customer_tax_class_id']],
						'product_tax_class_id': tax_id,
					}
					self.import_data_connector(
						self.create_insert_query_connector('tax_calculation', tax_calculation_data), 'tax')
		else:
			for customer_class_id in customer_class_ids:
				for tax_calculation_rate_id in tax_calculation_rate_ids:
					if tax_calculation_rule_ids:
						for tax_calculation_rule_id in tax_calculation_rule_ids:
							tax_calculation_data = {
								'tax_calculation_rate_id': tax_calculation_rate_id,
								'tax_calculation_rule_id': tax_calculation_rule_id,
								'customer_tax_class_id': customer_class_id,
								'product_tax_class_id': tax_id,
							}
							self.import_data_connector(
								self.create_insert_query_connector('tax_calculation', tax_calculation_data), 'tax')
		return response_success()

	def addition_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	# TODO: MANUFACTURER
	def prepare_manufacturers_import(self):
		return self

	def prepare_manufacturers_export(self):
		return self

	def get_mst_brands_main_export(self):
		id_src = self._notice['process']['manufacturers']['id_src']
		limit = self._notice['setting']['manufacturers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_mst_brands WHERE entity_id > " + to_str(id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit)
		}
		manufacturers = self.select_data_connector(query, 'manufacturers')

		if not manufacturers or manufacturers['result'] != 'success':
			return response_error()
		return manufacturers

	def get_manufacturers_main_export(self):
		if self._notice['src']['support'].get('mst_brands'):
			return self.get_mst_brands_main_export()
		id_src = self._notice['process']['manufacturers']['id_src']
		limit = self._notice['setting']['manufacturers']
		query = {
			'type': 'select',
			'query': "SELECT eao.* FROM _DBPRF_eav_attribute as ea LEFT JOIN _DBPRF_eav_attribute_option as eao ON "
			         "ea.attribute_id = eao.attribute_id WHERE ea.attribute_code = 'manufacturer' AND eao.option_id > "
			         "" + to_str(
				id_src) + " ORDER BY eao.option_id ASC LIMIT " + to_str(limit)
		}
		manufacturers = self.select_data_connector(query, 'manufacturers')

		if not manufacturers or manufacturers['result'] != 'success':
			return response_error()
		return manufacturers

	def get_mst_brands_ext_export(self, brands):
		brand_ids = duplicate_field_value_from_list(brands['data'], 'entity_id')
		brand_id_query = self.list_to_in_condition(brand_ids)
		brands_ext_queries = {
			'mst_brands_varchar': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_mst_brands_varchar WHERE entity_id IN " + brand_id_query
			},
			'mst_brands_int': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_mst_brands_int WHERE entity_id IN " + brand_id_query
			},
			'mst_brands_text': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_mst_brands_text WHERE entity_id IN " + brand_id_query
			}
		}

		brands_ext = self.select_multiple_data_connector(brands_ext_queries, 'manufacturers')

		if not brands_ext or (brands_ext['result'] != 'success'):
			return response_error()
		brands_ext_rel_queries = {
		}
		if brands_ext_rel_queries:
			brands_ext_rel = self.select_multiple_data_connector(brands_ext_rel_queries, 'categories')

			if not brands_ext_rel or brands_ext_rel['result'] != 'success':
				return response_error()
			brands_ext = self.sync_connector_object(brands_ext, brands_ext_rel)
		return brands_ext

	def get_manufacturers_ext_export(self, manufacturers):
		if self._notice['src']['support'].get('mst_brands'):
			return self.get_mst_brands_ext_export(manufacturers)
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
			manufacturers_ext_rel = self.select_multiple_data_connector(manufacturers_ext_rel_queries, 'manufacturer')

			if not manufacturers_ext_rel or manufacturers_ext_rel['result'] != 'success':
				return response_error()
			manufacturers_ext = self.sync_connector_object(manufacturers_ext, manufacturers_ext_rel)
		return manufacturers_ext

	def convert_mst_brand_export(self, brand, brands_ext):
		brand_data = self.construct_manufacturer()
		eav_attribute = self.get_attribute_mst_brands()
		if not eav_attribute:
			return response_error('attr is empty')
		brand_data['id'] = brand['entity_id']
		entity_varchar = get_list_from_list_by_field(brands_ext['data']['mst_brands_varchar'], 'entity_id', brand['entity_id'])
		entity_text = get_list_from_list_by_field(brands_ext['data']['mst_brands_text'], 'entity_id', brand['entity_id'])
		entity_int = get_list_from_list_by_field(brands_ext['data']['mst_brands_int'], 'entity_id', brand['entity_id'])

		descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('description'))
		description_def = get_row_value_from_list_by_field(descriptions, 'store_id', 0, 'value')
		images = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('image'))
		image_def_path = get_row_value_from_list_by_field(images, 'store_id', 0, 'value')
		names = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('title'))
		name_def = get_row_value_from_list_by_field(names, 'store_id', 0, 'value')
		url_key = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_key'], 'value')
		brand_data['name'] = name_def
		brand_data['created_at'] = brand['created_at']
		brand_data['updated_at'] = brand['updated_at']
		brand_data['description'] = description_def
		brand_data['url'] = url_key
		brand_data['status'] = True if to_int(brand['is_active']) == 1 else False
		if image_def_path:
			brand_data['thumb_image']['url'] = self.get_url_suffix('media/brands/image')
			brand_data['thumb_image']['path'] = to_str(brand['entity_id']) + '/' + image_def_path
		for language_id, label in self._notice['src']['languages'].items():
			name_lang = get_row_value_from_list_by_field(names, 'store_id', language_id, 'value')
			description_lang = get_row_value_from_list_by_field(descriptions, 'store_id', language_id, 'value')
			if not name_lang and not description_lang:
				continue
			brand_language_data = self.construct_category_lang()
			brand_language_data['name'] = name_lang if name_lang else brand_data['name']
			brand_language_data['description'] = description_lang if description_lang else brand_data['description']
			brand_data['languages'][language_id] = brand_language_data
		return response_success(brand_data)

	def convert_manufacturer_export(self, manufacturer, manufacturers_ext):
		if self._notice['src']['support'].get('mst_brands'):
			return self.convert_mst_brand_export(manufacturer, manufacturers_ext)
		manufacturer_data = self.construct_manufacturer()
		manufacturer_data['id'] = manufacturer['option_id']
		manufacturer_desc = get_list_from_list_by_field(manufacturers_ext['data']['eav_attribute_option_value'], 'option_id', manufacturer['option_id'])
		manufacturer_data['name'] = get_row_value_from_list_by_field(manufacturer_desc, 'store_id', 0, 'value')
		return response_success(manufacturer_data)

	def get_mst_brand_id_import(self, convert, manufacturer, manufacturers_ext):
		return manufacturer['entity_id']

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		if self._notice['src']['support'].get('mst_brands'):
			return self.get_mst_brand_id_import(convert, manufacturer, manufacturers_ext)
		return manufacturer['option_id']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id'], convert['code']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def get_manufacturer_attribute(self):
		if self.attribute_manufacturer:
			return self.attribute_manufacturer
		product_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['target']['extends']['catalog_product'] + " and attribute_code = 'manufacturer'"
			},
		}
		product_eav_attribute = self.select_multiple_data_connector(product_eav_attribute_queries)
		try:
			self.attribute_manufacturer = product_eav_attribute['data']['eav_attribute'][0]['attribute_id']
		except Exception:
			self.attribute_manufacturer = self.create_attribute('manufacturer', 'int', 'select', 4, 'Manufacturer')
		return self.attribute_manufacturer

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		attribute_id = self.get_manufacturer_attribute()
		if not attribute_id:
			response_error()
		manufacturer_id = self.check_option_exist(convert['name'], 'manufacturer')
		if not manufacturer_id:
			eav_attribute_option_data = {
				'attribute_id': attribute_id,
				'sort_order': 0
			}
			manufacturer_id = self.import_manufacturer_data_connector(
				self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data), True,
				convert['id'])
			if not manufacturer_id:
				return response_error()
			self.insert_map(self.TYPE_MANUFACTURER, convert['id'], manufacturer_id)
			eav_attribute_option_value_data = {
				'option_id': manufacturer_id,
				'store_id': 0,
				'value': convert['name'],
			}
			eav_attribute_option_value_id = self.import_manufacturer_data_connector(
				self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
		else:
			self.insert_map(self.TYPE_MANUFACTURER, convert['id'], manufacturer_id)
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
		category_ids = duplicate_field_value_from_list(categories['data'], 'entity_id')
		category_id_query = self.list_to_in_condition(category_ids)
		categories_ext_queries = {
			'catalog_category_entity_varchar': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_varchar WHERE entity_id IN " + category_id_query
			},
			'catalog_category_entity_text': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_text WHERE entity_id IN " + category_id_query
			},
			'catalog_category_entity_int': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_int WHERE entity_id IN " + category_id_query
			},
			'catalog_category_entity_decimal': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_decimal WHERE entity_id IN " + category_id_query
			},
			'catalog_category_entity_datetime': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_category_entity_datetime WHERE entity_id IN " + category_id_query
			},
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['src']['extends'][
					'catalog_category']
			},
			'core_store_group': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_core_store_group WHERE 1"
			},
			'core_store': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_core_store WHERE code != 'admin'"
			}
		}

		categories_ext_queries['url_rewrite'] = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_core_url_rewrite WHERE product_id IS NULL AND is_system = 1 AND category_id IN " +
			         category_id_query
		}
		categories_ext = self.select_multiple_data_connector(categories_ext_queries, 'categories')

		if not categories_ext or (categories_ext['result'] != 'success'):
			return response_error()
		categories_ext_rel_queries = {
		}
		if categories_ext_rel_queries:
			categories_ext_rel = self.select_multiple_data_connector(categories_ext_rel_queries, 'categories')

			if not categories_ext_rel or categories_ext_rel['result'] != 'success':
				return response_error()
			categories_ext = self.sync_connector_object(categories_ext, categories_ext_rel)
		return categories_ext

	def convert_category_export(self, category, categories_ext):
		category['level'] = to_len(category['path'].split('/')) - 1
		category_data = self.construct_category()
		parent = self.construct_category_parent()
		category_data = self.add_construct_default(category_data)
		parent = self.add_construct_default(parent)
		code_parent = ''
		parent['level'] = 1
		if category['parent_id'] and to_int(category['level']) > 2:
			if to_int(category['parent_id']) in self.cat_parent:
				self.log_primary('categories', 'parent', category['cat_id'])
				return response_error()
			self.cat_parent.append(to_int(category['parent_id']))
			parent['id'] = category['parent_id']
			parent_data = self.get_categories_parent(category['parent_id'])
			if parent_data['result'] == 'success' and parent_data['data']:
				parent = parent_data['data']
				code_parent = parent_data['url_key'] if 'url_key' in parent_data else ''
		store_data = list()
		if category['parent_id'] and to_int(category['level']) == 2:
			websites = get_list_from_list_by_field(categories_ext['data']['core_store_group'], 'root_category_id', category['parent_id'])
			if websites:
				for website in websites:
					stores = get_list_from_list_by_field(categories_ext['data']['core_store'], 'root_category_id', website['website_id'])
					store_data = list(set(store_data + duplicate_field_value_from_list(stores, 'store_id')))
		if 'store_ids' in parent:
			store_data = parent['store_ids']
		if not store_data:
			store_data = duplicate_field_value_from_list(categories_ext['data']['core_store'], 'store_id')

		eav_attribute = dict()
		for row in categories_ext['data']['eav_attribute']:
			eav_attribute[row['attribute_code']] = row['attribute_id']
		entity_varchar = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_varchar'], 'entity_id', category['entity_id'])
		entity_text = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_text'], 'entity_id', category['entity_id'])
		entity_int = get_list_from_list_by_field(categories_ext['data']['catalog_category_entity_int'], 'entity_id', category['entity_id'])

		language_default = self._notice['src']['language_default']
		is_active = get_list_from_list_by_field(entity_int, 'attribute_id', eav_attribute['is_active'])
		is_active_def = self.get_data_default(is_active, 'store_id', language_default, 'value', 0)
		include_in_menu = get_list_from_list_by_field(entity_int, 'attribute_id', eav_attribute['include_in_menu'])
		include_in_menu_def = self.get_data_default(include_in_menu, 'store_id', language_default, 'value', 0)
		images = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['image'])
		image_def_path = self.get_data_default(images, 'store_id', language_default, 'value', 0)
		imagesthumb = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('thumbnail'))
		image_def_path_thumb = self.get_data_default(imagesthumb, 'store_id', language_default, 'value', 0)
		category_data['id'] = category['entity_id']
		# category_data['code'] = url_key
		category_data['store_ids'] = store_data
		category_data['level'] = to_int(parent['level']) + 1
		category_data['parent'] = parent
		category_data['active'] = True if to_int(is_active_def) == 1 else False
		if image_def_path and image_def_path != 'no_selection':
			category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_category'])
			category_data['thumb_image']['path'] = image_def_path
		else:
			if image_def_path_thumb:
				category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_category'])
				category_data['thumb_image']['path'] = image_def_path_thumb
		category_data['sort_order'] = 1
		category_data['created_at'] = category['created_at']
		category_data['updated_at'] = category['updated_at']
		category_data['category'] = category
		category_data['categories_ext'] = categories_ext
		category_data['include_in_menu'] = to_int(include_in_menu_def)

		names = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('name'))
		name_def = self.get_data_default(names, 'store_id', language_default, 'value', 0)
		descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('description'))
		description_def = self.get_data_default(descriptions, 'store_id', language_default, 'value', 0)
		meta_titles = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('meta_title'))
		meta_title_def = self.get_data_default(meta_titles, 'store_id', language_default, 'value', 0)
		meta_keywords = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('meta_keywords'))
		meta_keywords_def = self.get_data_default(meta_keywords, 'store_id', language_default, 'value', 0)
		meta_descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('meta_description'))
		meta_description_def = self.get_data_default(meta_descriptions, 'store_id', language_default, 'value', 0)
		url_key = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('url_key'))
		url_key_def = self.get_data_default(url_key, 'store_id', language_default, 'value', 0)
		category_data['name'] = name_def if name_def else ''
		category_data['url_key'] = url_key_def if url_key_def else ''
		category_data['description'] = self.convert_image_in_description(description_def) if description_def else ''
		category_data['meta_title'] = meta_title_def if meta_title_def else ''
		category_data['meta_keyword'] = meta_keywords_def if meta_keywords_def else ''
		category_data['meta_description'] = meta_description_def if meta_description_def else ''

		for language_id, label in self._notice['src']['languages'].items():
			category_language_data = self.construct_category_lang()
			name_lang = get_row_value_from_list_by_field(names, 'store_id', language_id, 'value')
			description_lang = get_row_value_from_list_by_field(descriptions, 'store_id', language_id, 'value')
			meta_title_lang = get_row_value_from_list_by_field(meta_titles, 'store_id', language_id, 'value')
			meta_keyword_lang = get_row_value_from_list_by_field(meta_keywords, 'store_id', language_id, 'value')
			meta_description_lang = get_row_value_from_list_by_field(meta_descriptions, 'store_id', language_id, 'value')
			url_key_lang = get_row_value_from_list_by_field(url_key, 'store_id', language_id, 'value')
			category_language_data['name'] = name_lang if name_lang else category_data['name']
			category_language_data['url_key'] = url_key_lang if url_key_lang else category_data['url_key']
			category_language_data['description'] = description_lang if description_lang else category_data['description']
			category_language_data['meta_title'] = meta_title_lang if meta_title_lang else category_data['meta_title']
			category_language_data['meta_keyword'] = meta_keyword_lang if meta_keyword_lang else category_data['meta_keyword']
			category_language_data['meta_description'] = meta_description_lang if meta_description_lang else category_data['meta_description']
			category_data['languages'][language_id] = category_language_data

		detect_seo = self.detect_seo()
		category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)

		return response_success(category_data)

	def get_category_id_import(self, convert, category, categories_ext):
		return category['entity_id']

	def check_category_import(self, convert, category, categories_ext):
		return True if self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id']) else False

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def import_category_parent(self, parent):
		parent_data = list()
		parent_exist = self.select_category_map(parent['id'])
		if parent_exist:
			for parent_row in parent_exist:
				res = response_success(parent_row['id_desc'])
				res['cate_path'] = parent_row['code_desc']
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

	def category_import(self, convert, category, categories_ext):
		parent_data = list()
		# if
		if convert['parent'] and (convert['parent']['id'] != convert['id']) and (convert['parent']['id'] or convert['parent']['code']):
			parent_import = self.import_category_parent(convert['parent'])
			if parent_import['result'] != 'success':
				return response_warning('Could not import parent')
			parent_import_data = parent_import['data']
			for parent_row in parent_import_data:
				row = {
					'data': parent_row['data'],
					'cate_path': parent_row['cate_path']
				}
				parent_data.append(row)
		else:
			if convert['parent']['id'] in self._notice['map']['category_data']:
				parent_ids = self._notice['map']['category_data'][convert['parent']['id']]
				for parent_id in parent_ids:
					row = {
						'data': parent_id,
						'cate_path': '1/' + parent_id
					}
					parent_data.append(row)
			else:
				for key, root_cate_id in self._notice['target']['category_data'].items():
					row = {
						'data': root_cate_id,
						'cate_path': '1/' + to_str(key),
						'value': '',
					}
					parent_data.append(row)
		# endif
		response = list()
		for parent_row in parent_data:
			parent_id = parent_row['data']
			cate_path = parent_row['cate_path']
			category_data = {
				'attribute_set_id': 3,
				'entity_type_id': self._notice['target']['extends']['catalog_category'],
				'parent_id': parent_id,
				'created_at': convert['created_at'] if convert['created_at'] else get_current_time(),
				'updated_at': convert['updated_at'] if convert['updated_at'] else get_current_time(),
				'path': cate_path,
				'position': convert.get('category', {}).get('position', 0),
				'level': convert.get('level', 0),
				'children_count': convert.get('category', {}).get('children_count', 0)
			}
			category_id = self.import_category_data_connector(
				self.create_insert_query_connector('catalog_category_entity', category_data), True, convert['id'])
			if not category_id:
				return response_warning(self.warning_import_entity(self.TYPE_CATEGORY, convert['id']))
			update_path = self.import_category_data_connector(
				self.create_update_query_connector('catalog_category_entity',
				                                   {'path': cate_path + '/' + to_str(category_id)},
				                                   {'entity_id': category_id}), False)
			if not update_path:
				return response_error(self.warning_import_entity(self.TYPE_CATEGORY, convert['id']))
			self.insert_map(self.TYPE_CATEGORY, convert['id'], category_id, convert['code'],
			                cate_path + '/' + to_str(category_id))
			response_row = response_success(category_id)
			response_row['cate_path'] = cate_path + '/' + to_str(category_id)
			response.append(response_row)
		return response_success(response)

	def after_category_import(self, category_id, convert, category, categories_ext):
		all_queries = list()
		category_eav_attribute_data = self.get_attribute_category()
		if not category_eav_attribute_data:
			return response_error()
		url_image = self.get_connector_url('image')
		image_name = None
		# if
		if convert['thumb_image']['url'] and convert['thumb_image']['path']:
			# if
			if (not ('ignore_image' in self._notice['config'])) or (not self._notice['config']['ignore_image']):
				image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
				image_name = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_magento_image_path(image_process['path']) + os.path.basename(image_process['path']), self._notice['target']['config']['image_category']))
				if image_name:
					image_name = self.remove_prefix_path(image_name, self._notice['target']['config']['image_category'])

			# endif
			# else
			else:
				image_name = convert['thumb_image']['path']
		# endelse
		# endif

		thumbnail_name = None
		# if
		if convert.get('thumbnail') and convert['thumbnail']['url'] and convert['thumbnail']['path']:
			# if
			if (not ('ignore_image' in self._notice['config'])) or (not self._notice['config']['ignore_image']):
				image_process = self.process_image_before_import(convert['thumbnail']['url'], convert['thumbnail']['path'])
				thumbnail_name = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_magento_image_path(image_process['path']) + os.path.basename(image_process['path']), self._notice['target']['config']['image_category']))
				if thumbnail_name:
					thumbnail_name = self.remove_prefix_path(thumbnail_name, self._notice['target']['config']['image_category'])

			# endif
			# else
			else:
				thumbnail_name = convert['thumbnail']['path']

		if thumbnail_name and 'thumbnail' not in category_eav_attribute_data:
			attribute_thumbnail = self.create_attribute('thumbnail', 'varchar', 'image', 3, 'Thumbnail', 3)
			if attribute_thumbnail:
				category_eav_attribute_data['thumbnail'] = dict()
				category_eav_attribute_data['thumbnail']['attribute_id'] = attribute_thumbnail
				category_eav_attribute_data['thumbnail']['backend_type'] = 'varchar'

		if self._notice['config']['recent'] or self._notice['config']['add_new']:
			if convert.get('products'):
				for value in convert['products']:
					product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, value['id'])
					if not product_id:
						continue
					catalog_category_product_data = {
						'category_id': category_id,
						'product_id': product_id,
						'position': get_value_by_key_in_dict(value, 'position', 1),
					}
					all_queries.append(
						self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))

		cate_url_key = self.get_category_url_key(convert.get('url_key'), 0, convert['name'])
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
			'is_active': 1 if convert['active'] else 0,
			'landing_page': None,
			'level': None,
			'meta_description': convert.get('meta_description'),
			'meta_keywords': convert.get('meta_keywords'),
			'meta_title': convert.get('meta_title'),
			'name': self.strip_html_tag(convert['name']),
			'description': self.change_img_src_in_text(convert.get('description')),
			'display_mode': convert.get('display_mode', 'PRODUCTS'),
			'page_layout': None,
			'path': None,
			'path_in_store': None,
			'position': None,
			'url_key': cate_url_key if cate_url_key else None,
			'url_path': cate_url_path,
			'is_anchor': convert.get('is_anchor', 1),
		}
		if thumbnail_name:
			insert_attribute_data['thumbnail'] = thumbnail_name
		# for
		for key1, value1 in category_eav_attribute_data.items():

			# for
			for key2, value2 in insert_attribute_data.items():
				if key1 == key2:
					if key2 != 'include_in_menu' and not value2:
						continue
					category_attr_data = {
						'attribute_id': value1['attribute_id'],
						'store_id': 0,
						'entity_id': category_id,
						'value': value2,
						'entity_type_id': self._notice['target']['extends']['catalog_category'],
					}
					query = self.create_insert_query_connector('catalog_category_entity_' + value1['backend_type'],
					                                           category_attr_data)
					all_queries.append(
						self.create_insert_query_connector('catalog_category_entity_' + value1['backend_type'],
						                                   category_attr_data))
		# endfor

		# endfoar

		# if
		if convert['languages']:

			# for
			for language_id, language_data in convert['languages'].items():
				insert_attribute_data = {
					'meta_description': language_data.get('meta_description'),
					'meta_keywords': language_data.get('meta_keywords'),
					'meta_title': language_data.get('meta_title'),
					'name': self.strip_html_tag(language_data.get('name')),
					'description': self.change_img_src_in_text(language_data.get('description')),
					'display_mode': language_data.get('display_mode', 'PRODUCTS'),
					'url_key': get_value_by_key_in_dict(insert_attribute_data, 'url_key', ''),
					'url_path': get_value_by_key_in_dict(insert_attribute_data, 'url_path', ''),
					'is_anchor': language_data.get('is_anchor', 1),
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
								value2 = self.get_category_url_key(value2, store_id, language_data['name'])
							if key2 == 'url_path':
								value2 = self.get_category_url_path(value2, store_id)
							if not value2:
								continue
							category_attr_data = {
								'attribute_id': value1['attribute_id'],
								'store_id': store_id,
								'entity_id': category_id,
								'value': value2,
								'entity_type_id': self._notice['target']['extends']['catalog_category'],
							}
							all_queries.append(
								self.create_insert_query_connector('catalog_category_entity_' + value1['backend_type'],
								                                   category_attr_data))
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
						'store_id': store_id,
						'id_path': 'category/' + to_str(category_id),
						'request_path': seo_default,
						'target_path': 'catalog/category/view/id/' + to_str(category_id),
						'is_system': 1,
						'options': None,
						'description': None,
						'category_id': category_id,
						'product_id': None,
					}
					self.import_category_data_connector(self.create_insert_query_connector('core_url_rewrite', url_rewrite_data))

		is_default = False
		if (self._notice['config']['seo'] or seo_301) and 'seo' in convert:
			if seo_301:
				is_default = True
			for rewrite in convert['seo']:
				path = rewrite['request_path']
				if not path:
					continue
				default = True if rewrite['default'] and not is_default else False
				if default:
					is_default = True
				store_id = self.get_map_store_view(rewrite.get('store_id', 0))
				path = self.get_request_path(path, store_id, 'category')
				url_rewrite_data = {
					'store_id': store_id,
					'id_path': 'category/' + to_str(category_id),
					'request_path': path,
					'target_path': 'catalog/category/view/id/' + to_str(category_id) if not seo_301 else seo_default,
					'is_system': 1,
					'options': None,
					'description': None,
					'category_id': category_id,
					'product_id': None,
				}
				self.import_category_data_connector(self.create_insert_query_connector('core_url_rewrite', url_rewrite_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'category')
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	# TODO: ATTRIBUTES
	def prepare_attributes_import(self):
		return self

	def prepare_attributes_export(self):
		return self

	def get_attributes_main_export(self):
		id_src = self._notice['process']['attributes']['id_src']
		limit = self._notice['setting']['attributes']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_eav_attribute WHERE "
			         " entity_type_id = '" + self._notice['src']['extends'][
				         'catalog_product'] + "'  AND attribute_id > " + to_str(
				id_src) + " ORDER BY attribute_id ASC LIMIT " + to_str(limit)
		}
		attributes = self.select_data_connector(query, 'attributes')
		if not attributes or attributes['result'] != 'success':
			return response_error()
		return attributes

	def get_attributes_ext_export(self, attributes):
		url_query = self.get_connector_url('query')
		attribute_ids = duplicate_field_value_from_list(attributes['data'], 'attribute_id')
		attribute_con = self.list_to_in_condition(attribute_ids)
		attribute_queries = {
			'eav_entity_attribute': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_eav_entity_attribute WHERE attribute_id IN ' + attribute_con
			},
			'eav_attribute_option': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_eav_attribute_option WHERE attribute_id IN ' + attribute_con
			},
			'eav_attribute_label': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_eav_attribute_label WHERE attribute_id IN ' + attribute_con
			},
			'catalog_eav_attribute': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_catalog_eav_attribute WHERE attribute_id IN ' + attribute_con
			}
		}
		attribute_ext = self.select_multiple_data_connector(attribute_queries, 'attributes')
		if not attribute_ext or attribute_ext['result'] != 'success':
			return response_error()
		eav_attribute_group_ids = duplicate_field_value_from_list(attribute_ext['data']['eav_entity_attribute'],
		                                                          'attribute_group_id')
		eav_attribute_group_con = self.list_to_in_condition(eav_attribute_group_ids)
		option_ids = duplicate_field_value_from_list(attribute_ext['data']['eav_attribute_option'], 'option_id')
		option_id_con = self.list_to_in_condition(option_ids)
		attribute_rel_ext_query = {
			'eav_attribute_option_value': {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_eav_attribute_option_value WHERE option_id IN ' + option_id_con
			},
			'attribute_group': {
				'type': 'select',
				'query': "SELECT eg.*, es.attribute_set_name FROM _DBPRF_eav_attribute_group AS eg JOIN "
				         "_DBPRF_eav_attribute_set AS es"
				         " ON eg.attribute_set_id = es.attribute_set_id WHERE es.attribute_set_name IS NOT NULL AND es.attribute_set_name !='' AND eg.attribute_group_id IN " +
				         eav_attribute_group_con
			}
		}
		attribute_rel_ext = self.select_multiple_data_connector(attribute_rel_ext_query, 'attributes')

		if not attribute_rel_ext or attribute_rel_ext['result'] != 'success':
			return response_error()
		attribute_ext = self.sync_connector_object(attribute_ext, attribute_rel_ext)
		return attribute_ext

	def convert_attribute_export(self, attribute, attributes_ext):
		attribute_data = dict()
		attribute_data['id'] = attribute['attribute_id']
		attribute_data['code'] = attribute['attribute_code'] if attribute['attribute_code'] != 'options' else 'options_lit'
		attribute_data['entity_type_id'] = attribute['entity_type_id']
		attribute_data['backend_type'] = attribute['backend_type']
		attribute_data['frontend_input'] = attribute['frontend_input'] if attribute['frontend_input'] != 'videogallery' else 'text'
		attribute_data['frontend_label'] = attribute['frontend_label']
		attribute_data['is_required'] = attribute['is_required']
		attribute_data['is_user_defined'] = attribute['is_user_defined']
		attribute_data['is_unique'] = attribute['is_unique']
		attribute_data['source_model'] = attribute['source_model']
		attribute_data['is_boolean'] = True if attribute['source_model'] == 'eav/entity_attribute_source_boolean' else False

		attribute_data['default_value'] = attribute['default_value']
		attribute_label = get_list_from_list_by_field(attributes_ext['data']['eav_attribute_label'], 'attribute_id', attribute['attribute_id'])
		labels = dict()
		for label in attribute_label:
			if label['store_id'] not in self._notice['map']['languages']:
				continue
			labels[label['store_id']] = label['value']
		attribute_data['labels'] = labels
		catalog_eav_attribute = get_row_from_list_by_field(attributes_ext['data']['catalog_eav_attribute'], 'attribute_id', attribute['attribute_id'])
		if not catalog_eav_attribute:
			catalog_eav_attribute = dict()
		attribute_data['apply_to'] = get_row_value_from_list_by_field(attributes_ext['data']['catalog_eav_attribute'],
		                                                              'attribute_id', attribute['attribute_id'],
		                                                              'apply_to')
		attribute_data['catalog_eav_attribute'] = catalog_eav_attribute
		eav_attribute_option = get_list_from_list_by_field(attributes_ext['data']['eav_attribute_option'],
		                                                   'attribute_id', attribute['attribute_id'])
		attribute_option_data = dict()
		for option in eav_attribute_option:
			option_data = dict()
			option_value = get_list_from_list_by_field(attributes_ext['data']['eav_attribute_option_value'],
			                                           'option_id', option['option_id'])
			default_option = get_row_value_from_list_by_field(option_value, 'store_id', 0, 'value')
			if (not default_option) and to_len(option_value) > 0:
				default_option = option_value[0]['value']
			option_data['value'] = default_option
			option_data['languages'] = dict()
			for option_row in option_value:
				if to_int(option_row['store_id']) != 0:
					option_data['languages'][option_row['store_id']] = option_row['value']
			attribute_option_data[option['option_id']] = option_data

		attribute_data['option_data'] = attribute_option_data

		attribute_group = get_list_from_list_by_field(attributes_ext['data']['eav_entity_attribute'], 'attribute_id',
		                                              attribute['attribute_id'])
		attribute_group_ids = duplicate_field_value_from_list(attribute_group, 'attribute_group_id')
		attribute_set_group = get_list_from_list_by_field(attributes_ext['data']['attribute_group'],
		                                                  'attribute_group_id', attribute_group_ids)
		attribute_group_data = list()
		for attribute_set_group_row in attribute_set_group:
			group_set_data = dict()
			group_set_data['group_name'] = attribute_set_group_row['attribute_group_name']
			group_set_data['set_name'] = attribute_set_group_row['attribute_set_name']
			group_set_data['attribute_set_id'] = attribute_set_group_row['attribute_set_id']
			attribute_group_data.append(group_set_data)
		attribute_data['attribute_group_set'] = attribute_group_data
		return response_success(attribute_data)

	def get_attribute_id_import(self, convert, attribute, attributes_ext):
		return attribute['attribute_id']

	def check_attribute_import(self, convert, attribute, attributes_ext):
		return True if self.get_map_field_by_src(self.TYPE_ATTR, convert['id'], convert['code']) else False

	def router_attribute_import(self, convert, attribute, attributes_ext):
		return response_success('attribute_import')

	def before_attribute_import(self, convert, attribute, attributes_ext):
		return response_success()

	def attribute_import(self, convert, attribute, attributes_ext):
		attribute_src = {
			'entity_type_id': 4,
			'attribute_code': convert['code'],
			'attribute_model': None,
			'backend_model': None,
			'backend_type': convert['backend_type'],
			'backend_table': None,
			'frontend_model': None,
			'frontend_input': convert['frontend_input'],
			'frontend_label': convert['frontend_label'],
			'frontend_class': None,
			'source_model': convert.get('source_model'),
			'is_required': convert.get('is_required', 0),
			'is_user_defined': convert['is_user_defined'],
			'default_value': convert.get('default_value'),
			'is_unique': convert.get('is_unique', 0),
			'note': None,
		}
		attribute_value = {
			'src': attribute_src
		}
		query = "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['target']['extends'][
			'catalog_product'] + " AND (attribute_code = '" + convert['code'] + "' OR attribute_code = 'le_" + convert[
			        'code'] + "')"
		res = self.select_data_connector({
			'type': 'select',
			'query': query,
		})

		attribute_exist = False
		attribute_exist_data = dict()
		if res and res['data'] and res['result'] == 'success' and to_len(res['data']):
			attribute_exist = True
			if to_len(res['data']) > 1:
				for attribute_desc in res['data']:
					src_code = 'le_' + convert['code']
					if src_code == attribute_desc['attribute_code']:
						attribute_value['target'] = attribute_desc
						self.insert_map(self.TYPE_ATTR, convert['id'], attribute_desc['attribute_id'], convert['code'], attribute_desc['attribute_code'], json.dumps(attribute_value))
						return response_success(attribute_desc['attribute_id'])
				return response_warning()
			attribute_exist_data = res['data'][0]
		rename = False
		if attribute_exist:
			check_sync = self.check_attribute_sync(convert, attribute_exist_data)
			if check_sync:
				attribute_value['target'] = attribute_exist_data
				self.insert_map(self.TYPE_ATTR, convert['id'], attribute_exist_data['attribute_id'], convert['code'], attribute_exist_data['attribute_code'], json.dumps(attribute_value))
				return response_success(attribute_exist_data['attribute_id'])
			rename = True
		backend_type = convert['backend_type']
		if convert['frontend_input'] == 'multiselect':
			backend_type = 'varchar'
		if convert['frontend_input'] == 'select':
			backend_type = 'int'
		frontend_input = convert['frontend_input']
		if frontend_input == 'select' and convert.get('is_boolean'):
			frontend_input = 'boolean'
		eav_attribute_data = {
			'entity_type_id': 4,
			'attribute_code': 'le_' + convert['code'] if rename else convert['code'],
			'attribute_model': None,
			'backend_model': None,
			'backend_type': backend_type,
			'backend_table': None,
			'frontend_model': None,
			'frontend_input': frontend_input,
			'frontend_label': convert['frontend_label'],
			'frontend_class': None,
			'source_model': 'eav/entity_attribute_source_boolean' if convert.get('is_boolean') else None,
			'is_required': convert.get('is_required', 0),
			'is_user_defined': 1,
			'default_value': convert.get('default_value'),
			'is_unique': convert.get('is_unique', 0),
			'note': None,
		}
		attribute_id = self.import_attribute_data_connector(
			self.create_insert_query_connector('eav_attribute', eav_attribute_data))
		if not attribute_id:
			return response_warning()
		eav_attribute_data['attribute_id'] = attribute_id
		attribute_value['target'] = eav_attribute_data

		self.insert_map(self.TYPE_ATTR, convert['id'], attribute_id, convert['code'], eav_attribute_data['attribute_code'], json.dumps(attribute_value))

		catalog_eav_attribute = convert['catalog_eav_attribute']
		catalog_eav_attribute_data = {
			'attribute_id': attribute_id,
			'frontend_input_renderer': get_value_by_key_in_dict(catalog_eav_attribute, 'frontend_input_renderer'),
			'is_global': get_value_by_key_in_dict(catalog_eav_attribute, 'is_global', 1),
			'is_visible': get_value_by_key_in_dict(catalog_eav_attribute, 'is_visible', 1),
			'is_searchable': get_value_by_key_in_dict(catalog_eav_attribute, 'is_searchable', 1),
			'is_filterable': get_value_by_key_in_dict(catalog_eav_attribute, 'is_filterable', 1),
			'is_comparable': get_value_by_key_in_dict(catalog_eav_attribute, 'is_comparable', 1),

			'is_visible_on_front': get_value_by_key_in_dict(catalog_eav_attribute, 'is_visible_on_front', 1),
			'is_html_allowed_on_front': get_value_by_key_in_dict(catalog_eav_attribute, 'is_html_allowed_on_front', 1),
			'is_used_for_price_rules': get_value_by_key_in_dict(catalog_eav_attribute, 'is_used_for_price_rules', 0),
			'is_filterable_in_search': get_value_by_key_in_dict(catalog_eav_attribute, 'is_filterable_in_search', 1),
			'used_in_product_listing': get_value_by_key_in_dict(catalog_eav_attribute, 'used_in_product_listing', 0),
			'used_for_sort_by': get_value_by_key_in_dict(catalog_eav_attribute, 'used_for_sort_by', 0),
			'is_configurable': get_value_by_key_in_dict(catalog_eav_attribute, 'is_configurable', 1),
			'apply_to': get_value_by_key_in_dict(catalog_eav_attribute, 'apply_to', 0),
			'is_visible_in_advanced_search': get_value_by_key_in_dict(catalog_eav_attribute, 'is_visible_in_advanced_search', 0),
			'position': get_value_by_key_in_dict(catalog_eav_attribute, 'position', 0),
			'is_wysiwyg_enabled': get_value_by_key_in_dict(catalog_eav_attribute, 'is_wysiwyg_enabled', 0),
			'is_used_for_promo_rules': get_value_by_key_in_dict(catalog_eav_attribute, 'is_used_for_promo_rules', 0),
		}
		self.import_attribute_data_connector(self.create_insert_query_connector('catalog_eav_attribute', catalog_eav_attribute_data))

		return response_success(attribute_id)

	def after_attribute_import(self, attribute_id, convert, attribute, attributes_ext):
		attribute_id = to_str(attribute_id)
		all_queries = list()
		url_query = self.get_connector_url('query')
		for index, attribute_group_set in enumerate(convert['attribute_group_set']):
			attribute_group_set['group_name'] = attribute_group_set['group_name']
			convert['attribute_group_set'][index] = attribute_group_set
		attribute_group_set = convert['attribute_group_set']
		attribute_set_name = duplicate_field_value_from_list(attribute_group_set, 'set_name')
		attribute_group_name = duplicate_field_value_from_list(attribute_group_set, 'group_name')
		attribute_set_name = list(map(lambda x: to_str(x) + '_src' if to_str(x).lower() != 'default' else x, attribute_set_name))
		attribute_set_query = 'SELECT * FROM _DBPRF_eav_attribute_set WHERE entity_type_id = "' + to_str(self._notice['target']['extends']['catalog_product']) + '"'
		attribute_set = self.select_data_connector({
			'type': 'select',
			'query': attribute_set_query
		})
		attribute_set_target_name = list()
		if attribute_set and attribute_set['result'] == 'success':
			attribute_set_target_name = duplicate_field_value_from_list(attribute_set['data'], 'attribute_set_name')
		for name in attribute_set_name:
			if name not in attribute_set_target_name:
				self.create_attribute_set(name)
		if attribute_set_name and attribute_group_name:
			attribute_set_name_con = ''
			for row in attribute_set_name:
				if attribute_set_name_con:
					attribute_set_name_con += ", "
				attribute_set_name_con += "'" + row + "'"
			attribute_set_name_con = '(' + attribute_set_name_con + ')'
			attribute_group_name_con = ''
			for row in attribute_group_name:
				if attribute_group_name_con:
					attribute_group_name_con += ", "
				attribute_group_name_con += "'" + row + "'"
			attribute_group_name_con = '(' + attribute_group_name_con + ')'

			queries = {
				'eav_attribute_set': {
					'type': 'select',
					'query': 'SELECT * FROM _DBPRF_eav_attribute_set WHERE entity_type_id = "' +
					         self._notice['target']['extends'][
						         'catalog_product'] + '" AND attribute_set_name IN ' + attribute_set_name_con
				},
				'attribute_group': {
					'type': 'select',
					'query': 'SELECT eg.*, es.attribute_set_name FROM _DBPRF_eav_attribute_group AS eg JOIN _DBPRF_eav_attribute_set AS es'
					         ' ON eg.attribute_set_id = es.attribute_set_id WHERE es.attribute_set_name IN ' + attribute_set_name_con + ' AND eg.attribute_group_name IN ' + attribute_group_name_con
				},
				'eav_entity_attribute': {
					'type': 'select',
					'query': 'SELECT * FROM _DBPRF_eav_entity_attribute WHERE attribute_id = ' + attribute_id
				}
			}
			res = self.select_multiple_data_connector(queries)
			if res and res['result'] == 'success':
				attribute_set_group_exist = duplicate_field_value_from_list(res['data']['eav_entity_attribute'],
				                                                            'attribute_set_id')
				attribute_set_exist = dict()
				if res['data'] and 'eav_attribute_set' in res['data'] and res['data']['eav_attribute_set']:
					for eav_attribute_set in res['data']['eav_attribute_set']:
						attribute_set_exist[eav_attribute_set['attribute_set_name']] = eav_attribute_set['attribute_set_id']
					for attribute_set_name_src in attribute_group_set:
						set_name = to_str(attribute_set_name_src['set_name']) + '_src' if to_str(attribute_set_name_src['set_name']).lower() != 'default' else attribute_set_name_src['set_name']
						if set_name in attribute_set_exist:
							self._notice['map']['attributes'][attribute_set_name_src['attribute_set_id']] = \
								attribute_set_exist[set_name]
						else:
							attribute_set_data = {
								'entity_type_id': self._notice['target']['extends']['catalog_product'],
								'attribute_set_name': set_name,
								'sort_order': 0,
							}
							attribute_set_id = self.import_attribute_data_connector(
								self.create_insert_query_connector('eav_attribute_set', attribute_set_data))
							if attribute_set_id:
								self._notice['map']['attributes'][
									attribute_set_name_src['attribute_set_id']] = attribute_set_id
				attribute_group_data = res['data']['attribute_group']
				for attribute_set_name_src in attribute_group_set:
					attribute_group_id = None
					if 'attribute_group' in res['data'] and res['data']['attribute_group']:
						for group_name in attribute_group_data:
							if (attribute_set_name_src['group_name'] == group_name['attribute_group_name']) and (
									self._notice['map']['attributes'].get(attribute_set_name_src['attribute_set_id']) ==
									group_name['attribute_set_id']):
								attribute_group_id = group_name['attribute_group_id']

					if not attribute_group_id:
						if self._notice['map']['attributes'].get(attribute_set_name_src['attribute_set_id']):
							attribute_group_i_data = {
								'attribute_set_id': self._notice['map']['attributes'][
									attribute_set_name_src['attribute_set_id']],
								'attribute_group_name': attribute_set_name_src['group_name'],
								'sort_order': 0,
								'default_id': 1 if to_str(attribute_set_name_src['group_name']).lower() == 'general' else 0,
								# 'attribute_group_code': self.generate_url_key(attribute_set_name_src['group_name']),
								# 'tab_group_code': None,
							}
							attribute_group_id = self.import_attribute_data_connector(
								self.create_insert_query_connector('eav_attribute_group', attribute_group_i_data))
					special_attr = ['updated_at', 'created_at']
					if attribute_group_id and self._notice['map']['attributes'].get(
							attribute_set_name_src['attribute_set_id']) and (not (self._notice['map']['attributes'].get(
						attribute_set_name_src['attribute_set_id']) in attribute_set_group_exist)):
						if not (attribute_set_name_src['group_name'] in special_attr):
							eav_entity_attribute_data = {
								'entity_type_id': self._notice['target']['extends']['catalog_product'],
								'attribute_set_id': self._notice['map']['attributes'].get(
									attribute_set_name_src['attribute_set_id']),
								'attribute_group_id': attribute_group_id,
								'attribute_id': attribute_id,
								'sort_order': 0
							}

							all_queries.append(
								self.create_insert_query_connector('eav_entity_attribute', eav_entity_attribute_data))
		# self.save_user_notice(self._migration_id, self._notice)
		option_data = convert['option_data']
		for option_id, option_values in option_data.items():
			option_default = option_values['value']
			option_desc_id = self.check_option_exist(option_default, convert['code'])
			option_language = option_values['languages']
			if '0' not in option_language:
				option_language['0'] = option_default
			if not option_desc_id:
				eav_attribute_option_data = {
					'attribute_id': attribute_id,
					'sort_order': 0,
				}
				option_desc_id = self.import_attribute_data_connector(
					self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data))
			if not option_desc_id:
				continue
			list_store = list()
			for store_id, store_option_value in option_language.items():
				if to_str(store_id) != '0' and to_str(store_id) not in self._notice['map']['languages']:
					continue
				store_id_desc = to_int(self.get_map_store_view(store_id))
				if store_id_desc in list_store:
					continue
				list_store.append(store_id_desc)
				delete_data = {
					'option_id': option_desc_id,
					'store_id': store_id_desc,
				}
				all_queries.append(self.create_delete_query_connector('eav_attribute_option_value', delete_data))
				eav_attribute_option_value_data = {
					'option_id': option_desc_id,
					'store_id': store_id_desc,
					'value': store_option_value
				}
				all_queries.append(self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
			if not option_desc_id:
				continue
			self.insert_map(self.TYPE_ATTR_OPTION, option_id, option_desc_id, option_default)

		if convert.get('labels'):
			for store_id, label in convert['labels'].items():
				store_id_desc = self.get_map_store_view(store_id)
				label_data = {
					'attribute_id': attribute_id,
					'store_id': store_id_desc,
					'value': label
				}
				all_queries.append(self.create_delete_query_connector('eav_attribute_label', {'attribute_id': attribute_id, 'store_id': store_id_desc}))
				all_queries.append(self.create_insert_query_connector('eav_attribute_label', label_data))

		self.import_multiple_data_connector(all_queries, 'attribute', False)
		return response_success()

	def addition_attribute_import(self, convert, attribute, attributes_ext):
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
			'query': "SELECT * FROM _DBPRF_catalog_product_entity "
			         "WHERE entity_id NOT IN (SELECT product_id FROM _DBPRF_catalog_product_super_link WHERE parent_id NOT IN (SELECT entity_id FROM _DBPRF_catalog_product_entity where type_id ='grouped' )) "
			         "AND entity_id IN (select product_id from _DBPRF_catalog_product_website where" + self.get_con_website_select_count() + ")"
			                                                                                                                                 "AND entity_id > " + to_str(id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit),
		}
		products = self.select_data_connector(query, 'products')

		if not products or products['result'] != 'success':
			return response_error()
		return products

	def get_products_ext_export(self, products):
		url_query = self.get_connector_url('query')
		product_ids = duplicate_field_value_from_list(products['data'], 'entity_id')
		product_id_con = self.list_to_in_condition(product_ids)
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = store_id_con + ' AND '
		product_ext_queries = {
			'catalog_product_website': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_website as cw "
				         "LEFT JOIN `_DBPRF_core_store` cs ON cs.`website_id` = cw.`website_id` WHERE cw.product_id IN " + product_id_con,
			},
			'catalog_product_super_link': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_super_link WHERE parent_id IN " + product_id_con,
			},
			'catalog_product_relation': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_relation WHERE child_id IN " + product_id_con,
			},
			# 'eav_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['src']['extends']['catalog_product']
			# },
			# 'eav_entity_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_eav_entity_attribute WHERE entity_type_id = " + self._notice['src']['extends']['catalog_product']
			# },
			'tag_relation': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_tag_relation WHERE product_id IN " + product_id_con,
			},
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
				'query': "SELECT * FROM _DBPRF_catalog_product_link WHERE link_type_id = 3 AND (linked_product_id IN " + product_id_con + " OR product_id IN " + product_id_con + ")",
			},
			'catalog_product_option': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_option WHERE type IN ('drop_down', 'radio', 'checkbox', 'multiple', 'multiswatch') AND product_id IN " + product_id_con
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
			'core_store': {
				'type': "select",
				'query': "SELECT * FROM `_DBPRF_core_store` WHERE code != 'admin'"
			}
		}
		if self._notice['src']['support'].get('mst_brands'):
			product_ext_queries['mst_brands_product_list'] = {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_mst_brands_product_list WHERE product_id IN " + product_id_con
			}
		product_ext_queries['url_rewrite'] = {
			'type': "select",
			'query': "SELECT * FROM _DBPRF_core_url_rewrite WHERE " + store_id_con + "product_id IN " + product_id_con + " AND is_system = 1"
		}
		product_ext = self.select_multiple_data_connector(product_ext_queries, 'products')

		if (not product_ext) or product_ext['result'] != 'success':
			return response_error()
		download_able_link_ids = duplicate_field_value_from_list(product_ext['data']['downloadable_link'], 'link_id')
		download_able_link_id_con = self.list_to_in_condition(download_able_link_ids)
		download_sample_link_ids = duplicate_field_value_from_list(product_ext['data']['downloadable_sample'], 'sample_id')
		# parent_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_link'], 'parent_id')
		children_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_link'], 'product_id')
		allproduct_id_query = self.list_to_in_condition(list(set(product_ids + children_ids)))
		option_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_option'], 'option_id')
		option_id_query = self.list_to_in_condition(option_ids)
		link_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_link'], 'link_id')
		bundle_option_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_bundle_option'], "option_id")
		bundle_option_id_con = self.list_to_in_condition(bundle_option_ids)
		tag_ids = duplicate_field_value_from_list(product_ext['data']['tag_relation'], "tag_id")
		tag_ids_con = self.list_to_in_condition(tag_ids)
		# attribute_ids = duplicate_field_value_from_list(product_ext['data']['eav_attribute'], "attribute_id")
		# attribute_ids_con = self.list_to_in_condition(attribute_ids)
		product_ext_rel_queries = {
			# 'catalog_eav_attribute': {
			# 	'type': "select",
			# 	'query': "SELECT * FROM _DBPRF_catalog_eav_attribute WHERE attribute_id IN " + attribute_ids_con,
			# },
			'catalog_product_link_attribute_decimal': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_link_attribute_decimal WHERE product_link_attribute_id = 3 and link_id IN " + self.list_to_in_condition(link_ids),
			},
			'catalog_product_super_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_super_attribute WHERE attribute_id > 0 AND product_id IN " + allproduct_id_query,
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
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_gallery WHERE entity_id IN " + allproduct_id_query,
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
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_media_gallery WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_tier_price': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_tier_price WHERE entity_id IN " + allproduct_id_query,
			},
			'catalog_product_entity_group_price': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_group_price WHERE entity_id IN " + allproduct_id_query,
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
				'type': "select", 'query': "SELECT * FROM _DBPRF_catalog_product_option_title WHERE option_id IN " + option_id_query,
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
				'type': "select", 'query': "SELECT * FROM _DBPRF_catalog_product_bundle_option_value WHERE option_id IN " + bundle_option_id_con,
			},
			'catalog_product_bundle_selection': {
				'type': "select", 'query': "SELECT * FROM _DBPRF_catalog_product_bundle_selection WHERE option_id IN " + bundle_option_id_con,
			},
			'downloadable_link_title': {
				'type': "select", 'query': "SELECT * FROM _DBPRF_downloadable_link_title WHERE link_id IN " + download_able_link_id_con
			},
			'downloadable_link_price': {
				'type': "select", 'query': "SELECT * FROM _DBPRF_downloadable_link_price WHERE link_id IN " + download_able_link_id_con
			},
			'downloadable_sample_title': {
				'type': "select", 'query': "SELECT * FROM _DBPRF_downloadable_sample_title WHERE sample_id IN " + self.list_to_in_condition(download_sample_link_ids)
			},
			'tag': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_tag WHERE tag_id IN " + tag_ids_con,
			},
		}

		product_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, 'products')

		if (not product_ext_rel) or (product_ext_rel['result'] != 'success'):
			return response_error()
		product_ext = self.sync_connector_object(product_ext, product_ext_rel)
		option_type_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_option_type_value'], 'option_type_id')
		option_type_id_con = self.list_to_in_condition(option_type_ids)
		value_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_entity_media_gallery'], 'value_id')
		value_id_con = self.list_to_in_condition(value_ids)
		option_attr_ids = duplicate_field_value_from_list(product_ext['data']['catalog_product_entity_int'], 'value')
		option_attr_id_con = self.list_to_in_condition(option_attr_ids)
		eav_attribute = self.get_eav_attribute_product()
		multi = get_list_from_list_by_field(eav_attribute, 'frontend_input', 'multiselect')
		multi_ids = self.list_to_in_condition(multi)
		all_option = list()
		if multi:
			multi_option = get_list_from_list_by_field(product_ext['data']['catalog_product_entity_varchar'], 'attribute_id', multi_ids)
			for row in multi_option:
				if row['value']:
					new_option = row['value'].split(',')
					all_option = set(all_option + new_option)
		all_option_query = self.list_to_in_condition(all_option)
		super_attribute_id = duplicate_field_value_from_list(product_ext['data']['catalog_product_super_attribute'], 'product_super_attribute_id')
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
				'query': "SELECT * FROM _DBPRF_catalog_product_entity_media_gallery_value WHERE value_id IN " + value_id_con,
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
		if self._notice['src']['support'].get('custom_options'):
			product_ext_rel_rel_queries['catalog_product_option_type_image'] = {
				'type': 'select',
				'query': 'SELECT * FROM _DBPRF_mageworx_custom_options_option_type_image WHERE option_type_id IN ' + option_type_id_con,
			}
		product_ext_rel_rel = self.select_multiple_data_connector(product_ext_rel_rel_queries, 'products')

		if (not product_ext_rel_rel) or (product_ext_rel_rel['result'] != 'success'):
			return response_error()
		product_ext = self.sync_connector_object(product_ext, product_ext_rel_rel)
		return product_ext

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

	def convert_product_export(self, product, products_ext):
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
		tax_class_id = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute['tax_class_id'], 'value')
		product_data['tax']['id'] = tax_class_id
		# price = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute.get('price'), 'value')
		cost_price = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute.get('cost_price'), 'value')
		weight = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute.get('weight'), 'value')
		status = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute.get('status'), 'value')
		visibility = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute.get('visibility'), 'value')
		quantity = get_row_from_list_by_field(products_ext['data']['cataloginventory_stock_item'], 'product_id', product['entity_id'])
		dict_visibility = {
			'1': 'not',
			'2': 'catalog',
			'3': 'search',
			'4': 'all'
		}
		product_data['visibility'] = dict_visibility.get(visibility)
		product_data['id'] = product['entity_id']
		product_data['sku'] = product['sku']

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
		name_def = get_row_value_from_list_by_field(names, 'store_id', language_default, 'value')
		descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('description'))
		description_def = get_row_value_from_list_by_field(descriptions, 'store_id', language_default, 'value')
		short_descriptions = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute.get('short_description'))
		short_description_def = get_row_value_from_list_by_field(short_descriptions, 'store_id', language_default, 'value') if get_row_value_from_list_by_field(short_descriptions, 'store_id', language_default, 'value') else get_row_value_from_list_by_field(short_descriptions, 'store_id', 0, 'value')
		meta_titles = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute.get('meta_title'))
		meta_title_def = get_row_value_from_list_by_field(meta_titles, 'store_id', language_default, 'value')
		meta_keywords = get_list_from_list_by_field(entity_text, 'attribute_id', eav_attribute['meta_keyword'])
		meta_keyword_def = get_row_value_from_list_by_field(meta_keywords, 'store_id', language_default, 'value')
		meta_descriptions = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['meta_description'])
		meta_description_def = get_row_value_from_list_by_field(meta_descriptions, 'store_id', language_default, 'value')

		url_keys = get_list_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['url_key'])
		url_key_def = get_row_value_from_list_by_field(url_keys, 'store_id', language_default, 'value')

		product_stores = get_list_from_list_by_field(products_ext['data']['catalog_product_website'], 'product_id', product['entity_id'])

		product_data['store_ids'] = list()

		for product_store in product_stores:
			product_data['store_ids'].append(product_store['store_id'])
		if not product_data['store_ids']:
			product_data['store_ids'] = duplicate_field_value_from_list(products_ext['data']['core_store'], 'store_id')
		prices = get_list_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['price'])
		price_def = get_row_value_from_list_by_field(prices, 'store_id', language_default, 'value')
		price_def = price_def if price_def else get_row_value_from_list_by_field(prices, 'store_id', 0, 'value')
		product_data['price'] = self.convert_price(price_def if price_def else get_row_value_from_list_by_field(prices, 'store_id', 0, 'value'), tax_class_id)
		product_data['url_key'] = url_key_def if url_key_def else get_row_value_from_list_by_field(url_keys, 'store_id', 0, 'value')
		product_data['name'] = name_def if name_def else (names[0]['value'] if names else (product_data['sku'] if product_data['sku'] else ''))
		if not product_data['name']:
			return response_error("error convert product " + product['entity_id'] + ": product name empty!")
		product_data['description'] = self.convert_image_in_description(description_def if description_def else get_row_value_from_list_by_field(descriptions, 'store_id', 0, 'value'))
		product_data['short_description'] = self.convert_image_in_description(short_description_def if short_description_def else '')
		product_data['meta_title'] = meta_title_def if meta_title_def else get_row_value_from_list_by_field(meta_titles, 'store_id', 0, 'value')
		product_data['meta_keyword'] = meta_keyword_def if meta_keyword_def else get_row_value_from_list_by_field(meta_keywords, 'store_id', 0, 'value')
		product_data['meta_description'] = meta_description_def if meta_description_def else get_row_value_from_list_by_field(meta_descriptions, 'store_id', 0, 'value')
		image = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['thumbnail'], 'value')
		image_label = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute['thumbnail_label'], 'value')
		url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
		if image and image != 'no_selection':
			product_data['thumb_image']['url'] = url_product_image
			product_data['thumb_image']['path'] = image
			product_data['thumb_image']['label'] = image_label
		product_images = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery'], 'entity_id', product['entity_id'])
		if to_len(product_images) > 0:
			check_img = get_row_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_images[0]['value_id'])
			if check_img['disabled'] == '1':
				for product_image_thumb in product_images:
					check_img = get_row_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_image_thumb['value_id'])
					if product_image_thumb['value'] == 'no_selection' or check_img['disabled'] == '1':
						continue
					product_data['thumb_image']['url'] = url_product_image
					product_data['thumb_image']['path'] = product_image_thumb['value']
					product_data['thumb_image']['label'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_image_thumb['value_id'], 'label')
			elif not product_data['thumb_image']['path']:
				product_data['thumb_image']['url'] = url_product_image
				product_data['thumb_image']['path'] = product_images[0]['value']
				product_data['thumb_image']['label'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_images[0]['value_id'], 'label')
		image_data = list()
		check_position = True
		if product_images:
			for product_image in product_images:
				check_img = get_row_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_image['value_id'])
				if product_image['value'] == 'no_selection' or product_image['value'] == product_data['thumb_image']['path']:
					continue
				product_image_data = self.construct_product_image()
				product_image_data['label'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_image['value_id'], 'label')
				product_image_data['position'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id', product_image['value_id'], 'position')
				if not product_image_data['position']:
					check_position = False
				product_image_data['url'] = url_product_image
				product_image_data['path'] = product_image['value']
				product_image_data['status'] = False if check_img['disabled'] == '1' else True
				image_data.append(product_image_data)
		# sorted image
		# -------------
		image_data = sorted(image_data, key = itemgetter('position')) if check_position else image_data
		# -------------
		product_data['images'] = image_data
		special_price = get_row_value_from_list_by_field(entity_decimal, 'attribute_id', eav_attribute['special_price'], 'value')
		special_from_date = get_row_value_from_list_by_field(entity_datetime, 'attribute_id', eav_attribute['special_from_date'], 'value')
		special_to_date = get_row_value_from_list_by_field(entity_datetime, 'attribute_id', eav_attribute['special_to_date'], 'value')
		if special_price:
			product_data['special_price']['price'] = self.convert_price(special_price, tax_class_id)
			product_data['special_price']['start_date'] = special_from_date
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

		groups_price = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_group_price'], 'entity_id', product['entity_id'])
		for group in groups_price:
			group_price_data = self.construct_product_tier_price()
			group_price_data['customer_group_id'] = group['customer_group_id']
			group_price_data['price'] = group['value']
			product_data['group_prices'].append(group_price_data)

		if self._notice['src']['support'].get('mst_brands') and 'mst_brands_product_list' in products_ext['data']:
			manu_ids = get_list_from_list_by_field(products_ext['data']['mst_brands_product_list'], 'store', 0)
			product_data['manufacturer']['id'] = get_row_value_from_list_by_field(manu_ids, 'product_id', product['entity_id'], 'brand_id')
		else:
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
			name_lang = get_row_value_from_list_by_field(names, 'store_id', lang_id, 'value')
			description_lang = get_row_value_from_list_by_field(descriptions, 'store_id', lang_id, 'value')
			short_description_lang = get_row_value_from_list_by_field(short_descriptions, 'store_id', lang_id, 'value')
			meta_title_lang = get_row_value_from_list_by_field(meta_titles, 'store_id', lang_id, 'value')
			meta_keyword_lang = get_row_value_from_list_by_field(meta_keywords, 'store_id', lang_id, 'value')
			meta_description_lang = get_row_value_from_list_by_field(meta_descriptions, 'store_id', lang_id, 'value')
			url_key_lang = get_row_value_from_list_by_field(url_keys, 'store_id', lang_id, 'value')
			price_lang = get_row_value_from_list_by_field(prices, 'store_id', lang_id, 'value')
			product_language_data['url_key'] = url_key_lang if url_key_lang else product_data['url_key']
			if price_lang:
				product_language_data['price'] = self.convert_price(price_lang, tax_class_id)

			product_language_data['name'] = name_lang if name_lang else product_data['name']
			product_language_data['description'] = self.convert_image_in_description(description_lang) if description_lang else product_data['description']
			product_language_data['short_description'] = self.convert_image_in_description(short_description_lang if short_description_lang else product_data['short_description'])
			product_language_data['meta_title'] = meta_title_lang if meta_title_lang else (product_data['meta_title'] if product_data['meta_title'] else '')
			product_language_data['meta_keyword'] = meta_keyword_lang if meta_keyword_lang else (product_data['meta_keyword'] if product_data['meta_keyword'] else '')
			product_language_data['meta_description'] = meta_description_lang if meta_description_lang else (product_data['meta_description'] if product_data['meta_description'] else '')
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
		# parent_ids = duplicate_field_value_from_list(parents, 'product_id')
		# product_data['group_parent_ids'] = parent_ids
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
				if product_option['type'] not in ['drop_down', 'radio', 'checkbox', 'multiple', 'multiswatch']:
					product_data['options'].append(option_data)
					continue
				product_option_type_value = get_list_from_list_by_field(products_ext['data']['catalog_product_option_type_value'], 'option_id', product_option['option_id'])
				option_type_ids = duplicate_field_value_from_list(product_option_type_value, 'option_type_id')

				for option_type_id in option_type_ids:
					option_value_data = self.construct_product_option_value()
					option_value_data['id'] = option_type_id
					option_type_value = get_row_from_list_by_field(products_ext['data']['catalog_product_option_type_value'], 'option_type_id', option_type_id)
					catalog_product_option_type_title = get_list_from_list_by_field(products_ext['data']['catalog_product_option_type_title'], 'option_type_id', option_type_id)
					catalog_product_option_type_price = get_row_from_list_by_field(products_ext['data']['catalog_product_option_type_price'], 'option_type_id', option_type_id)
					option_type_source_def = get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', self._notice['src']['language_default']) if get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', self._notice['src']['language_default']) else get_row_from_list_by_field(catalog_product_option_type_title, 'store_id', 0)
					option_value_data['option_value_name'] = option_type_source_def.get('title')
					if option_type_value.get('sku'):
						option_value_data['option_value_sku'] = option_type_value['sku']
					if self._notice['src']['support'].get('custom_options'):
						option_type_image = get_row_value_from_list_by_field(products_ext['data']['catalog_product_option_type_image'], 'option_type_id', option_type_id, 'image_file')
						if option_type_image:
							option_value_data['option_value_image'] = self._cart_url.strip('/') + '/media/customoptions/' + option_type_image.strip('/')
					for id, name in self._notice['src']['languages'].items():
						option_value_language_data = self.construct_product_option_value_lang()
						option_value_name_lang = get_row_value_from_list_by_field(catalog_product_option_type_title, 'store_id', id, 'title')
						option_value_language_data['option_value_name'] = option_value_name_lang if option_value_name_lang else option_type_source_def.get('title')
						option_value_data['option_value_languages'][id] = option_value_language_data

					if catalog_product_option_type_price and catalog_product_option_type_price['price_type'] == 'fixed':
						price_option = catalog_product_option_type_price['price']
					elif catalog_product_option_type_price and catalog_product_option_type_price['price_type'] == 'percent':
						price_option = to_decimal(catalog_product_option_type_price['price']) * to_decimal(price_def) / 100
					else:
						price_option = 0
					option_value_data['option_value_price'] = price_option
					option_data['values'].append(option_value_data)
				product_data['options'].append(option_data)
		# Get children product config
		if product['type_id'] == 'configurable':
			product_data['manage_stock'] = False
			product_data['type'] = self.PRODUCT_CONFIG
			children_products = get_list_from_list_by_field(products_ext['data']['catalog_product_super_link'], 'parent_id', product['entity_id'])
			super_attributes = get_list_from_list_by_field(products_ext['data']['catalog_product_super_attribute'], 'product_id', product['entity_id'])
			if children_products and super_attributes:
				for children in children_products:
					children_source = get_row_from_list_by_field(products_ext['data']['catalog_product_entity'], 'entity_id', children['product_id'])
					if children_source:
						children_decimal = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_decimal'], 'entity_id', children_source['entity_id'])
						children_datetime = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_datetime'], 'entity_id', children_source['entity_id'])
						children_int = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_int'], 'entity_id', children_source['entity_id'])
						children_varchar = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_varchar'], 'entity_id', children_source['entity_id'])
						children_text = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_text'], 'entity_id', children_source['entity_id'])
						child_tax_class_id = get_row_value_from_list_by_field(children_int, 'attribute_id', eav_attribute['tax_class_id'], 'value')
						children_status = get_row_value_from_list_by_field(children_int, 'attribute_id', eav_attribute.get('status'), 'value')
						children_names = get_list_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['name'])
						children_name_def = get_row_value_from_list_by_field(children_names, 'store_id', 0, 'value')
						children_prices = get_list_from_list_by_field(children_decimal, 'attribute_id', eav_attribute['price'])
						children_price_def = get_row_value_from_list_by_field(children_prices, 'store_id', 0, 'value')
						children_weights = get_list_from_list_by_field(children_decimal, 'attribute_id', eav_attribute['weight'])
						children_weight_def = get_row_value_from_list_by_field(children_weights, 'store_id', 0, 'value')
						stock_item = get_row_from_list_by_field(products_ext['data']['cataloginventory_stock_item'], 'product_id', children_source['entity_id'])
						children_descriptions = get_list_from_list_by_field(children_text, 'attribute_id', eav_attribute['description'])
						children_description_def = get_row_value_from_list_by_field(children_descriptions, 'store_id', 0, 'value')
						children_short_descriptions = get_list_from_list_by_field(children_text, 'attribute_id', eav_attribute['short_description'])
						children_short_description_def = get_row_value_from_list_by_field(children_short_descriptions, 'store_id', 0, 'value')
						children_meta_titles = get_list_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['meta_title'])
						children_meta_title_def = get_row_value_from_list_by_field(children_meta_titles, 'store_id', 0, 'value')
						children_meta_keywords = get_list_from_list_by_field(children_text, 'attribute_id', eav_attribute['meta_keyword'])
						children_meta_keyword_def = get_row_value_from_list_by_field(children_meta_keywords, 'store_id', 0, 'value')
						children_meta_descriptions = get_list_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['meta_description'])
						children_meta_description_def = get_row_value_from_list_by_field(children_meta_descriptions, 'store_id', 0, 'value')
						quantity_child = get_row_from_list_by_field(products_ext['data']['cataloginventory_stock_item'], 'product_id', children_source['entity_id'])
						# if to_int(children_source['required_options']) > 0:
						# 	continue
						childen_data = self.construct_product_child()
						childen_data['id'] = children_source['entity_id']
						childen_data['tax']['id'] = tax_class_id

						childen_data['status'] = True if to_int(children_status) == 1 else False
						# if not childen_data['status']:
						# 	continue
						childen_data['name'] = children_name_def
						childen_data['description'] = children_description_def if children_description_def else ''
						childen_data['short_description'] = children_short_description_def if children_short_description_def else ''
						childen_data['meta_title'] = children_meta_title_def if children_meta_title_def else ''
						childen_data['meta_keyword'] = children_meta_keyword_def if children_meta_keyword_def else ''
						childen_data['meta_description'] = children_meta_description_def if children_meta_description_def else ''
						childen_data['sku'] = children_source['sku']
						childen_data['price'] = self.convert_price(to_decimal(children_price_def), child_tax_class_id)
						childen_data['weight'] = to_decimal(children_weight_def) if to_decimal(children_weight_def) else 0.0000
						childen_data['qty'] = to_int(stock_item['qty']) if stock_item else 0
						if quantity_child:
							if to_int(quantity_child['use_config_manage_stock']) == 1:
								childen_data['manage_stock'] = not self._notice['src']['config'].get('no_manage_stock')
							else:
								childen_data['manage_stock'] = True if to_int(quantity_child['manage_stock']) == 1 else False
								childen_data['is_in_stock'] = True if to_int(quantity_child['is_in_stock']) == 1 else False
						else:
							childen_data['manage_stock'] = False
							childen_data['is_in_stock'] = False
						# childen_data['manage_stock'] = True if quantity_child and (to_int(quantity_child['manage_stock']) == 1 or to_int(quantity_child['use_config_manage_stock']) == 1) else False
						# childen_data['is_in_stock'] = True if quantity_child and to_int(quantity_child['is_in_stock']) == 1 else False
						childen_data['created_at'] = children_source['created_at'] if children_source else get_current_time()
						childen_data['update_at'] = children_source['updated_at'] if children_source else get_current_time()

						image = get_row_value_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['image'], 'value')
						image_label = get_row_value_from_list_by_field(children_varchar, 'attribute_id', eav_attribute['image_label'], 'value')
						url_product_image = self.get_url_suffix(self._notice['src']['config']['image_product'])
						if image and image != 'no_selection':
							childen_data['thumb_image']['url'] = url_product_image
							childen_data['thumb_image']['path'] = image
							childen_data['thumb_image']['label'] = image_label
						product_image_chil = get_list_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery'], 'entity_id', children_source['entity_id'])
						if product_image_chil:
							for product_image in product_image_chil:
								if product_image['value'] == image and product_image['value'] != 'no_selection':
									continue
								product_image_data = self.construct_product_image()
								product_image_data['label'] = get_row_value_from_list_by_field(products_ext['data']['catalog_product_entity_media_gallery_value'], 'value_id',
								                                                               product_image['value_id'], 'label')
								product_image_data['url'] = url_product_image
								product_image_data['path'] = product_image['value']
								childen_data['images'].append(product_image_data)

						child_special_price = get_row_value_from_list_by_field(children_decimal, 'attribute_id', eav_attribute['special_price'], 'value')
						child_special_from_date = get_row_value_from_list_by_field(children_datetime, 'attribute_id', eav_attribute['special_from_date'], 'value')
						child_special_to_date = get_row_value_from_list_by_field(children_datetime, 'attribute_id', eav_attribute['special_to_date'], 'value')
						if child_special_price:
							childen_data['special_price']['price'] = self.convert_price(child_special_price, child_tax_class_id)
							childen_data['special_price']['start_date'] = child_special_from_date
							childen_data['special_price']['end_date'] = child_special_to_date

						for lang_id, lang_name in self._notice['src']['languages'].items():
							childen_language_data = self.construct_product_lang()
							childen_name_lang = get_row_value_from_list_by_field(children_names, 'store_id', lang_id, 'value')
							childen_description_lang = get_row_value_from_list_by_field(children_descriptions, 'store_id', lang_id, 'value')
							childen_short_description_lang = get_row_value_from_list_by_field(children_short_descriptions, 'store_id', lang_id, 'value')
							childen_meta_title_lang = get_row_value_from_list_by_field(children_meta_titles, 'store_id', lang_id, 'value')
							childen_meta_keyword_lang = get_row_value_from_list_by_field(children_meta_keywords, 'store_id', lang_id, 'value')
							childen_meta_description_lang = get_row_value_from_list_by_field(children_meta_descriptions, 'store_id', lang_id, 'value')
							childen_language_data['name'] = childen_name_lang if childen_name_lang else (children_name_def if children_name_def else '')
							childen_language_data['description'] = childen_description_lang if childen_description_lang else (children_description_def if children_description_def else '')
							childen_language_data['short_description'] = childen_short_description_lang if childen_short_description_lang else (children_short_description_def if children_short_description_def else '')
							childen_language_data['meta_title'] = childen_meta_title_lang if childen_meta_title_lang else (children_meta_title_def if children_meta_title_def else '')
							childen_language_data['meta_keyword'] = childen_meta_keyword_lang if childen_meta_keyword_lang else (children_meta_keyword_def if children_meta_keyword_def else '')
							childen_language_data['meta_description'] = childen_meta_description_lang if childen_meta_description_lang else (children_meta_description_def if children_meta_description_def else '')
							price_childen_lang = get_row_value_from_list_by_field(children_prices, 'store_id', lang_id, 'value')
							if price_childen_lang:
								childen_language_data['price'] = self.convert_price(price_childen_lang, tax_class_id)
							childen_data['languages'][lang_id] = childen_language_data

						for super_attribute in super_attributes:
							childen_product_option_data = self.construct_product_child_attribute()
							eav_attribute_desc = get_row_from_list_by_field(product_eav_attributes, 'attribute_id', super_attribute['attribute_id'])
							super_attribute_label = get_list_from_list_by_field(products_ext['data']['catalog_product_super_attribute_label'], 'product_super_attribute_id', super_attribute['product_super_attribute_id'])
							childen_product_option_data['option_id'] = super_attribute['product_super_attribute_id']
							childen_product_option_data['option_code'] = eav_attribute_desc.get('attribute_code')
							childen_product_option_data['option_name'] = eav_attribute_desc.get('frontend_label', get_row_value_from_list_by_field(super_attribute_label, 'store_id', 0, 'value'))
							for id_lang, nameLang in self._notice['src']['languages'].items():
								product_attribute_lang_data = self.construct_product_option_lang()
								option_name_lang_sa = get_row_value_from_list_by_field(super_attribute_label, 'store_id', id_lang, 'value')
								product_attribute_lang_data['option_name'] = option_name_lang_sa if option_name_lang_sa else eav_attribute_desc.get('frontend_label')
								childen_product_option_data['option_languages'][id_lang] = product_attribute_lang_data

							option_id = get_row_value_from_list_by_field(children_int, 'attribute_id', super_attribute['attribute_id'], 'value')
							eav_attribute_option_value = get_list_from_list_by_field(products_ext['data']['eav_attribute_option_value'], 'option_id', option_id)
							option_value_def = get_row_value_from_list_by_field(eav_attribute_option_value, 'store_id', language_default, 'value')
							if not option_value_def:
								option_value_def = get_row_value_from_list_by_field(eav_attribute_option_value, 'store_id', 0, 'value')
							if not option_value_def:
								for vlua in eav_attribute_option_value:
									if vlua['value']:
										option_value_def = vlua
										break
							childen_product_option_data['option_value_id'] = option_id
							childen_product_option_data['option_value_code'] = to_str(option_value_def).lower() if not isinstance(option_value_def, bool) else ''
							childen_product_option_data['option_value_name'] = option_value_def
							super_attribute_pricing = get_list_from_list_by_field(products_ext['data']['catalog_product_super_attribute_pricing'], 'product_super_attribute_id', super_attribute['product_super_attribute_id'])
							super_attribute_pricing_row = get_row_from_list_by_field(super_attribute_pricing, 'value_index', option_id)
							super_attribute_price = 0
							if super_attribute_pricing_row:
								if super_attribute_pricing_row['is_percent'] and super_attribute_pricing_row['is_percent'] != '0':
									super_attribute_price = to_decimal(super_attribute_pricing_row['pricing_value']) * to_decimal(price_def) / 100
								else:
									super_attribute_price = to_decimal(super_attribute_pricing_row['pricing_value'])
							childen_product_option_data['price'] = to_decimal(super_attribute_price)
							childen_data['price'] = to_decimal(product_data['price']) + to_decimal(super_attribute_price)
							childen_product_option_data['price'] = super_attribute_price
							for id_lang, name_lang in self._notice['src']['languages'].items():
								product_attribute_value_lang_data = self.construct_product_option_value_lang()
								option_value_lang = get_row_value_from_list_by_field(eav_attribute_option_value, 'store_id', id_lang, 'value')
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
			if not attribute_values.get(eav_attribute_row['backend_type']) or eav_attribute_row['frontend_input'] not in ['select', 'text', 'textarea', 'boolean', 'multiselect']:
				continue
			if eav_attribute_row['backend_type'] not in ['decimal', 'int', 'text', 'varchar', 'datetime']:
				continue
			if not self._notice['src']['support'].get('mst_brands'):
				if eav_attribute_row['attribute_code'] in ['manufacturer']:
					continue
			product_attribute_data = self.construct_product_attribute()
			product_attribute_data['option_id'] = eav_attribute_row['attribute_id']

			product_attribute_data['option_type'] = 'text'
			if eav_attribute_row['frontend_input'] == 'select':
				product_attribute_data['option_type'] = 'select'
			elif eav_attribute_row['frontend_input'] == 'multiselect':
				product_attribute_data['option_type'] = self.OPTION_MULTISELECT
			product_attribute_data['option_code'] = eav_attribute_row['attribute_code']
			product_attribute_data['option_name'] = eav_attribute_row['frontend_label']
			catalog_eav_attribute_data = get_row_from_list_by_field(catalog_eav_attribute, 'attribute_id', eav_attribute_row['attribute_id'])
			# catalog_eav_attribute_data = get_row_from_list_by_field(products_ext['data']['catalog_eav_attribute'], 'attribute_id', 1)
			# if catalog_eav_attribute_data and to_int(eav_attribute_row['is_user_defined']) == to_int(catalog_eav_attribute_data['is_visible_on_front']) == to_int(catalog_eav_attribute_data['is_wysiwyg_enabled']) == 0:
			if catalog_eav_attribute_data and to_int(eav_attribute_row['is_user_defined']) == 0:
				continue
			if catalog_eav_attribute_data and eav_attribute_row['frontend_input'] == 'textarea':
				catalog_eav_attribute_data['is_visible_on_front'] = 1
			if catalog_eav_attribute_data and (to_int(catalog_eav_attribute_data['is_visible_on_front']) == 1 or to_int(catalog_eav_attribute_data['is_wysiwyg_enabled']) == 1 or to_int(catalog_eav_attribute_data['is_visible']) == 1):
				product_attribute_data['is_visible'] = True
			else:
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
			if eav_attribute_row['frontend_input'] == 'text' or eav_attribute_row['frontend_input'] == 'textarea':
				product_attribute_data['option_value_name'] = self.strip_html_tag(option_value_default['value'], none_check = True)
				for id_lang, name_lang in self._notice['src']['languages'].items():
					option_value_language_data = self.construct_product_option_value_lang()
					option_value_name_lang = get_row_from_list_by_field(option_value, 'store_id', id_lang)
					if not option_value_name_lang:
						continue
					option_value_language_data['option_value_name'] = self.strip_html_tag(option_value_name_lang['value'], none_check = True)
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
			if eav_attribute_row['frontend_input'] == 'boolean':
				option_v_name = option_value_default['value']
				product_attribute_data['option_value_name'] = 'Yes' if option_v_name == '1' else 'No'
			product_data['attributes'].append(product_attribute_data)

		if product['type_id'] == 'virtual':
			product_data['type'] = self.PRODUCT_VIRTUAL
		if product['type_id'] == 'downloadable':
			product_data['type'] = self.PRODUCT_DOWNLOAD
			download_datas = get_list_from_list_by_field(products_ext['data']['downloadable_link'], 'product_id', product['entity_id'])
			if download_datas:
				product_data['downloadable'] = list()
				for download_data in download_datas:
					title_product_downloads = get_list_from_list_by_field(products_ext['data']['downloadable_link_title'], 'link_id', download_data['link_id'])
					price_product_downloads = get_list_from_list_by_field(products_ext['data']['downloadable_link_price'], 'link_id', download_data['link_id'])
					title_download_def = get_row_value_from_list_by_field(title_product_downloads, 'store_id', language_default, 'title')
					price_download_def = get_row_value_from_list_by_field(price_product_downloads, 'website_id', language_default, 'price')

					if not title_download_def:
						title_download_def = title_product_downloads[0]['title']
					if not price_download_def:
						price_download_def = price_product_downloads[0]['price']

					product_download_data = self.construct_product_downloadable()
					product_download_data['path'] = download_data['link_file'] if download_data['link_file'] else download_data['link_url']
					product_download_data['name'] = title_download_def
					product_download_data['limit'] = download_data['number_of_downloads']
					product_download_data['price'] = price_download_def
					product_download_data['sample']['path'] = download_data['sample_url'] if download_data['sample_url'] else download_data['sample_file']
					product_download_data['sample']['name'] = 'Sample'

					product_data['downloadable'].append(product_download_data)

		# tags
		tag_relation = get_list_from_list_by_field(products_ext['data']['tag_relation'], 'product_id', product['entity_id'])
		if tag_relation:
			tags = list()
			for product_tag in tag_relation:
				tag = get_row_from_list_by_field(products_ext['data']['tag'], 'tag_id', product_tag['tag_id'])
				if tag:
					tags.append(tag['name'])
			product_data['tags'] = ','.join(tags)
		# related
		relation_type = {
			'1': self.PRODUCT_RELATE,
			'4': self.PRODUCT_UPSELL,
			'5': self.PRODUCT_CROSS
		}
		catalog_product_link_parent = get_list_from_list_by_field(products_ext['data']['catalog_product_link'], 'product_id', product['entity_id'])
		catalog_product_link_children = get_list_from_list_by_field(products_ext['data']['catalog_product_link'], 'linked_product_id', product['entity_id'])
		if catalog_product_link_parent:
			for row in catalog_product_link_parent:
				if to_int(row['link_type_id']) == 3:
					continue
				key = get_value_by_key_in_dict(relation_type, to_str(row['link_type_id']), self.PRODUCT_RELATE)
				relation = self.construct_product_relation()
				relation['id'] = row['linked_product_id']
				relation['type'] = key
				product_data['relate']['children'].append(relation)
		if catalog_product_link_children:
			for row in catalog_product_link_children:
				if to_int(row['link_type_id']) == 3:
					continue
				key = get_value_by_key_in_dict(relation_type, to_str(row['link_type_id']), 'relate')
				relation = self.construct_product_relation()
				relation['id'] = row['product_id']
				relation['type'] = key
				product_data['relate']['parent'].append(relation)
		detect_seo = self.detect_seo()
		product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)
		return response_success(product_data)

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
			all_query.append(self.create_delete_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'], {'entity_id': product_id, 'attribute_id': product_eav_attribute_data[key]['attribute_id']}))
			product_attr_data = {
				'attribute_id': product_eav_attribute_data[key]['attribute_id'],
				'store_id': 0,
				'entity_id': product_id,
				'value': value,
			}
			all_query.append(self.create_insert_query_connector('catalog_product_entity_' + product_eav_attribute_data[key]['backend_type'], product_attr_data))
		self.import_multiple_data_connector(all_query, 'update_demo_product')
		return response_success()

	def router_product_import(self, convert, product, products_ext):
		return response_success('product_import')

	def before_product_import(self, convert, product, products_ext):
		return response_success()

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
		new_sku = sku
		if sku:
			while self.check_sku_exist(new_sku):
				index = time.time()
				new_sku = sku + to_str(index)

		catalog_product_entity_data = {
			'attribute_set_id': attribute_set_id,
			'entity_type_id': self._notice['target']['extends']['catalog_product'],
			'type_id': convert['type'] if convert['type'] else 'simple',
			'sku': new_sku,
			'has_options': 0,
			'required_options': 0,
			'created_at': convert_format_time(convert.get('created_at', get_current_time())),
			'updated_at': convert_format_time(convert.get('updated_at', get_current_time())),
		}
		if to_len(convert['options']) > 0:
			catalog_product_entity_data['has_options'] = 1
		if self._notice['config']['real_pre_prd']:
			catalog_product_entity_data['entity_id'] = convert['id']
		product_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity', catalog_product_entity_data), True, convert['id'])
		if not sku:
			self.import_data_connector(self.create_update_query_connector('catalog_product_entity', {'sku': product_id}, {'entity_id': product_id}))
		if not product_id:
			response['result'] = 'error'
			response['msg'] = self.warning_import_entity('product', convert['id'])
			return response
		if index:
			new_sku = to_str(sku) + '-' + to_str(product_id)
			self.import_data_connector(self.create_update_query_connector('catalog_product_entity', {'sku': new_sku}, {'entity_id': product_id}))
		self.insert_map(self.TYPE_PRODUCT, convert['id'], product_id, convert['code'])
		return response_success(product_id)

	def after_product_import(self, product_id, convert, product, products_ext):
		url_query = self.get_connector_url('query')
		url_image = self.get_connector_url('image')
		all_query = list()
		all_attribute = self.select_all_attribute_map()
		product_eav_attribute_data = dict()
		attribute_id_media = None
		for attribute_row in all_attribute:
			attribute = json_decode(attribute_row['value'])
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
							main_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
							if main_process['url'] == image_process['url']:
								image_name = item_image_name
						catalog_product_entity_media_gallery_data = {
							'attribute_id': attribute_id_media,
							'value': item_image_name,
							'entity_id': product_id,
						}
						value_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity_media_gallery', catalog_product_entity_media_gallery_data))
						if value_id:
							catalog_product_entity_media_gallery_value_data = {
								'value_id': value_id,
								'store_id': 0,
								'label': item.get('label', ''),
								'position': item.get('position', 1),
								'disabled': item.get('disabled', 0),
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
					'entity_id': product_id,
				}
				value_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_entity_media_gallery', catalog_product_entity_media_gallery_data))
				if value_id:
					catalog_product_entity_media_gallery_value_data = {
						'value_id': value_id,
						'store_id': 0,
						'label': convert['thumb_image'].get('label'),
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
					'position': value.get('position', 1),
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
							'position': value.get('position', 1),

						}
						all_query.append(self.create_insert_query_connector('catalog_category_product', catalog_category_product_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------

		# todo: cataloginventory_stock
		# begin
		cataloginventory_stock_item_data = {
			'product_id': product_id,
			'stock_id': 1,
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
						'customer_group_id': self._notice['map']['customer_group'].get(
							value.get('customer_group_id', -1), 0),
						'qty': value['qty'],
						'value': value['price'],
						'website_id': website_id,
					}
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
			all_query.append(self.create_insert_query_connector('catalog_product_website', catalog_product_website_data))
		# end

		# ------------------------------------------------------------------------------------------------------------------------

		# todo: product attribute
		# begin: attribute product
		# begin map
		tax_class_id = 0
		if convert['tax']['id'] or convert['tax']['code']:
			tax_class_id = self.get_map_field_by_src(self.TYPE_TAX_PRODUCT, convert['tax']['id'], convert['tax']['code'])
		pro_url_key = self.get_product_url_key(convert.get('url_key'), 0, convert.get('name', ''))
		# pro_url_path = self.get_product_url_path(convert.get('url_path'), 0, pro_url_key)

		data_attribute_insert = {
			'name': self.strip_html_tag(get_value_by_key_in_dict(convert, 'name', '')),
			'meta_title': get_value_by_key_in_dict(convert, 'meta_title', ''),
			'meta_description': get_value_by_key_in_dict(convert, 'meta_description', ''),
			'image': image_name if image_name else None,
			'small_image': image_name if image_name else None,
			'thumbnail': image_name if image_name else None,
			'description': self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'description', '')),
			'short_description': self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'short_description', convert['name'])),
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
			'url_key': pro_url_key,
			'country_of_manufacture': convert.get('country_of_manufacture'),
			'options_container': 'container1',
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
					backend_type = 'varchar'
				if attribute_src['option_type'] == self.OPTION_DATETIME:
					backend_type = 'datetime'
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
								'value': attribute_src['option_value_name']
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
		if 'downloadable' in convert:
			for download in convert['downloadable']:
				data_attribute_insert['links_title'] = download.get('name')
				data_attribute_insert['links_purchased_separately'] = download.get('path')
				data_attribute_insert['samples_title'] = download.get('sample', {}).get('name')
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
						'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['catalog_product']) + " and attribute_code = 'manufacturer'"
					},
				}
				product_eav_attribute = self.get_connector_data(url_query, {
					'serialize': True,
					'query': json.dumps(product_eav_attribute_queries)
				})
				try:
					attribute_id = product_eav_attribute['data']['eav_attribute'][0]['attribute_id']
				except Exception:
					attribute_id = self.create_attribute('manufacturer', 'int', 'select', self._notice['target']['extends']['catalog_product'], 'Manufacturer')
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
				manufacturer_id = self.import_manufacturer_data_connector(self.create_insert_query_connector('eav_attribute_option', eav_attribute_option_data), True, convert['id'])
				if manufacturer_id:
					# return response_error('Error import manufacturer')
					eav_attribute_option_value_data = {
						'option_id': manufacturer_id,
						'store_id': 0,
						'value': convert['manufacturer']['name'],
					}
					self.import_manufacturer_data_connector(
						self.create_insert_query_connector('eav_attribute_option_value', eav_attribute_option_value_data))
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
					'entity_type_id': self._notice['target']['extends']['catalog_product'],
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
					'url_key': language_data.get('url_key'),
					'url_path': language_data.get('url_path'),
				}
				for key1, value1 in product_eav_attribute_data.items():
					for key2, value2 in data_attribute_insert.items():
						if key2 != key1:
							continue
						store_id = self.get_map_store_view(language_id)
						if key2 == 'url_key':
							value2 = self.get_product_url_key(value2, store_id, language_data.get('name'))
						if not value2:
							continue
						product_attr_data = {
							'attribute_id': value1['attribute_id'],
							'store_id': store_id,
							'entity_id': product_id,
							'value': value2,
							'entity_type_id': self._notice['target']['extends']['catalog_product'],
						}
						all_query.append(self.create_insert_query_connector('catalog_product_entity_' + value1['backend_type'], product_attr_data))
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
			if convert['options']:
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
					product_super_attribute_id = self.import_product_data_connector(self.create_insert_query_connector('catalog_product_super_attribute', catalog_product_super_attribute_data))
					if not product_super_attribute_id:
						continue
					if attribute_label:
						catalog_product_super_attribute_label_data = {
							'product_super_attribute_id': product_super_attribute_id,
							'store_id': 0,
							'use_default': 1,
							'value': attribute_label
						}
						all_query.append(self.create_insert_query_connector('catalog_product_super_attribute_label', catalog_product_super_attribute_label_data))
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
						all_query.append(self.create_insert_query_connector('catalog_product_super_link', catalog_product_super_link_data))

			# end
			# ------------------------------------------------------------------------------------------------------------------------

			# todo: downloadable product
			# begin
			downloadable = convert.get('downloadable', dict())
			if 'link' in downloadable:
				for downloadable_link in downloadable['link']:
					link = downloadable_link['link']
					title = downloadable_link['title']
					price = downloadable_link['price']

					downloadable_link_data = {
						'product_id': product_id,
						'sort_order': 0,
						'number_of_downloads': link.get('number_of_download', 0),
						'is_shareable': link.get('is_shareable'),
						'link_url': link.get('link_url'),
						'link_file': link.get('link_file'),
						'link_type': link.get('link_type'),
						'sample_url': link.get('sample_url'),
						'sample_file': link.get('sample_file'),
						'sample_type': link.get('sample_type'),
					}
					downloadable_link_id = self.import_product_data_connector(self.create_insert_query_connector('downloadable_link', downloadable_link_data))
					if not downloadable_link_id:
						continue
					downloadable_link_title_data = {
						'link_id': downloadable_link_id,
						'store_id': 0,
						'title': title.get('title')
					}
					all_query.append(self.create_insert_query_connector('downloadable_link_title', downloadable_link_title_data))

					downloadable_link_price_data = {
						'link_id': downloadable_link_id,
						'website_id': 0,
						'price': price.get('price')
					}
					all_query.append(self.create_insert_query_connector('downloadable_link_price', downloadable_link_price_data))

			if 'samples' in downloadable:
				for downloadable_sample in downloadable['samples']:
					sample = downloadable_sample['sample']
					title = downloadable_sample.get('title', {})

					downloadable_sample_data = {
						'product_id': product_id,
						'sample_url': sample.get('sample_url'),
						'sample_file': sample.get('sample_file'),
						'sample_type': sample.get('sample_type'),
						'sort_order': sample.get('sort_order', 0),
					}
					downloadable_sample_id = self.import_product_data_connector(
						self.create_insert_query_connector('downloadable_sample', downloadable_sample_data))
					if not downloadable_sample_id:
						continue
					downloadable_sample_title_data = {
						'sample_id': downloadable_sample_id,
						'store_id': 0,
						'title': title.get('title')
					}
					all_query.append(self.create_insert_query_connector('downloadable_sample_title', downloadable_sample_title_data))
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
						product_link_attribute_id_data = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
						product_link_attribute_id = product_link_attribute_id_data['data'][0][
							'product_link_attribute_id']
						catalog_product_link_attribute_int = {
							"product_link_attribute_id": product_link_attribute_id,
							"link_id": link_id,
							"value": index,
						}
						index += 1
						all_query.append(self.create_insert_query_connector('catalog_product_link_attribute_int', catalog_product_link_attribute_int))
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
							'query': " SELECT " + to_str(product_link_attribute_id) + ", " + to_str(link_id) + " , MAX(`value`)+1 FROM _DBPRF_catalog_product_link_attribute_int WHERE `link_id` IN (SELECT link_id FROM _DBPRF_catalog_product_link WHERE product_id = " + to_str(relate_desc_id) + " and link_type_id = " + to_str(link_type_id) + ")"
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
				'store_id': store_id,
				'id_path': 'product/' + to_str(product_id),
				'request_path': seo,
				'target_path': 'catalog/product/view/id/' + to_str(product_id),
				'is_system': 1,
				'options': None,
				'description': None,
				'product_id': product_id,
				'category_id': None,
			}
			self.import_category_data_connector(self.create_insert_query_connector('core_url_rewrite', url_rewrite_data))

		is_default = False
		seo_301 = self._notice['config']['seo_301']
		if (self._notice['config']['seo'] or self._notice['config']['seo_301']) and 'seo' in convert:
			for url_rewrite_product in convert['seo']:
				if seo_301:
					is_default = True
				store_id = self.get_map_store_view(url_rewrite_product['store_id'])
				default = True if url_rewrite_product['default'] and not is_default else False
				if default:
					is_default = True
				if not url_rewrite_product['category_id']:
					url_rewrite_product_data = {
						'store_id': store_id,
						'id_path': 'product/' + to_str(product_id),
						'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
						'target_path': 'catalog/product/view/id/' + to_str(product_id),
						'is_system': 0,
						'options': 'RP' if seo_301 else None,
						'description': None,
						'product_id': product_id,
						'category_id': None,
					}
					self.import_product_data_connector(self.create_insert_query_connector('core_url_rewrite', url_rewrite_product_data))
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
						'store_id': store_id,
						'id_path': 'product/' + to_str(product_id) + '/' + to_str(category_desc_id),
						'request_path': self.get_request_path(url_rewrite_product['request_path'], store_id),
						'target_path': 'catalog/product/view/id/' + to_str(product_id) + '/category/' + to_str(category_desc_id),
						'is_system': 1,
						'options': 'RP' if seo_301 else None,
						'description': None,
						'product_id': product_id,
						'category_id': category_desc_id,
					}
					url_rewrite_id = self.import_product_data_connector(
						self.create_insert_query_connector('core_url_rewrite', url_rewrite_data))

		# end
		# ------------------------------------------------------------------------------------------------------------------------

		self.import_multiple_data_connector(all_query, 'products')
		return response_success(list_attr)

	def addition_product_import(self, convert, product, products_ext):
		return response_success()

	def finish_product_export(self):
		del self.eav_attribute_product
		del self.catalog_eav_attribute
		return response_success()

	# TODO: CUSTOMER
	def prepare_customers_import(self):
		return self

	def prepare_customers_export(self):
		return self

	def get_customers_main_export(self):
		id_src = self._notice['process']['customers']['id_src']
		limit = self._notice['setting']['customers']
		store_id_con = self.get_con_store_select()
		if store_id_con:
			store_id_con = store_id_con + ' AND '
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
				'query': "SELECT * FROM _DBPRF_newsletter_subscriber WHERE subscriber_status = 1 AND customer_id IN " + customer_id_con
			},
		}
		customer_ext = self.select_multiple_data_connector(customer_ext_queries, 'customers')

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
		customer_ext_rel = self.select_multiple_data_connector(customer_ext_rel_queries, 'customers')

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

		entity_int = get_list_from_list_by_field(customers_ext['data']['customer_entity_int'], 'entity_id', customer['entity_id'])
		entity_varchar = get_list_from_list_by_field(customers_ext['data']['customer_entity_varchar'], 'entity_id', customer['entity_id'])
		entity_datetime = get_list_from_list_by_field(customers_ext['data']['customer_entity_datetime'], 'entity_id', customer['entity_id'])

		subscriber = get_row_value_from_list_by_field(customers_ext['data']['newsletter_subscriber'], 'customer_id', customer['entity_id'], 'subscriber_status')
		pwd = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute_cus['password_hash'], 'value')
		first_name = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute_cus['firstname'], 'value')
		middle_name = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute_cus['middlename'], 'value')
		last_name = get_row_value_from_list_by_field(entity_varchar, 'attribute_id', eav_attribute_cus['lastname'], 'value')
		customer_data = self.construct_customer()
		customer_data = self.add_construct_default(customer_data)
		customer_data['id'] = customer['entity_id']
		customer_data['store_id'] = customer['store_id']
		customer_data['increment_id'] = customer['increment_id']
		customer_data['email'] = customer['email'].strip()
		customer_data['username'] = customer['email'].strip()
		customer_data['password'] = pwd if pwd else ''
		customer_data['first_name'] = first_name if first_name else ''
		customer_data['middle_name'] = middle_name if middle_name else ''
		customer_data['last_name'] = last_name if last_name else ''
		customer_group_ids = list()
		customer_group_ids.append(customer['group_id'])
		customer_data['group_id'] = customer['group_id']
		gender_id = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute_cus['gender'], 'value')
		customer_data['gender'] = self.GENDER_MALE if to_int(gender_id) == 1 else (self.GENDER_FEMALE if to_int(gender_id) == 2 else self.GENDER_OTHER)
		customer_data['dob'] = get_row_value_from_list_by_field(entity_datetime, 'attribute_id', eav_attribute_cus['dob'], 'value')
		customer_data['is_subscribed'] = True if to_int(subscriber) == 1 else False
		customer_data['active'] = customer['is_active']
		customer_data['created_at'] = customer['created_at']
		customer_data['updated_at'] = customer['updated_at']
		customer_data['telephone'] = ''
		id_address_billing = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute_cus['default_billing'], 'value')
		id_address_shipping = get_row_value_from_list_by_field(entity_int, 'attribute_id', eav_attribute_cus['default_shipping'], 'value')

		address_entity = get_list_from_list_by_field(customers_ext['data']['customer_address_entity'], 'parent_id',
		                                             customer['entity_id'])
		if address_entity:
			for address in address_entity:
				address_entity_int = get_list_from_list_by_field(customers_ext['data']['customer_address_entity_int'], 'entity_id', address['entity_id'])
				address_entity_text = get_list_from_list_by_field(customers_ext['data']['customer_address_entity_text'], 'entity_id', address['entity_id'])
				address_entity_varchar = get_list_from_list_by_field(customers_ext['data']['customer_address_entity_varchar'], 'entity_id', address['entity_id'])
				address_data = self.construct_customer_address()
				address_data = self.add_construct_default(address_data)
				address_data['id'] = address['entity_id']
				if to_int(address['entity_id']) == to_int(id_address_billing):
					address_data['default']['billing'] = True

				if to_int(address['entity_id']) == to_int(id_address_shipping):
					address_data['default']['shipping'] = True
				first_name = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['firstname'], 'value')
				last_name = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['lastname'], 'value')
				address_data['first_name'] = first_name if first_name else ''
				address_data['last_name'] = last_name if last_name else ''
				street = get_row_value_from_list_by_field(address_entity_text, 'attribute_id', eav_attribute_cusadd['street'], 'value')
				if street:
					street_line = street.splitlines()
					address_data['address_1'] = street_line[0] if to_len(street_line) > 0 else ''
					address_data['address_2'] = street_line[1] if to_len(street_line) > 1 else ''

				address_data['city'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['city'], 'value')
				address_data['postcode'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['postcode'], 'value')
				address_data['telephone'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['telephone'], 'value')
				address_data['company'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['company'], 'value')
				address_data['fax'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['fax'], 'value')
				# address_data['vat_number'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['taxvat'], 'value')
				country_code = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['country_id'], 'value')
				if country_code:
					address_data['country']['country_code'] = country_code
					address_data['country']['name'] = self.get_country_name_by_code(country_code)
				else:
					address_data['country']['country_code'] = 'US'
					address_data['country']['name'] = 'United States'

				state = get_row_from_list_by_field(address_entity_int, 'attribute_id', eav_attribute_cusadd['region_id'])
				if state and state['code'] and state['default_name']:
					address_data['state']['state_code'] = state['code']
					address_data['state']['name'] = state['default_name']
				else:
					address_data['state']['state_code'] = ''
					address_data['state']['name'] = get_row_value_from_list_by_field(address_entity_varchar, 'attribute_id', eav_attribute_cusadd['region'], 'value')

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

		customer_entity_data = {
			'entity_type_id': self._notice['target']['extends']['customer'],
			'attribute_set_id': 1,
			'website_id': self.get_website_id_by_store_id(store_id),
			'email': convert['email'],
			'group_id': self.get_map_customer_group(convert['group_id']),
			'increment_id': None,
			'store_id': store_id,
			'created_at': convert.get('created_at', get_current_time()),
			'updated_at': convert.get('updated_at', get_current_time()),
			'is_active': convert.get('is_active', 1),
		}

		if self._notice['config']['pre_cus']:
			self.delete_target_customer(convert['id'])
			customer_entity_data['entity_id'] = convert['id']
		customer_id = self.import_customer_data_connector(
			self.create_insert_query_connector('customer_entity', customer_entity_data), True, convert['id'])
		if not customer_id:
			return response_error()
		self.insert_map(self.TYPE_CUSTOMER, convert['id'], customer_id, convert['code'])
		return response_success(customer_id)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
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
		all_queries = list()
		billing_default = None
		shipping_default = None
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
		customer_address_eav_attribute = self.get_attribute_customer_address()

		for customer_address in convert['address']:
			if customer_address['state']['name']:
				region_id = self.get_region_id_by_state_name(customer_address['state']['name'])
			else:
				region_id = 0
			customer_address_entity_data = {
				'entity_type_id': self._notice['target']['extends']['customer_address'],
				'attribute_set_id': 2,
				'increment_id': increment_id,
				'parent_id': customer_id,
				'created_at': get_value_by_key_in_dict(customer_address, 'created_at', get_current_time()),
				'updated_at': get_value_by_key_in_dict(customer_address, 'updated_at', get_current_time()),
				'is_active': get_value_by_key_in_dict(customer_address, 'is_active', 1),
			}
			customer_address_id = self.import_customer_data_connector(self.create_insert_query_connector('customer_address_entity', customer_address_entity_data))
			if not customer_address_id:
				continue
			if customer_address['default']['billing']:
				billing_default = customer_address_id
			if customer_address['default']['shipping']:
				shipping_default = customer_address_id
			customer_address_attribute_data = {
				'prefix': get_value_by_key_in_dict(customer_address, 'prefix'),
				'firstname': get_value_by_key_in_dict(customer_address, 'first_name', ''),
				'lastname': get_value_by_key_in_dict(customer_address, 'last_name', ''),
				'middlename': get_value_by_key_in_dict(customer_address, 'middle_name'),
				'suffix': get_value_by_key_in_dict(convert, 'suffix'),
				'company': get_value_by_key_in_dict(customer_address, 'company'),
				'street': to_str(customer_address['address_1']) + "\n" + to_str(customer_address.get('address_2')),
				'city': get_value_by_key_in_dict(customer_address, 'city', ''),
				'country_id': get_value_by_key_in_dict(get_value_by_key_in_dict(customer_address, 'country', dict()), 'country_code', ''),
				'region': customer_address['state']['name'] if customer_address['state']['name'] else '',
				'region_id': region_id,
				'postcode': get_value_by_key_in_dict(customer_address, 'postcode'),
				'telephone': get_value_by_key_in_dict(customer_address, 'telephone', ''),
				'fax': get_value_by_key_in_dict(customer_address, 'fax'),
				'vat_id': get_value_by_key_in_dict(customer_address, 'vat_id'),
				'vat_is_valid': get_value_by_key_in_dict(customer_address, 'vat_is_valid'),
				'vat_request_date': get_value_by_key_in_dict(customer_address, 'vat_request_date'),
				'vat_request_id': get_value_by_key_in_dict(customer_address, 'vat_request_id'),
				'vat_request_success': get_value_by_key_in_dict(customer_address, 'vat_request_success'),
			}
			for key, value in customer_address_attribute_data.items():
				if key not in customer_address_eav_attribute:
					continue
				if not value:
					continue
				customer_address_attr_data = {
					'entity_type_id': self._notice['target']['extends']['customer_address'],
					'attribute_id': customer_address_eav_attribute[key]['attribute_id'],
					'entity_id': customer_address_id,
					'value': value,

				}
				all_queries.append(self.create_insert_query_connector('customer_address_entity_' + customer_address_eav_attribute[key]['backend_type'], customer_address_attr_data))
		# endfor
		# customer address end
		# ------------------------------------------------------------------------------------------------------------------------
		customer_eav_attribute = self.get_attribute_customer()
		customer_attribute_data = {
			'created_in': self._notice['target']['languages'].get(str(store_id), 'DEFAULT'),
			'prefix': get_value_by_key_in_dict(convert, 'prefix'),
			'firstname': get_value_by_key_in_dict(convert, 'first_name'),
			'middlename': get_value_by_key_in_dict(convert, 'middle_name'),
			'lastname': get_value_by_key_in_dict(convert, 'last_name'),
			'suffix': get_value_by_key_in_dict(convert, 'suffix'),
			'dob': get_value_by_key_in_dict(convert, 'dob', None),
			'password_hash': convert.get('password'),
			'default_billing': billing_default,
			'default_shipping': shipping_default,
			'rp_token': '',
			'rp_token_created_at': get_current_time(),
			'taxvat': to_str(get_value_by_key_in_dict(convert, 'taxvat'))[0:49],
			'confirmation': None,
			'gender': convert.get('gender'),
		}
		for key, value in customer_attribute_data.items():
			if key not in customer_eav_attribute:
				continue
			if not value:
				continue
			customer_attr_data = {
				'entity_type_id': self._notice['target']['extends']['customer'],
				'attribute_id': customer_eav_attribute[key]['attribute_id'],
				'entity_id': customer_id,
				'value': value,

			}
			all_queries.append(self.create_insert_query_connector('customer_entity_' + customer_eav_attribute[key]['backend_type'], customer_attr_data))
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
				'store_id': store_id,
				'customer_id': customer_id,
				'subscriber_email': convert.get('email'),
				'subscriber_status': 1,
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
			store_id_con = store_id_con + ' AND '
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_sales_flat_order WHERE " + self.get_con_store_select_count() + " AND entity_id > " + to_str(id_src) + " ORDER BY entity_id ASC LIMIT " + to_str(limit),
		}
		orders = self.select_data_connector(query, 'orders')

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
				'query': "SELECT * FROM _DBPRF_sales_flat_order_item WHERE order_id IN " + order_id_con
			},
			'sales_flat_order_address': {
				'type': 'select',
				'query': "SELECT sfoa.*,sdcr.code,sdcr.default_name FROM _DBPRF_sales_flat_order_address as sfoa LEFT "
				         "JOIN _DBPRF_directory_country_region as sdcr ON sfoa.region_id = sdcr.region_id WHERE "
				         "sfoa.parent_id IN " + order_id_con,
			},
			'sales_flat_order_status_history': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_flat_order_status_history WHERE parent_id IN " + order_id_con
			},
			'sales_flat_order_payment': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_flat_order_payment WHERE parent_id IN " + order_id_con
			},
			'sales_flat_invoice': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_sales_flat_invoice WHERE order_id IN " + order_id_con
			},
			# 'sales_flat_shipment': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_sales_flat_shipment WHERE order_id IN " + order_id_con
			# }
		}
		order_ext = self.select_multiple_data_connector(order_ext_quries, 'orders')
		if not order_ext or order_ext['result'] != 'success':
			return response_error()
		order_ext_rel_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT attribute_id FROM _DBPRF_eav_attribute WHERE entity_type_id = " + self._notice['src']['extends']['catalog_product'] + " AND attribute_code LIKE 'tax_class_id'"
			},
		}
		order_ext_rel = self.select_multiple_data_connector(order_ext_rel_queries, 'orders')
		if order_ext_rel and order_ext_rel['result'] == 'success' and order_ext_rel['data'] and order_ext_rel['data']['eav_attribute'][0]['attribute_id']:
			product_ids = duplicate_field_value_from_list(order_ext['data']['sales_flat_order_item'], 'product_id')
			product_id_con = self.list_to_in_condition(product_ids)
			order_ext_rel_rel_queries = {
				'product_vat_id': {
					'type': "select",
					'query': "SELECT * FROM _DBPRF_catalog_product_entity_int WHERE attribute_id = " + to_str(order_ext_rel['data']['eav_attribute'][0]['attribute_id']) + " AND entity_id IN " + product_id_con
				},
			}
			order_ext_rel_rel = self.select_multiple_data_connector(order_ext_rel_rel_queries, 'orders')
			if order_ext_rel_rel and order_ext_rel_rel['result'] == 'success' and order_ext_rel_rel['data']:
				order_ext = self.sync_connector_object(order_ext, order_ext_rel_rel)

		return order_ext

	def convert_order_export(self, order, orders_ext):
		order_data = self.construct_order()
		order_data['id'] = order['entity_id']
		order_data['status'] = order['status']
		order_data['store_id'] = order['store_id']
		order_data['tax']['title'] = "Taxes"
		order_data['tax']['amount'] = to_decimal(get_value_by_key_in_dict(order, 'base_tax_amount', 0.0000))
		order_data['shipping']['title'] = "Shipping"
		order_data['shipping']['amount'] = to_decimal(get_value_by_key_in_dict(order, 'base_shipping_amount', 0.0000))
		order_data['discount']['amount'] = abs(to_decimal(get_value_by_key_in_dict(order, 'discount_amount', 0.0000)))
		order_data['discount']['code'] = get_value_by_key_in_dict(order, 'coupon_code')
		order_data['subtotal']['title'] = 'Total products'
		order_data['subtotal']['amount'] = to_decimal(get_value_by_key_in_dict(order, 'subtotal', 0.0000))
		order_data['total']['title'] = 'Total'
		order_data['total']['amount'] = to_decimal(get_value_by_key_in_dict(order, 'grand_total', 0.0000))
		order_data['currency'] = order['store_currency_code']
		order_data['created_at'] = convert_format_time(order['created_at'])
		order_data['updated_at'] = convert_format_time(order['updated_at'])
		order_data['order_number'] = order['increment_id']

		order_customer = self.construct_order_customer()
		order_customer['id'] = order['customer_id']
		order_customer['email'] = order['customer_email']
		order_customer['first_name'] = order['customer_firstname']
		order_customer['last_name'] = order['customer_lastname']
		order_customer['middle_name'] = order['customer_middlename']
		order_data['customer'] = order_customer

		customer_address = self.construct_order_address()
		order_data['customer_address'] = customer_address

		order_address = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_address'], 'parent_id', order['entity_id'])
		billing_address = get_row_from_list_by_field(order_address, 'address_type', 'billing')
		order_billing = self.construct_order_address()
		if billing_address:
			order_billing['id'] = get_value_by_key_in_dict(billing_address, 'customer_address_id', '')
			order_billing['first_name'] = get_value_by_key_in_dict(billing_address, 'firstname', '') + ' ' + get_value_by_key_in_dict(billing_address, 'middlename', '')
			order_billing['last_name'] = billing_address['lastname']
			street = get_value_by_key_in_dict(billing_address, 'street', '').splitlines()
			order_billing['address_1'] = street[0] if to_len(street) > 0 else ''
			order_billing['address_2'] = street[1] if to_len(street) > 1 else ''
			order_billing['city'] = billing_address['city']
			order_billing['postcode'] = billing_address['postcode']
			order_billing['telephone'] = billing_address['telephone']
			order_billing['company'] = billing_address['company']
			if billing_address['country_id']:
				order_billing['country']['country_code'] = billing_address['country_id']
				order_billing['country']['name'] = self.get_country_name_by_code(billing_address['country_id'])
			else:
				order_billing['country']['country_code'] = 'US'
				order_billing['country']['name'] = 'United States'

			if billing_address['region_id']:
				order_billing['state']['name'] = billing_address['default_name'] if billing_address['default_name'] else ''
				order_billing['state']['state_code'] = billing_address['code'] if billing_address['code'] else ''
			else:
				order_billing['state']['name'] = billing_address['region'] if billing_address['region'] else ''
				order_billing['state']['state_code'] = ''

		order_data['billing_address'] = order_billing
		delivery_address = get_row_from_list_by_field(order_address, 'address_type', 'shipping')
		order_delivery = self.construct_order_address()
		if delivery_address:
			order_delivery['id'] = get_value_by_key_in_dict(delivery_address, 'customer_address_id', '')
			order_delivery['first_name'] = get_value_by_key_in_dict(delivery_address, 'firstname', '') + ' ' + get_value_by_key_in_dict(delivery_address, 'middlename', '')
			order_delivery['last_name'] = delivery_address['lastname']
			if delivery_address['street']:
				street_deli = delivery_address['street'].splitlines()
				order_delivery['address_1'] = street_deli[0]
				order_delivery['address_2'] = street_deli[1] if to_len(street_deli) > 1 else ''

			order_delivery['city'] = delivery_address['city']
			order_delivery['postcode'] = delivery_address['postcode']
			order_delivery['telephone'] = delivery_address['telephone']
			order_delivery['company'] = delivery_address['company']
			if delivery_address['country_id']:
				order_delivery['country']['country_code'] = delivery_address['country_id']
				order_delivery['country']['name'] = self.get_country_name_by_code(delivery_address['country_id'])
			else:
				order_delivery['country']['country_code'] = 'US'
				order_delivery['country']['name'] = 'United States'

			if delivery_address['region_id']:
				order_delivery['state']['name'] = delivery_address['default_name'] if delivery_address['default_name'] else ''
				order_delivery['state']['state_code'] = delivery_address['code'] if delivery_address['code'] else ''
			else:
				order_delivery['state']['name'] = delivery_address['region'] if delivery_address['region'] else ''
				order_delivery['state']['state_code'] = ''

		order_data['shipping_address'] = order_delivery

		order_payment = self.construct_order_payment()
		order_payment['title'] = get_row_value_from_list_by_field(orders_ext['data']['sales_flat_order_payment'], 'parent_id', order['entity_id'], 'method')
		order_data['payment'] = order_payment

		# Get product in order
		order_products = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_item'], 'order_id', order['entity_id'])
		order_parent_products = get_list_from_list_by_field(order_products, 'parent_item_id', None)
		order_items = list()
		for order_product in order_parent_products:
			order_item = self.construct_order_item()
			order_item['id'] = order_product['item_id']
			order_item['product']['id'] = order_product['product_id']
			order_item['product']['name'] = get_value_by_key_in_dict(order_product, 'name', '')
			order_item['product']['sku'] = get_value_by_key_in_dict(order_product, 'sku', '')
			# tax_class_id = 0
			# if order_item['product']['id'] and order_item['product']['id'] != '' and to_int(order_item['product']['id']) != 0:
			# 	if 'product_vat_id' in orders_ext['data'] and orders_ext['data']['product_vat_id']:
			# 		tax_class_id = get_row_value_from_list_by_field(orders_ext['data']['product_vat_id'], 'entity_id', order_item['product']['id'], 'value')
			# if tax_class_id and tax_class_id != 0:
			# 	order_item['price'] = self.convert_price(to_decimal(get_value_by_key_in_dict(order_product, 'original_price', 0.0000)), tax_class_id)
			# else:
			order_item['qty'] = to_int(get_value_by_key_in_dict(order_product, 'qty_ordered', 1))
			order_item['price'] = to_decimal(get_value_by_key_in_dict(order_product, 'price', 0.0000))
			order_item['original_price'] = to_decimal(get_value_by_key_in_dict(order_product, 'original_price', 0.0000))
			order_item['tax_amount'] = to_decimal(get_value_by_key_in_dict(order_product, 'tax_amount', 0.0000))
			order_item['tax_percent'] = 0.0000
			order_item['discount_amount'] = to_decimal(get_value_by_key_in_dict(order_product, 'discount_amount', 0.0000))
			order_item['discount_percent'] = 0.0000
			order_item['subtotal'] = to_decimal(order_item['price']) * to_decimal(order_item['qty'])
			order_item['total'] = to_decimal(order_item['subtotal']) + to_decimal(order_item['tax_amount']) - to_decimal(order_item['discount_amount'])
			if order_product['product_options']:
				options = php_unserialize(order_product['product_options'])
				if options and to_len(options) > 0:
					order_item_options = list()
					if 'options' in options:
						for key, option in options['options'].items():
							order_item_option = self.construct_order_item_option()
							order_item_option['option_name'] = option['label']
							order_item_option['option_value_name'] = option['value']
							order_item_options.append(order_item_option)

					if 'attributes_info' in options:
						for key, attr in options['attributes_info'].items():
							order_item_option = self.construct_order_item_option()
							order_item_option['option_name'] = attr['label']
							order_item_option['option_value_name'] = attr['value']
							order_item_options.append(order_item_option)
					if 'additional_options' in options:
						for key, attr in options['additional_options'].items():
							order_item_option = self.construct_order_item_option()
							order_item_option['option_name'] = attr['label']
							order_item_option['option_value_name'] = attr['value']
							order_item_options.append(order_item_option)
					order_item['options'] = order_item_options
			order_items.append(order_item)
		order_data['items'] = order_items

		# Get order history
		order_status_history = get_list_from_list_by_field(orders_ext['data']['sales_flat_order_status_history'], 'parent_id', order['entity_id'])
		for status_history in order_status_history:
			order_history = self.construct_order_history()
			# order_history = self.addConstructDefault(order_history)
			order_history['id'] = status_history['entity_id']
			order_history['status'] = status_history['status']
			order_history['comment'] = status_history['comment']
			order_history['created_at'] = status_history['created_at']
			order_history['notified'] = True if to_int(status_history['is_customer_notified']) == 1 else False
			order_history['visible_on_front'] = True if to_int(status_history.get('is_visible_on_front')) == 1 else False
			order_data['history'].append(order_history)

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
		self.import_data_connector(self.create_delete_query_connector('sales_order_item', {'order_id': order_id}))

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
				'qty_ordered': get_value_by_key_in_dict(item_order, 'qty'),
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
				'query': 'DELETE FROM _DBPRF_downloadable_link_purchased_item WHERE purchased_id IN (SELECT purchased_id FROM downloadable_link_purchased WHERE order_id = ' + to_str(order_id) + ')'
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
		# order = get_value_by_key_in_dict(convert, 'order', order)
		customer_id = None
		customer_group = None

		if convert['customer']['id']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'])
		if not customer_id:
			customer_id = None

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
		subtotal = 0

		for value in convert['items']:
			total_qty_ordered += to_int(to_decimal(value['qty'])) if value['qty'] else 0
			subtotal += to_decimal(value['subtotal']) if value['subtotal'] else 0
		currency_default = self._notice['target'].get('currency_default', 'USD')
		order_entity_data = {
			'state': self.get_order_state_by_order_status(order_status),
			'status': order_status,
			'coupon_code': get_value_by_key_in_dict(order, 'coupon_code'),
			'protect_code': get_value_by_key_in_dict(order, 'protect_code'),
			'shipping_description': get_value_by_key_in_dict(order, 'shipping_description'),
			'is_virtual': get_value_by_key_in_dict(order, 'is_virtual'),
			'store_id': store_id,
			'customer_id': customer_id,
			'base_discount_amount': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'base_discount_canceled': get_value_by_key_in_dict(order, 'base_discount_canceled'),
			'base_discount_invoiced': get_value_by_key_in_dict(order, 'base_discount_invoiced'),
			'base_discount_refunded': get_value_by_key_in_dict(order, 'base_discount_refunded'),
			'base_grand_total': convert['total']['amount'] if convert['total']['amount'] else None,
			'base_shipping_amount': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'base_shipping_canceled': get_value_by_key_in_dict(order, 'base_shipping_canceled'),
			'base_shipping_invoiced': get_value_by_key_in_dict(order, 'base_shipping_invoiced'),
			'base_shipping_refunded': get_value_by_key_in_dict(order, 'base_shipping_refunded'),
			'base_shipping_tax_amount': get_value_by_key_in_dict(order, 'base_shipping_tax_amount'),
			'base_shipping_tax_refunded': get_value_by_key_in_dict(order, 'base_shipping_tax_refunded'),
			'base_subtotal': convert['subtotal']['amount'] if convert['subtotal']['amount'] else None,
			'base_subtotal_canceled': get_value_by_key_in_dict(order, 'base_subtotal_canceled'),
			'base_subtotal_invoiced': get_value_by_key_in_dict(order, 'base_subtotal_invoiced'),
			'base_subtotal_refunded': get_value_by_key_in_dict(order, 'base_subtotal_refunded'),
			'base_tax_amount': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'base_tax_canceled': get_value_by_key_in_dict(order, 'base_tax_canceled'),
			'base_tax_invoiced': get_value_by_key_in_dict(order, 'base_tax_invoiced'),
			'base_tax_refunded': get_value_by_key_in_dict(order, 'base_tax_refunded'),
			'base_to_global_rate': get_value_by_key_in_dict(order, 'base_to_global_rate'),
			'base_to_order_rate': get_value_by_key_in_dict(order, 'base_to_order_rate'),
			'base_total_canceled': get_value_by_key_in_dict(order, 'base_total_canceled'),
			'base_total_invoiced': get_value_by_key_in_dict(order, 'base_total_invoiced'),
			'base_total_invoiced_cost': get_value_by_key_in_dict(order, 'base_total_invoiced_cost'),
			'base_total_offline_refunded': get_value_by_key_in_dict(order, 'base_total_offline_refunded'),
			'base_total_online_refunded': get_value_by_key_in_dict(order, 'base_total_online_refunded'),
			'base_total_paid': get_value_by_key_in_dict(order, 'base_total_paid'),
			'base_total_qty_ordered': get_value_by_key_in_dict(order, 'base_total_qty_ordered'),
			'base_total_refunded': get_value_by_key_in_dict(order, 'base_total_refunded'),
			'discount_amount': convert['discount']['amount'] if convert['discount']['amount'] else None,
			'discount_canceled': get_value_by_key_in_dict(order, 'discount_canceled'),
			'discount_invoiced': get_value_by_key_in_dict(order, 'discount_invoiced'),
			'discount_refunded': get_value_by_key_in_dict(order, 'discount_refunded'),
			'grand_total': convert['total']['amount'] if convert['total']['amount'] else None,
			'shipping_amount': convert['shipping']['amount'] if convert['shipping']['amount'] else None,
			'shipping_canceled': get_value_by_key_in_dict(order, 'shipping_canceled'),
			'shipping_invoiced': get_value_by_key_in_dict(order, 'shipping_invoiced'),
			'shipping_refunded': get_value_by_key_in_dict(order, 'shipping_refunded'),
			'shipping_tax_amount': get_value_by_key_in_dict(order, 'shipping_tax_amount'),
			'shipping_tax_refunded': get_value_by_key_in_dict(order, 'shipping_tax_refunded'),
			'store_to_base_rate': get_value_by_key_in_dict(order, 'store_to_base_rate'),
			'store_to_order_rate': get_value_by_key_in_dict(order, 'store_to_order_rate'),
			'subtotal': convert['subtotal']['amount'] if convert['subtotal']['amount'] else 0,
			'subtotal_canceled': get_value_by_key_in_dict(order, 'subtotal_canceled'),
			'subtotal_invoiced': get_value_by_key_in_dict(order, 'subtotal_invoiced'),
			'subtotal_refunded': get_value_by_key_in_dict(order, 'subtotal_refunded'),
			'tax_amount': convert['tax']['amount'] if convert['tax']['amount'] else None,
			'tax_canceled': get_value_by_key_in_dict(order, 'tax_canceled'),
			'tax_invoiced': get_value_by_key_in_dict(order, 'tax_invoiced'),
			'tax_refunded': get_value_by_key_in_dict(order, 'tax_refunded'),
			'total_canceled': get_value_by_key_in_dict(order, 'total_canceled'),
			'total_invoiced': get_value_by_key_in_dict(order, 'total_invoiced'),
			'total_offline_refunded': get_value_by_key_in_dict(order, 'total_offline_refunded'),
			'total_online_refunded': get_value_by_key_in_dict(order, 'total_online_refunded'),
			'total_paid': get_value_by_key_in_dict(order, 'total_paid'),
			'total_qty_ordered': total_qty_ordered if total_qty_ordered else None,
			'total_refunded': get_value_by_key_in_dict(order, 'total_refunded'),
			'can_ship_partially': get_value_by_key_in_dict(order, 'can_ship_partially'),
			'can_ship_partially_item': get_value_by_key_in_dict(order, 'can_ship_partially_item'),
			'customer_is_guest': get_value_by_key_in_dict(order, 'customer_is_guest'),
			'billing_address_id': get_value_by_key_in_dict(order, 'billing_address_id'),
			'customer_group_id': customer_group,
			'edit_increment': get_value_by_key_in_dict(order, 'edit_increment'),
			'email_sent': get_value_by_key_in_dict(order, 'email_sent'),
			'forced_shipment_with_invoice': get_value_by_key_in_dict(order, 'forced_shipment_with_invoice'),
			'payment_auth_expiration': get_value_by_key_in_dict(order, 'payment_auth_expiration'),
			'quote_address_id': None,
			'quote_id': None,
			'shipping_address_id': None,
			'adjustment_negative': get_value_by_key_in_dict(order, 'adjustment_negative'),
			'adjustment_positive': get_value_by_key_in_dict(order, 'adjustment_positive'),
			'base_adjustment_negative': get_value_by_key_in_dict(order, 'base_adjustment_negative'),
			'base_adjustment_positive': get_value_by_key_in_dict(order, 'base_adjustment_positive'),
			'base_shipping_discount_amount': get_value_by_key_in_dict(order, 'base_shipping_discount_amount', '0.0000'),
			'base_subtotal_incl_tax': get_value_by_key_in_dict(order, 'base_subtotal_incl_tax', '0.0000'),
			'base_total_due': convert['total']['amount'] if convert['total']['amount'] else None,
			'payment_authorization_amount': get_value_by_key_in_dict(order, 'payment_authorization_amount'),
			'shipping_discount_amount': get_value_by_key_in_dict(order, 'shipping_discount_amount', '0.0000'),
			'subtotal_incl_tax': get_value_by_key_in_dict(order, 'subtotal_incl_tax', '0.0000'),
			'total_due': convert['total']['amount'] if convert['total']['amount'] else None,
			'weight': get_value_by_key_in_dict(order, 'weight'),
			'customer_dob': get_value_by_key_in_dict(order, 'customer_dob'),
			'increment_id': get_value_by_key_in_dict(order, 'id'),
			'applied_rule_ids': get_value_by_key_in_dict(order, 'applied_rule_ids'),
			'base_currency_code': get_value_by_key_in_dict(order, 'base_currency_code', currency_default),
			'customer_email': convert['customer']['email'],
			'customer_firstname': convert['customer']['first_name'],
			'customer_lastname': convert['customer']['last_name'],
			'customer_middlename': convert['customer']['middle_name'],
			'customer_prefix': get_value_by_key_in_dict(order, 'customer_prefix'),
			'customer_suffix': get_value_by_key_in_dict(order, 'customer_suffix'),
			'customer_taxvat': get_value_by_key_in_dict(order, 'customer_taxvat'),
			'discount_description': get_value_by_key_in_dict(order, 'discount_description'),
			'ext_customer_id': None,
			'ext_order_id': None,
			'global_currency_code': get_value_by_key_in_dict(order, 'global_currency_code', currency_default),
			'hold_before_state': get_value_by_key_in_dict(order, 'hold_before_state'),
			'hold_before_status': get_value_by_key_in_dict(order, 'hold_before_status'),
			'order_currency_code': get_value_by_key_in_dict(order, 'order_currency_code', currency_default),
			'original_increment_id': get_value_by_key_in_dict(order, 'original_increment_id'),
			'relation_child_id': None,
			'relation_child_real_id': None,
			'relation_parent_id': None,
			'remote_ip': get_value_by_key_in_dict(order, 'remote_ip'),
			'shipping_method': get_value_by_key_in_dict(order, 'shipping_method'),
			'store_currency_code': get_value_by_key_in_dict(order, 'store_currency_code', currency_default),
			'store_name': store_name if store_name else None,
			'x_forwarded_for': str(get_value_by_key_in_dict(order, 'x_forwarded_for'))[:32],
			'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),
			'total_item_count': to_len(convert['items']),
			'customer_gender': get_value_by_key_in_dict(order, 'customer_gender'),
			'shipping_incl_tax': get_value_by_key_in_dict(order, 'shipping_incl_tax', '0.0000'),
			'base_shipping_incl_tax': get_value_by_key_in_dict(order, 'base_shipping_incl_tax', '0.0000'),
			'coupon_rule_name': get_value_by_key_in_dict(order, 'coupon_rule_name'),
			'gift_message_id': get_value_by_key_in_dict(order, 'gift_message_id'),
			'paypal_ipn_customer_notified': get_value_by_key_in_dict(order, 'paypal_ipn_customer_notified'),
		}

		if self._notice['config']['pre_ord'] and self._notice['src'].get('setup_type') != 'api':
			self.delete_target_order(convert['id'])
			order_entity_data['entity_id'] = convert['id']

		order_id = self.import_order_data_connector(
			self.create_insert_query_connector('sales_flat_order', order_entity_data), True, convert['id'])
		if not order_id:
			return response_error(self.warning_import_entity(self.TYPE_ORDER, convert['id']))
		self.insert_map(self.TYPE_ORDER, convert['id'], order_id)
		return response_success(order_id)

	def after_order_import(self, order_id, convert, order, orders_ext):
		# order = get_value_by_key_in_dict(convert, 'order', order)
		url_query = self.get_connector_url('query')
		all_queries = list()
		new_order_increment_id = '' + to_str(order_id)
		while to_len(new_order_increment_id) < 8:
			new_order_increment_id = '0' + new_order_increment_id
		new_order_increment_id = '1' + new_order_increment_id
		old_order_increment_id = convert.get('increment_id', convert['id'])
		if self._notice['config']['pre_ord']:
			order_increment_id = old_order_increment_id
		else:
			order_increment_id = new_order_increment_id

		customer = convert['customer']
		customer_id = None
		if customer['id']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, customer['id'])
		if not customer_id:
			customer_id = None

		if 'group_id' in convert['customer']:
			customer_group = self.get_map_customer_group(convert['customer']['group_id'])
			if not customer_group:
				customer_group = None

		try:
			order_status = self._notice['map']['order_status'][convert['status']]
		except Exception:
			order_status = 'canceled'
		order_state = self.get_order_state_by_order_status(order_status)

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

		customer_fullname = get_value_by_key_in_dict(customer, 'first_name', '') + get_value_by_key_in_dict(customer, 'middle_name', ' ') + get_value_by_key_in_dict(customer, 'last_name', '')
		billing_address = convert['billing_address']
		shipping_address = convert['shipping_address']
		billing_full_name = get_value_by_key_in_dict(billing_address, 'first_name', '') + get_value_by_key_in_dict(
			billing_address, 'middle_name', ' ') + get_value_by_key_in_dict(billing_address, 'last_name', '')
		shipping_full_name = get_value_by_key_in_dict(shipping_address, 'first_name', '') + get_value_by_key_in_dict(
			shipping_address, 'middle_name', ' ') + get_value_by_key_in_dict(shipping_address, 'last_name', '')

		billing_street = to_str(billing_address['address_1']) + "\n" + to_str(billing_address['address_2'])
		shipping_street = to_str(shipping_address['address_1']) + "\n" + to_str(shipping_address['address_2'])
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: sales order grip
		# begin
		currency_default = self._notice['target'].get('currency_default', 'USD')
		sales_order_grid_data = {
			'entity_id': order_id,
			'status': order_status,
			'store_id': store_id,
			'store_name': store_name,
			'customer_id': customer_id,
			'base_grand_total': convert['total']['amount'],
			'base_total_paid': None,
			'grand_total': convert['total']['amount'],
			'total_paid': None,
			'increment_id': order_increment_id,
			'base_currency_code': get_value_by_key_in_dict(order, 'base_currency_code', currency_default),
			'order_currency_code': get_value_by_key_in_dict(order, 'order_currency_code', currency_default),
			'shipping_name': shipping_full_name,
			'billing_name': billing_full_name,
			'created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
			'updated_at': get_value_by_key_in_dict(convert, 'updated_at', get_current_time()),
		}
		all_queries.append(self.create_insert_query_connector('sales_flat_order_grid', sales_order_grid_data))

		# end
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: order address
		# billing
		sales_order_address_billing_data = {
			'parent_id': order_id,
			'customer_address_id': None,
			'quote_address_id': None,
			'region_id': None,
			'customer_id': customer_id,
			'fax': billing_address['fax'],
			'region': billing_address['state']['name'],
			'postcode': billing_address['postcode'],
			'lastname': billing_address['last_name'],
			'street': billing_street,
			'city': billing_address['city'],
			'email': customer['email'],
			'telephone': billing_address['telephone'],
			'country_id': billing_address['country']['country_code'] if billing_address['country'][
				'country_code'] else 'US',
			'firstname': billing_address['first_name'],
			'address_type': 'billing',
			'prefix': None,
			'middlename': billing_address['middle_name'],
			'suffix': None,
			'company': billing_address['company'],
			'vat_id': None,
			'vat_is_valid': None,
			'vat_request_id': None,
			'vat_request_date': None,
			'vat_request_success': None,
		}
		sales_address_billing_id = self.import_order_data_connector(self.create_insert_query_connector('sales_flat_order_address', sales_order_address_billing_data))
		if not sales_address_billing_id:
			sales_address_billing_id = None
		# shipping
		sales_order_address_shipping_data = {
			'parent_id': order_id,
			'customer_address_id': None,
			'quote_address_id': None,
			'region_id': None,
			'customer_id': customer_id,
			'fax': shipping_address['fax'],
			'region': shipping_address['state']['name'],
			'postcode': shipping_address['postcode'],
			'lastname': shipping_address['last_name'],
			'street': shipping_street,
			'city': shipping_address['city'],
			'email': customer['email'],
			'telephone': shipping_address['telephone'],
			'country_id': shipping_address['country']['country_code'] if shipping_address['country'][
				'country_code'] else 'US',
			'firstname': shipping_address['first_name'],
			'address_type': 'shipping',
			'prefix': None,
			'middlename': shipping_address['middle_name'],
			'suffix': None,
			'company': shipping_address['company'],
			'vat_id': None,
			'vat_is_valid': None,
			'vat_request_id': None,
			'vat_request_date': None,
			'vat_request_success': None,
		}
		sales_address_shipping_id = self.import_order_data_connector(self.create_insert_query_connector('sales_flat_order_address', sales_order_address_shipping_data))
		if not sales_address_shipping_id:
			sales_address_shipping_id = None
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: payment
		payment = convert.get('payment', dict())
		sales_order_payment_data = {
			'parent_id': order_id,
			'base_shipping_captured': get_value_by_key_in_dict(payment, 'base_shipping_captured'),
			'shipping_captured': get_value_by_key_in_dict(payment, 'shipping_captured'),
			'amount_refunded': get_value_by_key_in_dict(payment, 'amount_refunded'),
			'base_amount_paid': get_value_by_key_in_dict(payment, 'base_amount_paid'),
			'amount_canceled': get_value_by_key_in_dict(payment, 'amount_canceled'),
			'base_amount_authorized': get_value_by_key_in_dict(payment, 'base_amount_authorized'),
			'base_amount_paid_online': get_value_by_key_in_dict(payment, 'base_amount_paid_online'),
			'base_amount_refunded_online': get_value_by_key_in_dict(payment, 'base_amount_refunded_online'),
			'base_shipping_amount': get_value_by_key_in_dict(payment, 'base_shipping_amount'),
			'shipping_amount': get_value_by_key_in_dict(payment, 'shipping_amount'),
			'amount_paid': get_value_by_key_in_dict(payment, 'amount_paid'),
			'amount_authorized': get_value_by_key_in_dict(payment, 'amount_authorized'),
			'base_amount_ordered': get_value_by_key_in_dict(payment, 'base_amount_ordered', convert['total']['amount']),
			'base_shipping_refunded': get_value_by_key_in_dict(payment, 'base_shipping_refunded'),
			'shipping_refunded': get_value_by_key_in_dict(payment, 'shipping_refunded'),
			'base_amount_refunded': get_value_by_key_in_dict(payment, 'base_amount_refunded'),
			'amount_ordered': get_value_by_key_in_dict(payment, 'base_amount_ordered', convert['total']['amount']),
			'base_amount_canceled': get_value_by_key_in_dict(payment, 'base_amount_canceled'),
			'quote_payment_id': None,
			'additional_data': None,
			'cc_exp_month': get_value_by_key_in_dict(payment, 'cc_exp_month'),
			'cc_ss_start_year': get_value_by_key_in_dict(payment, 'cc_ss_start_year'),
			'echeck_bank_name': get_value_by_key_in_dict(payment, 'echeck_bank_name'),
			'method': 'cashondelivery',
			'cc_debug_request_body': None,
			'cc_secure_verify': None,
			'protection_eligibility': None,
			'cc_approval': None,
			'cc_status_description': None,
			'echeck_type': None,
			'cc_debug_response_serialized': None,
			'cc_ss_start_month': '0',
			'echeck_account_type': None,
			'last_trans_id': get_value_by_key_in_dict(payment, 'last_trans_id', ''),

			'cc_cid_status': None,
			'cc_owner': None,
			'cc_type': None,
			'po_number': None,
			'cc_exp_year': '0',
			'cc_status': None,
			'echeck_routing_number': None,
			'account_status': None,
			'anet_trans_method': None,
			'cc_debug_response_body': None,
			'cc_ss_issue': None,
			'echeck_account_name': None,
			'cc_avs_status': None,
			'cc_number_enc': None,
			'cc_trans_id': None,
			'address_status': None,
			'additional_information': None,
		}
		all_queries.append(self.create_insert_query_connector('sales_flat_order_payment', sales_order_payment_data))
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: status history
		# begin
		for order_history in convert['history']:
			history_status = order_history['status'] if order_history['status'] else ''
			if history_status:
				try:
					history_status = self._notice['map']['order_status'][history_status]
				except Exception:
					history_status = 'pending'
			sales_order_status_history_data = {
				'parent_id': order_id,
				'is_customer_notified': 1,
				'is_visible_on_front': 0,
				'comment': order_history['comment'] if order_history['comment'] else '',
				'status': history_status,
				'created_at': order_history['created_at'] if order_history['created_at'] else '',
				'entity_name': 'order',
			}
			all_queries.append(self.create_insert_query_connector('sales_flat_order_status_history', sales_order_status_history_data))
		# end
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: order item
		# begin
		items_order = convert['items']
		item_queries = list()
		item_ids = list()
		map_item = dict()
		# for
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
				product_options = php_serialize(product_options)
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
				'qty_ordered': get_value_by_key_in_dict(item_order, 'qty'),
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
				'tax_canceled': get_value_by_key_in_dict(item_order, 'tax_canceled', 0),
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
			item_queries.append(self.create_insert_query_connector('sales_flat_order_item', sales_order_item_data, True))
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
				all_queries.append(self.create_update_query_connector('sales_flat_order_item', {'parent_item_id': parent_id}, {'item_id': child_id}))
		# end
		if 'link_purchased_data' in convert:
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
						purchased_item_id = self.import_order_data_connector(
							self.create_insert_query_connector('downloadable_link_purchased_item', item_data))
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: invoice
		# begin
		if 'invoice' in convert:
			invoices = convert['invoice']
			for invoice in invoices:
				invoice_data = invoice['data']
				sales_invoice_data = {
					'store_id': store_id,
					'base_grand_total': get_value_by_key_in_dict(invoice_data, 'base_grand_total'),
					'shipping_tax_amount': get_value_by_key_in_dict(invoice_data, 'shipping_tax_amount'),
					'tax_amount': get_value_by_key_in_dict(invoice_data, 'tax_amount'),
					'base_tax_amount': get_value_by_key_in_dict(invoice_data, 'base_tax_amount'),
					'store_to_order_rate': get_value_by_key_in_dict(invoice_data, 'store_to_order_rate'),
					'base_shipping_tax_amount': get_value_by_key_in_dict(invoice_data, 'base_shipping_tax_amount'),
					'base_discount_amount': get_value_by_key_in_dict(invoice_data, 'base_discount_amount'),
					'base_to_order_rate': get_value_by_key_in_dict(invoice_data, 'base_to_order_rate'),
					'grand_total': get_value_by_key_in_dict(invoice_data, 'grand_total'),
					'shipping_amount': get_value_by_key_in_dict(invoice_data, 'shipping_amount'),
					'subtotal_incl_tax': get_value_by_key_in_dict(invoice_data, 'subtotal_incl_tax'),
					'base_subtotal_incl_tax': get_value_by_key_in_dict(invoice_data, 'base_subtotal_incl_tax'),
					'store_to_base_rate': get_value_by_key_in_dict(invoice_data, 'store_to_base_rate'),
					'base_shipping_amount': get_value_by_key_in_dict(invoice_data, 'base_shipping_amount'),
					'total_qty': get_value_by_key_in_dict(invoice_data, 'total_qty'),
					'base_to_global_rate': get_value_by_key_in_dict(invoice_data, 'base_to_global_rate'),
					'subtotal': get_value_by_key_in_dict(invoice_data, 'subtotal'),
					'base_subtotal': get_value_by_key_in_dict(invoice_data, 'base_subtotal'),
					'discount_amount': get_value_by_key_in_dict(invoice_data, 'discount_amount'),
					'billing_address_id': sales_address_billing_id,
					'is_used_for_refund': get_value_by_key_in_dict(invoice_data, 'is_used_for_refund'),
					'order_id': order_id,
					'email_sent': get_value_by_key_in_dict(invoice_data, 'email_sent'),
					'can_void_flag': get_value_by_key_in_dict(invoice_data, 'can_void_flag'),
					'state': get_value_by_key_in_dict(invoice_data, 'state'),
					'shipping_address_id': sales_address_shipping_id,
					'store_currency_code': get_value_by_key_in_dict(invoice_data, 'store_currency_code', currency_default),
					'transaction_id': get_value_by_key_in_dict(invoice_data, 'transaction_id'),
					'order_currency_code': get_value_by_key_in_dict(invoice_data, 'order_currency_code', currency_default),
					'base_currency_code': get_value_by_key_in_dict(invoice_data, 'base_currency_code', currency_default),
					'global_currency_code': get_value_by_key_in_dict(invoice_data, 'global_currency_code', currency_default),
					'increment_id': None,
					'created_at': get_value_by_key_in_dict(invoice_data, 'created_at', get_current_time()),
					'updated_at': get_value_by_key_in_dict(invoice_data, 'updated_at', get_current_time()),
					'hidden_tax_amount': get_value_by_key_in_dict(invoice_data, 'hidden_tax_amount'),
					'base_hidden_tax_amount': get_value_by_key_in_dict(invoice_data, 'base_hidden_tax_amount'),
					'shipping_hidden_tax_amount': get_value_by_key_in_dict(invoice_data, 'shipping_hidden_tax_amount'),
					'base_shipping_hidden_tax_amnt': get_value_by_key_in_dict(invoice_data, 'base_shipping_hidden_tax_amnt'),
					'shipping_incl_tax': get_value_by_key_in_dict(invoice_data, 'shipping_incl_tax'),
					'base_shipping_incl_tax': get_value_by_key_in_dict(invoice_data, 'base_shipping_incl_tax'),
					'base_total_refunded': get_value_by_key_in_dict(order, 'base_total_refunded', None),
					'discount_description': get_value_by_key_in_dict(invoice_data, 'discount_description'),
				}
				sales_invoice_id = self.import_order_data_connector(
					self.create_insert_query_connector('sales_flat_invoice', sales_invoice_data))

				# if
				if sales_invoice_id:
					new_increment_id_invoice = '' + to_str(sales_invoice_id)
					while to_len(new_increment_id_invoice) < 8:
						new_increment_id_invoice = '0' + new_increment_id_invoice
					new_increment_id_invoice = '1' + new_increment_id_invoice
					old_increment_id_invoice = get_value_by_key_in_dict(invoice_data, 'increment_id', new_increment_id_invoice)

					if self._notice['config']['pre_ord']:
						increment_id_invoice = old_increment_id_invoice
					else:
						increment_id_invoice = new_increment_id_invoice
					all_queries.append(
						self.create_update_query_connector('sales_flat_invoice', {'increment_id': increment_id_invoice}, {'entity_id': sales_invoice_id}))
					# invoice grip
					sales_invoice_grid_data = {
						'entity_id': sales_invoice_id,
						'store_id': store_id,
						'base_grand_total': get_value_by_key_in_dict(invoice_data, 'base_grand_total'),
						'grand_total': get_value_by_key_in_dict(invoice_data, 'grand_total'),
						'state': get_value_by_key_in_dict(invoice_data, 'state'),
						'order_id': order_id,
						# 'customer_name': customer_fullname,
						# 'customer_email': customer['email'],
						# 'customer_group_id': customer_group,
						# 'payment_method': get_value_by_key_in_dict(convert, 'payment_method', 'cashondelivery'),
						'store_currency_code': get_value_by_key_in_dict(invoice_data, 'store_currency_code', currency_default),
						'order_currency_code': get_value_by_key_in_dict(invoice_data, 'order_currency_code', currency_default),
						'base_currency_code': get_value_by_key_in_dict(invoice_data, 'base_currency_code', currency_default),
						'global_currency_code': get_value_by_key_in_dict(invoice_data, 'global_currency_code', currency_default),
						'increment_id': increment_id_invoice,
						'order_increment_id': order_increment_id,
						'created_at': get_value_by_key_in_dict(invoice_data, 'created_at', get_current_time()),
						'order_created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
						'billing_name': billing_full_name,
						# 'billing_address': billing_street,
						# 'shipping_address': shipping_street,
						# 'shipping_information': None,
						# 'subtotal': get_value_by_key_in_dict(invoice_data, 'subtotal'),
						# 'shipping_and_handling': None,
						# 'updated_at': get_value_by_key_in_dict(invoice_data, 'updated_at', get_current_time()),
					}
					all_queries.append(
						self.create_insert_query_connector('sales_flat_invoice_grid', sales_invoice_grid_data))

					# invoice item
					invoice_item = get_value_by_key_in_dict(invoice, 'item', list())

					# for
					for item in invoice_item:
						order_item_src = item.get('order_item_id')
						order_item_id = map_item.get(str(order_item_src))
						if item['product_id']:
							product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item['product_id'])
							if not product_id:
								product_id = None
						else:
							product_id = None
						sales_invoice_item_data = {
							'parent_id': sales_invoice_id,
							'base_price': get_value_by_key_in_dict(item, 'base_price'),
							'tax_amount': get_value_by_key_in_dict(item, 'tax_amount'),
							'base_row_total': get_value_by_key_in_dict(item, 'base_row_total'),
							'discount_amount': get_value_by_key_in_dict(item, 'discount_amount'),
							'row_total': get_value_by_key_in_dict(item, 'row_total'),
							'base_discount_amount': get_value_by_key_in_dict(item, 'base_discount_amount'),
							'price_incl_tax': get_value_by_key_in_dict(item, 'price_incl_tax'),
							'base_tax_amount': get_value_by_key_in_dict(item, 'base_tax_amount'),
							'base_price_incl_tax': get_value_by_key_in_dict(item, 'base_price_incl_tax'),
							'qty': get_value_by_key_in_dict(item, 'qty'),
							'base_cost': get_value_by_key_in_dict(item, 'base_cost'),
							'price': get_value_by_key_in_dict(item, 'price'),
							'base_row_total_incl_tax': get_value_by_key_in_dict(item, 'base_row_total_incl_tax'),
							'row_total_incl_tax': get_value_by_key_in_dict(item, 'row_total_incl_tax '),
							'product_id': product_id,
							'order_item_id': order_item_id,
							'additional_data': get_value_by_key_in_dict(item, 'additional_data'),
							'description': get_value_by_key_in_dict(item, 'description'),
							'sku': get_value_by_key_in_dict(item, 'sku'),
							'name': get_value_by_key_in_dict(item, 'name'),
							'hidden_tax_amount': get_value_by_key_in_dict(item, 'hidden_tax_amount'),
							'base_hidden_tax_amount': get_value_by_key_in_dict(item, 'base_hidden_tax_amount'),
							'base_weee_tax_applied_amount': get_value_by_key_in_dict(item, 'base_weee_tax_applied_amount'),
							'base_weee_tax_applied_row_amnt': get_value_by_key_in_dict(item, 'base_weee_tax_applied_row_amnt'),
							'weee_tax_applied': None,
							'weee_tax_applied_amount': None,
							'weee_tax_applied_row_amount': None,
							'weee_tax_disposition': None,
							'weee_tax_row_disposition': None,
							'base_weee_tax_disposition': None,
							'base_weee_tax_row_disposition': None,
						}
						all_queries.append(
							self.create_insert_query_connector('sales_flat_invoice_item', sales_invoice_item_data))
					# endfor
					invoice_comment = get_value_by_key_in_dict(invoice, 'comment', list())
					# for
					for item in invoice_comment:
						sales_invoice_comment_data = {
							'parent_id': sales_invoice_id,
							'is_customer_notified': get_value_by_key_in_dict(item, 'is_customer_notified'),
							'is_visible_on_front': get_value_by_key_in_dict(item, 'is_visible_on_front', 0),
							'comment': get_value_by_key_in_dict(item, 'comment'),
							'created_at': get_value_by_key_in_dict(item, 'created_at', get_current_time()),

						}
						all_queries.append(
							self.create_insert_query_connector('sales_flat_invoice_comment', sales_invoice_comment_data))
		# endfor
		# endif
		# end
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: order shipment
		# begin
		if 'shipment' in convert:
			shipments = convert['shipment']
			for shipment in shipments:
				shipment_data = shipment['data']
				sales_shipment_data = {
					'store_id': store_id,
					'total_weight': get_value_by_key_in_dict(shipment_data, 'total_weight'),
					'total_qty': get_value_by_key_in_dict(shipment_data, 'total_qty'),
					'email_sent': get_value_by_key_in_dict(shipment_data, 'email_sent'),
					'order_id': order_id,
					'customer_id': customer_id,
					'shipping_address_id': sales_address_shipping_id,
					'billing_address_id': sales_address_billing_id,
					'shipment_status': get_value_by_key_in_dict(shipment_data, 'shipment_status'),
					'increment_id': None,
					'created_at': get_value_by_key_in_dict(shipment_data, 'created_at'),
					'updated_at': get_value_by_key_in_dict(shipment_data, 'updated_at'),
					'packages': get_value_by_key_in_dict(shipment_data, 'packages'),
					'shipping_label': get_value_by_key_in_dict(shipment_data, 'shipping_label'),

				}
				sales_shipment_id = self.import_order_data_connector(
					self.create_insert_query_connector('sales_flat_shipment', sales_shipment_data))

				# if
				if sales_shipment_id:
					new_increment_id_shipment = '' + to_str(sales_shipment_id)
					while to_len(new_increment_id_shipment) < 8:
						new_increment_id_shipment = '0' + new_increment_id_shipment
					new_increment_id_shipment = '1' + new_increment_id_shipment
					old_increment_id_shipment = get_value_by_key_in_dict(shipment_data, 'increment_id', new_increment_id_shipment)

					if self._notice['config']['pre_ord']:
						increment_id_shipment = old_increment_id_shipment
					else:
						increment_id_shipment = new_increment_id_shipment
					all_queries.append(self.create_update_query_connector('sales_flat_shipment', {'increment_id': increment_id_shipment}, {'entity_id': sales_shipment_id}))
					# shipment grip
					sales_shipment_grid_data = {
						'entity_id': sales_shipment_id,
						'store_id': store_id,
						'total_qty': get_value_by_key_in_dict(shipment_data, 'total_qty'),
						'order_id': order_id,
						'shipment_status': get_value_by_key_in_dict(shipment_data, 'shipment_status'),
						'increment_id': increment_id_shipment,
						'order_increment_id': order_increment_id,
						'created_at': get_value_by_key_in_dict(shipment_data, 'created_at'),
						'order_created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
						'shipping_name': shipping_full_name,
					}
					all_queries.append(self.create_insert_query_connector('sales_flat_shipment_grid', sales_shipment_grid_data))

					# shipment item
					shipment_item = get_value_by_key_in_dict(shipment, 'item', list())

					# for
					for item in shipment_item:
						order_item_src = item.get('order_item_id')
						order_item_id = map_item.get(str(order_item_src))
						if item['product_id']:
							product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item['product_id'])
							if not product_id:
								product_id = None
						else:
							product_id = None
						sales_shipment_item_data = {
							'parent_id': sales_shipment_id,
							'row_total': get_value_by_key_in_dict(item, 'row_total'),
							'price': get_value_by_key_in_dict(item, 'price'),
							'weight': get_value_by_key_in_dict(item, 'weight'),
							'qty': get_value_by_key_in_dict(item, 'qty'),
							'product_id': product_id,
							'order_item_id': order_item_id,
							'additional_data': get_value_by_key_in_dict(item, 'additional_data'),
							'description': get_value_by_key_in_dict(item, 'description'),
							'name': get_value_by_key_in_dict(item, 'name'),
							'sku': get_value_by_key_in_dict(item, 'sku'),

						}
						all_queries.append(
							self.create_insert_query_connector('sales_flat_shipment_item', sales_shipment_item_data))
					# endfor
					shipment_comment = get_value_by_key_in_dict(shipment, 'comment', list())
					# for
					for item in shipment_comment:
						sales_shipment_comment_data = {
							'parent_id': sales_shipment_id,
							'is_customer_notified': get_value_by_key_in_dict(item, 'is_customer_notified'),
							'is_visible_on_front': get_value_by_key_in_dict(item, 'is_visible_on_front', 0),
							'comment': get_value_by_key_in_dict(item, 'comment'),
							'created_at': get_value_by_key_in_dict(item, 'created_at', get_current_time()),

						}
						all_queries.append(
							self.create_insert_query_connector('sales_flat_shipment_comment', sales_shipment_comment_data))
		# endfor
		# endif
		# end
		# ------------------------------------------------------------------------------------------------------------------------
		# todo: creditmemo
		# begin
		if 'creditmemo' in convert:
			creditmemos = convert['creditmemo']
			for creditmemo in creditmemos:
				creditmemo_data = creditmemo['data']
				sales_creditmemo_data = {
					'store_id': store_id,
					'adjustment_positive': get_value_by_key_in_dict(creditmemo_data, 'adjustment_positive'),
					'base_shipping_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'base_shipping_tax_amount'),
					'store_to_order_rate': get_value_by_key_in_dict(creditmemo_data, 'store_to_order_rate'),
					'base_discount_amount': get_value_by_key_in_dict(creditmemo_data, 'base_discount_amount'),
					'base_to_order_rate': get_value_by_key_in_dict(creditmemo_data, 'base_to_order_rate'),
					'grand_total': get_value_by_key_in_dict(creditmemo_data, 'grand_total'),
					'base_adjustment_negative': get_value_by_key_in_dict(creditmemo_data, 'base_adjustment_negative'),
					'base_subtotal_incl_tax': get_value_by_key_in_dict(creditmemo_data, 'base_subtotal_incl_tax'),
					'shipping_amount': get_value_by_key_in_dict(creditmemo_data, 'shipping_amount'),
					'subtotal_incl_tax': get_value_by_key_in_dict(creditmemo_data, 'subtotal_incl_tax'),
					'adjustment_negative': get_value_by_key_in_dict(creditmemo_data, 'adjustment_negative'),
					'base_shipping_amount': get_value_by_key_in_dict(creditmemo_data, 'base_shipping_amount'),
					'store_to_base_rate': get_value_by_key_in_dict(creditmemo_data, 'store_to_base_rate'),
					'base_to_global_rate': get_value_by_key_in_dict(creditmemo_data, 'base_to_global_rate'),
					'base_adjustment': get_value_by_key_in_dict(creditmemo_data, 'base_adjustment'),
					'base_subtotal': get_value_by_key_in_dict(creditmemo_data, 'base_subtotal'),
					'discount_amount': get_value_by_key_in_dict(creditmemo_data, 'discount_amount'),
					'subtotal': get_value_by_key_in_dict(creditmemo_data, 'subtotal'),
					'adjustment': get_value_by_key_in_dict(creditmemo_data, 'adjustment'),
					'base_grand_total': get_value_by_key_in_dict(creditmemo_data, 'base_grand_total'),
					'base_adjustment_positive': get_value_by_key_in_dict(creditmemo_data, 'base_adjustment_positive'),
					'base_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'base_tax_amount'),
					'shipping_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'shipping_tax_amount'),
					'tax_amount': get_value_by_key_in_dict(creditmemo_data, 'tax_amount'),
					'order_id': order_id,
					'email_sent': get_value_by_key_in_dict(creditmemo_data, 'email_sent'),
					'creditmemo_status': get_value_by_key_in_dict(creditmemo_data, 'creditmemo_status'),
					'state': get_value_by_key_in_dict(creditmemo_data, 'state'),
					'shipping_address_id': sales_address_shipping_id,
					'billing_address_id': sales_address_billing_id,
					'invoice_id': None,
					'store_currency_code': get_value_by_key_in_dict(creditmemo_data, 'store_currency_code', currency_default),
					'order_currency_code': get_value_by_key_in_dict(creditmemo_data, 'order_currency_code', currency_default),
					'base_currency_code': get_value_by_key_in_dict(creditmemo_data, 'base_currency_code', currency_default),
					'global_currency_code': get_value_by_key_in_dict(creditmemo_data, 'global_currency_code', currency_default),
					'transaction_id': None,
					'increment_id': None,
					'created_at': get_value_by_key_in_dict(creditmemo_data, 'create_at', get_current_time()),
					'updated_at': get_value_by_key_in_dict(creditmemo_data, 'update_at', get_current_time()),
					'hidden_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'hidden_tax_amount'),
					'base_hidden_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'base_hidden_tax_amount'),
					'shipping_hidden_tax_amount': get_value_by_key_in_dict(creditmemo_data, 'shipping_hidden_tax_amount'),
					'base_shipping_hidden_tax_amnt': get_value_by_key_in_dict(creditmemo_data, 'base_shipping_hidden_tax_amnt'),
					'shipping_incl_tax': get_value_by_key_in_dict(creditmemo_data, 'shipping_incl_tax'),
					'base_shipping_incl_tax': get_value_by_key_in_dict(creditmemo_data, 'base_shipping_incl_tax'),
					'discount_description': get_value_by_key_in_dict(creditmemo_data, 'discount_description'),

				}
				sales_creditmemo_id = self.import_order_data_connector(
					self.create_insert_query_connector('sales_flat_creditmemo', sales_creditmemo_data))

				# if
				if sales_creditmemo_id:
					new_increment_id_creditmemo = '' + to_str(sales_creditmemo_id)
					while to_len(new_increment_id_creditmemo) < 8:
						new_increment_id_creditmemo = '0' + new_increment_id_creditmemo
					new_increment_id_creditmemo = '1' + new_increment_id_creditmemo
					old_increment_id_creditmemo = get_value_by_key_in_dict(creditmemo_data, 'increment_id', new_increment_id_creditmemo)

					if self._notice['config']['pre_ord']:
						increment_id_creditmemo = old_increment_id_creditmemo
					else:
						increment_id_creditmemo = new_increment_id_creditmemo
					all_queries.append(self.create_update_query_connector('sales_flat_creditmemo', {'increment_id': increment_id_creditmemo}, {'entity_id': sales_creditmemo_id}))
					# creditmemo grip
					sales_creditmemo_grid_data = {
						'entity_id': sales_creditmemo_id,
						'store_id': store_id,
						'store_to_order_rate': get_value_by_key_in_dict(creditmemo_data, 'store_to_order_rate'),
						'base_to_order_rate': get_value_by_key_in_dict(creditmemo_data, 'base_to_order_rate'),
						'grand_total': get_value_by_key_in_dict(creditmemo_data, 'grand_total'),
						'store_to_base_rate': get_value_by_key_in_dict(creditmemo_data, 'store_to_base_rate'),
						'base_to_global_rate': get_value_by_key_in_dict(creditmemo_data, 'base_to_global_rate'),
						'base_grand_total': get_value_by_key_in_dict(creditmemo_data, 'base_grand_total'),
						'order_id': order_id,
						'creditmemo_status': get_value_by_key_in_dict(creditmemo_data, 'creditmemo_status'),
						'state': get_value_by_key_in_dict(creditmemo_data, 'state'),
						'invoice_id': None,
						'store_currency_code': get_value_by_key_in_dict(creditmemo_data, 'store_currency_code', currency_default),
						'order_currency_code': get_value_by_key_in_dict(creditmemo_data, 'order_currency_code', currency_default),
						'base_currency_code': get_value_by_key_in_dict(creditmemo_data, 'base_currency_code', currency_default),
						'global_currency_code': get_value_by_key_in_dict(creditmemo_data, 'global_currency_code', currency_default),
						'increment_id': increment_id_creditmemo,
						'order_increment_id': order_increment_id,
						'created_at': get_value_by_key_in_dict(creditmemo_data, 'created_at'),
						'order_created_at': get_value_by_key_in_dict(convert, 'created_at', get_current_time()),
						'billing_name': billing_full_name,
					}
					all_queries.append(self.create_insert_query_connector('sales_flat_creditmemo_grid', sales_creditmemo_grid_data))

					# creditmemo item
					creditmemo_item = get_value_by_key_in_dict(creditmemo, 'item', list())

					# for
					for item in creditmemo_item:
						order_item_src = item.get('order_item_id')
						order_item_id = map_item.get(str(order_item_src))
						if item['product_id']:
							product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item['product_id'])
							if not product_id:
								product_id = None
						else:
							product_id = None
						sales_creditmemo_item_data = {
							'parent_id': sales_creditmemo_id,
							'base_price': get_value_by_key_in_dict(item, 'base_price'),
							'tax_amount': get_value_by_key_in_dict(item, 'tax_amount'),
							'base_row_total': get_value_by_key_in_dict(item, 'base_row_total'),
							'discount_amount': get_value_by_key_in_dict(item, 'discount_amount'),
							'row_total': get_value_by_key_in_dict(item, 'row_total'),
							'base_discount_amount': get_value_by_key_in_dict(item, 'base_discount_amount'),
							'price_incl_tax': get_value_by_key_in_dict(item, 'price_incl_tax'),
							'base_tax_amount': get_value_by_key_in_dict(item, 'base_tax_amount'),
							'base_price_incl_tax': get_value_by_key_in_dict(item, 'base_price_incl_tax'),
							'qty': get_value_by_key_in_dict(item, 'qty'),
							'base_cost': get_value_by_key_in_dict(item, 'base_cost'),
							'price': get_value_by_key_in_dict(item, 'price'),
							'base_row_total_incl_tax': get_value_by_key_in_dict(item, 'base_row_total_incl_tax'),
							'row_total_incl_tax': get_value_by_key_in_dict(item, 'row_total_incl_tax '),
							'product_id': product_id,
							'order_item_id': order_item_id,
							'additional_data': get_value_by_key_in_dict(item, 'additional_data'),
							'description': get_value_by_key_in_dict(item, 'description'),
							'sku': get_value_by_key_in_dict(item, 'sku'),
							'name': get_value_by_key_in_dict(item, 'name'),
							'hidden_tax_amount': get_value_by_key_in_dict(item, 'hidden_tax_amount'),
							'base_hidden_tax_amount': get_value_by_key_in_dict(item, 'base_hidden_tax_amount'),
							'weee_tax_disposition': None,
							'weee_tax_row_disposition': None,
							'base_weee_tax_disposition': None,
							'base_weee_tax_row_disposition': None,
							'weee_tax_applied': None,
							'base_weee_tax_applied_amount': None,
							'weee_tax_applied_row_amount': None,
							'weee_tax_applied_amount': None,
							'base_weee_tax_applied_row_amnt': None,

						}
						all_queries.append(
							self.create_insert_query_connector('sales_flat_creditmemo_item', sales_creditmemo_item_data))
					# endfor
					creditmemo_comment = get_value_by_key_in_dict(creditmemo, 'comment', list())
					# for
					for item in creditmemo_comment:
						sales_creditmemo_comment_data = {
							'parent_id': sales_creditmemo_id,
							'is_customer_notified': get_value_by_key_in_dict(item, 'is_customer_notified'),
							'is_visible_on_front': get_value_by_key_in_dict(item, 'is_visible_on_front', 0),
							'comment': get_value_by_key_in_dict(item, 'comment'),
							'created_at': get_value_by_key_in_dict(item, 'created_at', get_current_time()),
						}
						all_queries.append(self.create_insert_query_connector('sales_flat_creditmemo_comment', sales_creditmemo_comment_data))
		# endfor
		# endif
		# end

		order_update_data = {
			'increment_id': order_increment_id,
			'billing_address_id': sales_address_billing_id,
			'shipping_address_id': sales_address_shipping_id,
		}
		all_queries.append(self.create_update_query_connector('sales_flat_order', order_update_data, {
			'entity_id': order_id}))

		# ------------------------------------------------------------------------------------------------------------------------
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'order')
		return response_success()

	def addition_order_import(self, convert, order, orders_ext):
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
		reviews = self.select_data_connector(query, 'reviews')

		if not reviews or reviews['result'] != 'success':
			return response_error()
		return reviews

	def get_reviews_ext_export(self, reviews):
		review_ids = duplicate_field_value_from_list(reviews['data'], 'review_id')
		review_id_con = self.list_to_in_condition(review_ids)
		url_query = self.get_connector_url('query')
		order_ext_queries = {
			'review_detail': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_review_detail WHERE review_id IN " + review_id_con
			},
			'rating_option_vote': {
				'type': 'select',
				'query': "SELECT *, HEX(`remote_ip_long`) AS `remote_ip_long` FROM _DBPRF_rating_option_vote WHERE review_id IN " + review_id_con,
			},
			'review_store': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_review_store WHERE review_id IN " + review_id_con
			},

		}
		reviews_ext = self.select_multiple_data_connector(order_ext_queries)
		if not reviews_ext or reviews_ext['result'] != 'success':
			return response_error()
		return reviews_ext

	def convert_review_export(self, review, reviews_ext):
		review_data = self.construct_review()

		review_detail = get_row_from_list_by_field(reviews_ext['data']['review_detail'], 'review_id', review['review_id'])
		review_store = get_row_from_list_by_field(reviews_ext['data']['review_store'], 'review_id', review['review_id'])
		review_data['id'] = review['review_id']
		review_data['store_id'] = review_store.get('store_id', 0) if review_store else 0
		review_data['language_id'] = self._notice['src']['language_default']
		review_data['product']['id'] = review['entity_pk_value']
		if review_detail:
			review_data['customer']['id'] = review_detail['customer_id']
			review_data['customer']['name'] = review_detail['nickname']
			review_data['title'] = review_detail['title']
			review_data['content'] = review_detail['detail']

		review_data['status'] = 1 if to_int(review['status_id']) > 0 or to_int(review['status_id']) < 3 else 0
		review_data['created_at'] = review['created_at']
		review_data['updated_at'] = review['created_at']

		rating = self.construct_review_rating()
		rating_option_vote = get_list_from_list_by_field(reviews_ext['data']['rating_option_vote'], 'review_id', review['review_id'])
		rate_value = 0.0
		rating['rate_code'] = 'default'
		if rating_option_vote:
			for rating_value in rating_option_vote:
				rate_value += to_decimal(rating_value['value'])

			rating['rate'] = to_decimal(rate_value) / to_decimal(to_len(rating_option_vote))
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
		product_id = None
		if convert['product']['id']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'])
		if not product_id:
			self.log("Review id " + to_str(convert['id']) + ': ' + "product don't exist", 'reviews_errors')
			return response_error("Review id " + to_str(convert['id']) + ': ' + "product don't exist")
		review_data = {
			'entity_pk_value': product_id,
			'created_at': convert_format_time(convert['created_at']),
			'status_id': convert['status'] if convert['status'] else 1,
			'entity_id': 1,  # id type review in review_entity
		}
		review_id = self.import_review_data_connector(self.create_insert_query_connector('review', review_data), True, convert['id'])
		if not review_id:
			return response_warning('review id ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		return response_success(review_id)

	def after_review_import(self, review_id, convert, review, reviews_ext):
		all_queries = list()
		product_id = None
		if convert['product']['id']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'], convert['product']['code'])
		if (not product_id) and convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, convert['product']['code'])
		if not product_id:
			product_id = 0
		# multi_store = convert.get('multi_store', list())
		# store_target_ids = list()
		# for store_id in multi_store:
		# 	store_target_ids.append(self.get_map_store_view(store_id))
		# store_target_ids = list(set(store_target_ids))
		# for store_id in store_target_ids:
		review_store_data = {
			'review_id': review_id,
			'store_id': 0,
		}
		all_queries.append(self.create_insert_query_connector('review_store', review_store_data))
		review_store_data_1 = {
			'review_id': review_id,
			'store_id': 1,
		}
		all_queries.append(self.create_insert_query_connector('review_store', review_store_data_1))
		try:
			rating = convert['rating'][0]['rate']
		except Exception:
			rating = False
		if rating:
			rating_exist_data = {
				'entity_pk_value': product_id,
				'store_id': 0
			}
			primary_id = ''
			count = 0
			rating_summary = 0
			rating_exist = self.select_data_connector(self.create_select_query_connector('review_entity_summary', rating_exist_data))
			try:
				primary_id = rating_exist['data'][0]['primary_id']
			except Exception:
				pass
			if primary_id:
				count = to_int(rating_exist['data'][0]['reviews_count'])
				rating_summary = to_int(rating_exist['data'][0]['rating_summary'])
				value = round(((rating_summary * count + rating) / (count + 1)) / 10) * 10
				update_summary = {
					'reviews_count': count + 1,
					'rating_summary': value
				}
				all_queries.append(self.create_update_query_connector('review_entity_summary', update_summary, {'primary_id': primary_id}))
			else:
				review_entity_summary_data = {
					'entity_pk_value': product_id,
					'entity_type': 1,
					'reviews_count': 1,
					'rating_summary': convert['rating'][0]['rate'],
					'store_id': 0,
				}
				all_queries.append(
					self.create_insert_query_connector('review_entity_summary', review_entity_summary_data))
		customer_id = None
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'])
			if not customer_id and convert['customer']['code']:
				customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, None, convert['customer']['code'])
				if not customer_id:
					customer_id = 0
		review_detail_data = {
			'review_id': review_id,
			'store_id': 0,
			'title': convert['title'],
			'detail': convert['content'],
			'nickname': convert['customer']['name'],
			'customer_id': customer_id,
		}
		all_queries.append(self.create_insert_query_connector('review_detail', review_detail_data))
		# if convert.get('rating_vote'):
		# 	rating_vote = convert.get('rating_vote', list())
		# 	for vote in rating_vote:
		# 		remote_ip = get_value_by_key_in_dict(vote, 'remote_ip', '')
		# 		if remote_ip:
		# 			remote_ip_long = ip2long(remote_ip)
		# 		else:
		# 			remote_ip_long = None
		# 		percent = to_int(get_value_by_key_in_dict(vote, 'percent', 0))
		# 		value = round(percent / 100 * 5)
		# 		percent = round(value / 5 * 100)
		# 		rating_id = to_int(get_value_by_key_in_dict(vote, 'rating_id', 1))
		# 		rating_option_vote_data = {
		# 			'option_id': 5 * (rating_id - 1) + value,
		# 			'remote_ip': remote_ip,
		# 			'remote_ip_long': remote_ip_long,
		# 			'customer_id': customer_id,
		# 			'entity_pk_value': product_id,
		# 			'rating_id': rating_id,
		# 			'review_id': review_id,
		# 			'percent': percent,
		# 			'value': value,
		# 		}
		# 		all_queries.append(self.create_insert_query_connector('rating_option_vote', rating_option_vote_data))

		if all_queries:
			self.import_multiple_data_connector(all_queries, 'reviews')
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
		page_data['meta_keywords'] = self.convert_image_in_description(page['meta_keywords'])
		page_data['meta_description'] = self.convert_image_in_description(page['meta_description'])
		page_data['short_content'] = page['content_heading']
		page_data['url_key'] = page['identifier']
		page_data['status'] = True if to_int(page['is_active']) == 1 else False
		page_data['created_at'] = page['creation_time']
		page_data['updated_at'] = page['update_time']
		page_data['sort_order'] = to_int(page['sort_order'])
		page_data['store_ids'] = list()
		page_store = get_row_from_list_by_field(pages_ext['data']['cms_page_store'], 'page_id', page['page_id'])
		store = page_store.get('store_id')
		if to_int(store) > 0:
			page_data['store_ids'].append(store)
		else:
			for store in self._notice['src']['site']:
				page_data['store_ids'].append(store)
		return response_success(page_data)

	def get_page_id_import(self, convert, page, pages_ext):
		return page['page_id']

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
		id_src = self._notice['process']['blogs']['id_src']
		limit = self._notice['setting']['blogs']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_aw_blog WHERE "
			         " post_id > " + to_str(id_src) + " ORDER BY post_id ASC LIMIT " + to_str(limit)
		}
		blogs = self.select_data_connector(query, 'blogs')
		if not blogs or blogs['result'] != 'success':
			return response_error()
		return blogs

	def get_blogs_ext_export(self, blogs):
		blog_ids = duplicate_field_value_from_list(blogs['data'], 'post_id')
		blog_id_query = self.list_to_in_condition(blog_ids)
		blog_ext_queries = {
			'aw_blog_store': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_aw_blog_store WHERE post_id IN " + blog_id_query
			},
			'aw_blog_post_cat': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_aw_blog_post_cat as tb1 LEFT JOIN aw_blog_cat as tb2 ON tb1.cat_id = tb2.cat_id WHERE tb1.post_id IN " + blog_id_query
			},
			# 'comment': {
			# 	'type': 'select',
			# 	'query': "SELECT * FROM _DBPRF_aw_blog_comment WHERE post_id IN " + blog_id_query
			# },
		}

		blogs_ext = self.select_multiple_data_connector(blog_ext_queries, 'blogs')
		if not blogs_ext or (blogs_ext['result'] != 'success'):
			return response_error()
		cat_ids = duplicate_field_value_from_list(blogs_ext['data']['aw_blog_post_cat'], 'cat_id')
		cat_id_query = self.list_to_in_condition(cat_ids)
		blogs_ext_rel_queries = {
			'aw_blog_cat_store': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_aw_blog_cat_store WHERE cat_id IN " + cat_id_query
			},
		}
		blogs_ext_rel = self.select_multiple_data_connector(blogs_ext_rel_queries, 'blogs')

		if not blogs_ext_rel or blogs_ext_rel['result'] != 'success':
			return response_error()
		blogs_ext = self.sync_connector_object(blogs_ext, blogs_ext_rel)
		return blogs_ext

	def convert_blog_export(self, blog, blogs_ext):
		blog_data = self.construct_blog_post()
		blog_data['id'] = blog['post_id']
		blog_data['title'] = blog['title']
		blog_data['meta_description'] = blog['meta_description']
		blog_data['meta_keywords'] = blog['meta_keywords']
		blog_data['url_key'] = blog['identifier']
		blog_data['status'] = True if to_int(blog['status']) == 1 else False
		blog_data['created_at'] = blog['created_time']
		blog_data['updated_at'] = blog['update_time']
		blog_data['tags'] = blog['tags']
		blog_data['content'] = self.convert_image_in_description(blog['post_content'])
		blog_data['short_content'] = self.convert_image_in_description(blog['short_content'])
		# blog_comment = get_list_from_list_by_field(blogs_ext['data']['comment'], 'post_id', blog['post_id'])
		# for comment in blog_comment:
		# 	comment_data=self.construct_blog_post_comment()
		# 	comment_data['comment_id']=comment['comment_id']
		# 	comment_data['post_id']=comment['post_id']
		# 	comment_data['comment']=comment['comment']
		# 	comment_data['status']=comment['status']
		# 	comment_data['created_time']=comment['created_time']
		# 	comment_data['user']=comment['user']
		# 	comment_data['email']=comment['email']
		# 	blog_data['comment'].append(comment_data)
		blog_post_cat = get_list_from_list_by_field(blogs_ext['data']['aw_blog_post_cat'], 'post_id', blog['post_id'])
		if blog_post_cat:
			for blog_cat in blog_post_cat:
				blog_cat_data = self.construct_blog_category()
				blog_cat_data['id'] = blog_cat['cat_id']
				blog_cat_data['parent'] = self.construct_blog_category()
				blog_cat_data['name'] = blog_cat['title']
				blog_cat_data['url_key'] = blog_cat['identifier']
				blog_cat_data['sort_order'] = blog_cat['sort_order']
				blog_cat_data['meta_keyword'] = blog_cat['meta_keywords']
				blog_cat_data['meta_description'] = blog_cat['meta_description']
				blog_data['categories'].append(blog_cat_data)
		return response_success(blog_data)

	def get_blog_id_import(self, convert, blog, blogs_ext):
		return blog['post_id']

	def check_blog_import(self, convert, blog, blogs_ext):
		return False

	def router_blog_import(self, convert, blog, blogs_ext):
		return response_success('block_import')

	def before_blog_import(self, convert, blog, blogs_ext):
		return response_success()

	def blog_import(self, convert, blog, blogs_ext):
		return response_success(0)

	def after_blog_import(self, block_id, convert, blog, blogs_ext):
		return response_success()

	def addition_blog_import(self, convert, blog, blogs_ext):
		return response_success()

	# TODO: Coupon
	def prepare_coupons_import(self):
		return response_success()

	def prepare_coupons_export(self):
		return self

	def get_coupons_main_export(self):
		id_src = self._notice['process']['coupons']['id_src']
		limit = self._notice['setting'].get('coupons', 4)
		select_store = self._notice['src']['languages_select']
		website_id = 0
		if select_store:
			website_id = self._notice['src']['site'].get(select_store[0], 0)
		query = {
			'type': 'select',
			'query': "SELECT s.* FROM _DBPRF_salesrule as s left join _DBPRF_salesrule_coupon as sc on s.rule_id = sc.rule_id and sc.is_primary = 1 WHERE "
			         "(exists (select 1 from _DBPRF_salesrule_website as website where (website.website_id in ('" + to_str(website_id) + "')) and (s.rule_id = website.rule_id))) AND "
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
		coupon_data['created_at'] = convert_format_time(salesrule_coupon['created_at']) if salesrule_coupon.get('created_at') else ''
		coupon_data['from_date'] = convert_format_time(coupon['from_date'], '%Y-%m-%d')
		coupon_data['to_date'] = convert_format_time(coupon['to_date'], '%Y-%m-%d') if coupon.get('to_date') else convert_format_time(salesrule_coupon['expiration_date'] if salesrule_coupon.get('expiration_date') else get_current_time(), '%Y-%m-%d')
		coupon_data['times_used'] = salesrule_coupon.get('times_used')
		coupon_data['usage_limit'] = salesrule_coupon.get('usage_limit')
		coupon_data['discount_amount'] = coupon.get('discount_amount')
		coupon_data['usage_per_customer'] = salesrule_coupon.get('usage_per_customer')
		coupon_data['type'] = self.PERCENT if coupon['simple_action'] == 'by_percent' else self.FIXED
		if 'customer_group_ids' in coupon:
			customer_group = to_str(coupon['customer_group_ids']).split(',')
			customer_group_ids = list()
			for group in customer_group:
				group_id = self._notice['map']['customer_group'].get(to_str(group))
				if group_id not in customer_group_ids:
					customer_group_ids.append(group_id)
			coupon_data['customer_group'] = customer_group_ids
		elif coupons_ext['data'] and 'salesrule_customer_group' in coupons_ext['data']:
			customer_groups = get_list_from_list_by_field(coupons_ext['data']['salesrule_customer_group'], 'rule_id', coupon['rule_id'])
			customer_group_ids = list()
			for customer_group in customer_groups:
				group_id = self._notice['map']['customer_group'].get(to_str(customer_group['customer_group_id']))
				if group_id not in customer_group_ids:
					customer_group_ids.append(group_id)
			coupon_data['customer_group'] = customer_group_ids
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
				coupon_usage_data['timed_usage'] = coupon_usage['times_used']
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
		product_ids = convert.get('product_ids')
		if product_ids:
			product_id_arr = product_ids.split(',')
			product_id_map_arr = list()
			for product_id in product_id_arr:
				map_product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, product_id)
				if map_product_id:
					product_id_map_arr.append(to_str(map_product_id))
			if product_id_map_arr:
				product_ids = ','.join(product_id_map_arr)
			else:
				product_ids = None
		conditions = convert.get('conditions')
		if conditions:
			conditions = self.get_conditions_option(conditions)
			if conditions:
				conditions = self.magento_serialize(conditions)
		# conditions = conditions.replace('\\','\\\\')
		actions = convert.get('actions')
		if actions:
			actions = self.get_conditions_option(actions)
			if actions:
				actions = self.magento_serialize(actions)
		# actions = actions.replace('\\','\\\\')
		rule_data = {
			'name': get_value_by_key_in_dict(convert, 'name'),
			'description': get_value_by_key_in_dict(convert, 'description'),
			'from_date': get_value_by_key_in_dict(convert, 'from_date'),
			'to_date': get_value_by_key_in_dict(convert, 'to_date'),
			'uses_per_customer': get_value_by_key_in_dict(convert, 'uses_per_customer', 0),
			'is_active': get_value_by_key_in_dict(convert, 'is_active', 0),
			'conditions_serialized': conditions,
			'actions_serialized': actions,
			'product_ids': product_ids,
			'is_advanced': get_value_by_key_in_dict(convert, 'is_advanced', 1),
			'stop_rules_processing': get_value_by_key_in_dict(convert, 'stop_rules_processing', 1),
			'sort_order': get_value_by_key_in_dict(convert, 'sort_order', 0),
			'simple_action': get_value_by_key_in_dict(convert, 'simple_action'),
			'discount_qty': get_value_by_key_in_dict(convert, 'discount_qty'),
			'discount_amount': get_value_by_key_in_dict(convert, 'discount_amount', 0),
			'discount_step': get_value_by_key_in_dict(convert, 'discount_step', 0),
			'apply_to_shipping': get_value_by_key_in_dict(convert, 'apply_to_shipping', 0),
			'times_used': get_value_by_key_in_dict(convert, 'times_used', 0),
			'is_rss': get_value_by_key_in_dict(convert, 'is_rss', 0),
			'coupon_type': get_value_by_key_in_dict(convert, 'coupon_type', 1),
			'use_auto_generation': get_value_by_key_in_dict(convert, 'use_auto_generation', 0),
			'uses_per_coupon': get_value_by_key_in_dict(convert, 'uses_per_coupon', 0),
			'simple_free_shipping': get_value_by_key_in_dict(convert, 'simple_free_shipping', 0),

		}
		coupon_id = self.import_rule_data_connector(self.create_insert_query_connector('salesrule', rule_data), True, convert['id'])
		if not coupon_id:
			return response_error()
		self.insert_map(self.TYPE_COUPON, convert['id'], coupon_id)
		return response_success(coupon_id)

	def after_coupon_import(self, coupon_id, convert, coupon, coupons_ext):
		all_queries = list()
		if 'rule_website' in convert:
			website_ids = self.get_website_ids_target_by_id_src(convert['rule_website'])
			for website_id in website_ids:
				rule_website_data = {
					'rule_id': coupon_id,
					'website_id': website_id
				}
				all_queries.append(self.create_insert_query_connector('salesrule_website', rule_website_data))

		if 'customer' in convert:
			for customer in convert['customer']:
				customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, customer['customer_id'])
				if not customer_id:
					continue
				rule_customer_data = {
					'rule_id': coupon_id,
					'customer_id': customer_id,
					'times_used': get_value_by_key_in_dict(customer, 'times_used', 0)
				}
				all_queries.append(self.create_insert_query_connector('salesrule_customer', rule_customer_data))
		if 'customer_group' in convert:
			for customer_group in convert['customer_group']:
				rule_customer_group_data = {
					'rule_id': coupon_id,
					'customer_group_id': customer_group,
				}
				all_queries.append(self.create_insert_query_connector('salesrule_customer_group', rule_customer_group_data))

		if 'label' in convert:
			list_stores = list()
			for label in convert['label']:
				store_id = self.get_map_store_view(label['store_id'])
				if store_id in list_stores:
					continue
				list_stores.append(store_id)
				rule_label_data = {
					'rule_id': coupon_id,
					'store_id': store_id,
					'label': get_value_by_key_in_dict(label, 'label')
				}
				all_queries.append(
					self.create_insert_query_connector('salesrule_label', rule_label_data))

		if 'coupon' in convert:
			for coupon_row in convert['coupon']:
				coupon_data = coupon_row['data']
				coupon_code = get_value_by_key_in_dict(coupon_data, 'code')
				if not coupon_code:
					continue
				rule_coupon_data = {
					'rule_id': coupon_id,
					'code': coupon_code,
					'usage_limit': get_value_by_key_in_dict(coupon_data, 'usage_limit'),
					'usage_per_customer': get_value_by_key_in_dict(coupon_data, 'usage_per_customer'),
					'times_used': get_value_by_key_in_dict(coupon_data, 'times_used', 0),
					'expiration_date': get_value_by_key_in_dict(coupon_data, 'expiration_date'),
					'is_primary': get_value_by_key_in_dict(coupon_data, 'is_primary'),
					'created_at': get_value_by_key_in_dict(coupon_data, 'created_at'),
					'type': get_value_by_key_in_dict(coupon_data, 'type'),
				}
				coupon_row_id = self.import_rule_data_connector(self.create_insert_query_connector('salesrule_coupon', rule_coupon_data))
				if not coupon_row_id:
					continue
				if 'usage' in coupon:
					for usage in coupon['usage']:
						customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, usage['customer_id'])
						if not customer_id:
							continue
						coupon_usage_data = {
							'coupon_id': coupon_row_id,
							'customer_id': customer_id,
							'times_used': get_value_by_key_in_dict(usage, 'times_used', 0)
						}
						all_queries.append(self.create_insert_query_connector('salesrule_coupon_usage', coupon_usage_data))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'coupons')
		return response_success()

	def addition_coupon_import(self, convert, coupon, coupons_ext):
		return response_success()

	def get_default_language(self, stores):
		sort_order = 0
		if to_len(stores) > 0 and 'sort_order' in stores[0]:
			sort_order = stores[0]['sort_order']
		else:
			return 1
		for store in stores:
			if to_int(store['sort_order']) < to_int(sort_order):
				sort_order = store['sort_order']
		default_lang = 1
		for store in stores:
			if to_int(store['sort_order'] == to_int(sort_order)):
				default_lang = store['store_id']
		return default_lang

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

	def get_con_store_select(self):
		if not self._notice['support']['languages_select']:
			return ''
		select_store = self._notice['src']['languages_select'].copy()
		src_store = self._notice['src']['languages'].copy()
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		if not unselect_store:
			return ''
		select_store.append(0)
		if to_len(select_store) >= to_len(unselect_store):
			where = ' store_id IN ' + self.list_to_in_condition(select_store) + ' '
		else:
			where = ' store_id NOT IN ' + self.list_to_in_condition(unselect_store) + ' '

		return where

	def get_website_id_by_store_id_src(self, store_id):
		if to_int(store_id) == 0:
			return 0
		return self._notice['src'].get('store_site', dict()).get(to_str(store_id), 0)

	def get_con_store_select_count(self):
		if not self._notice['support']['languages_select']:
			return ' 1'
		select_store = self._notice['src']['languages_select'].copy()
		src_store = self._notice['src']['languages'].copy()
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		if not unselect_store:
			return ' 1'
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
		if not unselect_store:
			return ' 1'
		where = ' website_id IN ' + self.list_to_in_condition(select_website) + ' '
		return where

	def detect_seo(self):
		return 'default_seo'

	def categories_default_seo(self, category, categories_ext):
		result = list()
		key_entity = 'category_id'
		key_system = 'is_system'
		cat_desc = get_list_from_list_by_field(categories_ext['data']['url_rewrite'], key_entity, category['entity_id'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			if self._notice['support'].get('seo_301') and self._notice['config'].get('seo_301'):
				type_seo = self.SEO_301

			seo_cate = self.construct_seo_category()
			seo_cate['request_path'] = seo['request_path']
			seo_cate['default'] = to_int(seo[key_system]) == 1
			seo_cate['store_id'] = seo['store_id']
			seo_cate['type'] = type_seo
			result.append(seo_cate)
		return result

	def products_default_seo(self, product, products_ext):
		result = list()
		key_entity = 'product_id'
		key_system = 'is_system'
		cat_desc = get_list_from_list_by_field(products_ext['data']['url_rewrite'], key_entity, product['entity_id'])
		for seo in cat_desc:
			type_seo = self.SEO_DEFAULT
			if self._notice['support'].get('seo_301') and self._notice['config'].get('seo_301'):
				type_seo = self.SEO_301
			seo_product = self.construct_seo_product()
			seo_product['request_path'] = seo['request_path']
			seo_product['default'] = to_int(seo[key_system]) == 1
			seo_product['store_id'] = seo['store_id']
			seo_product['type'] = type_seo
			seo_product['category_id'] = seo['category_id']
			result.append(seo_product)
		return result

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
			description = description.replace(link[0], 'src="' + new_src + '"')
		return description

	def get_attribute_mst_brands(self):
		if self.attribute_mst_brands:
			return self.attribute_mst_brands
		brand_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['src']['extends'].get('brands', 9)),
			}
		}
		brand_eav_attribute = self.select_multiple_data_connector(brand_eav_attribute_queries)
		brand_eav_attribute_data = dict()
		for attribute in brand_eav_attribute['data']['eav_attribute']:
			brand_eav_attribute_data[attribute['attribute_code']] = attribute['attribute_id']
		self.attribute_mst_brands = brand_eav_attribute_data
		return self.attribute_mst_brands

	def create_attribute(self, attribute_code, backend_type, frontend_input, attribute_set_id = 4,
	                     frontend_label = None, entity_type_id = 4):
		all_query = list()
		eav_attribute_data = {
			'entity_type_id': 4,
			'attribute_code': attribute_code,
			'attribute_model': None,
			'backend_model': None,
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
		eav_attribute_id = self.import_data_connector(
			self.create_insert_query_connector('eav_attribute', eav_attribute_data), 'attribute')
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
			'is_configurable': 0,
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
		}
		all_query.append(self.create_insert_query_connector('catalog_eav_attribute', catalog_eav_attribute_data))

		self.import_multiple_data_connector(all_query, 'attribute')
		return eav_attribute_id

	def check_option_exist(self, option_value, attribute_code, store_id = None):
		option_value = self.escape(option_value)
		query = "SELECT a.option_id FROM _DBPRF_eav_attribute_option_value as a, _DBPRF_eav_attribute_option as b, _DBPRF_eav_attribute as c  WHERE a.value = " + option_value + " AND a.option_id = b.option_id AND b.attribute_id = c.attribute_id AND c.attribute_code = " + self.escape(attribute_code)
		if store_id:
			query += ' AND a.store_id = ' + to_str(store_id)
		res = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': query
			})
		})
		try:
			option_id = res['data'][0]['option_id']
		except Exception:
			option_id = False
		return option_id

	def get_attribute_category(self):
		if self.attribute_category:
			return self.attribute_category
		category_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['catalog_category']),
			}
		}
		category_eav_attribute = self.select_multiple_data_connector(category_eav_attribute_queries)
		category_eav_attribute_data = dict()
		for attribute in category_eav_attribute['data']['eav_attribute']:
			if attribute['backend_type'] != 'static':
				if not attribute['attribute_code'] in category_eav_attribute_data:
					category_eav_attribute_data[attribute['attribute_code']] = dict()
				category_eav_attribute_data[attribute['attribute_code']]['attribute_id'] = attribute['attribute_id']
				category_eav_attribute_data[attribute['attribute_code']]['backend_type'] = attribute['backend_type']
		self.attribute_category = category_eav_attribute_data
		return self.attribute_category

	def make_magento_image_path(self, image):
		image = os.path.basename(image)
		path = ''
		if to_len(image) >= 1:
			path = image[0]
			if to_len(image) >= 2:
				path = path + '/' + image[1]
		return path + '/'

	def get_category_url_key(self, url_key, store_id, name):
		if not url_key:
			url_key = self.generate_url_key(name)
		index = 0
		cur_url_key = url_key
		while self.check_exist_url_key(cur_url_key, store_id, 'category'):
			index += 1
			cur_url_key = url_key + '-' + to_str(index)
		return cur_url_key

	def generate_url_key(self, name):
		if not name:
			return ''
		name = to_str(name).lower()
		url_key = re.sub('[^0-9a-z]', '-', to_str(name))
		url_key = to_str(url_key).strip(' -')
		while to_str(url_key).find('--') != -1:
			url_key = to_str(url_key).replace('--', '-')
		return url_key

	def check_exist_url_key(self, url_key, store_id = 0, type_url = 'product'):
		query = "SELECT cp.* FROM _DBPRF_catalog_" + type_url + "_entity_varchar AS cp JOIN _DBPRF_eav_attribute AS ea ON ea.attribute_id = cp.attribute_id WHERE ea.attribute_code = 'url_key' AND cp.value = '" + url_key + "' AND cp.store_id = '" + to_str(
			store_id) + "'"
		res = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': query
			})
		})
		if (not res) or (res['result'] != 'success') or (not ('data' in res)):
			return False
		return to_len(res['data']) > 0

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

	def get_map_store_view(self, src_store):
		if src_store is None or to_str(src_store) == '0':
			return 0
		return self._notice['map']['languages'].get(to_str(src_store), self._notice['map']['languages'].get(
			self._notice['src']['language_default'], 0))

	def check_exist_url_path(self, url_path, store_id, type_url = 'product'):
		query = "SELECT cp.* FROM _DBPRF_catalog_" + type_url + "_entity_varchar AS cp JOIN _DBPRF_eav_attribute AS ea ON ea.attribute_id = cp.attribute_id WHERE ea.attribute_code = 'url_path' AND cp.value = '" + url_path + "' AND cp.store_id = '" + to_str(
			store_id) + "'"
		res = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select', 'query': query
			})
		})
		if (not res) or (res['result'] != 'success') or (not ('data' in res)):
			return False
		return to_len(res['data']) > 0 if res['data'] else False

	def get_request_path(self, request_path, store_id = 0, seo_type = 'product'):
		if not request_path:
			return ''
		has_suffix = False
		no_suffix = ''
		suffix = ''
		if to_str(request_path).find('.') != -1:
			list_request_path = to_str(request_path).split('.')
			suffix = list_request_path[to_len(list_request_path) - 1]
			has_suffix = True
			no_suffix = to_str(request_path).replace('.' + suffix, '')
		cur_request_path = to_str(request_path)
		index = 0
		while self.check_exist_request_path(cur_request_path, store_id, seo_type):
			index += 1
			if has_suffix:
				cur_request_path = no_suffix + '-' + to_str(index) + '.' + suffix
			else:
				cur_request_path = to_str(request_path) + '-' + to_str(index)
		return cur_request_path

	def check_exist_request_path(self, request_path, store_id = 0, seo_type = 'product'):
		where = {
			'request_path': request_path,
			# 'entity_type': seo_type,
			'store_id': store_id
		}
		url_data = self.get_connector_data(self.get_connector_url('query'), {
			'query': json.dumps({
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_core_url_rewrite WHERE " + self.dict_to_where_condition(where)
			})
		})
		if (not url_data) or (url_data['result'] != 'success') or (not ('data' in url_data)):
			return False
		return to_len(url_data['data']) > 0

	def check_attribute_sync(self, src_attr, target_attr):
		if (src_attr.get('backend_type') == 'static') or (to_str(src_attr.get('is_user_defined')) == '0'):
			return True
		if src_attr.get('frontend_input') == 'select':
			if target_attr.get('frontend_input') == 'select':
				return True
			if (src_attr.get('source_model') == 'eav/entity_attribute_source_boolean' or src_attr.get('is_boolean')) and target_attr.get('frontend_input') == 'boolean':
				return True
			return False
		field_check = ['frontend_input', 'backend_type']
		for field in field_check:
			if field == 'backend_type':
				if src_attr['frontend_input'] == 'multiselect':
					if src_attr['backend_type'] in ['varchar', 'text', 'textarea'] and target_attr['backend_type'] in ['textarea', 'varchar', 'text']:
						continue
			if (not (field in src_attr)) or (not (field in target_attr)) or (src_attr[field] != target_attr[field]):
				return False
		return True

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

	def import_attribute_data_connector(self, query, primary = False, entity_id = False, params = True):
		return self.import_data_connector(query, 'attributes', entity_id, primary, params)

	def create_attribute_set(self, name):
		attribute_set_data = {
			'entity_type_id': self._notice['target']['extends']['catalog_product'],
			'attribute_set_name': name,
			'sort_order': 0,
		}
		attribute_set_id = self.import_attribute_data_connector(
			self.create_insert_query_connector('eav_attribute_set', attribute_set_data))
		if not attribute_set_id:
			return False
		all_queries = list()
		attribute_group = [
			{
				'attribute_group_name': 'General',
				'default_id': 1,
				'attributes': [
					'status', 'name', 'sku', 'weight', 'visibility', 'news_from_date', 'news_to_date', 'description', 'short_description', 'url_key', 'country_of_manufacture'
				],
			},
			{
				'attribute_group_name': 'Bundle Items',
				'default_id': 0,
				'attributes': [
					'shipment_type'
				]
			},
			{
				'attribute_group_name': 'Images',
				'default_id': 0,
				'attributes': [
					'image', 'small_image', 'thumbnail', 'media_gallery', 'gallery'
				],
			},
			{
				'attribute_group_name': 'Meta Information',
				'default_id': 0,
				'attributes': [
					'meta_title', 'meta_keyword', 'meta_description'
				],
			},
			{
				'attribute_group_name': 'Prices',
				'default_id': 0,
				'attributes': [
					'price', 'tier_price', 'price_view', 'group_price', 'special_price', 'special_from_date', 'special_to_date', 'cost', 'msrp_enabled', 'msrp_display_actual_price_type', 'msrp', 'tax_class_id'
				],
			},
			{
				'attribute_group_name': 'Design',
				'default_id': 0,
			},
			{
				'attribute_group_name': 'Schedule Design Update',
				'default_id': 0,
				'attributes': [
					'custom_design_from', 'custom_design_to', 'custom_design', 'custom_layout_update', 'page_layout', 'options_container'
				],
			},
			{
				'attribute_group_name': 'Gift Options',
				'default_id': 0,
				'attributes': ['gift_message_available']
			},
		]

		product_eav_attribute_queries = {
			'eav_attribute': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['catalog_product'])
			}
		}
		product_eav_attribute = self.select_multiple_data_connector(product_eav_attribute_queries)
		product_eav_attribute_data = dict()
		for attribute in product_eav_attribute['data']['eav_attribute']:
			product_eav_attribute_data[attribute['attribute_code']] = attribute['attribute_id']
		index = 1
		for attribute_group_data in attribute_group:
			attribute_group_data['sort_order'] = index
			attribute_group_data['attribute_set_id'] = attribute_set_id
			attributes = list()
			if 'attributes' in attribute_group_data:
				attributes = attribute_group_data['attributes']
				del attribute_group_data['attributes']
			if not attributes:
				all_queries.append(self.create_insert_query_connector('eav_attribute_group', attribute_group_data))
			else:
				attribute_group_id = self.import_attribute_data_connector(
					self.create_insert_query_connector('eav_attribute_group', attribute_group_data))
				if not attribute_group_id:
					continue
				for attribute in attributes:
					attribute_id = None
					if attribute in product_eav_attribute_data:
						attribute_id = product_eav_attribute_data[attribute]
					if not attribute_id:
						continue
					eav_entity_attribute = {
						'entity_type_id': self._notice['target']['extends']['catalog_product'],
						'attribute_set_id': attribute_set_id,
						'attribute_group_id': attribute_group_id,
						'attribute_id': attribute_id,
						'sort_order': 0
					}
					all_queries.append(self.create_insert_query_connector('eav_entity_attribute', eav_entity_attribute))
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'attr', True)

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

	def get_product_url_key(self, url_key, store_id, name):
		if not url_key:
			url_key = self.generate_url_key(name)
		index = 0
		cur_url_key = url_key
		while self.check_exist_url_key(cur_url_key, store_id):
			index += 1
			cur_url_key = url_key + '-' + to_str(index)
		return cur_url_key

	def get_product_link_attribute_id(self, link_type_id):
		return link_type_id
		if to_int(link_type_id) == 1:
			return 1
		if to_int(link_type_id) == 3:
			return 2
		if to_int(link_type_id) == 4:
			return 3
		return 1

	def import_multiple_seo_data_connector(self, queries, import_type = 'query', is_log = True):
		all_import = self.get_connector_data(self.get_connector_url('query'), {
			'serialize': True,
			'query': json.dumps(queries),
		})
		if (not all_import) or (not all_import['data']):
			return list()
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
		# log err
		return all_import['data']

	def get_website_id_by_store_id(self, store_id):
		if to_int(store_id) == 0:
			return 0
		return self._notice['target']['site'].get(store_id, 0)

	def delete_target_customer(self, customer_id):
		if not customer_id:
			return True
		queries_delete = {
			'customer_address_entity': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_customer_address_entity WHERE parent_id = " + to_str(customer_id)
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

	def delete_target_order(self, order_ids):
		if not order_ids:
			return True
		if not isinstance(order_ids, list):
			order_ids = [order_ids]
		order_id_con = self.list_to_in_condition(order_ids)
		url_query = self.get_connector_url('query')
		delete_queries = {
			'sales_flat_order_item': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order_item WHERE order_id IN " + order_id_con,
			},
			'sales_flat_order_status_history': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order_status_history WHERE parent_id IN " + order_id_con,
			},

			'sales_flat_order_payment': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order_payment WHERE parent_id IN " + order_id_con,
			},
			'sales_flat_order_address': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order_address WHERE parent_id IN " + order_id_con,
			},
			'sales_flat_order_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order_grid WHERE entity_id IN " + order_id_con,
			},
			'sales_flat_shipment_item': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_shipment_item WHERE parent_id IN (" + "SELECT entity_id FROM _DBPRF_sales_flat_shipment WHERE order_id IN " + order_id_con + ')',
			},
			'sales_flat_invoice_item': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_invoice_item WHERE parent_id IN (" + "SELECT entity_id FROM _DBPRF_sales_flat_invoice WHERE order_id IN " + order_id_con + ')',
			},
			'sales_flat_creditmemo_item': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_creditmemo_item WHERE parent_id IN (" + "SELECT entity_id FROM _DBPRF_sales_flat_creditmemo WHERE order_id IN " + order_id_con + ')',
			},
			'sales_flat_invoice_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_invoice_grid WHERE order_id IN " + order_id_con,
			},
			'sales_flat_invoice': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_invoice WHERE order_id IN " + order_id_con,
			},
			'sales_flat_shipment_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_shipment_grid WHERE order_id IN " + order_id_con,
			},
			'sales_flat_shipment': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_shipment WHERE order_id IN " + order_id_con,
			},
			'sales_flat_creditmemo_grid': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_creditmemo_grid WHERE order_id IN " + order_id_con,
			},
			'sales_flat_creditmemo': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_creditmemo WHERE order_id IN " + order_id_con,
			},
			'sales_flat_order': {
				'type': 'query',
				'query': "DELETE FROM _DBPRF_sales_flat_order WHERE entity_id IN " + order_id_con,
			},
		}
		return self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(delete_queries)
		})

	def get_attribute_customer_address(self):
		if self.attribute_customer_address:
			return self.attribute_customer_address
		customer_address_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['customer_address']),
			}
		}
		customer_address_eav_attribute = self.select_multiple_data_connector(customer_address_eav_attribute_queries)
		customer_address_eav_attribute_data = dict()
		for attribute in customer_address_eav_attribute['data']['eav_attribute']:
			if attribute['backend_type'] != 'static':
				if not attribute['attribute_code'] in customer_address_eav_attribute_data:
					customer_address_eav_attribute_data[attribute['attribute_code']] = dict()
				customer_address_eav_attribute_data[attribute['attribute_code']]['attribute_id'] = attribute['attribute_id']
				customer_address_eav_attribute_data[attribute['attribute_code']]['backend_type'] = attribute['backend_type']
		self.attribute_customer_address = customer_address_eav_attribute_data
		return self.attribute_customer_address

	def get_region_id_by_state_name(self, state_name):
		query = self.create_select_query_connector('directory_country_region', {
			'default_name': state_name
		})
		regions = self.select_data_connector(query, 'customers')
		if regions['result'] == 'success' and regions['data'] and to_len(regions['data']) > 0 and regions['data'][0].get(
				'region_id'):
			return regions['data'][0]['region_id']
		return 57

	def get_attribute_customer(self):
		if self.attribute_customer:
			return self.attribute_customer
		customer_eav_attribute_queries = {
			'eav_attribute': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_eav_attribute WHERE entity_type_id = " + to_str(self._notice['target']['extends']['customer']),
			}
		}
		customer_eav_attribute = self.select_multiple_data_connector(customer_eav_attribute_queries)
		customer_eav_attribute_data = dict()
		for attribute in customer_eav_attribute['data']['eav_attribute']:
			if attribute['backend_type'] != 'static':
				if not attribute['attribute_code'] in customer_eav_attribute_data:
					customer_eav_attribute_data[attribute['attribute_code']] = dict()
				customer_eav_attribute_data[attribute['attribute_code']]['attribute_id'] = attribute['attribute_id']
				customer_eav_attribute_data[attribute['attribute_code']]['backend_type'] = attribute['backend_type']
		self.attribute_customer = customer_eav_attribute_data
		return self.attribute_customer

	def magento_serialize(self, obj):
		res = False
		try:
			if self.convert_version(self._notice['target']['config']['version'], 2) < 220:
				res = php_serialize(obj)
			else:
				res = json_encode(obj)
		except Exception:
			res = False
		return res

	def get_conditions_option(self, conditions):
		if not conditions:
			return None
		if 'conditions' in conditions:
			type_option = 'conditions'
		else:
			type_option = 'actions'
		attributes = ['base_subtotal',
		              'qty',
		              'base_row_total',
		              'quote_item_price',
		              'quote_item_qty',
		              'quote_item_row_total',
		              'total_qty',
		              'weight',
		              'shipping_method',
		              'postcode',
		              'region',
		              'region_id',
		              'country_id']
		if conditions['attribute'] == 'category_ids':
			category_ids = to_str(conditions['value']).split(',')
			if category_ids:
				category_ids_desc = list()
				for category_id in category_ids:
					if self._notice['map']['category_data'].get(to_str(category_id)):
						category_ids_desc.append(self._notice['map']['category_data'].get(to_str(category_id)))
					else:
						map_cate = self.get_map_field_by_src(self.TYPE_CATEGORY, category_id)
						if map_cate:
							category_ids_desc.append(to_str(map_cate))
				category_ids = ','.join(category_ids_desc)
				conditions['value'] = category_ids
		elif conditions['attribute'] == 'attribute_set_id':
			if to_str(conditions['value']) in self._notice['map']['attributes']:
				conditions['value'] = self._notice['map']['attributes'][to_str(conditions['value'])]
			else:
				conditions['value'] = None
		else:
			if conditions['attribute'] and conditions['attribute'] not in attributes:
				return None
		conditions['type'] = self.get_type_rule(conditions['type'])
		if type_option and type_option in conditions:
			if isinstance(conditions[type_option], list) or isinstance(conditions[type_option], dict):
				conditions_type = self.list_to_dict(conditions[type_option])
				new_conditions_type = dict()
				for key_option, value in conditions_type.items():
					condition = self.get_conditions_option(value)
					if condition:
						new_conditions_type[key_option] = condition
				conditions[type_option] = new_conditions_type
			else:
				condition = self.get_conditions_option(conditions[type_option])
				if condition:
					conditions[type_option] = condition
				else:
					del conditions[type_option]
		return conditions

	def get_type_rule(self, type_rule):
		if '_' not in type_rule:
			return type_rule
		types = type_rule.split('/')
		models = types[1].split('_')
		res = 'Magento\\' + to_str(types[0]).capitalize() + '\\Model'
		for model in models:
			res += '\\' + to_str(model).capitalize()
		res = res.replace('Salesrule', 'SalesRule')
		res = res.replace('Catalogrule', 'CatalogRule')
		return res

	def import_rule_data_connector(self, query, primary = False, entity_id = False, params = True):
		return self.import_data_connector(query, 'coupons', entity_id, primary, params)

	def import_seo_data_connector(self, query, primary = False, entity_id = False, params = True):
		return self.import_data_connector(query, 'seo', entity_id, primary, params)

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

	def create_attribute_code(self, attribute_name):
		attribute_name = to_str(attribute_name).lower()
		attribute_name = to_str(attribute_name).replace(' ', '_')
		return attribute_name

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

	def get_type_relation_product(self, src_type):
		target_type = {
			self.PRODUCT_RELATE: 1,
			self.PRODUCT_UPSELL: 4,
			self.PRODUCT_CROSS: 5,
		}
		return target_type.get(src_type, 1) if src_type else 1

	def get_all_tax_rate(self):
		if self.all_tax_rate:
			return self.all_tax_rate
		country = self._notice['src']['config'].get('shipping_country')
		query = 'SELECT * FROM _DBPRF_tax_calculation as t_cal LEFT JOIN _DBPRF_tax_calculation_rate as t_rate on t_rate.tax_calculation_rate_id = t_cal.tax_calculation_rate_id '
		if country:
			query += 'WHERE t_rate.tax_country_id = ' + self.escape(country)
		res = self.select_data_connector({
			'type': 'select',
			'query': query,
		})
		if res['result'] == 'success' and res['data']:
			for data in res['data']:
				if not data['rate']:
					continue
				if not self.all_tax_rate.get(to_str(data['product_tax_class_id'])):
					self.all_tax_rate[to_str(data['product_tax_class_id'])] = data['rate']
		return self.all_tax_rate

	def get_param_from_content_xml(self, content, field):
		try:
			root = ElementTree.fromstring(content)
			value = root.find(to_str(field)).text
			return value
		except Exception:
			self.log_traceback()
			return ''

	def convert_price(self, price, tax_id):
		if not price or not self._notice['src']['support'].get('price_includes_tax') or not tax_id:
			return price
		all_rate = self.get_all_tax_rate()
		if to_str(tax_id) not in all_rate:
			return price
		rate = to_decimal(all_rate[to_str(tax_id)])
		price = to_decimal(price)
		price = round(price / (100 + rate) * 100, 2)
		return price

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

	def get_data_default(self, data, field, store_id, need, store_default = 0):
		data_def = get_row_value_from_list_by_field(data, field, store_id, need)
		if data_def:
			return data_def
		return get_row_value_from_list_by_field(data, field, store_default, need)

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