from cartmigration.libs.mysql import Mysql
from cartmigration.libs.utils import *


class BaseModel:
	USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"

	def __init__(self):
		self._db = None
		self._db_center = None
		self._list_table_center = [TABLE_MIGRATION, TABLE_AUTO_TEST, TABLE_MIGRATION_TEST, TABLE_SETTING, TABLE_DEMO_ERROR]
		self._migration_id = None

	def get_db(self, config = None, test = False):
		if self._db:
			return self._db
		if (hasattr(self, '_mode') and getattr(self, '_mode') == 'test') or test or (hasattr(self, '_notice') and self._notice and self._notice['config'].get('test')):
			test = True
		self._db = Mysql(config, test = test, migration_id = self._migration_id)
		if not self._db.get_migration_id() and hasattr(self, '_migration_id'):
			self._db.set_migration_id(self._migration_id)

		return self._db

	def set_db(self, _db):
		self._db = _db

	def query_raw(self, query):
		return self.get_db().query_raw(query)

	def dict_to_create_table_sql(self, dictionary):
		return self.get_db().dict_to_create_table_sql(dictionary)

	def dict_to_insert_condition(self, dictionary, allow_key = None):
		return self.get_db().dict_to_insert_condition(dictionary, allow_key)

	def dict_to_where_condition(self, dictionary):
		return self.get_db().dict_to_where_condition(dictionary)

	def dict_to_set_condition(self, dictionary):
		return self.get_db().dict_to_set_condition(dictionary)

	def list_to_in_condition(self, list_data):
		return self.get_db().list_to_in_condition(list_data)

	def insert_obj(self, table, data, insert_id = False):
		return self.get_db().insert_obj(table, data, insert_id)

	def insert_raw(self, query, insert_id = False):
		return self.get_db().insert_raw(query, insert_id)

	def update_obj(self, table, data, where = None):
		return self.get_db().update_obj(table, data, where)

	def select_obj(self, table, where, select_field = None):
		return self.get_db().select_obj(table, where, select_field)

	def insert_multiple_obj(self, table, data):
		return self.get_db().insert_multiple_obj(table, data)

	def select_page(self, table, where = None, select_field = None, limit = None, offset = None, order_by = None):
		return self.get_db().select_page(table, where, select_field, limit, offset, order_by)

	def count_table(self, table, where = None):
		return self.get_db().count_table(table, where)

	def select_row(self, table, where, select_field = None):
		obj = self.select_obj(table, where, select_field)
		data = obj.get('data', [])
		if data and to_len(data) > 0:
			return data[0]
		return False

	def select_raw(self, query):
		return self.get_db().select_raw(query)

	def delete_obj(self, table, where = None):
		return self.get_db().delete_obj(table, where)

	def escape(self, value):
		return self.get_db().escape(value)

	def get_table_name(self, table):
		return self.get_db().get_table_name(table)

	def execute_scripts_from_file(self, file_path):
		return self.get_db().execute_scripts_from_file(file_path)
