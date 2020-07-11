import configparser

from cartmigration.libs.base_model import BaseModel
from cartmigration.libs.mysql import Mysql
from cartmigration.libs.utils import *


class Setup(BaseModel):
	_table_flag_stop = {
		'table': TABLE_FLAG_STOP,
		'rows': {
			'id': "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
			'migration_id': "INT(11) NOT NULL",
			'flag': "TINYINT(2) NOT NULL DEFAULT 1",
		},
		'unique': [
			['migration_id']
		]

	}

	_table_map = {
		'table': TABLE_MAP,
		'rows': {
			'id': 'BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY',
			'migration_id': "BIGINT NOT NULL",
			'type': "VARCHAR(255)",
			'id_src': "BIGINT",
			'id_desc': "BIGINT",
			'code_src': "VARCHAR(255)",
			'code_desc': "TEXT",
			'value': "LONGTEXT",
			'additional_data': "LONGTEXT",
			'store_id_src': "VARCHAR(255)",
			'store_id_desc': "VARCHAR(255)",
		},
		'index': [
			['migration_id'],
			['id_src'],
			['type'],
			['code_src'],

		],
	}

	_table_recent = {
		'table': TABLE_RECENT,
		'rows': {
			'id': 'BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY',
			'migration_id': "BIGINT NOT NULL",
			'notice': "LONGTEXT"
		},
		'unique': [
			['migration_id']
		]
	}

	_table_setting = {
		'table': TABLE_SETTING,
		'rows': {
			'id': 'BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY',
			'migration_id': "BIGINT NOT NULL",
			'setting': "LONGTEXT"
		},
		'unique': [
			{'migration_id'}
		]
	}

	_table_migration_history = {
		'table': TABLE_MIGRATION_HISTORY,
		'rows': {
			'id': 'BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY',
			'migration_id': "BIGINT NOT NULL",
			'type': "VARCHAR(255)",
			'created_at': "DATETIME",
			'notice': "LONGTEXT"
		},
	}

	_table_migration_process = {
		'table': TABLE_MIGRATION,
		'rows': {
			'migration_id': 'BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY',
			'pid': "int(11) NULL",
			'server_id': "int(11) NULL",
			'notice': "longtext NULL",
			'status': "tinyint(4) DEFAULT 1",
			'mode': "tinyint(4) DEFAULT 1",
			'migration_group': "tinyint(4) DEFAULT 1",
			'created_at': "timestamp NULL",
			'updated_at': "timestamp NULL",
			'demo_updated_at': "timestamp NULL",
			'on_error': "tinyint(4) DEFAULT 1",
			'ignore_existed_images': "tinyint(4) DEFAULT 1",
			'demo_limit': "int(11) DEFAULT 20",
			'entity_warning': "TEXT NULL",
			'last_full_mig_notice': "LONGTEXT NULL",
		},
	}
	def __init__(self):
		super().__init__()
		self._db_local = None
		self._db_center = None
		self.tables = [self._table_migration_process, self._table_flag_stop, self._table_map, self._table_recent, self._table_migration_history]

	def setup_db_for_migration(self, migration_id, test = False):
		if not migration_id:
			return False
		if to_str(get_config_ini('local', 'migration_separate_db', 'yes', migration_id)).lower().strip() == 'no':
			return True
		# migration = self.select_row(TABLE_MIGRATION, {'migration_id': migration_id})
		# if not migration:
		# 	return False
		# if migration.get('db_name'):
		# 	return True
		db = Mysql(test = test, migration_id = migration_id)
		db.set_migration_id(migration_id)
		db.create_database()
		db.set_config(None)
		for table in self.tables:
			if table['table'] == TABLE_MIGRATION and (not test and get_config_ini('local', 'mode') != 'test'):
				continue
			query = db.dict_to_create_table_sql(table)
			if query['result'] != 'success':
				return False
			res = db.query_raw(query['query'])
			if res['result'] != 'success':
				return False
		file_state = get_pub_path() + '/uploads/directory_country_region.sql'
		db.execute_scripts_from_file(file_state)
		db.close_connect()
		return True

	def run(self):
		config_file = get_root_path() + '/cartmigration/etc/config.ini'
		if os.path.isfile(config_file):
			config_local = None
		else:
			host = input('Enter local database host: \n')
			username = input('Enter local database username: \n')
			password = input('Enter local database password: \n')
			name = input('Enter local database name: \n')
			prefix = input('Enter local database prefix: \n')
			config_local = dict()
			config_local['db_host'] = host
			config_local['db_username'] = username
			config_local['db_password'] = password
			config_local['db_name'] = name
			config_local['db_prefix'] = prefix
		db = self.get_db(config_local)
		con = db.get_connect()
		if not con:
			print("Database local setup fail")
			print("----------------------------------")
			return False
		for table in self.tables:
			query = self.dict_to_create_table_sql(table)
			if query['result'] != 'success':
				return False
			res = self.query_raw(query['query'])
			if res['result'] != 'success':
				return False
		self.delete_obj('directory_country_region')
		file_state = get_pub_path() + '/uploads/directory_country_region.sql'
		import_cmd = ' -u ' + db.get_db_username() + ' -p'+ db.get_db_password() + ' ' + db.get_db_name() + ' < ' + file_state
		subprocess.call(['mysql', import_cmd], shell = False)
		with open(file_state, 'r') as f:
			command = ['mysql', '-u%s' % db.get_db_username(), '-p%s' % db.get_db_password(), db.get_db_name()]
			proc = subprocess.Popen(command, stdin = f, stdout=subprocess.PIPE)
			stdout, stderr = proc.communicate()
		print("Database local setup successfully")
		print("----------------------------------")
		if os.path.isfile(config_file):
			config_center = None
		else:

			host = input('Enter center database host: \n')
			username = input('Enter center database username: \n')
			password = input('Enter center database password: \n')
			name = input('Enter center database name: \n')
			prefix = input('Enter center database prefix: \n')
			config_center = dict()
			config_center['db_host'] = host
			config_center['db_username'] = username
			config_center['db_password'] = password
			config_center['db_name'] = name
			config_center['db_prefix'] = prefix
		db = self.get_db(False, config_center)
		con = db.get_connect()
		if not con:
			print("Can't connect to center database")
			print("----------------------------------")
			return False
		print("Connected Database center!")
		print("----------------------------------")
		if os.path.isfile(config_file):
			config = configparser.ConfigParser()
			config.read(config_file)
			try:
				port = config['server']['port']
				port_upload = config['server']['port_upload']
			except Exception:
				port = input('Enter port socket: \n')
				port_upload = input('Enter port upload file: \n')
			if not config.has_section('server'):
				config.add_section('server')
				config['server']['port'] = port
				config['server']['port_upload'] = port_upload
				with open(config_file, 'w') as configfile:  # save
					config.write(configfile)

		if not os.path.isfile(config_file):
			port = input('Enter port socket: \n')
			port_upload = input('Enter port upload file: \n')
			config = configparser.ConfigParser()
			config.add_section('server')
			config['server']['port'] = port
			config['server']['port_upload'] = port_upload
			config.add_section(Mysql.TYPE_LOCAL)
			for key, value in config_local.items():
				config[Mysql.TYPE_LOCAL][key] = value
			config.add_section(Mysql.TYPE_CENTER)
			for key, value in config_center.items():
				config[Mysql.TYPE_CENTER][key] = value
			config_file = get_root_path() + '/cartmigration/etc/config.ini'
			with open(config_file, 'w') as configfile:  # save
				config.write(configfile)
		return True
