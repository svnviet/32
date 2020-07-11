from cartmigration.models.abs_migration.abstract_migration import LeAbstractMigration
from cartmigration.libs.utils import *
class LeTestMigration(LeAbstractMigration):
	def get_migration_notice(self, migration_id):
		info_migration_id = self.get_info_migration(migration_id)
		if info_migration_id:
			try:
				notice_data = info_migration_id['notice']
				notice_data = json_decode(notice_data)
				return notice_data
			except Exception as e:
				return None
		return None

	def delete_migration_notice(self, migration_id):
		if not migration_id:
			return True
		delete = self.update_obj(TABLE_MIGRATION, {'notice': None}, {'migration_id': migration_id})
		if delete and delete['result'] == 'success':
			return True
		return False

	def update_notice(self, _migration_id, notice = None, pid = None, mode = None, status = None, finish = False, clear_entity_warning = False):
		update = self.before_update_notice(_migration_id, notice, pid, mode, status, finish, clear_entity_warning)
		update_notice = self.update_obj(TABLE_MIGRATION, update, {'migration_id': _migration_id})
		if update_notice['result'] == 'success':
			self.after_update_notice(notice)
		return update_notice

	def after_update_notice(self, notice):
		notice = json_decode(notice) if isinstance(notice, str) else notice
		msg_log = list()
		msg_log.append('Migration status: ' + self.get_process(notice))
		if notice['resume']['action'] == 'migrating':
			msg_log.append('Type: ' + notice['resume']['type'])
		msg_log.append("\n\n")
		format_str = "{:<20}" * 4
		msg_log.append(format_str.format('Name', 'Imported', 'Error', 'Total'))
		entities = ['taxes', 'manufacturers', 'categories', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules']
		for entity in entities:
			if not notice['config'].get(entity) or not notice['process'].get(entity):
				continue
			process = notice['process'][entity]
			msg_log.append(format_str.format(to_str(entity).capitalize(), process['imported'], process['error'], process['total']))
		self.log_process(msg_log, notice['migration_id'])

	def log_process(self, msg, migration_id):
		path = get_pub_path() + '/log/' + to_str(migration_id)
		if not os.path.exists(path):
			os.makedirs(path)
			os.chmod(path, 0o777)
		file_name = path + '/' + 'notice.log'
		check_exist = False
		if os.path.isfile(file_name):
			check_exist = True
		with open(file_name, 'w') as log_file:
			log_file.write("\n".join(msg))
		if not check_exist and os.path.isfile(file_name):
			os.chmod(file_name, 0o777)

	def get_process(self, notice):
		step = get_value_by_key_in_dict(notice['resume'], 'process', 'new')
		status = ''
		migration_type = 'New '
		if notice['config'].get('remigrate'):
			migration_type = 'Re'

		elif notice['config'].get('recent'):
			migration_type = 'Recent '
		elif notice['config'].get('update_latest_data'):
			migration_type = 'Smart Update '
		if step == 'new':
			status = migration_type + 'Migration Setup'
		elif step == 'configuring':
			status = migration_type + 'Migration Configuration'
		elif step in ['demo_completed', 'payment']:
			if notice['demo']['status'] == 'success' and to_int(notice['mode']) == MIGRATION_DEMO:
				status = 'Demo Migration Completed'
			else:
				status = migration_type + 'Migration Confirmation'

		else:
			if to_int(notice['mode']) == MIGRATION_DEMO:
				migration_type = 'Demo '
			if migration_type == 'New ':
				migration_type = 'Full '

			if notice['resume']['action'] == 'clear':
				status = migration_type + ' Clearing target store'
			elif notice['resume']['action'] == 'storage_data':
				status = migration_type + ' Storing data'
			else:
				status = migration_type + 'Migration Running'

		if notice['finish'] and to_int(notice['mode']) == MIGRATION_FULL:
			status = migration_type + 'Migration Completed'
		return status
		

	def set_status_migration(self, migration_id, status):
		update = {
			'status': status
		}
		where = {
			'migration_id': migration_id
		}
		self.update_obj(TABLE_MIGRATION, update, where)

	def save_migration(self, migration_id, data):
		if 'migration_id' in data:
			del data['migration_id']
		migration_data = self.before_save_migration(data)
		if migration_id:
			return self.update_obj(TABLE_MIGRATION, migration_data, {'migration_id': migration_id})
		else:
			return self.insert_obj(TABLE_MIGRATION, migration_data)

	def get_app_mode_limit(self):
		setting = self.select_row(TABLE_SETTING, {'key': 'app_mode'})
		if not setting:
			return False
		return setting['value']
	def get_info_migration(self, migration_id):
		if not migration_id:
			return None
		return self.select_row(TABLE_MIGRATION, {'migration_id': migration_id})

	def get_default_notice(self):
		default_notice = super().get_default_notice()
		default_notice['config']['test'] = True
		return default_notice