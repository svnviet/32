import sys
import traceback

from cartmigration.libs.utils import *

try:
	buffer = sys.argv[1]
	buffer = json.loads(buffer)
except:
	sys.exit()
controller_name = buffer.get('controller', 'migration')
action_name = buffer.get('action')
data = buffer.get('data')
if not action_name:
	sys.exit()
if not controller_name:
	controller_name = 'migration'
controller = get_controller(controller_name, data)
try:
	getattr(controller, action_name)(data)
except Exception:
	error = traceback.format_exc()
	log(error, data.get('migration_id') if data else None)
os.kill(os.getpid(), 9)