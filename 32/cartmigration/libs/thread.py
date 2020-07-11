import threading
import time
from cartmigration.libs.utils import *


class CustomThread(threading.Thread):
	def __init__(self, buffer):
		self._exit_flag = 0
		threading.Thread.__init__(self)
		self.threadID = buffer['data']['license']
		self.name = buffer['data']['license']


