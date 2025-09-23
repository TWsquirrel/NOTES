from __future__ import print_function
import multiprocessing
try:
	#PYTHON 3+
	import queue
except ImportError:
	import Queue as queue
import time
import datetime
import sys
import traceback
import signal
import os
import itertools
import logging
try: #PACKAGE psutil
	import psutil
	USE_PSUTIL = True
except ImportError:
	USE_PSUTIL = False

_msg_lvl = 25
logging.addLevelName(_msg_lvl, "MESSAGE")
logger = logging.getLogger(__name__)

#for python 2.6
def _timedelta_total_seconds(timedelta):
	return (
		timedelta.microseconds + 0.0 +
		(timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

def _process(info, process_id, program_order):
	while info.keep_running.value == 1: #for SIGTERM, let current job finish
		for program in program_order:
			try:
				current_task = info.queue_list[program].get(False)
				current_program = program
				break
			except queue.Empty:
				continue
		else: #Nothing to do
			#Trap for detailed check
			with info.untrapped_count.get_lock():
				info.untrapped_count.value -= 1
			logging.debug('{0} trapped in roughed check. Remaining process: {1}'.format(
				process_id,info.untrapped_count.value))
			info.detailed_check_event.wait()
			with info.untrapped_count.get_lock():
				info.untrapped_count.value += 1
			logging.debug('{0} untrapped from roughed check. Remaining process: {1}'.format(
				process_id,info.untrapped_count.value))
			for program in info.program_list:
				if not info.queue_list[program].empty():
					break
			else:
				with info.untrapped_count.get_lock():
					info.untrapped_count.value -= 1
				#Detailed check
				logging.debug('{0} detailed check. Remaining process: {1}'.format(
					process_id,info.untrapped_count.value))
				with info.detailed_check_lock:
					info.detailed_check_event.clear()
					#Wait for other threads
					while info.untrapped_count.value > 0 and info.keep_running.value == 1:
						logging.debug('{0} waiting in detailed check. Remaining process: {1}'.format(
							process_id,info.untrapped_count.value))
						time.sleep(1)
					logging.debug('{0} perform detail check, Queue: {1}'.format(
						process_id,[info.queue_list[program].qsize() for program in info.program_list]))
					for program in info.program_list:
						if not info.queue_list[program].empty():
							break
					else:
						#All thread is done
						#info.end_time = datetime.datetime.now()
						with info.end_time.get_lock():
							info.end_time.value = int(_timedelta_total_seconds(datetime.datetime.now()-info.start_time))
						info.detailed_check_event.set()
						return
					info.detailed_check_event.set()
				with info.untrapped_count.get_lock():
					info.untrapped_count.value += 1
			time.sleep(1)
			continue
		logging.debug('{0} >> execute function "{1}" with data: {2}'.format(
			process_id,current_program,current_task))
		try:
			new_tasks = info.program_list[current_program](process_id,
				info.lock_list, info.common_obj, current_task)
			#converge process should handle by user's function
			for (new_program, new_task) in new_tasks:
				info.queue_list[new_program].put(new_task)
		except BaseException:
			logging.error('An exception is catched wihout logging at thread {0} "{1}" with data: {2}.'.format(
				process_id,current_program,current_task))
			logging.error(traceback.format_exc())
		logging.debug('{0} << finish "{1}" with data: {2} Total task: {3}'.format(
			process_id,current_program,current_task,info.completed_task_count[program].value))
		with info.completed_task_count[program].get_lock():
			info.completed_task_count[program].value += 1

class Info(object): pass

class Scheduler(object):
	# Make this instance picklable, these variables are static member
	#_thread_list = [] #process object, reconstruct after join()
	#_thread_pid = [] #record the pid after start()
	#_info = Info()
	def __init__(self):
		super(Scheduler,self).__init__()
		signal.signal(signal.SIGINT, self.kill)
		signal.signal(signal.SIGABRT, self.kill)
		signal.signal(signal.SIGILL, self.kill)
		signal.signal(signal.SIGTERM, self.terminate)
		###
		self._thread_list = [] #process object, reconstruct after join()
		self._thread_pid = [] #record the pid after start()
		self._info = Info()
		###
		self._info.logger = logger
		self._info.start_time = None
		self._info.end_time = multiprocessing.Value('i',-2)
		self._info.common_obj = None
		self._info.program_list = {}
		self._info.keep_running = multiprocessing.Value('I',1)
		self._info.queue_list = {}
		self._info.lock_list = {}
		self._info.completed_task_count = {}
		self._info.detailed_check_lock = multiprocessing.Lock()
		self._info.detailed_check_event = multiprocessing.Event()
		self._info.detailed_check_event.set()
		self._info.untrapped_count = multiprocessing.Value('I',0)
		self._thread_affinity = [] #record the program order
		self._total_task_count = {}

	def assign_program(self, program_list):
		self._info.program_list = program_list
		self._info.queue_list = {}
		for program in program_list:
			self._info.queue_list[program] = multiprocessing.Queue()
			self._info.completed_task_count[program] = multiprocessing.Value('I',0)

	def assign_common_obj(self, obj):
		self._info.common_obj = obj

	def issue_task(self, program, task_list):
		for task in task_list:
			self._info.queue_list[program].put(task)

	def clear_task(self, program=None):
		assert not self.is_running(), 'clear_task() should be called without running.'
		for program in [program] if program else self._info.program_list:
			while not self._info.queue_list[program].empty():
				self._info.queue_list[program].get()

	def is_done(self):
		for program in self._info.program_list:
			if not self._info.queue_list[program].empty():
				return False
		return True

	def add_lock(self, name):
		self._info.lock_list[name] = multiprocessing.Lock()

	def add_total_task(self, program, count):
		self._total_task_count[program] = count

	def clear_total_task(self, program=None):
		for program in [program] if program else self._info.program_list:
			self._info.completed_task_count[program] = multiprocessing.Value('I',0)
			try:
				del self._total_task_count[program]
			except KeyError:
				pass

	def add_thread(self, program_order):
		program_order = program_order[:]
		program_order.reverse()
		thread_id = len(self._thread_list)
		self._thread_list.append(
			multiprocessing.Process(target=_process, args=(
				self._info, thread_id, program_order)))
		self._thread_affinity.append(program_order)

	def clear_thread(self):
		assert not self.is_running(), 'clear_thread() should be called without running.'
		self._thread_list[:] = []
		self._thread_affinity[:] = []

	def run(self):
		if self._info.start_time is None:
			self._info.start_time = datetime.datetime.now()
			with self._info.end_time.get_lock():
				self._info.end_time.value = -1
		for thread in self._thread_list:
			#print(thread)
			if not thread.pid: #ignore stared threads
				with self._info.untrapped_count.get_lock():
					self._info.untrapped_count.value += 1
				thread.start()
				self._thread_pid.append(thread.pid)

	def is_running(self):
		for thread in self._thread_list:
			if thread.is_alive():
				return True
		return False

	def join(self, timeout = None):
		for thread_id in range(len(self._thread_list)):
			#To avoid the multiprocessing package assertion failed.
			#replace the original multiprocessing.Process object after join().
			thread = self._thread_list[thread_id]
			logger.debug('join: {0} {1}'.format(thread_id,self._thread_list[thread_id].pid))
			if thread.pid is not None: #ignore not started threads.
				thread.join(timeout)
				if not thread.is_alive():
					self._thread_list[thread_id] = multiprocessing.Process(target=_process, args=(
						self._info, thread_id, self._thread_affinity[thread_id]))
				logger.debug('join result: {0} {1}'.format(thread_id,thread.is_alive()))
		self._thread_pid[:] = [thread.pid for thread in self._thread_list if thread.is_alive()]
		logger.debug('pid of remaining threads: {0}'.format(self._thread_pid))

		with self._info.end_time.get_lock():
			if self._thread_pid:
				logger.info('Scheduler is still running.')
			elif self._info.end_time.value >= 0:
				logger.info('Jobs finished with {0}'.format(
					datetime.timedelta(seconds=self._info.end_time.value)))
				self._info.start_time = None
			elif self._info.end_time.value == -1 and self._info.start_time is not None:
				logger.info('Jobs paused with {0}'.format(
					datetime.timedelta(seconds=int(_timedelta_total_seconds(
						datetime.datetime.now()-self._info.start_time)))))
				self._info.start_time = None
			else:
				logger.info('Scheduler is not running.')

	def kill(self, signum = signal.SIGINT, frame = None):
		logger.info('Kill scheduler')
		if USE_PSUTIL:
			#Kill all threads and their children
			for pid in self._thread_pid:
				process = psutil.Process(pid)
				logger.info('kill pid:{0} children pid:{1}'.format(pid,process.children(recursive=True)))
				for proc in process.children(recursive=True):
					proc.kill()
				process.kill()
		else:
			#Kill all threads
			logging.warning('''psutil not installed.
Killing the processes may not be properly handeled.''')
			for pid in self._thread_pid:
				logger.info('kill pid:{0} children pid:?'.format(pid))
				os.kill(pid, signum)
		logger.info('Kill main process')
		exit(signal.SIGINT)

	def terminate(self, signum = signal.SIGTERM, frame = None):
		logger.info('Terminate scheduler, waiting for all running jobs to finish.')
		self._info.keep_running.value = 0
		self.join()
		with self._info.untrapped_count.get_lock():
			self._info.untrapped_count.value = 0
		self._info.keep_running.value = 1
		logger.info('Terminate successfully.')

	def status(self, program_order = None, show_affinity = False):
		if self._info.start_time:
			seconds=int(_timedelta_total_seconds(datetime.datetime.now()-self._info.start_time))
			logger.log(_msg_lvl,'Time Elapsed: {0}'.format(datetime.timedelta(seconds=seconds)))
		else:
			logger.log(_msg_lvl,'Scheduler is not running.')
		#Code from python shutil 3.3
		try:
			max_width = int(os.environ['COLUMNS'])
		except (KeyError, ValueError):
			max_width = 0
		if max_width <= 0:
			try: #PYTHON 3.3+
				#pylint: disable=no-member
				max_width = os.get_terminal_size(sys.__stdout__.fileno()).columns
			except BaseException:
				max_width = 79
		#END
		if not program_order:
			program_order = self._info.program_list.keys()
		title = ['program','total','queue','done','tid']
		title_width = max(len('{0}'.format(len(self._thread_list))),*[len(x) for x in title])
		width = len(max(program_order, key = len))
		width = max(width,*[0]+[len('{0}'.format(num)) for num in self._total_task_count])
		width = max(width,*[0]+[len('{0}'.format(num)) for num in self._info.queue_list])
		width = max(width,*[0]+[len('{0}'.format(num)) for num in self._info.completed_task_count])
		column_format = '{{0:^{0}}}|'.format(width)
		title_format  = '{{0:^{0}}}|'.format(title_width)
		title_width += 1
		width += 1

		max_width -= title_width
		column = max_width/width
		if column <= 0:
			column = len(program_order)
		total_width = title_width + width*column
		while program_order:
			if len(program_order) <= column:
				order = program_order
				program_order = []
			else:
				order = program_order[:column]
				program_order = program_order[column:]
			logger.log(_msg_lvl,'='*total_width)
			line = title_format.format(title[0])
			for program in order:
				line += column_format.format(program)
			logger.log(_msg_lvl,line)
			line = title_format.format(title[1])
			for program in order:
				try:
					line += column_format.format(self._total_task_count[program])
				except KeyError:
					line += column_format.format('-')
			logger.log(_msg_lvl,line)
			line = title_format.format(title[2])
			for program in order:
				line += column_format.format(self._info.queue_list[program].qsize())
			logger.log(_msg_lvl,line)
			line = title_format.format(title[3])
			for program in order:
				line += column_format.format(self._info.completed_task_count[program].value)
			logger.log(_msg_lvl,line)
			if not show_affinity:
				continue
			logger.log(_msg_lvl,'-'*total_width)
			line = title_format.format(title[4])
			for program in order:
				line += column_format.format(program)
			logger.log(_msg_lvl,line)
			for tid in range(len(self._thread_list)):
				line = title_format.format(tid)
				for program in order:
					line += column_format.format('v' if program in self._thread_affinity[tid] else '')
				logger.log(_msg_lvl,line)
		logger.log(_msg_lvl,'='*total_width)

##For test
#sys.path.insert(0, '../')
#import scsp_shell.utility.sqlite3_wrapper as databaseWrapper
#import sqlite3
#def f0(lock_list, common_obj, task):
#	#print('f0: {0} start'.format(task))

#	db = databaseWrapper.DatabaseWrapper(common_obj)
#	data = {
#		'VERSION':task,
#		'FLAG': 0,
#		'MSG': 'haha',
#		'TYPE':     'seq'
#		}
#	cond = {
#		'CELL_L':   'test{}'.format(task),
#		'CELL_R':   'single',
#		'PROGRAM':  'libertyParser'
#		}
#	lock_list['db'].acquire()
#	try:
#		db.updateInsert('VERSION', data, cond)
#		time.sleep(1)
#		db.commit()
#	except sqlite3.OperationalError:
#		print()
#		time.sleep(1)
#	lock_list['db'].release()
#	#time.sleep(2)
#	#print('f0: {} finished'.format(task))
#	return [('f1',task*10)]
#def f1(lock_list, common_obj, task):
#	#print('f1: {} start'.format(task))
#	time.sleep(0.5)
#	#print('f1: {} finished'.format(task))
#	return []

#if __name__ == '__main__':

#	logging.getLogger().setLevel(logging.DEBUG)
#	#Screen
#	handler_screen = logging.StreamHandler(sys.stdout)
#	handler_screen.setLevel(logging.DEBUG)
#	logging.getLogger().addHandler(handler_screen)
#	logger.debug('xxx')
#	#exit()
#	scheduler = Scheduler()
#	scheduler.assign_program({'f0':f0,'f1':f1,'f2':f1,'f3':f1})
#	scheduler.assign_common_obj('../test.db')
#	scheduler.issue_task('f0', range(10))
#	scheduler.add_lock('db')
#	scheduler.add_thread(['f1','f0'])
#	scheduler.add_thread(['f1'])
#	scheduler.add_thread(['f0'])
#	scheduler.add_thread(['f0'])
#	scheduler.run()
#	print(scheduler.is_running())
#	#print(psutil.Process().children(recursive=True))
#	#scheduler.status(show_affinity = True)
#	print(scheduler.is_running())
#	time.sleep(5)
#	#scheduler.terminate()
#	scheduler.status(show_affinity = True)
#	print(scheduler.is_running())
#	time.sleep(15)
#	print(scheduler.is_running())
#	scheduler.join()
#	print('done1')
#	'''exit()
#	scheduler.issue_task('f0', range(2))
#	scheduler.run()
#	print(scheduler.is_running())
#	scheduler.join()
#	print('done')
#	#input()'''