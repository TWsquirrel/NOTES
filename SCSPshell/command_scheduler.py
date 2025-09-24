from __future__ import print_function
import traceback
import logging
import scsp_shell.settings.dependency as dependency
from scsp_shell.utility.scheduler import Scheduler
from scsp_shell.cmdshell.command import BaseCommand, CmdModule

_msg_lvl = 25
logging.addLevelName(_msg_lvl, "MESSAGE")
logger = logging.getLogger(__name__)

class CommandScheduler(CmdModule, Scheduler):
	def __init__(self):
		super(CommandScheduler, self).__init__()

	#TODO:
	# catch signal ?
	#------------------------------------------------------------------------------------------
	def _command_run(self):
		command = BaseCommand('run', self.do_run)
		command.set_description('start or restart the scheduler')
		return command
	def do_run(self, args):
		if self.is_done():
			logger.log(_msg_lvl,'No task found! Please run "commit" first.')
			return False
		if self.is_running():
			logger.log(_msg_lvl,'Scheduler is still running. ') #Add a new thread dynamically
		else:
			#Resume the scheduler after stop, or rerun after second commit
			#join() will ignore non-started threads, this is for resume case
			self.join()
			logger.log(_msg_lvl,'Restart/Start the scheduler') #Resume or New Run
		self.run()
		return False
	#------------------------------------------------------------------------------------------
	def _command_stop(self):
		command = BaseCommand('stop', self.stop)
		command.set_description('stop the scheduler')
		return command
	def stop(self, args):
		super(CommandScheduler, self).terminate()
	#------------------------------------------------------------------------------------------
	#def _command_is_running(self):
	#	command = BaseCommand('is_running', self.do_is_running)
	#	command.set_description('test if scheduler is running')
	#	return command
	#def do_is_running(self, args):
	#	if super(CommandScheduler, self).is_running():
	#		logger.log(_msg_lvl,'The scheduler is stilling running.')
	#	else:
	#		logger.log(_msg_lvl,'The scheduler is stopped.')
	#------------------------------------------------------------------------------------------
	def _command_wait(self):
		command = BaseCommand('wait', self.wait)
		command.set_description('wait program')
		command.parser.add_argument('-t','--timeout', default=None, type=int,
			help='timeout (s)')
		return command
	def wait(self, args):
		super(CommandScheduler, self).join(args.timeout)
	##------------------------------------------------------------------------------------------
	def _command_report_scheduler(self):
		command = BaseCommand('report_scheduler', self.do_status)
		command.set_description('report scheduler status')
		group = command.parser.add_mutually_exclusive_group()
		group.add_argument('-s','--single', action="store_true",
			help='only show single cell programs')
		group.add_argument('-d','--dual', action="store_true",
			help='only show dual cell programs')
		command.parser.add_argument('-a','--affinity', action="store_true",
			help='show affinity')
		return command
	def do_status(self, args):
		if args.dual:
			super(CommandScheduler, self).status(program_order = dependency.DUAL_ORDER,
													show_affinity = args.affinity)
		else:
			super(CommandScheduler, self).status(program_order = dependency.SINGLE_ORDER,
													show_affinity = args.affinity)

	#------------------------------------------------------------------------------------------
	def setTotalTaskCount(self, total_task_count):
		"""Reset scheduler total task and completed task"""
		self.clear_total_task()
		for program,task_count in total_task_count:
			self.add_total_task(program,task_count)

	def setRootTask(self, root_task_list):
		self.clear_task()
		for task_name, value in root_task_list:
			self.issue_task(task_name, value)