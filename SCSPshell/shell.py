from __future__ import print_function ## 啟用 Python 3 的 print() 函數，即使程式在 Python 2 環境下也能使用
import traceback ## 引入 Python 的追蹤模組，用於捕捉和顯示異常（Exception）信息 ，在程式出現錯誤時，方便打印堆疊訊息，便於除錯
import logging ## 引入日誌模組，用於記錄程式運行狀態、錯誤信息或調試信息
import os 
import glob ## 提供文件名匹配功能，類似 Linux 的 ls *.txt ，快速查找符合模式的檔案
import fnmatch ## 用於匹配文件名稱的模式（支持 *、? 等通配符），快速查找符合模式的檔案
import pickle
import collections
import gzip ## 支持 gzip 壓縮格式的文件讀寫，讀寫壓縮檔案，節省空間
import contextlib
import dill as pickle
#import pickle
import scsp_shell.settings.shell_settings
## 引入 SCSPShell 中不同容器模組，通常對應不同數據或邏輯模組
import scsp_shell.container.cell
import scsp_shell.container.dual_cell
import scsp_shell.container.value
import scsp_shell.container.target
import scsp_shell.container.group
import scsp_shell.container.report
import scsp_shell.container.design
import scsp_shell.container.model_1tf
import scsp_shell.container.model_2tf
## 引入 SCSPShell 的子程式工具
import scsp_shell.subprogram.command_scheduler
import scsp_shell.subprogram.task_manager
import scsp_shell.version_control
import scsp_shell.command_config
import scsp_shell.pattern_analyzer
import scsp_shell.data_analyzer
import scsp_shell.udfm_generator
import scsp_shell.ctm_generator
## 引入命令基類和模組類，啟動或操作整個 shell 系統
from scsp_shell.cmdshell.command import BaseCommand, CmdModule
from scsp_shell.cmdshell.cmdshell import CmdShell

#######################
import re


#######################

_msg_lvl = 25
logging.addLevelName(_msg_lvl, "MESSAGE")
logger = logging.getLogger(__name__)

class ScspShell(CmdShell, CmdModule):
	def __init__(self, handler_screen, handler_shell, root_path = os.getcwd()):
		super(ScspShell, self).__init__(root_path, handler_screen, handler_shell)
		CmdModule.__init__(self)
		self.shell_settings = scsp_shell.settings.shell_settings.ShellSettings(root_path)
		self.save_path = self.shell_settings.getSavePath()
		self.pickle_extension = '.pkl.gz'
		self.shell_settings.prepareEnvironment()
		# Instantiate command modules
		#  Containers
		self.container_cell = scsp_shell.container.cell.ContainerCell(self)
		self.container_dual_cell = scsp_shell.container.dual_cell.ContainerDualCell(self)
		self.container_value = scsp_shell.container.value.ContainerValue(self)
		self.container_target = scsp_shell.container.target.ContainerTarget(self)
		self.container_group = scsp_shell.container.group.ContainerGroup(self)
		self.container_report = scsp_shell.container.report.ContainerReport(self)
		self.container_design = scsp_shell.container.design.ContainerDesign(self)
		self.container_model_1tf = scsp_shell.container.model_1tf.ContainerModel1tf(self)
		self.container_model_2tf = scsp_shell.container.model_2tf.ContainerModel2tf(self)
		#  Other Modules
		self.scheduler = scsp_shell.subprogram.command_scheduler.CommandScheduler()
		self.task_manager = scsp_shell.subprogram.task_manager.TaskManager(self)
		self.command_config = scsp_shell.command_config.CommandConfig(self)
		self.version_controller = scsp_shell.version_control.VersionController(self)
		self.pattern_analyzer = scsp_shell.pattern_analyzer.PatternAnalyzer(self)
		self.data_analyzer = scsp_shell.data_analyzer.DataAnalyzer(self)
		self.udfm_generator = scsp_shell.udfm_generator.UdfmGenerator(self)
		self.ctm_generator = scsp_shell.ctm_generator.CtmGenerator(self)
		#self._model_manager = model.modelManager(self._shell_settings)
		#TODO remove below lines
		# Need a dedicated database initializer, maybe a new databaseWrapper
		self.container_cell.reload() # Move to VersionController init()
		self.version_controller.setCurrentProgramVersion(self.task_manager.getProgramVersion())
		# TODO: move all module to platform
		# => a platform class and a scsp shell class

		#TEST
		# Register commands
		self.register_commands(self.get_commands())
		for obj in self.__dict__.values():
			if isinstance(obj, scsp_shell.cmdshell.cmdshell.CmdModule):
				self.register_commands(obj.get_commands())

	#-------------------------------------------------------------------------------------
	# Patch commands
	def _command_clear_database(self):
		command = BaseCommand('clear_database', self.clear_database)
		command.set_description('clear_database')
		return command
	def clear_database(self, args):
		# TODO: get entries from version.db
		# delete all invalid cells, faults, values

		# Here is a temporary approach:
		# Find all value dir
		for value_path in glob.glob(os.path.join(
				self.shell_settings.getDatabasePath(),'*','*','*','*/')):
			value_path = os.path.normpath(value_path)
			if os.path.isdir(value_path) and os.path.basename(value_path) != 'info':
				# Compress existing log
				for log_file_path in glob.glob(os.path.join(value_path,'*.log')):
					self.shell_settings.compressFile(log_file_path,log_file_path)
				for log_file_path in glob.glob(os.path.join(value_path,'*.*tf.json')):
					self.shell_settings.compressFile(log_file_path,log_file_path)
				# Clear dir
				self.shell_settings.cell_structure.clear_value_dir(value_path,None)

	def _command_rename_savefile(self):
		command = BaseCommand('rename_savefile', self.rename_savefile)
		command.set_description('load the whole shell workspace')
		return command
	def rename_savefile(self, args):
		import re
		for filename in os.listdir(self.shedd_file_argumentdd_file_argumentll_settings.getSavePath()):
			a = filename
			b = re.sub(r'^(.*)\.(.*)\.list$',r'\2.\1.list',filename)
			if a!=b: logger.log(_msg_lvl, '{0:<35} > {1:<35}'.format(a,b))
			os.rename(os.path.join(self.shell_settings.getSavePath(),a),
						 os.path.join(self.shell_settings.getSavePath(),b))
	#---------------------------------------------------------------------------------------
	def add_file_argument(self, command, prefix, postfix):
		command.parser.add_argument('filename', type=str,
			help='a pure filename without prefix or postfix.')
		command.add_completion(0,BaseCommand.get_file_complete(self.save_path, prefix, postfix))
		command.add_completion(1,BaseCommand.get_no_complete())

	def _command_save_shell(self):
		command = BaseCommand('save_shell', self.save_shell)
		command.set_description('save the whole shell workspace')
		self.add_file_argument(command,'shell.',self.pickle_extension)
		return command
	def save_shell(self, args):
		with contextlib.closing(gzip.open(os.path.join(self.save_path,
				'shell.' + args.filename + self.pickle_extension),'w')) as new_file:
			container_list = []
			for var_name, obj in self.__dict__.iteritems():
				if isinstance(obj, scsp_shell.cmdshell.base_container.BaseContainer):
					container_list.append((var_name, obj.get_container()))
			pickle.dump(container_list, new_file)
			logger.log(_msg_lvl,'Save shell to {0} successfully.'.format(args.filename))

	def _command_load_shell(self):
		command = BaseCommand('load_shell', self.load_shell)
		command.set_description('load the whole shell workspace')
		self.add_file_argument(command,'shell.',self.pickle_extension)
		return command
	def load_shell(self, args):
		with contextlib.closing(gzip.open(os.path.join(self.save_path,
				'shell.' + args.filename + self.pickle_extension))) as new_file:
			container_list = pickle.load(new_file)
			for var_name, obj in container_list:
				getattr(self, var_name).set_container(obj)
			logger.log(_msg_lvl,'Load shell from {0} successfully.'.format(args.filename))

	def exit(self):
		""" Terminate the scheduler when exiting."""
		self.scheduler.terminate()
		logger.log(_msg_lvl,'Bye!')
        
        
    ##############################################
    # self-defined cmd
    ##############################################
    
	def _command_clean_spice(self):
		command = BaseCommand('clean_spice', self.clean_spice)
		command.set_description('clean .mt & .ms files')
		command.parser.add_argument(
			'folder', type=str, nargs='?', default=None,
			help='clean .mt & .ms files'
		)
		return command
	
	def clean_spice(self, args):
		if args.folder:
			target_path = os.path.join(self.root_path, "database", args.folder)
		else:
			target_path = os.path.join(self.root_path, "database")
		
		delete_spice_files(target_path, logger=logger, log_level=_msg_lvl)





def delete_spice_files(database_root, logger=None, log_level=25, dry_run=False):
	"""
	Recursively delete all files in `database_root` whose names contain
	'.ms' or '.mt' followed by digits (e.g., .ms12, .mt134).
	Compatible with Python 2/3 without glob(..., recursive=True)
	"""
	if not os.path.exists(database_root):
		if logger:
			logger.log(log_level, "Directory does not exist: {}".format(database_root))
		return 0

	pattern = re.compile(r'\.(ms|mt)\d*$')

	target_files = []
	# Recursively walk directories
	for root, dirs, files in os.walk(database_root):
		for f in files:
			if pattern.search(f):
				target_files.append(os.path.join(root, f))

	count = 0
	for f in target_files:
		try:
			if dry_run:
				if logger:
					logger.log(log_level, "[dry-run] Would delete {}".format(f))
			else:
				os.remove(f)
				count += 1
				if logger:
					logger.log(log_level, "Deleted {}".format(f))
		except Exception as e:
			if logger:
				logger.log(log_level, "Failed to delete {}: {}".format(f, e))

	if logger:
		logger.log(
		log_level,
		"Completed{}: found {} matching files, actually deleted {}.".format(
		" (dry-run)" if dry_run else "",
		len(target_files),
 		count
		)
	)

	return count
