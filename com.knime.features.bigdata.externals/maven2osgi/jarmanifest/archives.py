#!/usr/bin/env python3

import tarfile
import zipfile
import subprocess
import os
from jarmanifest import log
from jarmanifest.configuration import config

logger = log.getLogger('util.archives')

ignore_no_ext = False
ignore_ext_list = []
if config.has_option('archives','ignore_no_ext'):
	ignore_no_ext = eval(config.get('archives','ignore_no_ext'))
if config.has_option('archives','ignore_ext_list'):
	ignore_ext_list = config.get('archives','ignore_ext_list').split(',')

# handler class for tars with invalid headers
# Some .tgz patchs eg: JBPAPP-8049.zip/JBPAPP-8049-signed.tgz tend
# to have things like '\x81' in its headers, raiseing a
# UnicodeDecodeError
# We fake the tarfile module using the sytems 'tar' binary
class custom_tarfile:
	def __init__(self,filename):
		self.command = 'tar'
		self.filename = os.path.abspath(filename)
		self.filelist = self.get_filelist()
		self.extracted = False

	def get_filelist(self):
		files = subprocess.check_output([self.command,'-tf',self.filename])
		return [ f.strip() for f in files.decode().strip().split('\n') ]

	def getmembers(self):
		return self.filelist

	def extract(self,filename):
		if not self.extracted:
			subprocess.call([self.command,'-xf',self.filename])
			self.extracted = True

def filelist(archive):
	"""
	Returns a list of regular file names from a given archive.
	"""
	filelist = []
	infos = infolist(archive)
	for info in infos:
		if not isLink(info) and isFile(info):
			name = getname(info)
			if name is not None:
				filelist.append(name)
	return filelist

def infolist(archive):
	"""
	Returns a list of [Zip|Tar]Info objects of the members of an archvie
	"""
	if type(archive) is tarfile.TarFile:
		return archive.getmembers()
	if type(archive) is custom_tarfile:
		return archive.getmembers()
	if type(archive) is zipfile.ZipFile:
		return archive.infolist()
	return []

# We use a hack to figure out if an info object a link
# http://www.mail-archive.com/python-list@python.org/msg34223.html
zip_link_attrs = ['0xa1ff0000','0xa1ed0000']
def isLink(info):
	"""
	Returns True if [Zip|Tar]Info object is of a link
	"""
	if type(info) is zipfile.ZipInfo:
		return hex(info.external_attr) in zip_link_attrs
	elif type(info) is tarfile.TarInfo:
		return info.issym() or info.islink()
	return False

def isDir(info):
	"""
	Returns True if [Zip|Tar]Info object is of a directory
	"""
	if type(info) is zipfile.ZipInfo:
		return info.filename.endswith('/')
	elif type(info) is tarfile.TarInfo:
		return info.isdir()
	elif type(info) is str:
		return info.endswith('/')
	return False

def isFile(info):
	"""
	Returns True if [Zip|Tar]Info object is of a file
	"""
	if type(info) is zipfile.ZipInfo:
		return not info.filename.endswith('/')
	elif type(info) is tarfile.TarInfo:
		return info.isfile()
	elif type(info) is str:
		return not info.endswith('/')
	return False

def getname(info):
	"""
	Returns str name of the member
	"""
	if type(info) is zipfile.ZipInfo:
		return info.filename
	elif type(info) is tarfile.TarInfo:
		return info.name
	elif type(info) is str:
		return info
	return None

def namelist(archive):
	"""
	Returns a list of all member names from a given archive.
	"""
	if type(archive) is tarfile.TarFile:
		return archive.getnames()
	if type(archive) is zipfile.ZipFile:
		return archive.namelist()
	return []

def get_archive(filepath):
	"""
	Returns an archive object of the file. (ZipFile or TarFile)
	"""
	if tarfile.is_tarfile(filepath):
		try:
			return tarfile.TarFile(filepath)
		except:
			logger.debug('Python tarfile failed. Switching to native mode for %s'%(filepath))
			return custom_tarfile(filepath)
	if zipfile.is_zipfile(filepath):
		return zipfile.ZipFile(filepath)
	return None

def to_ignore(filepath,ignore_list=ignore_ext_list):
	if ignore_no_ext:
		ignore_list.append(filepath)
	return filepath.split('.')[-1] in ignore_list

def is_valid(filepath):
	"""
	Check if the given file is a valid archive file
	"""
	# Exclude usual suspects that give false positives
	if to_ignore(filepath):
		return False
	valid = zipfile.is_zipfile(filepath) or tarfile.is_tarfile(filepath)
	return valid
