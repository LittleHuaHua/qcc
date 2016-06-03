from setuptools import setup
from codecs import open 
import os
here = os.curdir
os.chdir(here)

setup(
	name = 'QCC',
	version = '1.1.0',
	description = 'Extract fundamental and quote data from the qcc database', 
	author = 'Hua Song', 
	author_email = 'huasong0916@gmail.com', 
	license = 'MIT', 
	packages = ['QCC'], 
	package_data = {'QCC': ['README.rst', 'MANIFEST.in']},
	install_requires = ['requests'], 
	zip_safe = False)


