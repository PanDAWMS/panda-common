#
#
# Setup prog for Panda Common
#
#
# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')

# get release version
import os
import PandaPkgInfo
release_version = PandaPkgInfo.release_version
if 'BUILD_NUMBER' in os.environ:
    release_version = '{0}.{1}'.format(release_version,os.environ['BUILD_NUMBER'])

from setuptools import setup,find_packages
        
setup(
    name="panda-common-s",
    version=release_version,
    description=' PanDA Common Package',
    long_description='''This package contains PanDA Common Components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=find_packages(),
    install_requires=['configparser',
                      'future',
                      ],
    data_files=[ 
                ('etc/panda',  
                 ['templates/panda_common.cfg.rpmnew.template']
                 ),
                ],
)
