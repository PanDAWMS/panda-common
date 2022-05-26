#
#
# Setup prog for Panda Common
#
#
# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')  # noqa: E402

import os

import PandaPkgInfo
from setuptools import setup, find_packages
from setuptools.command.install import install as install_org


# get release version
release_version = PandaPkgInfo.release_version
if 'BUILD_NUMBER' in os.environ:
    release_version = '{0}.{1}'.format(release_version,os.environ['BUILD_NUMBER'])


# custom install to disable egg
class install_panda (install_org):
    def finalize_options (self):
        install_org.finalize_options(self)
        self.single_version_externally_managed = True

setup(
    name="panda-common",
    version=release_version,
    description=' PanDA Common Package',
    long_description='''This package contains PanDA Common Components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=find_packages(),
    install_requires=['configparser',
                      'pytz',
                      'stomp.py >=4.1.23, <=7.0.0',
                      'requests',
                      ],
    data_files=[
                ('etc/panda',
                 ['templates/panda_common.cfg.rpmnew']
                 ),
                ],
    scripts=[
        'tools/panda_common-install_igtf_ca'
    ],
    cmdclass={
        'install': install_panda,
    }
)
