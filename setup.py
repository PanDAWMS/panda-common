#!/usr/bin/env python
#
# Setup prog for Panda Common
#
#
release_version='0.0.5'

import re
from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

# set overall prefix for bdist_rpm
class install_panda(install_org):
    def initialize_options (self):
        install_org.initialize_options(self)
        self.prefix = '/data/atlpan/srv'
                    

# generates files using templates and install them
class install_data_panda (install_data_org):
    def run (self):
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match != None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match != None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # create tmp area
        tmpDir = 'build/tmp'
        self.mkpath(tmpDir)
        new_data_files = []
        for destDir,dataFiles in self.data_files:
            newFilesList = []
            for srcFile in dataFiles:
                # check extension
                if not srcFile.endswith('.template'):
                    raise RuntimeError,"%s doesn't have the .template extension" % srcFile
                # dest filename
                destFile = re.sub('\.template$','',srcFile)
                destFile = destFile.split('/')[-1]
                destFile = '%s/%s' % (tmpDir,destFile)
                # open src
                inFile = open(srcFile)
                # read
                filedata=inFile.read()
                # close
                inFile.close()
                # replace patterns
                for item in re.findall('@@([^@]+)@@',filedata):
                    if not hasattr(self,item):
                        raise RuntimeError,'unknown pattern %s in %s' % (item,srcFile)
                    # get pattern
                    patt = getattr(self,item)
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb','',patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot','',patt)                    
                    # replace
                    filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
                oFile = open(destFile,'w')
                oFile.write(filedata)
                oFile.close()
                # append
                newFilesList.append(destFile)
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)
        
        
# setup for distutils
setup(
    name="panda-common",
    version=release_version,
    description=' PanDA Common Package',
    long_description='''This package contains PanDA Common Components''',
    license='GPL',
    author='Panda Team',
    author_email='hn-atlas-panda-pathena@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=[ 'pandacommon',
               'pandacommon.liveconfigparser',
               'pandacommon.pandalogger',               
              ],
    data_files=[ 
                ('etc/panda',  
                 ['templates/panda_common.cfg.rpmnew.template']
                 ),
                ],
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)
