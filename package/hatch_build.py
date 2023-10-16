import glob
import os
import stat

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        # chmod +x
        for f in glob.glob("./tools/*"):
            st = os.stat(f)
            os.chmod(f, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def finalize(self, version, build_data, artifact_path):
        pass
