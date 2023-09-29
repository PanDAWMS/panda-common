import threading

from pandacommon.pandalogger import logger_utils

# logger
base_logger = logger_utils.setup_logger("plugin_factory")


# plugin factory
class PluginFactory(object):
    # class lock
    __lock = threading.Lock()

    # constructor
    def __init__(self):
        self.classMap = {}

    # get plugin
    def get_plugin(self, plugin_conf):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, method_name="get_plugin")

        # use module + class as key
        module_name = plugin_conf["module"]
        class_name = plugin_conf["name"]
        plugin_params = plugin_conf.get("params", {})
        if module_name is None or class_name is None:
            tmp_log.warning("Invalid plugin; either module or name is missing ".format(module_name))
            return None
        plugin_key = "{0}.{1}".format(module_name, class_name)
        # get class
        with self.__lock:
            if plugin_key not in self.classMap:
                # import module
                tmp_log.debug("importing {0}".format(module_name))
                mod = __import__(module_name)
                for sub_module_name in module_name.split(".")[1:]:
                    mod = getattr(mod, sub_module_name)
                # get class
                tmp_log.debug("getting class {0}".format(class_name))
                cls = getattr(mod, class_name)
                # add
                self.classMap[plugin_key] = cls
                tmp_log.debug("loaded class {0}".format(plugin_key))
            else:
                tmp_log.debug("class {0} already loaded. Skipped".format(plugin_key))
        # instantiate
        cls = self.classMap[plugin_key]
        inst = cls(**plugin_params)
        for tmp_key, tmp_val in plugin_conf.items():
            if tmp_key in ["module", "name"]:
                continue
            setattr(inst, tmp_key, tmp_val)
        tmp_log.debug("created an instance of {0}".format(plugin_key))
        # return
        return inst
