import threading

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger('plugin_factory')


# plugin factory
class PluginFactory(object):
    # class lock
    __lock = threading.Lock()

    # constructor
    def __init__(self):
        self.classMap = dict()

    # get plugin
    def get_plugin(self, plugin_conf):
        # logger
        tmpLog = logger_utils.make_logger(base_logger, method_name='get_plugin')
        # use module + class as key
        moduleName = plugin_conf['module']
        className = plugin_conf['name']
        if moduleName is None or className is None:
            tmpLog.warning('Invalid plugin; either module or name is missing '.format(moduleName))
            return None
        pluginKey = '{0}.{1}'.format(moduleName, className)
        # get class
        with self.__lock:
            if pluginKey not in self.classMap:
                # import module
                tmpLog.debug('importing {0}'.format(moduleName))
                mod = __import__(moduleName)
                for subModuleName in moduleName.split('.')[1:]:
                    mod = getattr(mod, subModuleName)
                # get class
                tmpLog.debug('getting class {0}'.format(className))
                cls = getattr(mod, className)
                # add
                self.classMap[pluginKey] = cls
                tmpLog.debug('loaded class {0}'.format(pluginKey))
            else:
                tmpLog.debug('class {0} already loaded. Skipped'.format(pluginKey))
        # instantiate
        cls = self.classMap[pluginKey]
        inst = cls()
        for tmpKey, tmpVal in plugin_conf.items():
            if tmpKey in ['module', 'name']:
                continue
            setattr(inst, tmpKey, tmpVal)
        tmpLog.debug('created an instance of {0}'.format(pluginKey))
        # return
        return inst
