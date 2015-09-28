from pandalogger.PandaLogger import PandaLogger
tmpPandaLogger = PandaLogger()
tmpPandaLogger.lock()
tmpPandaLogger.setParams({'Type':'retryModule'})
tmpLogger = tmpPandaLogger.getHttpLogger('dev')
tmpLogger.debug("This is only a test")
