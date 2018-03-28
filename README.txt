Panda Common Project
--------------------

Includes all libraries used by both server and monitor (and others).


Release Note
------------

* current
  * added hook to LogWrapper
  * added sendMsg to LogWrapper
  * added buffering to LogWrapper
  * added a protection to PandaLogger to avoid duplicated logging
  * added a protection to PandaLogger against multiple import
  * added a build number to version name
  * added pandautils
  * added LogWrapper 
  * imposed the limit of 4000 on the logging message
  * use POST instead of GET to send log ( <- suspicious 2013/02/13 )
  * added semaphore to logger to limit the number of concurrent emitters

* 0.0.5 (5/15/2009)
  * tagged for 0.0.5

* 0.0.4 (5/15/2009)
  * changed default port for logger

* 0.0.3 (12/19/2008)
  * adjustments for CERN

* 0.0.2 (12/18/2008)
   * migrated to Oracle

* 0.0.1 (12/4/2008)
   * first import
