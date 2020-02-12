Panda Common Project
--------------------

Includes all libraries used by both server and monitor (and others).


Release Note
------------

* 0.0.20 (2/12/2020)
  * fixed logging.handlers

* 0.0.19 (2/6/2020)
  * fixed SSLCertVerificationError with hostname mismatch

* 0.0.18 (2/4/2020)
  * added HTTP Adaptor with random DNS resolution

* 0.0.17 (1/20/2020)
  * added msg stuff 
  
* 0.0.15 (11/7/2019)
  * disabled egg

* 0.0.12 (10/14/2019)
  * python 2 and 3
  * using pip
  * added log_level 
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
