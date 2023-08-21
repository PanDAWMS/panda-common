Panda Common Project
--------------------

Includes all libraries used by both server and monitor (and others).


Release Note
------------

* 0.0.43 (15/8/2023)
  * allow to send message headers of msg_processor for priority or other message broker options
  * improve efficiency of multi-threading msg_processor

* 0.0.42 (11/8/2023)
  * updates on thread_utils.py

* 0.0.41 (21/7/2023)
  * to avoid relative import

* 0.0.40 (21/7/2023)
  * to ignore robot.txt when getting CA certs

* 0.0.36 (19/5/2023)
  * support listener blocking, configurable max_buffer_len and ack_mode
  
* 0.0.34 (24/4/2023)
  * added LockPool

* 0.0.32 (17/11/2022)
  * heartbeat in stomp connection
  
* 0.0.31 (11/7/2022)
  * added expand_values

* 0.0.30 (1/7/2022)
  * added message removal functions to MBSenderProxy

* 0.0.29 (27/6/2022)
  * env vars in msg config json

* 0.0.28 (27/5/2022)
  * install_itgf_ca

* 0.0.27 (26/5/2022)
  * PANDA_BEHIND_REAL_LB

* 0.0.26 (25/5/2022)
  * for containerization

* 0.0.24 (24/6/2021)
  * added WeightedLists

* 0.0.21 (6/5/2021)
  * to change panda.log all-writable to avoid the ownership issue between root and service account when the service gets started

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
