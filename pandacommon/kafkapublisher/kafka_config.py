BASEDIR = "/opt/python/kafka-clients-example/python/confluent-kafka-python/kerberos/"
KAFKA_CLUSTER = "FIXME"
TOPIC = "bigpanda_mon"
PRINCIPAL = "FIXME"
GROUP_ID = "python-client-example-" + TOPIC
KEYTAB = BASEDIR + ".keytab"
CACERTS = "/etc/pki/tls/certs/"

##################################

import socket
BOOTSTRAP_SERVERS = ",".join(
                           map(lambda x: x+":9093"
                               , sorted([
                                  (socket.gethostbyaddr(i))[0] for i in (socket.gethostbyname_ex(KAFKA_CLUSTER+".cern.ch"))[2]
                                 ])
                           )
                         )
