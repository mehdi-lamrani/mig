QYJP5658
Za1jbb5iH
R12q!nQo?6M

DiGiT@L+15

Za1jbb5iH
R12q!nQo?6M

Kplr24$$$

kplr$$$
Orange@22
ssh datalab@172.21.14.254
Or0nget2@2*00

---- Rebond Windows
172.21.91.5
--- Cluster Elasticsearch

# PROD IPs
172.21.173.196	ingest1.adcm.orangecm		ingest1
172.21.173.197	ingest2.adcm.orangecm		ingest2
172.21.173.198	master1.adcm.orangecm		master1
172.21.173.199	master2.adcm.orangecm		master2
172.21.173.200	master3.adcm.orangecm		master3
172.21.173.201	hotdata1.adcm.orangecm		hotdata1
172.21.173.202	hotdata2.adcm.orangecm		hotdata2
172.21.173.203	hotdata3.adcm.orangecm		hotdata3
172.21.173.204	datawarm1.adcm.orangecm		datawarm1
172.21.173.205	datawarm2.adcm.orangecm		datawarm2
172.21.173.206	datawarm3.adcm.orangecm		datawarm3
172.21.173.207	cold.adcm.orangecm		cold
172.21.173.208	monitoring.adcm.orangecm	monitoring
172.26.74.20	sondjafront.adcm.orangecm	sondjafront # serveur sur lequel on a deployé l'appli web de quicksearch (nodejs, angular)

# MANAGEMENT IPs
172.21.113.119	ingest1.adcm.orangecm		ingest1
172.21.113.120	ingest2.adcm.orangecm		ingest2
172.21.113.121	master1.adcm.orangecm		master1
172.21.113.122	master2.adcm.orangecm		master2
172.21.113.123	master3.adcm.orangecm		master3
172.21.113.124	hotdata1.adcm.orangecm		hotdata1
172.21.113.125	hotdata2.adcm.orangecm		hotdata2
172.21.113.126	hotdata3.adcm.orangecm		hotdata3
172.21.113.127	datawarm1.adcm.orangecm		datawarm1
172.21.113.128	datawarm2.adcm.orangecm		datawarm2
172.21.113.129	datawarm3.adcm.orangecm		datawarm3
172.21.113.130	cold.adcm.orangecm		cold
172.21.113.131	monitoring.adcm.orangecm	monitoring
172.26.33.108 	sondjafront.adcm.orangecm	sondjafront

vlan prod : 1953
vlan manag : 767

L@mrani24
172.21.91.5 & 172.21.71.9 (Rebond Windows)
Mehdi LAMRANI –

CUID : 	 QYJP5658
PASSWORD : DiGiT@L+15
pAt5B3wWcC


CUID : BGWF2109
PASSWORD : bvYBpo07NU

172.21.91.4 & 172.21.71.4 (Rebond linux)
Mehdi LAMRANI –
a.      CUID : QYJP5658
b.      Password Linux ; C72#d}B%2.LX)?3

CUID : BGWF2109
Password Linux ; CMAdiCD8Nm6Ej.g

su root orange (sur tout les serveurs, sauf ingest1 et ingest1: Orange@22 )


-- Accès serveur SONDJA
ssh datalab@172.21.14.254
Or0nget2@2*00

Sondja nouveau : 172.21.14.254
user : datalab
mdp _bon: Or0nget2@2*00

export PS1="\[\e[32m\][\[\e[m\]\[\e[31m\]\u\[\e[m\]\[\e[33m\]@\[\e[m\]\[\e[32m\]\h\[\e[m\]:\[\e[36m\]\w\[\e[m\]\[\e[32m\]]\[\e[m\]\[\e[32m\]\\$\[\e[m\] "



Accès Kibana PROD
    https://172.21.173.208:5601/
    login 	: elastic
    password 	: orange


Accès Quicksearch
    https://172.26.74.20/
    login : login ad



1.	LAMRANI Mehdi -
a.	CUID : qyjp5658
b.	PASSWORD : Orange@22


a.	CUID : BGWF2109 bgwf2109
b.	PASSWORD : Orange@22

Repository connections use the JVM-wide trust store for certificate verification, so you can solve this by adding your own CA certificate to the JVM-wide truststore using keytool
