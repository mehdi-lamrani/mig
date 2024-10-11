update-alternatives --install /usr/bin/python python /usr/bin/python2 2
update-alternatives --install /usr/bin/python python /usr/bin/python3 1
sudo yum install gcc
sudo yum install libffi-devel
sudo yum install python3-devel
pip3 install --upgrade pip setuptools


sudo alternatives --config python
sudo yum update
sudo yum install libffi-devel
sudo yum install libffi-devel
sudo yum install gcc
sudo yum install python3-devel
pip3 install --upgrade pip setuptools
pip3 install paramiko

scp /usr/lib/systemd/system/elasticsearch.service user@serverB:/usr/lib/systemd/system/
scp /usr/lib/systemd/system/elasticsearch.service user@serverB:/tmp

curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
{
  "persistent": {
    "cluster.routing.allocation.exclude._name": "quicksearch-dnode-1"
  }
}'
sudo netstat -tuln | grep 9200
sudo firewall-cmd --list-ports
sudo firewall-cmd --zone=public --add-port=9200/tcp --permanent
sudo firewall-cmd --zone=public --add-port=9300/tcp --permanent
sudo firewall-cmd --zone=public --add-port=514/tcp --permanent
sudo firewall-cmd --reload
sudo firewall-cmd --list-ports


sudo mkdir -p /data/elasticsearch
sudo chown elasticsearch:elasticsearch /data/elasticsearch
sudo chmod 750 /data/elasticsearch

sudo ls -l /etc/elasticsearch/certs/quicksearch-dnode-8.p12
sudo chown elasticsearch:elasticsearch /etc/elasticsearch/certs/quicksearch-dnode-8.p12
sudo chmod 600 /etc/elasticsearch/certs/quicksearch-dnode-8.p12

sudo /usr/share/elasticsearch/bin/elasticsearch-keystore add xpack.security.transport.ssl.keystore.secure_password
sudo /usr/share/elasticsearch/bin/elasticsearch-keystore add xpack.security.transport.ssl.truststore.secure_password
sudo /usr/share/elasticsearch/bin/elasticsearch-keystore add xpack.security.http.ssl.keystore.secure_password
sudo /usr/share/elasticsearch/bin/elasticsearch-keystore add xpack.security.http.ssl.truststore.secure_password


sudo /usr/share/elasticsearch/bin/elasticsearch-certutil cert --ca /etc/elasticsearch/certs/ca/ca.p12 --name quicksearch-dnode-8 --ip 172.21.113.217 --dns cmndg-colddatanode02

sudo /usr/share/elasticsearch/bin/elasticsearch-certutil cert --ca /etc/elasticsearch/certs/ca/ca.p12 --name quicksearch-dnode-8 --ip 172.21.113.217 --dns cmndg-colddatanode02

sudo /usr/share/elasticsearch/bin/elasticsearch-certutil cert --ca-cert extracted_ca-master.cer --ca-key /etc/elasticsearch/certs/ca/ca_private.key --name quicksearch-dnode-8 --ip 172.21.113.217 --dns cmndg-colddatanode02

scp user@existing_node:/path/to/elastic-stack-ca.p12 /etc/elasticsearch/certs/
/etc/elasticsearch/certs/quicksearch-dnode-8.p12

openssl pkcs12 -export -in quicksearch.adcm.orangecm.crt -inkey quicksearch.key -out quicksearch-dnode-8.p12 -name quicksearch-dnode-8
ulimit -n 65535

keytool -import -alias rootCA -file rquicksearch.adcm.orangecm.crt -keystore /usr/share/elasticsearch/jdk/lib/security/cacerts -storepass orange

keytool -import -alias rootCA -file quicksearch.adcm.orangecm.crt -keystore /usr/share/elasticsearch/jdk/lib/security/cacerts -storepass orange

quicksearch.adcm.orangecm.crt
handshake failed because connection reset

sudo /usr/share/elasticsearch/bin/elasticsearch-certutil cert --in /home/cdjk5546/instance-ocm-dnode8.yml --keep-ca-key --out /home/cdjk5546/ocm-certificate-dnode-8.zip

sudo keytool -import -alias s3-cert -file /tmp/quicksearch-s3.adcm.orangecm.crt -keystore /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.131-11.b12.el7.x86_64/jre/lib/security/cacerts

keytool -list -v -keystore /etc/elasticsearch/certs/quicksearch-dnode-8.p12 -storepass orange
keytool -list -v -keystore /etc/elasticsearch/certs/quicksearch-mnode-1.p12 -storepass orange

/usr/share/elasticsearch/bin/elasticsearch-certutil cert --ca-cert /etc/elasticsearch/certs/ca/ca.p12 --ca-pass orange --name quicksearch-dnode-8 --dns cmndg-colddatanode02 --ip 172.21.113.217 --out /etc/elasticsearch/certs/quicksearch-dnode-8.p1200 --pass orange
