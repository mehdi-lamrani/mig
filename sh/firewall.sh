sudo firewall-cmd --add-port=8000/tcp --permanent
sudo firewall-cmd --add-port=4040/tcp --permanent
sudo firewall-cmd --add-port=8080/tcp --permanent
sudo firewall-cmd --reload
sudo firewall-cmd --list-all
