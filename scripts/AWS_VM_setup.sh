ssh -i "abhay_comp.pem" ec2-user@ec2-107-23-184-227.compute-1.amazonaws.com

clear
sudo yum update
sudo yum upgrade -y
sudo yum install git python3 python3-pip python-is-python3 python3-venv -y
git clone http://github.com/abhayhk2001/test-simdjson
pip3 install virtualenv

cd test-simdjson
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
mkdir output extras
clear