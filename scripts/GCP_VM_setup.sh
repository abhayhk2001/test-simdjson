clear
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install git python3 python3-pip python-is-python3 python3-venv -y
git clone http://github.com/abhayhk2001/test-simdjson
pip install virtualenv

cd test-simdjson
python -m venv env
source env/bin/activate
pip install -r requirements.txt
mkdir output extras
clear

