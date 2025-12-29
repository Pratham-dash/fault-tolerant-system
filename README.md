git clone https://github.com/Pratham-dash/fault-tolerant-system.git
cd fault-tolerant-system
pip install -r requirements.txt
python3 -m venv venv
source venv/bin/activate
python init_db.py
python Input_JSON.py

Open http://localhost:5000

