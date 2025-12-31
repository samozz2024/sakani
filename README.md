# Sakani Scraper

---

## Setup Instructions

### 1. Environment Variables
Create a `.env` file in the project root and add the following:

```env
PROXY_ENDPOINT=rp.scrapegw.com:6060
PROXY_USERNAME=username
PROXY_PASSWORD=password
USE_PROXY=True
```

### 2. Create Virtual Environment and Install Dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Scraper
```bash
python main.py
```

### note: 
- make sure to edit `configuration.py` if needed (Be careful with Rate limiting settings)
