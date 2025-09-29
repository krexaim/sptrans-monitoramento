from dotenv import load_dotenv
import os
import requests

BASE_URL = " http://api.olhovivo.sptrans.com.br/v2.1"

session = requests.Session()

auth_url = f"{BASE_URL}/Login/Autenticar?token={API_KEY}"
auth_response = session.post(auth_url)

if auth_response.status_code == 200 and auth_response.text.lower() == 'true':
    print("✅ Authenticated successfully!")

    # Step 2: Make an authenticated request (e.g. search bus line)
    line_search_url = f"{BASE_URL}/Linha/Buscar?termosBusca=8000"
    response = session.get(line_search_url)

    if response.status_code == 200:
        print("🔎 Bus Line Search Result:")
        print(response.json())
    else:
        print("❌ Failed to fetch bus line:", response.status_code)

else:
    print("❌ Authentication failed:", auth_response.text)

