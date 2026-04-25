import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import json
import os
import re
import time

# ---------------- CONFIG ----------------
BASE_URL = "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets/advisories"
OUTPUT_DIR = "Embeddings_Data/FincenAdvisories"
HEADERS = {"User-Agent": "Mozilla/5.0"}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------- HELPERS ----------------
def find_section(text, start_keywords, end_keywords):
    start_idx = -1
    for kw in start_keywords:
        match = re.search(r'\b' + re.escape(kw) + r'\b', text, re.IGNORECASE)
        if match:
            start_idx = match.end()
            break

    if start_idx == -1:
        return ""

    end_idx = len(text)
    for kw in end_keywords:
        match = re.search(r'\b' + re.escape(kw) + r'\b', text[start_idx:], re.IGNORECASE)
        if match:
            end_idx = start_idx + match.start()
            break

    content = text[start_idx:end_idx].strip()
    return re.sub(r'Page \d+ of \d+', '', content)


def extract_bullets(section_text):
    if not section_text:
        return []
    raw_list = re.split(r'\n\s*[•▪\-*]|\n\s*\d+\.', section_text)
    return [item.strip() for item in raw_list if len(item.strip()) > 15]


def process_advisory(pdf_url):
    try:
        print(f"⬇️ Downloading PDF: {pdf_url}")
        response = requests.get(pdf_url, headers=HEADERS, timeout=20)

        if response.status_code != 200:
            print("❌ Failed to download PDF")
            return None

        doc = fitz.open(stream=response.content, filetype="pdf")
        full_text = " ".join([page.get_text() for page in doc])

        if not full_text.strip():
            print("❌ Empty PDF text")
            return None

        # Extract ID
        sar_pattern = r"FIN-\d{4}-[A-Z0-9]+"
        sar_ids = list(set(re.findall(sar_pattern, full_text)))
        primary_id = sar_ids[0] if sar_ids else os.path.basename(pdf_url).split('.')[0]

        # Extract sections
        typology_raw = find_section(
            full_text,
            ["Introduction", "Overview", "Background", "Typologies"],
            ["Red Flag Indicators", "Instructions", "Conclusion"]
        )

        red_flags_raw = find_section(
            full_text,
            ["Red Flag Indicators", "Potential Indicators"],
            ["SAR Filing Instructions", "Conclusion", "Reminder"]
        )

        red_flags_list = extract_bullets(red_flags_raw)

        return {
            "metadata": {
                "advisory_id": primary_id,
                "source": pdf_url,
                "all_ids": sar_ids
            },
            "intel": {
                "typology_narrative": " ".join(typology_raw.split())[:2000],
                "red_flags": red_flags_list,
                "indicator_count": len(red_flags_list)
            }
        }

    except Exception as e:
        print(f"❌ Error processing PDF: {e}")
        return None


# ---------------- MAIN ----------------
print("🔍 Fetching advisory listing page...")
response = requests.get(BASE_URL, headers=HEADERS)
soup = BeautifulSoup(response.text, "html.parser")

# Step 1: Get advisory page links (NOT PDFs)
page_links = []
for a in soup.find_all("a", href=True):
    href = a['href']
    if "/resources/advisories/" in href:
        full_url = href if href.startswith("http") else "https://www.fincen.gov" + href
        page_links.append(full_url)

page_links = list(set(page_links))
print(f"📄 Advisory pages found: {len(page_links)}")

if not page_links:
    print("❌ No advisory pages found. Exiting.")
    exit()

# Step 2: Visit each page → extract PDF → process
saved_count = 0

for page_url in page_links[:20]:  # limit to 20
    print(f"\n🌐 Opening advisory page: {page_url}")

    try:
        page = requests.get(page_url, headers=HEADERS)
        page_soup = BeautifulSoup(page.text, "html.parser")

        pdf_link = None
        for a in page_soup.find_all("a", href=True):
            if ".pdf" in a['href']:
                pdf_link = a['href']
                if not pdf_link.startswith("http"):
                    pdf_link = "https://www.fincen.gov" + pdf_link
                break

        if not pdf_link:
            print("⚠️ No PDF found on this page")
            continue

        data = process_advisory(pdf_link)

        if data:
            filename = f"{data['metadata']['advisory_id']}.json"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)

            print(f"✅ Saved: {filename}")
            saved_count += 1

        time.sleep(1)  # be polite to server

    except Exception as e:
        print(f"❌ Error processing page: {e}")

print(f"\n🎉 Finished! Total files saved: {saved_count}")
print(f"📁 Output folder: {OUTPUT_DIR}")