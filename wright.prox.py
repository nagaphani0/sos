from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time


PROXY_USERNAME = "flkiiwre"
PROXY_PASSWORD = "j3av49mgrjcz"

def get_proxy():
    # rotating proxy session trick
    return {
        "server": "http://p.webshare.io:80",
        "username": f"{PROXY_USERNAME}-rotate",
        "password": PROXY_PASSWORD
    }


def is_cloudflare_blocked(html: str) -> bool:
    cloudflare_signatures = [
        "Just a moment",
        "Verify you are human",
        "cf-browser-verification",
        "Cloudflare Ray ID",
        "Attention Required"
    ]
    return any(sig.lower() in html.lower() for sig in cloudflare_signatures)


def fetch_page_html(url: str,counties:str,post=False, max_retries: int = 5) -> str:
    for attempt in range(1, max_retries + 1):
        print(f"\n🚀 Attempt {attempt}")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ]
            )

            context = browser.new_context(
                proxy=get_proxy(),
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            post_data = {
                'BirthCounty': counties,
                'BirthNameSearch': '',
                'BirthSubmit': 'Search',
            }
            post_params = {
                'PageNumber': str(1),
                'recordsPerPage': '50'
            }
            
            page = context.new_page()
            response = page.goto(
                url,
                
                wait_until="domcontentloaded",
                timeout=60000
            )
            page.wait_for_timeout(8000)
            
            html = page.content()

            if post:
                print('in post')
                soup = BeautifulSoup(html, "html.parser")
                nc_input = soup.find("input", {"name": "__ncforminfo"})
                if not nc_input:
                    print("❌ __ncforminfo not found")
                    browser.close()
                    continue

                nc_value = nc_input.get("value")

                print("🔑 Found __ncforminfo for Search",nc_value)

                response = context.request.post(
                url,
                headers={
                    "content-type": "application/x-www-form-urlencoded",
                    "origin": "https://s1.sos.mo.gov",
                    "referer": url
                },
                form={
                    "BirthCounty": counties,
                    "BirthNameSearch": "",
                    "BirthSubmit": "Search",
                    "__ncforminfo": nc_value
                }
                )
                post_html = response.text()
                if "Just a moment" not in post_html:
                    print("✅ POST Success")
                    browser.close()
                    return post_html

            # 🔎 Show response headers
            if response:
                print("📦 Response Headers:")
                for key, value in response.headers.items():
                    print(f"{key}: {value}")

            if not is_cloudflare_blocked(html):
                print("✅ Successfully fetched page!")
                browser.close()
                return html

            print("⚠ Cloudflare detected. Retrying with new IP...")
            browser.close()
            time.sleep(3)

    raise Exception("❌ Failed after multiple retries. Still blocked by Cloudflare.")

def get_data_by_id(record_id, record_type):
    
    params={}
    params['id'] = record_id
    params['type'] = record_type

    url = f"https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath/Detail?id={record_id}&type={record_type}"

    response = fetch_page_html(url)
    
# Assuming 'html_content' is the HTML string you provided
    soup = BeautifulSoup(response, 'html.parser')

    data = {}
    data['id'] = record_id
    data['type'] = record_type

    for table in soup.select("table.birthDeathDetail"):
        rows = table.find_all("tr")

        if len(rows) < 2:
            continue

        heads = rows[0].find_all(["th", "td"])
        vals  = rows[1].find_all("td")

        for h, v in zip(heads, vals):

            # CASE 1: li-based data
            if h.find("li") and v.find("li"):
                keys = [li.text.strip() for li in h.find_all("li")]
                vals2 = [li.text.strip() for li in v.find_all("li")]
                data.update(dict(zip(keys, vals2)))

            # CASE 2: normal th → td
            else:
                key = h.get_text(" ", strip=True)
                val = v.get_text(" ", strip=True)
                data[key] = val

    return data
    



if __name__ == "__main__":
    url='https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath/Results'

    get_tables={
        'url':'https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath',
            'post':True
            }

    html = fetch_page_html(url=get_tables['url'], post=get_tables['post'], counties="Adair")

    # data = get_data_by_id(13243, "Birth")
    print(html)