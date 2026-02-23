from bs4 import BeautifulSoup
import requests
import pandas as pd
# import mysql.connector
# from mysql.connector import Error
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random
import os
import re
import csv
try:
    from tqdm import tqdm
except Exception:
    # fallback dummy tqdm (keeps script working if tqdm isn't installed)
    class _DummyTqdm:
        def __init__(self, total=None, desc=None, unit=None):
            self.total = total
            self.desc = desc
            self.unit = unit
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def update(self, n=1):
            return None
        def set_description(self, desc=None):
            self.desc = desc
        def close(self):
            pass
    def tqdm(total=None, desc=None, unit=None):
        return _DummyTqdm(total=total, desc=desc, unit=unit)

class SOS:
    def __init__(self) :
        self.lock = threading.Lock()
        self.birth_url='https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath/'
        self.land_url='https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/Land/'
        self.naturalization_url='https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/Naturalization'

        # self.session = requests.Session()
        # self.session.headers.update(self.headers)
        # self.session.cookies.update(self.cookies)

        # single headers declaration
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

        self.cookies = {
            'BNES_ASP.NET_SessionId': '25Rp4LoPJ3UiqGHJf9d///t7o9wnw+Ab8K3T+wSqLzKZNjpYUQdE1CXgOwT4ms7nc0+EdWuEB6rzlzmHj0Z2FRp4nlPDhmXp',
            'cf_clearance': 'LrA1NNZR8KVm9LGuLUuq8YlspDMYLkt6b9gRpoPSVsY-1770980308-1.2.1.1-eU2rZw_RFZ937bH3I8jGpK0FVGZLKy12icBzEaCrxrobNc7cY23QHgoBtoAueiHiRgkGxlI28QtB3R0QFDSr_gf_e98hgLa.v9MdPq5YxRCBg8ntlKfJ4KdogNSQLFfpWVgtsDaqfY70q2qgIvVtXhb773nflgkDMG6_2XWBe4cpgvhuKSxrUPLczPyPiNSKnjOCxCESJmydBqBD4YqdLJc1gkjx2umqRLHoyWk4HvY',
        }
        # initialize a requests.Session to reuse connections (faster)
        self.session = requests.Session()
        # Increase connection pool size for high concurrency
        adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers.update(self.headers)
        # load cookies into session if present
        try:
            self.session.cookies.update(self.cookies)
        except Exception:
            pass
        self.land_data = {
            'LNSearchMethod': 'Contains',
            'LastName': '',
            'FNSearchMethod': 'Contains',
            'FirstName': '',
            'MNSearchMethod': 'Contains',
            'MiddleName': '',
            'NameNotes': '',
            'YearRangeBegin': '0',
            'YearRangeEnd': '2026',
            'Section': '',
            'Township': '',
            'Range': '',
            'CountyName': '',
            'SeriesName': '',
            'Search': 'Search',
            'hdnSearchId': 'Name',
            '__ncforminfo': 'S8APkqWjB7tvVNi0TzG0oZFvp95_38h1Hs8uqsGUyl-Ey2p_r8gd9EMUJk3soWTZ2Pu6d3Ut1kyVDXDgYQBelcpUeo0mknfSdzR_WkyXRbVhghKZU6hyV-wYEqLjHTaIqL_R7ZDiPiDG_0Ez6yTgEV0yrdO6Xty0xpb-akoEQi3gL-xXOpc03RmbB_YAEVjaSchktD6w7ibPu4l2YOOK3EZZrYf7Z5lJed00NKhVEoHC_6EcFw90oQKtm6LUEZ5utMvxFuHMf2cbHIQN5BNB2Q==',
            }

        self.naturalization_data =  {
                'FullName': '',
                'NativeCountry': '',
                'CountyName': 'Andrew',
                'YearRangeBegin': '1816',
                'YearRangeEnd': '1955',
                'Search': 'Search',
                '__ncforminfo': 'S8APkqWjB7tvVNi0TzG0oZFvp95_38h1Hs8uqsGUyl-Ey2p_r8gd9EMUJk3soWTZ2Pu6d3Ut1kyVDXDgYQBelcpUeo0mknfSdzR_WkyXRbVhghKZU6hyV-wYEqLjHTaIqL_R7ZDiPiDG_0Ez6yTgEV0yrdO6Xty0xpb-akoEQi3gL-xXOpc03RmbB_YAEVjaSchktD6w7ibPu4l2YOOK3EZZrYf7Z5lJed00NKhVEoHC_6EcFw90oQKtm6LUEZ5utMvxFuHMf2cbHIQN5BNB2Q==',
            }


        self.page_params = {
            'PageNumber': '',
            'recordsPerPage': '75'
        }
        # MySQL database configuration
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'test'
        }


        self.all_counties = [
            "Adair",
            "Andrew",
            "Atchison",
            "Audrain",
            "Barry",
            "Barton",
            "Bates",
            "Benton",
            "Bollinger",
            "Butler",
            "Caldwell",
            "Callaway",
            "Cape Girardeau",
            "Carroll",
            "Cass",
            "Cedar",
            "Chariton",
            "Christian",
            "Clark",
            "Clay",
            "Clinton",
            "Cole",
            "Cooper",
            "Dade",
            "Dallas",
            "Daviess",
            "Dekalb",
            "Dent",
            "Douglas",
            "Franklin",
            "Gasconade",
            "Gentry",
            "Greene",
            "Grundy",
            "Harrison",
            "Hickory",
            "Holt",
            "Howard",
            "Howell",
            "Iron",
            "Jackson",
            "Jasper",
            "Jefferson",
            "Johnson",
            "Knox",
            "Laclede",
            "Lawrence",
            "Lewis",
            "Linn",
            "Livingston",
            "Macon",
            "Madison",
            "Maries",
            "Marion",
            "McDonald",
            "Mercer",
            "Miller",
            "Moniteau",
            "Monroe",
            "Morgan",
            "Newton",
            "Nodaway",
            "Oregon",
            "Osage",
            "Ozark",
            "Pemiscot",
            "Perry",
            "Pettis",
            "Phelps",
            "Pike",
            "Platte",
            "Polk",
            "Putnam",
            "Ralls",
            "Ray",
            "Reynolds",
            "Ripley",
            "Sainte Genevieve",
            "Saline",
            "Schuyler",
            "Scotland",
            "Scott",
            "Shelby",
            "St Clair",
            "St Francois",
            "St. Clair",
            "St. Louis",
            "St. Louis City",
            "Ste Genevieve",
            "Ste. Genevieve",
            "Stoddard",
            "Sullivan",
            "Texas",
            "Vernon",
            "Warren",
            "Washington",
            "Webster",
            "Worth"
]


        self.land_counties = {
            'Adair': 2,
            'Allen': 119,
            'Andrew': 3,
            'Ashley': 120,
            'Atchison': 4,
            'Audrain': 5,
            'Barry': 6,
            'Barton': 7,
            'Bates': 8,
            'Benton': 9,
            'Bollinger': 10,
            'Boone': 11,
            'Buchanan': 12,
            'Butler': 13,
            'Caldwell': 14,
            'Callaway': 15,
            'Camden': 16,
            'Cape Girardeau': 17,
            'Carroll': 18,
            'Carter': 19,
            'Cass': 20,
            'Cedar': 21,
            'Chariton': 22,
            'Christian': 23,
            'Clark': 24,
            'Clay': 25,
            'Clinton': 26,
            'Cole': 27,
            'Cooper': 28,
            'Crawford': 29,
            'Dade': 30,
            'Dallas': 31,
            'Daviess': 32,
            'Decatur': 121,
            'Dekalb': 33,
            'Dent': 34,
            'Douglas': 35,
            'Dunklin': 36,
            'Franklin': 37,
            'Gasconade': 38,
            'Gentry': 39,
            'Greene': 40,
            'Grundy': 41,
            'Harrison': 42,
            'Henry': 43,
            'Hickory': 44,
            'Highland': 122,
            'Holt': 45,
            'Howard': 46,
            'Howell': 47,
            'Iron': 48,
            'Jackson': 49,
            'Jasper': 50,
            'Jefferson': 51,
            'Johnson': 52,
            'Kinderhook': 123,
            'Knox': 53,
            'Laclede': 54,
            'Lafayette': 55,
            'Lawrence': 56,
            'Lewis': 57,
            'Lillard': 124,
            'Lincoln': 58,
            'Linn': 59,
            'Livingston': 60,
            'Macon': 62,
            'Madison': 63,
            'Maries': 64,
            'Marion': 65,
            'Mcdonald': 61,
            'Mercer': 66,
            'Miller': 67,
            'Mississippi': 68,
            'Moniteau': 69,
            'Monroe': 70,
            'Montgomery': 71,
            'Morgan': 72,
            'New Madrid': 73,
            'Newton': 74,
            'Niangua': 125,
            'Nodaway': 75,
            'None Given/Unknown': 1,
            'Oregon': 76,
            'Osage': 77,
            'Out of State': 118,
            'Ozark': 78,
            'Pemiscot': 79,
            'Perry': 80,
            'Pettis': 81,
            'Phelps': 82,
            'Pike': 83,
            'Platte': 84,
            'Polk': 85,
            'Pulaski': 86,
            'Putnam': 87,
            'Ralls': 88,
            'Randolph': 89,
            'Ray': 90,
            'Reynolds': 91,
            'Ripley': 92,
            'Saline': 99,
            'Schuyler': 100,
            'Scotland': 101,
            'Scott': 102,
            'Seneca': 126,
            'Shannon': 103,
            'Shelby': 104,
            'St. Charles': 93,
            'St. Clair': 94,
            'St. Francois': 95,
            'St. Louis': 96,
            'St. Louis City': 117,
            'Ste. Genevieve': 97,
            'Stoddard': 105,
            'Stone': 106,
            'Sullivan': 107,
            'Taney': 108,
            'Texas': 109,
            'Van Buren': 127,
            'Vernon': 110,
            'Warren': 111,
            'Washington': 112,
            'Wayne': 113,
            'Webster': 114,
            'Worth': 115,
            'Wright': 116
            }

        self.birth_data = {
            'BirthCounty': '',
            'BirthNameSearch': '',
            'BirthSubmit': 'Search',
        }
        self.birth_params = {
            'PageNumber': '',
            'recordsPerPage': '50'
        }
        self.PROXY_USERNAME = "flkiiwre"
        self.PROXY_PASSWORD = "j3av49mgrjcz"
        self.session = requests.Session()
        self.session.proxies = self.get_proxy()

    def get_proxy(self):    
        return {
            "http": f"http://{self.PROXY_USERNAME}-rotate:{self.PROXY_PASSWORD}@p.webshare.io:80",
            "https": f"http://{self.PROXY_USERNAME}-rotate:{self.PROXY_PASSWORD}@p.webshare.io:80"
        }

    def is_blocked(self, response):
        if response.status_code != 200:
            return True
        blocked_keywords = ["cf-browser-verification", "Just a moment...", "Attention Required!", "Cloudflare"]
        for word in blocked_keywords:
            if word in response.text:
                return True
        return False

    def _refresh_session(self):
        with self.lock:
            try:
                # Optional: Force close old session adapters? 
                # self.session.close() # skipping to avoid thread errors
                pass
            except:
                pass
            # Create new session
            new_session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500)
            new_session.mount('https://', adapter)
            new_session.mount('http://', adapter)
            new_session.headers.update(self.headers)
            new_session.proxies = self.get_proxy()
            try:
                new_session.cookies.update(self.cookies)
            except:
                pass
            self.session = new_session
    # pre-compiled regexes (faster than parsing when possible)
    _ID_RE = re.compile(r'Detail[^"]*?\bid=(\d+)', re.IGNORECASE)
    _NEXT_RE = re.compile(r'page-link[^"]*>\s*Next\s*<', re.IGNORECASE)

    def safe_request(self, url, method='GET', max_retries=5, **kwargs):
        """Executes a request with retry logic, session rotation, and blocking detection."""
        for attempt in range(max_retries + 1):
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
                if not self.is_blocked(response):
                    try:
                        self.cookies.update(response.cookies.get_dict())
                    except:
                        pass
                    return response
                print(f"BLOCKED (Status {response.status_code}). Rotating session... ({attempt+1}/{max_retries})")
            except Exception as e:
                print(f"Request Error: {e} | Rotating... ({attempt+1}/{max_retries})")
            
            self._refresh_session()
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait_time)
        return None

    def _fetch_page(self, page_number, url, data, params, retry_count=0, max_retries=5):
        """Fetch a single page and extract IDs using safe_request."""
        local_params = params.copy() if params is not None else {}
        local_params['PageNumber'] = str(page_number)

        response = self.safe_request(url, method='POST', max_retries=max_retries, data=data, params=local_params)
        
        if not response or response.status_code != 200:
            return [], False

        print('fetch page', page_number, response.status_code)
        page_ids = []
        has_next_page = False
        text = response.text

        # fast path: extract record ids with regex
        found = self._ID_RE.findall(text)
        if found:
            page_ids = found
        else:
            soup = BeautifulSoup(text, 'html.parser')
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if 'id=' in href and 'Detail' in href:
                    record_id = href.split('id=')[1].split('&')[0]
                    page_ids.append(record_id)

        if self._NEXT_RE.search(text):
            has_next_page = True
        else:
            try:
                soup = BeautifulSoup(text, 'html.parser')
                next_button = soup.find('a', {'class': 'page-link'}, string='Next')
                if next_button and next_button.get('href'):
                    has_next_page = True
            except Exception:
                has_next_page = False

        print(f"Page {page_number}",end='')
        return page_ids, has_next_page

    def get_all_ids(self, url, data, params):
        """Fetch IDs from all pages using has_next_page flag"""
        ids = []
        seen_ids = set()  # track already collected IDs to avoid duplicates
        page_number = 1
        # avoid KeyError when data may not include 'BirthCounty'
        county_display = data.get('BirthCounty') or data.get('CountyName') or data.get('County') or ''
        print(f"current county: {county_display}")

        # pass copies to avoid shared-state mutation across threads
        page_ids, has_next = self._fetch_page(page_number, url, data.copy() if data else {}, params.copy() if params else {})
        # only add new IDs not already seen — normalize and ignore empty/invalid values
        for raw_pid in page_ids:
            pid = str(raw_pid).strip()
            if not pid:
                continue
            if pid not in seen_ids:
                ids.append(pid)
                seen_ids.add(pid)

        if not has_next or len(page_ids) == 0:
            print(f"Total IDs found: {len(ids)}")
            return ids

        # Keep fetching pages with parallel workers until has_next_page is False
        with ThreadPoolExecutor(max_workers=20) as executor:
            page_number = 2
            active_futures = {}
            max_concurrent_pages = 20

            # Submit initial batch of pages
            for _ in range(max_concurrent_pages):
                future = executor.submit(self._fetch_page, page_number, url, data.copy() if data else {}, params.copy() if params else {})
                active_futures[future] = page_number
                page_number += 1

            while active_futures:
                for future in as_completed(active_futures):
                    page_num = active_futures.pop(future)
                    try:
                        page_ids, has_next_page = future.result()
                        # only add new IDs not already seen — normalize and ignore empty/invalid values
                        for raw_pid in page_ids:
                            pid = str(raw_pid).strip()
                            if not pid:
                                continue
                            if pid not in seen_ids:
                                ids.append(pid)
                                seen_ids.add(pid)
                        # Small delay between adding results
                        # time.sleep(0.1)
                        # If this page has next, submit the next page
                        if has_next_page:
                            new_future = executor.submit(self._fetch_page, page_number, url, data.copy() if data else {}, params.copy() if params else {})
                            active_futures[new_future] = page_number
                            page_number += 1
                        # If this page has no next, we've reached the end
                        else:
                            for f in list(active_futures.keys()):
                                f.cancel()
                            active_futures.clear()
                            break
                    except Exception as e:
                        print(f"Error processing page {page_num}: {e}")

        # defensive final dedupe (preserve order) — protects against any accidental duplicates
        ordered = []
        seen_final = set()
        for pid in ids:
            if pid not in seen_final:
                ordered.append(pid)
                seen_final.add(pid)
        ids = ordered

        print(f"Total IDs found: {len(ids)}")
        return ids

    def get_land_and_naturalization_data_by_id(self, url, record_id):
        params = {}
        params['id'] = record_id
        if url == 'land':
            url = self.land_url + 'Detail'
        else:
            url = self.naturalization_url + 'Detail'

        response = self.safe_request(url, params=params)
        if not response:
            return {}

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            data = {'id': record_id}
            table = soup.find("table", id="detailsGrid")
            if not table:
                return data
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    data[th.get_text(" ", strip=True)] = td.get_text(" ", strip=True)
            return data
        except Exception as e:
            print(f"Error parse ID {record_id}: {e}")
            return {}

    def get_birth_data_by_id(self,record_id, record_type):
        params={'id': record_id, 'type': record_type}
        response = self.safe_request(self.birth_url + 'Detail', params=params)
        if not response:
            return {}
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            data = {'id': record_id, 'type': record_type}
            for table in soup.select("table.birthDeathDetail"):
                rows = table.find_all("tr")
                if len(rows) < 2: continue
                heads = rows[0].find_all(["th", "td"])
                vals  = rows[1].find_all("td")
                for h, v in zip(heads, vals):
                    if h.find("li") and v.find("li"):
                        keys = [li.text.strip() for li in h.find_all("li")]
                        vals2 = [li.text.strip() for li in v.find_all("li")]
                        data.update(dict(zip(keys, vals2)))
                    else:
                        data[h.get_text(" ", strip=True)] = v.get_text(" ", strip=True)
            return data
        except Exception as e:
            print(f"Error fetch/parse ID {record_id}: {e}")
            return {}

    def _append_rows_csv(self, rows, filename):
        """Append rows (dicts or tuples) to a delimited file efficiently."""
        if not rows:
            return

        first = rows[0]
        file_exists = os.path.exists(filename)
        # tuples / lists
        if isinstance(first, (list, tuple)):
            # infer header for 2-column tuples (id, county) for readability
            if len(first) == 2:
                header = ['id', 'county']
            else:
                header = [f'col{i}' for i in range(len(first))]
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                if not file_exists:
                    writer.writerow(header)
                writer.writerows(rows)
        # dicts
        elif isinstance(first, dict):
            fieldnames = list(first.keys())
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
                if not file_exists:
                    writer.writeheader()
                for r in rows:
                    writer.writerow(r)
        else:
            # fallback to pandas for unknown types
            df = pd.DataFrame(rows)
            df.to_csv(filename, sep='|', mode='a', index=False, header=not file_exists)

    def export_data(self, data,filename='Birth_ids_data.csv'):
        """Write `data` to `filename` using a memory-efficient writer.

        Accepts a list of dicts or a list of tuples/lists.
        """
        if not data:
            print(f"No rows to write to {filename}")
            return

        # write in chunks for large lists to avoid memory spikes
        chunk_size = 5000
        if isinstance(data, list) and len(data) > chunk_size:
            for i in range(0, len(data), chunk_size):
                self._append_rows_csv(data[i:i+chunk_size], filename)
        else:
            self._append_rows_csv(data, filename)

        print(f"Appended {len(data)} rows to {filename}")
      
    # Save to MySQL
    # self.save_to_mysql(data)

    # def save_to_mysql(self, data):
    #     """Save scraped data to MySQL database with exact columns"""
    #     try:
    #         conn = mysql.connector.connect(**self.db_config)
    #         cursor = conn.cursor()

    #         # Create table with exact columns from the data structure
    #         # create_table_query = """
    #         # CREATE TABLE IF NOT EXISTS birth_death_record (
    #         #     db_id INT AUTO_INCREMENT PRIMARY KEY,
    #         #     id VARCHAR(50),
    #         #     type VARCHAR(20),
    #         #     County VARCHAR(100),
    #         #     `Roll Number` VARCHAR(50),
    #         #     Page VARCHAR(50),
    #         #     Number VARCHAR(50),
    #         #     `Date of Return (Month/Day/Year)` VARCHAR(100),
    #         #     `Name of Child` VARCHAR(150),
    #         #     Sex VARCHAR(50),
    #         #     `No. of Child of this Mother` VARCHAR(50),
    #         #     `Race or Color` VARCHAR(100),
    #         #     `Date of Birth` VARCHAR(100),
    #         #     `Place of Birth` VARCHAR(150),
    #         #     `Nationality of Father` VARCHAR(100),
    #         #     Age VARCHAR(50),
    #         #     `Nationality of Mother` VARCHAR(100),
    #         #     `Full Name of Mother` VARCHAR(150),
    #         #     `Maiden Name of Mother` VARCHAR(150),
    #         #     `Residence of Mother` VARCHAR(150),
    #         #     `Full Name of Father` VARCHAR(150),
    #         #     Occupation VARCHAR(100),
    #         #     `Name and Address of Medical Attendant` VARCHAR(200),
    #         #     `Name and Address of Person making Certificate` VARCHAR(200),
    #         #     `Returned by` VARCHAR(150),
    #         #     NOTE LONGTEXT
    #         # )
    #         # """
    #         # cursor.execute(create_table_query)

    #         # Insert data
    #         insert_query = """
    #         INSERT INTO birth_death_record
    #         (id, type, County, `Roll Number`, Page, Number,
    #          `Date of Return (Month/Day/Year)`, `Name of Child`, Sex, `No. of Child of this Mother`,
    #          `Race or Color`, `Date of Birth`, `Place of Birth`, `Nationality of Father`, Age,
    #          `Nationality of Mother`, `Full Name of Mother`, `Maiden Name of Mother`,
    #          `Residence of Mother`, `Full Name of Father`, Occupation,
    #          `Name and Address of Medical Attendant`, `Name and Address of Person making Certificate`,
    #          `Returned by`, NOTE)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         """

    #         for record in data:
    #             values = (
    #                 record.get('id', ''),
    #                 record.get('type', ''),
    #                 record.get('County', ''),
    #                 record.get('Roll Number', ''),
    #                 record.get('Page', ''),
    #                 record.get('Number', ''),
    #                 record.get('Date of Return (Month/Day/Year)', ''),
    #                 record.get('Name of Child', ''),
    #                 record.get('Sex', ''),
    #                 record.get('No. of Child of this Mother', ''),
    #                 record.get('Race or Color', ''),
    #                 record.get('Date of Birth', ''),
    #                 record.get('Place of Birth', ''),
    #                 record.get('Nationality of Father', ''),
    #                 record.get('Age', ''),
    #                 record.get('Nationality of Mother', ''),
    #                 record.get('Full Name of Mother', ''),
    #                 record.get('Maiden Name of Mother', ''),
    #                 record.get('Residence of Mother', ''),
    #                 record.get('Full Name of Father', ''),
    #                 record.get('Occupation', ''),
    #                 record.get('Name and Address of Medical Attendant', ''),
    #                 record.get('Name and Address of Person making Certificate', ''),
    #                 record.get('Returned by', ''),
    #                 record.get('NOTE', '')
    #             )
    #             cursor.execute(insert_query, values)

    #         conn.commit()
    #         print(f"Successfully inserted {len(data)} records into MySQL database")

    #     except Error as e:
    #         print(f"Error while connecting to MySQL or inserting data: {e}")
    #     finally:
    #         if conn.is_connected():
    #             cursor.close()
    #             conn.close()
    #             print("MySQL connection closed")

    def process_county_birth(self, county, max_workers_data=50, max_retries=5):
        """Process a single county: Fetch IDs, then fetch details, saving incrementally."""
        local_data = self.birth_data.copy()
        local_data['BirthCounty'] = county
        local_params = self.birth_params.copy()

        ids = []
        # Retry logic for fetching IDs
        for attempt in range(max_retries + 1):
            try:
                ids = self.get_all_ids(self.birth_url, local_data, local_params)
                if ids:
                    break
            except Exception as e:
                print(f"Error fetching IDs for {county}: {e}")
            
            if attempt < max_retries:
                wait_time = (2 ** attempt) * 1
                # print(f"Retrying {county} IDs in {wait_time}s...")
                time.sleep(wait_time)
        
        if not ids:
            print(f"Skipping {county} (No IDs found after retries)")
            return

        # Save IDs immediately
        self.export_data([(rid, county) for rid in ids], filename=f"Birth_{county}_ids.csv")
        
        # Fetch Details
        records_file = f"Birth_{county}_records.csv"
        buffer = []
        
        with ThreadPoolExecutor(max_workers=max_workers_data) as executor:
            future_to_id = {executor.submit(self.get_birth_data_by_id, record_id, 'Birth'): record_id for record_id in ids}
            
            with tqdm(total=len(ids), desc=f"Birth - {county}", unit="id") as pbar:
                for future in as_completed(future_to_id):
                    try:
                        rec = future.result()
                        if isinstance(rec, dict):
                            rec['source_county'] = county
                        buffer.append(rec)
                        
                        if len(buffer) >= 200:
                            self.export_data(buffer, filename=records_file)
                            buffer = []
                    except Exception as e:
                        pass
                    finally:
                        pbar.update(1)
        
        # Final flush
        if buffer:
            self.export_data(buffer, filename=records_file)
        
        print(f"Completed {county}: {len(ids)} records saved.")

    def run_birth(self, county='Douglas', max_workers=50):
        """Scrape Birth records for a single county"""
        self.birth_data['BirthCounty'] = county
        ids = self.get_all_ids(self.birth_url, self.birth_data.copy(), self.birth_params.copy())
        data = []

        # Use ThreadPoolExecutor for concurrent fetching with a per-county progress bar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {executor.submit(self.get_birth_data_by_id, record_id, 'Birth'): record_id for record_id in ids}

            with tqdm(total=len(ids), desc=f"Birth - {county}", unit="id") as pbar:
                for future in as_completed(future_to_id):
                    record_id = future_to_id[future]
                    try:
                        record_data = future.result()
                        data.append(record_data)
                    except Exception as e:
                        # keep going on errors but show minimal info
                        print(f"Error scraping ID {record_id}: {e}")
                    finally:
                        pbar.update(1)

        # write per-county files and append to global file
        self.export_data(data, filename=f"Birth_{county}_records.csv")
        self.export_data([(rid, county) for rid in ids], filename=f"Birth_{county}_ids.csv")
        self.export_data(data)

    def run_land(self, max_workers=50):
        """Scrape Land records"""
        ids = self.get_all_ids(self.land_url, self.land_data.copy(), self.page_params.copy())
        data = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # use the land/naturalization detail fetcher (first arg indicates type)
            future_to_id = {executor.submit(self.get_land_and_naturalization_data_by_id, 'land', record_id): record_id for record_id in ids}

            with tqdm(total=len(ids), desc="Land", unit="id") as pbar:
                for future in as_completed(future_to_id):
                    record_id = future_to_id[future]
                    try:
                        record_data = future.result()
                        data.append(record_data)
                    except Exception as e:
                        print(f"Error scraping ID {record_id}: {e}")
                    finally:
                        pbar.update(1)

        # per-run CSV + aggregated
        self.export_data(data, filename='Land_records.csv')
        self.export_data([(rid, '') for rid in ids], filename='land_ids_by_county.csv')

    def run_all_counties_birth(self, max_workers_counties=10, max_workers_data=25, max_retries=5):
        """Fetch Birth data from all counties in parallel, processing each county fully."""
        print(f"\n{'='*60}")
        print(f"Starting parallel scrape for {len(self.all_counties)} counties")
        print(f"{'='*60}\n")

        with ThreadPoolExecutor(max_workers=max_workers_counties) as executor:
            # Submit all counties
            future_to_county = {
                executor.submit(self.process_county_birth, county, max_workers_data, max_retries): county 
                for county in self.all_counties
            }
            
            for future in as_completed(future_to_county):
                county = future_to_county[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"County {county} generated an exception: {e}")

        print(f"\n{'='*60}")
        print("All counties processed.")
        print(f"{'='*60}\n")

    def run_all_counties_land(self, max_workers_counties=10, max_workers_data=25, max_retries=5):
        """Fetch Land IDs from all counties using land_counties mapping"""
        print(f"\n{'='*60}")
        print(f"Starting to scrape Land records from all {len(self.land_counties)} counties")
        print(f"{'='*60}\n")

        all_ids_by_county = {}
        counties_to_process = list(self.land_counties.keys())
        retry_count = 0

        while counties_to_process and retry_count < max_retries:
            if retry_count > 0:
                wait_time = (2 ** (retry_count - 1)) * 5
                time.sleep(wait_time)

            with ThreadPoolExecutor(max_workers=max_workers_counties) as executor:
                futures = {}
                for county_name in counties_to_process:
                    current_data = self.land_data.copy()
                    current_data['CountyName'] = str(self.land_counties[county_name])
                    future = executor.submit(self.get_all_ids, url=self.land_url, data=current_data, params=self.page_params.copy())
                    futures[future] = county_name

                for future in as_completed(futures):
                    county = futures[future]
                    try:
                        ids = future.result()
                        all_ids_by_county[county] = ids
                        print(f"✓ Land County '{county}' (ID: {self.land_counties[county]}): {len(ids)} IDs fetched")
                    except Exception as e:
                        print(f"✗ Error fetching Land IDs for {county}: {e}")

            counties_to_process = [c for c in counties_to_process if len(all_ids_by_county.get(c, [])) == 0]
            retry_count += 1

        total_ids = sum(len(ids) for ids in all_ids_by_county.values())
        print(f"\nTotal Land IDs found: {total_ids}")

        all_record_ids = [(record_id, county) for county, ids in all_ids_by_county.items() for record_id in ids]

        # Fetch land record details per-county with a tqdm bar for each county
        print("Fetching land record details for all counties (per-county progress bars)...")
        buffer_size = 200

        for county, ids in all_ids_by_county.items():
            if not ids:
                continue

            county_details = []
            ids_file = f"Land_{county}_ids.csv"
            records_file = f"Land_{county}_records.csv"

            with ThreadPoolExecutor(max_workers=max_workers_data) as executor:
                futures = {executor.submit(self.get_land_and_naturalization_data_by_id, 'land', record_id): record_id for record_id in ids}

                with tqdm(total=len(ids), desc=f"Land - {county}", unit="id") as pbar:
                    for future in as_completed(futures):
                        record_id = futures[future]
                        try:
                            rec = future.result()
                            if isinstance(rec, dict):
                                rec['source_county'] = county
                            county_details.append(rec)

                            if len(county_details) >= buffer_size:
                                self.export_data(county_details, filename=records_file)
                                county_details = []
                        except Exception as e:
                            print(f"Error fetching land detail {record_id} for {county}: {e}")
                        finally:
                            pbar.update(1)

            if county_details:
                self.export_data(county_details, filename=records_file)

            self.export_data([(rid, county) for rid in ids], filename=ids_file)

        # export aggregated IDs for compatibility
        self.export_data(all_record_ids, filename='land_ids_by_county.csv')


    def run_all_counties_naturalization(self, max_workers_counties=10, max_workers_data=25, max_retries=5):
        """Fetch Naturalization IDs and details from all counties (per-county CSVs + progress bars)"""
        print(f"\n{'='*60}")
        print(f"Starting to scrape Naturalization records from all {len(self.all_counties)} counties")
        print(f"{'='*60}\n")

        all_ids_by_county = {}
        counties_to_process = self.all_counties[:]
        retry_count = 0

        while counties_to_process and retry_count < max_retries:
            if retry_count > 0:
                wait_time = (2 ** (retry_count - 1)) * 5
                time.sleep(wait_time)

            with ThreadPoolExecutor(max_workers=max_workers_counties) as executor:
                futures = {}
                for county in counties_to_process:
                    current_data = self.naturalization_data.copy()
                    current_data['CountyName'] = county
                    future = executor.submit(self.get_all_ids, url=self.naturalization_url, data=current_data, params=self.page_params.copy())
                    futures[future] = county

                for future in as_completed(futures):
                    county = futures[future]
                    try:
                        ids = future.result()
                        all_ids_by_county[county] = ids
                        print(f"✓ Naturalization County '{county}': {len(ids)} IDs fetched")
                    except Exception as e:
                        print(f"✗ Error fetching Naturalization IDs for {county}: {e}")

            counties_to_process = [c for c in counties_to_process if len(all_ids_by_county.get(c, [])) == 0]
            retry_count += 1

        total_ids = sum(len(ids) for ids in all_ids_by_county.values())
        print(f"\nTotal Naturalization IDs found: {total_ids}")

        all_record_ids = [(record_id, county) for county, ids in all_ids_by_county.items() for record_id in ids]

        # Fetch details per-county and write per-county CSVs with a tqdm per county
        buffer_size = 200
        for county, ids in all_ids_by_county.items():
            if not ids:
                continue

            county_details = []
            ids_file = f"Naturalization_{county}_ids.csv"
            records_file = f"Naturalization_{county}_records.csv"

            with ThreadPoolExecutor(max_workers=max_workers_data) as executor:
                futures = {executor.submit(self.get_land_and_naturalization_data_by_id, 'naturalization', record_id): record_id for record_id in ids}

                with tqdm(total=len(ids), desc=f"Naturalization - {county}", unit="id") as pbar:
                    for future in as_completed(futures):
                        record_id = futures[future]
                        try:
                            rec = future.result()
                            if isinstance(rec, dict):
                                rec['source_county'] = county
                            county_details.append(rec)

                            if len(county_details) >= buffer_size:
                                self.export_data(county_details, filename=records_file)
                                county_details = []
                        except Exception as e:
                            print(f"Error fetching naturalization detail {record_id} for {county}: {e}")
                        finally:
                            pbar.update(1)

            if county_details:
                self.export_data(county_details, filename=records_file)

            self.export_data([(rid, county) for rid in ids], filename=ids_file)

        # also keep aggregated export for compatibility
        self.export_data(all_record_ids)

if __name__ == "__main__":
    sos = SOS()

    # Option 1: Scrape Birth records from all counties
    sos.run_all_counties_birth(max_workers_counties=1,
                               max_workers_data=30,
                               max_retries=7)

    # Option 2: Scrape Birth records from a single county
    # sos.run_birth(county='Douglas', max_workers=15)

    # Option 3: Scrape Land records
    # sos.run_land(max_workers=10)
    # sos.run_all_counties_land(max_workers_counties=4)


    # print(sos.get_land_and_naturalization_data_by_id('land', '7672'))

    # print(sos.get_land_and_naturalization_data_by_id('naturalization', '8115'))
