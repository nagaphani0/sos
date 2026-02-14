from bs4 import BeautifulSoup
import requests
import pandas as pd
# import mysql.connector
# from mysql.connector import Error
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import os
import re
import csv
try:
    from tqdm import tqdm
except Exception:
    # fallback dummy tqdm (keeps script working if tqdm isn't installed)
    class tqdm:
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

class SOS:
    def __init__(self) :
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
        
    # pre-compiled regexes (faster than parsing when possible)
    _ID_RE = re.compile(r'Detail[^"]*?\bid=(\d+)', re.IGNORECASE)
    _NEXT_RE = re.compile(r'page-link[^"]*>\s*Next\s*<', re.IGNORECASE)

    def _fetch_page(self, page_number, url, data, params, retry_count=0, max_retries=20):
        """Fetch a single page and extract IDs with exponential backoff.

        Uses fast regex-based extraction for listing pages (much faster than full
        BeautifulSoup parsing). Falls back to BeautifulSoup only when necessary.
        Returns: (page_ids, has_next_page, success)
        """
        # work on a copy so we don't mutate the caller's dict (thread-safe)
        local_params = params.copy() if params is not None else {}
        local_params['PageNumber'] = str(page_number)

        try:
            response = self.session.post(
                url,
                params=local_params,
                cookies=self.cookies,
                headers=self.headers,
                data=data,
                timeout=60
            )

            page_ids = []
            has_next_page = False

            if response.status_code == 200:
                text = response.text

                # fast path: extract record ids with regex
                found = self._ID_RE.findall(text)
                if found:
                    page_ids = found
                else:
                    # fallback to BeautifulSoup for odd HTML
                    soup = BeautifulSoup(text, 'html.parser')
                    links = soup.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if 'id=' in href and 'Detail' in href:
                            record_id = href.split('id=')[1].split('&')[0]
                            page_ids.append(record_id)

                # detect "Next" using BS4 (regex is unreliable for attributes with quotes)
                try:
                    # We reuse the soup from fallback if it was created, or create new one
                    if 'soup' not in locals():
                        soup = BeautifulSoup(text, 'html.parser')
                    next_button = soup.find('a', {'class': 'page-link'}, string='Next')
                    if next_button and next_button.get('href'):
                        has_next_page = True
                except Exception:
                    has_next_page = False

                # print(f"Page {page_number}: Found {len(page_ids)} IDs | Has Next: {has_next_page}")
                print(f"Page {page_number}, ",end='', flush=True)
                return page_ids, has_next_page, True

            # If status code is not 200, retry with backoff
            elif response.status_code == 429:
                # 429 Too Many Requests - specific handling
                # Retry almost indefinitely for 429s (up to max_retries + 20)
                if retry_count < max_retries + 20: 
                    wait_time = 30 + (2 ** min(retry_count, 6)) + random.uniform(0, 10) # Cap exp backoff at 64s + 30s
                    print(f"Page {page_number}: Hit Rate Limit (429). Sleeping {wait_time:.2f}s... (Attempt {retry_count + 1})")
                    time.sleep(wait_time)
                    return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)
                else:
                    print(f"Page {page_number}: Failed after repeated 429s.")
                    return [], False, False

            elif retry_count < max_retries:
                wait_time = (2 ** min(retry_count, 6)) + random.uniform(0, 1)
                print(f"Page {page_number}: Status {response.status_code}. Retrying in {wait_time:.2f}s (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)
            else:
                print(f"Page {page_number}: Failed after {max_retries} retries. Status: {response.status_code}")
                return [], False, False

        except Exception as e:
            # Handle Timeout and ConnectionError aggressively
            if retry_count < max_retries:
                wait_time = (2 ** min(retry_count, 6)) + random.uniform(5, 10) # Longer base wait for connection issues
                print(f"Page {page_number}: Network Error - {e}. Retrying in {wait_time:.2f}s (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)
            else:
                print(f"Page {page_number}: Error after {max_retries} retries: {e}")
                return [], False, False

    def get_all_ids(self, url, data, params):
        """Fetch IDs from all pages using has_next_page flag"""
        ids = []
        seen_ids = set()  # track already collected IDs to avoid duplicates
        page_number = 1
        # avoid KeyError when data may not include 'BirthCounty'
        county_display = data.get('BirthCounty') or data.get('CountyName') or data.get('County') or ''
        print(f"current county: {county_display}")

        # pass copies to avoid shared-state mutation across threads
        page_ids, has_next, success = self._fetch_page(page_number, url, data.copy() if data else {}, params.copy() if params else {})
        
        # if the very first page fails, we might as well abort or return what we have
        if not success:
            print(f"First page failed for {county_display}")
            # Depending on desired behavior, could return empty or keep trying with other logic
            # For now, let's just return what we have (empty)
            return ids

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
        # Reduced max_workers to avoid 429s (Too Many Requests)
        max_threads = 3
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            page_number = 2
            active_futures = {}
            max_concurrent_pages = max_threads

            # Submit initial batch of pages
            for _ in range(max_concurrent_pages):
                future = executor.submit(self._fetch_page, page_number, url, data.copy() if data else {}, params.copy() if params else {})
                active_futures[future] = page_number
                page_number += 1

            while active_futures:
                for future in as_completed(active_futures):
                    page_num = active_futures.pop(future)
                    try:
                        page_ids, has_next_page, success = future.result()
                        
                        if success:
                            # only add new IDs not already seen — normalize and ignore empty/invalid values
                            for raw_pid in page_ids:
                                pid = str(raw_pid).strip()
                                if not pid:
                                    continue
                                if pid not in seen_ids:
                                    ids.append(pid)
                                    seen_ids.add(pid)
                            
                            # If this page has next, submit the next page
                            if has_next_page:
                                new_future = executor.submit(self._fetch_page, page_number, url, data.copy() if data else {}, params.copy() if params else {})
                                active_futures[new_future] = page_number
                                page_number += 1
                            else:
                                for f, p_num in list(active_futures.items()):
                                    if p_num > page_num:
                                        f.cancel()
                                        del active_futures[f]
                        else:
                            # If a page fails after all retries, we assume we might have reached the end or a hard continuous error.
                            # Blindly skipping to the next page causes infinite loops if the "next" pages also error (e.g. 404/500).
                            # So we just log it and stop this specific chain. 
                            print(f"Page {page_num} FAILED completely. Stopping chain from this thread.")
                            # We don't try to continue because we don't know if there IS a next page.
                            # The loop will naturally drain.
                            pass

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

        response = self.session.get(
            url,
            params=params,
            cookies=self.cookies,
            headers=self.headers,
        )

        soup = BeautifulSoup(response.text, 'html.parser')

        data = {}
        data['id'] = record_id

        # Target the correct table
        table = soup.find("table", id="detailsGrid")

        if not table:
            return data

        rows = table.find_all("tr")

        for row in rows:
            th = row.find("th")
            td = row.find("td")

            if th and td:
                key = th.get_text(" ", strip=True)
                value = td.get_text(" ", strip=True)

                data[key] = value

        return data

    def get_birth_data_by_id(self,record_id, record_type):

        params={}
        params['id'] = record_id
        params['type'] = record_type

        response = self.session.get(
            self.birth_url + 'Detail',
            params=params,
            cookies=self.cookies,
            headers=self.headers,
        )

    # Assuming 'html_content' is the HTML string you provided
        soup = BeautifulSoup(response.text, 'html.parser')

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
            # Using pandas is safer for variable keys in scraped data
            df = pd.DataFrame(rows)
            # If file exists, we append.
            # Note: If new columns appear in later chunks that weren't in the first chunk, 
            # they won't have a header in the CSV if the file already existed. 
            # But this prevents crashing on "extra keys".
            df.to_csv(filename, sep='|', mode='a', index=False, header=not file_exists)

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
    sos.run_all_counties_birth(max_workers_counties=12,
                               max_workers_data=30,
                               max_retries=7)

    # Option 2: Scrape Birth records from a single county
    # sos.run_birth(county='Douglas', max_workers=15)
     # sos.get_birth_data_by_id('121579','Birth')

    sos.process_county_birth(
    county='Douglas',
    max_workers_data=30,
    max_retries=7
    )

    # Option 3: Scrape Land records
    # sos.run_land(max_workers=10)
    # sos.run_all_counties_land(max_workers_counties=4)

    # print(sos.get_land_and_naturalization_data_by_id('land', '7672'))

    # print(sos.get_land_and_naturalization_data_by_id('naturalization', '8115'))
