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
        self.death_data = {
            'DeathCounty': '',
            'DeathNameSearch': '',
            'DeathSubmit': 'Search',
            # '__ncforminfo': 't-Hg9mr_FNwXJJin8XMh4m0jckjliJm8EjF6NJtitEgM__JvbscddmjXlPTXuyx1hyhHr0AqgrVdlYEDeJPH-PauvqBqNiAFO58Pucl_0fgeZodE8OS-Ow==',
        }

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

        # MySQL database configuration
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'test'
        }
        self.all_death_counties=[
        "Adair", "Andrew", "Audrain", "Barry", "Barton", "Bates", "Benton",
        "Bollinger", "Buchanan", "Butler", "Callaway", "Cape Girardeau",
        "Carroll", "Cass", "Cedar", "Chariton", "Clark", "Clay", "Clinton",
        "Cole", "Cooper", "Dade", "Dallas", "Daviess", "Dent", "Douglas",
        "Franklin", "Gasconade", "Gentry", "Greene", "Grundy", "Harrison",
        "Hickory", "Holt", "Howard", "Howell", "Iron", "Jackson", "Jasper",
        "Jefferson", "Johnson", "Knox", "Lawrence", "Lewis", "Lincoln",
        "Linn", "Livingston", "Macon", "Madison", "Maries", "Marion",
        "Mercer", "Miller", "Moniteau", "Monroe", "Morgan", "Newton",
        "Nodaway", "Oregon", "Osage", "Ozark", "Perry", "Phelps", "Platte",
        "Polk", "Ralls", "Ray", "Reynolds", "Ripley", "Sainte Genevieve",
        "Saline", "Schuyler", "Scotland", "Scott", "Shelby", "St. Clair",
        "St. Francois", "St. Louis", "St. Louis City", "Ste. Genevieve",
        "Sullivan", "Texas", "Vernon", "Warren", "Washington", "Webster",
        "Worth"
        ]

        self.all_birth_counties = [
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
            '__ncforminfo': 'QQGzh8actEkVswURTyIRLqske4Sxcsi-akMT1fROk56vuINWbFjp-FoGkNhWeMqbH7uAql7GAbZP6d3v4rMFI-RPhop0-HnbsWAMeW_zE_v2jbjqaH273g==',
        }
 
        self.birth_params = {
            'PageNumber': '',
            'recordsPerPage': '50'
        }

        self.birth_output_columns = [
            "id", "type", "County", "Roll Number", "Page", "Number", 
            "Date of Return (Month/Day/Year)", "Name of Child", "Sex", 
            "No. of Child of this Mother", "Race or Color", "Date of Birth", 
            "Place of Birth", "Nationality of Father", "Father's Place of Birth", 
            "Father's Age", "Nationality of Mother", "Mother's Place of Birth", 
            "Mother's Age", "Full Name of Mother", "Maiden Name of Mother", 
            "Residence of Mother", "Full Name of Father", "Father's Occupation", 
            "Name and Address of Medical Attendant", 
            "Name and Address of Person making Certificate", "Returned by", 
            "NOTE", "source_county"
        ]
        
    # pre-compiled regexes (faster than parsing when possible)
    _NEXT_RE = re.compile(r'page-link[^"]*>\s*Next\s*<', re.IGNORECASE)

    def _fetch_page(self, page_number, url, data, params, retry_count=0, max_retries=20):
        local_params = params.copy() if params is not None else {}
        local_params['PageNumber'] = str(page_number)
        page_ids = []
        try:
            self.session.post(
                    self.birth_url,
                    cookies=self.cookies,
                    headers=self.headers,
                    data=data,
                    timeout=60
                )
            response = self.session.get(
                url + 'Results',
                params=local_params,
                cookies=self.cookies,
                headers=self.headers,
                timeout=60
            )
            if response.status_code == 200:
                text = response.text

                soup = BeautifulSoup(text, 'html.parser')
                has_next =True if soup.find("a", class_="page-link", string="Next") else False 

                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'id=' in href and 'Detail' in href:
                        record_id = href.split('id=')[1].split('&')[0]
                        page_ids.append(record_id)


                
                # print('page ids',page_ids)

                # total_re = re.search(r"class=['\"]TotalDisplayNum['\"][^>]*>(\d+)<", text, re.IGNORECASE)
                # if total_re:
                #     total_records = int(total_re.group(1))
                # else:
                #     try:
                #         if 'soup' not in locals():
                #             soup = BeautifulSoup(text, 'html.parser')
                #         total_span = soup.find('span', {'class': 'TotalDisplayNum'})
                #         if total_span:
                #             total_records = int(total_span.text.strip())
                #             print('total records',total_records)
                #     except Exception:
                #         pass

                # print(f"Records:{page_ids}")

                print(f"Page {page_number}",end='', flush=True)
                return page_ids, has_next, True

            elif response.status_code == 429 and retry_count < max_retries + 20:
                wait_time = 30 + (2 ** min(retry_count, 6)) + random.uniform(0, 10)
                time.sleep(wait_time)
                return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)

            elif retry_count < max_retries:
                wait_time = (2 ** min(retry_count, 6)) + random.uniform(0, 1)
                time.sleep(wait_time)
                return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)
            
            return [], False, False

        except Exception as e:
            if retry_count < max_retries:
                time.sleep((2 ** min(retry_count, 6)) + 5)
                return self._fetch_page(page_number, url, data, params, retry_count + 1, max_retries)
            return [], False, False

    def get_all_ids(self, url, data, params):
        """Fetch IDs sequentially until no 'Next' link is found"""
        all_ids = []
        seen_ids = set()
        current_page = 1
        
        while True:
            page_ids, has_next, success = self._fetch_page(
                current_page, 
                url, 
                data.copy() if data else {}, 
                params.copy() if params else {}
            )
            all_ids.extend(page_ids)
            current_page+=1
            
            if not has_next:
                return all_ids
                

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

    def get_birth_data_by_id(self, record_id, record_type, retry_count=0, max_retries=20):

        params = {}
        params['id'] = record_id
        params['type'] = record_type

        try:
            response = self.session.get(
                self.birth_url + 'Detail',
                params=params,
                cookies=self.cookies,
                headers=self.headers,
                timeout=60
            )

            if response.status_code == 429:
                wait_time = 30 + (2 ** min(retry_count, 6)) + random.uniform(0, 10)
                print(f"{record_id} - 429 rate limited. Retrying in {wait_time:.1f}s (attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.get_birth_data_by_id(record_id, record_type, retry_count + 1, max_retries)

            elif response.status_code != 200:
                if retry_count < max_retries:
                    wait_time = (2 ** min(retry_count, 6)) + random.uniform(0, 5)
                    print(f"{record_id} - HTTP {response.status_code}. Retrying in {wait_time:.1f}s (attempt {retry_count + 1}/{max_retries})")
                    time.sleep(wait_time)
                    return self.get_birth_data_by_id(record_id, record_type, retry_count + 1, max_retries)
                else:
                    print(f"{record_id} - HTTP {response.status_code}. Max retries reached, skipping.")
                    return {'id': record_id, 'type': record_type}

        except Exception as e:
            if retry_count < max_retries:
                wait_time = (2 ** min(retry_count, 6)) + random.uniform(0, 5)
                print(f"{record_id} - Exception: {e}. Retrying in {wait_time:.1f}s (attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.get_birth_data_by_id(record_id, record_type, retry_count + 1, max_retries)
            else:
                print(f"{record_id} - Exception: {e}. Max retries reached, skipping.")
                return {'id': record_id, 'type': record_type}

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
            
            # Context state to disambiguate 'Age', 'Place of Birth' across columns (e.g. Father vs Mother)
            current_context = "" 

            for h, v in zip(heads, vals):
                header_text = h.get_text(" ", strip=True)
                
                # Detect context from header text
                if "Nationality of Father" in header_text or "Full Name of Father" in header_text:
                    current_context = "Father's "
                elif "Nationality of Mother" in header_text or "Full Name of Mother" in header_text:
                    current_context = "Mother's "
                elif "Name of Child" in header_text:
                    current_context = "" # Reset for child

                # CASE 1: li-based data
                if h.find("li") and v.find("li"):
                    keys = [li.text.strip() for li in h.find_all("li")]
                    vals2 = [li.text.strip() for li in v.find_all("li")]
                    
                    for k, val in zip(keys, vals2):
                        final_key = k
                        # Disambiguate common overlapping fields
                        if k in ["Place of Birth", "Age", "Occupation"] and current_context:
                            final_key = f"{current_context}{k}"
                        
                        # Fallback: if key still exists, append context or index
                        if final_key in data:
                            if current_context and current_context not in final_key:
                                final_key = f"{current_context}{final_key}"
                            else:
                                # Last resort unique
                                if final_key in data:
                                     final_key = f"{final_key}_2"

                        data[final_key] = val

                # CASE 2: normal th → td
                else:
                    key = header_text
                    final_key = key
                    # Handle standalone 'Age' columns in Table 3
                    if key == "Age" and current_context:
                        final_key = f"{current_context}Age"
                    
                    if final_key in data:
                         final_key = f"{final_key}_2"

                    data[final_key] = v.get_text(" ", strip=True)

        return data

    def _append_rows_csv(self, rows, filename, columns=None):
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
            
            if columns:
                # Enforce specific schema order and presence
                df = df.reindex(columns=columns).fillna('')
            
            # If file exists, we append.
            # Note: If new columns appear in later chunks that weren't in the first chunk, 
            # they won't have a header in the CSV if the file already existed. 
            # But this prevents crashing on "extra keys".
            df.to_csv(filename, sep='|', mode='a', index=False, header=not file_exists)

        else:
            # fallback to pandas for unknown types
            df = pd.DataFrame(rows)
            if columns:
                df = df.reindex(columns=columns).fillna('')
            df.to_csv(filename, sep='|', mode='a', index=False, header=not file_exists)

    def export_data(self, data, filename='Birth_ids_data.csv', columns=None):
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
                self._append_rows_csv(data[i:i+chunk_size], filename, columns=columns)
        else:
            self._append_rows_csv(data, filename, columns=columns)

        # print(f"Appended {len(data)} rows to {filename}")


    def process_county_birth(self, county,birth_death_data, max_workers_data=10, max_retries=5):
        """Process a single county: Fetch IDs, then fetch details, saving incrementally."""
        local_params = self.birth_params.copy()

        if birth_death_data=='Birth':
            local_data = self.birth_data.copy()
            local_data['BirthCounty'] = county
        else:
            local_data = self.death_data.copy()
            local_data['DeathCounty'] = county
            local_params['recordsPerPage'] = '75'
        ids = []
        for attempt in range(max_retries + 1):
            try:
                ids = self.get_all_ids(self.birth_url, local_data, local_params)
                print(f'{county} total IDs -',len(ids))
                if ids:
                    break
            except Exception as e:
                import sys
                print(f"Error fetching IDs for {county} on line {sys.exc_info()[-1].tb_lineno}: {e}")
            if attempt < max_retries:
                wait_time = (2 ** attempt) * 1
                time.sleep(wait_time)
        
        if not ids:
            print(f"Skipping {county} (No IDs found after retries)")
            return

        # Save IDs immediately
        # self.export_data([(rid, county) for rid in ids], filename=f"Birth_{county}_ids.csv")
        self.export_data([(rid, county) for rid in ids], filename=f"{birth_death_data}_ids.csv") 
        
        # Fetch Details
        records_file = f"{birth_death_data}_records.csv"
        buffer = []
        
        with ThreadPoolExecutor(max_workers=max_workers_data) as executor:
            future_to_id = {executor.submit(self.get_birth_data_by_id, record_id, birth_death_data): record_id for record_id in ids}
            
        with tqdm(total=len(ids), desc=f"Birth - {county}", unit="id") as pbar:
            for future in as_completed(future_to_id):
                try:
                    rec = future.result()
                    if isinstance(rec, dict):
                        rec['source_county'] = county
                    buffer.append(rec)
                    
                    if len(buffer) >= 200:
                        cols = self.birth_output_columns if birth_death_data == 'Birth' else None
                        self.export_data(buffer, filename=records_file, columns=cols)
                        buffer = []
                except Exception as e:
                    pass
                finally:
                    pbar.update(1)
        
        if buffer:
            cols = self.birth_output_columns if birth_death_data == 'Birth' else None
            self.export_data(buffer, filename=records_file, columns=cols)
        
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
        self.export_data(data, filename=f"Birth_{county}_records.csv", columns=self.birth_output_columns)
        self.export_data([(rid, county) for rid in ids], filename=f"Birth_{county}_ids.csv")
        self.export_data(data, columns=self.birth_output_columns)

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

    def run_all_counties_birth(self,birth_death_data, max_workers_counties=10, max_workers_data=25, max_retries=5):
        if birth_death_data=='Birth':
            counties = self.all_birth_counties
        else:
            counties = self.all_death_counties
        """Fetch Birth data from all counties in parallel, processing each county fully."""
        print(f"\n{'='*60}")
        print(f"Starting parallel scrape for {birth_death_data} -  {len(counties)} counties")
        print(f"{'='*60}\n")
        

        for county in counties:
            try:
                self.process_county_birth(county,birth_death_data, max_workers_data, max_retries)
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
        print(f"Starting to scrape Naturalization records from all {len(self.all_birth_counties)} counties")
        print(f"{'='*60}\n")

        all_ids_by_county = {}
        counties_to_process = self.all_birth_counties[:]
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
    # Limit to 3 counties for testing

    # sos.all_birth_counties = sos.all_birth_counties[0:3]
    # sos.all_birth_counties = ['Douglas','Clay']
    # sos._fetch_page('Birth',data=sos.birth_death_data)
    
    #Only total rec in county
    # sos.birth_data['recordsPerPage'] = '75'
    # for i in sos.all_birth_counties:
    #     sos.death_data['DeathCounty']=i
    #     print('county - ',i,'Records: ',sos._fetch_page(page_number=1, url=sos.birth_url, data=sos.death_data, params=sos.birth_params, retry_count=0, max_retries=20))


    #main All con
    sos.run_all_counties_birth(birth_death_data='Birth',max_workers_counties=12,
                               max_workers_data=30,
                               max_retries=7)
    # sos.run_all_counties_birth('Death',max_workers_counties=12,
    #                            max_workers_data=30,
    #                            max_retries=7)


    # Option 2: Scrape Birth records from a single county
    # sos.run_birth(county='Douglas', max_workers=15)

    
    # print(sos.get_birth_data_by_id('17141','Birth'))
    
    # for i in ['Douglas','Clay']:
    #     sos.process_county_birth(
    #     birth_death_data='Birth',
    #     county=i,
    #     max_workers_data=30,
    #     max_retries=7
    #     )

    # sos.process_county_birth(
    # birth_death_data='Birth',
    # county='Clay',
    # max_workers_data=5,
    # max_retries=7
    # )

    # Option 3: Scrape Land records
    # sos.run_land(max_workers=10)
    # sos.run_all_counties_land(max_workers_counties=4)

    # print(sos.get_land_and_naturalization_data_by_id('land', '7672'))

    # print(sos.get_land_and_naturalization_data_by_id('naturalization', '8115'))
