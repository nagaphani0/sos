from bs4 import BeautifulSoup
import requests
import pandas as pd
# import mysql.connector
# from mysql.connector import Error
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

class SOS:
    def __init__(self) :       
        self.cookies = {
            'BNES_ASP.NET_SessionId': '25Rp4LoPJ3UiqGHJf9d///t7o9wnw+Ab8K3T+wSqLzKZNjpYUQdE1CXgOwT4ms7nc0+EdWuEB6rzlzmHj0Z2FRp4nlPDhmXp',
            'cf_clearance': 'WGWXK5vOo9wU_PJ0R7s7IQEj9loUrMC6KAv4XNBQF0k-1770103737-1.2.1.1-kcSkvO5F8AB.KREErb4OBMP6EdqrJniwVIbQCYz2zDo7dXSyaT.md0ofPv7cddLzFmXITVOmNbstw4vY_YGlGNnJ829P4FfVY.Y3a6Ky.zI7N_Fya2Jn7TdxaC7EreFkYUBhnigvsc964fsZqZLzLSR8hu6ICk58FBxKKu9g3TvQQ_5V_Nc0fnzgE4JBksZi22sq8Z7clxqMn3gp_pZnGGaBsar6_MLHfpeHzpkvvJk',
        }

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
        self.birth_url='https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath/'
        
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

        self.birth_data = {
            'BirthCounty': '',
            'BirthNameSearch': '',
            'BirthSubmit': 'Search',
        }
        self.birth_params = {
            'PageNumber': '',
            'recordsPerPage': '50'
        }
    def _fetch_page(self,page_number,url, counties, retry_count=0, max_retries=5):
        """Fetch a single page and extract IDs with exponential backoff"""
        self.birth_data['BirthCounty'] = counties
        self.birth_params['PageNumber'] = str(page_number)

        url

        if birth:
            url = self.birth_url
            params = self.birth_params
            data = self.birth_data

        try:
            response = requests.post(
                url,
                params=params,
                cookies=self.cookies,
                headers=self.headers,
                data=data,
                timeout=30
            )
            
            page_ids = []
            has_next_page = False
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract record IDs from detail links
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'id=' in href and 'Detail' in href:
                        record_id = href.split('id=')[1].split('&')[0]
                        page_ids.append(record_id)
                
                # Check if "Next" button exists (indicates more pages)
                next_button = soup.find('a', {'class': 'page-link'}, string='Next')
                if next_button and next_button.get('href'):
                    has_next_page = True
                
                # print(f"Page {page_number}: Found {len(page_ids)} IDs | Has Next: {has_next_page}")
                print(f"Page {page_number}")
                return page_ids, has_next_page
            
            # If status code is not 200, retry with backoff
            elif retry_count < max_retries:
                wait_time = (2 ** retry_count) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Page {page_number}: Status {response.status_code}. Retrying in {wait_time:.2f}s (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self._fetch_page(page_number, counties, retry_count + 1, max_retries)
            else:
                print(f"Page {page_number}: Failed after {max_retries} retries. Status: {response.status_code}")
                return [], False
                
        except Exception as e:
            if retry_count < max_retries:
                wait_time = (2 ** retry_count) + random.uniform(0, 1)
                print(f"Page {page_number}: Error - {e}. Retrying in {wait_time:.2f}s (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self._fetch_page(page_number, counties, retry_count + 1, max_retries)
            else:
                print(f"Page {page_number}: Error after {max_retries} retries: {e}")
                return [], False

    def get_all_ids(self, counties, max_pages=50):
        """Fetch IDs from all pages using multithreading"""
        ids = []
        
        # First, fetch page 1 to check if there are more pages
        print(f"Scraping IDs for county: {counties}")
        first_page_ids, has_next = self._fetch_page(1, counties)
        ids.extend(first_page_ids)
        
        if not has_next or len(first_page_ids) == 0:  # No more pages or no results
            print(f"Total IDs found: {len(ids)}")
            return ids
        
        # Fetch remaining pages in parallel
        page_numbers = list(range(2, max_pages + 1))
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._fetch_page, page_num, counties): page_num 
                      for page_num in page_numbers}
            
            for future in as_completed(futures):
                try:
                    page_ids, has_next_page = future.result()
                    ids.extend(page_ids)
                    
                    # Small delay between adding results to avoid overwhelming server
                    time.sleep(0.1)
                    
                    # Stop if we get a page with no next button (last page)
                    if not has_next_page:
                        # Cancel remaining futures
                        for f in futures:
                            f.cancel()
                        break
                except Exception as e:
                    print(f"Error processing future: {e}")
        
        print(f"Total IDs found: {len(ids)}")
        return ids

    def get_data_by_id(self,record_id, record_type):
        
        params={}
        params['id'] = record_id
        params['type'] = record_type

        response = requests.get(
            self.base_url + 'Detail',
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
        
    def export_data(self, data):
        # Save to CSV
        df = pd.DataFrame(data)
        df.to_csv('sos_data.csv', index=False)
        print("Data exported to sos_data.csv")
        
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

    def run(self, county='Douglas', max_workers=10):
        ids = self.get_all_ids(county)
        data = []
        
        # Use ThreadPoolExecutor for concurrent fetching
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_id = {executor.submit(self.get_data_by_id, record_id, 'Birth'): record_id for record_id in ids}
            
            # Process completed tasks as they finish
            completed_count = 0
            for future in as_completed(future_to_id):
                record_id = future_to_id[future]
                try:
                    record_data = future.result()
                    data.append(record_data)
                    completed_count += 1
                    print(f"scraped ID: {record_id}, total scraped: {completed_count}/{len(ids)}")
                except Exception as e:
                    print(f"Error scraping ID {record_id}: {e}")
        
        self.export_data(data)

    def run_all_counties(self, max_workers_counties=4, max_workers_data=10, max_retries=5):
        """Fetch data from all counties in parallel with smart retries"""
        all_data = []
        
        print(f"\n{'='*60}")
        print(f"Starting to scrape all {len(self.all_counties)} counties")
        print(f"{'='*60}\n")
        
        # Step 1: Fetch IDs from all counties in parallel
        all_ids_by_county = {}
        counties_to_process = self.all_counties[:]
        retry_count = 0
        
        while counties_to_process and retry_count < max_retries:
            if retry_count > 0:
                # Exponential backoff between retry attempts
                wait_time = (2 ** (retry_count - 1)) * 5  # 5s, 10s, 20s, 40s...
                print(f"\n{'='*60}")
                print(f"Retry attempt {retry_count} for {len(counties_to_process)} counties with 0 IDs")
                print(f"Waiting {wait_time} seconds before retry...")
                print(f"{'='*60}\n")
                time.sleep(wait_time)
            
            with ThreadPoolExecutor(max_workers=max_workers_counties) as executor:
                futures = {executor.submit(self.get_all_ids, county): county 
                          for county in counties_to_process}
                
                counties_with_ids = []
                for future in as_completed(futures):
                    county = futures[future]
                    try:
                        ids = future.result()
                        all_ids_by_county[county] = ids
                        print(f"✓ County '{county}': {len(ids)} IDs fetched")
                        if len(ids) > 0:
                            counties_with_ids.append(county)
                    except Exception as e:
                        print(f"✗ Error fetching IDs for county {county}: {e}")
                        all_ids_by_county[county] = []
            
            # Update the list of counties to retry (those with 0 IDs only)
            counties_to_process = [county for county in counties_to_process if len(all_ids_by_county.get(county, [])) == 0]
            retry_count += 1
        
        # Report on retry results
        failed_counties = [county for county in self.all_counties if len(all_ids_by_county.get(county, [])) == 0]
        if failed_counties:
            print(f"\n{'='*60}")
            print(f"WARNING: {len(failed_counties)} counties still have 0 IDs after {max_retries} retries:")
            for county in failed_counties:
                print(f"  - {county}")
            print(f"{'='*60}\n")
        else:
            print(f"\n{'='*60}")
            print(f"SUCCESS: All counties have been successfully fetched!")
            print(f"{'='*60}\n")
        
        # Calculate total IDs
        total_ids = sum(len(ids) for ids in all_ids_by_county.values())
        print(f"\n{'='*60}")
        print(f"Total IDs across all counties: {total_ids}")
        print(f"Successfully fetched: {len(self.all_counties) - len(failed_counties)}/{len(self.all_counties)} counties")
        print(f"{'='*60}\n")
        
        # Step 2: Fetch data for all IDs in parallel
        all_record_ids = [(record_id, county) for county, ids in all_ids_by_county.items() for record_id in ids]
        
        # with ThreadPoolExecutor(max_workers=max_workers_data) as executor:
        #     future_to_id = {executor.submit(self.get_data_by_id, record_id, 'Birth'): (record_id, county) 
        #                    for record_id, county in all_record_ids}
            
        #     completed_count = 0
        #     for future in as_completed(future_to_id):
        #         record_id, county = future_to_id[future]
        #         try:
        #             record_data = future.result()
        #             all_data.append(record_data)
        #             completed_count += 1
        #             if completed_count % 10 == 0:
        #                 print(f"Progress: {completed_count}/{total_ids} records scraped")
        #         except Exception as e:
        #             print(f"Error scraping ID {record_id} from {county}: {e}")
        
        # print(f"\n{'='*60}")
        # print(f"Total records scraped: {len(all_data)}")
        # print(f"{'='*60}\n")
        
        self.export_data(all_record_ids)

    def search(self):
        data = {
        'BirthCounty': 'Andrew',
        'BirthNameSearch': '',
        'BirthSubmit': 'Search',
        # '__ncforminfo': 'Uvw09ryZMaVTMi9jv7_IuIAHDRy6jrMyFJtZaCjCDRtfmzfAtf8ySXIoNSxLvr5fhWWWfvpHvFakduraTSE-mBaJFIz0o1HE0uIr6IkDv0A=',
        }

        response = requests.post(
            self.base_url,
            cookies=self.cookies,
            headers=self.headers,
            data=data,
        )
        print(response.text)


if __name__ == "__main__":
    sos = SOS()
    # Fetch data from all counties
    # max_workers_counties: threads for fetching county pages (reduce for slower connection)
    # max_workers_data: threads for fetching individual records (reduce for slower connection)
    # max_retries: number of times to retry counties with 0 IDs (increase for unreliable connection)

    # Fetch with increased retries and reasonable parallel workers
    sos.run_all_counties(max_workers_counties=2,
                         max_workers_data=6,
                         max_retries=7)

    # Or run single county (original way)
    # sos.run(county='Douglas', max_workers=15)