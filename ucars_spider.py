from scrapy import Spider, signals, Selector
from scrapy.http import HtmlResponse
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import os
import csv
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

class UCars(Spider):
    name = 'ucars'
    base_url = 'https://www.copart.com/{}'
    login_url = 'https://ucars.pro/live-auctions/copart-us?auction=copart-us&status=1&page=1'
    custom_settings = {
        'FEEDS': {
            'ucars.csv': {
                'format': 'csv',
                'encoding': 'utf-8-sig'
            }
        }
    }

    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    options.add_argument('--disable-popup-blocking')
    options.add_argument("--no-first-run")
    options.add_argument("--disable-features=EnableImprovedCookieControls")
    options.add_argument("--disable-features=SameSiteByDefaultCookies,LaxSameSiteCookies")
    options.add_argument('--disable-features=PrivacySandboxSettings4')
    extension_path = os.path.abspath("adb.crx")
    options.add_extension(extension_path)
    driver = uc.Chrome(options=options)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UCars, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        
        self.driver.get(self.login_url)
        WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'main')))
        page_html = self.driver.page_source
        response = HtmlResponse(url=self.driver.current_url, body=page_html, encoding='utf-8')
        self.parse(response)

    def scrape_lot_details(self, driver, lot_url):
        driver.get(lot_url)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'main')))
        lot_details = self.scrape_lot_details_from_page(driver, lot_url)
        return lot_details

    def scrape_lot_details_from_page(self, driver, lot_url):
        self.driver.get(lot_url)
        wait = WebDriverWait(self.driver, 30)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'main')))
        vehicle_specs_table = self.driver.find_element(By.CSS_SELECTOR, 'div.card.mb-lg-60 table.card__table')
        
        name_element = self.driver.find_element(By.CSS_SELECTOR, 'ol.breadcrumbs li:nth-child(4) span[itemprop="name"]')
        name = name_element.text.strip() if name_element else None

        lot_number_element = self.driver.find_element(By.CSS_SELECTOR, 'div.lot__sidebar div.card div.row div.col-auto:nth-child(2) div.pill')
        lot_number = lot_number_element.text.strip() if lot_number_element else None

        auction_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Auction"]]/td[position()=2]')
        auction = auction_element.text.strip() if auction_element else None

        country_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Country"]]/td[position()=2]')
        country = country_element.text.strip() if country_element else None

        seller_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Seller"]]/td[position()=2]')
        seller = seller_element.text.strip() if seller_element else None

        salebranch_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Sale branch"]]/td[position()=2]')
        salebranch = salebranch_element.text.strip() if salebranch_element else None

        auctiondate_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Auction date"]]/td[position()=2]')
        auctiondate = auctiondate_element.text.strip() if auctiondate_element else None

        startat_element = self.driver.find_element(By.XPATH, '//tr[td[text()="Starts at"]]/td[position()=2]')
        startat = startat_element.text.strip() if startat_element else None

        vehicle_type_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Vehicle type"]]/td[position()=2]')
        vehicle_type = vehicle_type_element.text.strip() if vehicle_type_element else None

        year_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Year"]]/td[position()=2]')
        year = year_element.text.strip() if year_element else None

        make_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Make"]]/td[position()=2]')
        make = make_element.text.strip() if make_element else None

        model_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Model"]]/td[position()=2]')
        model = model_element.text.strip() if model_element else None
        
        color_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Color"]]/td[position()=2]')
        color = color_element.text.strip() if color_element else None

        bodytype_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Body type"]]/td[position()=2]')
        bodytype = bodytype_element.text.strip() if bodytype_element else None

        drive_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Drive"]]/td[position()=2]')
        drive = drive_element.text.strip() if drive_element else None

        feul_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Fuel"]]/td[position()=2]')
        feul = feul_element.text.strip() if feul_element else None

        engine_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Engine"]]/td[position()=2]')
        engine = engine_element.text.strip() if engine_element else None

        tranmission_element = vehicle_specs_table.find_element(By.XPATH, '//tr[td[text()="Transmission"]]/td[position()=2]')
        transmission = tranmission_element.text.strip() if tranmission_element else None

        return {
            'url': lot_url, 'vin': name, 'lot_number': lot_number, 'auction': auction, 'country': country,
            'seller': seller, 'sale_branch': salebranch, 'auction_date': auctiondate, 'start_at': startat, 
            'vehicle_type': vehicle_type, 'year': year, 'make': make, 'model': model, 'color': color, 
            'bodytype': bodytype, 'drive': drive, 'feul': feul, 'engine': engine, 'transmission': transmission
        }

    def parse(self, response):
        existing_vins = self.read_existing_vins('ucars.csv')
        unique_lots_vins = set(existing_vins)
        
        while True:
            lot_urls = self.get_page_data()
            for lot_url in lot_urls:
                vin_from_url = lot_url.split('-')[-1]
                vin_from_url = vin_from_url.upper()
                if vin_from_url in unique_lots_vins:
                    continue

                self.driver.execute_script("window.open('');")
                self.driver.switch_to.window(self.driver.window_handles[1])
                lot_details = self.scrape_lot_details(self.driver, lot_url)
                lot_number = lot_details['lot_number']
                vin = lot_details['vin']
                if vin not in unique_lots_vins:
                    unique_lots_vins.add(vin)
                    self.save_to_csv(lot_details)
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                time.sleep(2)

            if not self.go_to_next_page():
                break

            time.sleep(5)

    def process_lot_details(self, lot_url, unique_lots_vins):
        try:
            lot_details = self.scrape_lot_details(self.driver, lot_url)  # Use the same driver instance
            lot_number = lot_details['lot_number']
            vin = lot_details['vin']
            if vin not in unique_lots_vins:
                unique_lots_vins.add(vin)
                self.save_to_csv(lot_details)
        except Exception as e:
            print(f"Error processing lot details for URL {lot_url}: {e}")

    def save_to_csv(self, lot_details, filename='ucars.csv'):
        file_exists = os.path.isfile(filename)
        self.initialize_csv(filename, lot_details.keys())
        with open(filename, 'a', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=lot_details.keys())
            if not file_exists:
                dict_writer.writeheader()
            dict_writer.writerow(lot_details)

    def initialize_csv(self, filename, fieldnames):
        if not os.path.isfile(filename):
            with open(filename, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                dict_writer.writeheader()

    def read_existing_vins(self, filename):
        existing_vins = set()
        if os.path.isfile(filename):
            with open(filename, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if len(row) > 1:
                        existing_vins.add(row[1].upper())
                    else:
                        print(f"Skipping incomplete row: {row}")
        return existing_vins

    def get_page_data(self):
        html_content = self.driver.page_source
        selector_html = Selector(text=html_content)
        lot_urls = selector_html.css('a.vehicle-card__thumb::attr(href)').getall()
        return lot_urls

    def go_to_next_page(self):
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, 'a.page-link[aria-label="Next"]')
            next_button.click()
            return True
        except:
            return False

    def spider_closed(self, spider):
        if self.driver:
            self.driver.close()
