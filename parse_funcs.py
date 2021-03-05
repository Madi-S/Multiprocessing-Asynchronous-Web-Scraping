import csv
import logging
import requests
from bs4 import BeautifulSoup


# Logger configuration
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

scraped_count = 1



# Function to collect (crawl) all pertinent URLs on the page
def get_urls(html):
    soup = BeautifulSoup(html, 'html.parser')

    return ['https://www.healthgrades.com' + li.a['href'] for li in soup.find_all('li', class_='link-column__list')]


# Fucntion to parse given HTML/URL
def parse(url, html=None):
    global scraped_count

    logger.debug('Processing: %s\t\tCount: %s', url, scraped_count)

    try:
        if not html:
            html = requests.get(url).text

        soup = BeautifulSoup(html, 'lxml')

        try:
            biography = soup.find(
                attrs={'data-qa-target': 'premium-biography'}).get_text(strip=True)
            name = biography.split(',')[0]
        except AttributeError:
            logger.warning('Biograhy and name were not found %s', url)
            biography = 'Not given'
            name = 'Not given'

        try:
            gender = soup.find(
                attrs={'data-qa-target': 'ProviderDisplayGender'}).get_text(strip=True)
        except AttributeError:
            gender = 'Not given'

        try:
            age = soup.find(
                attrs={'data-qa-target': 'ProviderDisplayAge'}).get_text(strip=True).replace('•\xa0Age', '')
        except AttributeError:
            age = 'Not given'

        try:
            phone = soup.find_all(
                attrs={'data-qa-target': 'pdc-summary-new-patients-button'})[-1]['href'][4:]
        except:
            try:
                phone = soup.find(
                    attrs={'data-hgoname': 'summary-new-phone-number'})['href'][4:]
            except:
                phone = 'Not given'

        try:
            rating = float(soup.find(class_='score').find(
                'strong').get_text(strip=True))
        except:
            rating = 'Not given'

        try:
            try:
                ratingCount = int(
                    soup.find(class_='review-pill').find('small').get_text(strip=True))
            except AttributeError:
                ratingCount = int(soup.find(
                    class_='review-count').get_text(strip=True).replace('(', '').replace(')', ''))
        except ValueError::
            ratingCount = 0

        try:
            awards = soup.find(attrs={'data-qa-target': 'about-me-awards'}
                               ).get_text(strip=True).replace('Awards', '', 1)
        except AttributeError:
            awards = 'Not given'

        try:
            raw_locations = soup.find_all('div', class_='office-title')
            locations = ';'.join([location.get_text(
                strip=True) for location in raw_locations]) if len(raw_locations) else 'Not given'
        except AttributeError:
            locations = 'Not given'

        data = {
            'Name': name,
            'Gender': gender,
            'Age': age,
            'Phone': phone,
            'Rating': rating,
            'Rating count': ratingCount,
            'URL': url,
            'Locations': locations,
            'Awards': awards,
            'Biography': biography,
        }
        
        # Write scraped data to the .csv file
        with open('data.csv', 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.DictWriter(f, fieldnames=['Name', 'Gender', 'Age', 'Phone', 'Rating',
                                                       'Rating count', 'URL', 'Locations', 'Awards', 'Biography'], delimiter='\t')
            csv_writer.writerow(data)
        
        # Increment scraped_count
        scraped_count += 1

    except Exception as e:
        print(e)
