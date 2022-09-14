from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import requests
import json
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import html
import time
import random

class AddTransportationWeightsToEdges:

    def __init__(self, fromNode, toNode, driver, typeOfTransportation):
        self.driver = driver
        self.fromNode = fromNode
        self.toNode = toNode
        self.typeOfTransportation = typeOfTransportation
        self.geolocator = Nominatim(user_agent="trialApp")
        self.fromCoordinates = set()
        self.toCoordinates = set()
        self.fromName = ''
        self.toName = ''
        self.countries = [
            ('US', 'United States'),
            ('AF', 'Afghanistan'),
            ('AL', 'Albania'),
            ('DZ', 'Algeria'),
            ('AS', 'American Samoa'),
            ('AD', 'Andorra'),
            ('AO', 'Angola'),
            ('AI', 'Anguilla'),
            ('AQ', 'Antarctica'),
            ('AG', 'Antigua And Barbuda'),
            ('AR', 'Argentina'),
            ('AM', 'Armenia'),
            ('AW', 'Aruba'),
            ('AU', 'Australia'),
            ('AT', 'Austria'),
            ('AZ', 'Azerbaijan'),
            ('BS', 'Bahamas'),
            ('BH', 'Bahrain'),
            ('BD', 'Bangladesh'),
            ('BB', 'Barbados'),
            ('BY', 'Belarus'),
            ('BE', 'Belgium'),
            ('BZ', 'Belize'),
            ('BJ', 'Benin'),
            ('BM', 'Bermuda'),
            ('BT', 'Bhutan'),
            ('BO', 'Bolivia'),
            ('BA', 'Bosnia And Herzegowina'),
            ('BW', 'Botswana'),
            ('BV', 'Bouvet Island'),
            ('BR', 'Brazil'),
            ('BN', 'Brunei Darussalam'),
            ('BG', 'Bulgaria'),
            ('BF', 'Burkina Faso'),
            ('BI', 'Burundi'),
            ('KH', 'Cambodia'),
            ('CM', 'Cameroon'),
            ('CA', 'Canada'),
            ('CV', 'Cape Verde'),
            ('KY', 'Cayman Islands'),
            ('CF', 'Central African Rep'),
            ('TD', 'Chad'),
            ('CL', 'Chile'),
            ('CN', 'China'),
            ('CX', 'Christmas Island'),
            ('CC', 'Cocos Islands'),
            ('CO', 'Colombia'),
            ('KM', 'Comoros'),
            ('CG', 'Congo'),
            ('CK', 'Cook Islands'),
            ('CR', 'Costa Rica'),
            ('CI', 'Cote D`ivoire'),
            ('HR', 'Croatia'),
            ('CU', 'Cuba'),
            ('CY', 'Cyprus'),
            # ('CZ', 'Czech Republic'), Problem with Czechia
            ('DK', 'Denmark'),
            ('DJ', 'Djibouti'),
            ('DM', 'Dominica'),
            ('DO', 'Dominican Republic'),
            ('TP', 'East Timor'),
            ('EC', 'Ecuador'),
            ('EG', 'Egypt'),
            ('SV', 'El Salvador'),
            ('GQ', 'Equatorial Guinea'),
            ('ER', 'Eritrea'),
            ('EE', 'Estonia'),
            ('ET', 'Ethiopia'),
            ('FK', 'Falkland Islands (Malvinas)'),
            ('FO', 'Faroe Islands'),
            ('FJ', 'Fiji'),
            ('FI', 'Finland'),
            ('FR', 'France'),
            ('GF', 'French Guiana'),
            ('PF', 'French Polynesia'),
            ('TF', 'French S. Territories'),
            ('GA', 'Gabon'),
            ('GM', 'Gambia'),
            ('GE', 'Georgia'),
            ('DE', 'Germany'),
            ('GH', 'Ghana'),
            ('GI', 'Gibraltar'),
            ('GR', 'Greece'),
            ('GL', 'Greenland'),
            ('GD', 'Grenada'),
            ('GP', 'Guadeloupe'),
            ('GU', 'Guam'),
            ('GT', 'Guatemala'),
            ('GN', 'Guinea'),
            ('GW', 'Guinea-bissau'),
            ('GY', 'Guyana'),
            ('HT', 'Haiti'),
            ('HN', 'Honduras'),
            ('HK', 'Hong Kong'),
            ('HU', 'Hungary'),
            ('IS', 'Iceland'),
            ('IN', 'India'),
            ('ID', 'Indonesia'),
            ('IR', 'Iran'),
            ('IQ', 'Iraq'),
            ('IE', 'Ireland'),
            ('IL', 'Israel'),
            ('IT', 'Italy'),
            ('JM', 'Jamaica'),
            ('JP', 'Japan'),
            ('JO', 'Jordan'),
            ('KZ', 'Kazakhstan'),
            ('KE', 'Kenya'),
            ('KI', 'Kiribati'),
            ('KW', 'Kuwait'),
            ('KG', 'Kyrgyzstan'),
            ('LA', 'Laos'),
            ('LV', 'Latvia'),
            ('LB', 'Lebanon'),
            ('LS', 'Lesotho'),
            ('LR', 'Liberia'),
            ('LY', 'Libya'),
            ('LI', 'Liechtenstein'),
            ('LT', 'Lithuania'),
            ('LU', 'Luxembourg'),
            ('MO', 'Macau'),
            ('MK', 'Macedonia'),
            ('MG', 'Madagascar'),
            ('MW', 'Malawi'),
            ('MY', 'Malaysia'),
            ('MV', 'Maldives'),
            ('ML', 'Mali'),
            ('MT', 'Malta'),
            ('MH', 'Marshall Islands'),
            ('MQ', 'Martinique'),
            ('MR', 'Mauritania'),
            ('MU', 'Mauritius'),
            ('YT', 'Mayotte'),
            ('MX', 'Mexico'),
            ('FM', 'Micronesia'),
            ('MD', 'Moldova'),
            ('MC', 'Monaco'),
            ('MN', 'Mongolia'),
            ('MS', 'Montserrat'),
            ('MA', 'Morocco'),
            ('MZ', 'Mozambique'),
            ('MM', 'Myanmar'),
            ('NA', 'Namibia'),
            ('NR', 'Nauru'),
            ('NP', 'Nepal'),
            ('NL', 'Netherlands'),
            ('AN', 'Netherlands Antilles'),
            ('NC', 'New Caledonia'),
            ('NZ', 'New Zealand'),
            ('NI', 'Nicaragua'),
            ('NE', 'Niger'),
            ('NG', 'Nigeria'),
            ('NU', 'Niue'),
            ('NF', 'Norfolk Island'),
            ('MP', 'Northern Mariana Islands'),
            ('NO', 'Norway'),
            ('OM', 'Oman'),
            ('PK', 'Pakistan'),
            ('PW', 'Palau'),
            ('PA', 'Panama'),
            ('PG', 'Papua New Guinea'),
            ('PY', 'Paraguay'),
            ('PE', 'Peru'),
            ('PH', 'Philippines'),
            ('PN', 'Pitcairn'),
            ('PL', 'Poland'),
            ('PT', 'Portugal'),
            ('PR', 'Puerto Rico'),
            ('QA', 'Qatar'),
            ('RE', 'Reunion'),
            ('RO', 'Romania'),
            ('RU', 'Russian Federation'),
            ('RW', 'Rwanda'),
            ('KN', 'Saint Kitts And Nevis'),
            ('LC', 'Saint Lucia'),
            ('VC', 'St Vincent/Grenadines'),
            ('WS', 'Samoa'),
            ('SM', 'San Marino'),
            ('ST', 'Sao Tome'),
            ('SA', 'Saudi Arabia'),
            ('SN', 'Senegal'),
            ('SC', 'Seychelles'),
            ('SL', 'Sierra Leone'),
            ('SG', 'Singapore'),
            ('SK', 'Slovakia'),
            ('SI', 'Slovenia'),
            ('SB', 'Solomon Islands'),
            ('SO', 'Somalia'),
            ('ZA', 'South Africa'),
            ('ES', 'Spain'),
            ('LK', 'Sri Lanka'),
            ('SH', 'St. Helena'),
            ('PM', 'St.Pierre'),
            ('SD', 'Sudan'),
            ('SR', 'Suriname'),
            ('SZ', 'Swaziland'),
            ('SE', 'Sweden'),
            ('CH', 'Switzerland'),
            ('SY', 'Syrian Arab Republic'),
            ('TW', 'Taiwan'),
            ('TJ', 'Tajikistan'),
            ('TZ', 'Tanzania'),
            ('TH', 'Thailand'),
            ('TG', 'Togo'),
            ('TK', 'Tokelau'),
            ('TO', 'Tonga'),
            ('TT', 'Trinidad And Tobago'),
            ('TN', 'Tunisia'),
            ('TR', 'Turkey'),
            ('TM', 'Turkmenistan'),
            ('TV', 'Tuvalu'),
            ('UG', 'Uganda'),
            ('UA', 'Ukraine'),
            ('AE', 'United Arab Emirates'),
            ('UK', 'United Kingdom'),
            ('UY', 'Uruguay'),
            ('UZ', 'Uzbekistan'),
            ('VU', 'Vanuatu'),
            ('VA', 'Vatican City State'),
            ('VE', 'Venezuela'),
            ('VN', 'Viet Nam'),
            ('VG', 'Virgin Islands (British)'),
            ('VI', 'Virgin Islands (U.S.)'),
            ('YE', 'Yemen'),
            ('YU', 'Yugoslavia'),
            ('ZR', 'Zaire'),
            ('ZM', 'Zambia'),
            ('ZW', 'Zimbabwe')
        ]
        self.distance = 0
        self.duration = 0
        self.chooseTransportationMode()
        time.sleep(1)
        countryFromValue = self.insertFrom()
        time.sleep(1)
        countryToValue = self.insertTo()
        time.sleep(1)
        self.getResults(countryFromValue, countryToValue)
    
    # Using web scraping to gather the distance and duration data
    # This method is to get the type of shipment and click on the corresponding button
    def chooseTransportationMode(self):
        buttonList =  WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[1]/ul'))
        )

        items = buttonList.find_elements(By.TAG_NAME, 'li')
        if(self.typeOfTransportation == 'Sea'):
            items[0].click()
        elif(self.typeOfTransportation == 'Land'):
            items[1].click()
        else:
            items[2].click()

    # This method is to get the from country and insert in the corresponding search
    def insertFrom(self):
        searchFrom = self.driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/input')
        searchFrom.clear()
        searchFrom.send_keys(Keys.CONTROL + "a")
        searchFrom.send_keys(Keys.DELETE)
        if ("city_name" in self.fromNode['Attributes']):
            if(self.fromNode['Attributes']['city_name'] == 'Unknown'):
                searchFrom.send_keys(self.fromNode['Attributes']['country'])
                self.fromName = self.fromNode['Attributes']['country']
            else:
                searchFrom.send_keys(self.fromNode['Attributes']['city_name'])
                self.fromName = self.fromNode['Attributes']['city_name']
            
        chosenFrom = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/div'))
        )
        try:
            chosenFrom.find_elements(By.TAG_NAME, 'div')[0].click()
        except:
            searchFrom.clear()
            searchFrom.send_keys(Keys.CONTROL + "a")
            searchFrom.send_keys(Keys.DELETE)
            if ("country" in self.fromNode['Attributes']):
                if(self.fromNode['Attributes']['country'] != 'Unknown'):
                    searchFrom.send_keys(self.fromNode['Attributes']['country'])
                    self.fromName = self.fromNode['Attributes']['country']
            chosenFrom = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/div'))
            )
            try:
                chosenFrom.find_elements(By.TAG_NAME, 'div')[0].click()
            except:
                searchFrom.clear()
                searchFrom.send_keys(Keys.CONTROL + "a")
                searchFrom.send_keys(Keys.DELETE)
                self.fromName = random.choice(self.countries)[1]
                searchFrom.send_keys(self.fromName)      
                chosenFrom = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/div'))
                )
                chosenFrom.find_elements(By.TAG_NAME, 'div')[0].click()
        finally:
            return searchFrom.get_attribute('value')

    # This method is to get the to country and insert in the corresponding search
    def insertTo(self):
        searchTo = self.driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/input')
        searchTo.clear()
        searchTo.send_keys(Keys.CONTROL + "a")
        searchTo.send_keys(Keys.DELETE)

        if ("city_name" in self.toNode['Attributes']):
            if(self.toNode['Attributes']['city_name'] == 'Unknown'):
                searchTo.send_keys(self.toNode['Attributes']['country'])
                self.toName = self.toNode['Attributes']['country']
            else:
                searchTo.send_keys(self.toNode['Attributes']['city_name'])
                self.toName = self.toNode['Attributes']['city_name']
        
        chosenTo = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/div'))
        )
        try:
            chosenTo.find_elements(By.TAG_NAME, 'div')[0].click()
        except:
            searchTo.clear()
            searchTo.send_keys(Keys.CONTROL + "a")
            searchTo.send_keys(Keys.DELETE)
            if ("country" in self.toNode['Attributes']):
                if(self.toNode['Attributes']['country'] != 'Unknown'):
                    searchTo.send_keys(self.toNode['Attributes']['country'])
                    self.toName = self.toNode['Attributes']['country']
            chosenTo = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/div'))
            )
            try:
                chosenTo.find_elements(By.TAG_NAME, 'div')[0].click()
            except:
                searchTo.clear()
                searchTo.send_keys(Keys.CONTROL + "a")
                searchTo.send_keys(Keys.DELETE)
                self.toName = random.choice(self.countries)[1]
                searchTo.send_keys(self.toName)      
                chosenTo = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/div'))
                )
                chosenTo.find_elements(By.TAG_NAME, 'div')[0].click()
        finally:
            return searchTo.get_attribute('value')

    # This method is to get the results and accumalate the duration and distance betweeen the two countries
    def getResults(self, countryFrom, countryTo):
        # Click Search
        searchButton = WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/button')))
        self.driver.execute_script("arguments[0].click();", searchButton)
        # Get Results
        while(True):
            try:
                results = self.driver.find_element(By.XPATH, '//*[@id="panel"]/div[2]/ul')
                resultsList = results.find_elements(By.TAG_NAME, 'li')
                # print(countryFrom.split(',')[0])
                # print(resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(','))
                # print(countryTo.split(',')[0])
                # print(resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(','))
                
                if(((countryFrom.split(',')[0] in sum([html.unescape(x.lstrip(' ')).split(' ') for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])) or (countryFrom.split(',')[0] in sum([html.unescape(x.lstrip(' ')) for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], []))) or ((countryTo.split(',')[0] in sum([html.unescape(x.lstrip(' ')).split(' ') for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])) or (countryTo.split(',')[0] in sum([html.unescape(x.lstrip(' ')) for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])))):
                    break
            except:
                try:
                    results = WebDriverWait(self.driver, 1).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="panel"]/div[2]/ul'))
                    )
                    resultsList = results.find_elements(By.TAG_NAME, 'li')
                    if(((countryFrom.split(',')[0] in sum([html.unescape(x.lstrip(' ')).split(' ') for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])) or (countryFrom.split(',')[0] in sum([html.unescape(x.lstrip(' ')) for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], []))) or ((countryTo.split(',')[0] in sum([html.unescape(x.lstrip(' ')).split(' ') for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])) or (countryTo.split(',')[0] in sum([html.unescape(x.lstrip(' ')) for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')], [])))):
                        break
                except:
                    results = 'No Path'
                    # print(results)
                    break
    
        totalDuration = 0
        totalDistance = 0

        if(results != 'No Path'):
            self.fromName = resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML")
            # print(self.fromName)  
            # print('------------------')   
            for i in range(len(resultsList)):
                # print(resultsList[i].get_attribute("innerHTML"))
                if(i != (len(resultsList) - 1)):
                    try:
                        distance = resultsList[i].find_elements(By.TAG_NAME, 'p')[0].get_attribute('innerHTML').split(',')
                        totalDistance +=  float(distance[1].lstrip(' ').split(' ')[0].lstrip('('))
                    except:
                        totalDistance += 0

                    duration = (resultsList[i].find_elements(By.TAG_NAME, 'span')[1].get_attribute('innerHTML')).split(' ')
                    if(len(duration) == 6):
                        totalDuration += (int(duration[0]) * 24) + int(duration[2])
                    else:
                        if(duration[0] == 'an'):
                            totalDuration += 1
                        else:
                            totalDuration += int(duration[0])
            self.distance = totalDistance
            self.duration = totalDuration
            # print(f"Total Distance {totalDistance}")
            # print(f"Total Duration {totalDuration}")
            # print('------------------')
            self.toName = resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute('innerHTML')   
    
    # Tried Geopy to calculate only distances between two countries, but only returned distance
    def calculateCoordinatesForFromGeoPy(self):
        # print(f"Class From Row {self.fromNode['Attributes']}".encode('utf-8'))
        try:
            if ("city_name" in self.fromNode['Attributes']):
                if(self.fromNode['Attributes']['city_name'] == 'Unknown'):
                    location = self.geolocator.geocode(f"{self.fromNode['Attributes']['country']}")
                    self.fromCoordinates = (location.latitude, location.longitude)
                        
                else:
                    location = self.geolocator.geocode(f"{self.fromNode['Attributes']['city_name']} {self.fromNode['Attributes']['country']}")
                    self.fromCoordinates = (location.latitude, location.longitude)
                    
            elif ("country" in self.fromNode['Attributes']):
                if(self.fromNode['Attributes']['country'] != 'Unknown'):
                    location = self.geolocator.geocode(f"{self.fromNode['Attributes']['country']}")
                    self.fromCoordinates = (location.latitude, location.longitude)
                else:
                    self.fromCoordinates = (0.0, 0.0)
        except AttributeError:
                self.fromCoordinates = (0.0, 0.0)
            
    def calculateCoordinatesForToGeoPy(self):
        # print(f"Class To Row {self.toNode['Attributes']}".encode('utf-8'))
        try:
            if ("city_name" in self.toNode['Attributes']):
                if(self.toNode['Attributes']['city_name'] == 'Unknown'):
                    location = self.geolocator.geocode(f"{self.toNode['Attributes']['country']}")
                    self.toCoordinates = (location.latitude, location.longitude)
                        
                else:
                    location = self.geolocator.geocode(f"{self.toNode['Attributes']['city_name']} {self.toNode['Attributes']['country']}")
                    self.toCoordinates = (location.latitude, location.longitude)
                    
            elif ("country" in self.toNode['Attributes']):
                if(self.toNode['Attributes']['country'] != 'Unknown'):
                    location = self.geolocator.geocode(f"{self.toNode['Attributes']['country']}")
                    self.toCoordinates = (location.latitude, location.longitude)
                else:
                    self.toCoordinates = (0.0, 0.0)
        except AttributeError:
                self.toCoordinates = (0.0, 0.0)
    
    def calculateDistanceGeoPy(self):
        # print(f"From Coordinates are {self.fromCoordinates}".encode('utf-8'))
        # print(f"To Coordinates are {self.toCoordinates}".encode('utf-8'))
        # print(f"Distance is {self.distance}".encode('utf-8'))
        self.distance = geodesic(self.fromCoordinates, self.toCoordinates).miles
        
    # Tried SeaRates Api, which returns distance and duration but needed to subscribe to a paid plan
    def usingSeaRatesApi(self):
        # Api Key has been expired, so web scraping was used
        modes = ['sea', 'air','road', 'rail']
        apiKey = 'XXXX-XXXX-XXXX-XXXX'

        for mode in modes :
            api_url  = f"https://sirius.searates.com/api/distanceandtime?lat_from=43.7346565&lng_from=15.890404459765625&lat_to=29.2273166&lng_to=48.12082908736457&type={mode}&key={apiKey}&speed=15&road_speed=50"
            response = requests.get(api_url)
            if(response.ok):
                jData = json.loads(response.content)
                if "response" not in jData:
                    print(f"The response contains {jData}".encode('utf-8'))
                    print(f"Keys {jData.keys()}".encode('utf-8'))
                    print(f"Mode {jData[mode]}".encode('utf-8'))
                else:
                    print(f'{jData}')
