import scrapy
import pandas as pd

# -- Creation of un_url_list containing urls to be explored
#countries_df = pd.read_csv('C:/Users/npass/UNGEGN_World_Geographical_Names/un_countries.csv')
#countries_df = countries_df.drop(['country_name', 'country_coordinates'], axis=1)
#countries_df['url_link'] = countries_df['country_code']
#url_prefix = 'https://unstats.un.org/unsd/geoinfo/geonames/CitiesTable.ashx?id='
#countries_df['url_link'] = countries_df.apply(lambda row: url_prefix + row['country_code'], axis=1)
#un_url_list = countries_df['url_link'].tolist()
#countries_df = None

# -- Spider definition
class CitiesSpider(scrapy.Spider):
    name = 'UnitedNationsCitiesSpider'
    start_urls = ['https://unstats.un.org/unsd/geoinfo/geonames/CitiesTable.ashx?id=AFG']
        
    def parse(self, response):
        country_code = response.url[-3:]
        cities_collection = response.xpath('*//ul') # capturing all unordered list elements
        for c in cities_collection:

            if c.xpath("./@id").get() == 'capitalNames':
                
                is_capital = True # capital assumes True value

                # -- Capturing lists of languages, names and sources
                languages_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[1]/text()").getall()
                city_name_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[2]/text()").getall()
                source_name_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[3]/text()").getall()
                
                print(f'languages list: {languages_list}')
                print(f'city name list: {city_name_list}')
                print(f'source name list: {source_name_list}')

                # -- Removing 'United Nations Languages' from languages_list
                if 'United Nations Languages' in languages_list:
                    index = languages_list.index('United Nations Languages')
                    languages_list.pop(index)

                # -- Removing values after the first occurence of 'Variant name'
                if 'Variant name' in languages_list:
                    index = languages_list.index('Variant name')
                    languages_list = languages_list[:index]

                print(f'New language list: {languages_list}')

                languages_list_len = len(languages_list)
                print(f'Languages list len: {languages_list_len}')

                city_name_list_len = len(city_name_list)
                print(f'City name list len: {city_name_list_len}')

                diff = languages_list_len - city_name_list_len
                print(f'Diff: {diff}')

                print(f'Oficial languages: {len(languages_list[:-6])}')

                city_name = None
                city_name_language = None
                city_name_source = None

                if diff == 0:
                    print('diff == 0')

                    if len(languages_list[:-6]) == 1:
                        print('diff == 0 & len(languages_list[:-6]) == 1')
                        city_name = city_name_list[0]
                        city_name_language = languages_list[0]
                        city_name_source = source_name_list[0]

                    elif len(languages_list[:-6]) > 1:
                        print(f'diff == 0 & len(languages_list[:-6]) > 1')
                        city_name = ','.join([str(elem) for elem in city_name_list[:-6]])
                        city_name_language = ','.join([str(elem) for elem in languages_list[:-6]])
                        city_name_source = ','.join([str(elem) for elem in source_name_list[:-6]])

                elif diff < 0:
                    print('diff < 0')
                    city_name_list = city_name_list[:diff]
                    source_name_list = source_name_list[:diff]
                    city_name = ','.join([str(elem) for elem in city_name_list[:-6]])
                    city_name_language = ','.join([str(elem) for elem in languages_list[:-6]])
                    city_name_source = ','.join([str(elem) for elem in source_name_list[:-6]])

                city_lat = c.xpath("./li/a/span/text()").get().split(" ")[1]
                city_long = c.xpath("./li/a/span/text()").get().split(" ")[4]
                city_coordinates = f'{city_lat},{city_long}'

                city_name_AR = city_name_list[-6:][0]
                city_name_CH = city_name_list[-6:][1]
                city_name_EN = city_name_list[-6:][2]
                city_name_FR = city_name_list[-6:][3]
                city_name_RU = city_name_list[-6:][4]
                city_name_ES = city_name_list[-6:][5]

                yield {
                    'country_code': country_code,
                    'is_capital': is_capital,
                    'city_name': city_name,
                    'city_name_language': city_name_language,
                    'city_coordinates': city_coordinates,
                    'source': city_name_source,
                    'city_name_AR': city_name_AR,
                    'city_name_CH': city_name_CH,
                    'city_name_EN': city_name_EN,
                    'city_name_FR': city_name_FR,
                    'city_name_RU': city_name_RU,
                    'city_name_ES': city_name_ES
                }

            elif c.xpath("./@id").get() == 'cityNames':
                is_capital = False
                other_major_cities = c.xpath('./li')
                for omc in other_major_cities:
                    city_name = omc.xpath("./div/table/tr[3]/td[2]/text()").get()
                    city_name_language = omc.xpath("./div/table/tr[3]/td[1]/text()").get()
                    city_name_source = omc.xpath("./div/table/tr[3]/td[3]/text()").get()
                    city_lat = omc.xpath("./a/span/text()").get().split(" ")[1]
                    city_long = omc.xpath("./a/span/text()").get().split(" ")[4]
                    city_coordinates = f'{city_lat},{city_long}' 
                
                    yield {
                        'country_code': country_code,
                        'is_capital': is_capital,
                        'city_name': city_name,
                        'city_name_language': city_name_language,
                        'city_coordinates': city_coordinates,
                        'source': city_name_source,
                        'city_name_AR': None,
                        'city_name_CH': None,
                        'city_name_EN': None,
                        'city_name_FR': None,
                        'city_name_RU': None,
                        'city_name_ES': None
                    }