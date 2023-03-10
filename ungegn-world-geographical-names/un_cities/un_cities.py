import scrapy
import pandas as pd

# -- Creation of un_url_list containing urls to be explored
countries_df = pd.read_csv('C:/Users/npass/UNGEGN_World_Geographical_Names/un_countries.csv')
countries_df = countries_df.drop(['country_name', 'country_coordinates'], axis=1)
countries_df['url_link'] = countries_df['country_code']
url_prefix = 'https://unstats.un.org/unsd/geoinfo/geonames/CitiesTable.ashx?id='
countries_df['url_link'] = countries_df.apply(lambda row: url_prefix + row['country_code'], axis=1)
un_url_list = countries_df['url_link'].tolist()
countries_df = None

# -- Spider definition
class CitiesSpider(scrapy.Spider):
    name = 'UnitedNationsCitiesSpider'
    start_urls = un_url_list
        
    def parse(self, response):
        country_code = response.url[-3:]
        cities_collection = response.xpath('*//ul') # capturing all unordered list elements
        for c in cities_collection:

            if c.xpath("./@id").get() == 'capitalNames': # if we're dealing with capital's data
                
                is_capital = True # is_capital assumes True value

                # -- Capturing lists of languages, names and sources
                languages_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[1]/text()").getall()
                city_name_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[2]/text()").getall()
                source_list = c.xpath("./li/div/table/tr[@id='UNnames']/preceding-sibling::tr[@id='UNnames']//following-sibling::tr/td[3]/text()").getall()
                
                # -- Removing 'United Nations Languages' from languages_list
                if 'United Nations Languages' in languages_list:
                    index = languages_list.index('United Nations Languages')
                    languages_list.pop(index)

                # -- Removing values after the first occurence of 'Variant name'
                # -- in order to filter onlu country languages and United Nations Languages
                if 'Variant name' in languages_list:
                    index = languages_list.index('Variant name')
                    languages_list = languages_list[:index]

                # -- Getting lists' length
                languages_list_len = len(languages_list)
                city_name_list_len = len(city_name_list)

                # -- Verifying the difference between the lists
                diff = languages_list_len - city_name_list_len

                if diff == 0: # it means that there are no elements (variant names and its' sources) to be removed

                    if len(languages_list[:-6]) == 1: # identifying if there is a single language for capital's name
                        city_name = city_name_list[0]
                        city_name_language = languages_list[0]
                        city_name_source = source_list[0]

                    elif len(languages_list[:-6]) > 1: # identifying if there are more than one language for capital's name
                        city_name = ','.join([str(elem) for elem in city_name_list[:-6]])
                        city_name_language = ','.join([str(elem) for elem in languages_list[:-6]])
                        city_name_source = ','.join([str(elem) for elem in source_list[:-6]])

                elif diff < 0: # it means that there are elements (variant names and its' sources) to be removed
                    city_name_list = city_name_list[:diff]
                    source_list = source_list[:diff]
                    city_name = ','.join([str(elem) for elem in city_name_list[:-6]])
                    city_name_language = ','.join([str(elem) for elem in languages_list[:-6]])
                    city_name_source = ','.join([str(elem) for elem in source_list[:-6]])

                # -- Getting city coordinates (lat and long data)
                city_lat = c.xpath("./li/a/span/text()").get().split(" ")[1]
                city_long = c.xpath("./li/a/span/text()").get().split(" ")[4]
                city_coordinates = f'{city_lat},{city_long}'

                # -- Storing capital's name in each United Nations Language
                city_name_AR = city_name_list[-6:][0]
                city_name_CH = city_name_list[-6:][1]
                city_name_EN = city_name_list[-6:][2]
                city_name_FR = city_name_list[-6:][3]
                city_name_RU = city_name_list[-6:][4]
                city_name_ES = city_name_list[-6:][5]

                # -- Storing data in a dict
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

            elif c.xpath("./@id").get() == 'cityNames': # else if we're dealing with other major cities' data

                is_capital = False # is_capital assumes False value

                other_major_cities = c.xpath('./li') # getting all other major cities (each one is a <li> element)

                for omc in other_major_cities:

                    # -- Attributes city_name, city_name_language and city_name_source
                    # -- of the first presented name
                    city_name = omc.xpath("./div/table/tr[3]/td[2]/text()").get()
                    city_name_language = omc.xpath("./div/table/tr[3]/td[1]/text()").get()
                    city_name_source = omc.xpath("./div/table/tr[3]/td[3]/text()").get()

                    # -- Getting city coordinates (lat and long data)
                    city_lat = omc.xpath("./a/span/text()").get().split(" ")[1]
                    city_long = omc.xpath("./a/span/text()").get().split(" ")[4]
                    city_coordinates = f'{city_lat},{city_long}' 

                    # -- Storing data in a dict
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