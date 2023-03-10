import scrapy

class CountryCodeSpider(scrapy.Spider):
    name = 'UnitedNationsCountriesSpider'
    start_urls = [
        'https://unstats.un.org/unsd/geoinfo/geonames/',
    ]

    def parse(self, response):
        countries = response.xpath('*//select/option') # capturing all options in select button
        for c in countries:
            if c.xpath("./@id").get() is not None: # verifying if there is a country code (id attribute)
                yield {
                    'country_code': c.xpath("./@id").get(), # getting country code (id attribute)
                    'country_name': c.xpath("./text()").get(), # getting country name (text attribute)
                    'country_coordinates': c.xpath("./@value").get() # getting country coordinates (value attribute)
                }