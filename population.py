#!/home/vasanthan/PycharmProjects/hadoop/venv/bin/spark-submit
from pyspark.sql import SparkSession
import os
import traceback
import matplotlib.pyplot as plt

class PopulationStat:
    def __init__(self):
        resource_path = os.path.join(os.path.dirname(__file__), 'resources/population.csv')
        spark = SparkSession\
            .builder.appName("Population Stat").getOrCreate()
        self.data_frame = spark.read.option('header', 'true').option('inferSchema', 'true')\
            .csv("file://"+resource_path)


    def most_and_leat_populated_country_in_year(self, year, max= True):
        most_populated = {}
        try:
            ###we could not expect the number input should be a int..
            # so typecasting to integer.
            year = int(year)
            grouped_value = self.data_frame.filter(self.data_frame.Year == year).groupBy('CountryCode')
            if max:
                max_in_countries = grouped_value.agg({'Value': 'max'}).withColumnRenamed('max(Value)', 'Population')
                populated_country = max_in_countries.agg({'Population': 'max'}).withColumnRenamed('max(Population)',
                                                                                                      'Population')
            else:
                max_in_countries = grouped_value.agg({'Value': 'min'}).withColumnRenamed('min(Value)', 'Population')
                populated_country = max_in_countries.agg({'Population': 'min'}).withColumnRenamed('min(Population)',
                                                                                                      'Population')
            joinedData = populated_country.join(self.data_frame.withColumnRenamed
                                                    ('Value', 'Population'), ['Population'])
            max_populated_country = joinedData.toPandas()
            most_populated = max_populated_country.to_dict()
        except ValueError as ve:
            print('Cannot typecast to Integer. Class: ', ve.__class__, '\n', traceback.format_exception(ve.__class__,
                                                                                                        ve, ve.__traceback__))
        except Exception as e:
            print('unknown error occured, please check the code and do the needful. Class: ', e.__class__, '\n',
                  traceback.format_exception(e.__class__, e, e.__traceback__))

        return most_populated


    def get_the_spike_population_information(self, high_spike= True):
        spike_population_rate = {}
        try:
            if high_spike:
                spike_population_across_the_nation = self.data_frame.agg({'Value': 'max'}).withColumnRenamed('max(Value)', 'Population')
            else:
                spike_population_across_the_nation = self.data_frame.agg({'Value': 'min'}).withColumnRenamed('min(Value)', 'Population')
            spike_column = spike_population_across_the_nation.join(self.data_frame.withColumnRenamed('Value', 'Population'), 'Population')
            spike_population_rate = spike_column.toPandas().to_dict()
        except Exception as e:
            print('unknown error occured, please check the code and do the needful. Class: ', e.__class__, '\n',
                  traceback.format_exception(e.__class__, e, e.__traceback__))

        return spike_population_rate


    def get_max_and_least_populated_year_in_a_country(self, cc= None, max= True):
        max_or_leat_populated = {}
        if not cc:
            return max_or_leat_populated
        try:
            if max:
                max_populated_info = self.data_frame.filter(self.data_frame.CountryCode == str.upper(cc)).agg({'Value': 'max'})\
                    .withColumnRenamed('max(Value)', 'Population')
            else:
                max_populated_info = self.data_frame.filter(self.data_frame.CountryCode == str.upper(cc)).agg({'Value': 'min'})\
                    .withColumnRenamed('min(Value)', 'Population')
            max_or_leat_populated = max_populated_info.join(self.data_frame.withColumnRenamed('Value', 'Population'), 'Population')
        except Exception as e:
            print('unknown error occured, please check the code and do the needful. Class: ', e.__class__, '\n',
                  traceback.format_exception(e.__class__, e, e.__traceback__))

        return max_or_leat_populated.toPandas().to_dict()

    def six_years_stat_for_nation(self, cc= None, high= False):
        populated_stat = {}
        if not cc:
            return populated_stat
        try:
            if not high:
                max_populated_info = self.data_frame.filter(self.data_frame.CountryCode == str.upper(cc))\
                    .sort('Year', ascending= False).limit(6).sort('Year', ascending= True)
            else:
                max_populated_info = self.data_frame.filter(self.data_frame.CountryCode == str.upper(cc))\
                    .sort('Value', ascending= False).limit(6).sort('Year', ascending= True)
            populated_stat = max_populated_info.toPandas()
            ax = plt.gca()
            populated_stat.plot(kind='line',x='Year',y='Value',ax=ax)
            plt.show()
        except Exception as e:
            print('unknown error occured, please check the code and do the needful. Class: ', e.__class__, '\n',
                  traceback.format_exception(e.__class__, e, e.__traceback__))
        return populated_stat



if __name__ == '__main__':
    p = PopulationStat()
    # most_populated = p.most_and_leat_populated_country_in_year('1960', True)
    # print('---- Most populated country is: ',most_populated['CountryName'][0], ' and it\'s population is: ', most_populated['Population'][0],'----')
    # spike_population_rate = p.get_the_spike_population_information(False)
    # print('--- Spike population in the year', spike_population_rate['Year'][0], ', country is',
    #       spike_population_rate['CountryName'][0], 'and the population count is ', spike_population_rate['Population'][0], '---')
    # max_or_leat_populated = p.get_max_and_least_populated_year_in_a_country('HUN', True)
    # print('--- population in the year', max_or_leat_populated['Year'][0], ', country is',
    #       max_or_leat_populated['CountryName'][0], 'and the population count is ', max_or_leat_populated['Population'][0], '---')
    p.six_years_stat_for_nation('HUN', False)
