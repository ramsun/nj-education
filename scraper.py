import pandas as pd
import datetime
import pymongo
import json
from bson import Binary, Code, json_util, ObjectId
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
import pygeoj
import os
import itertools 
import numpy as np
import matplotlib.pyplot as plt
import requests
from census import Census
import gmaps
from us import states

# Go through all the data and push it into mongo
def init_db(db):
        # files
        rootdir = './data/4YearGraduation'

        # lists defined
        districts_list = []
        years = ['2018','2015','2014','2016','2017']

        # for loop to get all files
        for subdir, dirs, files in os.walk(rootdir):
                for file in files:
                        data = os.path.join(subdir, file)
                        districts_list.append(data)
        districts_list.pop(0)
        # districts_list.reverse()
        grad_df=[]
        #  add all 4 year graduation from 2013 to 2018 to mongod
        for (file, year) in zip(districts_list, years):
                data = file
        #     defined variables labels
                grad_label = "grad_"+ year
                df_label = grad_label +"_df"
        
        #     Read data & add to dataframe
                grad_label = pd.read_csv(data)
                df_label = pd.DataFrame(grad_label)

        #     replace * with 0
                df_label['FOUR_YR_GRAD_RATE'] = df_label['FOUR_YR_GRAD_RATE'].str.replace('*','0')
                df_label['FOUR_YR_ADJ_COHORT_COUNT'] = df_label['FOUR_YR_ADJ_COHORT_COUNT'].str.replace('*','0')
                df_label['GRADUATED_COUNT'] = df_label['GRADUATED_COUNT'].str.replace('*','0')
                df_label['FOUR_YR_GRAD_RATE'] = df_label['FOUR_YR_GRAD_RATE'].str.replace('-','0')
                df_label['FOUR_YR_ADJ_COHORT_COUNT'] = df_label['FOUR_YR_ADJ_COHORT_COUNT'].str.replace('-','0')
                df_label['GRADUATED_COUNT'] = df_label['GRADUATED_COUNT'].str.replace('-','0')
        # set previous change to numeric (int)
                df_label['FOUR_YR_GRAD_RATE'] = pd.to_numeric(df_label['FOUR_YR_GRAD_RATE'])
                df_label['FOUR_YR_ADJ_COHORT_COUNT'] = pd.to_numeric(df_label['FOUR_YR_ADJ_COHORT_COUNT'])
                df_label['GRADUATED_COUNT'] = pd.to_numeric(df_label['GRADUATED_COUNT'])
                grad_df.append(df_label)
        
        frame = pd.concat(grad_df, axis=0, ignore_index=True)

        frame = frame.filter(['YEAR','COUNTY_NAME', 'DISTRICT_NAME','SCHOOL_NAME','SUBGROUP','FOUR_YR_ADJ_COHORT_COUNT','FOUR_YR_GRAD_RATE','GRADUATED_COUNT'])
        frame['YEAR']=frame['YEAR'].astype(int)

        frame = frame.sort_values(by=['YEAR'])

        #################################################
        # School Budget Data
        # files
        rootdir = './data/district'

        # lists defined
        budget_list = []
        years = ['2017_2018','2016_2017','2015_2016','2014_2015','2013_2014']

        # for loop to get all files
        for subdir, dirs, files in os.walk(rootdir):
                for file in files:
                        data = os.path.join(subdir, file)
                        budget_list.append(data)
                        budget_list.sort()
                        budget_list.reverse()
                

        df_budget=[]
        #  add all 4 year graduation from 2013 to 2018 to mongod
        for (file, year) in zip(budget_list, years):
                data = file
        #     defined variables labels
                budget_label = "district_budget"+ year
                df_label = budget_label +"_df"    
        
        #     Read data & add to dataframe
                grad_label = pd.read_csv(data)
                df_label = pd.DataFrame(grad_label)
                df_label = df_label.filter(['YEAR','COUNTY_NAME', 'DISTRICT_NAME','TOTAL'])
                df_label = df_label.rename(index=str, columns={"TOTAL": "DISTRICT_BUDGET"})
                df_budget.append(df_label)

        frame_budget = pd.concat(df_budget, axis=0, ignore_index=True)
        frame_budget['YEAR']=frame_budget['YEAR'].astype(int)

        frame_budget = frame_budget.sort_values(by=['YEAR'])

        budget_df = pd.merge(frame_budget,
                        frame,
                        how='inner',
                        on=['YEAR', 'COUNTY_NAME', 'DISTRICT_NAME'])
        
        #########################
        # Scrape census data
        census_data = './data/citylocs_geocodio.csv'
        census_data = pd.read_csv(census_data)
        census_df = pd.DataFrame(census_data)
        census_df.columns = census_df.columns.str.replace(" ", "_")

        #Drop columns not required
        census_df = census_df.drop(columns=['Street', 'Country', 'State'])

        #Extract all the counties to be filtered from original dataset for census
        lstCounty = budget_df["COUNTY_NAME"].str.strip().tolist()
        lstUniqueCounty = pd.unique(lstCounty)

        #converting census data to match frame county
        census_df['County'] = census_df['County'].str.upper()
        census_df['County'] = census_df['County'].str.replace('COUNTY','').str.strip()

        #Filter only relevant data
        census_filtered_df = census_df[census_df['County'].isin(lstUniqueCounty)]

        # Scrape population data
        population_data = './data/census_pop.csv'
        population_data = pd.read_csv(population_data)
        population_df = pd.DataFrame(population_data)
        population_df = population_df.rename(columns = {'Zipcode': 'Zip'})
 
        #Merging population with regional census data
        pop_census_df = pd.merge(census_filtered_df,
                        population_df,
                        how='left',
                        on='Zip')

        # Merge census and graduation data
        popcensus_budget_df = pop_census_df.merge(budget_df, left_on=['County', 'DISTRICT_NAME'], right_on=['COUNTY_NAME', 'DISTRICT_NAME'], how='inner')

        # clean merged data set
        popcensus_budget_df = popcensus_budget_df[popcensus_budget_df.GRADUATED_COUNT !=0]

        # convert the dataframe to a diciontary, which can be pushed to mongo
        final_dict = popcensus_budget_df.to_dict('records')

        # create mongo collection
        collection = db.education_data
        # drop collection if duplicate
        db.collection.drop()
        # add data to collection
        collection.insert_many(final_dict)

        # df.to_csv(r'Path where you want to store the exported CSV file\File Name.csv')
        popcensus_budget_df.to_csv(r'./data/merge_data.csv')