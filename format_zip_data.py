import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import uszipcode
from uszipcode import SearchEngine

data_path = '/Users/ameliabaum/Desktop/Amelia/Berkeley Fall 2019/CY PLAN 257/Final Project/data/'
notebooks_path = '/Users/ameliabaum/Desktop/Amelia/Berkeley Fall 2019/CY PLAN 257/Final Project/notebooks/'

persons = pd.read_csv(data_path+'chts_csv_data/Deliv_PER.csv')
hh = pd.read_csv(data_path+'chts_csv_data/Deliv_HH.csv')
place = pd.read_csv(data_path+'chts_csv_data/Deliv_PLACE.csv')
activity = pd.read_csv(data_path+'chts_csv_data/Deliv_ACTIVITY.csv')[['SAMPN', 'PERNO', 'PLANO', 'APURP', 'O_APURP', 'STIME', 'ETIME']]

place['HHPER'] = place['SAMPN'].map(str) + place['PERNO'].map(str) 

place['HHPERPLA'] = place['HHPER'].map(str) + place['PLANO'].map(str)
place1 = place[['HHPERPLA','MODE',"ARR_HR", "ARR_MIN", "DEP_HR", "DEP_MIN","TRIPDUR", "CITY", 'PNAME', 
                'STATE', 'ZIP',"TRACT"]]


activity['HHPER'] = activity['SAMPN'].map(str) + activity['PERNO'].map(str)
activity['HHPERPLA'] = activity['HHPER'].map(str) + activity['PLANO'].map(str)
activity['TSERIES_NUM'] = activity.groupby('HHPERPLA').cumcount() +1 
activity1 = activity.drop("O_APURP", axis =1, inplace=False)

activity2 = pd.merge(place1, activity1, right_on="HHPERPLA", left_on="HHPERPLA")

persons['HHPER'] = persons['SAMPN'].map(str) + persons['PERNO'].map(str)
persons1 = persons[["HHPER", "RELAT", "GEND", "AGE", "HISP", "RACE1"]]
activity_people = pd.merge(persons1, activity2, right_on="HHPER", left_on="HHPER")

import time
start = time.time()
def is_zip(code):
    search = SearchEngine(simple_zipcode=True)
    zipcode = search.by_zipcode(code)
    if (zipcode.zipcode is None or zipcode.state != 'CA' or zipcode.zipcode == 99999):
        return False
    else:
        return True

   
# aa = activity_people.sample(20000)
activity_people["is_zip"] = activity_people["ZIP"].apply(lambda row: is_zip(row))
v_zips = activity_people[activity_people["is_zip"] ==True]
v_zips.head()


end = time.time()

print("elapsed filter ", end-start)


def get_trip_types(df):
    school_work = [5,6,9,10,11, 12, 16,17,18,19,20,25]
    nd_nw = [32,26, 24, 29]
    discretionary = [37,36,34,35,33,14,30]
    shopping = [27,28]
    home = [3,8]
    
    num_sw = len(df[(df["APURP"].isin(school_work))])
    num_ndnw = len(df[(df["APURP"].isin(nd_nw))])
    num_pickdrop = len(df[df["APURP"] == 22])
    num_discret = len(df[(df["APURP"].isin(discretionary))])
    num_shop = len(df[(df["APURP"].isin(shopping))])
    num_home = len(df[(df["APURP"].isin(home))])
    
    return [num_sw, num_ndnw, num_pickdrop, num_discret, num_shop, num_home]
    
def get_TOD_trips(df):
    '''categorized by arrival time'''
    
    
    early_morn = pd.Interval(left='03:00', right='06:59')
    morn = pd.Interval(left='07:00', right='11:59')
    afternoon = pd.Interval(left='12:00', right='16:59')
    eve_night = pd.Interval(left='17:00', right='2:59')
    
     
    num_em = len(df[(df["STIME"].apply(lambda x: x in early_morn))])
    num_m = len(df[(df["STIME"].apply(lambda x: x in morn))])
    num_aft = len(df[(df["STIME"].apply(lambda x: x in afternoon))])
    num_evn = len(df[(df["STIME"].apply(lambda x: x in eve_night))])
    return pd.Series([num_em, num_m, num_aft, num_evn])


def get_trip_modes(df):

    walk = 1
    bike_etc = [2,3,4,8] #Bike/scooter/mobility scooter/wheelchair
    auto_etc = [5,6,7,9,10] #Car/taxi/TNC
    transit_rail = [11,12,range(14,28)] #bus/paratransit/shuttle/rail 
    other = [13,29] #plane,ferry
    
    
    num_walk = len(df[(df["MODE"] == walk)])
    num_bike = len(df[(df["MODE"].isin(bike_etc))])
    num_auto = len(df[df["MODE"].isin(auto_etc)])
    num_transit = len(df[(df["MODE"].isin(transit_rail))])
    num_other = len(df[(df["MODE"].isin(other))])

    return pd.Series[num_walk, num_bike, num_auto, num_transit, num_other]

def get_features(df):
    
    def get_trip_modes(df):

        walk = 1
        bike_etc = [2,3,4,8] #Bike/scooter/mobility scooter/wheelchair
        auto_etc = [5,6,7,9,10] #Car/taxi/TNC
        transit_rail = [11,12,range(14,28)] #bus/paratransit/shuttle/rail 
        other = [13,29] #plane,ferry
    
    
        num_walk = len(df[(df["MODE"] == walk)])
        num_bike = len(df[(df["MODE"].isin(bike_etc))])
        num_auto = len(df[df["MODE"].isin(auto_etc)])
        num_transit = len(df[(df["MODE"].isin(transit_rail))])
        num_other = len(df[(df["MODE"].isin(other))])

        return [num_walk, num_bike, num_auto, num_transit, num_other]

    def get_TOD_trips(df):
        '''categorized by arrival time'''
    
    
        early_morn = pd.Interval(left='03:00', right='06:59')
        morn = pd.Interval(left='07:00', right='11:59')
        afternoon = pd.Interval(left='12:00', right='16:59')
        eve_night = pd.Interval(left='17:00', right='2:59')
    
     
        num_em = len(df[(df["STIME"].apply(lambda x: x in early_morn))])
        num_m = len(df[(df["STIME"].apply(lambda x: x in morn))])
        num_aft = len(df[(df["STIME"].apply(lambda x: x in afternoon))])
        num_evn = len(df[(df["STIME"].apply(lambda x: x in eve_night))])
        return [num_em, num_m, num_aft, num_evn]

    def get_trip_types(df):
        school_work = [5,6,9,10,11, 12, 16,17,18,19,20,25]
        nd_nw = [32,26, 24, 29]
        discretionary = [37,36,34,35,33,14,30]
        shopping = [27,28]
        home = [3,8]
    
        num_sw = len(df[(df["APURP"].isin(school_work))])
        num_ndnw = len(df[(df["APURP"].isin(nd_nw))])
        num_pickdrop = len(df[df["APURP"] == 22])
        num_discret = len(df[(df["APURP"].isin(discretionary))])
        num_shop = len(df[(df["APURP"].isin(shopping))])
        num_home = len(df[(df["APURP"].isin(home))])
    
        return [num_sw, num_ndnw, num_pickdrop, num_discret, num_shop, num_home]
    
    
    return pd.Series(np.concatenate([get_trip_types(df), get_TOD_trips(df), get_trip_modes(df)]))

start1 = time.time()
f = v_zips.groupby("ZIP").apply(get_features)
f.columns = ["num_sw", "num_ndnw", "num_pickdrop", "num_discret", "num_shop", "num_home",
  "num_em", "num_m", "num_aft", "num_evn", "num_walk", "num_bike", "num_auto", "num_transit", "num_other"]



end1 = time.time()
print("elapsed groupby apply: ", end1-start1)

f.to_csv("zip_code_features.csv")



















