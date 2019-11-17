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
print("hi")
place['HHPER'] = place['SAMPN'].map(str) + place['PERNO'].map(str) 

place['HHPERPLA'] = place['HHPER'].map(str) + place['PLANO'].map(str)
place1 = place[['HHPERPLA','MODE',"ARR_HR", "ARR_MIN", "DEP_HR", "DEP_MIN","TRIPDUR", "CITY", 'PNAME', "TRIPDUR",
                'STATE', 'ZIP',"TRACT"]]


activity['HHPER'] = activity['SAMPN'].map(str) + activity['PERNO'].map(str)
activity['HHPERPLA'] = activity['HHPER'].map(str) + activity['PLANO'].map(str)
activity['TSERIES_NUM'] = activity.groupby('HHPERPLA').cumcount() +1 
activity1 = activity.drop("O_APURP", axis =1, inplace=False)

activity2 = pd.merge(place1, activity1, right_on="HHPERPLA", left_on="HHPERPLA")

persons['HHPER'] = persons['SAMPN'].map(str) + persons['PERNO'].map(str)
persons1 = persons[["HHPER", "RELAT", "GEND", "AGE", "HISP", "RACE1"]]
activity_people = pd.merge(persons1, activity2, right_on="HHPER", left_on="HHPER")
print("here")
import time
start = time.time()
def is_zip(code):
    search = SearchEngine(simple_zipcode=True)
    zipcode = search.by_zipcode(code)
    if (zipcode.zipcode is None or zipcode.state != 'CA' or zipcode.zipcode == 99999):
        return False
    else:
        return True

   
# aa = activity_people.sample(2000)
activity_people["is_zip"] = activity_people["ZIP"].apply(lambda row: is_zip(row))
v_zips = activity_people[activity_people["is_zip"] ==True]
v_zips.head()


end = time.time()

print("elapsed filter ", end-start)




def get_features(df):
    
    def get_trip_modes(df):

        walk = 1
        bike_etc = [2,3,4,8] #Bike/scooter/mobility scooter/wheelchair
        auto_etc = [5,6,7,10] #Car/taxi/TNC
        taxi = [9]
        local_bus = [15,16,17,18,19,21,11]
        inter_rail = [22,23,24,25,12]
        intra_rail = [26,27]
        other = [13,29] #plane,ferry
    
    
        num_walk = len(df[(df["MODE"] == walk)])
        num_bike = len(df[(df["MODE"].isin(bike_etc))])
        num_taxi = len(df[(df["MODE"].isin(taxi))])
        num_local_bus = len(df[(df["MODE"].isin(local_bus))])
        num_inter_rail = len(df[(df["MODE"].isin(inter_rail))])
        num_intra_rail = len(df[(df["MODE"].isin(intra_rail))])

        num_auto = len(df[df["MODE"].isin(auto_etc)])
        num_other = len(df[(df["MODE"].isin(other))])

        return [num_walk, num_bike, num_taxi, num_local_bus, num_inter_rail, num_intra_rail, num_auto,
            num_other]

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
        school = [5,17,18,19,20]
        work = [6,9,10,16,12,11,25]
        personal_care = [32,30]
        errands = [26, 24, 29]
        discretionary = [37,36,34,35,33,14]
        shopping = [27,28]
        home = [3,8]
    
        num_sch = len(df[(df["APURP"].isin(school))])
        num_work = len(df[(df["APURP"].isin(work))])
        num_personal = len(df[(df["APURP"].isin(personal_care))])
        num_pickdrop = len(df[df["APURP"] == 22])
        num_discret = len(df[(df["APURP"].isin(discretionary))])
        num_shop = len(df[(df["APURP"].isin(shopping))])
        num_home = len(df[(df["APURP"].isin(home))])
    
        return [num_sch, num_work, num_personal, num_pickdrop, num_discret, num_shop, num_home]
    
#     def get_avg_dur(df):
#         df1 = df.fillna(df.mean())
#         return [(np.mean(df1["TRIPDUR"]))] #1
    
    
    return pd.Series(np.concatenate([get_trip_types(df), get_TOD_trips(df), get_trip_modes(df)]))


start1 = time.time()

f = v_zips.groupby("ZIP").apply(get_features)
# print(f.shape)
f.columns = ["num_sch", "num_work", "num_personal", "num_pickdrop", "num_discret", "num_shop", "num_home",
  "num_em", "num_m", "num_aft", "num_evn", "num_walk", "num_bike", "num_taxi", "num_local_bus", "num_inter_rail",
   "num_intra_rail", "num_auto", "num_other"]



end1 = time.time()
print("elapsed groupby apply: ", end1-start1)

f.to_csv("zip_code_features.csv")



















