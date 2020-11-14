import pandas as pd
import pycountry
import math
import pickle
import os
from datetime import datetime

data_paths = {
    "mobility": './Global_Mobility_Report.csv',
    "who": './WHO_Cleaned.csv',
    "cases": './COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
    "deaths": './COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
    "recovered": './COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv',
    "country_policy_map": './country_policy_map.pkl'
}

data_frames = {}

def load_data():
    data_frames["mobility"] = pd.read_csv(data_paths['mobility'], dtype={'metro_area': 'string'})
    data_frames["who"] = pd.read_csv(data_paths['who'], dtype={'link': 'string','alt_link_live': 'string','alt_link_eng': 'string'})
    data_frames["cases"] = pd.read_csv(data_paths['cases'])
    # data_frames["deaths"] = pd.read_csv(data_paths['deaths'])
    # data_frames["recovered"] = pd.read_csv(data_paths['recovered'])

def map_date(datemap, date, country):
    country_case_zero = datemap[country]
    time_delta = date - country_case_zero
    return time_delta.days

def is_policy_active(policy_map, date, country, policy):
    if country not in policy_map:
        print("Bad country {} given as argument".format(country))
        return 0
    country_measures = policy_map[country]
    if policy not in country_measures:
        return 0
    measure_active_intervals = policy_map[policy]
    for interval in measure_active_intervals:
        i_start = interval['start']
        i_end = interval['end']
        if i_start is None or date >= i_start:
            if i_end is None or date <= i_end:
                return 1
    return 0

def get_country_code(identifier, id_type='Name'):
    # some cruise ships are included in data and should be removed:
    #   MS Zaandam
    #   Diamond Princess
    # library used is missing some mappings
    # pretty terrible implementation, but time constraints
    hardcoded = {
        "burma": 'MM',
        "congo (brazzaville)": 'CG',
        "congo (kinshasa)": 'CD',
        "korea, south": 'KR',
        "laos": 'LA',
        "taiwan*": 'TW',
        "west bank and gaza": 'PS'
    }
    if identifier.lower() in hardcoded:
        return hardcoded[identifier.lower()]

    try:
        country = None
        if id_type == 'Name':
            country = pycountry.countries.search_fuzzy(identifier)
        else:
            country = pycountry.countries.lookup(identifier)
        return country[0].alpha_2
    except LookupError:
        print("Country '{}' not recognized by library".format(identifier))
        return identifier

def build_dataframe():
    # combine mobility data into two columns, split by countries
    mobility = data_frames['mobility']
    ess_cols = [
        'grocery_and_pharmacy_percent_change_from_baseline',
        'transit_stations_percent_change_from_baseline',
        'workplaces_percent_change_from_baseline'
        ]
    non_ess_cols = [
        'retail_and_recreation_percent_change_from_baseline',
        'parks_percent_change_from_baseline',
        'residential_percent_change_from_baseline'
    ]
    essential_df = mobility.loc[:, ess_cols].mean(1).rename('essential_percent_change_from_baseline')
    nonessential_df = mobility.loc[:, non_ess_cols].mean(1).rename('nonessential_percent_change_from_baseline')
    mobility_df = pd.concat([mobility["country_region_code"], mobility["date"], essential_df, nonessential_df], axis=1)
    # print(mobility_df.head())

    # add policy data
    who = data_frames['who']
    # filter out non-national level policies
    who = who.loc[who['admin_level'] == 'national']
    target_cols = [
        'iso',
        'who_code',
        'date_start',
        'date_end'
    ]
    who = who.loc[:, target_cols]
    print(who.head())

    # construct a map containing intervals when measures are active in a country
    # this takes a long time, so load from file if possible
    country_policy_map = {}
    if os.path.exists(data_paths['country_policy_map']):
        with open(data_paths['country_policy_map'], 'rb') as fp:
            country_policy_map = pickle.load(fp)
    else:
        for i,j in who.iterrows():
            if i % 10 == 0:
                print(i)
            code = get_country_code(j['iso'])
            if code not in country_policy_map:
                country_policy_map[code] = {}
            measure_id = j['who_code']
            start = datetime.strptime(j['date_start'], '%m/%d/%Y') if isinstance(j['date_start'], str) else None
            end = datetime.strptime(j['date_end'], '%m/%d/%Y') if isinstance(j['date_end'], str) else None
            if measure_id in country_policy_map[code]:
                country_policy_map[code][measure_id].append({
                    'start': start,
                    'end': end
                })
            else:
                country_policy_map[code][measure_id] = [{
                    'start': start,
                    'end': end
                }]
        with open(data_paths['country_policy_map'], 'wb') as fp:
            country_policy_map = pickle.dump(country_policy_map, fp)

    # get day-0 date
    # cases = data_frames['cases'].drop(['Province/State', 'Lat', 'Long'], axis=1)
    # day_zero_map = {}
    # for _,j in cases.iterrows():
    #     name = get_country_code(j['Country/Region'], id_type='Name')
    #     days = list(j)
    #     day_zero = '1/22/20'
    #     # start from one as first column is name of region
    #     for k in range(1, len(days)):
    #         if days[k] > 0:
    #             day_zero = cases.columns[k]
    #             break
    #     # some countries are broken up into subregions - use earliest date
    #     day_zero_datetime = datetime.strptime(day_zero, '%m/%d/%y')
    #     if name in day_zero_map:
    #         if day_zero_datetime < day_zero_map[name]:
    #             day_zero_map[name] = day_zero_datetime
    #     else:
    #         day_zero_map[name] = day_zero_datetime

load_data()
build_dataframe()