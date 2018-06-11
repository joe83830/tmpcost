import requests
import sys, os
import glob
from pprint import pprint
import json
import pandas as pd
from pandas.io.json import json_normalize
import datetime
from collections import defaultdict
import time
import math

application_columns = [
    'appId',
    'startedTime',
    'finishedTime',
    'host',
    'queue',
    'finalAppStatus',
    'elapsedTime',
    'appState',
    'name',
    'currentAppAttemptId']

container_columns = [
    'containerId',
    'startedTime',
    'finishedTime',
    'allocatedVCores',
    'allocatedMB']

filepath = '/Users/joeliao/Documents/Cost_analysis/test_csvs/'

def create_pandas_dataframe():
    
    allFiles = glob.glob(filepath + "application_*.csv")
    frame = pd.DataFrame()
    list_ = []
    count = 0
    for file_ in allFiles:
        count += 1
        df = pd.read_csv(file_, delimiter='\t', index_col=None, header=None,
                         names=['container_id', 'allocatedMB', 'allocatedVCores', 'startedTime', 'finishedTime',
                                'containerState'])
        df['application_id'] = file_.split("/")[-1].split(".")[0]
        list_.append(df)

    frame = pd.concat(list_)
    
    # df sorted by start time, unix time adjusted
    sorted_pd_start = frame.sort_values(by=['startedTime'])
    sorted_pd_start['startedTime'] = sorted_pd_start['startedTime'].apply(lambda x: (x / 1000))
    sorted_pd_start['finishedTime'] = sorted_pd_start['finishedTime'].apply(lambda x: (x / 1000))
    
    # df sorted by end time, unix time adjusted
    sorted_pd_end = sorted_pd_start.sort_values(by=['finishedTime'])
    
    return sorted_pd_start, sorted_pd_end

def get_cur_containers(sorted_pd_start, sorted_pd_end, total_memory, cost_per_sec):
    
    # Two pointer-like counters, i iters thru sorted_pd_start, j iters thru the other one
    i, j = 0, 0
    
    # The last point of time at which an event occured (added or deleted from CUR)
    last_event_time = sorted_pd_start.iloc[0, 3]
    
    # All the containers that are running at a given time.
    # Key = containerID, value = aggregated cost so far
    cur_containers = {}
    cur_cost = defaultdict(int)
    
    # Keep track of cur_memory
    cur_memory = 0
    total_len = len(sorted_pd_start)
    
    while i < total_len:
            
        while type(sorted_pd_start.iloc[i, 5]) != str:
            i += 1
            
        while  type(sorted_pd_end.iloc[j, 5]) != str:
            j += 1
            
        add_event_time = sorted_pd_start.iloc[i, 3]
        end_event_time = sorted_pd_end.iloc[j, 4]
        
        if add_event_time > end_event_time:
            
            # KICK FINISHED OUT02           
            del cur_containers[sorted_pd_end.iloc[j, 0]]
            cur_memory -= sorted_pd_end.iloc[j, 1]
            
            while j < total_len - 1:
                next_end_time = sorted_pd_end.iloc[j + 1, 4]
                if next_end_time != end_event_time:
                    break
                j += 1
                # KICK FINISHED OUT
                del cur_containers[sorted_pd_end.iloc[j, 0]]
                cur_memory -= sorted_pd_end.iloc[j, 1]
            j += 1
                
        if add_event_time < end_event_time:
            # ADD TO CUR
            cur_containers[sorted_pd_start.iloc[i, 0]] = (sorted_pd_start.iloc[i, 1], sorted_pd_start.iloc[i, 6])
            cur_memory += sorted_pd_start.iloc[i, 1]

            while i < total_len - 1:        
                next_start_time = sorted_pd_start.iloc[i + 1, 3]
                if next_start_time != add_event_time:
                    break
                i += 1
                # ADD TO CUR
                cur_containers[sorted_pd_start.iloc[i, 0]] = (sorted_pd_start.iloc[i, 1], sorted_pd_start.iloc[i, 6])
                cur_memory += sorted_pd_start.iloc[i, 1]
            i += 1
        
        if add_event_time == end_event_time:
            
            # KICK FINISHED OUT
            del cur_containers[sorted_pd_end.iloc[j, 0]]
            cur_memory -= sorted_pd_end.iloc[j, 1]
            
            while j < total_len - 1:
                next_end_time = sorted_pd_end.iloc[j + 1, 4]
                if next_end_time != end_event_time:
                    break
                j += 1
                # KICK FINISHED OUT
                del cur_containers[sorted_pd_end.iloc[j, 0]]
                cur_memory -= sorted_pd_end.iloc[j, 1]
            
            # ADD TO CUR
            cur_containers[sorted_pd_start.iloc[i, 0]] = (sorted_pd_start.iloc[i, 1], sorted_pd_start.iloc[i, 6])
            cur_memory += sorted_pd_start.iloc[i, 1]

            while i < total_len - 1:        
                next_start_time = sorted_pd_start.iloc[i + 1, 3]
                if next_start_time != add_event_time:
                    break
                i += 1
                # ADD TO CUR
                cur_containers[sorted_pd_start.iloc[i, 0]] = (sorted_pd_start.iloc[i, 1], sorted_pd_start.iloc[i, 6])
                cur_memory += sorted_pd_start.iloc[i, 1]
                
            i += 1
            j += 1
        
        if len(cur_containers) > 0:
            cal_cost(cur_containers, total_memory, cur_memory, cost_per_sec, last_event_time, add_event_time, cur_cost)
            last_event_time = add_event_time
        
#     print(cur_cost)
    result_filepath = '/Users/joeliao/Documents/Cost_analysis/test_csvs/test2.json'
    with open(result_filepath, 'w') as res_outfile:
        json.dump(cur_cost, res_outfile)

def cal_cost(cur_containers, total_memory, cur_memory, cost_per_sec, last_event_time, add_event_time, cur_cost):
    if float(total_memory) == 0:
        print('Total_mem = ' + str(0))
        
    allocated_cost = (float(cur_memory) / float(total_memory)) * float(cost_per_sec)
    unallocated_cost_divided = (float(cost_per_sec) - allocated_cost) / float(len(cur_containers))
    
    
#     print('allocated cost = ' + str(allocated_cost))
#     print('unallocated cost divided = ' + str(unallocated_cost_divided))
    t1 = datetime.datetime.fromtimestamp(last_event_time)
    t2 = datetime.datetime.fromtimestamp(add_event_time)
    elapsed = (t2 - t1).total_seconds()
    
    if cur_memory == 0:
        print('cur_mem = 0')
    for k, v in cur_containers.items():
        cur_cost[v[1]] += ((float(v[0] / cur_memory) * float(allocated_cost)) + unallocated_cost_divided) * elapsed

def main():
    start = time.time()
    df_start, df_end = create_pandas_dataframe()
    get_cur_containers(df_start, df_end, 13107200, 0.01902777777777778)
    end = time.time()
    print(end - start)

if __name__ == '__main__':
    main()