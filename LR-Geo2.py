

from igraph import *
from db import get_connection
from cfg import cfg_current_db as cfg,SORTED_COUNTRIES
from tables import sql_tables

from cal_error_street import cal_cdf,get_error
from tool_tracer import load_tracer
#from tool_select_lm import load_v4chinaDict,load_v4Dict,load_wifilmDict
from collections import defaultdict
import concurrent.futures
from myutilities import split_list_by_num
def train(paths):
    landmarks =set()
    landmark_city_dict = dict()
    router_dict = defaultdict(list)
    for path in paths:
        dst,ip,path_id,dst_ttl,dst_rtt,city = path[-1]
        landmark_city_dict[dst] = city
        landmarks.add(dst)
        for dst,ip,path_id,ttl,rtt,city in path[:-1]:
            # ttl-dst_ttl,rtt-dst_rtt: current router to dst
            router_dict[ip].append([dst,dst_ttl-ttl,dst_rtt-rtt])
    return router_dict,landmarks,landmark_city_dict

def test(paths:list,landmarks:set,router_dict:dict):
    geo_result = []
    target_city_dict = dict()
    fail_count = 0
    for path in paths:
        path_r = reversed(path[:-1])
        path_result = []
        dst,ip,path_id,ttl,dst_rtt,city = path[0]
        target_city_dict[dst] = city
        if dst in landmarks:continue
        for dst,ip,path_id,ttl,rtt,city in path_r:
            rtt_r2t = dst_rtt-rtt
            for lm,lm_ttl,lm_rtt in router_dict[ip]:
                relative_delay =  abs(rtt_r2t-lm_rtt)
                path_result.append([dst,lm,relative_delay])
            if path_result:# 如果发现最近的路由器则停止
                # 时延从小到大排序，选最小时延
                path_result.sort(key=lambda x:x[2])
                dst,closest_lm = path_result[0][:2]
                geo_result.append((dst,closest_lm))
                break
        if not path_result:
            fail_count+=1
    return geo_result,target_city_dict,fail_count


def lrgeo(lm_tracerTable:str,target_tracerTable:str,geo_table:str,geo_level='city',location_dict=None,target_paths=[],lm_paths=[]):
    process_num = 20
    conn = get_connection(cfg)
    cursor = conn.cursor()
    fw = open(f'./data/accuracy/lrgeo/{geo_table}.sql','w',encoding='utf8')
    fw.write(sql_tables(geo_table)+';\n')
    #cursor.execute(sql_tables(geo_table))
    landmarks = set()
    '''location propagation'''
    print(geo_table,'start training')
    landmarks = set()
    landmark_city_dict = dict()
    router_dict = defaultdict(list)
    if not lm_paths:
        lm_paths = load_tracer(lm_tracerTable,process_num=10)
        lm_paths = list(lm_paths)
    print(geo_table,'lm_len',len(lm_paths))
    results = []
    excutor = concurrent.futures.ProcessPoolExecutor(max_workers=40)
    for paths in split_list_by_num(lm_paths,40):
        future = excutor.submit(train,paths,)
        results.append(future)
    for future in concurrent.futures.as_completed(results):
        local_router_dict,local_landmarks,local_landmark_city_dict = future.result()
        landmarks.update(local_landmarks)
        router_dict.update(local_router_dict)
        landmark_city_dict.update(local_landmark_city_dict)

    '''geolocation'''
    fail_count = 0
    target_city_dict = dict()
    if not target_paths:
        target_paths = load_tracer(target_tracerTable,filter_targets=landmarks)
        target_paths = list(target_paths)
    geo_result = []
    print(geo_table,'start geo')
    results = []
    excutor = concurrent.futures.ProcessPoolExecutor(max_workers=40)
    for paths in split_list_by_num(target_paths,40):
        future = excutor.submit(test,paths,landmarks,router_dict)
        results.append(future)
    for future in concurrent.futures.as_completed(results):
        local_geo_result, local_target_city_dict, local_failgeo_count = future.result()
        geo_result.extend(local_geo_result)
        target_city_dict.update(local_target_city_dict)
        fail_count+=local_failgeo_count
    
    if geo_level == 'city':
        correct_count = 0
        for target,lm in geo_result:
            city = target_city_dict[target] # true location
            city = city.replace("'","''")
            geo_city = landmark_city_dict[lm] # geo location
            geo_city=geo_city.replace("'","''")
            sql = f"INSERT  INTO `{geo_table}`(ip,lm,city,geo_city) VALUES('{target}','{lm}','{city}','{geo_city}');"
            fw.write(sql+'\n')
            #cursor.execute(sql)
            if city==geo_city:correct_count+=1
        accuracy = correct_count/len(geo_result)
        target_num = len(geo_result)
        
        print(geo_table,'target_num\t',len(geo_result),'\tcorrect num\t',correct_count,'\tacuracy\t',correct_count/len(geo_result),'\tfail_count\t',fail_count)
        
        fw = open(f'./data/accuracy/lrgeo/accuracy.{geo_table}.txt','w')
        fw.write('target num/tcorrect num\taccuracy\tfail count\n')
        fw.write(f'{target_num}\t{correct_count}\t{accuracy}\t{fail_count}\n')
        fw.close()
        return geo_table,target_num,correct_count,accuracy,fail_count
    if geo_level == 'street':
        errors = []
        #location_dict = load_wifilmDict()
        #location_dict = load_v4chinaDict()

        for target,lm in geo_result:
            t_lat,t_lng = location_dict[target][:2] # true location
            lm_lat,lm_lng = location_dict[lm][:2] # geo location
            try:
                error = get_error((t_lat,t_lng),(lm_lat,lm_lng))
            except Exception as e:
                print(e)
            errors.append(error)
            sql = f"INSERT  INTO `{geo_table}`(ip,lm,error) VALUES('{target}','{lm}',{error})"
            cursor.execute(sql)
        cal_cdf(errors,geo_table)


def multiprocess():
    from cfg import SORTED_COUNTRIES
    countires = SORTED_COUNTRIES[1:]
    percent_n = [20,18,16,14,12]
    rlist = []
    excutor = concurrent.futures.ProcessPoolExecutor(max_workers=10)
    results = []
    location_dict = dict()
    
        #city = 'tokyo'


    for country in ['United States']:
        #country = 'Mexico'
        target_table = f'tracer_city_{country}'
        # target_paths = load_tracer(target_table,process_num=5)
        # print(country,'target total',len(target_paths))
        for p in percent_n:
            lm_table = f'tracer_city_{country}_lm_p{p}'
            geo_table = f'geo_lrgeo_{country}_p{p}'
            future = excutor.submit(lrgeo,lm_table,target_table,geo_table,'city',location_dict,[],[])
            results.append(future)

    print('output...')
    for future in concurrent.futures.as_completed(results):
        rlist = future.result()
        try:
            print(''.join(rlist))
        except:
            continue






if __name__ == '__main__':
    percent_n=[i for i in range(2,22,2)]+[0,1]
    for country in SORTED_COUNTRIES:
        #country = 'Mexico'
        target_table = f'tracer_city_{country}'
        target_paths = load_tracer(target_table,process_num=10)
        target_paths = list(target_paths)
        # print(country,'target total',len(target_paths))
        for p in percent_n:
            lm_table = f'tracer_city_{country}_lm_p{p}'
            geo_table = f'geo_lrgeo_{country}_p{p}'
            lrgeo(lm_table,target_table,geo_table,geo_level='city',target_paths=target_paths) 