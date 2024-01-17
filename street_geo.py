import ipaddress
import multiprocessing
import numpy as np
import psutil
from cfg import cfg_v6geo_street as cfg
from db import get_connection,select_from_table
import random
import time
from tables import sql_tables
from myipv6 import get_HMDistance,ipv62hexstr
from tool_select_lm import load_wifilmDict,load_point_distance_dict
from cal_error_street import get_error,cal_cdf
import concurrent.futures
from myio import write_list2file

def load_cluster_table(table_name:str,conn=None,process_num=0):
    '''
    load cluster from disk to memory.

    Parameters:
        - table_name: input table_name in mysql
        - conn: mysql connection. eg, conn = mysql.connector.connect(**mysql_config)
        - process_num: 
    input cluster table, output dict = {ip1:cluster_id1,ip2:cluster_id2,...}
    '''
    time_start = time.time()
    ip_id_dict = dict()
    if not table_name:
        raise Exception('cluster table name is required in func load_cluster_table')
    flag = 0
    if not conn:
        conn = get_connection(cfg)
        flag = 1
    if process_num:
        sql = f"SELECT ip,cluster_id FROM `{table_name}` "
        r = select_from_table(table_name=table_name,sql=sql,cfg=cfg,process_num=process_num)
        for ip,id in r:
            ip_id_dict[ip] = id
        return ip_id_dict
    cursor = conn.cursor()
    sql = f"SELECT MAX(id) FROM `{table_name}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT ip,cluster_id FROM `{table_name}` WHERE id>{start} AND id<={end}"
        cursor.execute(sql)
        for ip,id in cursor.fetchall():
            ip_id_dict[ip] = id
    if flag:
        conn.close()
    print(f'load {table_name} done, time cost: ',time.time()-time_start, 's')
    return ip_id_dict



    '''
    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP
    '''
    fw_sql = open(f'./data/accuracy/{geo_table}.sql','w',encoding='utf8')
    conn = get_connection(cfg)
    cursor = conn.cursor()
    ip_cid_dict = load_cluster_table(clusterTable)
    #cursor.execute(f"SHOW TABLES LIKE '{geo_table}'")
    cursor.execute(f"DROP TABLE IF EXISTS `{geo_table}`")
    cursor.execute(sql_tables(geo_table))
    
    all_landmarks = []
    geo_city_dict = dict()
    sql = f"SELECT MAX(id) FROM `{target_tracerTable}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT DISTINCT dst,city FROM `{target_tracerTable}` WHERE id>{start} and id<={end}"
        cursor.execute(sql)
        for dst,city in cursor.fetchall():
            geo_city_dict[dst] = city
            
    sql = f"SELECT MAX(id) FROM `{lm_tracerTable}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT DISTINCT dst,city FROM `{lm_tracerTable}` WHERE id>{start} and id<={end}"
        cursor.execute(sql)
        for dst,city in cursor.fetchall():
            geo_city_dict[dst] = city
            all_landmarks.append(dst)
    print(f'{geo_table} get path mode start')
    #nei_dict,relation_dict = load_neighborDict_from_db(conn,relation_table='',tracer_table=target_tracerTable)# target_tracerTable includes the target IPs and landmarks
    
    if not lm_path_mode:
        lm_cityMode_dict, lm_dstMode_dict = get_city_mode(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict)
    if not test_path_mode:
        target_cityMode_dict, target_dstMode_dict = get_city_mode(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict,exclusive_tracerTable=lm_tracerTable)
    print('....')
    data = []
    targets_wait2geo = set()
    for targets_mode,targets in target_dstMode_dict.items():
        str_targets_mode = ','.join([str(x) for x in targets_mode])
        t_cid1,t_cid2,t_cid3 = targets_mode
        score = 0
        score_list = []
        for city,city_modes in lm_cityMode_dict.items():
            city_max_score = 0
            city_max_mode = ()
            city_max = ''
            for city_mode in city_modes:
                lm_cid1,lm_cid2,lm_cid3 = city_mode
                if lm_cid1==t_cid1 and lm_cid1!='*':
                    score = 4
                    if lm_cid2 == t_cid2 and lm_cid2!='*':
                        score = 5
                        if lm_cid3 == t_cid3 and lm_cid3!='*':
                            score = 6
                        elif lm_cid3 == t_cid3 and lm_cid3=='*':
                            score = 5.5
                    elif lm_cid2 == t_cid2 and lm_cid3 == '*':
                        score = 4.5
                elif lm_cid2 == t_cid2 and lm_cid2!='*':
                    score = 2
                    if lm_cid3 == t_cid3 and lm_cid3!='*':
                        score = 3
                        if lm_cid1 == t_cid1 and lm_cid1=='*':
                            score = 3.5
                    elif lm_cid3 == t_cid3 and lm_cid3=='*':
                        score = 2.5
                    if lm_cid1 == lm_cid2 and lm_cid1 == '*':
                        score+=0.4
                elif lm_cid3 == t_cid3 and lm_cid3!='*':
                    score = 1
                    if t_cid1 == lm_cid1 and t_cid1 == '*':
                        score+=0.5
                    if t_cid2 == lm_cid2 and t_cid2 == '*':
                        score+=0.4
                if score > city_max_score:
                    city_max_score = score
                    city_max_mode = city_mode
                    city_max = city
                    if city_max_score == 6:break
            if city_max_score == 6:
                city_mode_str = ','.join([str(x) for x in city_max_mode])
                [data.append((target,str_targets_mode,city_mode_str,city,6)) for target in targets]
                break
            score_list.append((city,city_max_score,','.join([str(x) for x in city_max_mode])))
        if score != 0 and score!=6:
            score_list.sort(key=lambda x:x[1],reverse=True)
            city,score,city_mode_str = score_list[0]
            [data.append((target,str_targets_mode,city_mode_str,city,score)) for target in targets]
        else:
            if score!=6:
                targets_wait2geo.update(targets)

    data2 = []
    for target,str_targets_mode,city_mode_str,city,score in data:
        geo_city = geo_city_dict[target]
        error = 0 if geo_city == city else 1
        data2.append((target,city_mode_str,str_targets_mode,city,geo_city,error,score))
        # sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{target}','{city_mode_str}','{str_targets_mode}','{city}','{geo_city}',{error},{score})"
        # cursor.execute(sql) 

    for ip,lm_mode,geo_mode,city,geo_city,error,score in data2:
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    data2 = []
    write_list2file(targets_wait2geo,f'./data/accuracy/{geo_table}.wait2geo.txt')


    targets_wait2geo = list(targets_wait2geo)
    print('wate2geo num ',len(targets_wait2geo))
    targets_wait2geo_exploded = [ipv62hexstr(target) for target in targets_wait2geo]
    all_landmarks_exploded = [ipv62hexstr(lm) for lm in all_landmarks]
    print(f"{geo_table} lm num:",len(all_landmarks),'targets to geo num:',len(targets_wait2geo))
    for i in range(len(targets_wait2geo)):
        max_distance = 0
        closest_lm = ''
        for j in range(len(all_landmarks)):
            hd_distance = sum([k==l for k,l in zip(targets_wait2geo_exploded[i],all_landmarks_exploded[j])])
            if hd_distance>max_distance:
                max_distance = hd_distance
                closest_lm = all_landmarks[j]
        city = geo_city_dict[closest_lm]
        geo_city = geo_city_dict[targets_wait2geo[i]]
        error = 0 if geo_city == city else 1
        data2.append((target,'','',city,geo_city,error,0))
        # sql = f"INSERT INTO `{geo_table}`(ip,city,geo_city,error,score) VALUES('{target}','{city}','{geo_city}',{error},0)"
        # cursor.execute(sql)
    print('output..')
    for ip,lm_mode,geo_mode,city,geo_city,error,score in data2:
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    fw_sql.close()
    # len_data2 = len(data2)
    # num_turn = int(len_data2/10000)+2
    # for i in range(num_turn):
    #     data3 = data2[i*10000:(i+1)*10000]
    #     try:
    #         cursor.executemany(sql,data3)
    #     except Exception as e:
    #         print(e)
    #         conn = get_connection(cfg)
    #         cursor = conn.cursor()
    #         cursor.executemany(sql,data3)

    
    # sql = f"SELECT COUNT(id) FROM `{geo_table}`"
    # cursor.execute(sql)
    # total_target = cursor.fetchone()[0]
    # sql = f"SELECT COUNT(id) FROM `{geo_table}` WHERE city=geo_city"
    # cursor.execute(sql)
    # total_correct_target = cursor.fetchone()[0]
    # sql = f"SELECT COUNT(id) FROM `{geo_table}` WHERE score=0"
    # cursor.execute(sql)
    # total_score0 = cursor.fetchone()[0]
    # sql = f"SELECT COUNT(id) FROM `{geo_table}` WHERE score=0 AND city=geo_city"
    # cursor.execute(sql)
    # total_correct_score0 = cursor.fetchone()[0]
    # accuracy = total_correct_target/total_target
    # accuracy_ = (total_correct_target-total_correct_score0)/(total_target-total_score0)
    # accuracy_score0 = total_correct_score0/total_score0 if total_score0 else 0
    # print('*'*60)
    # print(geo_table)
    # fw.write(geo_table+'\n')
    # print('total target:'+str(total_target))
    # fw.write('total target :'+str(total_target)+'\n')
    # print('correct num:'+str(total_correct_target))
    # fw.write('total target :'+str(total_target)+'\n')
    # print('accuracy: '+str(accuracy))
    # fw.write('accuracy: '+str(accuracy)+'\n')
    # print('num score0: '+str(total_score0))
    # fw.write('num score0: '+str(total_score0)+'n')
    # print('num score0 correct: ',str(total_correct_score0))
    # fw.write('num score0 correct: '+str(total_correct_score0)+'\n')
    # print('score accuracy: '+str(accuracy_score0))
    # fw.write('score accuracy: '+str(accuracy_score0)+'\n')
    # print('remove score0 accuracy: '+str(accuracy_))
    # fw.write('remove score0 accuracy: '+str(accuracy_)+'\n')
    # fw.write('\n'*2)
    # fw.close()
    return target_tracerTable+' done'

    '''
    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP

    new. 不只用后3跳进行比较
    '''
    fw_sql = open(f'./data/accuracy/{geo_table}.sql','w',encoding='utf8')
    conn = get_connection(cfg)
    cursor = conn.cursor()
    ip_cid_dict = load_cluster_table(clusterTable) if not ip_cid_dict else ip_cid_dict
    #cursor.execute(f"SHOW TABLES LIKE '{geo_table}'")
    cursor.execute(f"DROP TABLE IF EXISTS `{geo_table}`")
    cursor.execute(sql_tables(geo_table))
    
    all_landmarks = []
    geo_city_dict = dict()
    sql = f"SELECT MAX(id) FROM `{target_tracerTable}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT DISTINCT dst,city FROM `{target_tracerTable}` WHERE id>{start} and id<={end}"
        cursor.execute(sql)
        for dst,city in cursor.fetchall():
            geo_city_dict[dst] = city
            
    sql = f"SELECT MAX(id) FROM `{lm_tracerTable}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT DISTINCT dst,city FROM `{lm_tracerTable}` WHERE id>{start} and id<={end}"
        cursor.execute(sql)
        for dst,city in cursor.fetchall():
            geo_city_dict[dst] = city
            all_landmarks.append(dst)
    print(f'{geo_table} get path mode start')

    
    if not lm_path_mode:
        lm_cityMode_dict, lm_dstMode_dict = get_city_mode(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict,last_hop=last_hop)
    if not test_path_mode:
        target_cityMode_dict, target_dstMode_dict = get_city_mode(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict,exclusive_tracerTable=lm_tracerTable,last_hop=last_hop)
    print('....')
    data = []
    SCORE_LIST = [2**i for i in range(abs(last_hop))]# 生成等比数列，例如16，8，4，2，1后面的和小于前面的数字
    SCORE_LIST.reverse()
    SUM_SCORE = sum(SCORE_LIST)
    targets_wait2geo = set()

    for targets_mode,targets in target_dstMode_dict.items():
        str_targets_mode = ','.join([str(x) for x in targets_mode])
        score_city_mode_list = []
         # max score
        flag = 1 #是否是得到最大分数后推出
        for city,city_modes in lm_cityMode_dict.items():
            max_mode_score = 0
            max_mode_score_mode = ()
            for city_mode in city_modes:
                mode_score = 0
                for t_cid,lm_cid,score_tmp in zip(targets_mode,city_mode,SCORE_LIST):
                    if t_cid == lm_cid:
                        if t_cid!='*':
                            mode_score+=score_tmp
                        else:#匿名路由器+0.1
                            mode_score+=0.1
                if mode_score > max_mode_score:
                    max_mode_score = mode_score
                    max_mode_score_mode = city_mode
                    if max_mode_score == SUM_SCORE:
                        break
            if max_mode_score == SUM_SCORE:
                flag = 0
                city_mode_str = ','.join([str(x) for x in max_mode_score_mode])
                for target in targets:
                    geo_city = geo_city_dict[target]
                    error = 0 if geo_city == city else 1
                    data.append((target,city_mode_str,str_targets_mode,city,geo_city,error,SUM_SCORE))
                break
            score_city_mode_list.append((city,max_mode_score,','.join([str(x) for x in max_mode_score_mode])))
        if score_city_mode_list and flag:
            score_city_mode_list.sort(key=lambda x:x[1],reverse=True)
            city,score,city_mode_str = score_city_mode_list[0]
            if score == 0:
                targets_wait2geo.update(targets)
                continue
            for target in targets:
                geo_city = geo_city_dict[target]
                error = 0 if geo_city == city else 1
                data.append((target,city_mode_str,str_targets_mode,city,geo_city,error,score))
                # city = city.replace("'","''")
                # geo_city = geo_city.replace("'","''")
                # sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{target}','{city_mode_str}','{str_targets_mode}','{city}','{geo_city}',{error},{score})"
                # cursor.execute(sql) 
        else:
            if flag:
                targets_wait2geo.update(targets)

    for ip,lm_mode,geo_mode,city,geo_city,error,score in data:
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    data = []
    write_list2file(targets_wait2geo,f'./data/accuracy/{geo_table}.wait2geo.txt')

    '''定位用路径无法定位的IP'''
    targets_wait2geo = list(targets_wait2geo)
    print('wate2geo num ',len(targets_wait2geo))
    targets_wait2geo_exploded = [ipv62hexstr(target) for target in targets_wait2geo]
    all_landmarks_exploded = [ipv62hexstr(lm) for lm in all_landmarks]
    print(f"{geo_table} lm num:",len(all_landmarks),'targets to geo num:',len(targets_wait2geo))
    for i in range(len(targets_wait2geo)):
        max_distance = 0
        closest_lm = ''
        for j in range(len(all_landmarks)):
            hd_distance = sum([k==l for k,l in zip(targets_wait2geo_exploded[i],all_landmarks_exploded[j])])
            if hd_distance>max_distance:
                max_distance = hd_distance
                closest_lm = all_landmarks[j]
        city = geo_city_dict[closest_lm]
        target = targets_wait2geo[i]
        geo_city = geo_city_dict[target]
        error = 0 if geo_city == city else 1
        data.append((target,'','',city,geo_city,error,0))
        # sql = f"INSERT INTO `{geo_table}`(ip,city,geo_city,error,score) VALUES('{target}','{city}','{geo_city}',{error},0)"
        # cursor.execute(sql)
    print('output..')
    for ip,lm_mode,geo_mode,city,geo_city,error,score in data:
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    fw_sql.close()
    return target_tracerTable+' done'


def split_dict(dictionary:dict,n:int):
    '''
    split a dict to n dict. return list of n dict
    '''
    count = len(dictionary)
    chunk_size = int(count/n)+1 
    keys = list(dictionary.keys())
    slices = [keys[i:i+chunk_size] for i in range(0, count, chunk_size)]
    result = []
    for slice in slices:
        chunk_dict = {key: dictionary[key] for key in slice}
        result.append(chunk_dict)
    return result


def geo_street4(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,relation_table='',delay_type='',strategy='1',closest_lm=''):
    '''
    geo2仅对路径经过的cluster_id进行比较，粗粒度适用于城市级定位，geo_street对比到相同cluster路径的时延，判断最近地标

    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP

    1. 鉴于geo_street2效果并不理想，查看路径后发现很多路径的cid并不相同。这个函数在geo_street的基础上修改，只对比倒数3跳的cid，如果不同则不继续对比，而是根据最近共同路由器到目标和地标的ttl和时延比较，时延有两个策略，相对时延和最短时延
    
    2. 在geo_street3上修改，加入判断匿名路由器的相似性，如lm_mode1=ipx,ip1,ipy,lm_mode2 = *,ip1,*,t_mode=*,ip1,*，应优先匹配lm_mode2
    '''
    errors = []
    flag= 0
    conn = get_connection(cfg) if not conn else conn
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS `{geo_table}`')
    cursor.execute(sql_tables(geo_table))
    geo_dict = load_wifilmDict(conn)
    if not closest_lm:
        distance_dict,landmark = load_point_distance_dict(lm_tracerTable)
    else:
        landmark = closest_lm
    #geo_dict = load_geoDict()
    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)

    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    rtt_dict.update(rtt_dict2)
    now = time.time()
    if delay_type == 'corr':
        scores = [1,2,3,4,5,6]
    else:
        scores = [1,4,2,5,3,6]
    for test_mode in test_path_mode:
        # 目标IP所在的cluster_id,倒数第一二三跳路由器所在的cluster_id
        target_cid,t_dst = test_mode[0]


        tc1,t_rip1 = test_mode[1] if test_mode[1] !='*' else ('*','')
        tc2,t_rip2 = test_mode[2] if test_mode[2] !='*' else ('*','')
        tc3,t_rip3 = test_mode[3] if test_mode[3] !='*' else ('*','')
        tcity,tisp,tdst,tprovince = test_mode[-1]
        geo_mode = ','.join(list(map(str,[tc1,tc2,tc3])))
        candidate_dict = dict()
        flag = 0
        result_list = []
        cid_target_dict = {tc1:t_rip1,tc2:t_rip2,tc3:t_rip3}
        landmarks = set()
        for lm_mode in lm_path_mode:
            # (cluster_id,ip in path). the orders are dst, last router, penultimate_router, antepenultimate_route
            lm_cid,lm_dst = lm_mode[0]
            landmarks.add(lm_dst)
            lc1,l_rip1 = lm_mode[1] if lm_mode[1]!='*' else ('*','')
            lc2,l_rip2 = lm_mode[2] if lm_mode[2]!='*' else ('*','')
            lc3,l_rip3 = lm_mode[3] if lm_mode[3]!='*' else ('*','')
            lcity,lisp,ldst,lprovince = lm_mode[-1]
            cid_lm_dict = {lc1:l_rip1,lc2:l_rip2,lc3:l_rip3}
            if ldst == tdst:
                flag = 1
                break
            lm_mode_str = ','.join(list(map(str,[lc1,lc2,lc3])))
            
            score = 0
            if tc1 == lc1 and tc1!='*': # 倒数第一跳cid相同且不为空。此时包含地标和目标属于同一个路由器，但没有进一步判断
                delay_r2lm = rtt_dict[(l_rip1,lm_dst)]
                delay_r2t = rtt_dict[(t_rip1,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[5] if l_rip1 == t_rip1 else scores[4]
                result_list.append((lm_dst,corr_delay,score)) # score
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc2 == lc2 and tc2!= '*':
                if tc1=='*' and lc1 =='*':
                    add_score = 0.5
                else:
                    add_score = 0 
                delay_r2lm = rtt_dict[(l_rip2,lm_dst)]
                delay_r2t = rtt_dict[(t_rip2,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[3]+add_score if l_rip2 == t_rip2 else scores[2]+add_score
                result_list.append((lm_dst,corr_delay,score))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc3 == lc3 and tc3!= '*':
                if tc1=='*' and lc1=='*':
                    if tc2=='*' and lc2=='*':
                        add_score = 0.8
                    else:
                        add_score = 0.5
                else:
                    if tc2=='*' and lc2=='*':
                        add_score = 0.3
                    else:
                        add_score = 0
                delay_r2lm = rtt_dict[(l_rip3,lm_dst)]
                delay_r2t = rtt_dict[(t_rip3,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[1]+add_score if l_rip3 == t_rip3 else scores[0]+add_score
                result_list.append((lm_dst,corr_delay,score))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            else: # 根据共同路由器比较
                if strategy == '1':# 寻找一个中间的地标作为估计
                    result_list.append((landmark,0,score))
                    candidate_dict[(tdst,landmark)] = (tdst,landmark,'',geo_mode,tcity,tprovince,'','','',tisp,score)
                elif strategy == '2':
                    for i in range(4,len(test_mode)-1):
                        if test_mode[i]=='*': continue
                        cid_target,router_target = test_mode[i]
                        for j in range(4,len(lm_mode)-1):
                            if lm_mode[j] == '*':continue
                            cid_lm,router_lm = lm_mode[j]
                            if cid_lm == cid_target:
                                score = 0 if router_lm == router_target else -1
                                delay_r2lm = rtt_dict[(router_lm,lm_dst)]
                                delay_r2t = rtt_dict[(router_target,t_dst)]
                                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                                result_list.append((lm_dst,corr_delay,score)) # 因为没有对位相等的cid，排在最后
                    
        # 最后一跳没在同一个cluster,或倒数第二第三跳没有同时在相同的cluster
        if flag == 1:
            continue
        if not result_list:
            score = -2
            rtt_t_dst = rtt_dict[(t_dst,t_dst)]
            for lm in landmarks:
                rtt_lm = rtt_dict[(lm,lm)]
                corr_delay = abs(rtt_t_dst-rtt_lm)
                result_list.append((lm,corr_delay,score))
        result_list.sort(key=lambda x:x[2],reverse=True)# 按score从大到小排
        closest_lm,min_rtt,max_score = result_list[0]
        for lm,delay,score in result_list: # 如果有公共路由器优先使用到公共路由器的时延
            if score<max_score:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        #tdst,ldst,lm_mode,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score = candidate_dict[closest_lm]
        ip,lm,lm_mode,geo_mode,city,tprovince,geo_city,lprovince,lm_isp,geo_isp,score = candidate_dict[(tdst,closest_lm)]
        lm_lat,lm_lng = geo_dict[closest_lm][:2]
        t_lat,t_lng = geo_dict[ip][:2]
        error = get_error((lm_lat,lm_lng),(t_lat,t_lng))
        errors.append(error)
        stm = "INSERT INTO `%s`(ip,lm,lm_mode,geo_mode,city,geo_city,province,geo_province,lm_isp,geo_isp,score,error) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)"%(geo_table,ip,lm,lm_mode,geo_mode,city,geo_city,tprovince,lprovince,lm_isp,geo_isp,score,error)
        cursor.execute(stm)
    cal_cdf(errors,geo_table)
    print(np.mean(errors),np.median(errors))
    return np.mean(errors),np.median(errors)
    print('geo time: %s min'%((time.time()-now)/60))


def geo_street4v2(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,relation_table='',delay_type='',strategy='1',closest_lm=''):
    '''
    geo2仅对路径经过的cluster_id进行比较，粗粒度适用于城市级定位，geo_street对比到相同cluster路径的时延，判断最近地标

    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP

    1. 鉴于geo_street2效果并不理想，查看路径后发现很多路径的cid并不相同。这个函数在geo_street的基础上修改，只对比倒数3跳的cid，如果不同则不继续对比，而是根据最近共同路由器到目标和地标的ttl和时延比较，时延有两个策略，相对时延和最短时延
    
    2. 在geo_street3上修改，加入判断匿名路由器的相似性，如lm_mode1=ipx,ip1,ipy,lm_mode2 = *,ip1,*,t_mode=*,ip1,*，应优先匹配lm_mode2

    new.当时延之间误差小于1ms时，用汉明距离计算，另外去掉匿名路由器的比较，score都为整数
    '''
    errors = []
    flag= 0
    if not conn:
        conn = get_connection(cfg)
        flag = 1
    geo_dict = load_wifilmDict(conn)
    if not closest_lm:
        distance_dict,landmark = load_point_distance_dict(lm_tracerTable)
    else:
        landmark = closest_lm
    #geo_dict = load_geoDict()
    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS `{geo_table}`")
    cursor.execute(sql_tables(geo_table))
    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    rtt_dict.update(rtt_dict2)
    now = time.time()
    if delay_type == 'corr':
        scores = [1,2,3,4,5,6]
    else:
        scores = [1,4,2,5,3,6]
    for test_mode in test_path_mode:
        # 目标IP所在的cluster_id,倒数第一二三跳路由器所在的cluster_id
        target_cid,t_dst = test_mode[0]
        # if t_dst=='240d:1a:4ed:5b00:d6c1:c8ff:fe63:b2da':
        #     print()
        # else:
        #     continue
        tc1,t_rip1 = test_mode[1] if test_mode[1] !='*' else ('*','')
        tc2,t_rip2 = test_mode[2] if test_mode[2] !='*' else ('*','')
        tc3,t_rip3 = test_mode[3] if test_mode[3] !='*' else ('*','')
        tcity,tisp,tdst,tprovince = test_mode[-1]
        geo_mode = ','.join(list(map(str,[tc1,tc2,tc3])))
        candidate_dict = dict()
        flag = 0
        result_list = []
        cid_target_dict = {tc1:t_rip1,tc2:t_rip2,tc3:t_rip3}
        landmarks = set()
        for lm_mode in lm_path_mode:
            # (cluster_id,ip in path). the orders are dst, last router, penultimate_router, antepenultimate_route
            lm_cid,lm_dst = lm_mode[0]
            landmarks.add(lm_dst)
            lc1,l_rip1 = lm_mode[1] if lm_mode[1]!='*' else ('*','')
            lc2,l_rip2 = lm_mode[2] if lm_mode[2]!='*' else ('*','')
            lc3,l_rip3 = lm_mode[3] if lm_mode[3]!='*' else ('*','')
            lcity,lisp,ldst,lprovince = lm_mode[-1]
            cid_lm_dict = {lc1:l_rip1,lc2:l_rip2,lc3:l_rip3}
            if ldst == tdst:
                flag = 1
                break
            lm_mode_str = ','.join(list(map(str,[lc1,lc2,lc3])))
            
            score = 0
            add_score = 0
            hd_distance = get_HMDistance(ldst,tdst)
            if tc1 == lc1 and tc1!='*': # 倒数第一跳cid相同且不为空。此时包含地标和目标属于同一个路由器，但没有进一步判断
                delay_r2lm = rtt_dict[(l_rip1,lm_dst)]
                delay_r2t = rtt_dict[(t_rip1,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[5] if l_rip1 == t_rip1 else scores[4]
                result_list.append((lm_dst,corr_delay,score,hd_distance)) # score
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc2 == lc2 and tc2!= '*':
                # if tc1=='*' and lc1 =='*':
                #     add_score = 0.5
                # else:
                #     add_score = 0 
                delay_r2lm = rtt_dict[(l_rip2,lm_dst)]
                delay_r2t = rtt_dict[(t_rip2,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[3]+add_score if l_rip2 == t_rip2 else scores[2]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc3 == lc3 and tc3!= '*':
                # if tc1=='*' and lc1=='*':
                #     if tc2=='*' and lc2=='*':
                #         add_score = 0.8
                #     else:
                #         add_score = 0.5
                # else:
                #     if tc2=='*' and lc2=='*':
                #         add_score = 0.3
                #     else:
                #         add_score = 0
                delay_r2lm = rtt_dict[(l_rip3,lm_dst)]
                delay_r2t = rtt_dict[(t_rip3,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[1]+add_score if l_rip3 == t_rip3 else scores[0]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            else: # 根据共同路由器比较
                if strategy == '1':# 寻找一个中间的地标作为估计
                    hd_distance = get_HMDistance(tdst,landmark)
                    result_list.append((landmark,0,score,hd_distance))
                    candidate_dict[(tdst,landmark)] = (tdst,landmark,'',geo_mode,tcity,tprovince,'','','',tisp,score)
                elif strategy == '2':
                    for i in range(4,len(test_mode)-1):
                        if test_mode[i]=='*': continue
                        cid_target,router_target = test_mode[i]
                        for j in range(4,len(lm_mode)-1):
                            if lm_mode[j] == '*':continue
                            cid_lm,router_lm = lm_mode[j]
                            if cid_lm == cid_target:
                                score = 0 if router_lm == router_target else -1
                                delay_r2lm = rtt_dict[(router_lm,lm_dst)]
                                delay_r2t = rtt_dict[(router_target,t_dst)]
                                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                                result_list.append((lm_dst,corr_delay,score)) # 因为没有对位相等的cid，排在最后
                    
        # 最后一跳没在同一个cluster,或倒数第二第三跳没有同时在相同的cluster
        if flag == 1:
            continue
        if not result_list:
            score = -2
            rtt_t_dst = rtt_dict[(t_dst,t_dst)]
            for lm in landmarks:
                rtt_lm = rtt_dict[(lm,lm)]
                corr_delay = abs(rtt_t_dst-rtt_lm)
                result_list.append((lm,corr_delay,score))
        result_list.sort(key=lambda x:x[2],reverse=True)# 按score从大到小排
        closest_lm,min_rtt,max_score,_ = result_list[0]
        for lm,delay,score,hd_distance in result_list: # 如果有公共路由器优先使用到公共路由器的时延
            if score<max_score:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        result_list2 = []
        for lm,delay,score,hd_distance in result_list:# 将与最小时延小于1ms的记录
            if score<max_score:break
            if abs(delay-min_rtt)<3:
                result_list2.append([lm,delay,score,hd_distance])
        result_list2.sort(key=lambda x:x[-1],reverse=True) # 汉明距离有大到小排序
        closest_lm,min_rtt,_,max_hd_distance = result_list2[0]
        for lm,delay,score,hd_distance in result_list2: # 如果有公共路由器优先使用到公共路由器的时延
            if hd_distance<max_hd_distance:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        
        ip,lm,lm_mode,geo_mode,city,tprovince,geo_city,lprovince,lm_isp,geo_isp,score = candidate_dict[(tdst,closest_lm)]
        lm_lat,lm_lng = geo_dict[closest_lm][:2]
        t_lat,t_lng = geo_dict[ip][:2]
        error = get_error((lm_lat,lm_lng),(t_lat,t_lng))
        errors.append(error)
        stm = "INSERT INTO `%s`(ip,lm,lm_mode,geo_mode,city,geo_city,province,geo_province,lm_isp,geo_isp,score,error) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)"%(geo_table,ip,lm,lm_mode,geo_mode,city,geo_city,tprovince,lprovince,lm_isp,geo_isp,score,error)
        cursor.execute(stm)
    cal_cdf(errors,geo_table)
    print(np.mean(errors),np.median(errors))
    return np.mean(errors),np.median(errors)
    print('geo time: %s min'%((time.time()-now)/60))

def geo_street4v3(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,geo_dict=None,relation_table='',delay_type='',strategy='1',default_landmark='',train_mode=False,code_test=False):
    '''
    geo2仅对路径经过的cluster_id进行比较，粗粒度适用于城市级定位，geo_street对比到相同cluster路径的时延，判断最近地标

    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP

    1. 鉴于geo_street2效果并不理想，查看路径后发现很多路径的cid并不相同。这个函数在geo_street的基础上修改，只对比倒数3跳的cid，如果不同则不继续对比，而是根据最近共同路由器到目标和地标的ttl和时延比较，时延有两个策略，相对时延和最短时延
    
    2. 在geo_street3上修改，加入判断匿名路由器的相似性，如lm_mode1=ipx,ip1,ipy,lm_mode2 = *,ip1,*,t_mode=*,ip1,*，应优先匹配lm_mode2

    3.当时延之间误差小于1ms时，用汉明距离计算，另外去掉匿名路由器的比较，score都为整数
    
    new. 加入匿名路由器的比较，score为分数，加入汉明距离
    '''
    errors = []
    flag= 0
    conn = get_connection(cfg) if not conn else conn
    cursor = conn.cursor()
    if not train_mode:
        cursor.execute(f"DROP TABLE IF EXISTS `{geo_table}`")
        cursor.execute(sql_tables(geo_table))
    geo_dict = load_wifilmDict(conn) if not geo_dict else geo_dict

    distance_dict,landmark = load_point_distance_dict(lm_tracerTable) if not default_landmark else (None,default_landmark)

    #geo_dict = load_geoDict()
    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)

    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    rtt_dict.update(rtt_dict2)
    now = time.time()
    if delay_type == 'corr':
        scores = [1,2,3,4,5,6]
    else:
        scores = [1,4,2,5,3,6]
    for test_mode in test_path_mode:
        # 目标IP所在的cluster_id,倒数第一二三跳路由器所在的cluster_id
        target_cid,t_dst = test_mode[0]
        if code_test:
            if t_dst=='2a02:1210:5805:9d00:1e24:cdff:fe72:3590':
                print()
            else:
                continue
        tc1,t_rip1 = test_mode[1] if test_mode[1] !='*' else ('*','')
        tc2,t_rip2 = test_mode[2] if test_mode[2] !='*' else ('*','')
        tc3,t_rip3 = test_mode[3] if test_mode[3] !='*' else ('*','')
        tcity,tisp,tdst,tprovince = test_mode[-1]
        geo_mode = ','.join(list(map(str,[tc1,tc2,tc3])))
        candidate_dict = dict()
        flag = 0
        result_list = []
        cid_target_dict = {tc1:t_rip1,tc2:t_rip2,tc3:t_rip3}
        landmarks = set()
        for lm_mode in lm_path_mode:
            # (cluster_id,ip in path). the orders are dst, last router, penultimate_router, antepenultimate_route
            lm_cid,lm_dst = lm_mode[0]
            landmarks.add(lm_dst)
            lc1,l_rip1 = lm_mode[1] if lm_mode[1]!='*' else ('*','')
            lc2,l_rip2 = lm_mode[2] if lm_mode[2]!='*' else ('*','')
            lc3,l_rip3 = lm_mode[3] if lm_mode[3]!='*' else ('*','')
            lcity,lisp,ldst,lprovince = lm_mode[-1]
            cid_lm_dict = {lc1:l_rip1,lc2:l_rip2,lc3:l_rip3}
            if ldst == tdst:
                flag = 1
                break
            lm_mode_str = ','.join(list(map(str,[lc1,lc2,lc3])))
            
            score = 0
            add_score = 0
            hd_distance = get_HMDistance(ldst,tdst)
            if tc1 == lc1 and tc1!='*': # 倒数第一跳cid相同且不为空。此时包含地标和目标属于同一个路由器，但没有进一步判断
                delay_r2lm = rtt_dict[(l_rip1,lm_dst)]
                delay_r2t = rtt_dict[(t_rip1,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[5] if l_rip1 == t_rip1 else scores[4]
                result_list.append((lm_dst,corr_delay,score,hd_distance)) # score
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc2 == lc2 and tc2!= '*':
                if tc1=='*' and lc1 =='*':
                    add_score = 0.5
                else:
                    add_score = 0 
                delay_r2lm = rtt_dict[(l_rip2,lm_dst)]
                delay_r2t = rtt_dict[(t_rip2,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[3]+add_score if l_rip2 == t_rip2 else scores[2]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc3 == lc3 and tc3!= '*':
                if tc1=='*' and lc1=='*':
                    if tc2=='*' and lc2=='*':
                        add_score = 0.8
                    else:
                        add_score = 0.5
                else:
                    if tc2=='*' and lc2=='*':
                        add_score = 0.3
                    else:
                        add_score = 0
                delay_r2lm = rtt_dict[(l_rip3,lm_dst)]
                delay_r2t = rtt_dict[(t_rip3,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[1]+add_score if l_rip3 == t_rip3 else scores[0]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            else: # 根据共同路由器比较
                if strategy == '1':# 寻找一个中间的地标作为估计
                    hd_distance = get_HMDistance(tdst,landmark)
                    result_list.append((landmark,0,score,hd_distance))
                    candidate_dict[(tdst,landmark)] = (tdst,landmark,'',geo_mode,tcity,tprovince,'','','',tisp,score)
                elif strategy == '2':
                    for i in range(4,len(test_mode)-1):
                        if test_mode[i]=='*': continue
                        cid_target,router_target = test_mode[i]
                        for j in range(4,len(lm_mode)-1):
                            if lm_mode[j] == '*':continue
                            cid_lm,router_lm = lm_mode[j]
                            if cid_lm == cid_target:
                                score = 0 if router_lm == router_target else -1
                                delay_r2lm = rtt_dict[(router_lm,lm_dst)]
                                delay_r2t = rtt_dict[(router_target,t_dst)]
                                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                                result_list.append((lm_dst,corr_delay,score)) # 因为没有对位相等的cid，排在最后
                    
        # 最后一跳没在同一个cluster,或倒数第二第三跳没有同时在相同的cluster
        if flag == 1:
            continue
        if not result_list:
            score = -2
            rtt_t_dst = rtt_dict[(t_dst,t_dst)]
            for lm in landmarks:
                rtt_lm = rtt_dict[(lm,lm)]
                corr_delay = abs(rtt_t_dst-rtt_lm)
                result_list.append((lm,corr_delay,score))
        result_list.sort(key=lambda x:x[2],reverse=True)# 按score从大到小排
        closest_lm,min_rtt,max_score,_ = result_list[0]
        for lm,delay,score,hd_distance in result_list: # 如果有公共路由器优先使用到公共路由器的时延
            if score<max_score:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        if code_test:
            errors_tmp = []
            for lm,delay,score,hd_distance in result_list:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
            errors_tmp.sort(key=lambda x:x[-1])
            for i in errors_tmp:
                print(i)
        
        result_list2 = []
        for lm,delay,score,hd_distance in result_list:# 将与最小时延小于1ms的记录
            if score<max_score:break
            if abs(delay-min_rtt)<3:
                result_list2.append([lm,delay,score,hd_distance])
        result_list2.sort(key=lambda x:x[-1],reverse=True) # 汉明距离有大到小排序
        closest_lm,min_rtt,_,max_hd_distance = result_list2[0]
        for lm,delay,score,hd_distance in result_list2: # 在最大汉明距离的地标中选择时延小的地标
            if hd_distance<max_hd_distance:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        if code_test:
            errors_tmp = []
            for lm,delay,score,hd_distance in result_list:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
            errors_tmp.sort(key=lambda x:x[-1])
            for i in errors_tmp:
                print(i)
        ip,lm,lm_mode,geo_mode,city,tprovince,geo_city,lprovince,lm_isp,geo_isp,score = candidate_dict[(tdst,closest_lm)]
        lm_lat,lm_lng = geo_dict[closest_lm][:2]
        t_lat,t_lng = geo_dict[ip][:2]
        error = get_error((lm_lat,lm_lng),(t_lat,t_lng))
        errors.append(error)
        if not train_mode:
            stm = "INSERT INTO `%s`(ip,lm,lm_mode,geo_mode,city,geo_city,province,geo_province,lm_isp,geo_isp,score,error) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)"%(geo_table,ip,lm,lm_mode,geo_mode,city,geo_city,tprovince,lprovince,lm_isp,geo_isp,score,error)
            cursor.execute(stm)
    if not train_mode:
        cal_cdf(errors,geo_table)
        print(np.mean(errors),np.median(errors))
        return np.mean(errors),np.median(errors)
    else:
        return np.mean(errors),np.median(errors)
    print('geo time: %s min'%((time.time()-now)/60))

def geo_street4_all(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,geo_dict=None,relation_table='',delay_type='corr',strategy='1',default_landmark='',rtt_dict=None,cal_anonymousRouter=True,cal_HmDistance=True,train_mode=False,code_test=False):
    '''
    结合了4,4v2 （匿名路由器计分）,4v3（路由器计分+计算汉明距离）,将4v2和4v3变成两个参数
    '''
    errors = []
    flag= 0
    conn = get_connection(cfg) if not conn else conn

    geo_dict = load_wifilmDict(conn) if not geo_dict else geo_dict

    distance_dict,landmark = load_point_distance_dict(lm_tracerTable) if not default_landmark else (None,default_landmark)

    #geo_dict = load_geoDict()
    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)
    cursor = conn.cursor()
    if not train_mode:
        cursor.execute(sql_tables(geo_table))
    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    if not rtt_dict:
        rtt_dict.update(rtt_dict2)
    now = time.time()
    if delay_type == 'corr':
        scores = [1,2,3,4,5,6]
    else:
        scores = [1,4,2,5,3,6]
    for test_mode in test_path_mode:
        # 目标IP所在的cluster_id,倒数第一二三跳路由器所在的cluster_id
        target_cid,t_dst = test_mode[0]
        if code_test:
            if t_dst=='2a02:1210:5805:9d00:1e24:cdff:fe72:3590':
                print()
            else:
                continue
        tc1,t_rip1 = test_mode[1] if test_mode[1] !='*' else ('*','')
        tc2,t_rip2 = test_mode[2] if test_mode[2] !='*' else ('*','')
        tc3,t_rip3 = test_mode[3] if test_mode[3] !='*' else ('*','')
        tcity,tisp,tdst,tprovince = test_mode[-1]
        geo_mode = ','.join(list(map(str,[tc1,tc2,tc3])))
        candidate_dict = dict()
        flag = 0
        result_list = []
        cid_target_dict = {tc1:t_rip1,tc2:t_rip2,tc3:t_rip3}
        landmarks = set()
        for lm_mode in lm_path_mode:
            # (cluster_id,ip in path). the orders are dst, last router, penultimate_router, antepenultimate_route
            lm_cid,lm_dst = lm_mode[0]
            landmarks.add(lm_dst)
            lc1,l_rip1 = lm_mode[1] if lm_mode[1]!='*' else ('*','')
            lc2,l_rip2 = lm_mode[2] if lm_mode[2]!='*' else ('*','')
            lc3,l_rip3 = lm_mode[3] if lm_mode[3]!='*' else ('*','')
            lcity,lisp,ldst,lprovince = lm_mode[-1]
            cid_lm_dict = {lc1:l_rip1,lc2:l_rip2,lc3:l_rip3}
            if ldst == tdst:
                flag = 1
                break
            lm_mode_str = ','.join(list(map(str,[lc1,lc2,lc3])))
            
            score = 0
            add_score = 0
            hd_distance = get_HMDistance(ldst,tdst)
            if tc1 == lc1 and tc1!='*': # 倒数第一跳cid相同且不为空。此时包含地标和目标属于同一个路由器，但没有进一步判断
                delay_r2lm = rtt_dict[(l_rip1,lm_dst)]
                delay_r2t = rtt_dict[(t_rip1,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[5] if l_rip1 == t_rip1 else scores[4]
                result_list.append((lm_dst,corr_delay,score,hd_distance)) # score
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc2 == lc2 and tc2!= '*':
                if cal_anonymousRouter:
                    if tc1=='*' and lc1 =='*':
                        add_score = 0.5
                    else:
                        add_score = 0 
                delay_r2lm = rtt_dict[(l_rip2,lm_dst)]
                delay_r2t = rtt_dict[(t_rip2,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[3]+add_score if l_rip2 == t_rip2 else scores[2]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc3 == lc3 and tc3!= '*':
                if cal_anonymousRouter:
                    if tc1=='*' and lc1=='*':
                        if tc2=='*' and lc2=='*':
                            add_score = 0.8
                        else:
                            add_score = 0.5
                    else:
                        if tc2=='*' and lc2=='*':
                            add_score = 0.3
                        else:
                            add_score = 0
                delay_r2lm = rtt_dict[(l_rip3,lm_dst)]
                delay_r2t = rtt_dict[(t_rip3,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[1]+add_score if l_rip3 == t_rip3 else scores[0]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            else: # 根据共同路由器比较
                if strategy == '1':# 寻找一个中间的地标作为估计
                    hd_distance = get_HMDistance(tdst,landmark)
                    result_list.append((landmark,0,score,hd_distance))
                    candidate_dict[(tdst,landmark)] = (tdst,landmark,'',geo_mode,tcity,tprovince,'','','',tisp,score)
                elif strategy == '2':
                    for i in range(4,len(test_mode)-1):
                        if test_mode[i]=='*': continue
                        cid_target,router_target = test_mode[i]
                        for j in range(4,len(lm_mode)-1):
                            if lm_mode[j] == '*':continue
                            cid_lm,router_lm = lm_mode[j]
                            if cid_lm == cid_target:
                                score = 0 if router_lm == router_target else -1
                                delay_r2lm = rtt_dict[(router_lm,lm_dst)]
                                delay_r2t = rtt_dict[(router_target,t_dst)]
                                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                                result_list.append((lm_dst,corr_delay,score)) # 因为没有对位相等的cid，排在最后
                    
        # 最后一跳没在同一个cluster,或倒数第二第三跳没有同时在相同的cluster
        if flag == 1:
            continue
        if not result_list:
            score = -2
            rtt_t_dst = rtt_dict[(t_dst,t_dst)]
            for lm in landmarks:
                rtt_lm = rtt_dict[(lm,lm)]
                corr_delay = abs(rtt_t_dst-rtt_lm)
                result_list.append((lm,corr_delay,score))
        result_list.sort(key=lambda x:x[2],reverse=True)# 按score从大到小排
        closest_lm,min_rtt,max_score,_ = result_list[0]
        for lm,delay,score,hd_distance in result_list: # 如果有公共路由器优先使用到公共路由器的时延
            if score<max_score:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        if code_test:
            errors_tmp = []
            for lm,delay,score,hd_distance in result_list:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
            errors_tmp.sort(key=lambda x:x[-1])
            for i in errors_tmp:
                print(i)
        if cal_HmDistance:
            result_list2 = []
            for lm,delay,score,hd_distance in result_list:# 将与最小时延小于1ms的记录
                if score<max_score:break
                if abs(delay-min_rtt)<3:
                    result_list2.append([lm,delay,score,hd_distance])
            result_list2.sort(key=lambda x:x[-1],reverse=True) # 汉明距离有大到小排序
            closest_lm,min_rtt,_,max_hd_distance = result_list2[0]
            for lm,delay,score,hd_distance in result_list2: # 在最大汉明距离的地标中选择时延小的地标
                if hd_distance<max_hd_distance:break
                if delay < min_rtt:
                    min_rtt = delay
                    closest_lm = lm
            if code_test:
                errors_tmp = []
                for lm,delay,score,hd_distance in result_list2:
                    lati,lngi = geo_dict[lm][:2]
                    latj,lngj = geo_dict[t_dst][:2]
                    error = get_error((lati,lngi),(latj,lngj))
                    errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
                errors_tmp.sort(key=lambda x:x[-1])
                for i in errors_tmp:
                    print(i)
        ip,lm,lm_mode,geo_mode,city,tprovince,geo_city,lprovince,lm_isp,geo_isp,score = candidate_dict[(tdst,closest_lm)]
        lm_lat,lm_lng = geo_dict[closest_lm][:2]
        t_lat,t_lng = geo_dict[ip][:2]
        error = get_error((lm_lat,lm_lng),(t_lat,t_lng))
        errors.append(error)
        if not train_mode:
            stm = "INSERT INTO `%s`(ip,lm,lm_mode,geo_mode,city,geo_city,province,geo_province,lm_isp,geo_isp,score,error) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)"%(geo_table,ip,lm,lm_mode,geo_mode,city,geo_city,tprovince,lprovince,lm_isp,geo_isp,score,error)
            cursor.execute(stm)
    if not train_mode:
        cal_cdf(errors,geo_table)
    else:
        return np.mean(errors),np.median(errors),landmark
    print('geo time: %s min'%((time.time()-now)/60))

def geo_street_main(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,relation_table='',delay_type='',strategy='1',default_closestLandmark='',code_test=False,):
    '''
    把地标划分为训练集80%和验证集20%，根据中值误差选择使用的算法
    '''

    conn = get_connection(cfg) if not conn else conn
    geo_dict = load_wifilmDict(conn)

    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)
    cursor = conn.cursor()
    cursor.execute(sql_tables(geo_table))
    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    rtt_dict.update(rtt_dict2)
    '''80%地标用来训练，20%用来验证'''
    error1_mean = 0
    error1_median = 0
    error2_mean = 0
    error2_median = 0
    score1 = 0
    score2 = 0
    for _ in range(5):
        train_path_mode = []
        validate_path_mode = []
        num_lm = len(lm_path_mode)
        num_train_lm = int(num_lm*0.5)
        train_pathMode_indices = random.sample(range(0,num_lm),num_train_lm)
        #validate_pathMode_indices = set(range(0,num_lm)) - set(train_pathMode_indices)
        for i in range(0,num_lm):
            if i in train_pathMode_indices:
                train_path_mode.append(lm_path_mode[i])
            else:
                validate_path_mode.append(lm_path_mode[i])
        
        error_mean_1,error_median_1,default_landmark=geo_street4_all(geo_table,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=train_path_mode,test_path_mode=validate_path_mode,tracer_dict=None,ip_id_dict=None,conn=conn,geo_dict=geo_dict,relation_table='',delay_type=delay_type,strategy='1',rtt_dict=rtt_dict,default_landmark='',cal_anonymousRouter=False,cal_HmDistance=False,train_mode=True,code_test=False)
        print(error_mean_1,error_median_1)
        error1_median+=error_median_1
        error1_mean+=error_mean_1

        error_mean_2,error_median_2,_=geo_street4_all(geo_table,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=train_path_mode,test_path_mode=validate_path_mode,tracer_dict=None,ip_id_dict=None,conn=conn,geo_dict=geo_dict,relation_table='',delay_type=delay_type,rtt_dict=rtt_dict,strategy='1',default_landmark=default_landmark,cal_anonymousRouter=True,cal_HmDistance=True,train_mode=True,code_test=False)
        print(error_mean_2,error_median_2)
        error2_median+=error_median_2
        error2_mean+=error_mean_2
        
    error1 = error1_mean-error2_mean+error1_median-error2_median
    print(geo_table,'*'*60,error1)
    if error1<0:
        print(1)
    else:
        print(2)
def geo_street4v4(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_id_dict=None,conn=None,relation_table='',delay_type='',strategy='1',closest_lm='',code_test=False):
    '''
    geo2仅对路径经过的cluster_id进行比较，粗粒度适用于城市级定位，geo_street对比到相同cluster路径的时延，判断最近地标

    给定地标路径（lm_tracer_table)，和目标路径（target_targetTable），对目标路径中的IP进行定位，如果目标路径中有IP与地标重复，则跳过该IP

    1. 鉴于geo_street2效果并不理想，查看路径后发现很多路径的cid并不相同。这个函数在geo_street的基础上修改，只对比倒数3跳的cid，如果不同则不继续对比，而是根据最近共同路由器到目标和地标的ttl和时延比较，时延有两个策略，相对时延和最短时延
    
    2. 在geo_street3上修改，加入判断匿名路由器的相似性，如lm_mode1=ipx,ip1,ipy,lm_mode2 = *,ip1,*,t_mode=*,ip1,*，应优先匹配lm_mode2

    3.当时延之间误差小于1ms时，用汉明距离计算，另外去掉匿名路由器的比较，score都为整数
    
    4. 加入匿名路由器的比较，score为分数，加入汉明距离
    
    new. 在score相同时，汉明距离和时延加权计算排位
    '''
    errors = []
    flag= 0
    if not conn:
        conn = get_connection(cfg)
        flag = 1
    geo_dict = load_wifilmDict(conn)
    if not closest_lm:
        distance_dict,landmark = load_point_distance_dict(lm_tracerTable)
    else:
        landmark = closest_lm
    #geo_dict = load_geoDict()
    #neighbor_dict,relation_dict = load_neighborDict_from_db(conn,relation_table)
    cursor = conn.cursor()
    cursor.execute(sql_tables(geo_table))
    
    if not lm_path_mode:
        lm_path_mode,rtt_dict,router2landmarkDict = get_path_mode2(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn,return_router2landmarkDict=True)
    if not test_path_mode:
        test_path_mode,rtt_dict2 = get_path_mode2(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_id_dict,conn=conn)
    rtt_dict.update(rtt_dict2)
    now = time.time()
    if delay_type == 'corr':
        scores = [1,2,3,4,5,6]
    else:
        scores = [1,4,2,5,3,6]
    for test_mode in test_path_mode:
        # 目标IP所在的cluster_id,倒数第一二三跳路由器所在的cluster_id
        target_cid,t_dst = test_mode[0]
        if code_test:
            if t_dst=='2a02:1210:5805:9d00:1e24:cdff:fe72:3590':
                print()
            else:
                continue
        tc1,t_rip1 = test_mode[1] if test_mode[1] !='*' else ('*','')
        tc2,t_rip2 = test_mode[2] if test_mode[2] !='*' else ('*','')
        tc3,t_rip3 = test_mode[3] if test_mode[3] !='*' else ('*','')
        tcity,tisp,tdst,tprovince = test_mode[-1]
        geo_mode = ','.join(list(map(str,[tc1,tc2,tc3])))
        candidate_dict = dict()
        flag = 0
        result_list = []
        cid_target_dict = {tc1:t_rip1,tc2:t_rip2,tc3:t_rip3}
        landmarks = set()
        for lm_mode in lm_path_mode:
            # (cluster_id,ip in path). the orders are dst, last router, penultimate_router, antepenultimate_route
            lm_cid,lm_dst = lm_mode[0]
            landmarks.add(lm_dst)
            lc1,l_rip1 = lm_mode[1] if lm_mode[1]!='*' else ('*','')
            lc2,l_rip2 = lm_mode[2] if lm_mode[2]!='*' else ('*','')
            lc3,l_rip3 = lm_mode[3] if lm_mode[3]!='*' else ('*','')
            lcity,lisp,ldst,lprovince = lm_mode[-1]
            cid_lm_dict = {lc1:l_rip1,lc2:l_rip2,lc3:l_rip3}
            if ldst == tdst:
                flag = 1
                break
            lm_mode_str = ','.join(list(map(str,[lc1,lc2,lc3])))
            
            score = 0
            add_score = 0
            hd_distance = get_HMDistance(ldst,tdst)
            if tc1 == lc1 and tc1!='*': # 倒数第一跳cid相同且不为空。此时包含地标和目标属于同一个路由器，但没有进一步判断
                delay_r2lm = rtt_dict[(l_rip1,lm_dst)]
                delay_r2t = rtt_dict[(t_rip1,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[5] if l_rip1 == t_rip1 else scores[4]
                result_list.append((lm_dst,corr_delay,score,hd_distance)) # score
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc2 == lc2 and tc2!= '*':
                if tc1=='*' and lc1 =='*':
                    add_score = 0.5
                else:
                    add_score = 0 
                delay_r2lm = rtt_dict[(l_rip2,lm_dst)]
                delay_r2t = rtt_dict[(t_rip2,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[3]+add_score if l_rip2 == t_rip2 else scores[2]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            elif tc3 == lc3 and tc3!= '*':
                if tc1=='*' and lc1=='*':
                    if tc2=='*' and lc2=='*':
                        add_score = 0.8
                    else:
                        add_score = 0.5
                else:
                    if tc2=='*' and lc2=='*':
                        add_score = 0.3
                    else:
                        add_score = 0
                delay_r2lm = rtt_dict[(l_rip3,lm_dst)]
                delay_r2t = rtt_dict[(t_rip3,t_dst)]
                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                score = scores[1]+add_score if l_rip3 == t_rip3 else scores[0]+add_score
                result_list.append((lm_dst,corr_delay,score,hd_distance))
                candidate_dict[(tdst,ldst)] = (tdst,ldst,lm_mode_str,geo_mode,tcity,tprovince,lcity,lprovince,lisp,tisp,score)
            else: # 根据共同路由器比较
                if strategy == '1':# 寻找一个中间的地标作为估计
                    hd_distance = get_HMDistance(tdst,landmark)
                    result_list.append((landmark,0,score,hd_distance))
                    candidate_dict[(tdst,landmark)] = (tdst,landmark,'',geo_mode,tcity,tprovince,'','','',tisp,score)
                elif strategy == '2':
                    for i in range(4,len(test_mode)-1):
                        if test_mode[i]=='*': continue
                        cid_target,router_target = test_mode[i]
                        for j in range(4,len(lm_mode)-1):
                            if lm_mode[j] == '*':continue
                            cid_lm,router_lm = lm_mode[j]
                            if cid_lm == cid_target:
                                score = 0 if router_lm == router_target else -1
                                delay_r2lm = rtt_dict[(router_lm,lm_dst)]
                                delay_r2t = rtt_dict[(router_target,t_dst)]
                                corr_delay = abs(delay_r2lm-delay_r2t) if delay_type == 'corr' else delay_r2lm
                                result_list.append((lm_dst,corr_delay,score)) # 因为没有对位相等的cid，排在最后
                    
        # 最后一跳没在同一个cluster,或倒数第二第三跳没有同时在相同的cluster
        if flag == 1:
            continue
        if not result_list:
            score = -2
            rtt_t_dst = rtt_dict[(t_dst,t_dst)]
            for lm in landmarks:
                rtt_lm = rtt_dict[(lm,lm)]
                corr_delay = abs(rtt_t_dst-rtt_lm)
                result_list.append((lm,corr_delay,score))
        result_list.sort(key=lambda x:x[2],reverse=True)# 按score从大到小排
        closest_lm,min_rtt,max_score,_ = result_list[0]
        for lm,delay,score,hd_distance in result_list: # 如果有公共路由器优先使用到公共路由器的时延
            if score<max_score:break
            if delay < min_rtt:
                min_rtt = delay
                closest_lm = lm
        if code_test:
            errors_tmp = []
            for lm,delay,score,hd_distance in result_list:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
            errors_tmp.sort(key=lambda x:x[-1])
            for i in errors_tmp:
                print(i)
        
        result_list2 = []
        for lm,delay,score,hd_distance in result_list:
            if score<max_score:break
            if abs(delay-min_rtt)<3:
                result_list2.append([lm,delay,score,hd_distance])
        # '''
        # 以下代码用于实现顺序递增的排序。例如前两名并列第一，那么排在后面的应该是第三。score初始化为1，先根据汉明距离排序，后者汉明距离大于前者则score+=1，score最大不会超过进行排序的地标数量。对时延相同操作。将两者的score加权。score其实就是排序的顺序。score越大越好，汉明距离从小到大排，时延从大到小排，排在后面的索引大，分数大。例如，最小汉明距离为10，有两个，则这两个lm的score为1，下一个lm汉明距离为11，则score为3。
        # '''
        result_list2.sort(key=lambda x:x[-1]) # 汉明距离小到大排序
        min_hd = result_list2[0][-1]
        hmdScore = 1 
        lm_hmdScore_dict = dict() # 汉明距离相同的分数一样，最小分数从1开始累加
        lm_delayScore_dict =dict()
        count_tmp = 0 # 用来实现占位，例如汉明距离都为10的有两个，分数是10，那个下一个分数是12
        for i,(lm,delay,score,hd_distance) in enumerate(result_list2,start=1):
            if hd_distance==min_hd:
                lm_hmdScore_dict[lm] = hmdScore
                
            else:
                min_hd = hd_distance
                hmdScore=i
                lm_hmdScore_dict[lm] = hmdScore
        result_list2.sort(key=lambda x:x[1],reverse=True) # 时延从大到小排序
        max_delay = result_list2[0][1]
        delayScore = 1
        count_tmp = 0 # 用来实现占位，例如汉明距离都为10的有两个，分数是10，那个下一个分数是12
        for i,(lm,delay,score,hd_distance) in enumerate(result_list2,start=1):
            if abs(max_delay-delay)<=0.01: # 认为0.01可以忽略，高斯噪声，在这个范围内认为时延相同
                lm_delayScore_dict[lm] = delayScore
                
            else:
                max_delay = delay
                delayScore = i
                lm_delayScore_dict[lm] = delayScore
        lm_score_weightedList = []
        for lm,hmdScore in lm_hmdScore_dict.items():
            delayScore = lm_delayScore_dict[lm]
            weighted_score = delayScore*0.6+hmdScore*0.4
            lm_score_weightedList.append((lm,weighted_score))
        lm_score_weightedList.sort(key=lambda x:x[-1])
        closest_lm = lm_score_weightedList[-1][0]
        if code_test:
            errors_tmp = []
            for lm,delay,score,hd_distance in result_list2:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                errors_tmp.append((lm,tdst,delay,score,hd_distance,error))
            errors_tmp.sort(key=lambda x:x[-1])
            for i in errors_tmp:
                print(i)
            print('*'*60)
            for lm,score in lm_score_weightedList:
                lati,lngi = geo_dict[lm][:2]
                latj,lngj = geo_dict[t_dst][:2]
                error = get_error((lati,lngi),(latj,lngj))
                print(lm,'weighted score:',score,' delay score:',lm_delayScore_dict[lm], 'hdm score',lm_hmdScore_dict[lm],' error:',error)

        ip,lm,lm_mode,geo_mode,city,tprovince,geo_city,lprovince,lm_isp,geo_isp,score = candidate_dict[(tdst,closest_lm)]
        lm_lat,lm_lng = geo_dict[closest_lm][:2]
        t_lat,t_lng = geo_dict[ip][:2]
        error = get_error((lm_lat,lm_lng),(t_lat,t_lng))
        errors.append(error)
        stm = "INSERT INTO `%s`(ip,lm,lm_mode,geo_mode,city,geo_city,province,geo_province,lm_isp,geo_isp,score,error) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s)"%(geo_table,ip,lm,lm_mode,geo_mode,city,geo_city,tprovince,lprovince,lm_isp,geo_isp,score,error)
        cursor.execute(stm)
    cal_cdf(errors,geo_table)
    print('geo time: %s min'%((time.time()-now)/60))


# return mode_list = [[cluster_id1,id2,id3,id4,(city,isp)],[],[]]
def get_path_mode(tracer_table:str,cluster_table:str,tracer_dict = None,ip_id_dict=None,conn=None,exclusive_tracerTable=''):
    if not tracer_table or not cluster_table:
        raise Exception('tracer table is blank in get_path_mode')
    if not tracer_dict:
        tracer_dict = get_path_sequence2(tracer_table,cluster_table,ip_id_dict=ip_id_dict,conn=conn,exclusive_tracerTable=exclusive_tracerTable)
    now = time.time()
    mode_list =[]
    for k,v in tracer_dict.items():
        
        # # 从最后一个IP向前输出
        l = list(reversed(v))
        hop_index,cid_dst,isp,city,dst,province = l[0]
        # # 倒数第一是dst，倒数前三跳是路由器IP
        mode = []
        count = 0
        for hop_id,cluster_id,isp,city,dst,province in l:
            while(hop_id!=hop_index):
                mode.append('*')
                count+=1
                hop_index-=1
            mode.append(cluster_id)
            count+=1
            hop_index-=1
            if count>=4:
                break
        mode.append((city,isp,dst,province))
        if mode not in mode_list:
            mode_list.append(mode)
    end = time.time()
    print('get_path_mode time: %s min'%((end-now)/60),)
    return mode_list

def get_city_mode(tracer_table='',cluster_table='',tracer_dict = None,ip_id_dict=None,conn=None,exclusive_tracerTable='',last_hop=-3):
    '''
    key = city, values = [mode1,mode2,mode3], mode1 = (cid1,cid2,cid3)

    Parameters:
        - last_hop=-3:倒数前3跳的路由器
    '''
    MAX_HOP = 1-last_hop # 1为探测目标
    STAR_MODE = tuple(['*']*(MAX_HOP-1))
    city_mode_dict = dict() # key是city，values是mode
    mode_dst_dict = dict() # key是mode，values是dst
    cityMode_dst_dict = dict() #key=(city,mode) values=(dst,)
    if not tracer_dict:
        tracer_dict = get_path_sequence2(tracer_table,cluster_table,ip_id_dict=ip_id_dict,conn=conn,exclusive_tracerTable=exclusive_tracerTable,last_hop=last_hop)
    now = time.time()
    star_count = 0

    for path in tracer_dict:
        
        # # 从最后一个IP向前输出
        l = list(reversed(path))
        hop_index,cid_dst,isp,city,dst,province = l[0]
        # # 倒数第一是dst，倒数前三跳是路由器IP
        mode = []
        count = 0
        for hop_id,cluster_id,isp,city,dst,province in l:
            while(hop_id!=hop_index):
                mode.append('*')
                count+=1
                if count>=MAX_HOP:break
                hop_index-=1
            mode.append(cluster_id)
            count+=1
            hop_index-=1
            if count>=MAX_HOP:
                break
        if len(mode)<MAX_HOP:
            add_num = MAX_HOP-len(mode)
            [mode.append('*') for _ in range(add_num)]
        mode = tuple(mode[1:MAX_HOP])
        if mode == STAR_MODE:
            star_count+=1
            continue
        if city in city_mode_dict.keys():
            city_mode_dict[city].add(mode)
        else:
            city_mode_dict[city] = {mode}
        if mode in mode_dst_dict.keys():
            mode_dst_dict[mode].add(dst)
        else:
            mode_dst_dict[mode] = {dst}
        mode_str = ','.join([str(x) for x in mode])
        if (city,mode_str) in cityMode_dst_dict.keys():
            cityMode_dst_dict[(city,mode_str)].add(dst)
        else:
            cityMode_dst_dict[((city,mode_str))] = {dst}
    end = time.time()
    print(tracer_table,' get_city_mode time: ', (end-now), '* mode num: ',star_count)
    return city_mode_dict,mode_dst_dict,cityMode_dst_dict

# return mode_list = [[(cluster_id1,dst),(cid2,last_router_ip),(cid3,penultimate_router),(cid4,antepenultimate_router),(city,isp,dst,province)],[],[]]
# return mode_list = [[path1],[path2]]..
def get_path_mode2(tracer_table:str,cluster_table:str,tracer_dict = None,ip_id_dict=None,conn=None,router_num=3,return_router2landmarkDict=False,return_unsigned_rtt=True):
    '''
    Parameters:
        - router_num: 倒数前n跳路由，默认为3，即保留路径上倒数3跳的路由器（不包括目标），返回会返回4个节点信息，第一个节点为目标的信息，然后是倒数第1跳路由器信息...即return mode_list = [[(cluster_id1,dst),(cid2,last_router_ip),(cid3,penultimate_router),(cid4,antepenultimate_router),(city,isp,dst,province)],[],[]]
        - return_router2landmarkDict=False, 是否返回，router2landmarkDict是路径上一个路由器IP能够到达的所有地标
        - return_unsigned_rtt: 控制返回的rtt_dict中时延的正负，如果True则rtt为绝对值，否则有负值，表示发生了时延膨胀，后跳IP的rtt小于前跳IP。
    Returns:
        - return mode_list = [[(cluster_id1,dst),(cid2,last_router_ip),(cid3,penultimate_router),(cid4,antepenultimate_router),(city,isp,dst,province)],[],[]]
        - rtt_dict: rtt of a router in path to the dst key=(ip,dst),value=rtt
        
    '''
    if not tracer_table or not cluster_table:
        raise Exception('tracer table is blank in get_path_mode')
    if not tracer_dict:
        tracer_dict = get_path_sequence3(tracer_table,cluster_table,ip_id_dict=ip_id_dict,conn=conn)
    now = time.time()
    mode_list =[]
    router2landmarkDict = dict()#路径上一个路由器IP能够到达的所有地标
    rtt_dict = dict()# 路径上路由器到目标IP的rtt
    for k,v in tracer_dict.items():
        # k=path_id, v= [(hop_id,cluster_id,ip,isp,city,dst,province,rtt),...] 是一条路径
        # # 从最后一个IP向前输出
        dst_rtt = v[-1][-1]
        dst = v[-1][5]
        rtt_dict[(dst,dst)] = dst_rtt
        for hop_id,cluster_id,ip,isp,city,dst,province,rtt in v:
            if ip in router2landmarkDict.keys():
                router2landmarkDict[ip].add(dst)
            else:
                router2landmarkDict[ip] = {dst}
            corr_delay = abs(rtt-dst_rtt) if return_unsigned_rtt else (dst_rtt-rtt)
            if (ip,dst) in rtt_dict.keys():
                old_delay = rtt_dict[(ip,dst)]
                # if corr_delay < old_delay:
                #     rtt_dict[(ip,dst)] = corr_delay
                if corr_delay < old_delay and old_delay>0 and corr_delay>0:
                    rtt_dict[(ip,dst)] = corr_delay
                # elif old_delay <0 and corr_delay>old_delay:
                #     rtt_dict[(ip,dst)] = corr_delay
            else:
                rtt_dict[(ip,dst)] = corr_delay
        l = list(reversed(v))
        # if len(l)<=3:
        #     print(str(l))
        hop_index = l[0][0]
        isp = l[0][3]
        city = l[0][4]
        dst = l[0][5]
        province = l[0][6]
        # # 倒数第一是dst，倒数前三跳是路由器IP
        mode = []
        count = 0
        for hop_id,cluster_id,ip,isp,city,dst,province,rtt in l:
            while(hop_id!=hop_index):
                mode.append('*')
                count+=1
                hop_index-=1
            mode.append((cluster_id,ip))
            count+=1
            hop_index-=1
            if count>=router_num+1:# +1为目标IP的信息
                break
        mode.append((city,isp,dst,province))
        mode_list.append(mode)
    end = time.time()
    print('get_path_mode time: %s min'%((end-now)/60),)
    if not return_router2landmarkDict:
        return mode_list,rtt_dict  
    return mode_list,rtt_dict,router2landmarkDict

# 配合get_path_mode_dic使用
def get_path_sequence2(tracer_table='',cluster_table='',ip_id_dict=None,conn=None,exclusive_tracerTable='',last_hop=-3,sql_results=None,exclusive_path_ids=[]):
    '''
    查询tracer_table，根据cluster_id和ip对应关系，将tracer_table中的ip使用cluster_id编号后，与其他信息一起返回
    '''
    time_start = time.time()
    conn = get_connection(cfg) if not conn else conn
    cursor = conn.cursor()
    if not ip_id_dict:
        ip_id_dict = load_cluster_table(cluster_table,conn=conn)
    
    if not exclusive_path_ids:
        exclusive_path_ids = set()
        if exclusive_tracerTable:
            sql = f"SELECT MAX(id) FROM `{exclusive_tracerTable}`"
            cursor.execute(sql)
            max_id = cursor.fetchone()[0]
            interval = 100000 # 每次查10w条
            end_range = int(max_id/interval)+1
            for i in range(end_range):
                start = i*interval
                end = (i+1)*interval
                sql = f"SELECT DISTINCT path_id FROM `{exclusive_tracerTable}` WHERE id>{start} and id<={end}"
                cursor.execute(sql)
                [exclusive_path_ids.add(id) for id, in cursor.fetchall()]
    '''由于有的表内容太多，分批次查询'''
    if sql_results:
        paths = []
        path = []
        path_id_old = -1
        for dst,path_id,ip,hop_id,isp,city,province in sql_results:
            if path_id in exclusive_path_ids: continue
            cluster_id = ip_id_dict[ip]
            if path_id!=path_id_old:
                paths.append(path[last_hop-1:])# last_hop=-3,默认取倒数后4，包括目标IP和3跳路由器
                path = []
                path_id_old = path_id
            path.append((hop_id,cluster_id,isp,city,dst,province))
        print('get_path_sequence2 time: %s min'%((time.time()-time_start)/60),)
        return paths[1:]
    sql = f"SELECT MAX(id) FROM `{tracer_table}`"
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]
    interval = 100000 # 每次查10w条
    end_range = int(max_id/interval)+1
    paths = []
    path = []
    path_id_old = -1
    for i in range(end_range):
        start = i*interval
        end = (i+1)*interval
        sql = f"SELECT dst,path_id,ip,hop_id,isp,city,province FROM `{tracer_table}` WHERE id>{start} and id<={end}"
        cursor.execute(sql)
        for dst,path_id,ip,hop_id,isp,city,province in cursor.fetchall():
            if path_id in exclusive_path_ids: continue
            cluster_id = ip_id_dict[ip]
            if path_id!=path_id_old:
                paths.append(path[last_hop-1:])# last_hop=-3,默认取倒数后4，包括目标IP和3跳路由器
                path = []
                path_id_old = path_id
            path.append((hop_id,cluster_id,isp,city,dst,province))
    print('get_path_sequence2 time: %s min'%((time.time()-time_start)/60),)
    return paths[1:]

def get_path_sequence3(tracer_table:str,cluster_table:str,ip_id_dict=None,conn=None):
    '''
    将cluster_id加入路径信息中，tmp_dict[path_id].append((hop_id,cluster_id,ip,isp,city,dst,province))
    '''
    now = time.time()
    if not ip_id_dict:
        ip_id_dict = load_cluster_table(cluster_table,conn=conn)
    end = time.time()
    print('load cluster table time: %s min'%((end-now)/60),)
    now = time.time()
    flag = 0
    if not conn:
        conn = get_connection(cfg)
        flag = 1
    cursor = conn.cursor()
    sql = "SELECT dst,path_id,ip,hop_id,isp,city,province,rtt FROM `%s`"%tracer_table
    cursor.execute(sql)
    tmp_dict = dict()
    for dst,path_id,ip,hop_id,isp,city,province,rtt in cursor.fetchall():
        cluster_id = ip_id_dict[ip]
        if path_id in tmp_dict.keys():
            tmp_dict[path_id].append((hop_id,cluster_id,ip,isp,city,dst,province,rtt))
        else:
            tmp_dict[path_id] = [(hop_id,cluster_id,ip,isp,city,dst,province,rtt)]
    if flag:
        conn.close()
    end = time.time()
    print('get_path_sequence2 time: %s min'%((end-now)/60),)
    return tmp_dict

def get_path_sequence():
    ip_id_dict = load_cluster_table()
    conn = get_connection(cfg)
    cursor = conn.cursor()
    sql = "SELECT dst,path_id,ip,hop_id,isp FROM `tracer_hk_completed` WHERE city='西安市'"
    cursor.execute(sql)
    tmp_dict = dict()
    for dst,path_id,ip,hop_id,isp in cursor.fetchall():
        cluster_id = ip_id_dict[ip]
        if path_id in tmp_dict.keys():
            tmp_dict[path_id].append((hop_id,cluster_id,isp))
        else:
            tmp_dict[path_id] = [(hop_id,cluster_id,isp)]
    return tmp_dict

# 逆序输出cluster_id序列
def output_path_sequence():
    fw = open('output/xian.txt','w')
    ps_dict = get_path_sequence()
    for k,v in ps_dict.items():
        # 从最后一个IP向前输出
        l = list(reversed(v))
        hop_index = l[0][0]
        isp = l[0][2]
        flag = 0
        for hop_id,cluster_id,isp in l:
            while(hop_id!=hop_index):
                fw.write('*: *\t')
                hop_index-=1
            fw.write('%s: %s\t'%(hop_id,cluster_id))
            hop_index-=1
        fw.write('\t%s\n'%isp)
    fw.close()

# 将后3跳输出，看看cluster_id的数量，几个地标（路径）一个cluster_id
def output_path_sequence2db():
    ps_dict = get_path_sequence()
    full = []
    for k,v in ps_dict.items():
        # 从最后一个IP向前输出
        l = list(reversed(v))
        hop_index = l[0][0]
        isp = l[0][2]
        flag = 0
        for hop_id,cluster_id,isp in l[:4]:
            while(hop_id!=hop_index):
                flag =1
                hop_index-=1
            hop_index-=1
        if not flag:
            full.append(l)
    conn = get_connection(cfg)
    cursor = conn.cursor()
    for items in full:
        tmp = []
        for hop_id,cluster_id,isp in items[1:]:
            tmp.append(cluster_id)
        sql = "INSERT INTO `西安_last3hop_no*`(`1`,`2`,`3`) VALUES(%s,%s,%s)"%(tmp[0],tmp[1],tmp[2])
        cursor.execute(sql)


def select_trace_lm_overn(n:int=100):
    '''
    读取city_lmnum_tracer_hk_completed表中的信息，选取城市地标数量大于n的路径
    '''
    conn = get_connection(cfg)
    cursor = conn.cursor()
    cursor2= conn.cursor()
    sql = "SELECT city FROM `city_lmnum_tracer_hk_completed` WHERE lm_num>=100"
    cursor.execute(sql)
    cities = cursor.fetchall()
    stm = "INSERT INTO `tracer_hk_completed_lm100`(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp) SELECT src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp FROM `tracer_hk_completed` WHERE city=%s"
    cursor2.executemany(stm,cities)
    # for city, in cursor.fetchall():
    #     if city=='':
    #         continue
    #     stm = "INSERT INTO `tracer_hk_completed_lm100`(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp) SELECT src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp FROM `tracer_hk_completed` WHERE city='%s'"%city
    #     cursor2.execute(stm)


def select_path_random_percentn(input_table:str,output_table:str='',percent_n=[0.2,],conn=None):
    '''
    # 从tracer表中选取n%的路径做为地标路径
    '''

    if not conn:
        conn = get_connection(cfg)
    cursor = conn.cursor()
    
    sql = "SELECT DISTINCT path_id,city FROM `%s`"%input_table
    city_pathid_dict = dict()
    #cursor.execute(sql)
    r=select_from_table(input_table,sql,cfg=cfg,conn=conn,process_num=10)
    for path_id,city in r:
        if city in city_pathid_dict.keys():
            city_pathid_dict[city].append(path_id)
        else:
            city_pathid_dict[city] = [path_id,]
    for n in percent_n:
        tmp_table = str(time.time())+str(random.randint(0,1000000))
        sql = f"CREATE TABLE `{tmp_table}`(path_id int(11) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8"
        cursor.execute(sql)
        for path_ids in city_pathid_dict.values():
            num2select = int(len(path_ids)*n)
            if num2select>0 and num2select<=len(path_ids):
                selected_ids = [(x,) for x in random.sample(path_ids,num2select)]
                sql = "INSERT INTO `"+tmp_table+"`(path_id) VALUES(%s)"
                cursor.executemany(sql,selected_ids)
        output_table = input_table+"_lm_p"+str(int(n*100)) if not output_table else output_table
        print(output_table)
        cursor.execute(f"DROP TABLE IF EXISTS `{output_table}`")
        sql = f"CREATE TABLE `{output_table}` AS SELECT * FROM `{input_table}` WHERE path_id IN (SELECT path_id FROM `{tmp_table}`)"
        cursor.execute(sql)
        cursor.execute(f"DROP TABLE `{tmp_table}`")
    return output_table


def select_path_random_percentn_constraint(input_tracer_table:str,output_tracer_table):
    '''
    选取路径，要求路径最后一跳不为匿名路由，或倒数二三跳不为匿名路由
    '''
    conn = get_connection(cfg)
    cursor = conn.cursor()
    create_tracer_sql = sql_tables(output_tracer_table)
    cursor.execute(create_tracer_sql)
    path_dict = dict()
    cursor = conn.cursor()
    sql = "SELECT src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp FROM `%s` WHERE stop_reason='COMPLETED'"%input_tracer_table
    cursor.execute(sql)
    for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in cursor.fetchall():
        if path_id in path_dict.keys():
            path_dict[path_id].append((src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp))
        else:
            path_dict[path_id] = [(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp),]
    path_id_list = []
    for path_id,v in path_dict.items():
        l = list(reversed(v))
        hop_index = l[0][6] - 1
        hop_list = []
        for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in l[1:4]:
            hop_list.append(hop_id)
        if hop_list[0] == hop_index:
            path_id_list.append(path_id)
        elif hop_list[1] == hop_index-1 and hop_list[2] == hop_index-2:
            path_id_list.append(path_id)
        else:continue
    cursor = conn.cursor()
    print('output..')
    for path_id in path_id_list:
        for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in path_dict[path_id]:
            sql= "INSERT INTO `%s`(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp) VALUES('%s','%s',%s,'%s','%s',%s,%s,'%s','%s','%s','%s','%s')"%(output_tracer_table,src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp)
            cursor.execute(sql)

def combine_same_dst_path(input_tracer_table:str,output_tracer_table:str):
    if not input_tracer_table or not output_tracer_table:
        raise Exception('table is NONE in func combine_same_dst_path')
    dst_dict = dict()
    conn = get_connection(cfg)
    cursor = conn.cursor()
    cursor2 = conn.cursor()
    sql_create = sql_tables(output_tracer_table)
    cursor.execute(sql_create)
    cursor = conn.cursor()
    sql = "SELECT src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp  FROM `%s`"%input_tracer_table
    cursor.execute(sql)
    for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in cursor.fetchall():
        if dst in dst_dict.keys():
            dst_dict[dst].append((src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp))
        else:
            dst_dict[dst] = [(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp),]
    for dst,path in dst_dict.items():
        path_dict = dict()
        for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in path:
            if path_id in path_dict.keys():
                path_dict[path_id].append((src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp))
            else:
                path_dict[path_id] = [(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp),]
        best_path = get_best_path(path_dict)
        for src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp in best_path:
            sql= "INSERT INTO `%s`(src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp) VALUES('%s','%s',%s,'%s','%s',%s,%s,'%s','%s','%s','%s','%s')"%(output_tracer_table,src,dst,path_id,stop_reason,ip,rtt,hop_id,province,city,district,detail,isp)
            cursor2.execute(sql)
# 搭配combine_same_dst_path使用。优先选最后一跳存在的路径，其次是第二跳，第三跳..
# 目前定位算法利用后三跳，所以利用select_path_random_percentn_constraint先选取了后三跳有IP的路径，因此简化了此处的函数
# 只需要选择匿名路由少的路径就可以了
# 20221018 添加了优先选择完整路径的代码
def get_best_path(path_dict:dict):
    # # key = path_id, value is score
    # path_score_dict = dict()
    best_path_id = -1
    path_len = 0
    completed_pathids = set()
    for path_id,path in path_dict.items():
        stop_reason = path[0][3]
        if stop_reason.lower() == 'completed':
            completed_pathids.add(path_id)
        if len(path) > path_len:
            best_path_id = path_id
            path_len = len(path)
    # 优先选择完整路径
    if completed_pathids:
        if best_path_id not in completed_pathids:
            best_path_id = completed_pathids.pop()
    return path_dict[best_path_id]




if __name__ == '__main__':
    pass