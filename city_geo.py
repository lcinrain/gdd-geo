
from cfg import cfg_v6geo_street as cfg
from db import get_connection,select_from_table
from tables import sql_tables
from myipv6 import ipv62hexstr
import random,time
import concurrent.futures

def write_list2file(targets:list or set,file_name:str,append_LF=True):
    '''
    write list to file. append_LF default True means add '\n' at each line.
    newline is LF not CRLF
    '''
    fw = open(file_name,'w',newline = '\n')
    if append_LF:
        fw.writelines([str(x)+'\n' for x in targets])
    else:
        fw.writelines(list(targets))
    fw.close()

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


def process_geo_city3(target_dstMode_dict:dict,lm_cityMode_dict:dict,SCORE_LIST:list,geo_city_dict:dict,cityMode_dst_dict:dict,weighted_hdDistancen=False):
    '''
    为geo_city3定位部分的执行体,搭配geo_city3使用
    
    Returns:
        - data
        - targets_wait2geo
    '''
    Weights = [1-i*0.0322 for i in range(32)]
    data = []
    targets_wait2geo = set()
    SUM_SCORE = sum(SCORE_LIST)
    for targets_mode,targets in target_dstMode_dict.items():
        targets = list(targets)
        str_targets_mode = ','.join([str(x) for x in targets_mode])
        score_city_mode_list = []
         # max score
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
                if mode_score>max_mode_score:
                    max_mode_score = mode_score
                    max_mode_score_mode = city_mode
            score_city_mode_list.append((city,max_mode_score,','.join([str(x) for x in max_mode_score_mode])))
        if score_city_mode_list:
            score_city_mode_list.sort(key=lambda x:x[1],reverse=True)
            max_city,max_score,max_city_mode_str = score_city_mode_list[0]
            if max_score == 0:
                targets_wait2geo.update(targets)
                continue
            # 不同的城市score一样，根据汉明距离算最近的城市
            index = 0
            for city,score,city_mode_str in score_city_mode_list:
                if score < max_score:break
                index+=1
            '''思路1：多个城市的路径相同找到与目标相连地标的汉明距离'''
            # if index>1:#
            #     pass
            '''思路2：当前城市路径模式相同的地标中，找到最大的汉明距离，那个城市的汉明距离最大，选哪个城市'''
            if index==1:# 当最大score只对应一个城市
                for target in targets:
                    geo_city = geo_city_dict[target]
                    error = 0 if geo_city == max_city else 1
                    data.append((target,max_city_mode_str,str_targets_mode,max_city,geo_city,error,max_score))
            else:# 多个城市score相同，根据汉明距离计算
                targets_exploded = [ipv62hexstr(x) for x in targets]
                for i in range(len(targets)):
                    target = targets[i]
                    # if target == '2409:8720:601::8':
                    #     print('')
                    # else:
                    #     continue
                    city_score_list = []
                    for city,score,city_mode_str in score_city_mode_list[:index]:
                        landmarks = cityMode_dst_dict[(city,city_mode_str)]
                        landmarks = list(landmarks)
                        landmarks_exploded = [ipv62hexstr(x) for x in landmarks]
                        max_hdDistance = 0
                        max_landmark = ''
                        for j in range(len(landmarks)):
                            hdDistance_list = [a==b for a,b in zip(landmarks_exploded[j],targets_exploded[i])]
                            if weighted_hdDistancen:
                                hdDistance = sum([a*b for a,b in zip(Weights,hdDistance_list)])
                            else:
                                hdDistance = sum(hdDistance_list)
                            if hdDistance>max_hdDistance:
                                max_hdDistance = hdDistance
                                max_landmark = landmarks[j]
                        city_score_list.append((city,max_hdDistance,score,city_mode_str,max_landmark))
                    city_score_list.sort(key=lambda x:x[1])
                    max_city,_,score,city_mode_str,max_landmark = city_score_list[-1]
                    
                    geo_city = geo_city_dict[target]
                    error = 0 if geo_city == max_city else 1
                    data.append((target,city_mode_str,str_targets_mode,max_city,geo_city,error,score))
        else:
            targets_wait2geo.update(targets)
    return data,targets_wait2geo






def geo_city3(geo_table:str,lm_tracerTable,target_tracerTable,clusterTable,last_hop=-5,process_num=10,lm_path_mode=None,test_path_mode=None,tracer_dict=None,ip_cid_dict=None,conn=None):
    '''
    geolocate the targets in target_tracerTable with the landmarks in lm_tracerTable by leveraging subgraphs in clusterTable and finally output the results to geo_table.

    Parameters:
        - geo_table: output geolocation results to geo_table
        - lm_tracerTable: input landmark paths
        - target_tracerTable: targets to geolocation
        - clusterTable: the IPs used to aggregate paths in sungraphs
        - last_hop=-5: only the last 5 hops are used
        - process_num: parallel process number to accelerate geolocation process
    '''
    total_correct = 0 # geo correct
    total = 0 # total to geo
    fw_sql = open(f'./data/accuracy/{geo_table}.sql','w',encoding='utf8')
    conn = get_connection(cfg) # get mysql connection
    cursor = conn.cursor() # create cursor
    # load cluster table
    ip_cid_dict = load_cluster_table(clusterTable) if not ip_cid_dict else ip_cid_dict
    # grop geo_table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS `{geo_table}`")
    # create geo_table
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
        lm_cityMode_dict, lm_dstMode_dict, cityMode_dst_dict = get_city_mode(tracer_table=lm_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict,last_hop=last_hop)
    if not test_path_mode:
        target_cityMode_dict, target_dstMode_dict,_ = get_city_mode(tracer_table=target_tracerTable,cluster_table=clusterTable,tracer_dict=tracer_dict,ip_id_dict=ip_cid_dict,exclusive_tracerTable=lm_tracerTable,last_hop=last_hop)
    print('....')
    data = []
    SCORE_LIST = [2**i for i in range(abs(last_hop))]# 生成等比数列，例如16，8，4，2，1后面的和小于前面的数字
    SCORE_LIST.reverse()
    data,targets_wait2geo = process_geo_city3(target_dstMode_dict,lm_cityMode_dict,SCORE_LIST,geo_city_dict,cityMode_dst_dict,)
    targets_wait2geo = set()
    slices_target_dstMode_dict = split_dict(target_dstMode_dict,process_num)

    print(f'split done {geo_table}')
    excutor = concurrent.futures.ProcessPoolExecutor(max_workers=process_num)
    results = []

    for t_dict in slices_target_dstMode_dict:
        future = excutor.submit(process_geo_city3,t_dict,lm_cityMode_dict,SCORE_LIST,geo_city_dict,cityMode_dst_dict)
        results.append(future)
    
    for future in concurrent.futures.as_completed(results):
        process_data,process_targets2geo = future.result()
        data+=process_data
        targets_wait2geo.update(process_targets2geo)
    print(f'{geo_table} output...')
    for ip,lm_mode,geo_mode,city,geo_city,error,score in data:
        if error==0:
            total_correct+=1
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    total+=len(data)

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
        if error ==0: total_correct+=1
        city = city.replace("'","''")
        geo_city = geo_city.replace("'","''")
        sql = f"INSERT INTO `{geo_table}`(ip,lm_mode,geo_mode,city,geo_city,error,score) VALUES('{ip}','{lm_mode}','{geo_mode}','{city}','{geo_city}',{error},{score});\n"
        fw_sql.write(sql)
    total+=len(data)
    fw_sql.close()
    #return target_tracerTable+' done'
    return total,total_correct

def geo_city3_test_aveAcc10rounds():
    fw = open('./data/accuracy/accuracy.dhc8.lmp20.10rounds.txt','w')
    conn = get_connection(cfg)
    cursor = conn.cursor()
    sql = "show TABLES like 'tracer_city_%'"
    cursor.execute(sql)
    tables = [x for x, in cursor.fetchall() if 'lm_p' not in x]
    for table in tables:
        country = table.split('_')[-1]
        target_tracerTable = 'tracer_city_'+country
        data = []
        for i in range(0,10):
            lm_tracerTable = target_tracerTable+f'_lm_p20_r{i}'
            geo_table = 'geo_city_'+country+f'_dhc8_lm_p20_r{i}'
            print(geo_table)
            cluster_table = 'cluster_city_'+country+'_dhc8'
            select_path_random_percentn(input_table=target_tracerTable,output_table=lm_tracerTable)
            num_targets,num_correct = geo_city3(geo_table=geo_table,lm_tracerTable=lm_tracerTable,target_tracerTable=target_tracerTable,clusterTable=cluster_table,last_hop=-5)
            data.append([country,num_targets,num_correct,num_correct/num_targets,i])
            print('*'*60,num_correct,num_targets,num_correct/num_targets)
        total = 0
        total_correct = 0
        for country,num_targets,num_correct,_,_ in data:
            total+=num_targets
            total_correct+=num_correct
        data.append([country,total,total_correct,total_correct/total,'ave'])
        for j in data:
            line = '\t'.join([str(x) for x in j])
            fw.write(line+'\n')
    fw.close()

def get_city_mode(tracer_table='',cluster_table='',tracer_dict = None,ip_id_dict=None,conn=None,exclusive_tracerTable='',last_hop=-3):
    '''
    key = city, values = [mode1,mode2,mode3], mode1 = (cid1,cid2,cid3)

    Parameters:
        - last_hop=-3:using the last 3 hops to geolocate targets
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
    #return output_table

def geo_instance(n=0.2,country='china'):
    '''
    select n landmarks and geolocate the targets in country

    Parameters:
        - n: float, proportion of landmarks. 0.2 means 20%
        - country: in 26 countries
    '''
    target_tracerTable = f'tracer_city_{country}'
    lm_percentage = n*100
    lm_tracerTable = target_tracerTable+f'_lm_p{lm_percentage}'
    geo_table = 'geo_city_'+country+f'_dhc8_lm_p{lm_percentage}'
    print(geo_table)
    cluster_table = 'cluster_city_'+country+'_dhc8'
    select_path_random_percentn(input_table=target_tracerTable,output_table=lm_tracerTable,n=[n,])
    num_targets,num_correct = geo_city3(geo_table=geo_table,lm_tracerTable=lm_tracerTable,target_tracerTable=target_tracerTable,clusterTable=cluster_table,last_hop=-5)
    print(f'total targets:',num_targets,'total correct:',num_correct,'accuracy:',num_correct/num_targets)


if __name__ == '__main__':
    # select 20% of the IPv6 addresses as landmarks, 80% as targets to geolocate the targets for 10 times. This process will cost much time.
    geo_city3_test_aveAcc10rounds()
    # geo_instance(n=0.3,country='china')