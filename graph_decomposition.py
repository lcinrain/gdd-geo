from igraph import *
from cfg import cfg_v6geo_street as cfg
from db import get_connection,select_from_table
import time,queue
from myipv6 import hexstr2ipv6,ipv62hexstr
from tables import sql_tables
import concurrent.futures


def graph_decomposition_delay(output_cluster_table:str,g:Graph = None,graph_dir:str='',weight:float=1):
    '''
    decompose graph according to delay

    Parameters:
        - output_cluster_table: decomposition result
        - g: input graph, default None.
    '''
    if not g:
        if not graph_dir:
            raise Exception('graph is None')
        g = Graph.Read_GraphMLz(graph_dir)
    conn = get_connection(cfg)
    cursor = conn.cursor()
    sql_create = sql_tables(output_cluster_table)
    cursor.execute(sql_create)
    cursor.close()
    cursor = conn.cursor()
    cluster_id = 0
    delete_edge_indices = []
    print(len(g.components()))
    for edge in g.es:
        index = edge.index
        if edge["weight"] > weight:
            delete_edge_indices.append(index)
    print('delete num',len(delete_edge_indices))
    g.delete_edges(delete_edge_indices)
    print('delete done')
    cs = g.components()
    print(len(cs))
    for component in cs:
        for index in component:
            ipv6 = g.vs[index]["name"]
            sql = "INSERT INTO `%s`(ip,cluster_id,ip_count) VALUES('%s',%s,%s)"%(output_cluster_table,ipv6,cluster_id,len(component))
            cursor.execute(sql)
        cluster_id+=1

def graph_dual_decomponent(input_cluster_table:str,output_cluster_table:str,relation_tables:list,graph_dir:str='',weight:float=1):
    '''
    decompose subgraphs afer graph_decomposition_dhc

    Parameters:
        - relation_tables: list, [table1,...] 
    '''
    cid = 0 # cluster_id
    conn = get_connection(cfg)
    cursor2 = conn.cursor()
    sql = sql_tables(output_cluster_table)
    cursor2.execute(sql)
    cursor = conn.cursor()
    
    relation_dict = {}
    nodes = set()
    for relation_table in relation_tables:
        sql = f"SELECT pcs,scs,delay FROM {relation_table}"
        cursor.execute(sql)
        r = cursor.fetchall()

        for pcs,scs,delay in r:
            if delay < 0: continue
            relation_dict[(pcs,scs)] = delay
            relation_dict[(scs,pcs)] = delay
            nodes.add(pcs)
            nodes.add(scs)
        
        neighbor_dict = {k:set() for k in nodes}
        for pcs,scs,delay in r:
            if delay < 0: continue
            neighbor_dict[pcs].add(scs)
            neighbor_dict[scs].add(pcs)
    
    sql = f"SELECT max(cluster_id) FROM {input_cluster_table}"
    cursor.execute(sql)
    max_id = cursor.fetchall()[0][0]
    cluster_dict = {k:[] for k in range(max_id+1)}
    sql = f"SELECT ip,cluster_id FROM {input_cluster_table}"
    cursor.execute(sql)
    for ip,cluster_id in cursor.fetchall():
        cluster_dict[cluster_id].append(ip)

    singles = []
    for k,cluster in cluster_dict.items():
        edges = [] # 边
        vs = set() # 节点
        weights = []
        if len(cluster) < 2:
            singles.append(cluster[0])
            continue
        for ip in cluster:
            vs.add(ip)
            try: 
                neighbors_ip = neighbor_dict[ip]
            # cluster中的孤立节点，没有连接
            except KeyError:
                continue
            intersection_ = set(neighbors_ip).intersection(set(cluster))
            if intersection_: # 如果有交集说明存在一条边
                for ip2 in intersection_:
                    edges.append((ip,ip2))
                    vs.add(ip2)
        weights = [relation_dict[(pcs,scs)] for pcs,scs in edges]
        # 如果由dhc得到的cluster直接没有连接关系，则直接输出
        if not weights:
            for ip in cluster:
                sql = "INSERT INTO `%s`(ip,cluster_id,ip_count,is_divided) VALUES('%s',%s,%s,0)"%(output_cluster_table,ip,cid,len(cluster)) # 0 表示拓扑子图没有经过时延的再分割
                cursor2.execute(sql)
            cid+=1
            continue
        g = Graph()
        vs = list(vs)
        node_dict = dict(zip(vs,list(range(len(vs)))))

        g.add_vertices(len(vs))
        g.vs["name"] = vs # set the name attribute for all vertices
        edges_index = []
        for pcs,scs in edges:
            edges_index.append((node_dict[pcs], node_dict[scs]))
        g.add_edges(edges_index)
        g.es["weight"] = weights
        g = g.as_undirected()
        summary(g)
        for edge in g.es:
            index = edge.index
            if edge["weight"] > weight:
                g.delete_edges(index)
        components = g.components()
        if len(components) == 1:
            is_divided = 1
        else:
            is_divided = 2
        for component in components:
            for index in component:
                ipv6 = g.vs[index]["name"]
                sql = "INSERT INTO `%s`(ip,cluster_id,ip_count,is_divided) VALUES('%s',%s,%s,%s)"%(output_cluster_table,ipv6,cid,len(component),is_divided)
                cursor2.execute(sql)
            cid+=1
    for ip in singles:
        sql = "INSERT INTO `%s`(ip,cluster_id,ip_count,is_divided) VALUES('%s',%s,%s,0)"%(output_cluster_table,ip,cid,len(cluster)) # 0 表示拓扑子图没有经过时延的再分割
        cursor2.execute(sql)
        cid+=1

def get_free_dimension(hitlist_exploded:list):
    '''
return free/variable dimensions and corresponding values(nybbles) in hitlist

Parameters:
    - hitlist_exploded:list. hex str format IPv6 without colon. e.g. an element in list is '20010db8000000000000000000000000'

Returns: 
    - return the first/leftmost free dimension and corresponding values(nybbles);
    
    '''
    free_dimension = dict()
    for i in range(0,32):
        col_i_values = [hitlist_exploded[j][i] for j in range(0,len(hitlist_exploded))]
        col_i_values = list(set(col_i_values))
        if len(col_i_values) > 1:
            return i,col_i_values
    return free_dimension

def leftmost(hitlist_exploded:list):
    free_dimension, variables = get_free_dimension(hitlist_exploded)
    nybble_seeds_dict = dict()
    # initialize dict according to keys(nybbles)
    for nybble in variables:
        nybble_seeds_dict[nybble] = []
    # 根据该维度的自由维度值聚类种子
    for ip in hitlist_exploded:
        nybble_seeds_dict[ip[free_dimension]].append(ip)
    return nybble_seeds_dict

def space_partition(exploded_hitlist:list,th=16,func=leftmost):
    '''
Parameters:
    - exploded_hitlist: list of exploded ipv6 hex str without ':'. e.g. 20010db8000000000000000000000000. its compressed format is 2001:db8::
    '''
    s = time.time()
    seed_regions = []
    q = queue.Queue()
    q.put(exploded_hitlist)
    while not q.empty():
        node = q.get()
        if len(node) <= th:
            seed_regions.append(node)
        else:
            new_nodes = func(node).values()
            for new_node in new_nodes:
                q.put(new_node)
    print(func.__name__,'space partition done, th=',th,'seed num',len(exploded_hitlist),'time cost',time.time()-s)
    print('*'*10)
    return seed_regions

def graph_decomposition_dhc(input_tracer_table='',output_cluster_table='',th=8,input_ips=None):
    """
    将tracer表中路径IP进行聚类

    Parameters:
        - input_tracer_table: input table name
        - output_cluster_table: output table name
        - th: threshold in dhc
    """
    conn = get_connection(cfg)
    cursor = conn.cursor()
    if not output_cluster_table:
        raise Exception('cluster table is None')
    cursor.execute(f"DROP TABLE IF EXISTS `{output_cluster_table}`")
    sql = sql_tables(output_cluster_table)
    cursor.execute(sql)
    if not input_ips:
        ipv6_list = set()
        sql = f"SELECT MAX(id) FROM `{input_tracer_table}`"
        cursor.execute(sql)
        max_id = cursor.fetchone()[0]
        interval = 100000 # 每次查10w条
        end_range = int(max_id/interval)+1
        for i in range(end_range):
            start = i*interval
            end = (i+1)*interval
            sql = f"SELECT DISTINCT ip FROM `{input_tracer_table}` WHERE id>{start} and id<={end}"
            cursor.execute(sql)
            ipv6_list.update([ip for ip, in cursor.fetchall()])
    else:
        ipv6_list = input_ips
        
    ipv6_list = [ipv62hexstr(x) for x in ipv6_list]
    new_clusters = space_partition(ipv6_list,th=th)
    cursor = conn.cursor()
    id = 0
    for cluster in new_clusters:
        ip_count = len(cluster)
        for ip in cluster:
            ipv6 = hexstr2ipv6(ip)
            sql = "INSERT INTO `%s`(ip,cluster_id,ip_count) VALUES('%s',%s,%s)"%(output_cluster_table,ipv6,id,ip_count)
            cursor.execute(sql)
        id+=1
    return f'{output_cluster_table} done.'

def graph_decomposition_dhc_multiprocess():
    '''
    dhc for each country with different thresholds
    '''
    countries = ['china',]
    for country in countries:
        tracer_table = f'tracer_city_{country}'
        conn = get_connection(cfg)

        sql = f"SELECT DISTINCT ip FROM `{tracer_table}`"
        rs = select_from_table(tracer_table,sql,conn=conn,process_num=10)
        ips = [x for x, in rs]
        ips = set(ips)
        print(len(ips))
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=20)
        results = []
        ths = [1]+[i for i in range(5,65,5)]
        
        for th in ths:
            cluster_table = f'cluster_city_{country}_dhc{th}'
            #graph_decomposition_dhc('',cluster_table,th,ips)
            future = executor.submit(graph_decomposition_dhc,'',cluster_table,th,ips)
            results.append(future)
            print(th)

        print('*'*60)
        concurrent.futures.wait(results)
        for future in results:
            result = future.result()
            print(result)

if __name__ == '__main__':
    graph_decomposition_dhc_multiprocess()
