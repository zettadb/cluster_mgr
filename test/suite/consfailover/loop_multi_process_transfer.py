#!/usr/bin/env python2

import psycopg2
import mysql.connector
import argparse
import logging
import re
import time
import random
import threading
import os

def get_mysql_conn_by_meta_host(meta_hosts, meta_user, meta_passwd):
    mhost_vec = re.split(r';', meta_hosts)
    for it in mhost_vec:
        ipport = re.split(r':', it)

        try:
            mysql_conn_params = {}
            mysql_conn_params['host'] = ipport[0]
            mysql_conn_params['port'] = ipport[1]
            mysql_conn_params['user'] = meta_user
            mysql_conn_params['password'] = meta_passwd
            mysql_conn_params["database"] = 'kunlun_metadata_db'
            mysql_conn = mysql.connector.connect(**mysql_conn_params)
        except mysql.connector.errors.InterfaceError as err:
            print "Unable to connect to {}, error: {}".format(str(mysql_conn_params), str(err))
            continue
        
        return mysql_conn

def stop_shard_master_mysqld(mysql_conn, shardid, clusterid, storage_bin_path, storage_package_name):
    sql_stmt="select hostaddr, port from shard_nodes where shard_id={shard_id} and db_cluster_id={cluster_id} and member_state='source'".format(
        shard_id=shardid, cluster_id=clusterid
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    hostaddr=row[0]
    master_port=row[1]

    sql_stmt="select datadir, nodemgr_port from server_nodes where hostaddr='{addr}' and machine_type='storage'".format(
        addr=hostaddr
    )
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    datadir = row[0]
    nodemgr_port=row[1]

    for i in range(1, 5):
        stop_cmd="cd {path}/instance_binaries/storage/{pt}/{pn}/dba_tools; sh ./stopmysql.sh {port}".format(
            path=storage_bin_path, pt=master_port, pn=storage_package_name, port=master_port
        )
        cmd="kunlun-test/util/test_client -host={hs} -port={pt} -cmd='{kc}'".format(
            hs=hostaddr, pt=nodemgr_port, kc=stop_cmd
        )
        #print cmd
        
        logging.info(cmd)
        os.system(cmd)
        time.sleep(1)

def set_shard_master_read_only(mysql_conn, shardid, clusterid):
    sql_stmt="select hostaddr, port from shard_nodes where shard_id={shard_id} and db_cluster_id={cluster_id} and member_state='source'".format(
        shard_id=shardid, cluster_id=clusterid
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    hostaddr=row[0]
    master_port=row[1]

    try:
        mysql_conn_params = {}
        mysql_conn_params['host'] = str(hostaddr)
        mysql_conn_params['port'] = str(master_port)
        mysql_conn_params['user'] = 'clustmgr'
        mysql_conn_params['password'] = 'clustmgr_pwd'
        shard_conn = mysql.connector.connect(**mysql_conn_params)
    except mysql.connector.errors.InterfaceError as err:
        print "Unable to connect to {}, error: {}".format(str(mysql_conn_params), str(err))
        return
    
    sql = "set global super_read_only=true"
    logging.info("set shard master read_only sql: " + sql)
    shard_cursor = shard_conn.cursor(prepared=True)
    shard_cursor.execute(sql)

def kill_shard_master_mysqld(mysql_conn, shardid, clusterid):
    sql_stmt="select hostaddr, port from shard_nodes where shard_id={shard_id} and db_cluster_id={cluster_id} and member_state='source'".format(
        shard_id=shardid, cluster_id=clusterid
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    hostaddr=row[0]
    master_port=row[1]

    sql_stmt="select datadir, nodemgr_port, nodemgr_bin_path from server_nodes where hostaddr='{addr}' and machine_type='storage'".format(
        addr=hostaddr
    )
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    datadir = row[0]
    nodemgr_port=row[1]
    nodemgr_bin_path=row[2]

    for i in range(1, 5):
        kill_cmd="cd {path}; ./util/safe_killmysql {pt} {dd}".format(
            path=nodemgr_bin_path, pt=master_port, dd=datadir
        )
        cmd="kunlun-test/util/test_client -host={hs} -port={pt} -cmd='{kc}'".format(
            hs=hostaddr, pt=nodemgr_port, kc=kill_cmd
        )
        #print cmd
        
        logging.info(cmd)
        os.system(cmd)
        time.sleep(1)

def kill_mysqld_random_time(mysql_conn, cluster_id, storage_bin_path, storage_package_name, timeout):
    sql_stmt = "select id from shards where db_cluster_id={di}".format(
        di=cluster_id
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    ids = mysql_cursor.fetchall()
    slp_cnt = random.randint(0,timeout)
    time.sleep(slp_cnt)

    id_pos = random.randint(0, len(ids)-1)
    shardid = ids[id_pos][0]

    rand_type = random.randint(0,2)
    if rand_type == 0:
        stop_shard_master_mysqld(mysql_conn, shardid, cluster_id, storage_bin_path, storage_package_name)
    elif rand_type == 1:
        set_shard_master_read_only(mysql_conn, shardid, cluster_id)
    else:
        kill_shard_master_mysqld(mysql_conn, shardid, cluster_id)

    #for sid in ids:
    #    shardid = sid[0]
    #    rand_type = random.randint(0,2)
    #    if rand_type == 0:
    #        stop_shard_master_mysqld(mysql_conn, shardid, cluster_id, storage_bin_path, storage_package_name)
    #    elif rand_type == 1:
    #        set_shard_master_read_only(mysql_conn, shardid, cluster_id)
    #    else:
    #        kill_shard_master_mysqld(mysql_conn, shardid, cluster_id)

def kill_pg_process(mysql_conn, comp_host, cluster_id):
    ipport = re.split(r'_', comp_host)
    hostaddr=ipport[0]
    port=ipport[1]

    sql_stmt="select comp_datadir, nodemgr_port, nodemgr_bin_path from server_nodes where hostaddr='{addr}' and machine_type='computer'".format(
        addr=hostaddr
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    comp_datadir = row[0]
    nodemgr_port=row[1]
    nodemgr_bin_path=row[2]
    
    kill_cmd = "cd {bp}; ./util/kill_pg.sh {pt} {cp}".format(
        bp=nodemgr_bin_path, pt=port, cp=comp_datadir
    )
    
    cmd="kunlun-test/util/test_client -host={hs} -port={pt} -cmd='{kc}'".format(
        hs=hostaddr, pt=nodemgr_port, kc=kill_cmd
    )

    #print cmd
    logging.info(cmd)
    os.system(cmd)

def stop_pg_process(mysql_conn, comp_host, cluster_id, comp_bin_path, comp_package_name):
    ipport = re.split(r'_', comp_host)
    hostaddr=ipport[0]
    port=ipport[1]

    sql_stmt="select comp_datadir, nodemgr_port from server_nodes where hostaddr='{addr}' and machine_type='computer'".format(
        addr=hostaddr
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    comp_datadir = row[0]
    nodemgr_port=row[1]

    stop_cmd = "cd {cb}/instance_binaries/computer/{pt}/{pn}/bin; ./pg_ctl -D {d_dir}/{pt1} stop".format(
        cb=comp_bin_path, pt=port, pn=comp_package_name, d_dir=comp_datadir, pt1=port
    )

    cmd="kunlun-test/util/test_client -host={hs} -port={pt} -cmd='{sc}'".format(
        hs=hostaddr, pt=nodemgr_port, sc=stop_cmd
    )
    #print cmd
    logging.info(cmd)
    os.system(cmd)


def kill_pg_random_time(meta_hosts, comp_nodes, cluster_id, comp_bin_path, comp_package_name, timeout): 
    slp_cnt = random.randint(0,timeout)
    time.sleep(slp_cnt)

    mysql_conn = get_mysql_conn_by_meta_host(meta_hosts, 'clustmgr', 'clustmgr_pwd')
    rand_type = random.randint(0,1)
    rand_pos = random.randint(0, len(comp_nodes)-1)
    if rand_type == 0:
        stop_pg_process(mysql_conn, comp_nodes[rand_pos], cluster_id, comp_bin_path, comp_package_name)
    else:
        kill_pg_process(mysql_conn, comp_nodes[rand_pos], cluster_id)

def kill_cluster_mgr_random_time(meta_hosts, cm_bin_path, timeout):
    slp_cnt = random.randint(0,timeout)
    time.sleep(slp_cnt)

    mysql_conn = get_mysql_conn_by_meta_host(meta_hosts, 'clustmgr', 'clustmgr_pwd')
    sql_stmt = "select hostaddr from cluster_mgr_nodes where member_state='source'"
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    hostaddr = row[0]

    sql = "select nodemgr_bin_path, nodemgr_port from server_nodes where hostaddr='{ht}'".format(
        ht=hostaddr
    )
    mysql_cursor.execute(sql)
    row = mysql_cursor.fetchone()
    nodemgr_bin_path = row[0]
    nodemgr_port = row[1]

    kill_cmd = "cd {bp}; ./util/kill_cm.sh {cb}".format(
        bp=nodemgr_bin_path, cb=cm_bin_path
    )

    cmd="kunlun-test/util/test_client -host={hs} -port={pt} -cmd='{kc}'".format(
        hs=hostaddr, pt=nodemgr_port, kc=kill_cmd
    )
    #print cmd
    logging.info(cmd)
    os.system(cmd)


def transfer_between_accounts(pg_conn, delta):
    cur = pg_conn.cursor()
    mid_pos = random.randint(1, 50000)
    first_pos = random.randint(1, mid_pos)
    second_pos = random.randint(mid_pos, 50000)
    if first_pos == second_pos:
        second_pos = random.randint(1, 50000)

    logging.info("first_pos: "+str(first_pos)+" second_pos: "+str(second_pos))
    if first_pos != second_pos:
        try:
            sql_stmt = "select money from transfer_account where id={id}".format(
                id=str(first_pos)
            )
            cur.execute(sql_stmt)
            row = cur.fetchone()
            money = int(row[0])
            if money < delta:
                logging.info("first_pos: "+str(first_pos)+" left money: "+str(money))
                return

            cur.execute("start transaction")
            fsql_stmt = "update transfer_account set money=money-{dt} where id={id}".format(
                dt=str(delta), id=str(first_pos)
            )
            cur.execute(fsql_stmt)
            logging.info(fsql_stmt)
            ssql_stmt = "update transfer_account set money=money+{dt} where id={id}".format(
                dt=str(delta), id=str(second_pos)
            )
            cur.execute(ssql_stmt)
            logging.info(ssql_stmt)
            cur.execute("commit")
            cur.close()
            return 0
        except (Exception, psycopg2.Error) as error:
            logging.error("Error update transfer_account for postgres: "+str(error))
            cur.close()
            pg_conn.close()
            return 1

def transfer_account_thd(pg_host, timeout):
    ipport = re.split(r'_', pg_host)
    hostaddr=ipport[0]
    port=ipport[1]

    pg_conn = psycopg2.connect(host=hostaddr, port=port, user='abc', database='postgres', password='abc')
    while True:
        res = transfer_between_accounts(pg_conn, 50)
        if res == 1:
            pg_conn = psycopg2.connect(host=hostaddr, port=port, user='abc', database='postgres', password='abc')
        
        time.sleep(1)
        timeout = timeout - 1
        if timeout < 0:
            break

def check_total_account_money(mysql_conn, clusterid, total_money):
    sql_stmt = "select hostaddr, port from comp_nodes where db_cluster_id={di} limit 1".format(
        di=clusterid
    )
    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    row = mysql_cursor.fetchone()
    hostaddr=row[0]
    port=row[1]
    money=0

    while True:        
        try:
            pg_conn = psycopg2.connect(host=hostaddr, port=port, user='abc', database='postgres', password='abc')
            cur = pg_conn.cursor()
            sql = "select sum(money) from transfer_account"
            cur.execute(sql)
            row = cur.fetchone()
            money = int(row[0])
            logging.info("current total account money: "+str(money))
            cur.close()
            break
        except (Exception, psycopg2.Error) as error:
            logging.error("Error get sum money of transfer_account for postgres: "+str(error))
            time.sleep(1)

    if money == total_money:
        return 0

    return 1

def get_comps_by_meta(mysql_conn, clusterid):
    sql_stmt = "select hostaddr, port from comp_nodes where db_cluster_id={di}".format(
        di=clusterid
    )

    mysql_cursor = mysql_conn.cursor(prepared=True)
    mysql_cursor.execute(sql_stmt)
    rows = mysql_cursor.fetchall()
    
    comp_nodes = []
    for row in rows:
        hostaddr = row[0]
        port = row[1]

        comp_nodes.append(hostaddr+"_"+str(port))

    return comp_nodes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Loop transfer between accounts")
    parser.add_argument('--meta_hosts', type=str, required=True, help='meta mysql iplists')
    parser.add_argument('--thread_num', type=int, required=True, help='connect pg update thread num')
    parser.add_argument('--clusterid', type=str, required=True, help='cluster id')
    parser.add_argument('--timeout', type=int, required=True, help='run time out')
    parser.add_argument('--total_money', type=int, required=True, help='total money')
    parser.add_argument('--storage_instance_path', type=str, required=True, help='storage bin running path')
    parser.add_argument('--storage_package_name', type=str, required=True, help='storage package name')
    parser.add_argument('--computer_instance_path', type=str, required=True, help='computer bin running path')
    parser.add_argument('--computer_package_name', type=str, required=True, help='computer package name')
    parser.add_argument('--cluster_mgr_path', type=str, required=True, help='cluster mgr running path')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        filename="./loop_multi_process_transfer.log",
                        filemode='a',
                        format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    
    mysql_conn = get_mysql_conn_by_meta_host(args.meta_hosts, 'clustmgr', 'clustmgr_pwd')
    comp_nodes = get_comps_by_meta(mysql_conn, args.clusterid)
    len_nodes = len(comp_nodes) - 1
    total_time = args.timeout
    delta_time = 10
    threads = []
    while True:
        pos = 0
        for i in range(args.thread_num):
            logging.info("i: "+str(i)+", pos: "+str(pos))
            comp_node=comp_nodes[pos]
            if pos >= len_nodes:
                pos = 0
            else:
                pos = pos + 1

            th = threading.Thread(target=transfer_account_thd, args=[comp_node, delta_time+40,])
            th.start()
            threads.append(th)

        kill_pg_th = threading.Thread(target=kill_pg_random_time, args=[args.meta_hosts, comp_nodes, args.clusterid, args.computer_instance_path, args.computer_package_name, delta_time,])
        kill_pg_th.start()
        threads.append(kill_pg_th)

        kill_cm_th = threading.Thread(target=kill_cluster_mgr_random_time, args=[args.meta_hosts, args.cluster_mgr_path, delta_time,])
        kill_cm_th.start()
        threads.append(kill_cm_th)

        kill_mysqld_random_time(mysql_conn, args.clusterid, args.storage_instance_path, args.storage_package_name, delta_time)

        #kill_pg_random_time(mysql_conn, comp_nodes, args.clusterid, args.computer_instance_path, args.computer_package_name, delta_time)

        for th in threads:
            th.join()
        #th.join()

        res = check_total_account_money(mysql_conn, args.clusterid, args.total_money)
        if res == 1:
            break
        
        total_time = total_time - delta_time
        logging.info("left timeout: "+str(total_time))
        if total_time < 0:
            break