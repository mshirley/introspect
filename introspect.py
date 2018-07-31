#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 21:21:23 2018

@author: user
"""

from py2neo import Graph, Node, Relationship
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

graph = Graph(user='neo4j',password='password')
graph.schema.create_uniqueness_constraint("PPID", 'ppid')
es_client = Elasticsearch()
s = Search(using=es_client).index('logstash-*')
#.filter('range' ,  **{'@timestamp': {'gte': 'now-12h' , 'lt': 'now-11h'}})


for item in s.scan():
    g = Graph()
    tx = g.begin()
    if hasattr(item, 'data'):
        if hasattr(item.data,'syscall'):
            ppid_node = Node("PPID", ppid=item.data.syscall.ppid)
            tx.merge(ppid_node, primary_label='PPID', primary_key='ppid')
            tx.create(ppid_node)
            pid_node = Node("PID", pid=item.data.syscall.pid)
            tx.merge(pid_node, primary_label='PID', primary_key='pid')
            tx.create(pid_node)
            uid_node = Node("UID_Name", uid_name=item.data.syscall.uid.name)
            #node_match = g.nodes.match("UID_Name", uid_name=item.data.syscall.uid.name).first()
            
#            if node_match:
#                user_exists = True
#            else:
#                user_exists = False
                
            tx.merge(uid_node, primary_label='UID_Name', primary_key='uid_name')
            tx.create(uid_node)
            #node_match = g.run('match (a)-[:CREATED]-()-[:FORKED]-()-[:CALLED]-(c) where a.uid_name = "{}" return c.syscall_name'.format(item.data.syscall.uid.name)).data()
            
#            if len(node_match) > 0:
#                for i in node_match:
#                    if item.data.syscall.name in i['c.syscall_name']:
#                        syscall_exists = True
#                        break
#                    else:
#                        syscall_exists = False
#            else:
#                syscall_exists = False
#                
#            if user_exists and syscall_exists:
#                for i in g.run('match (a)-[:CREATED]-()-[:FORKED]-()-[:CALLED]-(c) where a.uid_name = "{}" return c'.format(item.data.syscall.uid.name)):
#                    syscall_node = i.to_subgraph()
#                #tx.merge(d, primary_label='Syscall_Name', primary_key='syscall_name')
#            else:
            #syscall_node = Node("Syscall_Name", syscall_name=item.data.syscall.name)
            #tx.create(syscall_node)
                
            proctitle_node = Node("Proctitle", proctitle=item.data.proctitle)
            tx.merge(proctitle_node, primary_label='Proctitle', primary_key='proctitle')
            tx.create(proctitle_node)
            uid_ppid = Relationship(uid_node, "CREATED", ppid_node)
            ppid_pid = Relationship(ppid_node, "FORKED", pid_node)
            #pid_syscall = Relationship(pid_node, "CALLED", syscall_node)
            #syscall_proctitle = Relationship(syscall_node, "EXECUTED", proctitle_node)
            pid_proctitle = Relationship(pid_node, "EXECUTED", proctitle_node)
            tx.create(uid_ppid)
            tx.create(ppid_pid)
            tx.create(pid_proctitle)
            #tx.create(pid_syscall)
            #tx.create(syscall_proctitle)
            tx.commit()           