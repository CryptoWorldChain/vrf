#!/usr/bin/python
# -*- coding:utf-8 -*-

# pip install prettytable
import requests
import time
from prettytable import PrettyTable

dposuri = "http://172.16.211.%s:38000/cks/vrf/pbvni.do"

ip = "172.16.211.%s"

cluster = ['1', '2', '3', '4', '5', '6', '7', '8']

while True:
  table = PrettyTable(['ip', 'bit_idx', 'bcuid', 'state', 'height', 'beacon', 'address'])
  for node in cluster:
    try:
      r = requests.get(dposuri % node)
      if r.status_code == 200:
        d = r.json()
        nodeInfo = d.get('cn_node', {})
        # ms = [(item.get('node_idx', 'NA')) for item in d.get('miners',{})]
        # h = d.get('current_block_height',-1)
        # nextm = None
        # for item in d.get('last_voted_round',{}).get('rounds_seq',{}):
        #   if (item.get('block_height','0') == h + 1):
        #     nextm = item.get('miner_coaddr', 'NA')

        table.add_row([(ip % node),
                       nodeInfo.get('bit_idx', 'notReady'),
                       nodeInfo.get('bcuid', 'notReady'),
                       nodeInfo.get('state', 'notReady'),
                       nodeInfo.get('cur_block', -1),
                       nodeInfo.get('beacon_hash', 'notReady'),
                       nodeInfo.get('co_address', 'notReady')
                       ])
    except IOError:
      print('node[%s] connect error' % node)
  print(table)
  time.sleep(3)
