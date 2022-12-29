#https://explorer.bitquery.io/ru/bsc/block/23516200
import telebot
from telebot import types
import json
from web3 import Web3
import asyncio
import threading
import time
from threading import Thread

from parser import *

import argparse
from datetime import timedelta
import requests

arg_parser = argparse.ArgumentParser(
                    prog = 'CakeSwapParser',
                    description = 'parse blocks for swap and change',
                    epilog = 'from_block=start_block_number')

#arg_parser.add_argument('--from_block') 
arg_parser.add_argument('--cmd') 
arg_parser.add_argument('--tx') 
arg_parser.add_argument('--from_block') 

args = arg_parser.parse_args()

#23516200
#https://explorer.bitquery.io/ru/bsc/block/23516200
#1 Dec 2022

parser = Parser()
#parser.parseFromBlock(int(args.from_block))




start_time = time.time()

#parser.parseTx("0x29a7e11fb25eb4e3cbebe829b6ec3b640ffc398001ada2766b642a48fb89745a")

#try:
if args.cmd == "tx":
	parser.parseTx(args.tx)

if args.cmd == "parse":
	#1Dec
	#start_block = 23516200
	#27dec
	end_block = 24269598

	#start_block = 23551511
	start_block = int(args.from_block)

	print("Start parse blocks from " + args.from_block)
	print(args.from_block)

	block_n = start_block
	block_delta = 200

	while start_block <= end_block:
		print("Parse bocks: " + str(block_n) + "-" +  str(block_n+block_delta) + "(" + str(block_delta) + ")")
		url = 'https://api.bscscan.com/api?module=account&action=txlist&address=0x10ed43c718714eb63d5aa57b78b54704e256024e&startblock=' + str(block_n) + '&endblock=' + str(block_n+block_delta) + '&page=1&offset=0&sort=asc&apikey=J3NSSIP3WKM3PMSIXW6YG1DYTBM3NAW25H'
		print(url)
		r = requests.get(url)
		txs = r.json()["result"]
		print("Count transactions: " + str(len(txs)))
		tx_n = 0
		for tx in txs:
			tx_n +=1
			tx_parsed = False
			error_sleep_time = 1
			while not tx_parsed:
				try:
					print("parseTx " + str(tx_n) + "/" + str(len(txs)) + " " + tx["hash"] + " (blocks " + str(block_n) + "-" + str(block_n+block_delta) + ")")
					parser.parseTx(tx["hash"])
					tx_parsed = True
				except Exception as exc:
					print("ERROR")
					print(exc)
					print("Sleep " + str(error_sleep_time) + "s")
					time.sleep(error_sleep_time)
					error_sleep_time += 1
					print("Try again...")
		end_time = time.time()

		block_n = block_n + block_delta
		print("Parsed blocks: " + str(start_block) + "-" +  str(block_n) + "(" + str(block_n-start_block + 1) + "). Txs: " + str(len(txs)) + " Work time: " + str(timedelta(seconds = int(end_time-start_time))))

#except Exception as exc:
#	print("ERROR!")
#	print(exc)

print("Ended.")
#print(r.json())



