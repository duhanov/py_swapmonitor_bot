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
import traceback


arg_parser = argparse.ArgumentParser(
                    prog = 'CakeSwapParser',
                    description = 'parse blocks for swap and change',
                    epilog = 'start_block=start_block_number')

#arg_parser.add_argument('--from_block') 
arg_parser.add_argument('--cmd') 
arg_parser.add_argument('--tx') 
arg_parser.add_argument('--start_block') 
arg_parser.add_argument('--end_block') 

args = arg_parser.parse_args()

#23516200
#https://explorer.bitquery.io/ru/bsc/block/23516200
#1 Dec 2022

parser = Parser()
#parser.parseFromBlock(int(args.from_block))




parser.start_time = time.time()

#parser.parseTx("0x29a7e11fb25eb4e3cbebe829b6ec3b640ffc398001ada2766b642a48fb89745a")

#try:
if args.cmd == "tx":
	parser.parseTx(args.tx)

	

if args.cmd == "parse":
	#1Dec
	#start_block = 23516200
	#27dec
#	end_block = 24269598

	#start_block = 23551511
	start_block = int(args.start_block)
	end_block = int(args.end_block)

	print("Start parse blocks from " + str(start_block) + " to " + str(end_block))

	block_n = start_block
	block_delta = 200



	while block_n <= end_block and block_delta > 0:
		parser_blocks = False
		error_sleep_time = 1
		while not parser_blocks:
			try:
				if end_block-block_n < block_delta:
					block_delta = end_block - block_n
				print("block_delta=" + str(block_delta))
				parser.parseTxs(block_n, block_n+block_delta)
				block_n = block_n + block_delta
				parser_blocks = True
			except Exception as exc:
				print("ERROR")
				traceback.print_exc()
				print(exc)

				print("Sleep " + str(error_sleep_time) + "s")
				time.sleep(error_sleep_time)
				error_sleep_time += 1
				print("Try again...")


#except Exception as exc:
#	print("ERROR!")
#	print(exc)

print("Ended.")
#print(r.json())



