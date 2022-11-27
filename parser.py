from pathlib import Path
import os
import json
import time


from web3 import Web3
from web3.middleware import geth_poa_middleware

from threading import Thread

class Parser:
	web3 = Web3()
	config = json.load(open('config.json'))

	#Остановить парсинг блоков на этом блоке
	stop_parse_block = 0
	parser_progress = False

	def __init__(self):
		print("init")
		self.web3 = Web3(Web3.HTTPProvider(self.config["provider"], request_kwargs={'timeout': 12000}))
		self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
		#config = json.load(open('config.json'))

		#Остановить парсинг блоков на этом блоке
		self.stop_parse_block = self.web3.eth.get_block('latest')["number"]





	#Сохранить значение инвестиций
	def add_amount(self, token, address, amount, hash, source):
		total = 0
		fname = "accounts/" + token["name"] + "_" + address + ".json"
		path = Path(fname)
		if path.is_file():
			data = json.load(open(fname))
		else:
			data = {"total": 0, "txs": []}
		
		tx_found = False
		for tx in data["txs"]:
			if tx["hash"] == hash:
				tx_found = True
				break
		#Если еще не обрабатывали эту транзакцию
		if tx_found:
			print("TX ALREADY FOUND!")
		else:
			data["total"] = data["total"] + amount
			data["txs"].append({"time": time.time(), "amount": amount, "source": source, "hash": hash})
			with open(fname, 'w') as f:
				json.dump(data, f)



	def handle_event(self, token, event):
		data =  json.loads(Web3.toJSON(event))	
		print("HANDLE EVENT")
		print(data)
		if data["args"]["to"] == self.config["pool"]:
			print("EVENT! " + token["name"] + " send to pool " + str(data["args"]["value"]) + ' from ' + data["args"]["from"])
			self.add_amount(data["args"]["from"], data["args"]["value"], data["transactionHash"], "event")
			self.saveLastParsedBlock(data["blockNumber"])
		#remove from pool
		elif data["args"]["from"] == self.config["pool"]:
			print("EVENT! " + token["name"] + " remove from pool " + str(data["args"]["value"]) + ' by ' + data["args"]["to"])
			self.add_amount(data["args"]["to"], -1 * data["args"]["value"], data["transactionHash"], "event")
			self.saveLastParsedBlock(data["blockNumber"])


	def listen_events(self):
		for token in self.config["tokens"]:
			print("t")
			thread = Thread(target=self.listen_events_token, args=(token,))
			thread.start()
			print("Subscribe events for " + token["name"])

	def listen_events_token(self, token):
		print("Listen EVENTS " + token["name"] + " " + token["contract"])
		web3 = Web3(Web3.HTTPProvider(self.config["events_provider"], request_kwargs={'timeout': 12000}))
		contract = web3.eth.contract(address=Web3.toChecksumAddress(token["contract"]), abi=json.load(open('pair_abi.json')))

		event_filter = contract.events.Transfer.createFilter(fromBlock='latest')

		try:
			while True:
				for item in event_filter.get_new_entries():
					self.handle_event(token, item)
				print("Sleep " + str(self.config["sleep"]) + " seconds")
				time.sleep(self.config["sleep"])
		except Exception as exc:
			print("ERROR! Web3 exception " + token["name"])
			print(exc)
			print("Reconnect...")
			self.listen_events_token(token)




	def getLastParsedBlock(self):
		r = 0
		fname = "lastblock.txt"
		if Path(fname).is_file():
			with open(fname, 'r') as f:
				r = int(f.read())
		else:			
			r = self.getLastBlock()
			print("PARSER. Start from getLastBlock(): " + str(r))
		return r

	def getLastBlock(self):
		block = self.web3.eth.get_block('latest')
		return block["number"]


	def saveLastParsedBlock(self, n):
		fname = "lastblock.txt"
		with open(fname, 'w') as f:
			f.write(str(n))

	def parseTx(self, block, tx_hash):
		#print("tx " + tx_hash)
		tx = self.web3.eth.get_transaction(tx_hash)
		receipt = self.web3.eth.get_transaction_receipt(tx_hash)
#		print(receipt)
		for log in receipt["logs"]:
			found_abi = True
			#Если есть перевод на адрес пула
			if log["address"] == self.config["pool"]:
				abi = json.load(open('pool_abi.json'))
				contract = self.web3.eth.contract(self.config["pool"], abi=abi)
			else:
				found_abi = False
			if found_abi:
				receipt_event_signature_hex = self.web3.toHex(log["topics"][0])
				abi_events = [abi for abi in contract.abi if abi["type"] == "event"]
				for event in abi_events:
					if event["name"] == "Transfer":
						# Get event signature components
						name = event["name"]
						inputs = [param["type"] for param in event["inputs"]]
						inputs = ",".join(inputs)
						# Hash event signature
						event_signature_text = f"{name}({inputs})"
						event_signature_hex = self.web3.toHex(self.web3.keccak(text=event_signature_text))
						# Find match between log's event signature and ABI's event signature
						if event_signature_hex == receipt_event_signature_hex:
	    					# Decode matching log
							print("decode log..")
							decoded_logs = {}
							try:
								decoded_logs = contract.events[event["name"]]().processReceipt(receipt)
							except Warning:
								print("Warn ABI")
							for l in decoded_logs:
								for token in self.config["tokens"]:
									#Если вызов смартконтракта нашего токена
									if l["address"] == token["contract"]:
										#Перевели в пул
										if l["args"]["to"] == self.config["pool"]:
											print("Send  " + str(l["args"]["value"]) + " " + token["name"] + " TO Pool, from " + l["args"]["from"])
											print(l)
											self.add_amount(token, l["args"]["from"], l["args"]["value"], l["transactionHash"].hex(), "parser")

										elif l["args"]["from"] == self.config["pool"]:
											print("Back " +str(l["args"]["value"]) + " " + token["name"] + " from POOL, by " +  l["args"]["to"])
											print(l)
											self.add_amount(token, l["args"]["to"], -1 * l["args"]["value"], l["transactionHash"].hex(), "parser")

#										print("block info:")
#										print(block)
										#print(tx)
#		print(tx)
		#Обрабатываем нужны токены
#		for token in self.config["tokens"]:

#			if tx["to"] == self.["pool"]:
#				print(self.token["name"] + " send to pool " + str(data["args"]["value"]) + ' from ' + data["args"]["from"])
#				print(data)
#				self.add_amount(data["args"]["from"], data["args"]["value"])

#				print("eee")

	def parseBlock(self, n):
		start = time.time()
		block = self.web3.eth.get_block(n)
		count_tx = len(block["transactions"])
		print("count tx: "+ str(count_tx))
		n = 0
		for tx_hash in block["transactions"]:
			n += 1
			print(str(n) + "/" + str(count_tx) + ". tx " + tx_hash.hex())			
			self.parseTx(block, tx_hash.hex())
#			print("pool")
#			print(tx)
		stop =  time.time()
		parse_time = stop-start
		print("Block parse time: " +str(parse_time) + "s")

	#Parse All Blocks
	def parseAllBlocks(self):
		parser_progress = True
		start = time.time()

		print("parseAllBlocks()...")
		start_block = self.getLastParsedBlock()
		print("parse blocks from " + str(start_block) + " to " + str(self.stop_parse_block))
		n2 = 0
		for n in range(start_block, self.stop_parse_block):
			n2 += 1
			#Парсим блок
			print("parse block " + str(n) + " (" + str(n2)+"/" + str(self.stop_parse_block-start_block)+")")

			self.parseBlock(n)
			#Запоминаем последний обработанный
			self.saveLastParsedBlock(n)
		stop =  time.time()
		print("Parsing complete: " +str(stop-start) + "s")
		parser_progress  = False
