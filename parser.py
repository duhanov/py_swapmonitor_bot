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
	def save_pool(self, address, amount0, amount1, hash, source):
		total = 0
		fname = "accounts/pool_" + address + ".json"
		path = Path(fname)
		if path.is_file():
			data = json.load(open(fname))
		else:
			data = {"total0": 0, "total1": 0, "txs": []}
		
		tx_found = False
		for tx in data["txs"]:
			if tx["hash"] == hash:
				tx_found = True
				break
		#Если еще не обрабатывали эту транзакцию
		if tx_found:
			print("TX ALREADY FOUND!")
		else:
			data["total0"] = data["total0"] + amount0
			data["total1"] = data["total1"] + amount1
			data["txs"].append({"time": time.time(), "amount0": amount0, "amount1": amount1, "source": source, "hash": hash})
			with open(fname, 'w') as f:
				json.dump(data, f)


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



	def handle_event(self, event):
		data =  json.loads(Web3.toJSON(event))	
		if data["args"]["to"] == self.config["pool"]:
			print("EVENT!  send to contract " + str(data["args"]["value"]) + ' from ' + data["args"]["from"] + ", tx: " + data["transactionHash"])

			self.parseTx(data["transactionHash"])


			#self.add_amount(token, data["args"]["from"], data["args"]["value"], data["transactionHash"], "event")
			#self.saveLastParsedBlock(data["blockNumber"])
		#remove from pool
		elif data["args"]["from"] == self.config["pool"]:
			print("EVENT!  send from contract " + str(data["args"]["value"]) + ' by ' + data["args"]["to"]+ ", tx: " + data["transactionHash"])
			self.parseTx(data["transactionHash"])

			#self.add_amount(token, data["args"]["to"], -1 * data["args"]["value"], data["transactionHash"], "event")
			#self.saveLastParsedBlock(data["blockNumber"])


	#Слушаем евенты
	def listen_events(self):
		self.listen_pool_events()

	#Слушаем евенты на средства в пул
	def listen_pool_events(self):
		thread = Thread(target=self.listen_pool_events_thread)
		thread.start()

	def listen_pool_events_thread(self):
		print("Listen EVENTS POOL")
		web3 = Web3(Web3.HTTPProvider(self.config["events_provider"], request_kwargs={'timeout': 12000}))
		contract = web3.eth.contract(address=Web3.toChecksumAddress(self.config["pool"]), abi=json.load(open('abi/pool.json')))
		event_filter = contract.events.Transfer.createFilter(fromBlock='latest')
		try:
			while True:
				for item in event_filter.get_new_entries():
					self.handle_event(token, item)
				print("Sleep " + str(self.config["sleep"]) + " seconds")
				time.sleep(self.config["sleep"])
		except Exception as exc:
			print("ERROR! Web3 exception in listen_pool_events_thread()")
			print(exc)
			print("Reconnect...")
			self.listen_pool_events_thread()



	def listen_pool_events_old(self):
		for token in self.config["tokens"]:
			print("t")
			thread = Thread(target=self.listen_pool_events_token, args=(token,))
			thread.start()
			print("Subscribe events for " + token["name"])

	def listen_pool_events_token(self, token):
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
			self.listen_pool_events_token(token)




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


	def eventSignatureHex(self, event):
		name = event["name"]
		inputs = [param["type"] for param in event["inputs"]]
		inputs = ",".join(inputs)

		event_signature_text = f"{name}({inputs})"
		event_signature_hex = self.web3.toHex(self.web3.keccak(text=event_signature_text))
		return event_signature_hex

	def decodeLogs(self, contract, receipt, log, event):

		decoded_logs = {}
		try:
			decoded_logs = contract.events[event["name"]]().processReceipt(receipt)
		except Warning:
			print("Warn ABI")
			decoded_logs = {}
		return decoded_logs

	def addr_name(self, addr):
		if addr == self.config["pool"]:
			return "(pool)"
		elif addr == self.config["pair"]:
			return "(pair)"
		elif addr == self.config["TransferHelper"]:
			return "(TransferHelper)"
		else:
			return ""



	def save_min_buy(self, address, amount0, amount1, tx_hash):
		fname = "accounts/min_" + address + ".json"

		path = Path(fname)
		if path.is_file():
			data = json.load(open(fname))
		else:
			data = {"total0": 0, "total1": 0, "txs": []}
		
		tx_found = False
		for tx in data["txs"]:
			if tx["hash"] == tx_hash:
				tx_found = True
				break
		#Если еще не обрабатывали эту транзакцию
		if tx_found:
			print("TX ALREADY FOUND!")
		else:
			data["total0"] = data["total0"] + amount0
			data["total1"] = data["total1"] + amount1
			data["txs"].append({"time": time.time(), "amount0": amount0, "amount1": amount1, "hash": tx_hash})
			with open(fname, 'w') as f:
				json.dump(data, f)



	def parseTx(self, tx_hash):
		print("parseTx " + tx_hash)
		tx = self.web3.eth.get_transaction(tx_hash)
		receipt = self.web3.eth.get_transaction_receipt(tx_hash)
#		print("receipt:")
#		print(receipt)
#		print(receipt)
		for log in receipt["logs"]:
#			print(log["address"])
			receipt_event_signature_hex = self.web3.toHex(log["topics"][0])

			found_abi = True
			
			#Адрем пары обмена
#			print("log address: " + log["address"] + self.addr_name(log["address"]))
			if log["address"] == self.config["pair"]:
#				abi = json.load(open('abi/pair.json'))
				abi = json.load(open('pool_abi.json'))

				contract = self.web3.eth.contract(self.config["pool"], abi=abi)


			#Если есть перевод на адрес пула
			elif log["address"] == self.config["pool"]:
				abi = json.load(open('pool_abi.json'))
				contract = self.web3.eth.contract(self.config["pool"], abi=abi)
			else:
				found_abi = False
			if found_abi:
				abi_events = [abi for abi in contract.abi if abi["type"] == "event"]

				#Есть перевод LP-токенов
				foundLpTransfer = False
				#Есть перевод средств
				foundToken1Transfer = False
				foundToken2Transfer = False

				for event in abi_events:
					#Если в транзе есть евент
					if self.eventSignatureHex(event) == receipt_event_signature_hex:

						if event["name"] == "Swap":
							print("EVENT " + event["name"])
							decoded_logs = self.decodeLogs(contract, receipt, log, event)
							for l in decoded_logs:
								print(l)
								print("---")
								if l["args"]["sender"] == self.config["TransferHelper"]:
									if l["args"]["amount1In"] != 0 and l["args"]["amount0Out"] != 0:
										print("send " + str(l["args"]["amount1In"]) + " USDT")
										print("out " + str(l["args"]["amount0Out"]) + "DNT")
										print("account: " + l["args"]["to"])

										#fixe buy amounts
										if l["args"]["amount1In"] >= self.config["buy_min_amount"] * self.config["tokens"][1]["zeros"]:
											self.save_min_buy(l["args"]["to"], l["args"]["amount0Out"], l["args"]["amount1In"], l["transactionHash"].hex())

									elif l["args"]["amount0In"] != 0 and l["args"]["amount1Out"] != 0:
										print("send " + str(l["args"]["amount0In"]) + " DNT")
										print("out " + str(l["args"]["amount1Out"]) + "USDT")
										print("account: " + l["args"]["to"])

							

						if event["name"] in ["Transfer"]:
							print("EVENT " + event["name"])
							decoded_logs = self.decodeLogs(contract, receipt, log, event)
							token0Amount = 0
							token1Amount = 0
							account = ""
							foundPool = False
							for l in decoded_logs:
								#Перевели в пул и адрес=
								if l["args"]["to"] == self.config["pool"] and l["address"] == self.config["tokens"][0]["contract"]:
									token0Amount = l["args"]["value"]
									account = l["args"]["from"]
									print("send to pool "  + str(l["args"]["value"]/self.config["tokens"][0]["zeros"]) + " " + self.config["tokens"][0]["name"])
								if l["args"]["to"] == self.config["pool"] and l["address"] == self.config["tokens"][1]["contract"]:
									token1Amount = l["args"]["value"]
									account = l["args"]["from"]
									print("send to pool "  + str(l["args"]["value"]/self.config["tokens"][1]["zeros"]) + " " + self.config["tokens"][1]["name"])

								#Из пула
								if l["args"]["from"] == self.config["pool"] and l["address"] == self.config["tokens"][0]["contract"]:
									token0Amount = -1 * l["args"]["value"]
									account = l["args"]["to"]
									print("from pool "  + str(l["args"]["value"]/self.config["tokens"][0]["zeros"]) + " " + self.config["tokens"][0]["name"])

								if l["args"]["from"] == self.config["pool"] and l["address"] == self.config["tokens"][1]["contract"]:
									token1Amount = -1 * l["args"]["value"]
									account = l["args"]["to"]
									print("from pool "  + str(l["args"]["value"]/self.config["tokens"][1]["zeros"]) + " " + self.config["tokens"][1]["name"])

								#Перевод LP токенов
								if l["args"]["from"] == "0x0000000000000000000000000000000000000000" and l["address"] == self.config["pool"]:
									foundPool = True
							#Сохраняем POOL
							if foundPool and token0Amount != 0 and token1Amount != 0:
								print("POOL " + account + " " + str(token0Amount) + " " + str(token1Amount))
								self.save_pool(account, token0Amount, token1Amount, l["transactionHash"].hex(), "events")

#								print(l)
#								print("---")
#							print("###")
#							for l in decoded_logs:
#								for token in self.config["tokens"]:
#									#Если вызов смартконтракта нашего токена
#									if l["address"] == token["contract"]:
#										#Перевели в пул
#										if l["args"]["to"] == self.config["pool"]:
#											print("Send  " + str(l["args"]["value"]) + " " + token["name"] + " TO Pool, from " + l["args"]["from"])
#											print(l)
#											self.add_amount(token, l["args"]["from"], l["args"]["value"], l["transactionHash"].hex(), "parser")
#
#										elif l["args"]["from"] == self.config["pool"]:
#											print("Back " +str(l["args"]["value"]) + " " + token["name"] + " from POOL, by " +  l["args"]["to"])
#											print(l)
#											self.add_amount(token, l["args"]["to"], -1 * l["args"]["value"], l["transactionHash"].hex(), "parser")

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
			self.parseTx(tx_hash.hex())
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
