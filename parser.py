from pathlib import Path
import os
import json
import time


from web3 import Web3
from web3.middleware import geth_poa_middleware

from threading import Thread


from datetime import datetime
from datetime import timedelta
import requests

class Parser:
	web3 = Web3()
	config = json.load(open('config.json'))

	#Остановить парсинг блоков на этом блоке
	stop_parse_block = 0
	parser_progress = False

	def __init__(self):
		print("connect to web3...")
		self.web3 = Web3(Web3.HTTPProvider(self.config["provider"], request_kwargs={'timeout': 12000}))
		self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
		#config = json.load(open('config.json'))

		#Остановить парсинг блоков на этом блоке
		self.stop_parse_block = self.web3.eth.get_block('latest')["number"]



		print("OK")



	settings_file_name = "settings.json"

	def settings(self, name):
		if not name in ["buy_min_amount"]:
			return ""
		else:
			fname = ""
			if Path(self.settings_file_name).is_file():
				data = self.load_data(self.settings_file_name)
			else:
				data = {"buy_min_amount": 10}

			return data[name]

	def load_data(self, fname):
		return json.load(open(fname))

	def save_settings(self, name, value):
		data = {}
		if Path(self.settings_file_name).is_file():
			data = self.load_data(self.settings_file_name)
		else:
			data = {"buy_min_amount": 10}
		print("params:")
		print(data)
		print(name)
		print(value)
		data[name] = value
		self.save_data(self.settings_file_name, data)

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
			print("pool saved")
			with open(fname, 'w') as f:
				json.dump(data, f)


	def save_data(self, fname, data):
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
			print("pool saved")

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

	last_event_at = time.time()

	def listen_pool_events_thread(self):

		print("Listen EVENTS POOL")
		web3 = Web3(Web3.HTTPProvider(self.config["events_provider"], request_kwargs={'timeout': 12000}))
#		contract = web3.eth.contract(address=Web3.toChecksumAddress(self.config["TransferHelper"]), abi=json.load(open('abi/pool.json')))
#		print("Listen Transfer Event for " + self.config["TransferHelper"])
#		contract_address = self.config["TransferHelper"]
#		contract_address = self.config["TransferHelper"]
		contract_address = self.config["tokens"][0]["contract"]
		contract = web3.eth.contract(address=Web3.toChecksumAddress(contract_address), abi=json.load(open('abi/pool.json')))
		print("Listen Transfer Event for " + contract_address)

		event_filter = contract.events.Transfer.createFilter(fromBlock='latest')
		try:
			while True:
				for item in event_filter.get_new_entries():
					self.last_event_at = time.time()
					self.handle_event(item)
				if self.last_event_at > time.time() - self.config["sleep"] - 2:
					print("! NEW EVENTS !")


				print("Sleep " + str(self.config["sleep"]) + " seconds. Last event: " + str(time.time() - self.last_event_at) + "sec ago")

				time.sleep(self.config["sleep"])
#				print(datetime.fromtimestamp(last_event_at).strftime("%d.%m.%Y %H:%M"))
		except Exception as exc:
			print("ERROR! Web3 exception in listen_pool_events_thread()")
			print(exc)
			print("Reconnect...")
			self.listen_pool_events_thread()



#	def listen_pool_events_old(self):
#		for token in self.config["tokens"]:
#			print("t")
#			thread = Thread(target=self.listen_pool_events_token, args=(token,))
#			thread.start()
#			print("Subscribe events for " + token["name"])

#	def listen_pool_events_token(self, token):
#		print("Listen EVENTS " + token["name"] + " " + token["contract"])
#		web3 = Web3(Web3.HTTPProvider(self.config["events_provider"], request_kwargs={'timeout': 12000}))
#		contract = web3.eth.contract(address=Web3.toChecksumAddress(token["contract"]), abi=json.load(open('pair_abi.json')))

#		event_filter = contract.events.Transfer.createFilter(fromBlock='latest')
#
#		try:
#			while True:
#				for item in event_filter.get_new_entries():
##					self.handle_event(token, item)
#				print("Sleep " + str(self.config["sleep"]) + " seconds")
#				time.sleep(self.config["sleep"])
#		except Exception as exc:
#			print("ERROR! Web3 exception " + token["name"])
#			print(exc)
#			print("Reconnect...")
#			self.listen_pool_events_token(token)




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
		print(str(round(amount1/self.config["tokens"][1]["zeros"])))
		if amount1 >= self.settings("buy_min_amount") * self.config["tokens"][1]["zeros"]:
											
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

	start_time = 0

	def parseTxs(self, start_block, end_block):
		print("Parse bocks: " + str(start_block) + "-" +  str(end_block))# + "(" + str(block_delta) + ")")
		url = 'https://api.bscscan.com/api?module=account&action=txlist&address=0x10ed43c718714eb63d5aa57b78b54704e256024e&startblock=' + str(start_block) + '&endblock=' + str(end_block) + '&page=1&offset=0&sort=asc&apikey=J3NSSIP3WKM3PMSIXW6YG1DYTBM3NAW25H'
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
					print("parseTx " + str(tx_n) + "/" + str(len(txs)) + " " + tx["hash"] + " (blocks " + str(start_block) + "-" + str(end_block) + ")")
					self.parseTx(tx["hash"])
					tx_parsed = True
				except Exception as exc:
					print("ERROR")
					print(exc)
					print("Sleep " + str(error_sleep_time) + "s")
					time.sleep(error_sleep_time)
					error_sleep_time += 1
					print("Try again...")
		end_time = time.time()

		print("Parsed blocks: " + str(start_block) + "-" +  str(end_block) + "(" + str(end_block-start_block + 1) + "). Txs: " + str(len(txs)) + " Work time: " + str(timedelta(seconds = int(end_time-self.start_time))))


	def parseTx(self, tx_hash):
		#print("parseTx " + tx_hash)
		tx = self.web3.eth.get_transaction(tx_hash)
		receipt = self.web3.eth.get_transaction_receipt(tx_hash)
#		print("receipt:")
#		print(receipt)
#		print(receipt)
#		print("logs:")
#		print(receipt["logs"])
		for log in receipt["logs"]:
			if len(log["topics"])>0:
				receipt_event_signature_hex = self.web3.toHex(log["topics"][0])

				found_abi = True
			
			#Адрем пары обмена
#			print("log address: " + log["address"] + self.addr_name(log["address"]))
				if log["address"] == self.config["pair"]:
#					abi = json.load(open('abi/pair.json'))
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

				
					foundPool = False
					token0Amount = 0
					token1Amount = 0
					account = ""

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
										print("Swap by Cakeswap")
										if l["args"]["amount1In"] != 0 and l["args"]["amount0Out"] != 0:
											print("send " + str(l["args"]["amount1In"]) + " USDT")
											print("out " + str(l["args"]["amount0Out"]) + " DNT")
											print("account: " + l["args"]["to"])

											#fixe buy amounts
											#print("check " + str(self.settings("buy_min_amount")) +str(self.settings("buy_min_amount") * self.config["tokens"][1]["zeros"]))
											print("save_min_buy()")
#		if l["args"]["amount1In"] >= self.settings("buy_min_amount") * self.config["tokens"][1]["zeros"]:

#fix
											self.save_min_buy(l["args"]["to"], l["args"]["amount0Out"], l["args"]["amount1In"], l["transactionHash"].hex())
											break
										elif l["args"]["amount0In"] != 0 and l["args"]["amount1Out"] != 0:
											print("send " + str(l["args"]["amount0In"]) + " DNT")
											print("out " + str(l["args"]["amount1Out"]) + "USDT")
											print("account: " + l["args"]["to"])
											break
							

							if event["name"] in ["Transfer"]:
								print("EVENT " + event["name"])
								decoded_logs = self.decodeLogs(contract, receipt, log, event)
								#token0Amount = 0
								#token1Amount = 0
								#account = ""
								#foundPool = False
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
							#cycle by logs
					#event exist
				#cycle by events

					#Сохраняем POOL
					if foundPool and token0Amount != 0 and token1Amount != 0:
						print("SAVE POOL " + account + " " + str(token0Amount) + " " + str(token1Amount))
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
		parse_time = int(stop-start)
		print("Block parse time: " + str(timedelta(seconds = parse_time)))


	def parseFromBlock(self, start_block):
		parser_progress = True
		start = time.time()

		print("parseFromBlock()...")
		end_block = self.getLastParsedBlock()
		print("parse blocks from " + str(start_block) + " to " + str(end_block))
		n2 = 0
		for n in range(start_block, end_block):
			n2 += 1
			#Парсим блок
			print("parse block " + str(n) + " (" + str(n2)+"/" + str(self.stop_parse_block-start_block)+")")# + str(time.time()-start) + "s")

			self.parseBlock(n)
#			print(time.time()-start)

#			timedelta(seconds=sec)

			print("total work time: " + str(timedelta(seconds=int(time.time() - start))))
			#Запоминаем последний обработанный
#			self.saveLastParsedBlock(n)
		stop =  time.time()
		print("Parsing complete: " + str(stop-start) + "s")
		parser_progress  = False


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
