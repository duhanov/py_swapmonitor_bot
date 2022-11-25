import telebot
from telebot import types
import json
from web3 import Web3
import asyncio
import threading
import time
from threading import Thread
from pathlib import Path
import os

#Конфиг
config = json.load(open('config.json'))

#Админы
admins = config["admin_nicknames"].split(',')


#Бот телеграм
tg_bot=telebot.TeleBot(config["telegram_token"])

#Где находимся в меню
menu_position = ""


#Меню
def menu_markup():
	global menu_position
	print(menu_position)
	print("menu_markup(" + menu_position + ")")
	markup=types.ReplyKeyboardMarkup(resize_keyboard=True)
	#Главное меню
	if menu_position == "":
		markup.add(types.KeyboardButton("Статистика"))
		markup.add(types.KeyboardButton("Очистить статистику"))
	elif menu_position == "clear":
		markup.add(types.KeyboardButton("Да"))
		markup.add(types.KeyboardButton("Нет"))
	return markup


@tg_bot.message_handler(commands=['start'])
def start_message(message):
	global admins
	print("/start @" + message.from_user.username)
	if message.from_user.username.lower() in admins:
		tg_bot.send_message(message.chat.id,'Приветствую, @' + message.from_user.username,reply_markup=menu_markup())


def clear_stats():
	dir_list = os.listdir("accounts")
	print(dir_list)
	for fname in dir_list:
		os.remove("accounts/" + fname)

def get_stats():
	res = {}
	dir_list = os.listdir("accounts")
	print(dir_list)
	for fname in dir_list:
		if os.path.splitext(fname)[1] == ".txt":
			account = ""
			if fname[0:len(config["token1"]["name"])+1] == config["token1"]["name"] + "_":
				account = fname[len(config["token1"]["name"])+1:len(config["token1"]["name"])-3-len(config["token1"]["name"])]
				token = config["token1"]["name"]
			if fname[0:len(config["token2"]["name"])+1] == config["token2"]["name"] + "_":
				account = fname[len(config["token2"]["name"])+1:len(config["token2"]["name"])-3-len(config["token2"]["name"])]
				token = config["token2"]["name"]
			if account != "":
				print(fname)
				print(account)
				print(token)
				if not account in res.keys():					
					res[account] = {}
				if not token in res[account].keys():
					res[account][token] = 0

				with open("accounts/" + fname, 'r') as f:
					total = int(f.read())
				res[account][token] = res[account][token] + total

	arr = []
	for account in res.keys():
		arr.append([account, res[account][config["token1"]["name"]], res[account][config["token2"]["name"]]]);

#	arr.append(["test", 2,2])
#	arr.append(["test", 3,3])
#	arr.append(["test", 1,1])
	arr.sort(key=lambda x: x[1], reverse=True)

	text = ""
	for item in arr:
		text = text + item[0]+ ": " + str(round(item[1]/config["token1"]["zeros"])) + " " + config["token1"]["name"] + ", " + str(round(item[2]/config["token2"]["zeros"]))  + " " + config["token2"]["name"] + "\n"

	if text == "":
		text = "Нет данных"
	return text

#Обработка сообщений бота
@tg_bot.message_handler(content_types='text')
def message_reply(message):
	global menu_position
	print("message_reply()")
	print("menu_position=" + menu_position)
	#Только для админов
	if message.from_user.username.lower() in admins:
		#Назад
		if message.text in ["Назад", "Отмена", "-Назад"]:
			menu_position = ""
			tg_bot.send_message(message.chat.id, "Выберите действие",reply_markup=menu_markup())
			return
		elif message.text == "Статистика":
			menu_position = ""

			tg_bot.send_message(message.chat.id, get_stats(),reply_markup=menu_markup())

		elif message.text == "Очистить статистику":
			menu_position = "clear"
			tg_bot.send_message(message.chat.id, "Хотите очистить статистику?",reply_markup=menu_markup())
		elif message.text == "Да" and menu_position == "clear":
			menu_position = ""
			clear_stats()
			tg_bot.send_message(message.chat.id, "Статистика очищена",reply_markup=menu_markup())
		elif message.text == "Нет":
			menu_position = ""
			tg_bot.send_message(message.chat.id, "Выберите действие",reply_markup=menu_markup())

#tg_bot.infinity_polling()



#Поток обработки транзакций
class monitorTransfer(threading.Thread):
	def __init__(self, token):
		self.token = token
		# helper function to execute the threads



	def run(self):
#		block = web3.eth.get_block('latest')
		global config
		print(config["provider"])
		web3 = Web3(Web3.HTTPProvider(config["provider"], request_kwargs={'timeout': 12000}))
		print("Listen contract for " + self.token["name"] + self.token["contract"])
		contract = web3.eth.contract(address=Web3.toChecksumAddress(self.token["contract"]), abi=json.load(open('pair_abi.json')))

		event_filter = contract.events.Transfer.createFilter(fromBlock='latest')

		try:
			while True:
				for item in event_filter.get_new_entries():
					self.handle_event(item)
				print("Sleep " + str(config["sleep"]) + " seconds")
				time.sleep(config["sleep"])
		except Exception as exc:
			print("Web3 exception " + self.token["name"])
			print(exc)
			print("Reconnect...")
			self.run()


	#Сохранить значение инвестиций
	def add_amount(self, address, amount):
		total = 0
		fname = "accounts/" + self.token["name"] + "_" + address + ".txt"
		path = Path(fname)
		if path.is_file():
			with open(fname, 'r') as f:
			    total = f.read()
		with open(fname, 'w') as f:
			f.write(str(int(total) + amount))


	def handle_event(self, event):
		global config
		data =  json.loads(Web3.toJSON(event))	
		#send to pool
#		print(data)
		if data["args"]["to"] == config["pool"]:
			print(self.token["name"] + " send to pool " + str(data["args"]["value"]) + ' from ' + data["args"]["from"])
			print(data)
			self.add_amount(data["args"]["from"], data["args"]["value"])
		#remove from pool
		elif data["args"]["from"] == config["pool"]:
			print(self.token["name"] + " remove from pool " + str(data["args"]["value"]) + ' by ' + data["args"]["to"])
			print(data)
			self.add_amount(data["args"]["to"], -1 * data["args"]["value"])





monitor1 = monitorTransfer(config["token1"])
monitor2 = monitorTransfer(config["token2"])
#thread2.run()

thread1 = Thread(target=monitor1.run)
thread2 = Thread(target=monitor2.run)

thread1.start()
thread2.start()


tg_bot.infinity_polling()


print("Exit")