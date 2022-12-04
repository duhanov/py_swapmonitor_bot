import telebot
from telebot import types
import json
from web3 import Web3
import asyncio
import threading
import time
from threading import Thread

from parser import *


from web3.middleware import geth_poa_middleware

import re

import warnings


from datetime import datetime


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
		markup.add(types.KeyboardButton("Статистика пула"))
		markup.add(types.KeyboardButton("Статистика покупок"))
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

def token_value(value, token):
	return str(round(value/token["zeros"],2)) + " " + token["name"]


def get_stats():
	global config
	arr = []
	dir_list = os.listdir("accounts")
	print(dir_list)
	for fname in dir_list:
		if os.path.splitext(fname)[1] == ".json":
			print(fname[0:4] )
			if fname[0:5] == "pool_":
				account = fname[5:len(fname)-5]
				print("Account: " + account)
				acc_info = json.load(open("accounts/" + fname))
				arr.append([account, acc_info])


	arr.sort(key=lambda x: x[1]["total0"], reverse=True)

	text = ""
	for item in arr:
		text = text + item[0]+ ": " + token_value(item[1]["total0"], config["tokens"][0]) + ", " + token_value(item[1]["total1"], config["tokens"][1]) 

	if text == "":
		text = "Нет данных"
	return text


def tg_send(chat_id, msg):
	global tg_bot
	#Макс длина строги в тг
	max_len = 4096
	text_out = ""
	for line in msg.split("\n"):
		#Привысили макс длину - бьем гна подстроки
		if len(text_out + line) > max_len:
			part_str = ""
			for part_line in line.split(", "):
				if len(text_out + part_line) > max_len:					
					tg_bot.send_message(chat_id, text_out, reply_markup=menu_markup())
					text_out = ""
				else:
					text_out = text_out + part_line + ", "
		else:
			text_out = text_out + line + "\n"
	if text_out != "":
		tg_bot.send_message(chat_id, text_out, reply_markup=menu_markup())



def get_stats2():
	global config
	arr = []
	dir_list = os.listdir("accounts")
	print(dir_list)
	for fname in dir_list:
		if os.path.splitext(fname)[1] == ".json":
			if fname[0:4] == "min_":
				account = fname[4:len(fname)-4]
				print("Account: " + account)
				acc_info = json.load(open("accounts/" + fname))
				arr.append([account, acc_info])


	arr.sort(key=lambda x: x[1]["total0"], reverse=True)

	text = ""
	for item in arr:
		text = text + item[0] + ": " + token_value(item[1]["total1"], config["tokens"][1]) + " -> " + token_value(item[1]["total0"], config["tokens"][0])

	if text == "":
		text = "Нет данных"
	return text


def getAddrStat(address):
	global config
	text = "Транзакции из/в POOL:\n"
	fname = "accounts/pool_" + address +".json"
	if Path(fname).is_file():
		data = json.load(open(fname))
		for tx in data["txs"]:
			text = text + datetime.utcfromtimestamp(tx["time"]).strftime('%Y-%m-%d %H:%M') + " " + token_value(tx["amount0"], config["tokens"][0]) + ", " + token_value(tx["amount1"], config["tokens"][1]) + "\n"

	text = text + "\nТранзакции покупки " + config["tokens"][0]["name"] + ":\n"
	fname = "accounts/min_" + address +".json"
	if Path(fname).is_file():
		data = json.load(open(fname))
		for tx in data["txs"]:
			text = text + datetime.utcfromtimestamp(tx["time"]).strftime('%Y-%m-%d %H:%M') + " " + token_value(tx["amount0"], config["tokens"][0]) + ", " + token_value(tx["amount1"], config["tokens"][1]) + "\n"


	if text == "":
		text = "Данные для адреса " + address + " не найдены"
	return text

#Обработка сообщений бота
@tg_bot.message_handler(content_types='text')
def message_reply(message):
	global menu_position
	global parser
	print("message_reply()")
	print("menu_position=" + menu_position)
	#Только для админов
	if message.from_user.username.lower() in admins:
		#Назад
		if message.text in ["Назад", "Отмена", "-Назад"]:
			menu_position = ""
			tg_bot.send_message(message.chat.id, "Выберите действие",reply_markup=menu_markup())
			return
		if message.text  == "/lasttx":
			tg_bot.send_message(message.chat.id, "Последняя транзакция: " + str(time.time() - parser.last_event_at) + "sec" ,reply_markup=menu_markup())			
		elif message.text == "/minbuy":
			tg_bot.send_message(message.chat.id, "Фиксировать минимальную ценую покупки на: " + str(parser.settings("buy_min_amount")) + "USDT" ,reply_markup=menu_markup())			

		elif re.search("^\/minbuy [0-9\.]+$", message.text):
			buy_min_amount = float(message.text.split(" ")[1])
			print("new buy_min_amount = ")
			print(buy_min_amount)
			parser.save_settings("buy_min_amount", buy_min_amount)
			tg_bot.send_message(message.chat.id, "Фиксировать минимальную ценую покупки на: " + str(parser.settings("buy_min_amount")) + "USDT" ,reply_markup=menu_markup())			

		elif message.text == "Статистика пула":
			menu_position = ""

#			tg_bot.send_message(message.chat.id, get_stats(),reply_markup=menu_markup())
			tg_send(message.chat.id, get_stats())
		elif message.text == "Статистика покупок":
			menu_position = ""
			tg_send(message.chat.id, get_stats2())
			#tg_bot.send_message(message.chat.id, get_stats2(),reply_markup=menu_markup())
		elif re.search("^0x[a-zA-Z0-9]{40}$", message.text):
		#  ==  0xF300D3ac8B3f93550C5aD8d4cCfF88dbff3c3d23"asd			
			menu_position = ""
			tg_bot.send_message(message.chat.id, getAddrStat(message.text),reply_markup=menu_markup())
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



#Disable Warnings
warnings.simplefilter('ignore')



#Поток обработки транзакций


parser = Parser()



#Подписываемся на события
print("Listen Events...")
parser.listen_events()

#13 - 199
#parser.parseTx("0xc15d395ab4231f9e5a43cb95113aa16564727a609ce2fb69b65ca5b49517a9c9")
#dnt - usd
#parser.parseTx("0x4f51a757a6bf9b6a2dab20d2ee3f82ba567503a655c4ee95d3263c851956c960")


#remove 23 liq
#parser.parseTx("0x09998a959f22e1b762c03ad240e3f6e702af75445c83c2510ce28188a68e3142")


#add 12 USD liq
#parser.parseTx("0x7d4f308e0c3717299ba9d77420e985e6aaa91de39157e519f419dacd0a901a71")

#swap 15 USD->
#parser.parseTx("0x3d031b7eaeafdfd450539322ce1eb5deb5af3954daf30b42b21bb6a165af8cf5")


#print("Start Parser")
#Старт парсер блоков
#thread_parser = Thread(target=parser.parseAllBlocks)
#thread_parser.start()



#parser.parseTx({}, "0xcc900d007c0eff9186b4eab987222f069a1b01355408279e6477fc15156c6722")
#parser.parseBlock(23365221)
#parser.run()

tg_bot.infinity_polling()


print("Exit")