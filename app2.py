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
	global config
	res = {}
	dir_list = os.listdir("accounts")
	print(dir_list)
	for fname in dir_list:
		if os.path.splitext(fname)[1] == ".json":
			account = ""
			for token in config["tokens"]:
				if fname[0:len(token["name"])+1] == token["name"] + "_":
					account = fname[len(token["name"])+1:len(token["name"])-4-len(token["name"])]
					if account != "":
						print(fname)
						print(account)
						print(token["name"])
						#Запоминаем статистику
						if not account in res.keys():					
							res[account] = {}

						acc_info = json.load(open("accounts/" + fname))

						res[account][token["name"]] = {"total": acc_info["total"], "zeros": token["zeros"]}

	arr = []
	#res[account][config["tokens"][0]["name"]], res[account][config["tokens"]["name"]
	for account in res.keys():
		acc_tokens = []
		for token in res[account].keys():
			acc_tokens.append({"name": token, "total": res[account][token]})
		arr.append([account, acc_tokens])

#	arr.append(["test", 2,2])
#	arr.append(["test", 3,3])
#	arr.append(["test", 1,1])
	arr.sort(key=lambda x: x[1][0]["total"]["total"], reverse=True)

	text = ""
	for item in arr:
		text = text + item[0]+ ": "
		# + str(round(item[1]/config["token1"]["zeros"])) + " " + config["token1"]["name"] + ", " + str(round(item[2]/config["token2"]["zeros"]))  + " " + config["token2"]["name"] + "\n"
		for token in item[1]:
			text = text + str(round(token["total"]["total"]/token["total"]["zeros"], 2)) + " " + token["name"] + ", "
		text = text + "\n"
	if text == "":
		text = "Нет данных"
	return text


def getAddrStat(address):
	global config
	text = "Transaction for " + address + "\n"
	for token in config["tokens"]:
		fname = "accounts/" + token["name"] + "_" + address +".json"
		text = text + "TX FOR " + token["name"] + "\n"
		if Path(fname).is_file():
			data = json.load(open(fname))
			for tx in data["txs"]:
				text = text + datetime.utcfromtimestamp(tx["time"]).strftime('%Y-%m-%d %H:%M') + " " + str(round(tx["amount"]/token["zeros"],2)) + " " + token["name"] + "\n"
		else:
			print("cant find " + fname)
	if text == "":
		text = "Данные для адреса " + address + " не найдены"
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



#Поток обработки транзакций


parser = Parser()



#Подписываемся на события
print("Listen Events...")
parser.listen_events()


print("Start Parser")
#Старт парсер блоков
thread_parser = Thread(target=parser.parseAllBlocks)
thread_parser.start()



#parser.parseTx({}, "0xcc900d007c0eff9186b4eab987222f069a1b01355408279e6477fc15156c6722")
#parser.parseBlock(23365221)
#parser.run()

tg_bot.infinity_polling()


print("Exit")