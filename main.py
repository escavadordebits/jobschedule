import os
import time
import logging
import schedule
from flask import Flask, request,  make_response, jsonify
from pip._vendor import requests
import sqlite3
import json


sCompanyDB = "SBO_DESENV_EVO"
sPassword = "Evo@09"
sUserName = "manager"


try:
    conn = sqlite3.connect('Adtos.db')
    print("Opened database successfully")
    query = f'CREATE TABLE COMPANY (ID INTEGER PRIMARY KEY AUTOINCREMENT    NOT NULL, SessionID           TEXT    NOT NULL);'
    conn.execute(query)
    print("Tabela Criada Com Sucesso")

except Exception as e:
    print("Error", e)
try:
    conn = sqlite3.connect('Adtos.db')
    print("Opened database successfully")
    query = f'CREATE TABLE DRAFTS (DocEntry INTEGER PRIMARY KEY     NOT NULL,DocNum INTEGER  not null,CardCode CHAR(50) not null, CardName CHAR(100)  not null,DocTotal REAL not null,DocObjectCode CHAR(50) not null,DocumentStatus CHAR(50) not null,U_DwnPmtAuto CHAR(1) not null,Status CHAR(50) not null);'
    conn.execute(query)
    print("Tabela Criada Com Sucesso")

except Exception as e:
    print("Error", e)

try:
    conn = sqlite3.connect('Adtos.db')
    print("Opened database successfully")
    query = f'CREATE TABLE INVOICES (DocEntry INTEGER PRIMARY KEY not null,DocNum INTEGER not null,CardCode CHAR(50) not null, CardName CHAR(100)  not null,DocTotal REAL not null,DocObjectCode CHAR(50) not null,DocumentStatus CHAR(50) not null,U_DwnPmtAuto CHAR(1) not null,Status CHAR(50) not null);'
    conn.execute(query)
    print("Tabela Criada Com Sucesso")

except Exception as e:
    print("Error", e)

app = Flask(__name__)


def job1():
    print("Conectando na base")

    payload = {
        "CompanyDB": sCompanyDB,
        "Password": sPassword,
        "UserName": sUserName
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    url = f"http://192.1.2.7:50001/b1s/v1/Login"

    response = requests.post(url, json=payload, headers=headers, verify=False)
    if response.status_code == 200:
        conn = sqlite3.connect('Adtos.db')
        print("Opened database successfully")
        data = json.loads(response.content.decode('utf-8'))
        token = data["SessionId"]
        sToken = f"INSERT INTO COMPANY (ID, SessionID,CompanyDB,Password,UserName,URL) VALUES (1, '{token}','{sCompanyDB}','{sPassword}','{sUserName}','{url}'); "

        try:
            sDelete = f"delete from COMPANY"
            conn.execute(sDelete)
            conn.commit()
            conn.execute(sToken)
            conn.commit()
            conn.close()
        except Exception as e:
            print("Error", e)


def job2():
    print("Pesquisando Rescunhos")
    sPesquisa = f"Select SessionID FROM COMPANY"
    conn = sqlite3.connect('Adtos.db')
    cursor = conn.execute(sPesquisa)
    for row in cursor:
        token = row[0]

    stoken = f"B1SESSION={token}"
    headers = {
        "sessionid": token,
        "Cookie": stoken
    }

    url = f"http://192.1.2.7:50001/b1s/v1/Drafts?$select=DocEntry,CardCode,CardName,DocNum,DocObjectCode,DocTotal,DocumentStatus,U_DwnPmtAuto&$filter=DocObjectCode eq 'oInvoices' and DocumentStatus eq 'bost_Open'"

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))

        print(data)


def job3():
    print("Terceciro job 21 segundos")

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    url = f"http://192.1.2.7:50001/b1s/v1/http://192.1.2.7:50001/b1s/v1/DownPayments"

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 201:
        return make_response(201)


schedule.every(10).seconds.do(job1)
schedule.every(20).seconds.do(job2)
# schedule.every(30).seconds.do(job3)

while True:
    schedule.run_pending()
    time.sleep(5)
