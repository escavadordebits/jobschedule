import os
import time
import logging
import schedule
from flask import Flask, request,  make_response, jsonify
from pip._vendor import requests
import sqlite3
import json

with open('dadosacesso.json') as f:
    data = json.load(f)

sCompanyDB = data["CompanyDB"]
sPassword = data["Password"]
sUserName = data["UserName"]
UrlSAP = data["Url"]


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
        "CompanyDB": f"{sCompanyDB}",
        "Password": f"{sPassword}",
        "UserName": f"{sUserName}"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    url = f"{UrlSAP}/Login"

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

# busca os esbocos de notas fiscais de saida no sap


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

    url = f"{UrlSAP}/Drafts?$select=DocEntry,CardCode,CardName,DocNum,DocObjectCode,DocTotal,DocumentStatus,U_DwnPmtAuto&$filter=DocObjectCode eq 'oInvoices' and DocumentStatus eq 'bost_Open'"

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))

        for rows in data['value']:
            sDocEntry = rows["DocEntry"]
            sDocNum = rows["DocNum"]
            sCardCode = rows["CardCode"]
            sCardName = rows["CardName"]
            sDocTotal = rows["DocTotal"]
            sDocObjectCode = rows["DocObjectCode"]
            sDocumentStatus = 'bost_Open'
            sU_DwnPmtAuto = rows["U_DwnPmtAuto"]
            Status = 'N'

            conn = sqlite3.connect('Adtos.db')
            queryselect = f"Select Docentry from DRAFTS WHERE DocEntry = {sDocEntry}"
            cursor = conn.execute(queryselect)
            if (cursor.rowcount == -1):
                query = f"INSERT INTO DRAFTS (DocEntry,DocNum,CardCode, CardName,DocTotal,DocObjectCode,DocumentStatus,U_DwnPmtAuto,Status) values ('{sDocEntry}','{sDocNum}','{sCardCode}','{sCardName}','{sDocTotal}','{sDocObjectCode}','{sDocumentStatus}','{sU_DwnPmtAuto}','{Status}');"
                conn.execute(query)
                conn.commit()
            print("Tabela Criada Com Sucesso")

        print(data)

# busca os  adtos de clientes no SAP

def job3():
    print("Terceciro job 21 segundos")

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

    url = f"{UrlSAP}/DownPayments?$select=DocEntry,CardCode,CardName,DocNum,DocObjectCode,DocTotal,DocumentStatus,U_DwnPmtAuto&$filter=U_DwnPmtAuto eq 'S' and DocumentStatus eq 'bost_Close'"

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))

        for rows in data['value']:
            sDocEntry = rows["DocEntry"]
            sDocNum = rows["DocNum"]
            sCardCode = rows["CardCode"]
            sCardName = rows["CardName"]
            sDocTotal = rows["DocTotal"]
            sDocObjectCode = rows["DocObjectCode"]
            sDocumentStatus = 'bost_Open'
            sU_DwnPmtAuto = rows["U_DwnPmtAuto"]
            Status = 'N'

            conn = sqlite3.connect('Adtos.db')
            queryselect = f"Select Docentry from INVOICES WHERE DocEntry = {sDocEntry}"
            cursor = conn.execute(queryselect)
            Linhas = cursor.fetchall()
            if (cursor.fetchall() == 0):
                query = f"INSERT INTO INVOICES (DocEntry,DocNum,CardCode, CardName,DocTotal,DocObjectCode,DocumentStatus,U_DwnPmtAuto,Status) values ('{sDocEntry}','{sDocNum}','{sCardCode}','{sCardName}','{sDocTotal}','{sDocObjectCode}','{sDocumentStatus}','{sU_DwnPmtAuto}','{Status}');"
                conn.execute(query)
                conn.commit()
            print("Tabela Criada Com Sucesso")

# Post da INvoice atualizando o valor a ser sacado total de adto


def job4():
    print("Quarto job 21 segundos")
    ListaAdto = list()
    pesquisa = f"select  T0.DocEntry DocAdto,T0.DocTotal,T1.DocEntry DocNota from DRAFTS T1\
            inner join INVOICES T0 on T0.CardCode=T1.CardCode\
            WHERE t1.DocTotal = t0.DocTotal and t1.U_DwnPmtAuto = 'S' and T0.Status = 'N'"

    pToken = f"Select SessionID FROM COMPANY"
    conn = sqlite3.connect('Adtos.db')
    cursor = conn.execute(pToken)
    for row in cursor:
        token = row[0]

    stoken = f"B1SESSION={token}"
    headers = {
        "sessionid": token,
        "Cookie": stoken
    }
    cursor2 = conn.execute(pesquisa)
    Adtos = cursor2.fetchall()
    for ListaAdtos in Adtos:
        ListaAdto.append(
            {
                "DocNota": ListaAdtos[2],
                "DocAdto": ListaAdtos[0],
                "TotalNota": ListaAdtos[1]
            }
        )

    for i in ListaAdto:
        NumDraft = i["DocNota"]
        NumAdto = i["DocAdto"]
        DocTotal = i["TotalNota"]
        url = f"{UrlSAP}/Drafts({NumDraft})"
        payload = {
            "DocEntry": f"{NumDraft}",
            "DownPaymentsToDraw": [
                {
                    "DocEntry": f"{NumAdto}",
                    "AmountToDraw": f"{DocTotal}",
                    "AmountToDrawSC": f"{DocTotal}"

                }
            ],
            "DiscountPercent": 0.0
        }

        response = requests.patch(
            url, headers=headers, data=json.dumps(payload), verify=False)
        if response.status_code == 204:

            payload2 = {
                "DocEntry": f"{NumDraft}",
                "DiscountPercent": 0.0
            }
            response = requests.patch(
                url, headers=headers, data=json.dumps(payload2), verify=False)
            atualizadesc = f"update INVOICES SET STATUS = 'S' where DocEntry = '{NumAdto}'"
            cursor2 = conn.execute(atualizadesc)
            cursor2.connection.commit()

            url = f"h{url}/DraftsService_SaveDraftToDocument"

            payload3 = {
                "Document": {

                    "DocEntry": f"{NumDraft}"
                }

            }
            response = requests.patch(
                url, headers=headers, data=json.dumps(payload3), verify=False)

            print("Nota Vinculada ao Adto  Com Sucesso")


schedule.every(10).seconds.do(job1)
# schedule.every(40).seconds.do(job2)
# schedule.every(50).seconds.do(job3)
schedule.every(15).seconds.do(job4)


while True:
    schedule.run_pending()
    time.sleep(3)
