""" Лабораторна робота №1. Орєхова Вікторія, КМ-82.
    Варіант №7
    Завдання. Порівняти найкращий бал з Математики у 2020 та 2019 роках серед тих кому було зараховано тест.
"""
import psycopg2
import psycopg2.errorcodes
import csv
import time
from datetime import timedelta

# з'єднання з базою даних
connection = psycopg2.connect(
            host = "localhost",
            database="my_db",
            user = "postgres",
            password = "postgreadminSQL",
            port = "5432")
cursor = connection.cursor()

# видалення таблиці, якщо така існує
cursor.execute("DROP TABLE IF EXISTS ZNO;")
cursor.execute("DROP TABLE IF EXISTS Task;")

# створення таблиці
query1 = '''
CREATE TABLE ZNO (
	OUTID	TEXT	NOT NULL	PRIMARY KEY, 
	Birth	INTEGER		NOT NULL,	
	SEXTYPENAME	TEXT	NOT NULL,	
	REGNAME	TEXT	NOT NULL,	
	AREANAME	TEXT	NOT NULL,	
	TERNAME	TEXT	NOT NULL,	
	REGTYPENAME	TEXT	NOT NULL,	
	TerTypeName	TEXT	NOT NULL,	
	ClassProfileNAME	TEXT,	
	ClassLangName	TEXT,	
	EONAME	TEXT,	
	EOTYPENAME	TEXT,	
	EORegName	TEXT,	
	EOAreaName	TEXT,	
	EOTerName	TEXT,	
	EOParent	TEXT,	
	UkrTest	TEXT,	
	UkrTestStatus	TEXT,	
	UkrBall100	VARCHAR,	
	UkrBall12	VARCHAR,	
	UkrBall	VARCHAR,	
	UkrAdaptScale	FLOAT,	
	UkrPTName	TEXT,	
	UkrPTRegName	TEXT,	
	UkrPTAreaName	TEXT,	
	UkrPTTerName	TEXT,	
	histTest	TEXT,	
	HistLang	TEXT,	
	histTestStatus	TEXT,	
	histBall100	VARCHAR,	
	histBall12	VARCHAR,	
	histBall	VARCHAR,	
	histPTName	TEXT,	
	histPTRegName	TEXT,	
	histPTAreaName	TEXT,	
	histPTTerName	TEXT,	
	mathTest	TEXT,	
	mathLang	TEXT,	
	mathTestStatus	TEXT,	
	mathBall100	VARCHAR,	
	mathBall12	VARCHAR,	
	mathBall	VARCHAR,	
	mathPTName	TEXT,	
	mathPTRegName	TEXT,	
	mathPTAreaName	TEXT,	
	mathPTTerName	TEXT,	
	physTest	TEXT,	
	physLang	TEXT,	
	physTestStatus	TEXT,	
	physBall100	VARCHAR,	
	physBall12	VARCHAR,	
	physBall	VARCHAR,	
	physPTName	TEXT,	
	physPTRegName	TEXT,	
	physPTAreaName	TEXT,	
	physPTTerName	TEXT,	
	chemTest	TEXT,	
	chemLang	TEXT,	
	chemTestStatus	TEXT,	
	chemBall100	VARCHAR,	
	chemBall12	VARCHAR,	
	chemBall	VARCHAR,	
	chemPTName	TEXT,	
	chemPTRegName	TEXT,	
	chemPTAreaName	TEXT,	
	chemPTTerName	TEXT,	
	bioTest	TEXT,	
	bioLang	TEXT,	
	bioTestStatus	TEXT,	
	bioBall100	VARCHAR,	
	bioBall12	VARCHAR,	
	bioBall	VARCHAR,	
	bioPTName	TEXT,	
	bioPTRegName	TEXT,	
	bioPTAreaName	TEXT,	
	bioPTTerName	TEXT,	
	geoTest	TEXT,	
	geoLang	TEXT,	
	geoTestStatus	TEXT,	
	geoBall100	VARCHAR,	
	geoBall12	VARCHAR,	
	geoBall	VARCHAR,	
	geoPTName	TEXT,	
	geoPTRegName	TEXT,	
	geoPTAreaName	TEXT,	
	geoPTTerName	TEXT,	
	engTest	TEXT,	
	engTestStatus	TEXT,	
	engBall100	VARCHAR,	
	engBall12	VARCHAR,	
	engDPALevel	TEXT,	
	engBall	VARCHAR,	
	engPTName	TEXT,	
	engPTRegName	TEXT,	
	engPTAreaName	TEXT,	
	engPTTerName	TEXT,	
	fraTest	TEXT,	
	fraTestStatus	TEXT,	
	fraBall100	VARCHAR,	
	fraBall12	VARCHAR,	
	fraDPALevel	TEXT,	
	fraBall	VARCHAR,	
	fraPTName	TEXT,	
	fraPTRegName	TEXT,	
	fraPTAreaName	TEXT,	
	fraPTTerName	TEXT,	
	deuTest	TEXT,	
	deuTestStatus	TEXT,	
	deuBall100	VARCHAR,	
	deuBall12	VARCHAR,	
	deuDPALevel	TEXT,	
	deuBall	VARCHAR,	
	deuPTName	TEXT,	
	deuPTRegName	TEXT,	
	deuPTAreaName	TEXT,	
	deuPTTerName	TEXT,	
	spaTest	TEXT,	
	spaTestStatus	TEXT,	
	spaBall100	VARCHAR,	
	spaBall12	VARCHAR,	
	spaDPALevel	TEXT,	
	spaBall	VARCHAR,	
	spaPTName	TEXT,	
	spaPTRegName	TEXT,	
	spaPTAreaName	TEXT,	
	spaPTTerName	TEXT,
	Year    INTEGER     NOT NULL    	
);
'''
cursor.execute(query1)

try:
    # відкриття текстового файлу, у який записується час виконання завантаження даних у базу даних
    f = open('text.txt', 'w')
    start_time = time.monotonic()
    # додавання даних з першого csv файлу у таблицю
    cursor.execute("COPY ZNO FROM 'D:\DBIS\Odata2019File.csv' DELIMITER ';' CSV NULL '' HEADER ENCODING 'win1251';")
    end_time = time.monotonic()
    print('На даний файл витрачено --' + str(timedelta(seconds=end_time - start_time)))
    # запис у текстовий файл часу завантаження даних з першого файлу
    l = [str(timedelta(seconds=end_time - start_time))]
    for index in l:
        f.write('На перший файл витрачено --' + index + '\n')

    start_time = time.monotonic()
    # додавання даних з другого csv файлу у таблицю
    cursor.execute("COPY ZNO FROM 'D:\DBIS\Odata2020File.csv' DELIMITER ';' CSV NULL '' HEADER ENCODING 'win1251';")
    end_time = time.monotonic()
    print('На даний файл витрачено --' + str(timedelta(seconds=end_time - start_time)))
    # запис у текстовий файл часу завантаження даних з першого файлу
    k = [str(timedelta(seconds=end_time - start_time))]
    for index in k:
        f.write('На другий файл витрачено --' + index + '\n')
    f.close()
# перевірка на розрив з'єднання з базою даних
except psycopg2.OperationalError:
    if psycopg2.OperationalError.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
        connection_restored = False
        while not connection_restored:
            # поновлення з'єднання з базою даних
            try:
                connection = psycopg2.connect(
                    host="localhost",
                    database="my_db",
                    user="postgres",
                    password="postgreadminSQL",
                    port="5432")
                cursor = connection.cursor()
                connection_restored = True
            except psycopg2.OperationalError:
                pass

# виконання завдання відповідно до варіанту
query2 = '''
SELECT REGNAME AS "Область", Year AS "Рік", max(mathBall100) AS "Максимальний бал" INTO Task
FROM ZNO
WHERE mathTestStatus = 'Зараховано' 
GROUP BY REGNAME, Year;
'''
cursor.execute(query2)

# запис результату виконаного завдання у csv файл
table = ['Task']
for t in table:
    with open(t + '.csv', 'w', newline = '') as csvfile:
        cursor.execute('SELECT * FROM ' + t)
        row = cursor.fetchone()
        writeCSV = csv.writer(csvfile, delimiter=',')

        while row:
            writeCSV.writerow(row)
            row = cursor.fetchone()

connection.commit()
cursor.close()
connection.close()
