#!/usr/bin/env python
import optparse
import os
import platform
import sys
import codecs
import sqlite3
import csv
import pprint
import threading
import logging
import decimal
import datetime
import math
D=decimal.Decimal
logging.basicConfig(filename='Log.log',level=logging.DEBUG)

from ConfigParser import *

config = ConfigParser()
settingsFile = os.path.abspath(os.path.expanduser("settings.cfg"))
config.read(settingsFile)


if not config.has_section('db'):
    config.add_section('db')


dbFile = os.path.abspath(os.path.expanduser('industry.db'));


for section in config.sections():
    for option in config.options(section):
        if option == 'database_file' and section=='db':
            dbFile = config.get(section, option)

def isReturnFile(myfile):
    if os.path.abspath(os.path.expanduser(myfile.strip())) != False:
        return os.path.abspath(os.path.expanduser(myfile.strip()))
    else:
        print 'You can\'t save to that location'
        sys.exit()

def adapt_decimal(d):
    return str(d)

def convert_decimal(s):
    return D(s)

def isFloat(num):
    try:
        return float(num)
    except:
        return 0

def is_number(s):
    try:
        return float(s)
    except:
        return False

def is_number_none(s):
    try:
        return float(s)
    except:
        return None


def isInt(num):
    try:
        return int(round(float(num)))
    except:
        return False

def intNone(num):
    try:
        return int(round(float(num)))
    except:
        return None

def isDec(num):
    try:
        return D(str(num))
    except:
        return D('0')


def AddMethods(connection):
    c = connection.cursor()
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (1,'Fill in single missing value')")
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (2,'Midpoint of bin')")
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (3,'Fill in remaining cells by weighting')")
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (4,'Fill based on constraints')")
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (5,'Fill based on proportions')")
    c.execute("INSERT OR IGNORE INTO Methods (id,Description) VALUES (6,'Fill based on super code weight')")
    connection.commit()
    c.close()

def CreateTables(connection):
    c = connection.cursor()
    c.execute("CREATE TABLE if not exists StateWeights(State TEXT NOT NULL, StateShort TEXT NOT NULL, Employment REAL NOT NULL, DataYear INTEGER NOT NULL, UNIQUE(StateShort, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists StateData(StateShort TEXT NOT NULL, id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, IndustryCode TEXT NOT NULL, IndustryCodeLength INTEGER NOT NULL DEFAULT 0, AtMost REAL, AtLeast REAL, ExpEmpValue REAL, DataYear INTEGER NOT NULL, UNIQUE(StateShort, IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists NationalData(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, IndustryCode TEXT NOT NULL, IndustryCodeLength INTEGER NOT NULL DEFAULT 0, AtMost REAL, AtLeast REAL, ExpEmpValue REAL, DataYear INTEGER NOT NULL, BinLower INTEGER, BinHigher INTEGER)")
    c.execute("CREATE TABLE if not exists Notes(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, NationalDataID INTEGER, StateDataID INTEGER, MethodsID INTEGER NOT NULL)")
    c.execute("CREATE TABLE if not exists Methods(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, Description TEXT UNIQUE)")
    c.execute("CREATE TABLE if not exists LookupValues(LKey TEXT PRIMARY KEY NOT NULL, LowerValue INTEGER, HigherValue INTEGER)")
    c.execute("CREATE TABLE if not exists HHI(IndustryCode TEXT NOT NULL, DataYear INTEGER NOT NULL, Herf REAL, UNIQUE(IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists Plants(IndustryCode TEXT NOT NULL, DataYear INTEGER NOT NULL, NPlants REAL, UNIQUE(IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists Gamma(IndustryCode TEXT NOT NULL, DataYear INTEGER NOT NULL, Gamma REAL, UNIQUE(IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists Gini(IndustryCode TEXT NOT NULL, DataYear INTEGER NOT NULL, Gini REAL, UNIQUE(IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("CREATE TABLE if not exists Sim(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, Plants INTEGER NOT NULL, Sigma REAL, DataYear INTEGER NOT NULL, cvlow REAL, cvhigh REAL, hlow REAL, hhigh REAL)")
    c.execute("CREATE TABLE if not exists Sig(IndustryCode TEXT NOT NULL, DataYear INTEGER NOT NULL, YesSig INTEGER NOT NULL DEFAULT 0, UNIQUE(IndustryCode, DataYear) ON CONFLICT REPLACE)")
    c.execute("""
          CREATE TRIGGER if not exists update_of_national_industry_code AFTER UPDATE OF IndustryCode ON NationalData
            FOR EACH ROW
            BEGIN
               UPDATE NationalData SET IndustryCodeLength = LENGTH(NEW.IndustryCode) WHERE NationalData.id=NEW.id;
            END
     """)
    c.execute("""
        CREATE TRIGGER if not exists insert_of_national_industry_code AFTER INSERT ON NationalData
           FOR EACH ROW
           BEGIN
             UPDATE NationalData SET IndustryCodeLength = LENGTH(NEW.IndustryCode) WHERE NationalData.id=NEW.id;
           END
    """)
    c.execute("""
          CREATE TRIGGER if not exists update_of_state_industry_code AFTER UPDATE OF IndustryCode ON StateData
            FOR EACH ROW
            BEGIN
               UPDATE StateData SET IndustryCodeLength = LENGTH(NEW.IndustryCode) WHERE StateData.id=NEW.id;
            END
    """)
    c.execute("""
        CREATE TRIGGER if not exists insert_of_state_industry_code AFTER INSERT ON StateData
           FOR EACH ROW
           BEGIN
             UPDATE StateData SET IndustryCodeLength = LENGTH(NEW.IndustryCode) WHERE StateData.id=NEW.id;
           END
    """)
    connection.commit()
    c.close()

def CreateRowSW(connection,state,short,emp,year):
    c = connection.cursor()
    if is_number_none(emp) != None:
        emp = isFloat(emp)
    else:
        emp = None
    
    mylist = (state,short,emp,year)
    if year != False and len(state)>0 and len(short)>0 and emp != None:
        c.execute('INSERT INTO StateWeights(State,StateShort,Employment,DataYear) VALUES(?,?,?,?)',mylist)
        connection.commit()
    c.close()

def ReadWeights(connection,filename):
    if os.path.isfile(os.path.abspath(os.path.expanduser(filename))) != False:
        reader = csv.DictReader(open(os.path.abspath(os.path.expanduser(filename)), 'rU'))
        for row in reader:
            state = ''
            stateshort = ''
            emp = None
            year = False
            for name in row.keys():
                if name.lower() == 'state' and len(unicode(str(row.get(name,'')).strip().lower(), "utf8"))>2:
                    state = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower()=='st' or name.lower()=='state short':
                    stateshort = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower() == 'employment':
                    emp = row.get(name,'')
                elif name.lower() == 'year':
                    year = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
            CreateRowSW(connection,state,stateshort,emp,year)
    else:
        sys.exit("Bad filename: " + filename)


def CreateRowKeys(connection,key,lvalue,hvalue):
    c = connection.cursor()
    key = key.replace("(","").replace(")","").replace("[","").replace("]","").strip().lower()
    lvalue = intNone(lvalue)
    hvalue = intNone(hvalue)    
    mylist = (key,lvalue,hvalue)
    if len(key)>0:
        c.execute('INSERT OR IGNORE INTO LookupValues(LKey,LowerValue,HigherValue) VALUES(?,?,?)',mylist)
        connection.commit()
    c.close()


def ReadKey(connection,filename):
    if os.path.isfile(os.path.abspath(os.path.expanduser(filename))) != False:
        reader = csv.DictReader(open(os.path.abspath(os.path.expanduser(filename)), 'rU'))
        for row in reader:
            key = ''
            lValue = None
            hValue = None
            for name in row.keys():
                if name.lower() == 'key' or name.lower() == 'code':
                    key = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower() == 'lower' or name.lower() == 'lower value' or name.lower() == 'bin low':
                    lValue = row.get(name,'')
                elif name.lower() == 'higher' or name.lower() == 'higher value' or name.lower() == 'bin high':
                    hValue = row.get(name,'')
            CreateRowKeys(connection,key,lValue,hValue)
    else:
        sys.exit("Bad filename: " + filename)

def CreateRowStateData(connection,state,code,emp,lvalue,hvalue,year):
    c = connection.cursor()
    mylist = (state,str(code),hvalue,lvalue,emp,year)
    if year != False and len(state)>0 and len(code)>0:
        c.execute('INSERT INTO StateData(StateShort,IndustryCode,AtMost,AtLeast,ExpEmpValue,DataYear) VALUES(?,?,?,?,?,?)',mylist)
        connection.commit()
    c.close()

def ReadStateData(connection,filename):
    if os.path.isfile(os.path.abspath(os.path.expanduser(filename))) != False:
        reader = csv.DictReader(open(os.path.abspath(os.path.expanduser(filename)), 'rU'))
        for row in reader:
            state = ''
            code = ''
            atmost = None
            atleast = None
            emp = None
            year = False
            
            for name in row.keys():
                if name.lower() == 'state':
                    state = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower() == 'code':
                    code = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower() =='year':
                    year = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'value' or name.lower()=='employment':
                    emp = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                    if intNone(emp) != None:
                        emp = isInt(emp)
                        atmost = emp
                        atleast = emp
                    else:
                        atleast, atmost = LookupValue(connection,emp)
                        emp = None
            CreateRowStateData(connection,state,code,emp,atleast,atmost,year)
    else:
        sys.exit("Bad filename: " + filename)



def CreateRowNationalData(connection,code,emp,lvalue,hvalue,year,blow,bhigh):
    c = connection.cursor()
    if blow == False or blow==0:
        blow = None
    if bhigh == False or bhigh==0:
        bhigh = None
    mylist = (str(code),hvalue,lvalue,emp,year,blow,bhigh)
    if year != False and len(code)>0:
        c.execute('INSERT INTO NationalData(IndustryCode,AtMost,AtLeast,ExpEmpValue,DataYear,BinLower,BinHigher) VALUES(?,?,?,?,?,?,?)',mylist)
        connection.commit()
    c.close()


def CreateRowSim(connection,plants,sigma,cvlow,cvhigh,hlow,hhigh,year):
    c = connection.cursor()
    mylist = (plants,sigma,year,cvlow,cvhigh,hlow,hhigh)
    c.execute('INSERT INTO Sim(Plants,Sigma,DataYear,cvlow,cvhigh,hlow,hhigh) VALUES(?,?,?,?,?,?,?)',mylist)
    connection.commit()

def LoadSim(connection,filename,year):
    if os.path.isfile(os.path.abspath(os.path.expanduser(filename))) != False:
        reader = csv.DictReader(open(os.path.abspath(os.path.expanduser(filename)), 'rU'))
        for row in reader:
            plants = None
            sigma = None
            cvlow = None
            cvhigh = None
            hlow = None
            hhigh = None
            for name in row.keys():
                if name.lower() == 'plants' or name.lower() == 'numberoffirms':
                    plants = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'sigma' or name.lower() == 'stdev':
                    sigma = isFloat(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'cvlow' or name.lower() == 'c005' or name.lower() == 'cv05':
                    cvlow = isFloat(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'cvhigh' or name.lower() == 'c095' or name.lower() == 'cv95':
                    cvhigh = isFloat(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'hlow' or name.lower() == 'hc0025':
                    hlow = isFloat(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'hhigh' or name.lower() == 'hc0975':
                    hhigh = isFloat(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
            if plants != None and plants != False and sigma != None and cvlow != None and cvhigh != None and hlow != None and hhigh != None and sigma!=0 and cvlow != 0 and cvhigh != 0 and hlow != 0 and hhigh != 0:
                CreateRowSim(connection,plants,sigma,cvlow,cvhigh,hlow,hhigh,year)

def ReadNationalData(connection,filename):
    if os.path.isfile(os.path.abspath(os.path.expanduser(filename))) != False:
        reader = csv.DictReader(open(os.path.abspath(os.path.expanduser(filename)), 'rU'))
        for row in reader:
            code = ''
            atmost = None
            atleast = None
            emp = None
            year = False
            blow = None
            bhigh = None
            for name in row.keys():
                if name.lower() == 'code':
                    code = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                elif name.lower() =='year':
                    year = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'bin low' or name.lower() == 'bin lower':
                    blow = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'bin high' or name.lower() == 'bin higher':
                    bhigh = isInt(unicode(str(row.get(name,'')).strip().lower(), "utf8"))
                elif name.lower() == 'value' or name.lower()=='employment':
                    emp = unicode(str(row.get(name,'')).strip().lower(), "utf8")
                    if intNone(emp) != None:
                        emp = isInt(emp)
                        atmost = emp
                        atleast = emp
                    else:
                        atleast, atmost = LookupValue(connection,emp)
                        emp = None
            CreateRowNationalData(connection,code,emp,atleast,atmost,year,blow,bhigh)
    else:
        sys.exit("Bad filename: " + filename)



def LookupValue(connection,key):
    key = key.replace("(","").replace(")","").replace("[","").replace("]","").strip().lower()
    c = connection.cursor()
    hvalue = None
    lvalue = None
    c.execute('SELECT LowerValue, HigherValue FROM LookupValues WHERE LKey=? LIMIT 1', (key,))
    for row in c:
        lvalue= row[0]
        hvalue= row[1]
    c.close()
    return (lvalue,hvalue)

def ExportState(conn, filename):
    if os.path.abspath(os.path.expanduser(filename)) != False:
        with open(os.path.abspath(os.path.expanduser(filename)), "wb") as f:
            fileWriter = csv.writer(f, delimiter=',',quoting=csv.QUOTE_MINIMAL)
            c = conn.cursor()
            header = ['State Short','State','Year','State Employment Share','Industry Code','At Most', 'At Least','Industry Employment','Notes']
            print "Exporting ..."
            fileWriter.writerow(header)
            c.execute("""
            SELECT StateData.StateShort AS StateShort, StateWeights.State AS State, StateData.DataYear As DataYear, StateWeights.Employment AS EmploymentShare, StateData.IndustryCode AS IndustryCode, StateData.AtMost AS AtMost, StateData.AtLeast AS AtLeast, StateData.ExpEmpValue AS ExpEmpValue, (
                SELECT GROUP_CONCAT(Methods.Description) AS FillNotes FROM Notes AS Notes JOIN Methods AS Methods ON Notes.MethodsID=Methods.id WHERE Notes.StateDataID=StateData.id
                GROUP BY Notes.StateDataID
            )  FROM StateData AS StateData LEFT JOIN StateWeights AS StateWeights ON StateData.StateShort=StateWeights.StateShort AND StateData.DataYear=StateWeights.DataYear
            """)
            for row in c:
                fileWriter.writerow(row)
            c.close()
    else:
        print "Could not write to that location " + str(os.path.abspath(os.path.expanduser(filename)))


def ExportNational(conn, filename):
    if os.path.abspath(os.path.expanduser(filename)) != False:
        with open(os.path.abspath(os.path.expanduser(filename)), "wb") as f:
            fileWriter = csv.writer(f, delimiter=',',quoting=csv.QUOTE_MINIMAL)
            c = conn.cursor()
            header = ['Year','Industry Code','At Most', 'At Least','Industry Employment','Bin Low','Bin High','Notes','HHI','Gamma']
            print "Exporting ..."
            fileWriter.writerow(header)
            c.execute("""
            SELECT NationalData.DataYear As DataYear, NationalData.IndustryCode AS IndustryCode, NationalData.AtMost AS AtMost, NationalData.AtLeast AS AtLeast, NationalData.ExpEmpValue AS ExpEmpValue, NationalData.BinLower AS BinLow, NationalData.BinHigher AS BinHigh,  (
                SELECT GROUP_CONCAT(Methods.Description) AS FillNotes FROM Notes AS Notes JOIN Methods AS Methods ON Notes.MethodsID=Methods.id WHERE Notes.NationalDataID=NationalData.id
                GROUP BY Notes.NationalDataID
            ), HHI.Herf AS herf, Gamma.Gamma AS g FROM NationalData AS NationalData LEFT JOIN HHI ON NationalData.DataYear=HHI.DataYear AND NationalData.IndustryCode=HHI.IndustryCode LEFT JOIN Gamma ON NationalData.DataYear=Gamma.DataYear AND NationalData.IndustryCode=Gamma.IndustryCode
            """)
            for row in c:
                fileWriter.writerow(row)
            c.close()
    else:
        print "Could not write to that location " + str(os.path.abspath(os.path.expanduser(filename)))


'''
def CheckForNationalNullData(conn,year):
    c = conn.cursor()
    c.execute("SELECT (AtMost-AtLeast) AS MidPoint, id FROM NationalData WHERE ExpEmpValue IS NULL AND BinLower IS NULL AND BinHigher IS NULL")
    for row in c:
        id = row[1]
        value = row[0]
        if isInt(id) != False and isFloat(value)>0:
            UpdateNationalNullRow(conn,id,value)

def UpdateNationalNullRow(conn,id,rowemployment):
    c = conn.cursor()
    mylist = (rowemployment,id)
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT INTO Notes(NationalDataID,MethodsID) VALUES(?,2)',(id,))
    conn.commit()
    c.close()
'''


def FillNationalCodeYear(conn,code,year):
    c = conn.cursor()
    totalEmpl = False
    constraint = False
    c.execute("SELECT ExpEmpValue, BinHigher FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NOT NULL ORDER BY BinHigher ASC",(year,str(code)))
    results = c.fetchall()
    c.close()
    for row in results:
        totalEmpl=row[0]
        constraint=row[1]
        if intNone(constraint) != None and is_number_none(totalEmpl) != None:
            CalcNationalFill(conn,year,code,constraint,totalEmpl)
    c = conn.cursor()
    c.execute("SELECT ExpEmpValue FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NULL AND BinHigher IS NULL AND ExpEmpValue IS NOT NULL ORDER BY ExpEmpValue DESC LIMIT 1",(year,str(code)))
    results = c.fetchall()
    c.close()
    for row in results:
        totalEmpl=row[0]
        if is_number_none(totalEmpl) != None:
            CalcNationalFill(conn,year,code,False,totalEmpl)

def FindMostCorrectProportion(conn,code,binhigh,binlow,year,basecode):
    if len(code)>1:
        c = conn.cursor()
        if binhigh != None:
            c.execute("SELECT MAX(nd.ExpEmpValue) AS emp FROM NationalData AS nd WHERE nd.ExpEmpValue IS NOT NULL AND nd.IndustryCode=? AND nd.DataYear=? AND nd.BinLower=? AND nd.BinHigher=? GROUP BY nd.IndustryCode",(str(code),year,binlow,binhigh))
        else:
            c.execute("SELECT MAX(nd.ExpEmpValue) AS emp FROM NationalData AS nd WHERE nd.ExpEmpValue IS NOT NULL AND nd.IndustryCode=? AND nd.DataYear=? AND nd.BinLower=? AND nd.BinHigher IS NULL GROUP BY nd.IndustryCode",(str(code),year,binlow))
        results = c.fetchall()
        
        emp = None
        temp = None
        for row in results:
            emp = row[0]
        if emp != None:
            c.execute("SELECT TOTAL(nd.ExpEmpValue) AS temp FROM NationalData AS nd JOIN NationalData AS ndt ON nd.BinLower=ndt.BinLower AND (nd.BinHigher=ndt.BinHigher OR (nd.BinHigher IS NULL AND ndt.BinHigher IS NULL)) AND nd.DataYear=ndt.DataYear AND ndt.ExpEmpValue IS NULL AND ndt.BinLower IS NOT NULL AND ndt.IndustryCode=? WHERE nd.ExpEmpValue IS NOT NULL AND nd.IndustryCode=? AND nd.DataYear=? GROUP BY nd.IndustryCode",(str(basecode),str(code),year))
            results = c.fetchall()
            for row in results:
                temp = row[0]
            if temp > 0:
                c.close()
                return emp/temp
        c.close()
        return FindMostCorrectProportion(conn,code[:-1],binhigh,binlow,year,basecode)
    else:
        return None


def CalcNationalFill(conn,year,code,constraint,totalEmpl):
    c = conn.cursor()
    knownEmployment = 0
    if constraint != False:
        c.execute("SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL GROUP BY IndustryCode",(year,str(code),constraint))
    else:
        c.execute("SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL GROUP BY IndustryCode",(year,str(code)))
    results = c.fetchall()
    for row in results:
        knownEmployment = row[0]

    if isFloat(totalEmpl-knownEmployment)>0:
        remaining = totalEmpl-knownEmployment
        if constraint != False:
            c.execute("SELECT COUNT(*) AS c, MAX(id) AS id, MAX(BinLower) AS bl FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL GROUP BY IndustryCode",(year,str(code),constraint))
        else:
            c.execute("SELECT COUNT(*) AS c, MAX(id) AS id, MAX(BinLower) AS bl FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL GROUP BY IndustryCode",(year,str(code)))
        results = c.fetchall()
        for row in results:
            if row[0]==1:
                id = row[1]
                bl = row[2]
                if bl > remaining:
                    remaining = 0
                if isInt(id) != False:
                    c.close()
                    UpdateNationalRowOne(conn,id,remaining)
                    return None
        if constraint != False:
            c.execute("SELECT COUNT(*) AS c FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL AND AtLeast IS NULL AND AtMost IS NULL GROUP BY IndustryCode",(year,str(code),constraint))
        else:
            c.execute("SELECT COUNT(*) AS c FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL AND AtMost IS NULL AND AtLeast IS NULL GROUP BY IndustryCode",(year,str(code)))
        results = c.fetchall()
        for rows in results:
            if isInt(rows[0])==0:
                if constraint != False:
                    c.execute("SELECT TOTAL(AtMost) AS AtMost, TOTAL(AtLeast) AS AtLeast, COUNT(*) AS c FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL GROUP BY IndustryCode",(year,str(code),constraint))
                else:
                    c.execute("SELECT TOTAL(AtMost) AS AtMost, TOTAL(AtLeast) AS AtLeast, COUNT(*) AS c FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL GROUP BY IndustryCode",(year,str(code)))
                resultstwo = c.fetchall()
                for row in resultstwo:
                    mostEmp = row[0]
                    leastEmp = row[1]
                    empRange = mostEmp-leastEmp
                if isFloat(empRange)>0:
                    perPoint = remaining/empRange
                    if perPoint > 0:
                        if constraint != False:
                            c.execute("SELECT AtMost, AtLeast, id, BinLower FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL",(year,str(code),constraint))
                        else:
                            c.execute("SELECT AtMost, AtLeast, id, BinLower FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL",(year,str(code)))
                        for row in c:
                            if row[0] != None:
                                mostEmp = row[0]
                            if row[1] != None:
                                leastEmp = row[1]
                            id = row[2]
                            bl = row[3]
                            rowemployment = isFloat(leastEmp+perPoint*(mostEmp-leastEmp))
                            if isInt(id) != False and rowemployment>bl:
                                UpdateNationalRow(conn,id,rowemployment)
                            elif isInt(id) != False:
                                UpdateNationalRow(conn,id,bl)
        if constraint != False:
            c.execute("SELECT (AtMost+AtLeast)/2 AS midpoint, id, BinLower, AtLeast, AtMost FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL AND AtLeast IS NOT NULL AND AtMost IS NOT NULL ORDER BY (AtMost-AtLeast) ASC LIMIT 1",(year,str(code),constraint))
        else:
            c.execute("SELECT (AtMost+AtLeast)/2 AS midpoint, id, BinLower, AtLeast, AtMost FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL AND AtLeast IS NOT NULL AND AtMost IS NOT NULL ORDER BY (AtMost-AtLeast) ASC LIMIT 1",(year,str(code)))
        results = c.fetchall()
        for row in results:
            id = row[1]
            emp = row[0]
            bl = row[2]
            atleast = row[3]
            atmost = row[4]
            if emp>remaining:
                emp = remaining
            if emp<atleast:
                emp = atleast
            if emp>atmost:
                emp = atmost
            if isInt(id) != False:
                c.close()
                UpdateNationalRowMidpoint(conn,id,emp)
                CalcNationalFill(conn,year,code,constraint,totalEmpl)
                return None
        
        if constraint != False:
            c.execute("SELECT id, BinLower, BinHigher FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL ORDER BY BinLower DESC",(year,str(code),constraint))
        else:
            c.execute("SELECT id, BinLower, BinHigher FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL ORDER BY BinLower DESC",(year,str(code)))
        results = c.fetchall()
        for row in results:
            id = row[0]
            blower = row[1]
            bhigher = row[2]
            prop = FindMostCorrectProportion(conn,code[:-1],bhigher,blower,year,code)
            if prop != None:
                emp = remaining*prop
                if emp > blower:
                    UpdateNationalRowW(conn,id,emp)
                else:
                    UpdateNationalRowW(conn,id,0)
                c.close()
                CalcNationalFill(conn,year,code,constraint,totalEmpl)
                return None

        if constraint != False:
            c.execute("SELECT id, BinLower FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL ORDER BY BinLower DESC",(year,str(code),constraint))
        else:
            c.execute("SELECT id, BinLower FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL ORDER BY BinLower DESC",(year,str(code)))
        results = c.fetchall()
        if len(results)>0:
            valueper = remaining/len(results)
            for row in results:
                id = row[0]
                bl = row[1]
                if isInt(id) != False and valueper>bl:
                    UpdateNationalRowLast(conn,id,valueper)
                elif isInt(id) != False:
                    UpdateNationalRowLast(conn,id,0)
                    c.close()
                    CalcNationalFill(conn,year,code,constraint,totalEmpl)
                    return None
        c.close()
        return None
    else:
        if constraint != False:
            c.execute("SELECT id FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinHigher<=? AND BinLower IS NOT NULL AND BinHigher IS NOT NULL AND ExpEmpValue IS NULL",(year,str(code),constraint))
        else:
            c.execute("SELECT id FROM NationalData WHERE DataYear=? AND IndustryCode=? AND BinLower IS NOT NULL AND ExpEmpValue IS NULL",(year,str(code)))
        results = c.fetchall()
        for row in results:
            id = row[0]
            if isInt(id) != False:
                UpdateNationalRow(conn,id,0)
        c.close()


def UpdateNationalRow(conn,id,rowemployment):
    c = conn.cursor()
    mylist = (rowemployment,id)
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT OR IGNORE INTO Notes(NationalDataID,MethodsID) VALUES(?,4)',(id,))
    conn.commit()
    c.close()

def UpdateNationalRowMidpoint(conn,id,rowemployment):
    c = conn.cursor()
    mylist = (rowemployment,id)
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT OR IGNORE INTO Notes(NationalDataID,MethodsID) VALUES(?,2)',(id,))
    conn.commit()
    c.close()

def UpdateNationalRowOne(conn,id,rowemployment):
    c = conn.cursor()

    c.execute('SELECT AtLeast, AtMost FROM NationalData WHERE id=? AND ExpEmpValue IS NULL',(id,))

    for row in c:
        atleast = row[0]
        atmost = row[1]

    if isFloat(atleast)>rowemployment:
        rowemployment = isFloat(atleast)
    
    if isFloat(atmost)<rowemployment and isFloat(atmost)>0:
        rowemployment = isFloat(atmost)
    
    mylist = (rowemployment,id)
    
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT OR IGNORE INTO Notes(NationalDataID,MethodsID) VALUES(?,1)',(id,))
    conn.commit()
    c.close()
    
def UpdateNationalRowLast(conn,id,rowemployment):
    c = conn.cursor()
    mylist = (rowemployment,id)
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT OR IGNORE INTO Notes(NationalDataID,MethodsID) VALUES(?,5)',(id,))
    conn.commit()
    c.close()

def UpdateNationalRowW(conn,id,rowemployment):
    c = conn.cursor()
    mylist = (rowemployment,id)
    c.execute('UPDATE NationalData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',mylist)
    c.execute('INSERT OR IGNORE INTO Notes(NationalDataID,MethodsID) VALUES(?,6)',(id,))
    conn.commit()
    c.close()


def FillNationalData(conn,year):
    IcodeL = 2
    while IcodeL < 7:
        if isInt(year) != False:
            c = conn.cursor()
            c.execute("SELECT IndustryCode FROM NationalData WHERE DataYear=? AND IndustryCodeLength=? GROUP BY IndustryCode",(year,IcodeL))
            results = c.fetchall()
            c.close()
            for row in results:
                code = row[0]
                if isInt(code) != False:
                    FillNationalCodeYear(conn,code,year)
        IcodeL = IcodeL+1


def FindHerfAndGamma(conn,year):
    herfgammaList = []
    codelengthtoadd = None
    if isInt(year) != False:
        c = conn.cursor()
        c.execute('SELECT MAX(IndustryCodeLength) AS c FROM NationalData WHERE DataYear=? and IndustryCodeLength<7',(year,))
        codelengthtoadd = c.fetchone()[0]
        c.close()
    if isInt(codelengthtoadd) != False:
        shortcode = isInt(isInt(codelengthtoadd)/2)
        herfgammaList.append(shortcode)
        herfgammaList.append(isInt(codelengthtoadd))
    else:
        herfgammaList.append(2)
        herfgammaList.append(3)
        herfgammaList.append(4)
        herfgammaList.append(6)
    for codeL in herfgammaList:
        if isInt(year) != False:
            c = conn.cursor()
            c.execute("SELECT IndustryCode FROM NationalData WHERE DataYear=? AND IndustryCodeLength=? GROUP BY IndustryCode",(year,codeL))
            results = c.fetchall()
            c.close()
            for row in results:
                code = row[0]
                if isInt(code) != False:
                    herf, plantcount = CalculateHerf(conn,code,year)
                    if is_number_none(herf) != None and is_number_none(plantcount) != None:
                        InsertHerf(conn,code,year,herf)
                        InsertPlants(conn,code,year,plantcount)
                        gamma, gini = CalcGamma(conn,code,year,herf)
                        if gamma != None:
                            InsertGamma(conn,code,year,gamma)
                        if gini != None:
                            InsertGini(conn,code,year,gini)
    return None
                   



def StateTopFill(conn,year,code,supercode,totalEmployment,codeLength):
    if codeLength>2:
        c = conn.cursor()
        supercodeLength = codeLength-1
        c.execute('SELECT COUNT(id) AS c, StateShort FROM StateData WHERE DataYear=? AND IndustryCodeLength=? AND SUBSTR(IndustryCode,1,?)=? AND ExpEmpValue IS NULL GROUP BY StateShort',(year,codeLength,supercodeLength,str(supercode)))
        results = c.fetchall()
        c.close()
        for row in results:
            foundNulls = row[0]
            foundState = row[1]
            c = conn.cursor()
            c.execute('SELECT id FROM StateData WHERE DataYear=? AND IndustryCodeLength=? AND ExpEmpValue IS NOT NULL AND IndustryCode=? AND StateShort=?',(year,supercodeLength,supercode,foundState))
            resultstwo = c.fetchall()
            c.close()
            topCodeExists = False
            for rowtwo in resultstwo:
                topCodeExists = True    
            if foundNulls == 1 and topCodeExists==True:
                UpdateTopFillState(conn,year,codeLength,supercodeLength,supercode,foundState)
                WalkSuperCode(conn,year,supercode,codeLength)
                return None
            elif foundNulls == 1 and topCodeExists==False:
                logging.warning('Top code does not exist! Year: ' +  str(year) + ' Code: ' + str(supercode) + " State: " + str(foundState))
    c = conn.cursor()
    c.execute('SELECT COUNT(id) AS c, MAX(id) AS cid FROM StateData WHERE DataYear=? AND IndustryCode=? AND ExpEmpValue IS NULL',(year,str(code)))
    results = c.fetchall()
    c.close()
    for row in results:
        foundNulls = row[0]
        foundNullsId = row[1]
        if foundNulls == 1:
            UpdateTopFillTotalEmployment(conn,year,code,totalEmployment,foundNullsId)
            if codeLength>2:
                WalkSuperCode(conn,year,supercode,codeLength)
            else:
                StateTopFill(conn,year,code,supercode,totalEmployment,codeLength)
                #WeightFill(conn,year,code,totalEmployment,codeLength)
            return None


def WalkSuperCode(conn,year,code,codeLength):
    c = conn.cursor()
    supercodeLength = codeLength-1
    c.execute('SELECT StateData.IndustryCode AS code, MAX(NationalData.ExpEmpValue) AS NationalEmployment FROM StateData JOIN NationalData ON StateData.IndustryCode=NationalData.IndustryCode AND StateData.DataYear=NationalData.DataYear AND NationalData.BinHigher IS NULL AND NationalData.BinHigher IS NULL WHERE StateData.DataYear=? AND StateData.IndustryCodeLength=? AND SUBSTR(StateData.IndustryCode,1,?)=? GROUP BY StateData.IndustryCode',(year,codeLength,supercodeLength,str(code)))
    results = c.fetchall()
    c.close()
    for row in results:
        scode = row[0]
        nationalEmployment = row[1]
        if isInt(scode)>0 and is_number(nationalEmployment):
            StateTopFill(conn,year,scode,code,nationalEmployment,codeLength)
            #WeightFill(conn,year,scode,nationalEmployment,codeLength)
    return None


def UpdateTopFillTotalEmployment(conn,year,code,totalEmployment,foundNullsId):
    c = conn.cursor()
    knownemployment = 0
    c.execute('SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM StateData WHERE DataYear=? AND IndustryCode=?',(year,str(code)))
    for row in c:
        knownemployment = row[0]
    remainingEmployment = isFloat(totalEmployment-knownemployment)
    
    c.execute('SELECT AtLeast, AtMost FROM StateData WHERE id=? AND ExpEmpValue IS NULL',(foundNullsId,))

    for row in c:
        atleast = row[0]
        atmost = row[1]

    if isFloat(atleast)>remainingEmployment:
        remainingEmployment = isFloat(atleast)
    
    if isFloat(atmost)<remainingEmployment and isFloat(atmost)>0:
        remainingEmployment = isFloat(atmost)
    
    if remainingEmployment<0:
        remainingEmployment = 0
    c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(remainingEmployment,foundNullsId))
    c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,1)',(foundNullsId,))
    conn.commit()


def UpdateTopFillState(conn,year,codeLength,supercodeLength,supercode,foundState):
    totalEmployment = 0
    knownEmployment = 0
    isTopCode = False
    c = conn.cursor()
    c.execute('SELECT id FROM StateData WHERE DataYear=? AND IndustryCodeLength=? AND SUBSTR(IndustryCode,1,?)=? AND ExpEmpValue IS NULL AND StateShort=?',(year,codeLength,supercodeLength,str(supercode),foundState))
    id = None
    for row in c:
        id = row[0]
    c.execute('SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM StateData WHERE DataYear=? AND IndustryCodeLength=? AND SUBSTR(IndustryCode,1,?)=? AND StateShort=?',(year,codeLength,supercodeLength,str(supercode),foundState))
    for row in c:
        knownEmployment = row[0]
    c.execute('SELECT ExpEmpValue, AtLeast, AtMost FROM StateData WHERE DataYear=? AND IndustryCode=? AND StateShort=? AND ExpEmpValue IS NOT NULL',(year,str(supercode),foundState))
    for row in c:
        isTopCode = True
        totalEmployment = row[0]
        atleast = isFloat(row[1])
        atmost = isFloat(row[2])
    
    
    remainingEmployment = isFloat(totalEmployment-knownEmployment)
    
    if remainingEmployment<0:
        remainingEmployment = 0
    
    c.execute('SELECT AtLeast, AtMost FROM StateData WHERE id=? AND ExpEmpValue IS NULL',(id,))

    for row in c:
        atleast = row[0]
        atmost = row[1]
    
    
    if isFloat(atleast)>remainingEmployment:
        remainingEmployment = isFloat(atleast)
    
    if isFloat(atmost)<remainingEmployment and isFloat(atmost)>0:
        remainingEmployment = isFloat(atmost)
    
    if isTopCode == True:
        c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(remainingEmployment,id))
        c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,1)',(id,))
        conn.commit()
    else:
        print 'Top code error (in update)! Code: ' + str(supercode) + " State: " + str(foundState)
    c.close()



def FillStateData(conn,year):
    IcodeL = 2
    while IcodeL < 7:
        if isInt(year) != False:
            c = conn.cursor()
            c.execute("""
            SELECT MyTable.IC AS TIC, MyTable.NationalEmployment AS TNE 
            FROM
            (SELECT n.IndustryCode AS IC, MAX(n.ExpEmpValue) AS NationalEmployment,n.DataYear AS DY
            FROM NationalData AS n WHERE n.DataYear=? AND n.IndustryCodeLength=? AND n.BinHigher IS NULL AND n.BinLower IS NULL GROUP BY n.IndustryCode) AS MyTable
            JOIN
            (
              SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS nTB ON MyTable.IC=nTB.sIC AND MyTable.DY=nTB.sDataYear
            JOIN  
            (
             SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS cCTB ON MyTable.IC=cCTB.sIC AND MyTable.DY=cCTB.sDataYear
            ORDER BY nTB.cid ASC, cCTB.cid DESC
            """,(year,IcodeL))
            results = c.fetchall()
            c.close()
            for row in results:
                code = row[0]
                totalEmployment = row[1]
                if IcodeL>2:
                    supercode = code[:-1]
                else:
                    supercode = False
                if isInt(code) != False and is_number_none(totalEmployment) != None:
                    StateTopFill(conn,year,code,supercode,totalEmployment,IcodeL)
                    #WeightFill(conn,year,code,totalEmployment,IcodeL)
                else:
                    logging.warning('Code is not an int or total employment is zero! code:' + str(code) + " employment: " + str(totalEmployment) + " year: " + str(year))
            c = conn.cursor()
            c.execute("""
            SELECT MyTable.IC AS TIC, MyTable.NationalEmployment AS TNE 
            FROM
            (SELECT n.IndustryCode AS IC, MAX(n.ExpEmpValue) AS NationalEmployment,n.DataYear AS DY
            FROM NationalData AS n WHERE n.DataYear=? AND n.IndustryCodeLength=? AND n.BinHigher IS NULL AND n.BinLower IS NULL GROUP BY n.IndustryCode) AS MyTable
            JOIN
            (
              SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS nTB ON MyTable.IC=nTB.sIC AND MyTable.DY=nTB.sDataYear
            JOIN  
            (
             SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS cCTB ON MyTable.IC=cCTB.sIC AND MyTable.DY=cCTB.sDataYear
            ORDER BY nTB.cid ASC, cCTB.cid DESC
            """,(year,IcodeL))
            resultsthree = c.fetchall()
            c.close()
            for row in resultsthree:
                code = row[0]
                totalEmployment = row[1]
                if IcodeL>2:
                    supercode = code[:-1]
                else:
                    supercode = False
                if isInt(code) != False and is_number_none(totalEmployment) != None:
                    #StateTopFill(conn,year,code,supercode,totalEmployment,IcodeL)
                    WeightFill(conn,year,code,totalEmployment,IcodeL)
                else:
                    logging.warning('Code is not an int or total employment is zero! code:' + str(code) + " employment: " + str(totalEmployment) + " year: " + str(year))
            c = conn.cursor()
            c.execute("""
            SELECT MyTable.IC AS TIC, MyTable.NationalEmployment AS TNE 
            FROM
            (SELECT n.IndustryCode AS IC, MAX(n.ExpEmpValue) AS NationalEmployment,n.DataYear AS DY
            FROM NationalData AS n WHERE n.DataYear=? AND n.IndustryCodeLength=? AND n.BinHigher IS NULL AND n.BinLower IS NULL GROUP BY n.IndustryCode) AS MyTable
            JOIN
            (
              SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS nTB ON MyTable.IC=nTB.sIC AND MyTable.DY=nTB.sDataYear
            JOIN  
            (
             SELECT COUNT(s.id) AS cid, s.IndustryCode AS sIC, s.DataYear AS sDataYear FROM StateData AS s WHERE s.ExpEmpValue IS NULL GROUP BY s.IndustryCode, s.DataYear
            ) AS cCTB ON MyTable.IC=cCTB.sIC AND MyTable.DY=cCTB.sDataYear
            ORDER BY nTB.cid ASC, cCTB.cid DESC
            """,(year,IcodeL))
            resultstwo = c.fetchall()
            c.close()
            for rowtwo in resultstwo:                
                logging.warning('Code not cleared! Code: ' + str(row[0]) + " employment: " + str(totalEmployment) + " year: " + str(year))    
            IcodeL = IcodeL+1



def FindStateWeights(conn,code,year,num):
    c = conn.cursor()
    c.execute("""
    SELECT id, empW, atmost, atleast FROM
    (SELECT 1 AS ord, sd.id AS id, sw.ExpEmpValue AS empW, sd.AtMost AS atmost, sd.AtLeast AS atleast  FROM StateData AS sd JOIN StateData AS sw ON sd.DataYear=sw.DataYear AND sd.StateShort=sw.StateShort AND sw.IndustryCode=SUBSTR(sd.IndustryCode,1,LENGTH(sd.IndustryCode)-1) WHERE sd.DataYear=? AND sd.IndustryCode=? AND sd.ExpEmpValue IS NULL AND sd.AtMost IS NOT NULL AND sd.AtLeast IS NOT NULL AND sw.ExpEmpValue IS NOT NULL
    UNION
    SELECT 2 AS ord, sd.id AS id, sw.ExpEmpValue AS stateEmployment, sd.AtMost AS atmost, sd.AtLeast AS atleast FROM StateData AS sd JOIN StateData AS sw ON sd.DataYear=sw.DataYear AND sd.StateShort=sw.StateShort AND sw.IndustryCode=SUBSTR(sd.IndustryCode,1,LENGTH(sd.IndustryCode)-1) WHERE sd.DataYear=? AND sd.IndustryCode=? AND sd.ExpEmpValue IS NULL AND sd.AtMost IS NULL AND sd.AtLeast IS NOT NULL AND sw.ExpEmpValue IS NOT NULL
    UNION
    SELECT 3 AS ord, sd.id AS id, sw.ExpEmpValue AS stateEmployment, sd.AtMost AS atmost, sd.AtLeast AS atleast FROM StateData AS sd JOIN StateData AS sw ON sd.DataYear=sw.DataYear AND sd.StateShort=sw.StateShort AND sw.IndustryCode=SUBSTR(sd.IndustryCode,1,LENGTH(sd.IndustryCode)-1) WHERE sd.DataYear=? AND sd.IndustryCode=? AND sd.ExpEmpValue IS NULL AND sd.AtMost IS NULL AND sd.AtLeast IS NULL AND sw.ExpEmpValue IS NOT NULL) As mytable
    ORDER BY ord ASC, (atmost-atleast) ASC, empW ASC
    """,(year,str(code),year,str(code),year,str(code)))
    results = c.fetchall()
    c.close()
    if len(results)==num:
        return results
    elif len(code)>3:
        logging.warning('Recursive call on code: ' + str(code) + " year: " + str(year))
        return FindStateWeights(conn,code[:-1],year,num)
    else:
        logging.warning('No result on code: ' + str(code) + " year: " + str(year))
        return []


def WeightFill(conn,year,code,totalEmployment,codeLength):
    c = conn.cursor()
    constaintHit = False
    madeChanges = False
    baseUnallocated = 0
    knownEmployment = 0
    c.execute('SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM StateData WHERE DataYear=? AND IndustryCode=? GROUP BY IndustryCode',(year,str(code)))
    for row in c:
        knownEmployment = row[0]        
    remainingEmployment = isFloat(totalEmployment-knownEmployment)
    if remainingEmployment>0:
        c.execute("""
        SELECT id, stateEmployment, atmost, atleast FROM
        (
        SELECT 1 As ord, StateData.id AS id, StateWeights.Employment AS stateEmployment, StateData.AtMost AS atmost, StateData.AtLeast AS atleast FROM StateData JOIN StateWeights ON StateData.DataYear=StateWeights.DataYear AND StateData.StateShort=StateWeights.StateShort WHERE StateData.DataYear=? AND StateData.IndustryCode=? AND StateData.ExpEmpValue IS NULL AND StateData.AtMost IS NOT NULL AND StateData.AtLeast IS NOT NULL
        UNION 
        SELECT 2 AS ord, StateData.id AS id, StateWeights.Employment AS stateEmployment, StateData.AtMost AS atmost, StateData.AtLeast AS atleast FROM StateData JOIN StateWeights ON StateData.DataYear=StateWeights.DataYear AND StateData.StateShort=StateWeights.StateShort WHERE StateData.DataYear=? AND StateData.IndustryCode=? AND StateData.ExpEmpValue IS NULL AND StateData.AtMost IS NULL AND StateData.AtLeast IS NOT NULL
        UNION
        SELECT 3 AS ord, StateData.id AS id, StateWeights.Employment AS stateEmployment, StateData.AtMost AS atmost, StateData.AtLeast AS atleast FROM StateData JOIN StateWeights ON StateData.DataYear=StateWeights.DataYear AND StateData.StateShort=StateWeights.StateShort WHERE StateData.DataYear=? AND StateData.IndustryCode=? AND StateData.ExpEmpValue IS NULL AND StateData.AtMost IS NULL AND StateData.AtLeast IS NULL
        )
        ORDER BY ord ASC, (atmost-atleast) ASC, stateEmployment ASC
        """
        ,(year,str(code),year,str(code),year,str(code)))
        stateresults = c.fetchall()
        count = len(stateresults)
        if codeLength>2 and count>0:
            result = FindStateWeights(conn,code,year,count)
        else:
            result = []
        
        baseUnallocated = 0
        if len(result)==0 and count>0:
            result = stateresults
            if len(code)>2 and len(stateresults)>0:
                logging.warning('Using state results on code ' + str(code) + ' year ' + str(year))
        for row in result:
            weight = row[1]
            if is_number_none(weight) != None:
                baseUnallocated = baseUnallocated + weight
        
        for row in result:
            updateId = row[0]
            weight = row[1]
            atmost = isFloat(row[2])
            atleast = isFloat(row[3])
            if is_number_none(weight) != None:
                if baseUnallocated > 0:
                    stateEmployment = isFloat((weight/baseUnallocated)*remainingEmployment)
                else:
                    stateEmployment = 0
                if isFloat(atmost)>0 and stateEmployment>isFloat(atmost):
                    c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(atmost,updateId))
                    c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,4)',(updateId,))
                    conn.commit()
                    constaintHit = True
                    madeChanges = True
                if isFloat(atleast)>0 and stateEmployment<isFloat(atleast):
                    c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(atleast,updateId))
                    c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,4)',(updateId,))
                    conn.commit()
                    constaintHit = True
                    madeChanges = True
            else:
                logging.warning('Weight isn\'t a number: ' + str(weight) + ' code: ' +  str(code) +  ' year: ' + str(year) + ' id: '+ str(updateId))
                val = 0
                if isFloat(atleast)>0:
                    val = atleast
                c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(val,updateId))
                c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,4)',(updateId,))
                conn.commit()
                madeChanges = True
                constaintHit = True
            if constaintHit == True:
                c.close()
                if codeLength>2:
                    #WalkSuperCode(conn,year,code[:-1],codeLength)
                    WeightFill(conn,year,code,totalEmployment,codeLength)
                else:
                    WeightFill(conn,year,code,totalEmployment,codeLength)
                return None
        for row in result:
            updateId = row[0]
            weight = row[1]
            atmost = isFloat(row[2])
            atleast = isFloat(row[3])
            if is_number_none(weight) != None:
                if baseUnallocated>0:               
                    stateEmployment = isFloat((weight/baseUnallocated)*remainingEmployment)
                else:
                    stateEmployment = 0
                c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=? AND ExpEmpValue IS NULL',(stateEmployment,updateId))
                if isFloat(atleast)>0 or isFloat(atmost)>0:
                    c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,4)',(updateId,))
                else:
                    c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,3)',(updateId,))
                conn.commit()
                madeChanges = True

    else:
        c.execute('SELECT id, AtLeast FROM StateData WHERE DataYear=? AND IndustryCode=? AND ExpEmpValue IS NULL',(year,str(code)))
        results = c.fetchall()
        for row in results:
            updateId = row[0]
            atleast = row[1]
            val = 0
            if isFloat(atleast)>0:
                val = atleast
            c.execute('UPDATE StateData SET ExpEmpValue=? WHERE id=?',(val,updateId))
            c.execute('INSERT OR IGNORE INTO Notes(StateDataID,MethodsID) VALUES(?,4)',(updateId,))
            conn.commit()
            madeChanges = True
    if madeChanges == True:
        c.close()
        if codeLength>2:
            #WalkSuperCode(conn,year,code[:-1],codeLength)
            WeightFill(conn,year,code,totalEmployment,codeLength)
            return None
        else:
            WeightFill(conn,year,code,totalEmployment,codeLength)
            return None
        return None

def InsertHerf(conn,code,year,herf):
    c = conn.cursor()
    mylist = (str(code),year,herf)
    c.execute('INSERT OR IGNORE INTO HHI(IndustryCode, DataYear, Herf) VALUES(?,?,?)',mylist)
    conn.commit()
    c.close()

def InsertPlants(conn,code,year,plants):
    c = conn.cursor()
    mylist = (str(code),year,plants)
    c.execute('INSERT OR IGNORE INTO Plants(IndustryCode, DataYear, NPlants) VALUES(?,?,?)',mylist)
    conn.commit()
    c.close()

def InsertGamma(conn,code,year,gamma):
    c = conn.cursor()
    mylist = (str(code),year,gamma)
    c.execute('INSERT OR IGNORE INTO Gamma(IndustryCode, DataYear, Gamma) VALUES(?,?,?)',mylist)
    conn.commit()
    c.close()

def InsertGini(conn,code,year,gini):
    c = conn.cursor()
    mylist = (str(code),year,gini)
    c.execute('INSERT OR IGNORE INTO Gini(IndustryCode, DataYear, Gini) VALUES(?,?,?)',mylist)
    conn.commit()
    c.close()

def CalculateHerf(conn,code,year):
    c = conn.cursor()
    totalemp = 0
    totalplants = 0
    c.execute("SELECT TOTAL(ExpEmpValue) AS TotalEmployment FROM NationalData WHERE IndustryCode=? AND DataYear=? AND BinLower IS NOT NULL GROUP BY IndustryCode",(str(code),year))
    for row in c:
        totalemp = row[0]
    if totalemp==0:
        logging.info('ERROR: Total National Emp = 0? Code: ' + str(code) + ' year: ' + str(year))
        return (None, None)
    c.execute("SELECT ExpEmpValue, BinLower, BinHigher FROM NationalData WHERE IndustryCode=? AND DataYear=? AND BinLower IS NOT NULL AND ExpEmpValue IS NOT NULL ORDER BY BinLower ASC",(str(code),year))
    start=0
    slope=0
    for row in c:
        binlow = row[1]
        binhigh = row[2]
        employment = row[0]
        binshare = employment/totalemp
        if employment == 0:
            slope = 0
        elif binhigh == False or binhigh == None:
            if slope>0 and employment>0:
                insidesqrnum = binlow**2+2*employment*slope
                firms = (math.sqrt(insidesqrnum) - binlow)/slope
                binhigh = binlow + slope*firms
                delta = binhigh-binlow
                afirmsize = binshare/firms            
            else:
                firms=employment/binlow
                binhigh = binlow
                delta = 0
                afirmsize = binshare/firms 
        else:
            delta = binhigh-binlow
            midpoint = delta/2+binlow
            firms = employment/midpoint
            afirmsize=binshare/firms
            slope = delta/firms
        if employment>0:
            herfpartnum = (firms**2-1)*(delta/totalemp)**2
            herfpartdenum = 12*firms
            herfpart = firms*afirmsize**2+herfpartnum/herfpartdenum
            if herfpart>0:
                totalplants = totalplants + firms
                start = start + herfpart
            else:
                logging.info('Tried to calculate a negative HHI: Code:' + str(code) + " year: " + str(year))
    c.close()
    return (start, totalplants)


def CalculateX2(connection, year):
    c = connection.cursor()
    x2 =0
    c.execute("SELECT Employment FROM StateWeights WHERE DataYear=?",(year,))
    for row in c:
        x2 = x2 + row[0]**2
    c.close()
    return x2

def CalculateG(connection,year,code):
    c = connection.cursor()
    totalemp = 0
    c.execute('SELECT TOTAL(ExpEmpValue) AS KnownEmployment FROM StateData WHERE DataYear=? AND IndustryCode=? GROUP BY IndustryCode',(year,str(code)))
    for row in c:
        totalemp = row[0]
    if totalemp==0 or totalemp == None:
        logging.warning('Known state employment zero: code: ' + str(code) + ' year: ' + str(year))
        return None
    c.execute('SELECT StateWeights.Employment AS StateShare, StateData.ExpEmpValue AS InStateEmployment FROM StateData JOIN StateWeights ON StateData.DataYear=StateWeights.DataYear AND StateData.StateShort=StateWeights.StateShort WHERE StateData.DataYear=? AND StateData.IndustryCode=?',(year,str(code)))
    rows = c.fetchall()
    g = 0
    for row in rows:
        expectedShare = row[0]
        if isFloat(row[1])>0:
            actualShare = row[1]/totalemp
        else:
            actualShare = 0
        difer = actualShare - expectedShare
        g = g + difer**2
    c.close()
    return g

def CalcGamma(connection,code,year,herf):
    g = CalculateG(connection,year,code)
    if g == None:
        return (None, None)
    x2 = CalculateX2(connection,year)
    omin = 1 - x2
    numer = g - omin*herf
    ominh = 1 - herf
    den = omin*ominh
    gamma = numer/den
    return (gamma, g)


def InsertSig(conn,code,year,sig):
    c = conn.cursor()
    mylist = (str(code),year,sig)
    c.execute('INSERT INTO Sig(IndustryCode, DataYear, YesSig) VALUES(?,?,?)',mylist)
    conn.commit()
    c.close()

def FindSig(conn):
    c = conn.cursor()
    sig = 0
    c.execute("SELECT Gamma.DataYear AS y, Gamma.IndustryCode As code, Gamma.Gamma AS g, Plants.NPlants AS n, HHI.Herf AS h FROM Gamma JOIN Plants ON Gamma.IndustryCode=Plants.IndustryCode AND Gamma.DataYear=Plants.DataYear JOIN HHI ON Gamma.IndustryCode=HHI.IndustryCode AND Gamma.DataYear=HHI.DataYear")
    rows = c.fetchall()
    for row in rows:
        found = False
        sig = 0
        if row[2]>0:
            c.execute('SELECT Sim.cvhigh AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.hhigh>=? AND Sim.hlow<=? AND Sim.DataYear=? ORDER BY Sim.Plants DESC, cvhigh DESC LIMIT 1',(row[3],row[4],row[4],row[0]))
        else:
            c.execute('SELECT Sim.cvlow AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.hhigh>=? AND Sim.hlow<=? AND Sim.DataYear=? ORDER BY Sim.Plants DESC, cvlow ASC LIMIT 1',(row[3],row[4],row[4],row[0]))
        
        results = c.fetchall()
        for r in results:
            found = True
            if row[2]>r[0] and row[2]>0:
                sig = 1
            elif row[2]<0 and row[2]<r[0]:
                sig = 1
            InsertSig(conn,row[1],row[0],sig)
        
        if found == False:
            if row[2]>0:
                c.execute('SELECT Sim.cvhigh AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.DataYear=? ORDER BY Sim.Plants DESC, cvhigh DESC LIMIT 1',(row[3],row[0]))
            else:
                c.execute('SELECT Sim.cvlow AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.DataYear=? ORDER BY Sim.Plants DESC, cvlow ASC LIMIT 1',(row[3],row[0]))
            results = c.fetchall()
            for r in results:
                if row[2]>r[0] and row[2]>0:
                    found = True
                    sig = 1
                    InsertSig(conn,row[1],row[0],sig)
                elif row[2]<0 and row[2]<r[0]:
                    found = True
                    sig = 1
                    InsertSig(conn,row[1],row[0],sig)
        if found == False:
            if row[2]>0:
                c.execute('SELECT Sim.cvhigh AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.DataYear=? AND Sim.hhigh>=? ORDER BY Sim.Plants DESC, hlow ASC, cvhigh DESC LIMIT 1',(row[3],row[0],row[4]))
            else:
                c.execute('SELECT Sim.cvlow AS cv FROM Sim WHERE Sim.Plants<=? AND Sim.DataYear=? AND Sim.hhigh>=? ORDER BY Sim.Plants DESC, hlow ASC, cvlow ASC LIMIT 1',(row[3],row[0],row[4]))
            results = c.fetchall()
            for r in results:
                if row[2]>r[0] and row[2]>0:
                    found = True
                    sig = 1
                    InsertSig(conn,row[1],row[0],sig)
                elif row[2]<0 and row[2]<r[0]:
                    found = True
                    sig = 1
                    InsertSig(conn,row[1],row[0],sig)
        if found == False:
            InsertSig(conn,row[1],row[0],sig)
    c.close()

def main():
    global dbFile
    desc = 'Calculate cells'
    p = optparse.OptionParser(description=desc)
    p.add_option('--dbfile', dest="dbfile", help="Set database file", default='', metavar='"<File Path>"')
    p.add_option('--importkey', '-i', dest="importkey", help="File containing text to disclosure information", default='', metavar='"<File Path>"')
    p.add_option('--loadnational', '-n', dest="loadnational", help="File containing national industry information", default='', metavar='"<File Path>"')
    p.add_option('--loadstate', '-s', dest="loadstate", help="File containing state industry information", default='', metavar='"<File Path>"')
    p.add_option('--weights', '-w', dest="weights", help="File containing national employment weights", default='', metavar='"<File Path>"')
    p.add_option('--saveState', dest="saveState", help="Save state data to CSV", default='', metavar='"<File Path>"')
    p.add_option('--saveNational', dest="saveNational", help="Save national data to CSV", default='', metavar='"<File Path>"')
    p.add_option('--nationalFillYear', dest="nationFillYear", help="Run National Fill Year", default=0, type="int", metavar='"1997"')
    p.add_option('--stateFillYear', dest="stateFillYear", help="Run State Fill Year", default=0, type="int", metavar='"1997"')
    p.add_option('--calcHerfGammaYear', dest="calcYear", help="Calculate Gamma and HHI for a given Year", default=0, type="int", metavar='"1997"')
    p.add_option('--loadsim', dest="loadsim", help="File containing simulated data", default='', metavar='"<File Path>"')
    p.add_option('--simyear', dest="simyear", help="Specify simulation year", default=0, type="int", metavar='"1997"')
    p.add_option('--findsig', action="store_true", dest="findsig", default=False)
    
    
    (options, arguments) = p.parse_args()
    
    if len(options.dbfile.strip())>0:
        if isReturnFile(options.dbfile.strip()) != False:
            dbFile = isReturnFile(options.dbfile.strip())
    if len(dbFile)>0:
        sqlite3.register_adapter(D, adapt_decimal)
        sqlite3.register_converter("decimal", convert_decimal)
        conn = sqlite3.connect(dbFile, detect_types=sqlite3.PARSE_DECLTYPES)
        CreateTables(conn)
        AddMethods(conn)
        
        if len(options.importkey.strip()) > 0:
            if os.path.isfile(os.path.abspath(os.path.expanduser(options.importkey.strip()))) != False:
                ReadKey(conn,os.path.abspath(os.path.expanduser(options.importkey.strip())))
        
        if len(options.loadnational.strip()) > 0:
            if os.path.isfile(os.path.abspath(os.path.expanduser(options.loadnational.strip()))) != False:
                ReadNationalData(conn,os.path.abspath(os.path.expanduser(options.loadnational.strip())))
        
        if len(options.loadstate.strip()) > 0:
            if os.path.isfile(os.path.abspath(os.path.expanduser(options.loadstate.strip()))) != False:
                ReadStateData(conn,os.path.abspath(os.path.expanduser(options.loadstate.strip())))
        
        if len(options.weights.strip()) > 0:
            if os.path.isfile(os.path.abspath(os.path.expanduser(options.weights.strip()))) != False:
                ReadWeights(conn,os.path.abspath(os.path.expanduser(options.weights.strip())))
        
        if len(options.saveState.strip())>0:
            ExportState(conn,os.path.abspath(os.path.expanduser(options.saveState.strip())))
        
        if len(options.saveNational.strip())>0:
            ExportNational(conn,os.path.abspath(os.path.expanduser(options.saveState.strip())))
        
        if options.nationFillYear>0:
            FillNationalData(conn,options.nationFillYear)
        
        if options.stateFillYear>0:
            FillStateData(conn,options.stateFillYear)
        
        if options.calcYear>0:
            FindHerfAndGamma(conn,options.calcYear)
        
        if options.simyear>0 and len(options.loadsim.strip()) > 0:
            if os.path.isfile(os.path.abspath(os.path.expanduser(options.loadsim.strip()))) != False:
                LoadSim(conn,os.path.abspath(os.path.expanduser(options.loadsim.strip())),options.simyear)
                
        if options.findsig == True:
            FindSig(conn)


if __name__ == '__main__':
    main()