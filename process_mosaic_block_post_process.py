#!C:\Python27\python.exe
# -*- coding: utf-8 -*-
import psycopg2
#note that we have to import the Psycopg2 extras library!
import psycopg2.extras
import sys
import os
import subprocess
import json
import time
 
def main():
	try:
		starttime = time.time()
		protocol = "LTE"
		dataset = "201301"

		host = "yourhost"
		dbname = "yourdb" 
		schema = "yourschema"
		tableName = "block2010"
		user = "user"
		password = "pw"
		conn_string = "host='" + host + "' dbname='" + dbname + "' user='" + user + "' password='" + password + "'"	 

		tableName = schema + ".coverageright" + dataset + protocol +"block_final"
		sourceTableName = schema + ".coverageright" + dataset + protocol + "block"

		conn = psycopg2.connect(conn_string)
		cursor = conn.cursor()

		sqlComm = "DROP TABLE IF EXISTS " + tableName + " CASCADE"
		cursor.execute(sqlComm)
		conn.commit()
		sqlComm = "CREATE TABLE " + tableName + " (entity varchar(100), protocol varchar(100), geoid10 varchar(15), pct double precision)"
		cursor.execute(sqlComm)
		conn.commit()

		#create index on source table
		print "create index"
		sqlComm = "DROP INdex if exists "+ sourceTableName.replace(".","_") + "_geoidstate;"
		sqlComm = sqlComm + "CREATE INDEX " + sourceTableName.replace(".","_") + "_geoidstate ON " + sourceTableName + " USING btree(substr(geoid10::text, 1, 2) );"

		cursor.execute(sqlComm)
		conn.commit()

		statefps = ("01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","53","54","55","56","60","66","69","72","78")

		for s in statefps:
			print "process state " + s
			starttimeState = time.time()
			sqlComm = "insert into " + tableName + " select entity, protocol, geoid10, round(cast(sum(pct) as numeric),4) from " + sourceTableName + \
					" where substr(geoid10,1,2) = '" + s + "' group by geoid10,entity,protocol"	
			cursor.execute(sqlComm)
			conn.commit()
			print "process state " + s + " takes " + str(int((time.time()-starttimeState)/60))+ " minutes"	
				
		print "Whole Process taken in " + str(int((time.time()-starttime)/60)) + " minutes"
	except psycopg2.DatabaseError, e: 
		if conn:
			conn.rollback() 
		print 'Error %s' % e    
		sys.exit(1)   
	finally:   
		if conn:
			conn.close()
 
if __name__ == "__main__":
	main()