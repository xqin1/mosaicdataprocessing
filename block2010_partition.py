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
		host = "yourhost"
		dbname = "yourdb" 
		schema = "yourschema"
		tableName = "block2010"
		user = "user"
		password = "pw"

		conn_string = "host='" + host + "' dbname='" + dbname + "' user='" + user + "' password='" + password + "'"	 
		conn = psycopg2.connect(conn_string)
		cursor = conn.cursor()
		statefp = ("01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","53","54","55","56","60","66","69","72","78")
		
		sqlComm = "DROP TABLE IF EXISTS census.block2010partision CASCADE"
		cursor.execute(sqlComm)
		conn.commit()
		sqlComm ="CREATE TABLE census.block2010partision(" + \
					"id serial NOT NULL," + \
  					"statefp10 character varying(2)," + \
  					"countyfp10 character varying(3)," + \
  					"tractce10 character varying(6)," + \
  					"blockce10 character varying(4)," + \
  					"geoid10 character varying(15)," + \
  					"name10 character varying(10)," + \
 					"mtfcc10 character varying(5)," + \
  					"ur10 character varying(1)," + \
  					"uace10 character varying(5)," + \
  					"funcstat10 character varying(1)," + \
  					"aland10 double precision," + \
  					"awater10 double precision," + \
  					"intptlat10 character varying(11)," + \
  					"intptlon10 character varying(12)," + \
  					"geom geometry)"
		cursor.execute(sqlComm)
		conn.commit()
		for s in statefp:
			print "create block table for state " + s
			sqlComm = "DROP TABLE IF EXISTS census.block2010_" + s + " CASCADE"
			cursor.execute(sqlComm)
			conn.commit()

			sqlComm ="CREATE TABLE census.block2010_" + s + "(" + \
					  "CONSTRAINT dims_geom CHECK (st_ndims(geom) = 2)," + \
	  				   "CONSTRAINT geom CHECK (geometrytype(geom) = 'MULTIPOLYGON'::text OR geom IS NULL)," + \
	  					"CONSTRAINT srid_geom CHECK (st_srid(geom) = 4326)," + \
	  					"CONSTRAINT statefp_check CHECK (statefp10::text = '" + s + "'::text)) " + \
					"INHERITS (census.block2010partision);"
			cursor.execute(sqlComm)

			sqlComm = "Insert INTO census.block2010_" + s + " select id, statefp10,countyfp10,tractce10,blockce10,geoid10,name10,mtfcc10,ur10,uace10,funcstat10," + \
					"aland10,awater10,intptlat10,intptlon10,geom from census.block2010 where statefp10='" + s + "'"
			print sqlComm
			cursor.execute(sqlComm)
			sqlComm  = "CREATE INDEX " + tableName + "_" + s +"_geom_gist ON " + schema + "." + tableName + "_" + s + " USING gist (geom )"
			cursor.execute(sqlComm)
			sqlComm = "CREATE INDEX " + tableName + "_" + s + "_geoidstate ON " + schema + "." + tableName + "_" +s + " USING btree (substr(geoid10::text, 1, 2))"
			cursor.execute(sqlComm)
			conn.commit()
			sqlComm  = "CREATE INDEX " + tableName + "_" + s + "_countyfips ON " + schema + "." + tableName + "_" +s + " USING btree (countyfp10)"
			cursor.execute(sqlComm)
			conn.commit()
			sqlComm  = "CREATE INDEX " + tableName + "_" + s + "_geoid ON " + schema + "." + tableName + "_" +s + " USING btree (geoid10)"
			cursor.execute(sqlComm)
			conn.commit()
		print " time to create block partition is " + str(int((time.time()-starttime)/60))+ " minutes"	

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