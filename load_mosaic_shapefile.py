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
		#constants 
		protocol = "LTE"
		dataset = "201301"
		verticeThreshhold = "5000"
		xgridnum = "4"
		ygridnum = "4"
		rootDir = "C:\\projects\\Data\\americaroamer\\2013 Jan\\Carriers"
		schema = "swat"
		tableName = schema +".coverageright" + dataset + protocol
		host = "yourhost"
		dbname = "yourdb" 
		schema = "yourschema"
		tableName = "block2010"
		user = "user"
		password = "pw"
		conn_string = "host='" + host + "' dbname='" + dbname + "' user='" + user + "' password='" + password + "'"	 

		starttime = time.time()

		conn = psycopg2.connect(conn_string)
		cursor = conn.cursor()
		techCount = 0
		companyCount = 0
		# sqlComm = "DROP TABLE IF EXISTS " + tableName + " CASCADE"
		# cursor.execute(sqlComm)
		# conn.commit()
		# sqlComm = "CREATE TABLE " + tableName + " (entity varchar(100), protocol varchar(100), geom geometry)"
		# cursor.execute(sqlComm)
		# conn.commit()

		for techType in os.listdir(rootDir):
		    techTypeDir = rootDir + "\\" + techType
		    if techType == protocol:
			    for company in os.listdir(techTypeDir):
			    	companyCount+=1
			    	#if len(company)>0:
			    	#if company == "SprintNextel Affiliates":
			    	if companyCount > 16:
				    	print techType + " " + company
				    	shapeFileDir = techTypeDir + "\\" + company + "\\SHP"
				    	for shapeFile in os.listdir(shapeFileDir):
							if shapeFile.endswith("shp"):
								starttimeCompany = time.time()
								companyTableName = (schema + ".coverageright_" + shapeFile.replace(".shp","")).lower()
								tempTableName = (schema + ".coverageright_" + shapeFile.replace(".shp","") + "_temp").lower()

								sqlComm = "DROP TABLE IF EXISTS " + companyTableName + " CASCADE"
								cursor.execute(sqlComm)
								sqlComm ="CREATE TABLE " + companyTableName + "(" + \
						  				 "CONSTRAINT dims_geom CHECK (st_ndims(geom) = 2)," + \
		  				   				 "CONSTRAINT geom CHECK ((geometrytype(geom) = ANY (ARRAY['MULTIPOLYGON'::text, 'POLYGON'::text])) OR geom IS NULL)," + \
		  								 "CONSTRAINT srid_geom CHECK (st_srid(geom) = 4326), " \
		  								 "CONSTRAINT entity_check CHECK (entity::text = '" + company + "')) " + \
										 "INHERITS (" + tableName + ");"
								cursor.execute(sqlComm)
								conn.commit()

								sqlComm = "drop table if EXISTS " + tempTableName + " CASCADE"
								cursor.execute(sqlComm)
								conn.commit()

								shpComm = 'shp2pgsql -s 4326 -d -I -W latin1 -g geom "' + shapeFileDir + "\\" + shapeFile + '" '  + tempTableName + " | psql -h " + host + " -d " + dbname + " -U " + user
								print "load " + shapeFile + " to postgres"
								shpP = subprocess.call(shpComm,shell=True)
								#repare geometry,do not use ST_MAKEVALID, takes forever, instead use st_buffer
								print "repair geometry for company " + company
								sqlComm = "update " + tempTableName + " set geom = st_multi(st_buffer(geom,0.0)) where st_isvalid(geom) = false"
								cursor.execute(sqlComm)
								conn.commit()

								#tiling geometry which has a massive vertices
								sqlComm = "select gid from " + tempTableName + " where st_npoints(geom) > " + verticeThreshhold
								cursor.execute(sqlComm)
								gids = cursor.fetchall()
								while len(gids)>0:
									print "tiling " + company + " for " + str(len(gids)) + " geometries at threshhold " + verticeThreshhold 
									for gid in gids:
										sqlComm = "WITH geomext AS(select st_setSRID(" + \
													"cast(st_envelope(s.geom) as geometry),4326) as geom_ext," + \
													xgridnum + " as x_gridcnt, " + ygridnum + " as y_gridcnt, s.geom as geom " + \
													"from (select geom from " + tempTableName + " where gid = " + str(gid[0]) + ")  as s),"
										sqlComm = sqlComm + "grid_dim AS (select (st_xmax(geom_ext) - st_xmin(geom_ext))/x_gridcnt as g_width, " + \
													"st_xmin(geom_ext) as xmin, st_xmax(geom_ext) as xmax, " + \
													"(st_ymax(geom_ext)-st_ymin(geom_ext))/y_gridcnt as g_height, " + \
													"st_ymin(geom_ext) as ymin, st_ymax(geom_ext) as ymax from geomext),"
										sqlComm = sqlComm + "grid AS (select x, y, st_setSrid(st_makeBox2d(st_point(xmin + (x-1)*g_width, ymin + (y-1)*g_height), " + \
													"st_point(xmin + x*g_width, ymin + y*g_height)),4326) as grid_geom " + \
													"from (select generate_series(1,x_gridcnt) from geomext) as x(x) " + \
													"cross join (select generate_series(1,y_gridcnt) from geomext) as y(y) " + \
													"cross join grid_dim) "
										#direct insert after WITH clause not working on linux, have to create temp table to hold the geometries
										# sqlComm = sqlComm + "insert into " + tempTableName +" (geom) select st_multi(st_intersection(geomext.geom,grid.grid_geom)) " + \
										# 					"from geomext,grid " + \
										# 					"where st_intersects(geomext.geom,grid.grid_geom) and st_geometrytype(st_intersection(geomext.geom,grid.grid_geom)) like '%Polygon'"
										sqlComm = sqlComm + "select st_multi(st_intersection(geomext.geom,grid.grid_geom)) as geom into swat.mytemp " + \
															"from geomext,grid " + \
															"where st_intersects(geomext.geom,grid.grid_geom) and st_geometrytype(st_intersection(geomext.geom,grid.grid_geom)) like '%Polygon';"
										sqlComm = sqlComm + "insert into " + tempTableName +" (geom) select geom from swat.mytemp;"
										sqlComm = sqlComm + "drop table if exists swat.mytemp;"
										cursor.execute(sqlComm)
										conn.commit()

										sqlComm = "delete from " + tempTableName + " where gid = " + str(gid[0])
										cursor.execute(sqlComm)
										conn.commit()

									sqlComm = "select gid from " + tempTableName + " where st_npoints(geom) > " + verticeThreshhold
									cursor.execute(sqlComm)
									gids = cursor.fetchall()
								print "finish tiling, start inserting records"
								sqlComm = "insert into " + companyTableName + " select '" + company + "','" + techType + "', geom from " +  tempTableName

								cursor.execute(sqlComm)
								conn.commit()

								sqlComm  = "CREATE INDEX " + companyTableName.replace(".","_") + "_geom_gist ON " + companyTableName + " USING gist (geom )"
								cursor.execute(sqlComm)
								conn.commit()

								sqlComm = "drop table if EXISTS " + tempTableName + " CASCADE"
								cursor.execute(sqlComm)
								conn.commit()	
								print "process " + company + " takes " + str(int((time.time()-starttimeCompany)/60))+ " minutes"	
		print "process loading files takes " + str(int((time.time()-starttime)/60))+ " minutes"	

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