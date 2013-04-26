# Process Mosaic Coverage data and overlay with census block using PostGIS

### Problem to Solve:

+ We have Mosaic Coverage boundary data for various providers/technology in Shape file format
+ We need to overlay the boundary data with census block to determine the coverage percentage for each block by provider and technology
+ Some of the geometires are quiet large, for example, one polygon has 269231 vertices
+ Some geometries are not OGC compliant and considered invalid in PostGIS
+ there're over 11 millions census block, as a result the overlay process is unacceptably slow
+ Simplification is not an option due to uncertainty regarding how it affects data accuracy

****

### Solutions:

+ Tiling large polygons to reduce the number of vertices
+ Partition both census block and Mosaic coverage table
+ Repair geometry during shape file loading process 
+ process overlay by state and country to make memory usage managable
+ employing these techniques cut down the process for one technology from 5 days to 10 hours

****


### Steps

+ run *block2010_partition.py* to partision census block 2010 table by state, and create necessary indexes
    + create parent table 
    + loop through each state to create child table
    + this step takes 30 minutes and only needs to be run once
+ run *load_mosaic_shapefile.py* to load shapefile into Postgres
    + the outer loop is for the specific folder structure, to find correct shape file by technology and company
    + use *shp2pgsql* to load each shapefile to a temporary postgis table
    + use *st_buffer(geom, 0.0)* to repair geometry, the *makeValid* (from postgis 2.0) won't work due to large geometry
    + create 4x4 tiles based on geometry extend, cut the geometry until the number of vertices below preset threshhold (5000 in this case)
    + in each iteration, insert the tiled geometry into the temporary table and delete the old record.
    + in case st_intersection() returns GeometryCollection type, use st_buffer() to convert it to multipolygon
    + move all records from the temporary table to the partisioned coverage boundary table and drop the temporary table
+ run *process_mosaic_block_overlay.py* to process the coverage boundary overlay with census block table
    + again, outer loop is for folder struction to find company/technology used to locate right table
    + to minimize memory usage, we do block overlays by state and county
    + since *st_intersection* is very expensive, we use *st_contains* first to avoid unnecessary *intersection* operation.
    + we also use *st_envelope(block_geom)* for first iteration *intersection*, then using the result to *st_intersection(block_geom)*, this seems to produce better performance
+ run *process_mosaic_block_post_process.py* to get coverage percentage for each census block
    + again, we do this by state to minimize memory usage
    + we round the summed percentage result to 4 decimal degress due to rounding errors during overlay process

****

### Conclusion
+ for large dataset with complex geometry, partision the table and use tiling to reduce the geometry complexicity can greatly enhance the performance
+ We have tried ArcGIS and FME and both failed to finish the operation, PostGIS got the work done with some tweaking
+ the ST_INTERSECTION in PostGIS is very slow commaring to similar operation in ArcGIS, hope it could get a performance boost in the near future

****

### Improvement
+ need to add logging to better track and analyze the process
+ one large provider takes more than half of the processing time, we may need to partision it by state before processing

