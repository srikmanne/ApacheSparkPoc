# ApacheSparkPoc

This Sample project demonstrates how to process parquet files with and without SQL
I've taken the attached userdata1.parquet with 13 columns((registration_dttm|id|first_name|last_name|email|gender|ip_address|cc|country| birthdate|salary|title|comments|))
 and transformed it to output.parquet file with 7 columns( id| first_name| last_name| registration_EST| year| country| email|)

The following columns has been converted or renamed 
1)registration_dttm has been renamed to registration_EST and as the new name says it now displays EST format
2)birthdate has been renamed to year_of_birth and using an UDF the year has been extracted and displayed now
3)id has been renamed to index

All these transformations have been done with Spark SQL and Spark Dataframe.

Please see the committed files which contains both the programs WithSQL.java and WithoutSQL.java and Sample output.
