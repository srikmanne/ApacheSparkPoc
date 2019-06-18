package espn.trail;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;


public class WithoutSQL {
	 public static void main( String[] args )
	    {		 
		 SparkSession spark = SparkSession
				  .builder().master("local")				  			 
				  .getOrCreate();			  
		 
		 Dataset<Row> df = spark.read().parquet("E:/data/userdata1.parquet");

		// Register the UDF(extract year from given format Ex: 02/04/2018 --> 2018 )
         spark.udf().register("extract_year", new UDF1<String, String>() {
             public String call(String sDob) {            	
                 if(sDob != null && !sDob.equals("") ) {
                     String[] arrOfStr = sDob.split("/");
                     if(arrOfStr.length >= 2) {
                    	 return (arrOfStr[2]);
                     }else {
                    	 return "";
                     }
                 } else {
                     return "";
                 }
             }

         }, DataTypes.StringType);    
         
         //-Transform
         df.select("id","first_name","last_name", "registration_dttm","birthdate" ,"country","email")
		 .withColumn("birthdate",callUDF("extract_year", col("birthdate")))
		 .withColumn("registration_dttm",to_utc_timestamp(col("registration_dttm"), "EST"))
		 .withColumnRenamed("birthdate", "year_of_birth")
		 .withColumnRenamed("id", "index")
		 .withColumnRenamed("registration_dttm", "registration_EST").write().mode(SaveMode.Overwrite).parquet("E:/data/output.parquet");  		 
		 
	    }
}

