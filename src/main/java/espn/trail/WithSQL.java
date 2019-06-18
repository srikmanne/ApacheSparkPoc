package espn.trail;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
public class WithSQL {
    public static void main( String[] args )
    {
        try {
            SparkSession spark = SparkSession
                    .builder().master("local")    
                    .getOrCreate();

            //-Read the local file
            Dataset<Row> df = spark.read().parquet("E:/data/userdata1.parquet");

            //-Create a view
            df.createOrReplaceTempView("espn_data");

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

            //-Run the query 
            Dataset<Row> dtRow = spark.sql("SELECT id as index, first_name, last_name, to_utc_timestamp(registration_dttm, 'EST') AS registration_EST, extract_year(birthdate) AS year_of_birth, country, email FROM espn_data"); 

            dtRow.show(false);
            dtRow.printSchema();            
            dtRow.write().mode(SaveMode.Overwrite).parquet("E:/data/output.parquet");    
        } catch(Exception e) {
            e.printStackTrace();
        }

    }
}

