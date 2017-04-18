
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import com.arsmith.FlatDataFrame._

object FlatDataFrameLoader {
  def main(args: Array[String]) {

    val fileName = "./src/test/resources/testFile.xml"
    val rowTag = "Row" //the XML tag that specifies the start of a new row
    val hiveTable = "schema.table1"
    	
    val spark = SparkSession
     .builder()
     .appName("FlatDataFrame")
     .enableHiveSupport()
     .getOrCreate()
       
    //Read the xml file into a DataFrame using the databricks XML parser
    val df = spark.read
            .format("com.databricks.spark.xml")
            .option("rowTag",rowTag)
            .option("attributePrefix","_")
            .option("valueTag","VAL")
            .load(fileName)  
    
    // use the Flat DataFrame function to denormalize data
    val flatDf = flatten(df)

   // val ds = flat.as(RowEncoder(flat.schema))
    
    
    //persist dataframe into hive table
    flatDf.write.mode("append").format("ORC").saveAsTable(hiveTable)
            

  }
}