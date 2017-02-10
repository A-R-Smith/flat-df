
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import com.arsmith.FlatDataFrame._

object FlatDataFrameLoader {
  def main(args: Array[String]) {

    val fileName = "./src/test/resources/testFile.xml"
    val rowTag = "Row"
    	
    val spark = SparkSession
     .builder()
     .appName("FlatDataFrame")
     .enableHiveSupport()
     .getOrCreate()
       
    val df = spark.read
            .format("com.databricks.spark.xml")
            .option("rowTag",rowTag)
            .option("attributePrefix","_")
            .option("valueTag","VAL")
            .load(fileName)  
            
    val flat = flatten(df)

    val ds = flat.as(RowEncoder(flat.schema))
            

  }
}