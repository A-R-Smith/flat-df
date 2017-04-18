package com.arsmith.util
import org.apache.spark.sql.functions._
object Util {
  	  val getConcatenated = udf( (first: String, second: String, third: String) => { first + second + third} )

	  val getConcatenated2 = udf( (first: String, second: String) => { first + second} )
	  
			  
			  def strConcat  (args: String*) : String = {
			    val list = args.toList
			    if (list.length==0) return ""
			    if (list.length==1) return list(0)
			    list(0)+strConcat(list.drop(1) : _ *)
			  }
}