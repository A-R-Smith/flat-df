package com.arsmith

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.functions.col

object FlatDataFrame {
  
  def flatten (df    : DataFrame,
    subStr2remove : Array[String] = Array(), 
    str2rename    : Array[(String,String)] = Array()
                   ) : DataFrame = {
    
    val columns = flattenColumns(df.schema.fields)
    df.select(columns.map(col):_*).toDF(createColumnNames(columns,subStr2remove,str2rename):_*) 
  }
  
  def flattenColumns (f : Array[StructField]) : Array[String] = {
    f.map(sf=>sf.dataType match {
      case x: StructType => flattenColumns(x.fields).map(x=>sf.name+"."+x)
      case x: ArrayType =>  x.elementType match {
        case y : StructType => y.fields.map(z=>sf.name+"."+z.name)
      }
      case _ => Array(sf.name)
    }).flatMap(x=>x)  
  }
  
  def createColumnNames(colNames    : Array[String], 
                   subStr2remove : Array[String] = Array(), 
                   str2rename    : Array[(String,String)] = Array()
                      ): Array[String] = {
    colNames.map(x => {
      renameColumn(removeSubStrs(x.replace(".","_"),subStr2remove).replace("__", "_"),str2rename)     
    })
  }

  def removeSubStrs(name : String, subStrings : Array[String]) : String =  {
    (subStrings.sortBy(_.length).reverse).fold(name){(acc,n)=>acc.replaceAll(n,"")}
  }
  
  def renameColumn(name : String, str2rename : Array[(String,String)]) : String = {
    for ((findStr,replaceStr) <- str2rename) {
      if (name.equals(findStr)) {
        return replaceStr
      }
    }
    return name
  }

  def writeColumnDefs (df : org.apache.spark.sql.DataFrame, fileName : String) {
    val writer = new java.io.PrintWriter(new java.io.File(fileName))
    df.schema.fields.foreach(x=>writer.println(x.name+"|"+x.dataType.simpleString))
    writer.close
  }
}