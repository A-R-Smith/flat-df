package com.arsmith

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.functions.col

object FlatDataFrame {
  
  /**
   * flatten
   * This function does two things: denormalize a dataframe as well as manage the renaming of columns.
   * Columns need to be renamed  because the algorithm with just concatenate column names when
   * demoralizing nested columns. This can lead to redundant and extremely long column names. 
   * Renaming columns, or subtrings within columns is completely optional
   * 
   * Can be used as flatten(df), flatten(df,subStr2remove) or flatten(df,subStr2remove,str2rename)
   * 
   * PARAMS:
   * df            : dataframe to flatten(denormalize)
   * subStr2remove : Array of Strings, substrings to be removed from new column names
   * str2Rename    : Array of String tuples, columns to be renamed, left String is old name, right String is new name
   * 
   */
  def flatten (df    : DataFrame,
    subStr2remove : Array[String] = Array(), 
    str2rename    : Array[(String,String)] = Array()
                   ) : DataFrame = {
    
    val columns = flattenColumns(df.schema.fields)
    df.select(columns.map(col):_*).toDF(createColumnNames(columns,subStr2remove,str2rename):_*) 
  }
  
  
  /**
   * flattenColumns
   * 
   * The main algorithm to denormalize the dataframe.
   * Recursively descends schema structure to pull out nested columns
   * Stops after one level of descent
   * 
   */
  def flattenColumns (f : Array[StructField]) : Array[String] = {
    f.map(sf=>sf.dataType match {
      case x: StructType => flattenColumns(x.fields).map(x=>sf.name+"."+x)
      case x: ArrayType =>  x.elementType match {
        case y : StructType => y.fields.map(z=>sf.name+"."+z.name)
      }
      case _ => Array(sf.name)
    }).flatMap(x=>x)  
  }
  
  /**
   * createColumnNames
   * 
   * iterates thru the new column names to either replace substrings or rename columns
   * 
   * 
   */
  def createColumnNames(colNames    : Array[String], 
                   subStr2remove : Array[String] = Array(), 
                   str2rename    : Array[(String,String)] = Array()
                      ): Array[String] = {
    colNames.map(x => {
      renameColumn(removeSubStrs(x.replace(".","_"),subStr2remove).replace("__", "_"),str2rename)     
    })
  }

  /**
   * removeSubStrs
   * 
   * removes substrings contained in subStrings param from the name param
   * 
   * 
   */
  def removeSubStrs(name : String, subStrings : Array[String]) : String =  {
    (subStrings.sortBy(_.length).reverse).fold(name){(acc,n)=>acc.replaceAll(n,"")}
  }
  
  /**
   * renameColumn
   * 
   * renames "name" if its contained in the str2rename tuple array
   * 
   * 
   */
  def renameColumn(name : String, str2rename : Array[(String,String)]) : String = {
    for ((findStr,replaceStr) <- str2rename) {
      if (name.equals(findStr)) {
        return replaceStr
      }
    }
    return name
  }

  /**
   * writeColumnDefs
   * 
   * prints out column names and type
   * for debugging
   * 
   */
  def writeColumnDefs (df : org.apache.spark.sql.DataFrame, fileName : String) {
    val writer = new java.io.PrintWriter(new java.io.File(fileName))
    df.schema.fields.foreach(x=>writer.println(x.name+"|"+x.dataType.simpleString))
    writer.close
  }
}