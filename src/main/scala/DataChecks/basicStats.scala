package DataChecks

import com.databricks.spark.avro._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}
import utils.FirstPassStatsModel
import scala.util.control.Breaks._
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
/**
  * Created by Naresh on 2/05/16.
  */
object basicStats extends SparkJob {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("localtrial")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("{\n        \"tableName\" =\"test.csv\"\n\t\"column\" = [{\n\t\t\t\"name\" : \"PatientMRN\",\n\t\t\t\"datatype\" : \"long\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"Admittingdoctorname\",\n\t\t\t\"datatype\" : \"string\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"ProcedurePostingDate\",\n\t\t\t\"datatype\" : \"timestamp\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"ProcedureServiceMMYYYY\",\n\t\t\t\"datatype\" : \"long\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"ProcedureServiceDate\",\n\t\t\t\"datatype\" : \"long\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"ProcedureDescription\",\n\t\t\t\"datatype\" : \"string\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"ProcedureNumber\",\n\t\t\t\"datatype\" : \"long\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t},\n\t\t\t{\n\t\t\t\"name\" : \"AdmittingdoctorNPI\",\n\t\t\t\"datatype\" : \"long\",\n\t\t\t\"format\" : \"\",\n\t\t\t\"validaton\" : \"\"\n\t\t\t}\n\t\t\t]\n}\n\n\n\t\t\t\t\t\t\t")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }


  override def runJob(sc: SparkContext, config: Config): Any = {
    val sqlContext = new SQLContext(sc)

    //[[Very Important]] To get map method for config List object
    import collection.JavaConversions._
    val tableName = config.getString("tableName")
    val columnList = config.getConfigList("column")

    //Reading DataFrame
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/home/ubuntu/Downloads/"+tableName)

    //Casting columns as per schema
    val transformedDF = df.select(columnList.map{ confObject =>
      val colName = confObject.getString("name")
      val colDatatype = confObject.getString("datatype")
      col(colName).cast(colDatatype.replace("timestamp", "string"))
    }:_*)

    

  }



  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case ms: List[_] => flatten(ms)
    case e => List(e)
  }
}