package test

import org.apache.spark._
import au.com.bytecode.opencsv._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

object RuleEngineTest {
  case class KeyValue(key: String, value: String)
  case class Condition(dimname: String, operator: String, values: Array[KeyValue], condnoperator: String)
  case class Target(dimname: String, operator: String, values: KeyValue)
  case class Input(condition: Array[Condition], target: Target)
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    var InputString = """{"condition":[{"dimname":"Name","operator":"IN","values":[{"key":74,"value":"test1"},{"key":74,"value":"test3"},{"key":74,"value":"test7"}],"condnoperator":"AND"},{"dimname":"Company","operator":"IN","values":[{"key":73262,"value":"EV"},{"key":73212,"value":"INFY"}],"condnoperator":""}],"target":{"dimname":"Des","operator":"=","values":[{"key":329,"value":"Manager"}]}}"""
    val inputConfVal = parse(InputString).extract[Input]
    var conditions = inputConfVal.condition
    var target = inputConfVal.target
    var whereCondition = conditions.map(condition =>
      {
        condition.dimname + " " + condition.operator + " (" + condition.values.map(_.value).map(a => { "'" + a + "'" }).mkString(",") + ") " + condition.condnoperator
      }).mkString(" ")

    println(whereCondition)

    var classify = target.dimname + target.operator + target.values.value

    var updated = "'" + target.values.value + "'"

    println(classify)
    var sc = new SparkContext("local", "test");
    var sqlContext = new SQLContext(sc)
    var data = sc.textFile("C:/Users/Gourav_Gupta/Desktop/TestDataRule.csv")
    val datawithoutHeader =
      data.mapPartitionsWithIndex((idx, lines) => {
        if (idx == 0) {
          lines.drop(1)
        }
        lines
      })
    var parser = new CSVParser(',', CSVParser.DEFAULT_QUOTE_CHARACTER)
    var header = parser.parseLine(data.first())
    var schema = StructType(header.map(value => { StructField(value, StringType, true) }))
    var dataRdd = datawithoutHeader.map(line => {
      var parser = new CSVParser(',', CSVParser.DEFAULT_QUOTE_CHARACTER)
      Row.fromSeq(parser.parseLine(line).toSeq)
    })
    var df = sqlContext.createDataFrame(dataRdd, schema)
    df.registerTempTable("InputTable")
    df.show()

    var str = "select Name,Company,Location,Salary,case when " + whereCondition + " then " + updated + " end as description from InputTable"

    println(str)

    sqlContext.sql("select Name,Company,Location,Salary,case when " + whereCondition + " then " + updated + " end as description from InputTable").show();

  }

}
