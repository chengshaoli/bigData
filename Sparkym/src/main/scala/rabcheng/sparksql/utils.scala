package rabcheng.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object utils {

  def getSparkSql(): SQLContext ={

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    new SQLContext(sc)

  }

}
