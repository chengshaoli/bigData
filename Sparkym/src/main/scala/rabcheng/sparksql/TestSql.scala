package rabcheng.sparksql

import org.apache.spark.sql.Dataset

object TestSql {

  def main(args: Array[String]): Unit = {

    val sQLContext = utils.getSparkSql()

    import sQLContext.implicits._
    val dataFrame = sQLContext.read.text("Test1")

    val data: Dataset[Test1] = dataFrame.map(row => {
      val str = row.getAs[String]("value")
      val arr = str.split(" ")
      Test1(arr(0), arr(1), arr(2))
    })
    data.createTempView("person_info")

    val sql =
      """
        |select
        |	constellation_blood_type,
        |	concat_ws('|',collect_set(name))
        |from
        |	(select
        |	concat_ws(",",constellation,blood_type) constellation_blood_type,name
        |from
        |	person_info)t1
        |group by constellation_blood_type
      """.stripMargin

    sQLContext.sql(sql).show()

    sQLContext.sparkContext.stop()

  }

}
case class Test1(name:String,constellation:String,blood_type:String)
