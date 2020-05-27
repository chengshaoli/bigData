import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    val data = sc.makeRDD(List("hotel1,一星级,19829383","hotel1,一星级,19829383","hotel1,一星级,19829383","hotel1,二星级,19829383","hotel2,一星级,19829383","hotel2,二星级,19829383","hotel2,二星级,19829383","hotel2,二星级,19829383","hotel2,一星级,19829383"))

/*
    val dataWC: RDD[((String, String), Int)] = data.map(str => {
      val arr = str.split(",")
      ((arr(0), arr(1)), 1)
    })

    val reduceData: RDD[((String, String), Int)] = dataWC.reduceByKey(_+_)

    val groupData: RDD[(String, Iterable[((String, String), Int)])] = reduceData.groupBy(_._1._2)

    val value: RDD[(String, List[(String, Int)])] = groupData.mapValues(iter => {
      val list: List[((String, String), Int)] = iter.toList
      list.sortBy(-_._2).map(tp => (tp._1._1, tp._2)).take(2)
    })
    value.foreach(println)
    */

    import sQLContext.implicits._

    val frame = data.map(t => {
      val arr = t.split(",")
      hotel(arr(0), arr(1), 1)

    }).toDF()
    frame.createTempView("table")

//    frame.show()

    sQLContext.sql(
      """
        |select
        | sum(count),dengji
        | from table
        | group by dengji
      """.stripMargin).show()


    sc.stop()

  }


}
case class hotel(hotelName:String,dengji:String,count:Int)
