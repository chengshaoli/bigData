import org.apache.spark.{SparkConf, SparkContext}


object Wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 100)

    val rdd2 = rdd.zipWithIndex()

    val value = sc.broadcast("")
    value.unpersist(true)

    val rdd3 = rdd2.reduceByKey(_+_)

    rdd3.foreach(println)

    sc.stop()

  }

}
