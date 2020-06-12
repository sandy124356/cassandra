import org.apache.spark._
import org.apache.spark.sql._
import com.datastax.spark.connector._


object cass {


  def main(args: Array[String]): Unit =
  {

    val sparkConf = new SparkConf()
      .setAppName("cassandra_delta")
      .setMaster("local[*]")  //Spark Master
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
    //
    //    val sc = new SparkContext(sparkConf)
    //
    //    val rdd1 = sc.cassandraTable("keyspace","sampleTable1")
    //
    //
    //    val df1=rdd1.toDF()

    val spark= SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    //import spark.implicits._

       val df1= spark.read.format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "dataset1",
                        "keyspace" -> "basics"))
          .load()

    df1.createOrReplaceTempView("table1")

    val df2=spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "dataset2",
                    "keyspace" -> "basics"))
      .load()

    df2.createOrReplaceTempView("table2")

    //    df1.show()

    //    df2.show()

    val joindf=spark.sql("select t1.id, t1.name as boy_name, t2.name as girl_name, " +
      "t1.address as boy_address, t2.address as girl_address from table1 t1 join table2 t2 on (t1.id=t2.id)")

    joindf.show()



    joindf.createCassandraTable(keyspaceName = "latest", tableName = "joindataset")

    joindf.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "joindataset", "keyspace" -> "latest")).save()



  }
}
