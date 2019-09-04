
/**
  * Created by yinmuyang on 19-8-26 17:07.
  */

object ToClickHouse {

  def main(args: Array[String]): Unit = {

    println(System.getProperty("sun.boot.class.path"))
    println(System.getProperty("java.ext.dirs"))
    import io.clickhouse.ext.ClickhouseConnectionFactory
    import io.clickhouse.ext.spark.ClickhouseSparkExt._
    import org.apache.spark.sql.SparkSession

    // spark config
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("local spark")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    // create test DF

    val df = sqlContext.createDataFrame(1 to 1000 map(i => Row1(s"$i", i, i + 10)) )

    // clickhouse params

    // any node
    val anyHost = "127.0.0.1"
    val db = "default"
    val tableName = "t1"
    // cluster configuration must be defined in config.xml (clickhouse config)
    val clusterName = Some("test_shard_localhost"): Option[String]

    // define clickhouse datasource
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(anyHost,user="browser-clickhouse",password = "eR961I+4")

    // create db / table
    //df.dropClickhouseDb(db, clusterName)
    df.createClickhouseDb(db, clusterNameO=None)
    df.createClickhouseTable(db, tableName, "mock_date", Seq("name"), clusterNameO = None)

    // save DF to clickhouse table
    val res = df.saveToClickhouse(db, "t1", (row) => java.sql.Date.valueOf("2000-12-01"), "mock_date", clusterNameO = None)
    assert(res.size == 1)
    assert(res.get("localhost") == Some(df.count()))
  }
}
