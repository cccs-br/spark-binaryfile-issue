import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

import java.nio.file.Paths

class BinaryFileSuite extends QueryTest with SharedSparkSession {


  test("binary file dataframe") {
    // load files in directly into df using 'binaryFile' format.
    val df = spark
      .read
      .format("binaryFile")
      .load("src/test/resources/files")

    df.createOrReplaceTempView("files")



    // This works as expected (3.3.0/3.2.1).
    val like_count = spark.sql("select * from files where path like '%.csv'").count()
    assert(like_count === 1)

    // This does not work as expected (3.3.0/3.2.1).
    val not_like_count = spark.sql("select * from files where path not like '%.csv'").count()
     assert(not_like_count === 2)

    // This used to work in 3.2.1
    // df.filter(col("path").endsWith(".csv") === false).show()
  }

  test("Works against regular old dataframe with matching mock data") {

    val schema = StructType(
      Array[StructField](
        StructField("path", StringType, nullable = true)
      )
    )

    val row = Seq(
      Row(s"file:${Paths.get("src/test/resources/files/test2.json")}"),
      Row(s"file:${Paths.get("/src/test/resources/files/test1.csv")}"),
      Row(s"file:${Paths.get("src/test/resources/files/test3.txt")}")
    )

    spark
      .createDataFrame(spark.sparkContext.parallelize(row), schema)
      .createOrReplaceTempView("files")

    // Works as expected.
    val like_count = spark.sql("select * from files where path like '%.csv'").count()
    assert(like_count === 1)

    // Works as expected.
    val not_like_count = spark.sql("select * from files where path not like '%.csv'").count()
    assert(not_like_count === 2)
  }


}
