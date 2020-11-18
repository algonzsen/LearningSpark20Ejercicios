import org.apache.log4j.helpers.OptionConverter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, count, desc, sum}

object capitulo2 extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("LearningSpark").master("local[*]").getOrCreate()

  import spark.implicits._

  val parsed_df = spark.read.option("header", "true").csv("mnm_dataset.csv")
  parsed_df.show(10)

  //parsed_df.withColumn("Count", toInt(parsed_df("Count")))
  //cambio el tipo de count a int
  val parsed_df2 = parsed_df.selectExpr("State", "Color", "cast(Count as int) Count")
  parsed_df2.printSchema()

  //¿¿QUE ES EL COUNT EN LA TABLA????? cuantos mnms hay de cada color en las diferentes tiendas de los estados

  //agrupo por estados y colores, cuento mnsm y ordeno descendentemente
  val guardar = parsed_df2.groupBy("State", "Color").sum("Count").sort(desc("sum(Count)")).cache()
  guardar.show(20)

  //filtro por estado y cuentas los que tiene de cada color
  guardar.filter($"State" === "CA").show()

  //cuento el numero de mnms totales por estado
  parsed_df2.groupBy("State").sum("Count").show()

  //distintos estados que hay
  parsed_df2.select(approx_count_distinct("State")).show()

  //agregacion, CUENTO LAS TIENDAS QUE HYA Y LOS MNMS TOTALES
  parsed_df2.groupBy("State").agg(count("State"),sum("Count")).filter($"STATE"==="AZ")show

  parsed_df2.createOrReplaceTempView("vista")
  spark.table("vista").show(10)

}
