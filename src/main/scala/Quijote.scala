import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Quijote extends  App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("Quijote").master("local[*]").getOrCreate()

  import spark.implicits._

  val parsed_df = spark.read.csv("el_quijote.txt")
  parsed_df.show(10)
  //filas sin truncar
  parsed_df.show(false)

  //numero filas
  println("El total de lineas es: " + parsed_df.count())

  //muestra la cabecera
  println(parsed_df.head())
  //muestra las 10 primeras filas
  parsed_df.take(10)
  //muestra la primera linea
  println(parsed_df.first())

  parsed_df.createOrReplaceTempView("vistaquijote")

  spark.catalog.listTables.show()
  spark.sql("select * from vistaquijote").show()

}
