// вариант 3 

import org.apache.spark.sql.functions._

// Чтение CSV с заголовком и определением типов
val cons = spark.read.option("header", "true").option("inferSchema", "true").csv("countries3.csv")

// 1. Страны с населением > 75000
val populousCountries = cons.filter(col("Population") > 75000).select("Country", "Population")
populousCountries.show()

// 2. Страны регионов NEAR EAST или AFRIKA с площадью выше средней площади этих регионов
val sub = cons.filter(col("Region") === "NEAR EAST" || col("Region") === "AFRIKA")
val avgAreaRow = sub.agg(avg("Area")).first()

if (!avgAreaRow.isNullAt(0)) {
  val avg_area = avgAreaRow.getDouble(0)
  println(f"Средняя площадь для регионов NEAR EAST и AFRIKA: $avg_area")

  val largeAreaCountries = cons.filter(
    (col("Region") === "NEAR EAST" || col("Region") === "AFRIKA") && col("Area") > avg_area
  ).select("Country", "Area")

  largeAreaCountries.show()
} else {
  println("Нет данных для регионов NEAR EAST или AFRIKA для вычисления средней площади")
}

// 3. Обновить население для Bahrein на 999
val updatedPopulation = cons.withColumn("Population", when(col("Country") === "Bahrein", 999).otherwise(col("Population")))
updatedPopulation.show()

// 4. Подтаблица с Country и Area
val countryArea = cons.select("Country", "Area")
countryArea.show()
