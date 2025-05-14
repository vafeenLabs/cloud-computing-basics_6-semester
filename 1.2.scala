// Вариант 2 
import org.apache.spark.sql.functions._

// Чтение CSV с заголовком и определением типов
val con = spark.read.option("header", "true").option("inferSchema", "true").csv("countries2.csv")

// 1. Страны с площадью > 30000
val largeCountries = con.filter(col("Площадь") > 30000).select("Название", "Площадь")
largeCountries.show()

// 2. Страны Карибского региона с площадью > 200
val caribbeanCountries = con.filter(col("Регион") === "Caribbean" && col("Площадь") > 200)
  .select("Название", "Регион", "Площадь")
caribbeanCountries.show()

// 3. Обновить площадь для Andorra на 555
val updatedArea = con.withColumn("Площадь", when(col("Название") === "Andorra", 555).otherwise(col("Площадь")))
updatedArea.show()

// 4. Подтаблица с Континентом и Регионом
val continentRegion = con.select("Континент", "Регион")
continentRegion.show()