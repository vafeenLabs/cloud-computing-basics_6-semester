// Вариант 1 
import org.apache.spark.sql.functions._

// Чтение CSV с заголовком и определением типов
val st1 = spark.read.option("header", "true").option("inferSchema", "true").csv("students1.csv")

// 1. Имена студентов с оценкой по Java > 60
val javaStudents = st1.filter(col("Java") > 60).select("Имя", "Java")
javaStudents.show()

// 2. Студенты с оценкой по Python выше среднего
val avgPython = st1.agg(avg("Python")).first().getDouble(0)
val pythonAboveAvg = st1.filter(col("Python") > avgPython)
pythonAboveAvg.show()

// 3. Обновить оценку по PHP для Jack на 5
val updatedPHP = st1.withColumn("PHP", when(col("Имя") === "Jack", 5).otherwise(col("PHP")))
updatedPHP.show()

// 4. Подтаблица с колонками Имя и Python
val subTable = st1.select(col("Имя").alias("name"), col("Python").alias("python"))
subTable.show()