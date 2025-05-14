// задание 2 

// 1. Анализ текстового файла
val rdd = sc.textFile("RDD.txt")
val lines = rdd.count()
val non_empty = rdd.filter(_.nonEmpty)
val non_empty_lines = non_empty.count()
val word = "roses"
val word_count = rdd.filter(_.contains(word)).count()

// 2. Операции с RDD
val rdd1 = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))
val rdd2 = sc.parallelize(Array(("a", 4), ("b", 5), ("c", 6), ("d", 7), ("e", 8)))
val comb = rdd1.union(rdd2)
comb.collect
val aggr = comb.reduceByKey(_ + _)
aggr.collect
val sorted = aggr.sortBy(_._2, false)