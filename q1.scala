// Question a
val simpleDF = spark.read.option("header", "true").csv("/home/icyblast/Desktop/simpleCSV.csv")
simpleDF.show()

// Question b
// print data frame using printSchema() function
simpleDF.printSchema()

// Question c
// Convert DataFrame to RDD by using '.rdd'
val simpleRDD = simpleDF.rdd
// Because the size of RDD is not too big, 
// so we can use '.collect()' to show all the contents. 
// Otherwise it is better to use '.take(n)'
simpleRDD.collect().foreach(println)

// Question d
// call the system environment variable $SPARK_HOME
val textfileRDD = sc.textFile(sys.env("SPARK_HOME") + "/README.md")
// display all the contents of textfile
//textfileRDD.collect().foreach(println)

// Question e
val textfileLength = textfileRDD.flatMap(line => line.split("\n").map(_.length)).sum().toInt
println("Total length: " + textfileLength)
//textfileLength.collect().foreach(println)

// Question f
val wordPairs = textfileRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
wordPairs.collect().foreach(println)

// Question g
val totalWordCount = textfileRDD.flatMap(line => line.split(" ")).filter(!_.trim.equals("")).count()
println(totalWordCount)

// Question h
// factorial function
def factorial(n : Long) : Long = if (n == 0) 1 else n * factorial(n - 1)
Array(1, 2, 3, 4, 5).map(x => factorial(x)).sum
// create RDD 
//val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
// map each element to the factorial function
//rdd.map(x => factorial(x)).sum().toInt
