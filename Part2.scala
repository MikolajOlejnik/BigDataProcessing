import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TotalGoals {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Total Goals")
    val sc = new SparkContext(conf)
    // All season CSVs in directory
    val allData = sc.textFile("~/Part2/season*.csv")
    // Filter out headings
    val filteredData = allData.filter(x => x.split(",")(0) != "Div")
    // filteredData RDD will be used twice, hence we save it in memory to improve performance
    filteredData.persist()
    // Extract home goals column as team/goals-scored key-value pair
    val homeGoals = filteredData.map(line => {
      val goal = line.split(",")
      (goal(2), goal(4).toInt)
    })
    // Extract away goals column as team/goals-scored key-value pair
    val awayGoals = filteredData.map(line => {
      val goal = line.split(",")
      (goal(3), goal(5).toInt)
    })
    // Combine RDDs
    val merged = homeGoals ++ awayGoals
    // Add all goals by grouping together by key and adding all numbers corresponding to the same key together
    val totalGoals = merged.reduceByKey ( (a,b) => a + b )
    // Save the output
    totalGoals.saveAsTextFile("~/Part2/spark-output")
  }
}
