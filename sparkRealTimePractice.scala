//TrendyTech Practice
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object sparkRealTimePractice extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
    val sc = new SparkContext("local[*]","RealWorldExample")
    
    val readFile = sc.textFile("D:/trendytech/Week 10/datasets_for_practice/bigdatacampaigndata.csv")
    
    val mappedColumns = readFile.map(x => (x.split(",")(10),x.split(",")(0)))
    
    // Output from above (24.06,big data contents), (59.94,spark training with lab access)
    
    val flattenColumnsByValue = mappedColumns.flatMapValues(x => x.split(" "))
    
    //Output (24.06,big), (24.06,data), (24.06,contents), (59.94,spark), (59.94,training), (59.94,with), (59.94,lab), (59.94,access)
    
    // we would need to reverse to do the aggregation 
    
    val reverseColumns = flattenColumnsByValue.map(x => (x._2,x._1.toFloat))
    //Output (big,24.06), (data,24.06), (contents,24.06), (spark,59.94), (training,59.94), (with,59.94), 
    
    // now the aggregation 
    
    val aggregationData = reverseColumns.reduceByKey((x,y) => x + y)
    
    /// ====> this was wrong as we reduce the String 
    // So i converted into Float ((nural,34.22), (scala,19.982024.318.9824.4919.1634.2234.9617.9119.4827.24), (paper,23.3834.79), (offer,24.75), (guide,34.0219.12),
    
    // post conversion on DF reverseColumn i got the below output which seems consistent now
    //output ((nural,34.22), (scala,250.73), (paper,58.17), (offer,24.75), (guide,53.14), (edvancer,34.35), (ofhadoop,20.87),
    // (standbyexception,29.6), (edition,21.32), (type,12.22), (rands,28.8), (behind,16.78), (inceptez,77.3), (analyst,340.41), (cca175,29.75),
        
    // if we want to take top 20 spendings 
    
   val sortingDescAmountBased = aggregationData.sortBy(x => x._2,false)
   //output (data,16394.64), (big,12889.278), (in,5774.84), (hadoop,4818.34)
   
   // Now to find the top 20 searches and amount spends 
   val top20Searches = sortingDescAmountBased.take(20).foreach(println)
   
  }


//==========================================================================================================
//                                     Output 
// comment out when using as a program
(data,16394.64)
(big,12889.278)
(in,5774.84)
(hadoop,4818.34)
(course,4191.5903)
(training,4099.3696)
(online,3484.4202)
(courses,2565.78)
(intellipaat,2081.22)
(analytics,1458.51)
(tutorial,1383.3701)
(hyderabad,1118.16)
(spark,1078.72)
(best,1047.7)
(bangalore,1039.27)
(and,985.8)
(certification,967.44)
(for,967.05005)
(of,871.42004)
(to,848.32996)
