package logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
    
    def isIntact(log: String):Boolean = {
        val res = PATTERN.findFirstMatchIn(log); //determine whether string is intact
        return !res.isEmpty
    }
    
    //------------------------- Top 10 URLs ----------------------------
    // parse URL from the log
    def parseURL(log: String):(String) = {
        val res = PATTERN.findFirstMatchIn(log).get;
        return (res.group(6))
    }
    
    // get the top 10 requested URLs
    def gettop10urls(accessLogs:RDD[String], sc:SparkContext):Array[(String,Int)] = {
        val urlAccessLogs = accessLogs.filter(isIntact)
        var parsedURL = urlAccessLogs.map(parseURL)
        var url_tuples = parsedURL.map((_, 1));
        var frequencies = url_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(10)
    }
    
    //------------------------- Top 5 Hosts ----------------------------
    // parse host from the log
    def parseHost(log: String):(String) = {
        val res = PATTERN.findFirstMatchIn(log).get;
        return (res.group(1))
    }
    
    // get the top 5 hosts
    def gettop5Hosts(accessLogs:RDD[String], sc:SparkContext):Array[(String,Int)] = {
        val hostAccessLogs = accessLogs.filter(isIntact)
        var parsedHost = hostAccessLogs.map(parseHost)
        var host_tuples = parsedHost.map((_, 1));
        var frequencies = host_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(5)
    }
    
    //------------------------- Top 5 Time Frames (Highest/Lest Traffic) ----------------------------
    // parse Timestamps from the log
    def parseTimeStamps(log: String):(String) = {
        val res = PATTERN.findFirstMatchIn(log).get;
        return (res.group(4))
    }
    
    // get top 5 time frames for highest/least traffic
    def gettop5timeframe(accessLogs:RDD[String], sc:SparkContext, isAsc:Boolean):Array[(String,Int)] = {
        // isAsc = true: least traffic; asc = false: highest traffic
        
        val urlAccessLogs = accessLogs.filter(isIntact)
        //get map timestamp logs to substring(0, 14) to get the date and hour
        var parsedTimeFrame = urlAccessLogs.map(parseTimeStamps).map(_.substring(0, 14))
        var tf_tuples = parsedTimeFrame.map((_, 1));
        var frequencies = tf_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, isAsc)
        return sortedfrequencies.take(5)
    }
    
    //------------------------- Unique HTTP Counts ----------------------------
    // parse HTTP code from the log
    def parseHTTP(log: String):(Int) = {
        val res = PATTERN.findFirstMatchIn(log).get;
        return (res.group(8).toInt)
    }

    // find HTTP code
    def countHTTP(accessLogs:RDD[String], sc:SparkContext):Array[(Int,Int)] = {
        val urlAccessLogs = accessLogs.filter(isIntact)
        var parsedHTTP = urlAccessLogs.map(parseHTTP)
        var http_tuples = parsedHTTP.map((_, 1));
        var frequencies = http_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        //only return the top 10 HTTP for convenience
        return sortedfrequencies.take(10)
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <file_or_directory_in_hdfs>
        Eample: EntryPoint /data/spark/project/NASA_access_log_Aug95.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 2) {
            println("Expected:2 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("ParseLogs")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        var accessLogs = sc.textFile(args(1))
        
        val top10url = utils.gettop10urls(accessLogs, sc)
        println("===== URL Count =====")
        for(i <- top10url){
            println(i)
        }
        
        val top5Hosts = utils.gettop5Hosts(accessLogs, sc)
        println("===== Hosts Count =====")
        for(i <- top5Hosts){
            println(i)
        }
        
        val top5timeframe = utils.gettop5timeframe(accessLogs, sc, false)
        println("===== TOP 5 Time Frames for Highest Traffic =====")
        for(i <- top5timeframe){
            println(i)
        }
        
        val last5timeframe = utils.gettop5timeframe(accessLogs, sc, true)
        println("===== Top 5 Time Frames for Least Traffic =====")
        for(i <- last5timeframe){
            println(i)
        }
        
        val httpcodesCount = utils.countHTTP(accessLogs, sc)
        println("====== HTTP Code Count =======")
        for(i <- httpcodesCount){
            println(i)
        }
        
    }
}

