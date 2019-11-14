package logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    
    test("Compute top 10 URL") {
        var log1 = "uplherc.upl.com - - [01/Aug/1995:00:02:11 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0589.gif HTTP/1.0\" 200 45846"
        var log2 = "piweba4y.prodigy.com - - [01/Aug/1995:00:02:12 -0400] \"GET /history/apollo/images/footprint-small.gif HTTP/1.0\" 200 18149"
        var log3 = "piweba1y.prodigy.com - - [01/Aug/1995:00:02:13 -0400] 200 7280"

        val utils = new Utils

        val list = List(log1, log2, log3)
        val rdd = sc.parallelize(list);

        assert(rdd.count == list.length)   

        val records = utils.gettop10urls(rdd, sc)
        assert(records.length == 2)    
        assert(records(0)._1 == "/shuttle/missions/sts-71/images/KSC-95EC-0589.gif")
    }
    
    test("Compute top 5 Hosts") {
        var log1 = "uplherc.upl.com - - [01/Aug/1995:00:02:11 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0589.gif HTTP/1.0\" 200 45846"
        var log2 = "piweba4y.prodigy.com - - [01/Aug/1995:00:02:12 -0400] \"GET /history/apollo/images/footprint-small.gif HTTP/1.0\" 200 18149"
        var log3 = "piweba1y.prodigy.com - - [01/Aug/1995:00:02:13 -0400] 200 7280"

        val utils = new Utils

        val list = List(log1, log2, log3)
        val rdd = sc.parallelize(list);

        assert(rdd.count == list.length)   

        val records = utils.gettop5Hosts(rdd, sc)
        assert(records.length == 2)    
        assert(records(0)._1 == "piweba4y.prodigy.com")
    }
    
    
    test("Compute top5 time stamps by traffic") {
        var log1 = "uplherc.upl.com - - [01/Aug/1995:00:02:11 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0589.gif HTTP/1.0\" 200 45846"
        var log2 = "piweba4y.prodigy.com - - [01/Aug/1995:00:02:12 -0400] \"GET /history/apollo/images/footprint-small.gif HTTP/1.0\" 200 18149"
        var log3 = "piweba1y.prodigy.com - - [01/Aug/1995:00:02:13 -0400] 200 7280"
        
        val utils = new Utils

        val list = List(log1, log2, log3)
        val rdd = sc.parallelize(list);

        val records = utils.gettop5timeframe(rdd, sc, false)
        assert(records.length == 1)
        assert(records(0)._1 == "01/Aug/1995:00")
    }
    
    test("Compute HTTP") {
        var log1 = "uplherc.upl.com - - [01/Aug/1995:00:02:11 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0589.gif HTTP/1.0\" 200 45846"
        var log2 = "piweba4y.prodigy.com - - [01/Aug/1995:00:02:12 -0400] \"GET /history/apollo/images/footprint-small.gif HTTP/1.0\" 200 18149"
        var log3 = "piweba1y.prodigy.com - - [01/Aug/1995:00:02:13 -0400] 200 7280"
        
        val utils = new Utils

        val list = List(log1, log2, log3)
        val rdd = sc.parallelize(list);

        val records = utils.countHTTP(rdd, sc)
        assert(records.length == 1)
        assert(records(0)._1 == 200)
    }
}