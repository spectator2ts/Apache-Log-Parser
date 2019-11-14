package logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

    "parseURL" should "parse URLs" in {
            
        val utils = new Utils
        val testLog = "uplherc.upl.com - - [01/Aug/1995:00:01:48 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939"
        val testURL = utils.parseURL(testLog)
        assert(testURL == "/shuttle/missions/sts-71/images/KSC-95EC-0423.gif")
    
    }

    "parseHost" should "parse hosts" in {
        
        val utils = new Utils
        val testLog = "uplherc.upl.com - - [01/Aug/1995:00:01:48 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939" 
        val testHost = utils.parseHost(testLog)
        assert(testHost == "uplherc.upl.com")
    
    }
      
    "parseTimeStamps" should "parse timestamps" in {
    
        val utils = new Utils
        val testLog = "uplherc.upl.com - - [01/Aug/1995:00:01:48 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939" 
        val testTimeStamp = utils.parseTimeStamps(testLog)
        assert(testTimeStamp == "01/Aug/1995:00:01:48 -0400")
    
    }        
     
    "parseHTTP" should "parse HTTP" in {
    
        val utils = new Utils
        val testLog = "uplherc.upl.com - - [01/Aug/1995:00:01:48 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939" 
        val testHTTP = utils.parseHTTP(testLog)
        assert(testHTTP == 200)
    
    }            
    
    "isIntact" should "check if a log is intact or not" in {
    
        val utils = new Utils
        val testLog = "uplherc.upl.com - - [01/Aug/1995:00:01:48 -0400] \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939" 
        assert(utils.isIntact(testLog))
        
        val testLog2 = "uplherc.upl.com - - \"GET /shuttle/missions/sts-71/images/KSC-95EC-0423.gif HTTP/1.0\" 200 64939"
        assert(!utils.isIntact(testLog2))
    }             
               
}
