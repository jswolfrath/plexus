
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import java.io.File

class SysConfig(filename: String) {

    protected var mConfig = ConfigFactory.parseFile(new File(filename))

    def getKafkaHost() : String = {
        return mConfig.getString("kafka.host")
    }

    def getKafkaPort() : Int = {
        return mConfig.getInt("kafka.port")
    }

    def getOrchestratorTopic() : String = {
        return mConfig.getString("orchestrator.kafka-topic")
    }

    def getOrchestratorWorkDir() : String = {
        return mConfig.getString("orchestrator.work-dir")
    }

    def getOrchestratorResultDir() : String = {
        return mConfig.getString("orchestrator.result-dir")
    }

    def getHostById(id: String) : String = {

        if(id == "orchestrator") {
            return mConfig.getString("orchestrator.host")
        }

        return mConfig.getString("sites." + id + ".host")
    }

    def getSiteNames() : List[String] = {

        var sList = new MutableList[String]

        mConfig.getObject("sites").asScala.foreach({
            case (k, v) => sList += k
        })

        return sList.toList
    }

    def getSiteIndex(id: String) : Long = {
        
        val names = getSiteNames()
        return names.indexOf(id).toLong
    }

    def getNumSites() : Long = {
        return getSiteTopics().length
    }

    def getSiteTopics() : List[String] = {

        var sList = new MutableList[String]

        mConfig.getObject("sites").asScala.foreach({
            case (k, v) => sList += mConfig.getString("sites." + k + ".kafka-topic")
        })

        return sList.toList
    }

    def getSshUserById(id: String) : String = {

        if(id == "orchestrator") {
            return mConfig.getString("orchestrator.ssh-user")
        }

        return mConfig.getString("sites." + id + ".ssh-user")
    }

    def getSshPasswordById(id: String) : String = {
        
        if(id == "orchestrator") {
            return mConfig.getString("orchestrator.ssh-user")
        }

        return mConfig.getString("sites." + id + ".ssh-user")
    }

    def getSiteTopic(name: String) : String = {
        val sid = "sites." + name + ".kafka-topic"
        return mConfig.getString(sid)
    }

    def getSiteDataDirectory(name: String) : String = {
        val sid = "sites." + name + ".data-dir"
        return mConfig.getString(sid)
    }

    def getSiteWorkDirectory(name: String) : String = {
        val sid = "sites." + name + ".work-dir"
        return mConfig.getString(sid)
    }

    def useCompression() : Boolean = {
        return "yes" == mConfig.getString("strategy.compression")
    }

    def getQuery() : ChainJoinQuery = {

        var query: ChainJoinQuery = null

        val table0 = new TableInfo("tbl0", "csv", LocalSchema.tbl0_schema)
        val table1 = new TableInfo("tbl1", "csv", LocalSchema.tbl1_schema)

        val JOIN_TABLES = List(table0, table1)
        val JOIN_ATTRS = List(new JoinAttr("key","key"))

        return new ChainJoinQuery(JOIN_TABLES, JOIN_ATTRS)
    }

    def getNumSamples() : Long = {
        return mConfig.getLong("strategy.num-samples")
    }

    def isValid() : Boolean = {

        var valid = true
        return valid
    }

    def print() {

        println("------------------------------------------")
        println("  Config file:        " + filename)
        println("  Number of sites:    " + getSiteNames().length)
        println("  Query:              " + mConfig.getString("query.name"))
        println("  Dataset:            " + mConfig.getString("query.dataset"))
        println("  Join Algorithm:     " + mConfig.getString("strategy.algorithm"))
        println("  Compression:        " + mConfig.getString("strategy.compression"))
        println("------------------------------------------")
    }   
}
