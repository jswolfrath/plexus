
import org.apache.spark.sql.SparkSession

object ServerMain {

    def main(args: Array[String]) {

        if(args.length != 1) {
            println("usage: ServerMain [config file]")
            System.exit(1)
        }

        val configFile = args(0)
        var config = new SysConfig(configFile)

        if(!config.isValid()) {
            println("Invalid config file. Exiting...")
            System.exit(1)
        }

        val spark = SparkSession
            .builder
            .appName("Geo-Distributed Exact Weights")
            .getOrCreate()


        // Initialize data structures
        var nwmgr = new NetworkIOMgr(config, "orchestrator", config.getOrchestratorTopic())
        var orchestrator = new Orchestrator(config, nwmgr, spark)
        var siteTopics = config.getSiteTopics()
        val numSites = siteTopics.size

        nwmgr.createTopicsAndBlock(siteTopics)

        println("Configuration:")
        config.print()
        
        print("\nPress enter to proceed with query execution: ")

        scala.io.StdIn.readLine()

        nwmgr.sendAsyncCommandToAllSites(siteTopics,
                                         new Command(Command.ID_LIST_TABLES))

        val responses = nwmgr.blockWaitingForAcks(numSites)
        orchestrator.parseTableInfo(responses)

        var query = config.getQuery()
        orchestrator.sampleFromQuery(query, config.getNumSamples())

        nwmgr.sendAsyncCommandToAllSites(siteTopics,
                                         new Command(Command.ID_JOB_TERMINATE))

        // Sleep for a bit to make sure the messages get out
        Thread.sleep(5 * 1000)
        spark.stop()
    }
}
