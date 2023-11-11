
import java.io.File
import java.util.concurrent.Executors

import scala.collection.mutable.HashMap

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}

object ClientMain {
    
    def main(args: Array[String]): Unit = {

        if(args.length != 2) {
            println("usage: ClientMain [config file] [siteId]")
            System.exit(1)
        }

        val configFile = args(0)
        val siteId = args(1)
        var config = new SysConfig(configFile)

        if(!config.isValid()) {
            println("Invalid config file. Exiting...")
            System.exit(1)
        }

        val spark = SparkSession.builder
                                .appName("Join_Client")
                                .getOrCreate()

        // Initialize data structures
        var nwIoMgr = new NetworkIOMgr(config, siteId, config.getSiteTopic(siteId))
        var siteDataMgr = new SiteDataMgr(config, siteId, nwIoMgr, spark)

        // Start threads
        val executor = Executors.newFixedThreadPool(2)
        var asyncThread = new Runnable() {
            def run() {
                siteDataMgr.doAsyncWork()
            }
        }
        executor.submit(asyncThread)

        println("Client " + siteId + " ready to accept commands...")

        var shouldExit = false
        while( ! shouldExit) {

            var cmdList = nwIoMgr.readCommandsWithBlock()
            for(cmd <- cmdList) {

                if(cmd.isTerminationCommand()) {

                    // Just process on this thread
                    siteDataMgr.processCommand(cmd)

                    shouldExit = true
                    executor.shutdownNow()
                }
                else if(cmd.isAsyncCommand()) {
                    siteDataMgr.updateAsyncThread(cmd)
                }
                else if( ! shouldExit) {
                    var threadRunnable = new Runnable() {
                        def run() {
                            try {
                              siteDataMgr.processCommand(cmd)
                            }
                            catch {
                                case e: Exception => println("Exception occurred : " + e) ; e.printStackTrace()
                            }
                        }
                    }
                    executor.submit(threadRunnable)
                }
            }
        }

        spark.stop()
    }
}

