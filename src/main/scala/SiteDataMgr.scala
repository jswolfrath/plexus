
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList

import scala.util.control.Breaks._

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}

import org.apache.spark.sql.functions.{lit, col, count, sum, udf, asc, desc, monotonically_increasing_id, explode}

import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}

class SiteDataMgr(var config:       SysConfig,
                  var siteName:     String,
                  var networkIO:    NetworkIOMgr,
                  var sparkSession: SparkSession) {

    protected val mSiteName = siteName
    protected val mDataDir = config.getSiteDataDirectory(siteName)
    protected val mWorkDir = config.getSiteWorkDirectory(siteName)

    protected var mSpark = sparkSession
    protected var mIoMgr = networkIO
    protected var mConfig = config
    protected var mTableMap = new HashMap[TableInfo, TablePartition]
    protected val mStorage = new StorageIOMgr(mConfig, mSpark)

    protected val mLock = new Object()

    pInitialize()
        
    def processCommand(cmd: Command) {

        val sqlContext = mSpark.sqlContext
        import sqlContext.implicits._

        var query: ChainJoinQuery = mConfig.getQuery()
        var parms: List[String] = cmd.getParms()
        val response = new Command(Command.ID_ACK)
        var curTable: TablePartition = null

        println("\n[CMD] " + cmd.getIdAsString())
        pTraceCommandParms(cmd)

        if(Command.ID_LIST_TABLES == cmd.getId()) {

            for ((tinfo,tpart) <- mTableMap) {

                response.addParm(tinfo.getName())
            }
        }
        else if(Command.ID_GET_EXACT_WEIGHTS == cmd.getId()) {

            pHandleGetWeightsCommand(cmd, response)
        }
        else if(Command.ID_COLLECT_SAMPLE == cmd.getId()) {

            pHandleCollectSampleCommand(cmd, response)
        }

        mIoMgr.sendOrchestratorCommandAsync(response)
    }

    def doAsyncWork() {

        // Nothing to do currently
    }

    def updateAsyncThread(cmd: Command) {

        // Nothing to do currently
    }

    protected def pHandleGetWeightsCommand(cmd: Command, resp: Command) {

        var query: ChainJoinQuery = mConfig.getQuery()
        var parms: List[String] = cmd.getParms()

        val tableInfo = pFindTableInfo(parms(0))
        val curTable = mTableMap(tableInfo)

        val remoteMemoPath = parms(1)
        val remoteOutputPath = parms(2)
        val localPath = pGetOutputMemoDir(tableInfo)

        var memo: DataFrame = null
        if(remoteMemoPath.trim().length > 0) {

            val memoPath = pGetInputMemoDir(tableInfo)
            mIoMgr.downloadFile("orchestrator", remoteMemoPath, memoPath)
            memo = pReadMemo(memoPath)

            memo.cache()
            memo.count()
        }

        curTable.load()

        var wDf = curTable.computeWeights(query, memo)
        wDf.cache()
        val keyCnt = wDf.count()

        mStorage.writeLocalTable(localPath, wDf)
        mIoMgr.uploadFile("orchestrator", remoteOutputPath, localPath)

        if(null != memo) memo.unpersist(true)
        wDf.unpersist(true)
        curTable.evict()
    }

    protected def pHandleCollectSampleCommand(cmd: Command, resp: Command) {

        var query: ChainJoinQuery = mConfig.getQuery()
        var parms: List[String] = cmd.getParms()

        val tableInfo = pFindTableInfo(parms(0))
        val curTable = mTableMap(tableInfo)

        val numSamples = parms(1).toLong
        val remoteSampleSizePath = parms(2)
        val remoteOutputPath = parms(3)
        val localSamplesPath = pGetLocalSamplesPath(tableInfo)
        val localSizesPath = pGetLocalSizesPath(tableInfo)

        var sampleDf: DataFrame = null
        var req = numSamples.toInt

        curTable.cache()

        if(numSamples > 0) {

            // No samples per key; sample across all local tuples
            sampleDf = curTable.sample(numSamples)
        }
        else {

            mIoMgr.downloadFile("orchestrator", remoteSampleSizePath, localSizesPath)

            var sampleKeys = mStorage.readLocalTableWithSchema(localSizesPath,
                                                               LocalSchema.freq_tbl)
                                     .where(col("count") =!= 0)

            sampleKeys.count()  // force in memory for timings

            sampleDf = curTable.sample(sampleKeys, pReadMemo(pGetOutputMemoDir(tableInfo)))

            // Just debug info, not strictly required
            req = sampleKeys.agg(sum("count").cast("double")).first.getDouble(0).toInt
        }
    
        sampleDf.cache()
        val scnt = sampleDf.count

        println("  [SAMPLE]:  requested " + req + " samples")
        println("  [SAMPLE]:  collected " + scnt + " samples")

        mStorage.writeLocalTable(localSamplesPath, sampleDf)
        mIoMgr.uploadFile("orchestrator", remoteOutputPath, localSamplesPath)

        curTable.evict()
        sampleDf.unpersist(true)
    }

    protected def pTraceCommandParms(cmd: Command) {

        var parms: List[String] = cmd.getParms()
        for(parm <- parms) { println("  [PARM]:    " + parm) }
    }

    protected def pFindTableInfo(name: String) : TableInfo = {

        for ((k,v) <- mTableMap) { if(k.getName() == name) return k }

        return null
    }

    protected def pGetInputMemoDir(table: TableInfo) : String = {

        val memoPath = mWorkDir + table.getName() + "_memo_in/"
        pMkdirIfRequired(memoPath)

        return memoPath
    }

    protected def pGetOutputMemoDir(table: TableInfo) : String = {

        val localPath = mWorkDir + table.getName() + "/out_memo/"
        pMkdirIfRequired(localPath)

        return localPath
    }

    protected def pGetLocalSizesPath(table: TableInfo) : String = {

        val localSizesPath = mWorkDir + table.getName() + "/sizes/"
        pMkdirIfRequired(localSizesPath)

        return localSizesPath
    }

    protected def pGetLocalSamplesPath(table: TableInfo) : String = {

        val localSamplesPath = mWorkDir + table.getName() + "/samples/"
        pMkdirIfRequired(localSamplesPath)

        return localSamplesPath    
    }

    protected def pReadMemo(path: String) : DataFrame = {

        if(mConfig.useCompression())
        {
            val df = mStorage.readLocalTableWithSchema(path, LocalSchema.freq_tbl)
            val newNames = Seq("key", "count")
            return df.toDF(newNames: _*)
        }
        
        return mSpark.read.format("csv")
                     .option("header", "true")
                     .option("inferSchema", "false")
                     .option("delimiter", ",")
                     .schema(LocalSchema.freq_tbl)
                     .load(path)
    }

    protected def pInitialize() {

        val file = new File(mDataDir)
        println("data dir: " + mDataDir)
        println("files: " + file.listFiles)
        var tblNameList = file.listFiles
                              .filter(_.isDirectory)
                              .map(_.getName).toList

        for(name <- tblNameList) {

            val tblInfo = LocalSchema.getTableInfoByName(name)
            val tblPart = new TablePartition(mConfig, mSpark, tblInfo, mSiteName,
                                             mDataDir + tblInfo.getName() + "/raw.csv")
            mTableMap += tblInfo -> tblPart

            if(mConfig.getQuery().isLastTable(tblInfo)) {
                //tblPart.load()
            }
        }

        pMkdirIfRequired(mWorkDir)
    }

    protected def pMkdirIfRequired(path: String) {

        var dir: File = new File(path);
        if (! dir.exists()) {
            dir.mkdirs();
        }   
    }
}
