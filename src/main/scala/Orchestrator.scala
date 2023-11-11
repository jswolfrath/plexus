
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, col, count, greatest, sum, udf, cume_dist}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.util.sketch.{BloomFilter,CountMinSketch}

import scala.collection.mutable.Seq

import scala.collection.mutable.{HashMap, MutableList}

import scala.math._

import breeze.linalg._
import breeze.stats.distributions._

import java.io.{File, FileInputStream, FileOutputStream}

class Orchestrator(var mConfig: SysConfig,
                   var mIoMgr: NetworkIOMgr,
                   var mSparkSession: SparkSession) {
    
    protected class SiteInfo(name: String = "", tables: List[String] = null) {
    
        var mName = name
        var mTables = tables
    }

    protected val mWorkDir = mConfig.getOrchestratorWorkDir()
    protected var mSiteList = new MutableList[SiteInfo]
    protected val mStorageIO = new StorageIOMgr(mConfig, mSparkSession)
    protected var mComputeTime: Long = 0
    protected var mTotalTime: Long = 0

    def sampleFromQuery(query: ChainJoinQuery, sampleSize: Long) {
        
        //
        // BACKWARD pass
        //
        println("Generating weights...")
        pComputeWeights(query)

        var keyCounts: DataFrame = null
        var listOfSamples: MutableList[DataFrame] = new MutableList[DataFrame]

        //
        // FORWARD pass
        //
        for (tblIdx <- 0 to (query.mTables.length - 1) by 1) {

            var table: TableInfo = query.mTables(tblIdx)

            var nextTable: TableInfo = null
            if(tblIdx < query.mTables.length - 1) nextTable = query.mTables(tblIdx+1)

            var tableDf = mSparkSession.createDataFrame(mSparkSession.sparkContext.emptyRDD[Row],
                                                        table.schema)

            var sampleDirs = new MutableList[String]
            var sizesLong: List[Long] = null
            var sizesDfs: List[DataFrame] = null

            println("  [SAMPLE] computing sample size for " + table.getName())

            if(null == keyCounts) {
                sizesLong = pGetPerSiteSampleSizes(table, sampleSize)
            } else {
                sizesDfs = pGetPerSiteSampleSizes(table, keyCounts)
            }
 
            println("  [SAMPLE] requesting samples for " + table.getName())

            var numSites = 0
            // Sample the sites accordingly
            for (eIdx <- 0 to (mSiteList.length - 1) by 1) {

                var einfo = mSiteList(eIdx)
                var siteHasSamples: Boolean = false

                if(null != sizesLong && sizesLong(eIdx) > 0) siteHasSamples = true
                else if(null != sizesDfs && sizesDfs(eIdx).count > 0) siteHasSamples = true

                if(einfo.mTables.contains(table.getName()) && siteHasSamples) {

                    var cmd = new Command(Command.ID_COLLECT_SAMPLE)
                    cmd.addParm(table.name)

                    var keyCountPath = pGetKeyCountPath(einfo, table)
                    var dstSamplePath = pGetSamplePath(einfo, table)

                    pMkdirIfRequired(keyCountPath)
                    pMkdirIfRequired(dstSamplePath)

                    if(null == keyCounts) {

                        cmd.addParm(sizesLong(eIdx).toString)
                        cmd.addParm(" ")
                    }
                    else {

                        val sizesDf = sizesDfs(eIdx)
                        mStorageIO.writeLocalTable(keyCountPath, sizesDf)

                        cmd.addParm("0")
                        cmd.addParm(mIoMgr.getSparkDataFile(keyCountPath))

                        sizesDf.unpersist()
                    }

                    cmd.addParm(dstSamplePath)
                    mIoMgr.sendAsyncCommandToSite(einfo.mName, cmd)


                    numSites = numSites + 1
                    sampleDirs += dstSamplePath
                }
            }

            mIoMgr.blockWaitingForAcks(numSites)

            for(dir <- sampleDirs) {

                var sDf = mStorageIO.readLocalTableWithSchema(dir, table.schema)
                tableDf = tableDf.union(sDf)
            }

            val attrs: List[String] = query.getJoinAttrs(table)

            listOfSamples += tableDf.orderBy(attrs(0))    // Add samples for this table

            // Update key counts for the next table
            val nextAttr = attrs(attrs.length - 1)
            keyCounts = tableDf.groupBy(nextAttr)
                               .agg(count("*").as("count"))
        }

        for (idx <- 0 to (listOfSamples.length - 1) by 1) {

            val tbl = query.mTables(idx)
            val sDf = listOfSamples(idx)
            //sDf.show()
            println("JSW COUNT: " + sDf.count)

            //pWriteResultDf(sDf, tbl)
        }
    }

    def parseTableInfo(cmdList: List[Command]) {
        
        for(cmd <- cmdList) {
            mSiteList += new SiteInfo(cmd.getSender(), cmd.getParms())
        }
    }

    protected def pGetPerSiteSampleSizes(table:        TableInfo,
                                         totalSamples: Long) : List[Long] = {

        var ss = new MutableList[Long]
        var vec = DenseVector.zeros[Double](mSiteList.length)
        var w_sum: Double = 0.0

        // First table. Just use the memo.
        for (idx <- 0 to (mSiteList.length - 1) by 1) {

            var einfo = mSiteList(idx)

            if(einfo.mTables.contains(table.getName())) {

                val memo: DataFrame = pReadMemo(pGetPerSiteMemoDir(einfo.mName, table))
                val wEntry = memo.agg(sum("count").cast("double")).first

                var w: Double = 0.0
                if( ! wEntry.isNullAt(0)) {
                    w = wEntry.getDouble(0)
                }

                w_sum += w
                vec.update(idx, w)
            }

            ss += 0 // reuse this loop to initialize the sample sizes
        }

        vec = vec / scala.math.max(w_sum, 1.0)   // convert to probabilities
/*
        implicit val randBasis: RandBasis = RandBasis.mt0

        def dist = vec
        def params = dist * 1.0
        val mDist = new Multinomial(params)

        // Not sure why computing sample sizes needs to be O(n).
        // Should look for another implementation, maybe in Java.
        for(cnt <- 1 to totalSamples.toInt by 1) {

            val idx = mDist.draw
            ss(idx) = ss(idx) + 1
        }

  */

        for (idx <- 0 to (mSiteList.length - 1) by 1) {
            ss(idx) = scala.math.round(vec(idx)*totalSamples.toDouble).toLong
        }
        return ss.toList
    }

    protected def pGetPerSiteSampleSizes(table:        TableInfo,
                                         keyCounts:    DataFrame) : List[DataFrame] = {

        var ss = new MutableList[DataFrame]

        val sqlContext = mSparkSession.sqlContext
        import sqlContext.implicits._

        val r_samp = (w:Long, ss:Long, wsum:Long) => {
            val p: Double = w.toDouble / wsum
            Binomial(ss.toInt, p).draw.toLong
        }

        val sampUDF = udf(r_samp)

        val allMemos = pGetAllMemosDf(table)
        val countKey: String = keyCounts.columns(0)

        val keyTotals = allMemos.as("m")
                                .join(keyCounts.as("k"), allMemos("key") === keyCounts(countKey))
                                .select($"m.*")
                                .groupBy("key")
                                .agg(sum("count").as("total"))

        // These will be used for each site DF computation
        allMemos.cache()
        keyCounts.cache()
        keyTotals.cache()

        for (idx <- 0 to (mSiteList.length - 1) by 1) {

            var ename = mSiteList(idx).mName
            var siteCounts = allMemos.where(allMemos("site") === ename)
                                     .as("m")
                                     .join(keyTotals.as("k"), $"m.key" === keyTotals("key"))
                                     .join(keyCounts.as("c"), $"m.key" === keyCounts(countKey))
                                     .withColumn("gen", sampUDF($"m.count", $"c.count", $"k.total"))
                                     .select($"m.key", $"gen")
                                     .withColumnRenamed("gen", "count")

            siteCounts.cache()
            siteCounts = siteCounts.filter(siteCounts("count") > 0)

            ss += siteCounts
        }

        return ss.toList
    }

    protected def pComputeWeights(query: ChainJoinQuery) {

        var lastMemoDir: String = " "
        var lastTable: TableInfo = null

        // This is the BACKWARD pass
        for (tblIdx <- (query.mTables.length - 1) to 0 by -1) {

            var table: TableInfo = query.mTables(tblIdx)
            var siteMemoDirs: MutableList[String] = new MutableList[String]

            var numSites = 0
            for(einfo <- mSiteList) {

                if(einfo.mTables.contains(table.getName())) {

                    val outputMemoDir: String = pGetPerSiteMemoDir(einfo.mName, table)

                    var cmd = new Command(Command.ID_GET_EXACT_WEIGHTS)
                    cmd.addParm(table.getName())
                    cmd.addParm(lastMemoDir)
                    cmd.addParm(outputMemoDir)
                    
                    mIoMgr.sendAsyncCommandToSite(einfo.mName, cmd)

                    if(tblIdx != 0) {   // All tables but the last should have memos

                        siteMemoDirs += outputMemoDir
                    }

                    numSites = numSites + 1
                }
            }

            mIoMgr.blockWaitingForAcks(numSites)

            if(siteMemoDirs.length > 0) {

                var aggMemoDf = mSparkSession.createDataFrame(mSparkSession.sparkContext.emptyRDD[Row],
                                                              LocalSchema.freq_tbl)

                for(dir <- siteMemoDirs) {

                    aggMemoDf = aggMemoDf.union(pReadMemo(dir))
                }

                aggMemoDf = aggMemoDf.groupBy("key")
                                     .agg(sum("count").as("count"))
                

                val memoPath: String = mWorkDir + "out_memo/" + table.name + "/"
                pWriteMemo(aggMemoDf, memoPath)
                lastMemoDir = mIoMgr.getSparkDataFile(memoPath)
            }
            else {
                lastMemoDir = " "
            }
            
            lastTable = table
        }
    }

    protected def pGetAllMemosDf(table: TableInfo) : DataFrame = {

        var allMemos = mSparkSession.createDataFrame(mSparkSession.sparkContext.emptyRDD[Row],
                                                     LocalSchema.site_memo_tbl)

        for (idx <- 0 to (mSiteList.length - 1) by 1) {

            var einfo = mSiteList(idx)
            if(einfo.mTables.contains(table.getName())) {

                val memo = pReadMemo(pGetPerSiteMemoDir(einfo.mName, table))
                allMemos = allMemos.union(memo.withColumn("site", lit(einfo.mName)))
            }
        }

        return allMemos
    }

    protected def pIsTableAtMoreThanOneSite(table: TableInfo) : Boolean = {

        var cnt = 0

        for (idx <- 0 to (mSiteList.length - 1) by 1) {

            if(mSiteList(idx).mTables.contains(table.getName())) {

                cnt = cnt + 1
                if(cnt > 1) return true
            }
        }

        return false
    }

    protected def pGetPerSiteDataDir(site:  String,
                                     table: TableInfo) : String = {

        val dir: String = mWorkDir + site + "/" + table.name + "/data/"
        pMkdirIfRequired(dir)
        return dir
    }

    protected def pWriteResultDf(df: DataFrame) {

        val dir: String = mConfig.getOrchestratorResultDir()
        pMkdirIfRequired(dir)

        mStorageIO.writeLocalTable(dir, df)
    }

    protected def pWriteResultDf(df: DataFrame, table: TableInfo) {

        val dir: String = mConfig.getOrchestratorResultDir() + table.name + "/"
        pMkdirIfRequired(dir)

        mStorageIO.writeLocalTable(dir, df)
    }

    protected def pGetPerSiteMemoDir(site:  String,
                                     table: TableInfo) : String = {

        val dir: String = mWorkDir + site + "/" + table.name + "/memo/"
        pMkdirIfRequired(dir)
        return dir
    }

    protected def pGetPerSitePrefetchDir(site:  String,
                                         table: TableInfo) : String = {

        val dir: String = mWorkDir + site + "/" + table.name + "/prefetch/"
        pMkdirIfRequired(dir)
        return dir
    }

    protected def pGetKeyCountPath(site: SiteInfo, table: TableInfo) : String = {
        return mWorkDir + site.mName + "/" + table.name + "/counts/"
    }

    protected def pGetSamplePath(site: SiteInfo, table: TableInfo) : String = {
        return mWorkDir + site.mName + "/" + table.name + "/samples/"
    }

    protected def pGetCombinedFilterPath(table1: TableInfo, attr1: String,
                                         table2: TableInfo, attr2: String) : String = {
        val subdir = table1.getName() + "-" + attr1 + "_" + table2.getName() + "-" + attr2
        return mWorkDir + "out_filter/" + subdir + "/"
    }

    protected def pGetFilterPath(site: SiteInfo, table: TableInfo, attr: String) : String = {
        return mWorkDir + site.mName + "/" + table.name + "/filter/" + attr + "/"
    }

    protected def pGetPrefetchOutputDir(table: TableInfo) : String = {
        return mWorkDir + "out_prefetch/" + table.name + "/"
    }

    protected def pReadMemo(path: String) : DataFrame = {

        val df = mStorageIO.readLocalTableWithSchema(path, LocalSchema.freq_tbl)
        if(mConfig.useCompression()) {
            val newNames = Seq("key", "count")
            return df.toDF(newNames: _*)
        }
        return df
    }

    protected def pWriteMemo(memo: DataFrame, path: String) {

        pMkdirIfRequired(path)
        mStorageIO.writeLocalTable(path, memo)
    }

    protected def pMkdirIfRequired(path: String) {

        var dir: File = new File(path)
        if (! dir.exists()) {
            dir.mkdirs();
        }   
    }
}
