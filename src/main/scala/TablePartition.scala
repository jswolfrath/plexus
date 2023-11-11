
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, col, count, sum, udf, asc, desc, monotonically_increasing_id, explode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.util.sketch.{BloomFilter,CountMinSketch}

import scala.collection.mutable.{HashMap, MutableList}

import breeze.stats.distributions._

import java.io.File

import scala.math._

class TablePartition(var mConfig:        SysConfig,
                     var mSparkSession:  SparkSession,
                     val mTable:         TableInfo,
                     val mSiteId:        String,
                     val mRootFile:      String) {

    protected var mDataFrame: DataFrame = null
    protected val mStorage = new StorageIOMgr(mConfig, mSparkSession)
    protected var mNumTuples: Long = 0
    protected var mPrefetchTimeAllowedInMillis: Long = 0

    def load() {

        pLoadDataFrame()
        cache()
    }

    def cache() {

        mDataFrame.cache()
        mNumTuples = mDataFrame.count()
    }

    def evict() {
        mDataFrame.unpersist(true)
    }

    def getTableInfo() : TableInfo = {
        return mTable
    }

    def computeWeights(query: ChainJoinQuery,
                       memo:  DataFrame) : DataFrame = {

        val sqlContext = mSparkSession.sqlContext
        import sqlContext.implicits._

        var df = pLoadDataFrame()
        var nextMemoDf: DataFrame = null
        val attrs: List[String] = query.getJoinAttrs(mTable)

        // JSW delete
        // Cleanup any old weights from previous queries
        //if(df.columns.contains("weight")) {
        //    df = df.drop("weight")
        //}

        if(query.isFirstTable(mTable)) {

            // Compute weights
            df = df.as("t")
                   .join(memo.as("m"), df(attrs(0)) === memo("key"))
                   .select($"t.*",$"m.count")
                   .withColumnRenamed("count","weight")

            nextMemoDf = df.groupBy(attrs(0))
                           .agg(sum("weight").as("count"))
        }
        else if(query.isLastTable(mTable)) {

            // First table in the backward pass gets 1's
            df = df.withColumn("weight", lit(1).cast(LongType))

            nextMemoDf = df.groupBy(attrs(0))
                           .agg(count("*").as("count"))
        }
        else { // we are in the middle somewhere

            // Compute weights
            df = df.as("t")
                   .join(memo.as("m"), df(attrs(1)) === memo("key"))
                   .select($"t.*",$"m.count")
                   .withColumnRenamed("count","weight")

            nextMemoDf = df.groupBy(attrs(0))
                           .agg(sum("weight").as("count"))

        }

        // just eliminate tuples not participating in the join now
        df = df.filter(df("weight") > 0)
        pUpdateDataFrame(df)

        return nextMemoDf
    }

    def sample(numSamples: Long) : DataFrame = {

        var df = pLoadDataFrame()
        return pWeightedSample(df, numSamples)
    }

    def sample(keyCounts: DataFrame) : DataFrame = {

        var df = pLoadDataFrame()
        return pWeightedSample(df, keyCounts)
    }

    def sample(kc: DataFrame, memo: DataFrame) : DataFrame = {

        var df = pLoadDataFrame()
        var keyCounts = kc

        val sqlContext = mSparkSession.sqlContext
        import sqlContext.implicits._

        val r_samp = (w:Long, ss:Long, wsum:Long) => {
            val p: Double = w.toDouble / wsum.toDouble
            val s: Int = Binomial(ss.toInt, p).draw
            val arr: Array[Int] = Array.fill(s){0}
            arr
        }

        val r_samp_p = (w:Long, ss:Long, wsum:Long) => {
            val p: Double = w.toDouble / wsum.toDouble
            Binomial(ss.toInt, p).draw
        }

        val r_arr = (cnt:Int) => {
            Array.fill(cnt){0}
        }

        val sampUDF = udf(r_samp)
        val samp_pUDF = udf(r_samp_p)
        val samp_aUDF = udf(r_arr)

        val keyCol = mConfig.getQuery().getJoinAttrs(mTable)(0)

        val tmpDf = keyCounts.as("k")
                         .join(memo.as("m"), memo("key") === keyCounts("key"))
                         .join(df.as("t"),   df(keyCol) === keyCounts("key"))
                         .select($"t.*",$"k.count",$"m.count")
                         .withColumn("binom", samp_pUDF($"t.weight", $"k.count", $"m.count"))
                         .withColumn("barr",  samp_aUDF($"binom"))
                         //.limit(20)

        tmpDf.cache()

        val s = tmpDf
                         .withColumn("dummy", explode($"barr"))
                         .drop("dummy")
                         .drop("barr")
                         .drop("binom")
                         .drop($"k.count")
                         .drop($"m.count")
                         .drop("record_id")
                         .drop("weight")

        return s
    }

    protected def pWeightedSample(df: DataFrame, ss: Long) : DataFrame = {

        val result = SampleUtils.weightedRandomSample(mSparkSession, df, ss)
        val samples = df.drop("weight")
                 .as("t")
                 .join(result.as("k"), df("record_id") === result("record_id"))
                 .select("t.*")
                 .drop("record_id")
                 .drop("weight")

        return samples
    }

    protected def pWeightedSample(df: DataFrame, keyCounts: DataFrame) : DataFrame = {

        var df = pLoadDataFrame()
        val keyCol = mConfig.getQuery().getJoinAttrs(mTable)(0)
        var sampleKeys = mSparkSession.createDataFrame(mSparkSession.sparkContext.emptyRDD[Row],
                                                       LocalSchema.record_id_schema)
        for (row <- keyCounts.rdd.collect)  // TODO this is inefficient
        {   
            val k = row.getString(0)
            val c = row.getLong(1)

            val subDf = df.filter(df(keyCol) === k)
            val result = SampleUtils.weightedRandomSample(mSparkSession, subDf, c)

            sampleKeys = sampleKeys.union(result)
        }

        val samples = df.as("t")
                 .join(sampleKeys.as("k"), df("record_id") === sampleKeys("record_id"))
                 .select("t.*")
                 .drop("weight")
                 .drop("record_id")

        return samples
    }

    protected def pUpdateDataFrame(df: DataFrame) {

        mDataFrame = df
    }

    protected def pLoadDataFrame() : DataFrame = {

        if(null == mDataFrame) {

            mDataFrame = mStorage.readLocalTable(mSiteId, mTable)
                                 .withColumn("record_id", monotonically_increasing_id())
        }

        return mDataFrame
    }
}
