
import java.util.{NavigableMap, TreeMap}
import java.io.{File}

import scala.collection.mutable.MutableList

import scala.util.Random
import scala.math.{log, sqrt}

import org.apache.spark.sql.{DataFrame, SparkSession, Row}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions.{abs, avg, lit, col, count, sum, udf, asc, desc, lag, explode, min, max, struct, cume_dist, row_number, last}

import breeze.stats.distributions._

import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest

object SampleUtils {

    def weightedRandomSample(spark: SparkSession,
                             df: DataFrame, ss: Long) : DataFrame = {
        df.cache

        // todo could grab from memo
        val wsum: Double = df.agg(sum("weight").cast("double")).first.getDouble(0)

        val sampUDF = udf( (w:Long, ws: Double) => {
            val p: Double = w.toDouble / wsum
            Binomial(ss.toInt, p).draw
        })
        val arrUDF = udf( (c:Integer) => {
            Array.fill(c){0}
        })

        var samples = df.withColumn("bin", sampUDF(df("weight"), lit(wsum)))
        samples.cache

        samples = samples.withColumn("gen", arrUDF(col("bin")))
                         .withColumn("dummy", explode(col("gen")))
                         .select(col("record_id"))

        df.unpersist
        return samples
    }
}
