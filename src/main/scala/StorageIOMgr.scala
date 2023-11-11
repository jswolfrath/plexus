
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}

import org.apache.spark.sql.types.StructType

import java.io.{File, FileInputStream, FileOutputStream}
 
import org.apache.spark.sql.functions.{col, lit, min, max}
import org.apache.spark.util.sketch.{BloomFilter,CountMinSketch}

class StorageIOMgr(config: SysConfig,
                   sparkSession: SparkSession) {

    protected val mConfig: SysConfig = config
    protected var mSpark:  SparkSession = sparkSession

    def readLocalTableNoSchema(path: String) : DataFrame = {

        if(mConfig.useCompression()) {
            return mSpark.read.parquet(path)
        }

        return mSpark.read.format("csv")
                     .option("header", "true")
                     .option("inferSchema", "false")
                     .option("delimiter", ",")
                     .load(path)
    }

    def readLocalTableWithSchema(path: String, schema: StructType) : DataFrame = {

        if(mConfig.useCompression()) {
            return mSpark.read.parquet(path)
        }

        return mSpark.read.format("csv")
                     .option("header", "true")
                     .option("inferSchema", "false")
                     .option("delimiter", ",")
                     .schema(schema)
                     .load(path)
    }

    def readLocalTable(site: String, tblInfo: TableInfo) : DataFrame = {

        val path = mConfig.getSiteDataDirectory(site) + tblInfo.getName()
        var df: DataFrame = null

        if("csv" == tblInfo.getFormat()) {

            df = mSpark.read.format("csv")
                       .option("header", "true")
                       .option("inferSchema", "false")
                       .option("delimiter", ",")
                       .schema(tblInfo.schema)
                       .load(path)
        }
        else {
            df = mSpark.read.parquet(path)
        }

        return df
    }

    def writeLocalTable(path: String, df: DataFrame) {

        if(mConfig.useCompression()) {

            df.repartition(1)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(path)
        }
        else {

            df.repartition(1)
              .write.format("csv")
              .option("header","true")
              .mode(SaveMode.Overwrite)
              .save(path)
        }
    }
}
