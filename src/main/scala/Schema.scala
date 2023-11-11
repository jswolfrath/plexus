
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object LocalSchema {

    val freq_tbl = new StructType()
        .add("key", StringType, true)
        .add("count", LongType, true)

    val dist_tbl = new StructType()
        .add("key", StringType, true)
        .add("freq", DoubleType, true)

    val site_memo_tbl = new StructType()
        .add("key", StringType, true)
        .add("count", LongType, true)
        .add("site", StringType, true)

    val tbl0_schema = new StructType()
        .add("key", StringType, true)
        .add("value", StringType, true)

    val tbl1_schema = new StructType()
        .add("key", StringType, true)
        .add("value", StringType, true)

    val tbl_metadata = new StructType()
        .add("key",    StringType, true)
        .add("count",  LongType,   true)
        .add("site",   StringType, true)
        .add("weight", LongType,   true)

    val test_schema = new StructType()
        .add("key", StringType, true)

    val record_id_schema = new StructType()
        .add("record_id", LongType, true)

    def getTableInfoByName(name: String) : TableInfo = {

        if(name == "tbl0") {
            return new TableInfo(name, "csv", LocalSchema.tbl0_schema)
        }
        else if(name == "tbl1") {
            return new TableInfo(name, "csv", LocalSchema.tbl1_schema)
        }

        return null
    }
}

