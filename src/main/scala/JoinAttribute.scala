
import org.apache.spark.sql.types.StructType

class TableInfo(val name:   String,
                val format: String, // csv, tbl, etc.
                val schema: StructType,
                val tupleSize: Long = 64) {

    override def equals(that: Any): Boolean =
    that match {
      case that: TableInfo => 
        this.name == that.name && this.schema == that.schema
      case _ => false
    }

    def getName() : String = {
        return name
    }

    def getFormat() : String = {
        return format
    }

    def getTupleSizeInBytes() : Long = {
        return tupleSize
    }
}

class JoinAttr(val srcColumn: String,
               val dstColumn: String) {

}

class TableColumn(val tableName:  String,
                  val columnName: String) {

    override def toString: String = tableName + "_" + columnName
}

class JoinAttribute(val srcColumn:  TableColumn,
                    val dstColumn:  TableColumn) {

}
