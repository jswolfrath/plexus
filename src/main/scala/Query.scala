
import scala.collection.mutable.MutableList

trait JoinQuery {

    def isChainJoin() : Boolean
}

class ChainJoinQuery(var mTables: List[TableInfo],
                     var mAttrs:  List[JoinAttr]) extends JoinQuery {

    // Ensure consistency for input parms
    assert(mAttrs.length == (mTables.length - 1))

    override def isChainJoin() : Boolean = { return true }

    def isFirstTable(table: TableInfo) : Boolean = {
        return mTables.head == table
    }

    def isLastTable(table: TableInfo) : Boolean = {
        return mTables.last == table
    }

    def getTableIndex(table: TableInfo) : Int = {
        return mTables.indexOf(table)
    }

    def getJoinAttrs(table: TableInfo) : List[String] = {

        var attrs = new MutableList[String]
        var tblIdx = getTableIndex(table)

        if(tblIdx == 0) {
            attrs += mAttrs(0).srcColumn
        }
        else if(tblIdx == (mTables.length - 1)) {
            attrs += mAttrs(mAttrs.length - 1).dstColumn
        }
        else {
            attrs += mAttrs(tblIdx - 1).dstColumn
            attrs += mAttrs(tblIdx).srcColumn
        }

        return attrs.toList
    }
}

class AcyclicQuery() extends JoinQuery {

    override def isChainJoin() : Boolean = { return false }
}
