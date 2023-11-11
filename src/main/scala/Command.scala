
import scala.collection.mutable.MutableList

import scala.util.Random

object Command {

    val ID_INVALID              = 0x00
    val ID_HEARTBEAT            = 0x01
    val ID_ACK                  = 0x02
    val ID_JOB_TERMINATE        = 0x03
    val ID_LIST_TABLES          = 0x04
    val ID_GET_EXACT_WEIGHTS    = 0x05
    val ID_GET_APPROX_WEIGHTS   = 0x06
    val ID_COLLECT_SAMPLE       = 0x07
    val ID_GET_FULL_TABLE       = 0x08
    val ID_SAMPLE_PREFETCH      = 0x09
    val ID_GET_FILTER           = 0x0A
    val ID_START_PREFETCHING    = 0x0B
    val ID_STOP_PREFETCHING     = 0x0C
}

class Command(id: Int = Command.ID_INVALID,
              payload: String = " ",
              correlator: String = "") {

    protected var mPayload: String = payload
    protected var mId: Int = id
    protected var mCorrelator: String = correlator
    protected var mSender: String = "?"
    protected val DELIMITER = "::"
    protected val PARM_DELIMITER = "&"

    if(correlator.isEmpty) {
        mCorrelator = Random.alphanumeric.filter(_.isDigit).take(8).mkString
    }

    // Copy constructor
    def this(c: Command) = this(c.mId, c.mPayload, c.mCorrelator)

    def isHeartbeat() : Boolean = {
        return mId == Command.ID_HEARTBEAT
    }

    def isAck() : Boolean = {
        return mId == Command.ID_ACK
    }

    def isTerminationCommand() : Boolean = {
        return mId == Command.ID_JOB_TERMINATE
    }

    def isAsyncCommand() : Boolean = {
        return false
    }

    def addParm(parm: String) {

        val valueToAppend: String = parm + PARM_DELIMITER

        if(mPayload == " ") {
            mPayload = valueToAppend
        } else {
            mPayload = mPayload.concat(valueToAppend)
        }
    }

    def addParms(parms: List[String]) {

        for(parm <- parms) {
            addParm(parm)
        }
    }

    def getParms() : List[String] = {

        var parms: Array[String] = mPayload.split(PARM_DELIMITER)
        return parms.toList.map(item => item.trim())
    }

    def marshall() : String = {

        return mSender      + DELIMITER + 
               mId.toString + DELIMITER +
               mCorrelator  + DELIMITER +
               mPayload
    }

    def unmarshall(bytes: String) : Command = {

        var tokens: Array[String] = bytes.split(DELIMITER)
        mSender = tokens(0)
        mId = tokens(1).toInt
        mCorrelator = tokens(2)
        mPayload = tokens(3)
        return this
    }

    def setId(id: Int) = {
        mId = id
    }

    def getId() : Int = {
        return mId
    }

    def getSender() : String = {
        return mSender
    }

    def setSender(sender: String) {
        mSender = sender
    }

    def getPayload() : String = {
        return mPayload
    }

    def getCorrelator() : String = {
        return mCorrelator
    }

    def setCorrelator(token: String) {
        mCorrelator = token
    }

    def getIdAsString() : String = {

        mId match {
            case Command.ID_INVALID =>              return "INVALID"
            case Command.ID_HEARTBEAT =>            return "HEARTBEAT"
            case Command.ID_ACK =>                  return "ACK"
            case Command.ID_JOB_TERMINATE =>        return "JOB_TERMINATE"
            case Command.ID_LIST_TABLES =>          return "LIST_TABLES"
            case Command.ID_GET_EXACT_WEIGHTS =>    return "GET_EXACT_WEIGHTS"
            case Command.ID_GET_APPROX_WEIGHTS =>   return "GET_APPROX_WEIGHTS"
            case Command.ID_COLLECT_SAMPLE =>       return "COLLECT_SAMPLE"
            case Command.ID_GET_FULL_TABLE =>       return "GET_FULL_TABLE"
            case Command.ID_SAMPLE_PREFETCH =>      return "SAMPLE_PREFETCH"
            case Command.ID_GET_FILTER =>           return "GET_FILTER"
            case Command.ID_START_PREFETCHING =>    return "START_PREFETCHING"
            case Command.ID_STOP_PREFETCHING =>     return "STOP_PREFETCHING"
        }

        return "UNKNOWN: " + mId.toString
    }
}
