
import java.io.{File, IOException}
import java.nio.file.{Files, Path, StandardCopyOption}

import java.time.Duration
import java.time.temporal.ChronoUnit

import java.util
import java.util.HashMap
import java.util.Properties
import java.util.concurrent.Future

import scala.util.Random

import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import org.apache.kafka.common.KafkaFuture

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsResult, NewTopic}

import java.io.FileInputStream;
import java.io.FileOutputStream;

import net.schmizz.sshj.{SSHClient, DefaultConfig}
import net.schmizz.sshj.common.KeyType
import net.schmizz.sshj.xfer.FileSystemFile
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.keepalive.KeepAliveProvider

class NetworkIOMgr(config: SysConfig,
                   senderId: String,
                   myClientTopic: String) {

    protected val mOrchestratorTopic = config.getOrchestratorTopic()
    protected val mClientTopic = myClientTopic
    protected val mServerString = config.getKafkaHost() + ":" + config.getKafkaPort().toString
    protected var mConsumer: KafkaConsumer[String, String] = null
    protected val mSenderId = senderId
    protected val mConfig = config

    protected var mProducerProps = new Properties()
    mProducerProps.put("bootstrap.servers", mServerString)
    mProducerProps.put("client.id", "IOMgr")
    mProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    mProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    protected var mConsumerProps = new Properties()
    mConsumerProps.put("bootstrap.servers", mServerString)
    mConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    mConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    mConsumerProps.put("group.id", "None")
    mConsumerProps.put("auto.offset.reset", "earliest")
    mConsumerProps.put("enable.auto.commit", "true")
    mConsumerProps.put("auto.commit.interval.ms", "1000")
    mConsumerProps.put("session.timeout.ms", "30000")

    def generateCorrelator() : String = {
        
        return Random.alphanumeric.filter(_.isDigit).take(8).mkString
    }

    def sendOrchestratorCommandAsync(cmd: Command) {

        pWriteTopic(mOrchestratorTopic, cmd)
    }

    def sendAsyncCommandToAllSites(siteTopics: List[String], cmd: Command) {

        for(siteTopic <- siteTopics) {
            pWriteTopic(siteTopic, cmd)
        }
    }

    def sendAsyncCommandToSite(site: String, cmd: Command) {

        pWriteTopic(mConfig.getSiteTopic(site), cmd)
    }

    def readCommandsWithBlock() : List[Command] = {

        return pReadTopicWithBlock(mClientTopic)
    }

    def blockWaitingForAcks(count: Int) : List[Command] = {

        val acks = new MutableList[Command]

        while(acks.length < count) {

            val cmds = readCommandsWithBlock()
            for(cmd <- cmds) { if(cmd.isAck()) acks += cmd }
        }

        return acks.toList
    }

    def createTopicsAndBlock(siteTopics: List[String]) {

        var props = new Properties()
        props.put("bootstrap.servers", mServerString);

        var admin: AdminClient = AdminClient.create(props)

        var configs = new HashMap[String, String]
        val partitions: Int = 1;
        val replication: Short = 1;

        var topicList = new MutableList[NewTopic]

        for(topic <- siteTopics) {
            topicList += new NewTopic(topic, partitions, replication).configs(configs)
        }

        admin.createTopics(topicList.toList.asJava).all().get()
    }

    def getSparkDataFile(path: String) : String = {

        var dir = new File(path)
        for(f <- dir.listFiles()) {
            if(f.isFile() && f.getName().startsWith("part-")) {
                return path + f.getName()
            }
        }

        return path // nothing found, return parent
    }

    def downloadFile(srcId: String, remotePath: String, localPath: String) {

        // Assumes the remote path already has the file name correct
        if(pIsRunningOnSameMachine(srcId)) {

            pMoveFile(remotePath, localPath)
        }
        else {

            val ssh: SSHClient = pGetSshClientAndConnect(srcId)

            try {
                ssh.newSCPFileTransfer().download(remotePath, new FileSystemFile(localPath))
            }
            finally {
                ssh.disconnect()
            }
        }
    }

    def uploadFile(dstId: String, remotePath: String, localPath: String) {

        val srcFile = getSparkDataFile(localPath)
        val bytes: Long = new File(srcFile).length()

        if(pIsRunningOnSameMachine(dstId)) {

            pMoveFile(srcFile, remotePath) 
        }
        else {

            val ssh: SSHClient = pGetSshClientAndConnect(dstId)

            try {
                ssh.newSCPFileTransfer().upload(new FileSystemFile(srcFile), remotePath)
            }
            finally {
                ssh.disconnect()
            }
        }
    }

    protected def pGetSshClientAndConnect(sysId: String) : SSHClient = {

        val sshConfig: DefaultConfig = new DefaultConfig()
        sshConfig.setKeepAliveProvider(KeepAliveProvider.KEEP_ALIVE)

        val ssh:SSHClient = new SSHClient(sshConfig);
        ssh.addHostKeyVerifier(new PromiscuousVerifier())
        ssh.getConnection().getKeepAlive().setKeepAliveInterval(5)
        ssh.connect(mConfig.getHostById(sysId))
        ssh.authPassword(mConfig.getSshUserById(sysId), mConfig.getSshPasswordById(sysId))

        return ssh
    }

    protected def pReadTopicWithBlock(topic: String) : List[Command] = {

        val timeout = Duration.of(100, ChronoUnit.MILLIS)
        val consumer = pGetKafkaConsumer()
        var payloadList = new MutableList[Command]

        consumer.subscribe(util.Arrays.asList(topic))

        var records = consumer.poll(timeout)
        while(records.count() == 0) {

            records = consumer.poll(timeout)
        }

        var itr = records.iterator()
        while(itr.hasNext()) {
            val record: ConsumerRecord[String, String] = itr.next()
            val offset = record.offset()
            val key = String.valueOf(record.key())
            val value = record.value()

            payloadList += new Command().unmarshall(value)
        }

        return payloadList.toList
    }

    protected def pWriteTopic(topic: String, cmd: Command) : Future[RecordMetadata] = {

        cmd.setSender(mSenderId)

        var producer = new KafkaProducer[String, String](mProducerProps)
        val record = new ProducerRecord(topic, "cmd", cmd.marshall())

        val future = producer.send(record)
        producer.close()

        return future
    }

    protected def pGetKafkaConsumer() : KafkaConsumer[String, String] = {

        if(mConsumer == null) {
            mConsumer = new KafkaConsumer[String, String](mConsumerProps)
        }

        return mConsumer
    }

    protected def pMoveFile(src: String, dst: String) {

        var srcFile = new File(src)
        val bytes: Double = srcFile.length().toDouble
        val dstPath = new File(dst + srcFile.getName()).toPath

        try {
            // Files.copy and Files.move are also options here
            Files.createSymbolicLink(dstPath, srcFile.toPath)
        }
        catch {
            case _: Throwable => println("Failed to simulate file move on local machine")
        }
    }

    protected def pByteCountStr(byteCount: Long) : String = {

        var bytes: Long = byteCount
        if (bytes < 1024) { return bytes.toString() + " B" }
        bytes = bytes / 1024

        if (bytes < 1024) { return bytes.toString() + " KiB" }
        bytes = bytes / 1024

        if (bytes < 1024) { return bytes.toString() + " MiB" }
        bytes = bytes / 1024

        return bytes.toString() + " GiB"
    }

    protected def pIsRunningOnSameMachine(procId: String) : Boolean = {

        val host1 = mConfig.getHostById(mSenderId)
        val host2 = mConfig.getHostById(procId)

        return host1 == host2
    }
}
