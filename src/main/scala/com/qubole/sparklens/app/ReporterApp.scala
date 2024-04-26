package com.qubole.sparklens.app

import com.ning.compress.lzf.LZFInputStream
import com.qubole.sparklens.QuboleJobListener
import com.qubole.sparklens.analyzer.AppAnalyzer
import com.qubole.sparklens.common.{AppContext, Json4sWrapper}
import com.qubole.sparklens.helper.EmailReportHelper
import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.history.EventHistoryReplayer
import org.apache.spark.{HDFSConfigHelper, SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.xerial.snappy.SnappyInputStream

import java.io.{BufferedInputStream, InputStream}
import java.net.URI


object ReporterApp extends App {

  val usage = "Need to specify sparklens data file\n" +
    "Of specify event-history file and also add \"source=history\" or \"source=sparklens\".\n" +
    "If \"source\" is not specified, sparklens is chosen by default." +
    "If \"source=history\", specify \"appId=your_spark_app_idhistory\" and optionally \"attemptId=1\" if needed."

  val conf = new SparkConf()

  checkArgs()
  parseInput()

  private def checkArgs(): Unit = {
    args.size match {
      case x if x < 1 => throw new IllegalArgumentException(usage)
      case _ => // Do nothing
    }
  }

  private def startAnalysersFromAppContext(appContext: AppContext): Unit = {
    AppAnalyzer.startAnalyzers(appContext)
  }


  private def parseInput(): Unit = {
    getSource match {
      case "sparklens" => {
        throw new Exception("Sparklens dumps are no longer supported, please use a history file.")
      }
      case _ =>
        getAppId match {
          case Some(appId) => // History Provider
            val sparkConf = SparkContext.getOrCreate().getConf
            new EventHistoryReplayer(sparkConf, appId, getAttemptId)
          case _ =>
            new EventHistoryReporter(args(0)) // event files
      }
    }
  }

  private def getSource: String = {
    args.foreach(arg => {
      val splits = arg.split("=")
      if (splits.size == 2) {
        if ("source".equalsIgnoreCase(splits(0))) {
          if ("history".equalsIgnoreCase(splits(1))) {
            return "history"
          } else return "sparklens"
        } else new IllegalArgumentException(usage)
      }
    })
    return "sparklens"

  }

  private def getAppId: Option[String] = {
    args.foreach(arg => {
      val splits = arg.split("=")
      if (splits.size == 2 && "appId".equalsIgnoreCase(splits(0))) {
          return Option(splits(1))
      } else None
    })
    None
  }

  private def getAttemptId: Option[String] = {
    args.foreach(arg => {
      val splits = arg.split("=")
      if (splits.size == 2 && "attemptId".equalsIgnoreCase(splits(0))) {
          return Option(splits(1))
      }
    })
    None
  }

  def reportFromEventHistory(file: String): Unit = {
    val busKlass = Class.forName("org.apache.spark.scheduler.ReplayListenerBus")
    val bus = busKlass.newInstance()

    val addListenerMethod = busKlass.getMethod("addListener", classOf[java.lang.Object])

    val conf = new SparkConf()
      .set("spark.sparklens.reporting.disabled", "false")
      .set("spark.sparklens.save.data", "false")

    val listener = new QuboleJobListener(conf)

    addListenerMethod.invoke(bus, listener)


    val replayMethod = busKlass.getMethod("replay", classOf[InputStream], classOf[String],
      classOf[Boolean], classOf[(String) => Boolean])

    replayMethod.invoke(bus, getDecodedInputStream(file, conf), file, boolean2Boolean(false),
      getFilter _)
  }

  // Borrowed from CompressionCodecs in spark
  private def getDecodedInputStream(file: String, conf: SparkConf): InputStream = {

    val fs = FileSystem.get(new URI(file), HDFSConfigHelper.getHadoopConf(Some(conf)))
    val path = new Path(file)
    val bufStream = new BufferedInputStream(fs.open(path))

    val logName = path.getName.stripSuffix(".inprogress")
    val codecName: Option[String] = logName.split("\\.").tail.lastOption

    codecName.getOrElse("") match {
      case "lz4" => new LZ4BlockInputStream(bufStream)
      case "lzf" => new LZFInputStream(bufStream)
      case "snappy" => new SnappyInputStream(bufStream)
      case _ => bufStream
    }

  }

  private def getFilter(eventString: String): Boolean = {
    implicit val formats = DefaultFormats
    eventFilter.contains(Json4sWrapper.parse(eventString).extract[Map[String, Any]].get("Event")
      .get.asInstanceOf[String])
  }

  private def eventFilter: Set[String] = {
    Set(
      "SparkListenerTaskEnd",
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "SparkListenerExecutorAdded",
      "SparkListenerExecutorRemoved",
      "SparkListenerJobStart",
      "SparkListenerJobEnd",
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted",
      "SparkListenerEnvironmentUpdate" // To get the config
    )
  }

}
