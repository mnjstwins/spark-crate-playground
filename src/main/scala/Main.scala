import java.sql.{Timestamp, DriverManager}
import java.util.Properties

import akka.actor.{Actor, Props, ActorSystem}
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._
import scala.util.Try

object Main {

  def main(args: Array[String]): Unit = {
    val dbURI = args(0)

    val size = args(1).toInt
    val freq = if (args.length < 3) 1 else args(2).toInt

    val parallelism = if (args.length < 4) 10 else args(3).toInt

    val JDBCDriver = "io.crate.client.jdbc.CrateDriver"
    val ConnectionURL = s"jdbc:crate://$dbURI"

    val table = "spark_limits_test"
    createLimitsTestTable(JDBCDriver, ConnectionURL, table).failed foreach {e => throw e}

    val sc = new SparkContext(new SparkConf().setAppName("spark-limits-test"))
    val sqlContext = SQLContext.getOrCreate(sc)

    byClock(freq) { ts =>
      Future {
        val tickDbg = dbg(ts) _
        tickDbg("Next tick")

        val rows = sc.parallelize(1 to size + 1, parallelism).map { series =>
          DataRow(new Timestamp(ts), s"Series-$series", Math.random())
        }
        sqlContext.createDataFrame(rows)
          .write.mode("append")
          .jdbc(ConnectionURL, table, Map("driver" -> JDBCDriver))

        tickDbg(s"Persisted $size")
      }.failed foreach {System.err.println}
    }
  }

  private def dbg(ts: Long)(msg: Any) = println(s"DEBUG[$ts]: $msg")

  private def createLimitsTestTable(driver: String, connectionURL: String, table: String) = {
    val conTry = Try {
      Class.forName(driver).newInstance
      DriverManager.getConnection(connectionURL)
    }
    val sqlTry = conTry.flatMap { con =>
      Try { con.createStatement().execute(
        s"""CREATE TABLE IF NOT EXISTS $table (
           |time TIMESTAMP,
           |label STRING,
           |value DOUBLE,
           |comment STRING
           |)
           |CLUSTERED INTO 40 shards with (number_of_replicas = 1, refresh_interval=10000)
           |""".stripMargin)
      }
    }

    conTry.foreach { _.close() }
    sqlTry
  }

  private def byClock[T](timeScale: Int)(action: Long => T): Unit = {
    val system = ActorSystem("clock")
    val trigger = system.actorOf(Props(new TimeTrigger(action)))

    system.scheduler.schedule(0.millis, (60 * 1000 / timeScale).millis, trigger, TimeTrigger.Tick)
    system.awaitTermination()
  }

  class TimeTrigger[T](action: Long => T) extends Actor {
    import TimeTrigger._

    def receive: Receive = {
      case Tick => action(System.currentTimeMillis)
    }

  }

  object TimeTrigger {
    object Tick
  }

  case class DataRow(time: Timestamp, label: String, value: Double, comment: String = "Test data")

  implicit def mapToProperties(map: Map[String, String]): Properties = {
    map.foldLeft(new Properties()) { (props, tuple) =>
      props.setProperty(tuple._1, tuple._2)
      props
    }
  }

}
