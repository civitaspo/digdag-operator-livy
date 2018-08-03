package pro.civitaspo.digdag.plugin.livy.operator

import com.google.common.base.Optional
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam

import scala.collection.JavaConverters._

class LivySubmitJobOperator(context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractLivyOperator(context, systemConfig, templateEngine) {

  val job: Config = params.getNested("job")
  val waitUntilFinished: Boolean = params.get("wait_until_finished", classOf[Boolean], true)
  val waitTimeoutDuration: DurationParam = params.get("wait_timeout_duration", classOf[DurationParam], DurationParam.parse("45m"))

  override def runTask(): TaskResult = {
    null
  }

  protected def submitJob = {
    val file: String = job.get("file", classOf[String])
    val proxyUser: Optional[String] = job.getOptional("proxy_user", classOf[String])
    val className: Optional[String] = job.getOptional("class_name", classOf[String])
    val args: Seq[String] = job.getListOrEmpty("args", classOf[String]).asScala
    val jars: Seq[String] = job.getListOrEmpty("jars", classOf[String]).asScala
    val pyFiles: Seq[String] = job.getListOrEmpty("py_files", classOf[String]).asScala
    val files: Seq[String] = job.getListOrEmpty("files", classOf[String]).asScala
    val driverMemory: Optional[String] = job.getOptional("driver_memory", classOf[String])
    val driverCores: Optional[Int] = job.getOptional("driver_cores", classOf[Int])
    val executorMemory: Optional[String] = job.getOptional("executor_memory", classOf[String])
    val executorCores: Optional[Int] = job.getOptional("executor_cores", classOf[Int])
    val numExecutors: Optional[Int] = job.getOptional("num_executors", classOf[Int])
    val archives: Seq[String] = job.getListOrEmpty("archives", classOf[String]).asScala
    val queue: Optional[String] = job.getOptional("queue", classOf[String])
    val name: String = job.get("name", classOf[String], s"digdag-${params.get("session_uuid", classOf[String])}")
    val conf: Map[String, String] = params.getMapOrEmpty("conf", classOf[String], classOf[String]).asScala.toMap


  }
}
