package pro.civitaspo.digdag.plugin.livy.operator

import java.time.Duration

import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.livy.wrapper.{ParamInGiveup, ParamInRetry, RetryExecutorWrapper}
import scalaj.http.{HttpResponse, HttpStatusException}

import scala.collection.JavaConverters._

class LivySubmitJobOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractLivyOperator(operatorName, context, systemConfig, templateEngine) {

  val job: Config = params.getNested("job")
  val waitUntilFinished: Boolean = params.get("wait_until_finished", classOf[Boolean], true)
  val waitTimeoutDuration: DurationParam = params.get("wait_timeout_duration", classOf[DurationParam], DurationParam.parse("45m"))

  val url: String = s"${baseUrl}/batches"

  override def runTask(): TaskResult = {
    val responceBody = parseResponce(submitJob(jobJson))

    val sessionId: Int = responceBody.get("id", classOf[Int])
    val applicationId: Optional[String] = responceBody.getOptional("appId", classOf[String])
    val applicationInfo: Config = responceBody.getNestedOrGetEmpty("appInfo")
    val state: String = responceBody.get("state", classOf[String])

    val result: Config = cf.create()
    val last: Config = result.getNestedOrSetEmpty("livy").getNestedOrSetEmpty("last_job")
    last.set("session_id", sessionId)
    last.set("application_id", applicationId)
    last.set("application_info", applicationInfo)
    last.set("state", state)

    val builder = TaskResult.defaultBuilder(request)
    builder.resetStoreParams(ImmutableList.of(ConfigKey.of("livy", "last_job")))
    builder.storeParams(result)
    if (waitUntilFinished) builder.subtaskConfig(buildWaiterSubTaskConfig(sessionId))
    builder.build()
  }

  protected def jobJson: String = {
    val file: String = job.get("file", classOf[String])
    val proxyUser: Optional[String] = job.getOptional("proxy_user", classOf[String])
    val className: Optional[String] = job.getOptional("class_name", classOf[String])
    val args: Seq[String] = job.parseListOrGetEmpty("args", classOf[String]).asScala
    val jars: Seq[String] = job.parseListOrGetEmpty("jars", classOf[String]).asScala
    val pyFiles: Seq[String] = job.parseListOrGetEmpty("py_files", classOf[String]).asScala
    val files: Seq[String] = job.parseListOrGetEmpty("files", classOf[String]).asScala
    val driverMemory: Optional[String] = job.getOptional("driver_memory", classOf[String])
    val driverCores: Optional[Int] = job.getOptional("driver_cores", classOf[Int])
    val executorMemory: Optional[String] = job.getOptional("executor_memory", classOf[String])
    val executorCores: Optional[Int] = job.getOptional("executor_cores", classOf[Int])
    val numExecutors: Optional[Int] = job.getOptional("num_executors", classOf[Int])
    val archives: Seq[String] = job.parseListOrGetEmpty("archives", classOf[String]).asScala
    val queue: Optional[String] = job.getOptional("queue", classOf[String])
    val name: String = job.get("name", classOf[String], s"digdag-${params.get("session_uuid", classOf[String])}")
    val conf: Map[String, String] = params.getMapOrEmpty("conf", classOf[String], classOf[String]).asScala.toMap

    val content: Config = cf.create()
    content.set("file", file)
    if (proxyUser.isPresent) content.set("proxyUser", proxyUser.get())
    if (className.isPresent) content.set("className", className.get())
    if (args.nonEmpty) content.set("args", seqAsJavaList(args))
    if (jars.nonEmpty) content.set("jars", seqAsJavaList(jars))
    if (pyFiles.nonEmpty) content.set("pyFiles", seqAsJavaList(pyFiles))
    if (files.nonEmpty) content.set("files", seqAsJavaList(files))
    if (driverMemory.isPresent) content.set("driverMemory", driverMemory.get())
    if (driverCores.isPresent) content.set("driverCores", driverCores.get())
    if (executorMemory.isPresent) content.set("executorMemory", executorMemory.get())
    if (executorCores.isPresent) content.set("executorCores", executorCores.get())
    if (numExecutors.isPresent) content.set("numExecutors", numExecutors.get())
    if (archives.nonEmpty) content.set("archives", seqAsJavaList(archives))
    if (queue.isPresent) content.set("queue", queue.get())
    content.set("name", name)
    if (conf.nonEmpty) content.set("conf", mapAsJavaMap(conf))

    content.toString
  }

  protected def submitJob(json: String): HttpResponse[String] = {
    logger.info(s"[${operatorName}] submit job: ${json}")
    RetryExecutorWrapper()
      .withInitialRetryWait(Duration.ofSeconds(1L))
      .withMaxRetryWait(Duration.ofSeconds(30L))
      .withRetryLimit(3)
      .withWaitGrowRate(2.0)
      .onRetry { p: ParamInRetry =>
        logger.info(s"[${operatorName}] retry ${json} (next retry: ${p.retryCount}, total wait: ${p.totalWaitMillis} ms)")
        logger.debug(s"[${operatorName}] content: ${json}, status: ${p.toString}")
      }
      .onGiveup { p: ParamInGiveup =>
        logger.error(s"[${operatorName}] submit job failed: ${p.firstException.getMessage}")
      }
      .retryIf {
        case ex: HttpStatusException =>
          logger.warn(s"[${operatorName}] submit job failed: ${ex.getMessage}")
          if (ex.code / 100 == 4) false
          else true
        case _ => false
      }
      .runInterruptible {
        withHttp(url) { http =>
          val res: HttpResponse[String] = http.postData(json).asString
          res.throwError
        }
      }
  }

  protected def buildWaiterSubTaskConfig(sessionId: Int): Config = {
    val subTask: Config = cf.create()
    subTask.set("_command", sessionId)
    subTask.set("_type", "livy.wait_job")
    subTask.set("success_states", seqAsJavaList(Seq("success")))
    subTask.set("error_states", seqAsJavaList(Seq("error", "dead", "killed")))
    subTask.set("timeout_duration", waitTimeoutDuration)

    subTask.set("host", host)
    subTask.set("port", port)
    subTask.set("scheme", scheme)
    subTask.set("header", header)

    subTask
  }
}
