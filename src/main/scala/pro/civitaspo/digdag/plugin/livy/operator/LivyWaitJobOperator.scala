package pro.civitaspo.digdag.plugin.livy.operator

import java.time.Duration

import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskExecutionException, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.livy.wrapper.{ParamInGiveup, ParamInRetry, RetryExecutorWrapper}
import scalaj.http.{HttpResponse, HttpStatusException}

import scala.collection.JavaConverters._

class LivyWaitJobOperator(context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractLivyOperator(context, systemConfig, templateEngine) {

  class LivyWaitJobException(message: String) extends TaskExecutionException(message)
  class LivyWaitJobRetryableSessionStateException(message: String) extends LivyWaitJobException(message)
  class LivyWaitJobErrorSessionStateException(message: String) extends LivyWaitJobException(message)

  val sessionId: Int = params.get("_command", classOf[Int])
  val successStates: Seq[String] = params.getList("success_states", classOf[String]).asScala
  val errorStates: Seq[String] = params.getListOrEmpty("error_states", classOf[String]).asScala
  val pollingInterval: DurationParam = params.get("polling_interval", classOf[DurationParam], DurationParam.parse("5s"))
  val timeoutDuration: DurationParam = params.get("timeout_duration", classOf[DurationParam], DurationParam.parse("45m"))

  val url: String = s"${baseUrl}/batches/${sessionId}"

  override def runTask(): TaskResult = {
    val responceBody = parseResponce(waitJob)

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
    builder.build()
  }

  protected def waitJob: HttpResponse[String] = {
    RetryExecutorWrapper()
      .withInitialRetryWait(pollingInterval.getDuration)
      .withMaxRetryWait(pollingInterval.getDuration)
      .withRetryLimit(Int.MaxValue)
      .withWaitGrowRate(1.0)
      .withTimeout(timeoutDuration.getDuration)
      .onRetry { p: ParamInRetry =>
        logger.info(
          s"[${operatorName}] retry session_id: $sessionId (next retry: ${p.retryCount}, total wait: ${p.totalWaitMillis} ms, msg: ${p.e.getMessage})"
        )
        logger.debug(s"[${operatorName}] session_id: $sessionId, status: ${p.toString}")
      }
      .onGiveup { p: ParamInGiveup =>
        logger.error(s"[${operatorName}] wait job failed: ${p.firstException.getMessage}")
      }
      .retryIf {
        case ex: LivyWaitJobRetryableSessionStateException =>
          logger.info(ex.getMessage)
          true
        case ex: HttpStatusException =>
          logger.warn(s"[${operatorName}] wait job failed: ${ex.getMessage}")
          if (ex.code / 100 == 4) false
          else true
        case _ => false
      }
      .runInterruptible {
        withHttp(url) { http =>
          val res: HttpResponse[String] = http.asString.throwError
          val responseBody: Config = parseResponce(res)
          val state: String = responseBody.get("state", classOf[String])
          if (errorStates.contains(state)) throw new LivyWaitJobErrorSessionStateException(s"state: ${state} is error state.")
          if (successStates.contains(state)) res
          else throw new LivyWaitJobRetryableSessionStateException(s"state: ${state} is retryable.")
        }
      }
  }
}
