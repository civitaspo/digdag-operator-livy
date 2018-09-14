package pro.civitaspo.digdag.plugin.livy.operator

import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.livy.wrapper.{NotRetryableException, ParamInGiveup, ParamInRetry, RetryableException, RetryExecutorWrapper}
import scalaj.http.{HttpResponse, HttpStatusException}

import scala.collection.JavaConverters._

class LivyWaitJobOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractLivyOperator(operatorName, context, systemConfig, templateEngine) {

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
        logger.error(s"[${operatorName}] wait job failed: ${p.lastException.getMessage}")
      }
      .retryIf {
        case ex: RetryableException =>
          logger.info(ex.getMessage)
          true
        case ex: HttpStatusException =>
          logger.warn(s"[${operatorName}] wait job failed: ${ex.getMessage}")
          if (ex.code / 100 == 4) false
          else true
        case ex: Exception =>
          logger.error(s"[${operatorName}] wait job failed: ${ex.getMessage}")
          false
      }
      .runInterruptible {
        withHttp(url) { http =>
          val res: HttpResponse[String] = http.asString.throwError
          val responseBody: Config = parseResponce(res)
          val state: String = responseBody.get("state", classOf[String])
          if (errorStates.contains(state)) throw new NotRetryableException(message = s"state: ${state} is error state.")
          if (successStates.contains(state)) res
          else throw new RetryableException(message = s"state: ${state} is retryable.")
        }
      }
  }
}
