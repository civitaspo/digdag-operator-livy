package pro.civitaspo.digdag.plugin.livy.operator

import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.standards.operator.state.TaskState
import io.digdag.util.DurationParam

class LivySubmitJobOperator(context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractLivyOperator(context, systemConfig, templateEngine) {

  val state: TaskState = TaskState.of(request)
  val job: Config = params.getNested("job")
  val waitUntilFinished: Boolean = params.get("wait_until_finished", classOf[Boolean], true)
  val waitTimeoutDuration: DurationParam = params.get("wait_timeout_duration", classOf[DurationParam], DurationParam.parse("45m"))

  override def runTask(): TaskResult = {
    null
  }
}
