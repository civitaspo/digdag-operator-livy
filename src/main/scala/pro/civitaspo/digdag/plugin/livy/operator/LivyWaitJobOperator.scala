package pro.civitaspo.digdag.plugin.livy.operator

import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

class LivyWaitJobOperator(context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractLivyOperator(context, systemConfig, templateEngine) {
  override def runTask(): TaskResult = null
}
