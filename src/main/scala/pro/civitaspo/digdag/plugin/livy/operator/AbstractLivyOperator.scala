package pro.civitaspo.digdag.plugin.livy.operator

import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TemplateEngine}
import io.digdag.util.BaseOperator
import org.slf4j.{Logger, LoggerFactory}

abstract class AbstractLivyOperator (context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine) extends BaseOperator(context) {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected val params: Config = request.getConfig.mergeDefault(request.getConfig.getNestedOrGetEmpty("livy"))

  protected val host: String = params.get("host", classOf[String])
  protected val port: Int = params.get("port", classOf[Int], 8998)
  protected val scheme: String = params.get("schema", classOf[String], "http")
}
