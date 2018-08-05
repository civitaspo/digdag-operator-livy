package pro.civitaspo.digdag.plugin.livy

import java.util.{Arrays => JArrays, List => JList}
import java.lang.reflect.Constructor

import io.digdag.client.config.Config
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, OperatorProvider, Plugin, TemplateEngine}
import javax.inject.Inject
import pro.civitaspo.digdag.plugin.livy.operator.{AbstractLivyOperator, LivySubmitJobOperator}

object LivyPlugin {

  class LivyOperatorProvider extends OperatorProvider {

    @Inject protected var systemConfig: Config = null
    @Inject protected var templateEngine: TemplateEngine = null

    override def get(): JList[OperatorFactory] = {
      JArrays.asList(operatorFactory("livy.submit_job", classOf[LivySubmitJobOperator]))
    }

    private def operatorFactory[T <: AbstractLivyOperator](operatorName: String, klass: Class[T]): OperatorFactory = {
      new OperatorFactory {
        override def getType: String = operatorName
        override def newOperator(context: OperatorContext): Operator = {
          val constructor: Constructor[T] = klass.getConstructor(classOf[OperatorContext], classOf[Config], classOf[TemplateEngine])
          constructor.newInstance(context, systemConfig, templateEngine)
        }
      }
    }
  }
}

class LivyPlugin extends Plugin {
  override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] = {
    if (`type` ne classOf[OperatorProvider]) return null
    classOf[LivyPlugin.LivyOperatorProvider].asSubclass(`type`)
  }
}
