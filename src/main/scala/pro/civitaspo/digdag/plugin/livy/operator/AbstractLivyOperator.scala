package pro.civitaspo.digdag.plugin.livy.operator

import io.digdag.client.config.{Config, ConfigFactory}
import io.digdag.spi.{OperatorContext, TemplateEngine}
import io.digdag.util.BaseOperator
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.collection.JavaConverters._

abstract class AbstractLivyOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine) extends BaseOperator(context) {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected val cf: ConfigFactory = request.getConfig.getFactory
  protected val params: Config = {
    val elems: Seq[String] = operatorName.split("\\.")
    elems.indices.foldLeft(request.getConfig) { (p: Config, idx: Int) =>
      p.mergeDefault((0 to idx).foldLeft(request.getConfig) { (nestedParam: Config, keyIdx: Int) =>
        nestedParam.getNestedOrGetEmpty(elems(keyIdx))
      })
    }
  }

  protected val host: String = params.get("host", classOf[String])
  protected val port: Int = params.get("port", classOf[Int], 8998)
  protected val scheme: String = params.get("scheme", classOf[String], "http")
  protected val header: Map[String, String] = params.getMapOrEmpty("header", classOf[String], classOf[String]).asScala.toMap

  protected lazy val baseUrl: String = s"${scheme}://${host}:${port}"

  protected def withHttp[T](url: String)(f: HttpRequest => T): T = {
    val http: HttpRequest = Http(url).headers(("Content-type", "application/json"), header.toSeq: _*)
    f(http)
  }

  protected def parseResponce(res: HttpResponse[String]): Config = {
    cf.fromJsonString(res.body)
  }
}
