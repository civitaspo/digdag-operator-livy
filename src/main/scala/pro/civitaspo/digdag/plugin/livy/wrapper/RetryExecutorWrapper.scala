package pro.civitaspo.digdag.plugin.livy.wrapper

import java.time.Duration
import java.util.concurrent.Callable

import io.digdag.util.RetryExecutor
import io.digdag.util.RetryExecutor.{GiveupAction, RetryAction, RetryGiveupException, RetryPredicate}

case class ParamInWrapper(timeoutDurationMillis: Int, totalWaitMillisCounter: Iterator[Int] = Stream.from(0).iterator)
case class ParamInRetry(e: Exception, retryCount: Int, retryLimit: Int, retryWaitMillis: Int, totalWaitMillis: Long)
case class ParamInGiveup(firstException: Exception, lastException: Exception)

class RetryExecutorWrapper(exe: RetryExecutor, param: ParamInWrapper) {

  private def incrementTotalWaitMillis(retryWait: Int): Int = {
    val totalWaitMillis: Int = param.totalWaitMillisCounter.next()
    if (totalWaitMillis > param.timeoutDurationMillis) {
      throw new RetryGiveupException(new IllegalStateException(s"Total Wait: ${totalWaitMillis}ms is exceeded Timeout: ${param.timeoutDurationMillis}ms"))
    }
    (1 until retryWait).foreach(_ => param.totalWaitMillisCounter.next())
    totalWaitMillis
  }

  val exeMod: RetryExecutor = {
    val r = new RetryAction {
      override def onRetry(exception: Exception, retryCount: Int, retryLimit: Int, retryWait: Int): Unit = {
        incrementTotalWaitMillis(retryWait)
      }
    }
    exe.onRetry(r)
  }

  def withRetryLimit(count: Int): RetryExecutorWrapper = {
    RetryExecutorWrapper(exeMod.withRetryLimit(count), param)
  }

  def withInitialRetryWait(duration: Duration): RetryExecutorWrapper = {
    RetryExecutorWrapper(exeMod.withInitialRetryWait(duration.toMillis.toInt), param)
  }

  def withMaxRetryWait(duration: Duration): RetryExecutorWrapper = {
    RetryExecutorWrapper(exeMod.withMaxRetryWait(duration.toMillis.toInt), param)
  }

  def withWaitGrowRate(rate: Double): RetryExecutorWrapper = {
    RetryExecutorWrapper(exeMod.withWaitGrowRate(rate), param)
  }

  def withTimeout(duration: Duration): RetryExecutorWrapper = {
    val newParam: ParamInWrapper = ParamInWrapper(duration.toMillis.toInt, param.totalWaitMillisCounter)
    RetryExecutorWrapper(exeMod, newParam)
  }

  def retryIf(retryable: Exception => Boolean): RetryExecutorWrapper = {
    val r = new RetryPredicate {
      override def test(t: Exception): Boolean = retryable(t)
    }
    RetryExecutorWrapper(exeMod.retryIf(r), param)
  }

  def onRetry(f: ParamInRetry => Unit): RetryExecutorWrapper = {
    val r = new RetryAction {
      override def onRetry(exception: Exception, retryCount: Int, retryLimit: Int, retryWait: Int): Unit = {
        val totalWaitMillis: Int = incrementTotalWaitMillis(retryWait)
        f(ParamInRetry(exception, retryCount, retryLimit, retryWait, totalWaitMillis))
      }
    }
    RetryExecutorWrapper(exeMod.onRetry(r), param)
  }

  def onGiveup(f: ParamInGiveup => Unit): RetryExecutorWrapper = {
    val g = new GiveupAction {
      override def onGiveup(firstException: Exception, lastException: Exception): Unit = {
        f(ParamInGiveup(firstException, lastException))
      }
    }
    RetryExecutorWrapper(exeMod.onGiveup(g), param)
  }

  def runInterruptible[T](f: => T): T = {
    val c = new Callable[T] {
      override def call(): T = f
    }
    exeMod.runInterruptible(c)
  }

  def run[T](f: => T): T = {
    val c = new Callable[T] {
      override def call(): T = f
    }
    exeMod.run(c)
  }
}

object RetryExecutorWrapper {

  def apply(exe: RetryExecutor, param: ParamInWrapper): RetryExecutorWrapper = new RetryExecutorWrapper(exe, param)

  def apply(): RetryExecutorWrapper = RetryExecutorWrapper(RetryExecutor.retryExecutor(), ParamInWrapper(Int.MaxValue))

}
