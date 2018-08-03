package pro.civitaspo.digdag.plugin.livy.wrapper

import java.util.concurrent.Callable

import io.digdag.util.RetryExecutor
import io.digdag.util.RetryExecutor.{GiveupAction, RetryAction, RetryPredicate}

class RetryExecutorWrapper(
  exe: RetryExecutor,
  totalWaitMillisCounter: Iterator[Int] = Stream.from(1).iterator
) {

  case class ParamInRetry(e: Exception, retryCount: Int, retryLimit: Int, retryWaitMillis: Int, totalWaitMillis: Long)
  case class ParamInGiveup(firstException: Exception, lastException: Exception)

  def withRetryLimit(count: Int): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withRetryLimit(count), totalWaitMillisCounter)
  }

  def withInitialRetryWait(msec: Int): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withInitialRetryWait(msec), totalWaitMillisCounter)
  }

  def withMaxRetryWait(msec: Int): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withMaxRetryWait(msec), totalWaitMillisCounter)
  }

  def withWaitGrowRate(rate: Double): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withWaitGrowRate(rate), totalWaitMillisCounter)
  }

  def retryIf(retryable: Exception => Boolean): RetryExecutorWrapper = {
    RetryExecutorWrapper(
      exe.retryIf(new RetryPredicate {
        override def test(t: Exception): Boolean = retryable(t)
      }),
      totalWaitMillisCounter
    )
  }

  def onRetry(f: ParamInRetry => Unit): RetryExecutorWrapper = {
    RetryExecutorWrapper(
      exe.onRetry(new RetryAction {
        override def onRetry(
          exception: Exception,
          retryCount: Int,
          retryLimit: Int,
          retryWait: Int
        ): Unit = {
          var totalWaitMillis: Int = _
          (1 to retryWait).foreach(_ => totalWaitMillis = totalWaitMillisCounter.next())
          f(ParamInRetry(exception, retryCount, retryLimit, retryWait, totalWaitMillis))
        }
      }),
      totalWaitMillisCounter
    )
  }

  def onGiveup(f: ParamInGiveup => Unit): RetryExecutorWrapper = {
    RetryExecutorWrapper(
      exe.onGiveup(new GiveupAction {
        override def onGiveup(firstException: Exception, lastException: Exception): Unit = {
          f(ParamInGiveup(firstException, lastException))
        }
      }),
      totalWaitMillisCounter
    )
  }

  def runInterruptible[T](f: () => T): T = {
    exe.runInterruptible(new Callable[T] {
      override def call(): T = f()
    })
  }

  def run[T](f: () => T): T = {
    exe.run(new Callable[T] {
      override def call(): T = f()
    })
  }
}

object RetryExecutorWrapper {

  def apply(
    exe: RetryExecutor,
    totalWaitMillisCounter: Iterator[Int] = Stream.from(1).iterator
  ): RetryExecutorWrapper = new RetryExecutorWrapper(exe, totalWaitMillisCounter)

  def apply(): RetryExecutorWrapper = new RetryExecutorWrapper(RetryExecutor.retryExecutor())

}
