package pro.civitaspo.digdag.plugin.livy.wrapper

import java.util.function.Predicate

import io.digdag.standards.operator.DurationInterval
import io.digdag.standards.operator.state.{Action, Operation, PollingRetryExecutor, TaskState}

class PollingRetryExecutorWrapper(exe: PollingRetryExecutor) {

  def withErrorMessage(errMsgFmt: String, errMsgParams: Any*): this.type = {
    new PollingRetryExecutorWrapper(exe.withErrorMessage(errMsgFmt, errMsgParams: _*))
  }

  def retryUnless(exceptionHandler: Exception => Boolean): this.type = {
    new PollingRetryExecutorWrapper(exe.retryUnless(new Predicate[Exception] {
      override def test(t: Exception): Boolean = exceptionHandler(t)
    }))
  }

  def retryUnless[T <: Exception](exceptionClass: Class[T], exceptionHandler: T => Boolean): this.type  = {
    new PollingRetryExecutorWrapper(exe.retryUnless(exceptionClass, new Predicate[T] {
      override def test(t: T): Boolean = exceptionHandler(t)
    }))
  }

  def retryIf(exceptionHandler: Exception => Boolean): this.type = {
    new PollingRetryExecutorWrapper(exe.retryIf(new Predicate[Exception] {
      override def test(t: Exception): Boolean = exceptionHandler(t)
    }))
  }

  def retryIf[T <: Exception](exceptionClass: Class[T], exceptionHandler: T => Boolean): this.type  = {
    new PollingRetryExecutorWrapper(exe.retryIf(exceptionClass, new Predicate[T] {
      override def test(t: T): Boolean = exceptionHandler(t)
    }))
  }

  def withRetryInterval(durationInterval: DurationInterval): this.type = {
    new PollingRetryExecutorWrapper(exe.withRetryInterval(durationInterval))
  }

  def runOnce(f: TaskState => Unit): Unit = {
    exe.runOnce(new Action {
      override def perform(state: TaskState): Unit = f(state)
    })
  }

  def runOnce[T](`type`: Class[T], f: TaskState => T): T = {
    exe.runOnce(`type`, new Operation[T] {
      override def perform(state: TaskState): T = f(state)
    })
  }

  def runAction(f: TaskState => Unit): Unit = {
    exe.runAction(new Action {
      override def perform(state: TaskState): Unit = f(state)
    })
  }

  def run[T](f: TaskState => T): T = {
    exe.run(new Operation[T] {
      override def perform(state: TaskState): T = f(state)
    })
  }
}

object PollingRetryExecutorWrapper {
  def apply(state: TaskState, stateKey: String): PollingRetryExecutorWrapper = {
    new PollingRetryExecutorWrapper(
      PollingRetryExecutor
        .pollingRetryExecutor(state, stateKey)
    )
  }

  def apply(exe: PollingRetryExecutor): PollingRetryExecutorWrapper = {
    new PollingRetryExecutorWrapper(exe)
  }
}
