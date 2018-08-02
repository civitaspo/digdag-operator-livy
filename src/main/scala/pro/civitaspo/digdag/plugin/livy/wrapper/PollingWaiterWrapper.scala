package pro.civitaspo.digdag.plugin.livy.wrapper

import com.google.common.base.Optional
import io.digdag.standards.operator.DurationInterval
import io.digdag.standards.operator.state.{Operation, PollingWaiter, TaskState}

class PollingWaiterWrapper(waiter: PollingWaiter) {

  def withWaitMessage(waitMsgFmt: String, waitMsgParams: Any*): this.type = {
    new PollingWaiterWrapper(
      waiter.withWaitMessage(
        waitMsgFmt,
        waitMsgParams: _*
      )
    )
  }

  def withPollInterval(durationInterval: DurationInterval): this.type = {
    new PollingWaiterWrapper(
      waiter.withPollInterval(durationInterval)
    )
  }

  def awaitOnce[T](`type`: Class[T], f: TaskState => T): T = {
    waiter.awaitOnce(
      `type`,
      new Operation[Optional[T]] {
        override def perform(state: TaskState): Optional[T] = {
          Optional.fromNullable(
            f(state)
          )
        }
      }
    )
  }

  def await[T](f: TaskState => T): T = {
    waiter.await(
      new Operation[Optional[T]] {
        override def perform(state: TaskState): Optional[T] = {
          Optional.fromNullable(
            f(state)
          )
        }
      }
    )
  }

}

object PollingWaiterWrapper {
  def apply(waiter: PollingWaiter): PollingWaiterWrapper = new PollingWaiterWrapper(waiter)

  def apply(state: TaskState, stateKey: String): PollingWaiterWrapper = {
    new PollingWaiterWrapper(PollingWaiter.pollingWaiter(state, stateKey))
  }
}
