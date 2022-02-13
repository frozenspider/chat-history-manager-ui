package org.fs.chm.utility

import scala.concurrent.Future

import scala.concurrent._
import java.util.concurrent.atomic.AtomicReference

/**
 * Code is taken from https://viktorklang.com/blog/Futures-in-Scala-protips-6.html
 */
object InterruptableFuture {

  final class Interrupt extends (() => Boolean) {
    // We need a state-machine to track the progress.
    // It can have the following states:
    // a null reference means execution has not started.
    // a Thread reference means that the execution has started but is not done.
    // a this reference means that it is already cancelled or is already too late.
    private[this] final var state: AnyRef = null

    /**
     * This is the signal to cancel the execution of the logic.
     * Returns whether the cancellation signal was successully issued or not.
     **/
    override final def apply(): Boolean = this.synchronized {
      state match {
        case null        =>
          state = this
          true
        case _: this.type => false
        case t: Thread   =>
          state = this
          t.interrupt()
          true
      }
    }

    // Initializes right before execution of logic and
    // allows to not run the logic at all if already cancelled.
    private[this] final def enter(): Boolean =
      this.synchronized {
        state match {
          case _: this.type => false
          case null =>
            state = Thread.currentThread
            true
        }
      }

    // Cleans up after the logic has executed
    // Prevents cancellation to occur "too late"
    private[this] final def exit(): Boolean =
      this.synchronized {
        state match {
          case _: this.type => false
          case t: Thread =>
            state = this
            true
        }
      }

    /**
     * Executes the suplied block of logic and returns the result.
     * Throws CancellationException if the block was interrupted.
     **/
    def interruptibly[T](block: =>T): T =
      if (enter()) {
        try block catch {
          case _: InterruptedException => throw new CancellationException()
        } finally {
          if(!exit() && Thread.interrupted())
            () // If we were interrupted and flag was not cleared
        }
      } else throw new CancellationException()
  }

  case class InterruptableFuture[+T](future: Future[T], cancel: Interrupt)

  implicit class FutureInterrupt(val future: Future.type) extends AnyVal {
    def interruptibly[T](block: => T)(implicit ec: ExecutionContext): InterruptableFuture[T] = {
      val interrupt = new Interrupt()
      InterruptableFuture(Future(interrupt.interruptibly(block))(ec), interrupt)
    }
  }
}
