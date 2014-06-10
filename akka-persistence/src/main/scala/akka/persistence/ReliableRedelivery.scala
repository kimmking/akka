/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence

import scala.annotation.tailrec
import scala.collection.breakOut
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import akka.actor.Actor
import akka.actor.ActorPath

object ReliableRedelivery {

  @SerialVersionUID(1L)
  case class ReliableRedeliverySnapshot(currentSeqNr: Long, unconfirmedDeliveries: immutable.Seq[UnconfirmedDelivery]) {

    // FIXME Java api
    // FIXME protobuf serialization

  }

  @SerialVersionUID(1L)
  case class UnconfirmedWarning(unconfirmedDeliveries: immutable.Seq[UnconfirmedDelivery])

  case class UnconfirmedDelivery(seqNr: Long, destination: ActorPath, msg: Any)

  class MaxUnconfirmedMessagesExceededException(message: String) extends RuntimeException(message)

  object Internal {
    case class Delivery(destination: ActorPath, msg: Any, timestamp: Long, attempt: Int)
    case object RedeliveryTick
  }

}

trait ReliableRedelivery extends PersistentActor {
  import ReliableRedelivery._
  import ReliableRedelivery.Internal._

  /**
   * Interval between redelivery attempts.
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.redeliver-interval`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def redeliverInterval: FiniteDuration = defaultRedeliverInterval

  private val defaultRedeliverInterval: FiniteDuration =
    Persistence(context.system).settings.atLeastOnceDelivery.redeliverInterval

  /**
   * After this number of delivery attempts a [[ReliableRedelivery.UnconfirmedWarning]] message
   * will be sent to `self`.
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.warn-after-number-Of-unconfirmed-attempts`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def warnAfterNumberOfUnconfirmedAttempts: Int = defaultWarnAfterNumberOfUnconfirmedAttempts

  private val defaultWarnAfterNumberOfUnconfirmedAttempts: Int =
    Persistence(context.system).settings.atLeastOnceDelivery.warnAfterNumberOfUnconfirmedAttempts

  /**
   * Maximum number of unconfirmed messages that this actor is allowed to hold in memory.
   * If this number is exceed [[#deliver]] will not accept more messages, i.e. it will throw a
   * [[ReliableRedelivery.MaxUnconfirmedMessagesExceededException]].
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.max-unconfirmed-messages
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def maxUnconfirmedMessages: Int = defaultMaxUnconfirmedMessages

  private val defaultMaxUnconfirmedMessages: Int =
    Persistence(context.system).settings.atLeastOnceDelivery.maxUnconfirmedMessages

  private val redeliverTask = {
    import context.dispatcher
    val interval = redeliverInterval / 2
    context.system.scheduler.schedule(interval, interval, self, RedeliveryTick)
  }

  private var deliverySequenceNr = 0L
  private var unconfirmed = immutable.SortedMap.empty[Long, Delivery]

  private def nextDeliverySequenceNr(): Long = {
    // FIXME one counter sequence per destination?
    deliverySequenceNr += 1
    deliverySequenceNr
  }

  def deliver(destination: ActorPath, seqNrToMessage: Long => Any): Unit = {
    if (unconfirmed.size >= maxUnconfirmedMessages)
      throw new MaxUnconfirmedMessagesExceededException(
        s"Too many unconfirmed messages, maximum allowed is [$maxUnconfirmedMessages]")

    val seqNr = nextDeliverySequenceNr()
    val now = System.nanoTime()
    val d = Delivery(destination, seqNrToMessage(seqNr), now, attempt = 0)
    if (recoveryRunning)
      unconfirmed = unconfirmed.updated(seqNr, d)
    else
      send(seqNr, d, now)
  }

  // FIXME Java API for deliver

  def confirmDelivery(seqNr: Long): Boolean = {
    if (unconfirmed.contains(seqNr)) {
      unconfirmed -= seqNr
      true
    } else false
  }

  /**
   * Number of messages that have not been confirmed yet.
   */
  def numberOfUnconfirmed: Int = unconfirmed.size

  private def redeliverOverdue(): Unit = {
    val now = System.nanoTime()
    val deadline = now - redeliverInterval.toNanos
    var warnings = Vector.empty[UnconfirmedDelivery]
    unconfirmed foreach {
      case (seqNr, delivery) =>
        if (delivery.timestamp <= deadline) {
          send(seqNr, delivery, now)
          if (delivery.attempt == warnAfterNumberOfUnconfirmedAttempts)
            warnings :+= UnconfirmedDelivery(seqNr, delivery.destination, delivery.msg)
        }
    }
    if (warnings.nonEmpty)
      self ! UnconfirmedWarning(warnings)
  }

  private def send(seqNr: Long, d: Delivery, timestamp: Long): Unit = {
    context.actorSelection(d.destination) ! d.msg
    unconfirmed = unconfirmed.updated(seqNr, d.copy(timestamp = timestamp, attempt = d.attempt + 1))
  }

  def getDeliverySnapshot: ReliableRedeliverySnapshot =
    ReliableRedeliverySnapshot(deliverySequenceNr,
      unconfirmed.map { case (seqNr, d) => UnconfirmedDelivery(seqNr, d.destination, d.msg) }(breakOut))

  def setDeliverySnapshot(snapshot: ReliableRedeliverySnapshot): Unit = {
    deliverySequenceNr = snapshot.currentSeqNr
    val now = System.nanoTime()
    unconfirmed = snapshot.unconfirmedDeliveries.map(d =>
      d.seqNr -> Delivery(d.destination, d.msg, now, 0))(breakOut)
  }

  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    redeliverTask.cancel()
    super.aroundPreRestart(reason, message)
  }

  override protected[akka] def aroundPostStop(): Unit = {
    redeliverTask.cancel()
    super.aroundPostStop()
  }

  override protected[akka] def aroundReceive(receive: Receive, message: Any): Unit =
    message match {
      case RedeliveryTick ⇒ redeliverOverdue()
      case _              ⇒ super.aroundReceive(receive, message)
    }
}
