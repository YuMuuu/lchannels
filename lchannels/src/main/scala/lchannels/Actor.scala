// lchannels - session programming in Scala
// Copyright (c) 2016, Alceste Scalas and Imperial College London
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
/** @author Alceste Scalas <alceste.scalas@imperial.ac.uk> */
package lchannels

import scala.concurrent.{Await, blocking, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorPath
import org.apache.pekko.actor.typed.{
  ActorRef,
  ActorRefResolver,
  ActorSystem,
  Behavior,
  Props,
  SpawnProtocol
}
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.pattern.ExplicitlyAskableActorRef
import org.apache.pekko.util.Timeout

/** The medium of actor-based channels. */
case class Actor()

/* Type used by dispatcher behaviors, to receive their destination actor and
 * the actual transmitted value (in any order).
 */
private sealed abstract class DispatcherMessage[T]
private case class Value[T](v: Try[T]) extends DispatcherMessage[T]
private case class Pull[T](replyTo: ActorRef[Try[T]])
    extends DispatcherMessage[T]
private case class StopDispatcher[T]() extends DispatcherMessage[T]

protected[lchannels] object ActorChannelGuardian {
  sealed trait Command {
    private[lchannels] def handle(
        ctx: ActorContext[Command]
    ): Behavior[Command]
  }

  final case class Create[T](
      name: Option[String],
      replyTo: ActorRef[(ActorIn[T], ActorOut[T])]
  ) extends Command {
    override private[lchannels] def handle(
        ctx: ActorContext[Command]
    ): Behavior[Command] = {
      val dref = name match {
        case Some(actorName) =>
          ctx.spawn(dispatcher[T](None, None), actorName)
        case None =>
          ctx.spawnAnonymous(dispatcher[T](None, None))
      }
      replyTo ! (ActorIn[T](dref), ActorOut[T](dref, ctx.self))
      Behaviors.same
    }
  }

  final case class Cleanup(replyTo: ActorRef[Unit]) extends Command {
    override private[lchannels] def handle(
        ctx: ActorContext[Command]
    ): Behavior[Command] = {
      val children = ctx.children.toSet
      if (children.isEmpty) {
        replyTo ! ()
        Behaviors.stopped
      } else {
        children.foreach { child =>
          ctx.watchWith(child, ChildStopped(child))
          ctx.stop(child)
        }
        stopping(children, replyTo)
      }
    }
  }

  private final case class ChildStopped(ref: ActorRef[Nothing])
      extends Command {
    override private[lchannels] def handle(
        ctx: ActorContext[Command]
    ): Behavior[Command] = {
      Behaviors.same
    }
  }

  def apply(): Behavior[Command] = {
    Behaviors.receive { (ctx, command) => command.handle(ctx) }
  }

  private def stopping(
      children: Set[ActorRef[Nothing]],
      replyTo: ActorRef[Unit]
  ): Behavior[Command] = {
    Behaviors.receiveMessage {
      case ChildStopped(ref) => {
        val remaining = children - ref
        if (remaining.isEmpty) {
          replyTo ! ()
          Behaviors.stopped
        } else {
          stopping(remaining, replyTo)
        }
      }
      case _ => Behaviors.same
    }
  }

  private def dispatcher[T](
      recvdValue: Option[Try[T]],
      recvdPull: Option[ActorRef[Try[T]]]
  ): Behavior[DispatcherMessage[T]] = {
    Behaviors.receiveMessage[DispatcherMessage[T]] {
      case Value(v) =>
        recvdPull match {
          case Some(ref) => {
            ref ! v
            Behaviors.stopped
          }
          case None => dispatcher(Some(v), recvdPull)
        }
      case Pull(replyTo) =>
        recvdValue match {
          case Some(v) => {
            replyTo ! v
            Behaviors.stopped
          }
          case None => dispatcher(recvdValue, Some(replyTo))
        }
      case StopDispatcher() => Behaviors.stopped
    }
  }
}

final class ActorChannelRuntime private (
    val actorSystem: ActorSystem[SpawnProtocol.Command],
    val executionContext: ExecutionContext,
    private val spawnTimeout: Timeout
) {
  private val guardianName = "lchannels"
  private var guardianRef: Option[ActorRef[ActorChannelGuardian.Command]] =
    None

  private def spawnGuardian(): ActorRef[ActorChannelGuardian.Command] = {
    Await.result(
      actorSystem.ask[ActorRef[ActorChannelGuardian.Command]] { replyTo =>
        SpawnProtocol.Spawn(
          ActorChannelGuardian(),
          guardianName,
          Props.empty,
          replyTo
        )
      }(spawnTimeout, actorSystem.scheduler),
      spawnTimeout.duration
    )
  }

  private def guardian: ActorRef[ActorChannelGuardian.Command] = {
    synchronized {
      guardianRef.getOrElse {
        val ref = spawnGuardian()
        guardianRef = Some(ref)
        ref
      }
    }
  }

  /** Create a pair of actor-based I/O channel endpoints. */
  def factory[T](): (ActorIn[T], ActorOut[T]) = create[T](None)

  /** Create a named pair of actor-based I/O channel endpoints. */
  def factory[T](name: String): (ActorIn[T], ActorOut[T]) = {
    assert(name != "")
    create[T](Some(name))
  }

  private def create[T](name: Option[String]): (ActorIn[T], ActorOut[T]) = {
    Await.result(
      guardian.ask[(ActorIn[T], ActorOut[T])] { replyTo =>
        ActorChannelGuardian.Create(name, replyTo)
      }(spawnTimeout, actorSystem.scheduler),
      spawnTimeout.duration
    )
  }

  /** Spawn two functions as threads communicating via actor-based endpoints. */
  def parallel[T, R1, R2](
      p1: ActorIn[T] => R1,
      p2: ActorOut[T] => R2
  ): (Future[R1], Future[R2]) = {
    val (in, out) = factory[T]()
    (
      Future { blocking { p1(in) } }(executionContext),
      Future { blocking { p2(out) } }(executionContext)
    )
  }

  /** Gracefully stop the guardian and all channel actors owned by this runtime. */
  def cleanup(): Unit = {
    val active = synchronized {
      val ref = guardianRef
      guardianRef = None
      ref
    }
    active.foreach { ref =>
      Await.result(
        ref.ask[Unit] { replyTo =>
          ActorChannelGuardian.Cleanup(replyTo)
        }(spawnTimeout, actorSystem.scheduler),
        spawnTimeout.duration
      )
    }
  }
}

object ActorChannelRuntime {
  def apply(
      as: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext
  ): ActorChannelRuntime = {
    apply(as, ec, FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS))
  }

  def apply(
      as: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext,
      timeout: FiniteDuration
  ): ActorChannelRuntime = {
    new ActorChannelRuntime(as, ec, Timeout(timeout))
  }

  private[lchannels] val internalAskTimeout: Timeout =
    Timeout(FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS))

  // Pekko ask requires a finite timeout even when lchannels exposes Duration.Inf.
  private[lchannels] val unboundedReceiveTimeout: Timeout =
    Timeout(FiniteDuration(7, java.util.concurrent.TimeUnit.DAYS))
}

/** Channels that implement message delivery by automatically spawning Pekko
  * Typed actors.
  */
object ActorChannel {

  /** Create an actor-channel runtime bound to an actor system. */
  def runtime(
      as: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext
  ): ActorChannelRuntime = {
    ActorChannelRuntime(as, ec)
  }

  /** Release the resources used by actor-based channels. */
  def cleanup()(implicit runtime: ActorChannelRuntime): Unit =
    runtime.cleanup()

  /** Create a pair of actor-based I/O channel endpoints.
    *
    * @param runtime
    *   Actor-channel runtime for internal actor and `Future` management
    */
  def factory[T]()(implicit runtime: ActorChannelRuntime)
      : (ActorIn[T], ActorOut[T]) = runtime.factory[T]()

  /** Create a pair of actor-based I/O channel endpoints, with a specific name.
    *
    * Unlike [[factory[T]()*]], this method allows to assign a meaningful name
    * to the actor giving access to returned I/O channel endpoints: this is
    * reflected in their Actor Paths.
    *
    * @see
    *   [[ActorIn.path]] and [[ActorOut.path]]
    *
    * @param name
    *   Name of the Pekko actor giving access to the returned actor endpoints.
    * @param runtime
    *   Actor-channel runtime for internal actor and `Future` management
    */
  def factory[T](name: String)(implicit runtime: ActorChannelRuntime)
      : (ActorIn[T], ActorOut[T]) = runtime.factory[T](name)

  /** Spawn two functions as threads communicating via a pair of actor-based
    * channel endpoints.
    *
    * This method invokes [[factory[T]()*]] to create a pair of channel
    * endpoints `(in,out)`, and then spawns `p1(in)` and `p2(out)`.
    *
    * @return
    *   A pair of `Future`s `(f1, f2)`, completed respectively when `p1(in)` and
    *   `p2(out)` terminate.
    *
    * @param p1
    *   Function using the input channel endpoint
    * @param p2
    *   Function using the output channel endpoint
    * @param runtime
    *   Actor-channel runtime where `p1` and `p2` will run
    */
  def parallel[T, R1, R2](p1: ActorIn[T] => R1, p2: ActorOut[T] => R2)(implicit
      runtime: ActorChannelRuntime
  ): (Future[R1], Future[R2]) = runtime.parallel(p1, p2)
}

/** Actor-based input channel endpoint, usually created through the
  * [[[ActorIn$.apply* companion object]]] or via [[ActorChannel.factory]].
  */
@SerialVersionUID(1L)
protected[lchannels] class ActorIn[+T](
    private[this] val dref: ActorRef[DispatcherMessage[T]]
) extends medium.In[Actor, T]
    with Serializable {
  // We have to reimplement usage flags in a serializable way
  private var used = false
  protected final def _markAsUsed(): Unit = synchronized {
    if (used) {
      throw new lchannels.AlreadyUsed()
    }
    used = true
  }

  /** Return the path of the Pekko actor giving access to the channel endpoint
    *
    * The path allows to (remotely) proxy the channel endpoint, via
    * [[ActorIn $.apply]].
    */
  def path: ActorPath = dref.path

  override def receive(implicit atMost: Duration): T = {
    _markAsUsed()

    val timeout = ActorIn.receiveTimeout(atMost)
    val received = Await
      .result(
        new ExplicitlyAskableActorRef(dref.toClassic)
          .ask { replyTo =>
            Pull[T](replyTo.toTyped[Try[T]])
          }(timeout),
        ActorIn.awaitDuration(atMost)
      )
      .asInstanceOf[Try[T]]

    received match {
      case Success(value) => value
      case Failure(error) => throw error
    }
  }

  override def toString() = {
    s"ActorIn@${hashCode.toString} (dispatcher: ${dref.path.toString})"
  }
}

/** Actor-based input channel endpoint. */
object ActorIn {
  private[lchannels] def apply[T](
      dref: ActorRef[DispatcherMessage[T]]
  ): ActorIn[T] = {
    new ActorIn(dref)
  }

  /** Proxy an [[ActorIn]] instance reachable through the given Pekko actor path
    *
    * @param path
    *   Actor path, matching the value of some [[ActorIn.path]]
    * @param runtime
    *   Actor-channel runtime for path resolution and internal actor handling
    * @param timeout
    *   Max wait time for path resolution
    */
  def apply[T](path: ActorPath)(implicit
      runtime: ActorChannelRuntime,
      timeout: FiniteDuration
  ): ActorIn[T] = {
    apply(ActorIn.resolvePath[T](path, timeout))
  }

  /** Proxy an [[ActorIn]] instance reachable through the given Pekko actor path
    * (given as a string).
    *
    * @param path
    *   Actor path, matching the value of some [[ActorIn.path]]
    * @param runtime
    *   Actor-channel runtime for path resolution and internal actor handling
    * @param timeout
    *   Max wait time for path resolution
    */
  def apply[T](path: String)(implicit
      runtime: ActorChannelRuntime,
      timeout: FiniteDuration
  ): ActorIn[T] = {
    apply(org.apache.pekko.actor.ActorPaths.fromString(path))
  }

  private[lchannels] def resolvePath[T](
      path: ActorPath,
      timeout: FiniteDuration
  )(implicit
      runtime: ActorChannelRuntime
  ): ActorRef[DispatcherMessage[T]] = {
    ActorRefResolver(runtime.actorSystem)
      .resolveActorRef[DispatcherMessage[T]](path.toString)
  }

  private[lchannels] def resolveGuardian(
      path: ActorPath
  )(implicit runtime: ActorChannelRuntime): ActorRef[ActorChannelGuardian.Command] = {
    ActorRefResolver(runtime.actorSystem)
      .resolveActorRef[ActorChannelGuardian.Command](path.parent.toString)
  }

  private[lchannels] def receiveTimeout(atMost: Duration): Timeout = {
    atMost match {
      case fd: FiniteDuration => Timeout(fd)
      case Duration.Inf       => ActorChannelRuntime.unboundedReceiveTimeout
      case _ =>
        throw new IllegalArgumentException("Duration.Undefined is not allowed")
    }
  }

  private[lchannels] def awaitDuration(atMost: Duration): Duration = {
    atMost match {
      case Duration.Inf => Duration.Inf
      case fd: FiniteDuration =>
        fd
      case _ =>
        throw new IllegalArgumentException("Duration.Undefined is not allowed")
    }
  }
}

/** Actor-based output channel endpoint, usually created through the
  * [[[ActorOut$.apply* companion object]]] or via [[ActorChannel.factory]].
  */
@SerialVersionUID(1L)
protected[lchannels] class ActorOut[-T](
    private[this] val dref: ActorRef[DispatcherMessage[T]],
    private[this] val guardian: ActorRef[ActorChannelGuardian.Command]
) extends medium.Out[Actor, T]
    with Serializable {
  // We have to reimplement usage flags in a serializable way
  private var used = false
  protected final def _markAsUsed(): Unit = synchronized {
    if (used) {
      throw new lchannels.AlreadyUsed()
    }
    used = true
  }

  /** Return the path of the Pekko actor giving access to the channel endpoint
    *
    * The path allows to (remotely) proxy the channel endpoint, via
    * [[ActorOut$.apply]].
    */
  def path: ActorPath = dref.path

  override def send(v: T): Unit = synchronized {
    _markAsUsed()
    dref ! Value(Success(v))
  }

  override def create[U](): (ActorIn[U], ActorOut[U]) = {
    Await
      .result(
        new ExplicitlyAskableActorRef(guardian.toClassic)
          .ask { replyTo =>
            ActorChannelGuardian.Create[U](
              None,
              replyTo.toTyped[(ActorIn[U], ActorOut[U])]
            )
          }(ActorChannelRuntime.internalAskTimeout),
        ActorChannelRuntime.internalAskTimeout.duration
      )
      .asInstanceOf[(ActorIn[U], ActorOut[U])]
  }

  override def toString(): String = {
    s"ActorOut@${hashCode.toString} → ${dref.path.toString}"
  }
}

/** Actor-based output channel endpoint. */
object ActorOut {
  private[lchannels] def apply[T](
      dref: ActorRef[DispatcherMessage[T]],
      guardian: ActorRef[ActorChannelGuardian.Command]
  ): ActorOut[T] = {
    new ActorOut(dref, guardian)
  }

  /** Proxy an [[ActorOut]] instance reachable through the given Pekko actor
    * path
    *
    * @param path
    *   Actor path, matching the value of some [[ActorIn.path]]
    * @param runtime
    *   Actor-channel runtime for path resolution and internal actor handling
    * @param timeout
    *   Max wait time for path resolution
    */
  def apply[T](path: ActorPath)(implicit
      runtime: ActorChannelRuntime,
      timeout: FiniteDuration
  ): ActorOut[T] = {
    apply(
      ActorIn.resolvePath[T](path, timeout),
      ActorIn.resolveGuardian(path)
    )
  }

  /** Proxy an [[ActorOut]] instance reachable through the given Pekko actor
    * path (given as a string).
    *
    * @param path
    *   Actor path, matching the value of some [[ActorIn.path]]
    * @param runtime
    *   Actor-channel runtime for path resolution and internal actor handling
    * @param timeout
    *   Max wait time for path resolution
    */
  def apply[T](path: String)(implicit
      runtime: ActorChannelRuntime,
      timeout: FiniteDuration
  ): ActorOut[T] = {
    apply(org.apache.pekko.actor.ActorPaths.fromString(path))
  }
}
