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
package lchannels.examples.game.c

import lchannels._
import lchannels.examples.game.protocol.binary
import lchannels.examples.game.protocol.c._

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

class Client(name: String, s: In[binary.PlayC], wait: Duration)(implicit
    timeout: Duration
) extends Runnable
    with StrictLogging {
  private def logTrace(msg: String) =
    logger.trace(s"${name.toString}: ${msg.toString}")
  private def logDebug(msg: String) =
    logger.debug(s"${name.toString}: ${msg.toString}")
  private def logInfo(msg: String) =
    logger.info(s"${name.toString}: ${msg.toString}")
  private def logWarn(msg: String) =
    logger.warn(s"${name.toString}: ${msg.toString}")
  private def logError(msg: String) =
    logger.error(s"${name.toString}: ${msg.toString}")

  // Own thread
  private val thread = { val t = new Thread(this); t.start(); t }
  def join() = thread.join()

  override def run() = {
    val c = MPPlayC(s) // Wrap the channel in a multiparty session obj

    logInfo("Started.  Waiting for multiparty session...")
    val game = c.receive.p
    logInfo("...done.  Waiting for B's info...")
    val info = game.receive
    logInfo(
      s"...got InfoBC(${info.p.toString}): sending to A, and starting game loop."
    )
    val info2 = info.cont.send(InfoCA(s"${info.p.toString}, ${name.toString}"))
    loop(info2)
  }

  @scala.annotation.tailrec
  private def loop(g: MPMov1BCOrMov2BC): Unit = {
    logInfo(s"Delay: ${wait.toString}")
    Thread.sleep(wait.toMillis)
    logInfo("Waiting for B's move...")
    g.receive match {
      case Mov1BC(p, cont) => {
        logInfo(
          s"Got Mov1BC(${p.toString}), sending Mov1CA(${p.toString}) and looping"
        )
        val g2 = cont.send(Mov1CA(p))
        loop(g2)
      }
      case Mov2BC(p, cont) => {
        logInfo(
          s"Got Mov2BC(${p.toString}), sending Mov2CA(${p.toString}) and looping"
        )
        val g2 = cont.send(Mov2CA(p))
        loop(g2)
      }
    }
  }
}

object Actor extends App {
  // Helper method to ease external invocation
  def run() = main(Array())

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.typesafe.config.ConfigFactory
  import org.apache.pekko.actor.ActorSystem

  import binary.actor.{ConnectC => Connect}

  val config = ConfigFactory.load() // Loads resources/application.conf
  implicit val as: ActorSystem = ActorSystem(
    "GameClientCSys",
    config = Some(config.getConfig("GameClientCSys")),
    defaultExecutionContext = Some(global)
  )

  ActorChannel.setDefaultEC(global)
  ActorChannel.setDefaultAS(as)

  implicit val timeout: FiniteDuration = 60.seconds

  val serverPath = "pekko://GameServerSys@127.0.0.1:31340/user/c"
  println(s"[*] Connecting to ${serverPath.toString}...")
  val c: Out[Connect] = ActorOut[Connect](serverPath)
  val c2 = c !! Connect() _

  val client = new Client("Carol", c2, 1.seconds)(30.seconds)

  client.join()
  // Cleanup and hut down the actor system
  ActorChannel.cleanup()
  as.terminate()
}
