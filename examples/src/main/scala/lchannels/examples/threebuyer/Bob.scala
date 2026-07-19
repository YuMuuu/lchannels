// lchannels - session programming in Scala
// Copyright (c) 2017, Alceste Scalas and Imperial College London
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
package lchannels.examples.threebuyer.bob

import lchannels._
import lchannels.examples.threebuyer.protocol.binary
import lchannels.examples.threebuyer.protocol.bob._

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

class Bob(
    s: In[binary.PlayBob],
    carolConnector: (String => Unit) => Out[binary.Contrib]
)(implicit timeout: Duration)
    extends Runnable
    with StrictLogging {
  private def logTrace(msg: String) = logger.trace(msg)
  private def logDebug(msg: String) = logger.debug(msg)
  private def logInfo(msg: String) = logger.info(msg)
  private def logWarn(msg: String) = logger.warn(msg)
  private def logError(msg: String) = logger.error(msg)

  // Own thread
  private val thread = { val t = new Thread(this); t.start(); t }
  def join() = thread.join()

  override def run() = {
    val c = MPPlayBob(s) // Wrap the channel in a multiparty session obj

    logInfo("Started.  Waiting for multiparty session...")
    val c2 = c.receive.p

    logInfo("Waiting for book quote...")
    val quote = c2.receive
    logInfo(
      s"Got quote: ${quote.p.toString}.  Waiting to know Alice's share..."
    )
    val contrib = quote.cont.receive
    logInfo(s"Got Alice's share: ${contrib.p.toString}")

    val myShare = quote.p - contrib.p
    assert(myShare >= 0)

    val budget = 25

    logInfo(
      s"My share is: ${myShare.toString}; my maximum budget is: ${budget.toString}"
    )
    if (myShare > budget) {
      val needed = myShare - budget
      logInfo(s"I need ${needed.toString} more.  Involving Carol...")
      delegateCarol(contrib.cont, needed)
    } else {
      logInfo("Accepting proposal, sending address, waiting delivery date")
      val delivery = contrib.cont
        .send(OkA(()))
        .send(OkS(()))
        .send(Address("Bob Smith, 221B Baker Street, London, UK"))
        .receive
      logInfo(s"Got delivery date: ${delivery.p.toString}")
    }

    logInfo("Terminating.")
  }

  private def delegateCarol(s: MPOkAOrQuitA, needed: Int) = {
    // Wrap Carol's channel in a session object
    val carol = MPContrib(carolConnector(logInfo))

    logInfo(
      s"Telling Carol to contribute ${needed.toString}; delegating session with Alice and Seller"
    )
    val ccont = carol.send(Contrib(needed)).send(Delegate(s))
    logInfo(s"Waiting for Carol's decision...")
    ccont.receive match {
      case OkC(())   => logInfo("Carol accepted")
      case QuitC(()) => logInfo("Carol declined")
    }
  }
}

object Actor extends App {
  // Helper method to ease external invocation
  def run() = main(Array())

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.typesafe.config.ConfigFactory
  import org.apache.pekko.actor.typed.ActorSystem
  import org.apache.pekko.actor.typed.SpawnProtocol

  import binary.actor.{ConnectBob, ConnectCarol}

  val config = ConfigFactory.load() // Loads resources/application.conf
  implicit val as: ActorSystem[SpawnProtocol.Command] =
    ActorSystem[SpawnProtocol.Command](
      SpawnProtocol(),
      "ThreeBuyerBobSys",
      config.getConfig("ThreeBuyerBobSys")
    )

  implicit val runtime: ActorChannelRuntime = ActorChannelRuntime(as, global)

  implicit val timeout: FiniteDuration = 60.seconds

  val sellerPath =
    "pekko://ThreeBuyerSellerSys@127.0.0.1:31350/user/lchannels/bob"
  println(s"[*] Connecting to ${sellerPath.toString}...")
  val c: Out[ConnectBob] = ActorOut[ConnectBob](sellerPath)
  val c2 = c !! ConnectBob() _

  def connector(logger: String => Unit) = {
    // Path where Carol is waiting for Bob's connection
    val carolPath =
      "pekko://ThreeBuyerCarolSys@127.0.0.1:31353/user/lchannels/bob"
    logger(s"Connecting to ${carolPath.toString}...")
    val c: Out[ConnectCarol] = ActorOut[ConnectCarol](carolPath)
    c !! ConnectCarol() _
  }

  val bob = new Bob(c2, connector)(30.seconds)

  bob.join()
  Thread.sleep(2000) // Just to deliver pending actor messages
  as.terminate()
}
