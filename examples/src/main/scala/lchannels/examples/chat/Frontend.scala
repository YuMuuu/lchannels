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
package lchannels.examples.chat.frontend

import lchannels._

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Try, Success, Failure}

import com.typesafe.scalalogging.StrictLogging

import lchannels.examples.chat.protocol.public._
import lchannels.examples.chat.protocol.internal.session.{
  GetSession => IntGetSession,
  Success => IntSuccess,
  Failure => IntFailure
}
import lchannels.examples.chat.protocol.internal.auth.{
  GetAuthentication => IntGetAuth
}

/** Chat server frontend */
class Frontend(
    sessionSrv: Out[IntGetSession],
    authSrv: Out[IntGetAuth],
    factory: () => (In[GetSession], Out[GetSession])
)(implicit ec: ExecutionContext, timeout: Duration)
    extends Runnable
    with StrictLogging {
  import scala.concurrent.Channel

  private def logTrace(msg: String) = logger.trace(s"${msg.toString}")
  private def logDebug(msg: String) = logger.debug(s"${msg.toString}")
  private def logInfo(msg: String) = logger.info(s"${msg.toString}")
  private def logWarn(msg: String) = logger.warn(s"${msg.toString}")
  private def logError(msg: String) = logger.error(s"${msg.toString}")

  // FIFO queue with requests from clients
  private val requests: Channel[GetSession] = new Channel()

  // Own thread
  private val thread = { val t = new Thread(this); t.start(); t }
  def quit() = thread.interrupt()

  /** Return a channel for interacting with the frontend. */
  def connect(): Out[GetSession] = {
    logDebug("new client connection")
    val (in, out) = factory()
    in.future.onComplete(queueRequest) // Async. add request to FIFO
    out
  }

  private def queueRequest(req: Try[GetSession]): Unit = req match {
    case Success(r) => {
      logDebug(s"queueing ${r.toString}")
      requests.write(r)
    }
    case Failure(e) => logDebug("got failure, not enqueuing")
  }

  override def run(): Unit = {
    logDebug("started")
    serverLoop(sessionSrv, authSrv)
    logDebug("quitting")
  }

  @tailrec
  private def serverLoop(
      sessionSrv: Out[IntGetSession],
      authSrv: Out[IntGetAuth]
  )(implicit timeout: Duration): Unit = {
    val req = try {
      Some(requests.read)
    } catch {
      case _: InterruptedException => {
        logDebug("interrupted, leaving main loop")
        None
      }
    }

    req match {
      case Some(req) => {
        logDebug(s"dequeuing ${req.toString}, now serving")
        val (sessionSrv2, authSrv2) = serve(req, sessionSrv, authSrv)
        serverLoop(sessionSrv2, authSrv2)
      }
      case None => ()
    }
  }

  private def serve(
      req: GetSession,
      sessionSrv: Out[IntGetSession],
      authSrv: Out[IntGetAuth]
  )(implicit timeout: Duration): (Out[IntGetSession], Out[IntGetAuth]) = {
    logDebug(s"trying to retrieve active session")
    (sessionSrv !! IntGetSession(req.id) _) ? {
      case ssrv @ IntSuccess(sessc) => {
        logDebug(s"got active session channel, forwarding to client")
        req.cont ! Active(sessc)
        (ssrv.cont, authSrv)
      }
      case ssrv @ IntFailure() => {
        logDebug(s"no active session, getting authentication channel")
        (authSrv !! IntGetAuth() _) ? { auth =>
          logDebug(s"forwarding authentication channel to client")
          req.cont ! New(auth.channel)
          (ssrv.cont, auth.cont)
        }
      }
    }
  }
}
