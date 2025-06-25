import com.example.jsonstream.{MessageEnvelop, SurfaceMessage, SurfaceMessageAdapter}
import com.fasterxml.jackson.core.JsonFactory
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import org.slf4j.LoggerFactory
import rx.lang.scala.{Observable, Scheduler, schedulers}
import rx.lang.scala.schedulers.ExecutionContextScheduler

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.json.JSONObject

import scala.util.{Success, Failure, Try}


class ReactBootstrapApiConnector(host: String, port: Int, prefix: String, isSSL: Boolean, vertx: Vertx, surfaceMessageAdapter: SurfaceMessageAdapter) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jsonFactory = new JsonFactory()
  private val client: HttpClient = vertx.createHttpClient()
  private val REACT_BOOTSTRAP_API_RESPONSE_COMPLETED = "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"
  private val APPEND_JSON_START = "{\"surfaceResult\":{"
  private val APPEND_JSON_END = "}"
  private val startindex = 19
  private val splitOn = "},\\{\"surfaceResult\":\\{"
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"
  private val executorService = Executors.newSingleThreadExecutor
  private val adaptor = new SurfaceMessageAdapter()
  lazy val reactEndpointTabLeLoaderScheduler: Scheduler = {
    val threadFactory = new ThreadFactoryBuilder().setNameFormat("react").build()
    val executionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(1, threadFactory)
    )
    ExecutionContextScheduler(executionContext)

  }
  def streamRatesFromReact: Observable[String] = {
    Observable[String] {
      subscribe => {
        client.request().onComplete { reqAr =>
          if (reqAr.succeeded()) {
            val request = reqAr.result()
            request.send().onComplete { respAsr =>
              if (respAsr.succeeded()) {
                val response = respAsr.result()
                if (response.statusCode() == 200) {
                  response.handler { b =>
                    executorService.execute(() => {
                      subscribe.onNext(b.toString())
                    })
                  }.exceptionHandler {
                    e => subscribe.onError(e)
                  }.endHandler { _ =>
                    subscribe.onNext(REACT_BOOTSTRAP_API_RESPONSE_COMPLETED)
                    subscribe.onCompleted()
                  }
                } else subscribe.onError(new RuntimeException(response.statusMessage()))
              } else subscribe.onError(respAsr.cause())
            }
          } else subscribe.onError(reqAr.cause())
        }
      }
    }.onBackpressureBuffer.onErrorReturn(e => {
      logger.info("errror", e)
      REACT_REST_API_EXCEPTION
    })
  }

  def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
    streamRatesFromReact.scan((new StringBuilder(), Array[String]())) { (acc, next) =>
        val builder = acc._1
        var res: Array[String] = new Array[String](0)
        if (next == REACT_REST_API_EXCEPTION)
          res = Array(REACT_REST_API_EXCEPTION)
        else if (next == REACT_BOOTSTRAP_API_RESPONSE_COMPLETED) {
          res = builder.substring(startindex, builder.length - 2).split(splitOn)
        }
        else
          builder.append(next)
        (builder, res)
      }.map(_._2).filter(_.nonEmpty).flatMapIterable(b => b).observeOn(reactEndpointTabLeLoaderScheduler)
      .map(ss => {
        if (ss == REACT_REST_API_EXCEPTION) {
          ("Error", ss)
        } else {
          ("CreditRiskJobDetails.COMPLETED".toString, ss.mkString(APPEND_JSON_START, "", APPEND_JSON_END))
        }
      }
      ).observeOn(reactEndpointTabLeLoaderScheduler)
      .map(b => {
        if (b._2.contains(REACT_REST_API_EXCEPTION)) {
          new MessageEnvelop[SurfaceMessage](false, None, None, Some(b._1))
        } else {
          Try(adaptor.adapt(Array(new JSONObject(b._2))).map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some(b._1), None, None, None)).get)
          match {
            case Success(messageEnvelop) => messageEnvelop
            case failure => new MessageEnvelop[SurfaceMessage](false, None, None, Some(b._1))
          }
        }

      })

  }
  def bootstrapReactApi(): Observable[MessageEnvelop[SurfaceMessage]] = {
    val backoff = ExponentialBackoff(base = 1, multiplier = 2, maxALlowedFailure = 5)
    val retryKey = "react-api"

    retryWithExponentialBackoff(streamRatesFromReact(), backoff, retryKey)
      .onBackpressureBuffer
      .observeOn(creditRiskWorkerSchedulerFactory.reactEndpointTableLoaderScheduler)
      .map { json =>
        // You can add a guard if `json` contains an "error" field
        Try(surfaceMessageAdapter.adapt(json)) match {
          case Success(surfaceMessage) =>
            new MessageEnvelop[SurfaceMessage](true, Some(surfaceMessage), None, Some("Loading"))
          case Failure(ex) =>
            new MessageEnvelop[SurfaceMessage](false, None, None, Some(REACT_REST_API_EXCEPTION))
        }
      }
      .onErrorReturn { ex =>
        // Ensure any final unexpected error is still returned downstream
        new MessageEnvelop[SurfaceMessage](false, None, None, Some(s"Final failure: ${ex.getMessage}"))
      }
  }

  def retryWithExponentialBackoff[T](
                                      source: Observable[T],
                                      backoff: ExponentialBackoff,
                                      retryKey: String
                                    ): Observable[T] = {
    source.retryWhen { errors =>
      errors.flatMap {
        case _: TooManyRequestsException =>
          val (isAllowed, waitMillis) = backoff.isRetryAllowedWithWaitIntervalUntilSeconds(retryKey)
          if (isAllowed) {
            println(s"[RETRY] Waiting $waitMillis ms before retrying for $retryKey")
            Observable.timer(Duration(waitMillis, MILLISECONDS))
          } else {
            println(s"[FAILURE] Max retries exceeded for $retryKey")
            Observable.error(new RuntimeException(s"Max retries exceeded for $retryKey"))
          }

        case ex =>
          println(s"[ERROR] Not retryable: ${ex.getMessage}")
          Observable.error(ex)
      }
    }
  }
  case 429 =>
  logger.warn("Received 429 Too Many Requests")
  subscriber.onError(new TooManyRequestsException("Rate limit hit"))

  class TooManyRequestsException(message: String) extends RuntimeException(message)

}

