import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClient
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.JsonParser
import io.vertx.rxjava3.core.Vertx
import io.vertx.rxjava3.core.http.HttpClientRequest
import io.vertx.rxjava3.core.http.HttpClientResponse
import io.vertx.rxjava3.core.parsetools.JsonParser
import rx.Observable
import java.util.concurrent.Executors
import scala.util.{Failure, Success, Try}

class ReactBootstrapApiConnector(
                                  host: String,
                                  port: Int,
                                  prefix: String,
                                  isSSL: Boolean,
                                  vertx: Vertx,
                                  surfaceMessageAdapter: SurfaceMessageAdapter
                                ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val client: HttpClient = vertx.createHttpClient()
  private val REACT_BOOTSTRAP_API_RESPONSE_COMPLETED = "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"

  private val executorService = Executors.newSingleThreadExecutor()
  private val adaptor = new SurfaceMessageAdapter()

  lazy val reactEndpointTabLeLoaderScheduler: Scheduler = {
    val threadFactory = new ThreadFactoryBuilder().setNameFormat("react").build()
    val executionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(1, threadFactory)
    )
    ExecutionContextScheduler(executionContext)
  }

  /**
   * Handles JSON parsing using Vert.x JsonParser and streams parsed objects.
   */
  private def parseJsonStream(subscriber: rx.Subscriber[_ >: JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    parser.handler { event =>
      if (event.isObject()) {
        subscriber.onNext(event.objectValue()) // Emit JSON objects as they arrive
      }
    }
    parser.exceptionHandler { e =>
      subscriber.onError(e) // Handle parsing errors
    }
    parser.endHandler { _ =>
      subscriber.onNext(new JsonObject().put("status", REACT_BOOTSTRAP_API_RESPONSE_COMPLETED))
      subscriber.onCompleted()
    }
    parser
  }

  /**
   * Streams JSON objects using JsonParser
   */
  def streamRatesFromReact: Observable[JsonObject] = {
    Observable.create { subscriber =>
      client.request().onComplete { reqAr =>
        if (reqAr.succeeded()) {
          val request = reqAr.result()
          request.send().onComplete { respAsr =>
            if (respAsr.succeeded()) {
              val response = respAsr.result()
              if (response.statusCode() == 200) {
                val parser = parseJsonStream(subscriber) // Use extracted method for JSON parsing
                response.handler { buffer: Buffer =>
                  parser.handle(buffer) // Feed received data into the parser
                }
              } else {
                subscriber.onError(new RuntimeException(response.statusMessage()))
              }
            } else {
              subscriber.onError(respAsr.cause())
            }
          }
        } else {
          subscriber.onError(reqAr.cause())
        }
      }
    }.onErrorReturn { e =>
      logger.error("Error in streamRatesFromReact", e)
      new JsonObject().put("status", REACT_REST_API_EXCEPTION)
    }
  }

  /**
   * Processes JSON objects and adapts them into MessageEnvelop[SurfaceMessage]
   */
  def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
    streamRatesFromReact
      .observeOn(reactEndpointTabLeLoaderScheduler)
      .map { json =>
        if (json.getString("status") == REACT_REST_API_EXCEPTION) {
          new MessageEnvelop[SurfaceMessage](false, None, None, Some(REACT_REST_API_EXCEPTION))
        } else {
          Try(adaptor.adapt(Array(json)))
            .map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some("CreditRiskJobDetails.COMPLETED")))
            .getOrElse(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error")))
        }
      }
  }
}
