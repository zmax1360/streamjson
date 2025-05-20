import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClient
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import io.vertx.rxjava3.core.Vertx
import rx.Subscriber
import rx.lang.scala.Observable
import org.slf4j.LoggerFactory
import scala.util.Try

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
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"

  /**
   * Streaming JSON Handler Interface
   * Defines how to handle different JSON parsing events.
   */
  trait StreamingJsonHandler {
    def begin(): Unit
    def handle(event: JsonEvent): Unit
    def handleError(e: Throwable): Unit
    def end(): Unit
  }

  /**
   * JSON Streaming Handler Implementation
   * Handles JSON objects and emits them to the subscriber.
   */
  class MyStreamJsonHandler(subscriber: Subscriber[JsonObject]) extends StreamingJsonHandler {
    private var currentObject: JsonObject = _
    private var currentFieldName: String = _
    private var insideArray = false

    override def begin(): Unit = logger.debug("JSON Streaming started")

    override def handle(event: JsonEvent): Unit = {
      event.eventType() match {
        case JsonEventType.START_ARRAY =>
          insideArray = true

        case JsonEventType.END_ARRAY =>
          insideArray = false
          subscriber.onCompleted()

        case JsonEventType.START_OBJECT =>
          currentObject = new JsonObject()


        case JsonEventType.END_OBJECT =>
          if (insideArray) {
            subscriber.onNext(currentObject) // Emit each object inside the array
          }
          currentObject = null

        case _ => // Handle unexpected cases
      }
    }

    override def handleError(e: Throwable): Unit = {
      logger.error("Error in JSON Streaming", e)
      subscriber.onError(e)
    }

    override def end(): Unit = {
      subscriber.onCompleted()
    }
  }

  /**
   * JSON Parser Handler
   * Manages JSON parsing and streaming.
   */


  /**
   * Handles JSON arrays and objects inside arrays.
   */
  object HandlerHelper {
    private val logger = LoggerFactory.getLogger(getClass)

    def arrayJsonEventHandler(parser: JsonParser, handler: ReactStreamJsonHandler, event: JsonEvent): Unit = {
      event.`type`() match {
        case JsonEventType.START_ARRAY =>
          logger.debug("Start of JSON array")
          parser.objectValueMode() // Switch to object value mode for nested objects

        case JsonEventType.VALUE =>
          logger.debug(s"Processing JSON value: ${event.value()}")
          handler.handle(event) // Delegate to ReactHandler

        case JsonEventType.END_ARRAY =>
          logger.debug("End of JSON array")
          handler.end() // Signal the end of the array

        case _ =>
          logger.warn(s"Unhandled JSON event type in array: ${event.`type`()}")
      }
    }
  }

  /**
   * Parses JSON as a stream and emits JSON objects.
   */
  private def parseJsonStream(subscriber: Subscriber[JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    val handler = new MyStreamJsonHandler(subscriber)
    streamingParser(handler, parser) // Attach parser to handler
  }

  /**
   * Streams JSON objects using Vert.x JsonParser.
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
                val parser = parseJsonStream(subscriber)
                response.handler { buffer: Buffer =>
                  parser.handle(buffer)
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
   * Converts streamed JSON objects into MessageEnvelop[SurfaceMessage].
   */
  def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
    streamRatesFromReact
      .map { json =>
        if (json.getString("status") == REACT_REST_API_EXCEPTION) {
          new MessageEnvelop[SurfaceMessage](false, None, None, Some(REACT_REST_API_EXCEPTION))
        } else {
          Try(surfaceMessageAdapter.adapt(Array(json)))
            .map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some("CreditRiskJobDetails.COMPLETED")))
            .getOrElse(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error")))
        }
      }
  }
}
