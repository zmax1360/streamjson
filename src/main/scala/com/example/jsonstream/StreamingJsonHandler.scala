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

  trait StreamJsonHandler {
    def handle(event: JsonEvent): Unit
    def handleError(e: Throwable): Unit
    def end(): Unit
  }

  class MyStreamJsonHandler(subscriber: Subscriber[JsonObject]) extends StreamJsonHandler {
    private var currentObject: JsonObject = _
    private var currentFieldName: String = _
    private var insideArray = false

    override def handle(event: JsonEvent): Unit = {
      event.eventType() match {
        case JsonEventType.START_ARRAY =>
          insideArray = true

        case JsonEventType.END_ARRAY =>
          insideArray = false
          subscriber.onCompleted()

        case JsonEventType.START_OBJECT =>
          currentObject = new JsonObject()

        case JsonEventType.FIELD_NAME =>
          currentFieldName = event.fieldName()

        case JsonEventType.VALUE_STRING =>
          if (currentObject != null) currentObject.put(currentFieldName, event.stringValue())

        case JsonEventType.VALUE_NUMBER =>
          if (currentObject != null) currentObject.put(currentFieldName, event.numberValue())

        case JsonEventType.VALUE_BOOLEAN =>
          if (currentObject != null) currentObject.put(currentFieldName, event.booleanValue())

        case JsonEventType.VALUE_NULL =>
          if (currentObject != null) currentObject.putNull(currentFieldName)

        case JsonEventType.END_OBJECT =>
          if (insideArray) {
            subscriber.onNext(currentObject)
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

  private def parseJsonStream(subscriber: Subscriber[JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    val handler = new MyStreamJsonHandler(subscriber)

    parser.handler { event =>
      handler.handle(event)
    }

    parser.exceptionHandler { e =>
      handler.handleError(e)
    }

    parser.endHandler { _ =>
      subscriber.onNext(new JsonObject().put("status", "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"))
      subscriber.onCompleted()
    }

    parser
  }

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
