import com.example.jsonstream.{MessageEnvelop, SurfaceMessage, SurfaceMessageAdapter}
import com.fasterxml.jackson.core.JsonFactory
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import scala.util.{Failure, Success, Try}
import org.json.JSONObject
import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import rx.lang.scala.{Observable, Scheduler}
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.ExecutionContext
import com.google.common.util.concurrent.ThreadFactoryBuilder

class ReactBootstrapApiConnector(
                                  host: String,
                                  port: Int,
                                  prefix: String,
                                  isSSL: Boolean,
                                  vertx: Vertx,
                                  surfaceMessageAdapter: SurfaceMessageAdapter
                                ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jsonFactory = new JsonFactory()
  private val client: HttpClient = vertx.createHttpClient()
  private val executorService = Executors.newSingleThreadExecutor()

  private val objectMapper = new ObjectMapper()
  private var buffer = new StringBuilder()

  lazy val reactEndpointTableLoaderScheduler: Scheduler = {
    val threadFactory = new ThreadFactoryBuilder().setNameFormat("react").build()
    val executionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(1, threadFactory)
    )
    ExecutionContextScheduler(executionContext)
  }

  def streamRatesFromReact(): Observable[String] = {
    Observable.create { subscriber =>
      client.request().onComplete { reqAr =>
        if (reqAr.succeeded()) {
          val request = reqAr.result()
          request.send().onComplete { respAr =>
            if (respAr.succeeded()) {
              val response = respAr.result()
              if (response.statusCode() == 200) {
                response.handler { b =>
                  executorService.execute(() => processChunk(b.toString()))
                }.exceptionHandler { e =>
                  subscriber.onError(e)
                }.endHandler { _ =>
                  subscriber.onCompleted()
                }
              } else {
                subscriber.onError(new RuntimeException(response.statusMessage()))
              }
            } else {
              subscriber.onError(respAr.cause())
            }
          }
        } else {
          subscriber.onError(reqAr.cause())
        }
      }
    }
  }

  def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
    streamRatesFromReact
      .flatMapIterable(chunk => {
        processChunk(chunk)
        val (completeObjects, remainingBuffer) = parseBuffer()

        completeObjects.map(obj => convertToMessageEnvelop(obj))
      })
      .onErrorReturn(e => {
        logger.error("Error processing React API stream", e)
        new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error processing data"))
      })
  }

  private def processChunk(chunk: String): Unit = {
    buffer.append(chunk) // Append new data to the buffer
  }

  private def parseBuffer(): (Array[String], String) = {
    val extractedObjects = scala.collection.mutable.ListBuffer[String]()
    val parser = objectMapper.createParser(buffer.toString())
    var lastProcessedIndex = 0

    try {
      while (parser.nextToken() != null) {
        if (parser.getCurrentToken == JsonToken.START_OBJECT) {
          val objectStart = parser.getCurrentLocation.getCharOffset.toInt
          parser.skipChildren()
          val objectEnd = parser.getCurrentLocation.getCharOffset.toInt

          val jsonString = buffer.substring(objectStart, objectEnd + 1)
          extractedObjects.append(jsonString)
          lastProcessedIndex = objectEnd + 1
        }
      }
    } catch {
      case e: Exception =>
        logger.warn("Parsing incomplete, waiting for next chunk", e)
    }
    val remainingBuffer = if (lastProcessedIndex > 0 && lastProcessedIndex < buffer.length) {
      buffer.substring(lastProcessedIndex)
    } else ""
    (extractedObjects.toArray, remainingBuffer)
  } = {
    val extractedObjects = scala.collection.mutable.ListBuffer[String]()
    val parser = objectMapper.createParser(buffer.toString())
    var lastProcessedIndex = 0

    try {
      while (parser.nextToken() == JsonToken.START_OBJECT) {
        val objectStart = parser.getCurrentLocation.getCharOffset.toInt
        parser.skipChildren()
        val objectEnd = parser.getCurrentLocation.getCharOffset.toInt

        val jsonString = buffer.substring(objectStart, objectEnd + 1)
        extractedObjects.append(jsonString)
        lastProcessedIndex = objectEnd + 1
      }
    } catch {
      case _: Exception =>
    }
    val remainingBuffer = if (lastProcessedIndex > 0 && lastProcessedIndex < buffer.length) {
      buffer.substring(lastProcessedIndex)
    } else ""
    (extractedObjects.toArray, remainingBuffer)
  }

  private def convertToMessageEnvelop(json: String): MessageEnvelop[SurfaceMessage] = {
    Try {
      val jsonObject = new JSONObject(json)
      val adapted = surfaceMessageAdapter.adapt(Array(jsonObject))
      new MessageEnvelop[SurfaceMessage](true, Some(adapted), None, Some("Success"))
    } match {
      case Success(messageEnvelope) => messageEnvelope
      case Failure(e) =>
        logger.error("Failed to process object", e)
        new MessageEnvelop[SurfaceMessage](false, None, None, Some("Parsing Error"))
    }
  }

  private def finalizeProcessing(): Unit = {
    if (buffer.nonEmpty) {
      logger.warn("Unprocessed JSON remains in buffer: " + buffer.toString())
    }
    buffer.clear() // Clear buffer when stream ends
  }
}
