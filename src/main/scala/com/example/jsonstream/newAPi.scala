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

  def streamRatesFromReact(): Unit = {
    client.request().onComplete { reqAr =>
      if (reqAr.succeeded()) {
        val request = reqAr.result()
        request.send().onComplete { respAr =>
          if (respAr.succeeded()) {
            val response = respAr.result()
            if (response.statusCode() == 200) {
              response.handler { b =>
                executorService.execute(() => processChunk(b.toString))
              }.exceptionHandler { e =>
                logger.error("Error in response", e)
              }.endHandler { _ =>
                finalizeProcessing()
              }
            } else {
              logger.error("HTTP error: " + response.statusMessage())
            }
          } else logger.error("Request failed", respAr.cause())
        }
      } else logger.error("Request creation failed", reqAr.cause())
    }
  }

  private def processChunk(chunk: String): Unit = {
    buffer.append(chunk) // Append new data to the buffer
    parseBuffer() // Try extracting complete JSON objects
  }

  private def parseBuffer(): Unit = {
    val parser: JsonParser = objectMapper.createParser(buffer.toString())
    val extractedObjects = scala.collection.mutable.ListBuffer[String]()
    var lastProcessedIndex = 0

    try {
      if (parser.nextToken() == JsonToken.START_ARRAY) {
        lastProcessedIndex = parser.getCurrentLocation.getCharOffset.toInt
        while (parser.nextToken() == JsonToken.START_OBJECT) {
          val objectStart = parser.getCurrentLocation.getCharOffset.toInt
          parser.skipChildren() // Move to the end of the object
          val objectEnd = parser.getCurrentLocation.getCharOffset.toInt

          // Extract JSON substring from buffer
          val jsonString = buffer.substring(objectStart, objectEnd + 1)
          extractedObjects.append(jsonString)
          lastProcessedIndex = objectEnd + 1
        }
      }
    } catch {
      case _: Exception => // Incomplete object, keep buffer as is
    }

    // Emit all complete objects
    extractedObjects.foreach(obj => processObject(obj))

    // Retain only unprocessed portion in buffer
    if (lastProcessedIndex > 0 && lastProcessedIndex < buffer.length) {
      buffer = new StringBuilder(buffer.substring(lastProcessedIndex))
    }
  }

  private def processObject(json: String): Unit = {
    Try {
      val jsonObject = new JSONObject(json)
      val adapted = surfaceMessageAdapter.adapt(Array(jsonObject))
      val messageEnvelope = new MessageEnvelop[SurfaceMessage](true, Some(adapted), None, Some("Success"))
      logger.info("Processed Message: " + messageEnvelope)
    } match {
      case Success(_) =>
      case Failure(e) => logger.error("Failed to process object", e)
    }
  }

  private def finalizeProcessing(): Unit = {
    if (buffer.nonEmpty) {
      logger.warn("Unprocessed JSON remains in buffer: " + buffer.toString())
    }
    buffer.clear() // Clear buffer when stream ends
  }
}
