import com.example.jsonstream.{MessageEnvelop, SurfaceMessage}
import com.fasterxml.jackson.core.{JsonFactory, JsonParseException, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.JsonNode
import io.reactivex.rxjava3.core.Observable
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.JsonObject
import rx.Observable

import java.nio.CharBuffer

class ReactBootstrapApiConnector(host: String, port: Int, prefix: String, isSSL: Boolean, vertx: Vertx, surfaceMessageAdapter: SurfaceMessageAdapter) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jsonFactory = new JsonFactory()
  private val apiJsonStreamParser = new ApiJsonStreamParser(jsonFactory, processJsonObject) // Initialize the parser

  private val REACT_BOOTSTRAP_API_RESPONSE_COMPLETED = "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"
  private val APPEND_JSON_START = "{\"surfaceResult\":{"
  private val APPEND_JSON_END = "}"
  private val splitOn = "},\\{\\"surfaceResult\":\\{"
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"

  def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
    streamRatesFromReact
      .scan(("", Array.empty[String])) { case ((builder, _), next) =>
        val newBuilder = new StringBuilder(builder)
        val res =
          if (next == REACT_REST_API_EXCEPTION) Array(REACT_REST_API_EXCEPTION)
          else if (next == REACT_BOOTSTRAP_API_RESPONSE_COMPLETED)
            newBuilder.substring(startIndex, newBuilder.length - 2).split(splitOn)
          else {
            newBuilder.append(next)
            Array.empty[String]
          }
        (newBuilder.toString, res)
      }
      .map(_._2)
      .filter(_.nonEmpty)
      .flatMapIterable(identity)
      .observeOn(creditRiskWorkerSchedulerFactory.reactEndpointTableLoaderScheduler)
      .map { ss =>
        if (ss == REACT_REST_API_EXCEPTION)
          (CreditRiskJobDetails.ERROR.toString, ss)
        else
          (CreditRiskJobDetails.COMPLETED.toString, ss.mkString(APPEND_JSON_START, "", APPEND_JSON_END))
      }
      .observeOn(creditRiskWorkerSchedulerFactory.reactEndpointTableLoaderScheduler)
      .map { case (status, jsonData) =>
        if (jsonData.contains(REACT_REST_API_EXCEPTION)) {
          new MessageEnvelop[SurfaceMessage](false, None, None, Some(status))
        } else {
          Try {
            val jsonObj = new JsonObject(jsonData)
            new MessageEnvelop[SurfaceMessage](true, Some(jsonObj), None, Some(status))
          }.getOrElse(new MessageEnvelop[SurfaceMessage](false, None, None, Some(status)))
        }
      }
  }

  // This method will be called by ApiJsonStreamParser for each complete JsonNode
  private def processJsonObject(node: JsonNode): Unit = {
    Try {
      val jsonString = node.toString // Convert JsonNode to String
      val jsonObject = new JsonObject(jsonString) // Create JsonObject
      processedObjects.append(jsonObject)  // Store the processed JsonObject
    } catch {
      case e: Exception =>
        logger.error("Error converting JsonNode to JsonObject", e)
    }
  }

  private val processedObjects = ListBuffer[JsonObject]() // Buffer to hold processed JsonObjects

  // Method to get and clear processed objects
  private def getAndClearProcessedObjects(): List[JsonObject] = {
    val result = processedObjects.toList
    processedObjects.clear()
    result
  }

  private def convertToMessageEnvelop(jsonObject: JsonObject): MessageEnvelop[SurfaceMessage] = {
    // ... (Your implementation for convertToMessageEnvelop) ...
  }

  // Assuming streamRatesFromReact is defined elsewhere
  private def streamRatesFromReact: Observable[String] = {
    // ... (Your implementation for streamRatesFromReact) ...
  }
}

class ApiJsonStreamParser(jsonFactory: JsonFactory, processJsonObject: JsonNode => Unit) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val charBuffer = CharBuffer.allocate(ApiJsonStreamProcessor.INITIAL_BUFFER_SIZE)
  private var parser: JsonParser = _
  private var depth = 0
  private var totalProcessed = 0L
  private var objectCount = 0

  def processChunk(chunk: Buffer): Unit = {
    if (chunk != null && chunk.length() > 0) {
      ensureBufferCapacity(chunk.length())
      charBuffer.append(chunk.toString())
      processBuffer()
    }
  }

  private def ensureBufferCapacity(additionalSize: Int): Unit = {
    if (charBuffer.remaining() < additionalSize) {
      val newSize = Math.min(
        charBuffer.capacity() * 2,
        ApiJsonStreamProcessor.MAX_BUFFER_SIZE
      )
      if (newSize >= ApiJsonStreamProcessor.MAX_BUFFER_SIZE) {
        logger.warn("Buffer size reached maximum limit")
      }
      val newBuffer = CharBuffer.allocate(newSize)
      charBuffer.flip()
      newBuffer.put(charBuffer)
      charBuffer.clear()
      charBuffer.array() = newBuffer.array()
    }
  }

  private def processBuffer(): Unit = {
    try {
      if (parser == null) {
        charBuffer.flip()
        parser = ApiJsonStreamProcessor.jsonFactory.createParser(charBuffer.toString())
      }

      var token: JsonToken = null
      while ({token = parser.nextToken(); token != null}) {
        token match {
          case JsonToken.START_OBJECT =>
            depth += 1
            if (depth == 1) {
              processRootObject()
            }
          case JsonToken.END_OBJECT =>
            depth -= 1
          case JsonToken.START_ARRAY =>
            if (depth == 0) {
              processArrayElements()
            }
          case _ => // Other tokens handled within processRootObject or processArrayElements
        }
      }

      // Update buffer with unprocessed content
      val processed = parser.getTokenLocation().getCharOffset().toInt
      totalProcessed += processed
      charBuffer.position(processed)
      charBuffer.compact()

      parser.close()
      parser = null

    } catch {
      case e: JsonParseException =>
        logger.error(s"JSON parsing error at offset ${e.getLocation.getCharOffset}: ${e.getMessage}")
        if (parser != null) {
          parser.close()
          parser = null
        }
      case e: Exception =>
        logger.error(s"Error processing buffer: ${e.getMessage}", e)
        if (parser != null) {
          parser.close()
          parser = null
        }
    }
  }

  private def processRootObject(): Unit = {
    try {
      val node = parser.readValueAsTree[JsonNode]()
      objectCount += 1
      processJsonObject(node)
      depth = 0 // Reset depth after processing
    } catch {
      case e: Exception =>
        logger.error(s"Error processing root object: ${e.getMessage}", e)
    }
  }

  private def processArrayElements(): Unit = {
    try {
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        if (parser.currentToken() == JsonToken.START_OBJECT) {
          val node = parser.readValueAsTree[JsonNode]()
          objectCount += 1
          processJsonObject(node)
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error processing array elements: ${e.getMessage}", e)
    }
  }

  def processRemainingData(): Unit = {
    if (charBuffer.position() > 0) {
      try {
        charBuffer.flip()
        val remainingData = charBuffer.toString()
        if (remainingData.trim.nonEmpty) {
          val tempParser = ApiJsonStreamProcessor.jsonFactory.createParser(remainingData)
          while (tempParser.nextToken() != null) {
            if (tempParser.currentToken() == JsonToken.START_OBJECT) {
              val node = tempParser.readValueAsTree[JsonNode]()
              objectCount += 1
              processJsonObject(node)
            }
          }
          tempParser.close()
        }
      } catch {
        case e: JsonParseException =>
          logger.error(s"Error parsing remaining data: ${e.getMessage}", e)
        case e: Exception =>
          logger.error(s"Error processing remaining data: ${e.getMessage}", e)
      }
    }
    logger.info(s"Completed processing $objectCount objects, total bytes processed: $totalProcessed")
  }

  private def processJsonObject(node: JsonNode): Unit = {
    try {
      // Example implementation - replace with actual processing logic
      if (node.has("id")) {
        logger.info(s"Processing object with ID: ${node.get("id")}")
      }

      // Implement your specific processing logic here
      // For example:
      // - Transform the data
      // - Store in database
      // - Send to another service
      // - etc.

    } catch {
      case e: Exception =>
        logger.error(s"Error in processJsonObject: ${e.getMessage}", e)
    }
  }

  def cleanup(): Unit = {
    try {
      if (parser != null) {
        parser.close()
        parser = null
      }
    } catch {
      case e: Exception =>
        logger.error("Error during cleanup", e)
    }
  }
}