package com.example.jsonstream

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.client.{HttpResponse, WebClient}
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.core.streams.ReadStream
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken, JsonParseException}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.CharBuffer
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

object ApiJsonStreamProcessor {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val jsonFactory = new JsonFactory(mapper)
    .enable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
    .disable(JsonParser.Feature.AUTO_CLOSE_TARGET)
  
  // Configuration constants
  private val INITIAL_BUFFER_SIZE = 8192 // 8KB initial size
  private val MAX_BUFFER_SIZE = 1024 * 1024 * 10 // 10MB max size
  private val MIN_CHUNK_SIZE = 1024 // 1KB minimum chunk size
  
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Please provide the API URL as an argument")
      System.exit(1)
    }

    val apiUrl = args(0)
    val vertx = Vertx.vertx()
    
    // Create WebClient with appropriate options
    val options = new WebClientOptions()
      .setMaxPoolSize(1)
      .setKeepAlive(true)
      .setTryUseCompression(true)
      .setIdleTimeout(30) // 30 seconds timeout
      .setConnectTimeout(5000) // 5 seconds connect timeout
    
    val client = WebClient.create(vertx, options)
    
    processApiJson(vertx, client, apiUrl)
  }

  def processApiJson(vertx: Vertx, client: WebClient, apiUrl: String): Unit = {
    try {
      val url = new java.net.URL(apiUrl)
      val host = url.getHost
      val path = if (url.getPath.isEmpty) "/" else url.getPath
      val port = if (url.getPort == -1) if (url.getProtocol == "https") 443 else 80 else url.getPort
      val ssl = url.getProtocol == "https"
      
      client.get(port, host, path)
        .ssl(ssl)
        .as(io.vertx.core.buffer.Buffer.FACTORY)
        .send(new Handler[AsyncResult[HttpResponse[Buffer]]] {
          override def handle(ar: AsyncResult[HttpResponse[Buffer]]): Unit = {
            if (ar.succeeded()) {
              val response = ar.result()
              if (response.statusCode() == 200) {
                val streamParser = new ApiJsonStreamParser()
                val processedBytes = new AtomicLong(0)
                
                val stream = response.request().response().toStream()
                stream.pause()
                
                stream.handler(new Handler[Buffer] {
                  override def handle(chunk: Buffer): Unit = {
                    try {
                      if (chunk.length() < MIN_CHUNK_SIZE) {
                        logger.debug(s"Received small chunk of size ${chunk.length()}")
                      }
                      
                      processedBytes.addAndGet(chunk.length())
                      if (processedBytes.get() > MAX_BUFFER_SIZE) {
                        logger.warn("Processing large amount of data, consider increasing buffer size")
                      }
                      
                      streamParser.processChunk(chunk)
                    } catch {
                      case e: JsonParseException =>
                        logger.error(s"JSON parsing error: ${e.getMessage}", e)
                        stream.pause()
                      case e: Exception =>
                        logger.error(s"Error processing chunk: ${e.getMessage}", e)
                        stream.pause()
                    }
                  }
                })
                
                stream.endHandler(new Handler[Void] {
                  override def handle(v: Void): Unit = {
                    logger.info(s"Stream ended, processed ${processedBytes.get()} bytes")
                    try {
                      streamParser.processRemainingData()
                    } catch {
                      case e: Exception =>
                        logger.error("Error processing remaining data", e)
                    } finally {
                      streamParser.cleanup()
                      client.close()
                      vertx.close()
                    }
                  }
                })
                
                stream.exceptionHandler(new Handler[Throwable] {
                  override def handle(t: Throwable): Unit = {
                    logger.error(s"Stream error: ${t.getMessage}", t)
                    streamParser.cleanup()
                    client.close()
                    vertx.close()
                  }
                })
                
                stream.resume()
                
              } else {
                logger.error(s"API request failed with status code: ${response.statusCode()}")
                client.close()
                vertx.close()
              }
            } else {
              logger.error(s"Error making API request: ${ar.cause().getMessage}", ar.cause())
              client.close()
              vertx.close()
            }
          }
        })
    } catch {
      case e: Exception =>
        logger.error(s"Error parsing URL: ${e.getMessage}", e)
        client.close()
        vertx.close()
    }
  }
}

class ApiJsonStreamParser {
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
      // Copy reference to the new buffer
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