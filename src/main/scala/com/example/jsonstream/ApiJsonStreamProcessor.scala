package com.example.jsonstream

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.client.{HttpResponse, WebClient}
import io.vertx.ext.web.client.WebClientOptions
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.StringBuilder

object ApiJsonStreamProcessor {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val jsonFactory = new JsonFactory(mapper)
  
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Please provide the API URL as an argument")
      System.exit(1)
    }

    val apiUrl = args(0)
    val vertx = Vertx.vertx()
    
    // Create WebClient with appropriate options
    val options = new WebClientOptions()
      .setMaxPoolSize(1) // Limit concurrent connections
      .setKeepAlive(true)
      .setTryUseCompression(true)
    
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
        .send(new Handler[AsyncResult[HttpResponse[Buffer]]] {
          override def handle(ar: AsyncResult[HttpResponse[Buffer]]): Unit = {
            if (ar.succeeded()) {
              val response = ar.result()
              if (response.statusCode() == 200) {
                val streamParser = new ApiJsonStreamParser()
                streamParser.processChunk(response.body())
                streamParser.cleanup()
              } else {
                println(s"API request failed with status code: ${response.statusCode()}")
              }
            } else {
              println(s"Error making API request: ${ar.cause().getMessage}")
            }
            client.close()
            vertx.close()
          }
        })
    } catch {
      case e: Exception =>
        println(s"Error parsing URL: ${e.getMessage}")
        client.close()
        vertx.close()
    }
  }
}

class ApiJsonStreamParser {
  private val buffer = new StringBuilder()
  private var parser: JsonParser = _

  def processChunk(chunk: Buffer): Unit = {
    if (chunk != null && chunk.length() > 0) {
      buffer.append(chunk.toString())
      processBuffer()
    }
  }

  private def processBuffer(): Unit = {
    try {
      if (parser == null) {
        parser = ApiJsonStreamProcessor.jsonFactory.createParser(buffer.toString)
      }

      while (parser.nextToken() != null) {
        if (parser.currentToken() == JsonToken.START_OBJECT) {
          val node = parser.readValueAsTree[JsonNode]()
          processJsonObject(node)
        }
      }

      // Clear processed content from buffer
      val processed = parser.getTokenLocation().getCharOffset().toInt
      buffer.delete(0, processed)
      
      // Create new parser for remaining content
      parser.close()
      parser = null
      
    } catch {
      case e: Exception =>
        // If we hit an incomplete JSON object, keep it in buffer
        if (parser != null) {
          parser.close()
          parser = null
        }
    }
  }

  private def processJsonObject(node: JsonNode): Unit = {
    // Placeholder for actual JSON object processing
    // Here you can implement your specific processing logic
    println(s"Processing JSON object: ${node.toString.take(100)}...")
  }

  def cleanup(): Unit = {
    if (parser != null) {
      parser.close()
    }
  }
} 