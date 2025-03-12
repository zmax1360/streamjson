package com.example.jsonstream

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.{AsyncFile, FileSystem, OpenOptions}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.StringBuilder

object JsonStreamProcessor {
  private val CHUNK_SIZE = 1024 * 1024 // 1MB chunk size
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val jsonFactory = new JsonFactory(mapper)
  
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Please provide the JSON file path as an argument")
      System.exit(1)
    }

    val filePath = args(0)
    val vertx = Vertx.vertx()
    
    processJsonFile(vertx, filePath)
  }

  def processJsonFile(vertx: Vertx, filePath: String): Unit = {
    val fs = vertx.fileSystem()
    
    fs.open(filePath, new OpenOptions(), new Handler[AsyncResult[AsyncFile]] {
      override def handle(asyncResult: AsyncResult[AsyncFile]): Unit = {
        if (asyncResult.succeeded()) {
          val file = asyncResult.result()
          val streamParser = new JsonStreamParser(file)
          streamParser.startParsing()
        } else {
          println(s"Error opening file: ${asyncResult.cause().getMessage}")
          vertx.close()
        }
      }
    })
  }
}

class JsonStreamParser(file: AsyncFile) {
  private val buffer = new StringBuilder()
  private var parser: JsonParser = _
  private var isReading = true

  def startParsing(): Unit = {
    readNextChunk()
  }

  private def readNextChunk(): Unit = {
    if (!isReading) return

    file.read(Buffer.buffer(JsonStreamProcessor.CHUNK_SIZE), 0, 0, JsonStreamProcessor.CHUNK_SIZE, 
      new Handler[AsyncResult[Buffer]] {
        override def handle(ar: AsyncResult[Buffer]): Unit = {
          if (ar.succeeded()) {
            val chunk = ar.result()
            if (chunk.length() == 0) {
              // End of file reached
              processRemainingBuffer()
              cleanup()
            } else {
              buffer.append(chunk.toString())
              processBuffer()
              readNextChunk()
            }
          } else {
            println(s"Error reading file: ${ar.cause().getMessage}")
            cleanup()
          }
        }
      })
  }

  private def processBuffer(): Unit = {
    try {
      if (parser == null) {
        parser = JsonStreamProcessor.jsonFactory.createParser(buffer.toString)
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
        // If we hit an incomplete JSON object, keep it in buffer for next chunk
        if (parser != null) {
          parser.close()
          parser = null
        }
    }
  }

  private def processRemainingBuffer(): Unit = {
    if (buffer.nonEmpty) {
      try {
        val node = JsonStreamProcessor.mapper.readTree(buffer.toString)
        processJsonObject(node)
      } catch {
        case e: Exception =>
          println(s"Error processing remaining buffer: ${e.getMessage}")
      }
    }
  }

  private def processJsonObject(node: JsonNode): Unit = {
    // Placeholder for actual JSON object processing
    // Here you can implement your specific processing logic
    println(s"Processing JSON object: ${node.toString.take(100)}...")
  }

  private def cleanup(): Unit = {
    isReading = false
    if (parser != null) {
      parser.close()
    }
    file.close()
  }
} 