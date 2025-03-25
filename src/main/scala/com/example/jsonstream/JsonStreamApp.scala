package com.example.jsonstream

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import rx.lang.scala.{Observable, Subscriber}
import java.math.BigInteger
import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}

object JsonStreamApp {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val vertx = Vertx.vertx()
    val client = vertx.createHttpClient()

    val remoteServerHost = "your-remote-server-host" // Replace with your server host
    val remoteServerPort = 80 // Replace with your server port
    val remoteServerPath = "/your-api-path" // Replace with your API path

    streamAndPrintJson(client, remoteServerHost, remoteServerPort, remoteServerPath)
      .subscribe(
        jsonObject => logger.info(s"Received JSON Object: $jsonObject"),
        error => logger.error("Error during JSON streaming", error),
        () => logger.info("JSON streaming completed")
      )
    // Add a shutdown hook to ensure Vert.x is properly closed
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Shutting down Vert.x")
      vertx.close()
    }))
  }

  def streamAndPrintJson(client: HttpClient, host: String, port: Int, path: String): Observable[JsonObject] = {
    Observable[JsonObject] { subscriber =>
      client.request().onComplete { requestAr =>
        if (requestAr.succeeded()) {
          val request = requestAr.result()
          request.send().onComplete { responseAr =>
            if (responseAr.succeeded()) {
              val response = responseAr.result()
              if (response.statusCode() == 200) {
                val parser = createJsonParser(subscriber)
                response.handler { buffer: Buffer =>
                  logger.debug(s"Received JSON chunk: ${buffer.toString.take(200)}...") // Log first 200 chars
                  parser.handle(buffer)
                }
                response.exceptionHandler { error =>
                  subscriber.onError(error)
                }
                response.endHandler { _ =>
                  subscriber.onCompleted()
                }
              } else {
                subscriber.onError(new RuntimeException(s"HTTP request failed: ${response.statusCode()} ${response.statusMessage()}"))
              }
            } else {
              subscriber.onError(responseAr.cause())
            }
          }
        } else {
          subscriber.onError(requestAr.cause())
        }
      }
    }
  }

  def createJsonParser(subscriber: Subscriber[JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    parser.objectValueMode()
    parser.handler(event => handleJsonEvent(event, subscriber))
    parser.exceptionHandler(subscriber.onError(_))
    parser.endHandler(_ => subscriber.onCompleted())
    parser
  }

  def handleJsonEvent(event: JsonEvent, subscriber: Subscriber[JsonObject]): Unit = {
    var currentObject: JsonObject = null
    var currentKey: String = null

    event.`type`() match {
      case JsonEventType.START_OBJECT =>
        currentObject = new JsonObject()
      case JsonEventType.VALUE =>
        if (currentObject != null) {
          if (currentKey == null) {
            currentKey = event.fieldName()
          } else {
            val value = event.value()
            if (value != null) {
              try {
                val stringValue = value.toString
                try {
                  currentObject.put(currentKey, stringValue.toLong)
                } catch {
                  case _: NumberFormatException =>
                    currentObject.put(currentKey, new BigInteger(stringValue))
                }
              } catch {
                case e: Exception =>
                  logger.warn("Error parsing value, keeping original type", e)
                  currentObject.put(currentKey, value)
              }
            } else {
              currentObject.put(currentKey, null)
            }
            currentKey = null
          }
        }
      case JsonEventType.END_OBJECT =>
        if (currentObject != null) {
          subscriber.onNext(currentObject)
        }
      case JsonEventType.START_ARRAY =>
      case JsonEventType.END_ARRAY =>
      case _ =>
    }
  }
}
