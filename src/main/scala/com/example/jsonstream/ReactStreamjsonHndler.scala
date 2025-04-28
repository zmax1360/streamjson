package com.example.jsonstream

import io.vertx.core.json.{JsonEvent, JsonEventType, JsonObject, JsonParser}
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import org.slf4j.LoggerFactory
import rx.lang.scala.Subscriber

import java.math.BigInteger

trait ReactStreamJsonHandler {
  def begin(): Unit

  def handle(event: JsonEvent): Unit

  def handleError(e: Throwable): Unit

  def end(): Unit
}

class ReactHandler(subscriber: Subscriber[JsonObject]) extends ReactStreamJsonHandler {
  private var objectCount = 0
  private val logger = LoggerFactory.getLogger(getClass)
  private var currentObject: JsonObject = null
  private var currentKey: String = null

  override def begin(): Unit = logger.info("JSON stream started")

  override def handle(event: JsonEvent): Unit = {
    if (event.isObject) {
      val jsonObj = event.objectValue()
      handleLargeNumbers(jsonObj)
      objectCount += 1
      subscriber.onNext(jsonObj)
    }
  }
  private def handleLargeNumbers(jsonObj: JsonObject): Unit = {
    jsonObj.fieldNames().forEach { fieldName =>
      val value = jsonObj.getValue(fieldName)
      value match {
        case n: Number =>
          // Convert to string if number is too large for Long
          try {
            n.longValueExact() // Will throw if doesn't fit in Long
          } catch {
            case _: ArithmeticException =>
              jsonObj.put(fieldName, n.toString)
          }
        case _ => // Do nothing for non-number fields
      }
    }
  }

  override def handleError(e: Throwable): Unit = {
    logger.error("Error occurred during JSON streaming", e)
    subscriber.onError(e)
  }

  override def end(): Unit = {
    logger.info("JSON stream ended")
    logger.info("Number of recorde processed: ", objectCount)
    subscriber.onCompleted()
  }
}

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
  def parseJsonStream(subscriber: Subscriber[JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    parser.objectValueMode()
    val handler = new ReactHandler(subscriber)
    streamingParser(handler, parser)
  }

  def streamingParser(handler: ReactStreamJsonHandler, parser: JsonParser): JsonParser = {
    parser.handler(event => handler.handle(event))
    parser.exceptionHandler(e => handler.handleError(e))
    parser.endHandler(_ => handler.end())
  }
}