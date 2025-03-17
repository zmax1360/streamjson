package com.example.jsonstream

import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import org.slf4j.LoggerFactory
import rx.Subscriber
import java.math.BigInteger

trait ReactStreamJsonHandler {
  def begin(): Unit
  def handle(event: JsonEvent):Unit
  def handleError(e: Throwable):Unit
  def end():Unit
}


class ReactHandler(subscriber: Subscriber[JsonObject]) extends ReactStreamJsonHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  override def begin(): Unit = logger.info("JSON stream started")
  override def handle(event: JsonEvent): Unit = {
    event.`type`() match {
      case JsonEventType.VALUE if event.isNumber =>
        // Handle large numeric values
        val numericValue = event.value().toString
        try {
          val bigIntValue = new BigInteger(numericValue)
          logger.debug(s"Handling large numeric value: $bigIntValue")
          // Create a JsonObject with the numeric value
          val jsonObject = new JsonObject().put("value", bigIntValue.toString)
          subscriber.onNext(jsonObject)
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Failed to parse large numeric value: $numericValue", e)
            subscriber.onError(e)
        }
      case JsonEventType.VALUE if event.isObject =>
        // Handle JSON objects as before
        logger.debug(s"Handling JSON object: ${event.objectValue().toString}")
        subscriber.onNext(event.objectValue())
      case _ =>
        // Ignore other event types
        logger.warn(s"Unhandled JSON event type: ${event.`type`()}")
    }
  }
  override def handleError(e: Throwable): Unit = {
    logger.error("Error occurred during JSON streaming", e)
    subscriber.onError(e)
  }
  override def end(): Unit = {
    logger.info("JSON stream ended")
    subscriber.onCompleted()
  }
}
object HandlerHelper {
  def arrayJsonEventHandler(parser: JsonParser, handler: ReactStreamJsonHandler, event: JsonEvent): Unit = {
    event.`type`() match {
      case JsonEventType.START_ARRAY =>
        parser.objectValueMode()
      case JsonEventType.VALUE =>
        handler.handle(event)
      case JsonEventType.END_ARRAY =>
        handler.end()
      case _ =>
      // Handle other event types if needed
    }
  }

  def parseJsonStream(subscriber: Subscriber[JsonObject]): JsonParser = {
    val parser = JsonParser.newParser()
    val handler = new ReactHandler(subscriber)
    streamingParser(handler, parser)
  }

  def streamingParser(handler: ReactStreamJsonHandler, parser: JsonParser): JsonParser = {
    parser.handler(event => arrayJsonEventHandler(parser, handler, event))
    parser.exceptionHandler(e => handler.handleError(e))
    parser.endHandler(_ => handler.end())
    parser
  }
}
