package com.example.jsonstream

import io.vertx.core.json.{JsonEvent, JsonEventType, JsonObject, JsonParser}
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
  private val logger = LoggerFactory.getLogger(getClass)
  private var currentObject: JsonObject = null
  private var currentKey: String = null

  override def begin(): Unit = logger.info("JSON stream started")

  override def handle(event: JsonEvent): Unit = {
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
                val stringValue = value.toString;
                try {
                  currentObject.put(currentKey, stringValue.toLong)
                }catch {
                  case _:NumberFormatException =>
                    currentObject.put(currentKey, new BigInteger(stringValue))
                }
              } catch {
                case e:Exception => currentObject.put(currentKey, value);
              }
            } else {
              currentObject.put(currentKey, null);
            }
            currentKey = null
          }
        }
      case JsonEventType.END_OBJECT =>
        if (currentObject != null) {
          subscriber.onNext(currentObject)
          currentObject = null
          currentKey = null
        }
      case JsonEventType.START_ARRAY =>
      case JsonEventType.END_ARRAY =>
      case _ =>
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