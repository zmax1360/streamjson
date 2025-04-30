package com.example.jsonstream

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import io.vertx.core.json.JsonObject
import rx.lang.scala.Subscriber
import org.slf4j.LoggerFactory

object JacksonStreamHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jsonFactory = new JsonFactory()

  def parseArrayStream(json: String, subscriber: Subscriber[JsonObject]): Unit = {
    val parser = jsonFactory.createParser(json)

    if (parser.nextToken() != JsonToken.START_ARRAY) {
      subscriber.onError(new IllegalArgumentException("Expected a JSON array"))
      return
    }

    try {
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        // Jackson reads full object, supports BigInteger, BigDecimal
        val node = io.vertx.core.json.jackson.DatabindCodec.mapper().readTree(parser)
        val jsonObject = new JsonObject(node.toString)
        subscriber.onNext(jsonObject)
      }
      subscriber.onCompleted()
    } catch {
      case e: Throwable =>
        logger.error("Error during Jackson stream parsing", e)
        subscriber.onError(e)
    } finally {
      parser.close()
    }
  }
}

