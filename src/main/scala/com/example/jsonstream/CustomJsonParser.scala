package com.example.jsonstream

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.{JsonEvent, JsonEventType}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import rx.lang.scala.Subscriber

import java.math.BigInteger

class CustomJsonParser(subscriber: Subscriber[JsonObject]) {
  private val jsonFactory = new JsonFactory()
  private val mapper = new ObjectMapper()

  def parse(buffer: Buffer): Unit = {
    val parser = jsonFactory.createParser(buffer.getBytes)

    while (parser.nextToken() != null) {
      parser.currentToken() match {
        case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
          // Handle large numbers
          val numericValue = parser.getText
          try {
            val bigIntValue = new BigInteger(numericValue)
            val jsonObject = new JsonObject().put("value", bigIntValue.toString)
            subscriber.onNext(jsonObject)
          } catch {
            case e: NumberFormatException =>
              subscriber.onError(e)
          }
        case JsonToken.START_OBJECT =>
          // Handle JSON objects
          val jsonObject = mapper.readValue[JsonObject](parser, classOf[JsonObject])
          subscriber.onNext(jsonObject)
        case _ =>
        // Ignore other tokens
      }
    }
  }
}
