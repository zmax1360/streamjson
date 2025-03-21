package com.example.jsonstream

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.buffer.Buffer

def parseWithJackson(buffer: Buffer, handler: ReactStreamJsonHandler): Unit = {
  val jsonFactory = new JsonFactory()
  val parser = jsonFactory.createParser(buffer.getBytes)
  val mapper = new ObjectMapper()

  while (parser.nextToken() != null) {
    parser.currentToken() match {
      case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
        // Handle large numbers
        val numericValue = parser.getText
        try {
          val bigIntValue = new BigInteger(numericValue)
          val jsonObject = new JsonObject().put("value", bigIntValue.toString)
          handler.handle(new JsonEvent(jsonObject, JsonEventType.VALUE))
        } catch {
          case e: NumberFormatException =>
            handler.handleError(e)
        }
      case JsonToken.START_OBJECT =>
        // Handle JSON objects
        val jsonObject = mapper.readValue[JsonObject](parser, classOf[JsonObject])
        handler.handle(new JsonEvent(jsonObject, JsonEventType.VALUE))
      case _ =>
      // Ignore other tokens
    }
  }
}
