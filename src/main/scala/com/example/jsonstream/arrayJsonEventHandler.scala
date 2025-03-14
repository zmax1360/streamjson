package com.example.jsonstream

import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}

def arrayJsonEventHandler(p: JsonParser, handler: StreamJsonHandler)(event: JsonEvent): Unit = {
  event.eventType() match {
    case JsonEventType.START_ARRAY =>
      p.objectValueMode()
    case JsonEventType.VALUE.INSTANCE => // Corrected line
      handler.handle(event) // Pass the event, not 'o'.
    case JsonEventType.END_ARRAY.INSTANCE => // added end array handling
      handler.end()
    case _ =>
    // Handle other event types if needed
  }
}
