package com.example.jsonstream

import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonEventType
import io.vertx.core.json.JsonParser

class ReactApiJsonHandler(subscribe: Observer[MessageEnvelop[SurfaceMessage]], adaptor: Adaptor) extends StreamingJsonHandler {

  private var currentObject: JsonObject = null
  private var currentArray: JsonArray = null
  private var inArray: Boolean = false

  override def handle(event: JsonEvent): Unit = {
    event.eventType match {
      case JsonEventType.START_OBJECT =>
        currentObject = new JsonObject()
      case JsonEventType.END_OBJECT =>
        if (inArray) {
          currentArray.add(currentObject)
        } else {
          processJsonObject(currentObject)
        }
        currentObject = null
      case JsonEventType.FIELD_NAME =>
        currentObject.put(event.fieldName, null) // Placeholder, value will be set in VALUE event
      case JsonEventType.VALUE =>
        if (currentObject != null) {
          currentObject.put(event.fieldName, event.value)
        }
      case JsonEventType.START_ARRAY =>
        currentArray = new JsonArray()
        inArray = true
      case JsonEventType.END_ARRAY =>
        inArray = false
        processJsonArray(currentArray)
        currentArray = null
      case _ => // Handle other events if needed
    }
  }

  private def processJsonObject(jsonObject: JsonObject): Unit = {
    Try(adaptor.adapt(Array(jsonObject)).map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some("CreditRiskJobDetails.COMPLETED"), None, None, None)).get) match {
      case Success(messageEnvelop) => subscribe.onNext(messageEnvelop)
      case Failure(ex) => subscribe.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error"), Some(ex.getMessage), None, None))
    }
  }

  private def processJsonArray(jsonArray: JsonArray): Unit = {
    Try(adaptor.adapt(jsonArray.getList.toArray).map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some("CreditRiskJobDetails.COMPLETED"), None, None, None)).get) match {
      case Success(messageEnvelop) => subscribe.onNext(messageEnvelop)
      case Failure(ex) => subscribe.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error"), Some(ex.getMessage), None, None))
    }
  }

  override def handleError(event: JsonEvent): Unit = {
    subscribe.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error"), Some(event.getMessage), None, None))
  }

  override def end(): Unit = {
    subscribe.onCompleted()
  }

  override def begin(): Unit = {} // No specific action needed at the beginning
}
