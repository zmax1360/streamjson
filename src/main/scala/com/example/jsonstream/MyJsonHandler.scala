import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import io.vertx.core.json.JsonObject
import rx.Subscriber
import org.slf4j.LoggerFactory

import io.vertx.core.parsetools.JsonEvent

trait StreamJsonHandler {
  def handle(event: JsonEvent): Unit
  def handleError(e: Throwable): Unit
  def end(): Unit
}
class MyStreamJsonHandler(subscriber: Subscriber[JsonObject]) extends StreamJsonHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  private var currentObject: JsonObject = _
  private var currentFieldName: String = _
  private var insideArray = false

  override def handle(event: JsonEvent): Unit = {
    if (event.eventType() == JsonEventType.VALUE.INSTANCE) {
      if (event.stringValue() != null) {
        subscriber.onNext(new JsonObject().put("value", event.stringValue()))
      } else if (event.numberValue() != null) {
        subscriber.onNext(new JsonObject().put("value", event.numberValue()))
      } else if (event.booleanValue() != null) {
        subscriber.onNext(new JsonObject().put("value", event.booleanValue()))
      } else if (event.isNull()) {
        subscriber.onNext(new JsonObject().putNull("value"))
      } else {
        // Handle other value types or unexpected cases.
      }
    }
  }

  override def handleError(e: Throwable): Unit = {
    logger.error("Error in MyStreamJsonHandler", e)
    subscriber.onError(e)
  }

  override def end(): Unit = {
    logger.debug("Array processing completed.")
  }
}