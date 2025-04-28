package com.example.jsonstream

import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import rx.lang.scala.Subscriber

import java.nio.file.{Files, Paths}
import scala.io.Source

class ReactStreamJsonHandlerTest extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  private var subscriber: Subscriber[JsonObject] = _
  private var reactHandler: ReactHandler = _
  private var jsonParser: JsonParser = _
  private var mockEvent: JsonEvent = _

  override def beforeEach(): Unit = {
    subscriber = mock[Subscriber[JsonObject]]
    reactHandler = new ReactHandler(subscriber)
    jsonParser = JsonParser.newParser()
    mockEvent = mock[JsonEvent]
  }

  describe("ReactHandler") {
    it("should call begin() and log stream start") {
      reactHandler.begin()
      // In a real test, you would verify logging using a test appender
    }

    it("should handle JSON object events and increment count") {
      val jsonObj = new JsonObject().put("test", "value")
      when(mockEvent.isObject).thenReturn(true)
      when(mockEvent.objectValue()).thenReturn(jsonObj)

      reactHandler.handle(mockEvent)

      verify(subscriber).onNext(jsonObj)
    }

    it("should handle errors by calling subscriber.onError") {
      val testError = new RuntimeException("test error")
      reactHandler.handleError(testError)
      verify(subscriber).onError(testError)
    }

    it("should call end() and complete the subscriber") {
      reactHandler.end()
      verify(subscriber).onCompleted()
    }
  }

  describe("HandlerHelper") {
    it("should parse a JSON array stream correctly") {
      // Load test JSON file from resources
      val jsonString = Source.fromResource("test_array.json").mkString
      val jsonArray = new JsonArray(jsonString)

      val parser = HandlerHelper.parseJsonStream(subscriber)
      parser.handle(jsonString)

      // Verify subscriber was called for each object in array
      jsonArray.forEach { item =>
        verify(subscriber).onNext(item.asInstanceOf[JsonObject])
      }
      verify(subscriber).onCompleted()
    }

    it("should handle START_ARRAY event by switching to object value mode") {
      when(mockEvent.`type`()).thenReturn(JsonEventType.START_ARRAY)
      HandlerHelper.arrayJsonEventHandler(jsonParser, reactHandler, mockEvent)
      // In a real test, you might verify parser mode was changed
    }

    it("should handle VALUE event by delegating to handler") {
      val jsonObj = new JsonObject().put("key", "value")
      when(mockEvent.`type`()).thenReturn(JsonEventType.VALUE)
      when(mockEvent.isObject).thenReturn(true)
      when(mockEvent.objectValue()).thenReturn(jsonObj)

      HandlerHelper.arrayJsonEventHandler(jsonParser, reactHandler, mockEvent)
      verify(reactHandler).handle(mockEvent)
    }

    it("should handle END_ARRAY event by calling handler.end()") {
      when(mockEvent.`type`()).thenReturn(JsonEventType.END_ARRAY)
      HandlerHelper.arrayJsonEventHandler(jsonParser, reactHandler, mockEvent)
      verify(reactHandler).end()
    }

    it("should log warning for unhandled event types") {
      when(mockEvent.`type`()).thenReturn(JsonEventType.KEY_NAME)
      HandlerHelper.arrayJsonEventHandler(jsonParser, reactHandler, mockEvent)
      // Verify warning was logged (would need a logging test appender)
    }
  }

  describe("JSON File Processing") {
    it("should process all JSON objects in a file") {
      // Create test JSON with known number of objects
      val testJson = """
                       |[
                       |  {"id": 1, "name": "first"},
                       |  {"id": 2, "name": "second"},
                       |  {"id": 3, "name": "third"}
                       |]
                       |""".stripMargin

      val tempFile = Files.createTempFile("test", ".json")
      Files.write(tempFile, testJson.getBytes)

      try {
        val parser = HandlerHelper.parseJsonStream(subscriber)
        parser.handle(Files.readString(tempFile))

        verify(subscriber, times(3)).onNext(any[JsonObject])
        verify(subscriber).onCompleted()
      } finally {
        Files.deleteIfExists(tempFile)
      }
    }

    it("should handle empty JSON array") {
      val testJson = "[]"
      val parser = HandlerHelper.parseJsonStream(subscriber)
      parser.handle(testJson)

      verify(subscriber, never).onNext(any[JsonObject])
      verify(subscriber).onCompleted()
    }

    it("should handle malformed JSON with error") {
      val testJson = "{ malformed }"
      val parser = HandlerHelper.parseJsonStream(subscriber)
      parser.handle(testJson)

      verify(subscriber).onError(any[Throwable])
    }
  }
}
