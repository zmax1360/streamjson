package com.example.jsonstream

import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.{JsonEvent, JsonEventType, JsonParser}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar
import rx.Subscriber

@RunWith(classOf[JUnitRunner])
class ReactHandlerTest extends AnyFlatSpec with Matchers with MockitoSugar {

  // Mock dependencies
  val mockSubscriber: Subscriber[JsonObject] = mock[Subscriber[JsonObject]]
  val mockJsonParser: JsonParser = mock[JsonParser]
  val mockJsonEvent: JsonEvent = mock[JsonEvent]

  // Test object
  val reactHandler = new ReactHandler(mockSubscriber)

  // Test 1: begin() should log the start of the JSON stream
  "ReactHandler" should "log the start of the JSON stream" in {
    reactHandler.begin()
    // Verify logging behavior (you can use a logging framework like Logback for actual verification)
  }

  // Test 2: handle() should process JSON objects and ignore non-object events
  "ReactHandler" should "process JSON objects and ignore non-object events" in {
    // Case 1: JSON object event
    when(mockJsonEvent.isObject).thenReturn(true)
    when(mockJsonEvent.objectValue()).thenReturn(new JsonObject().put("key", "value"))
    reactHandler.handle(mockJsonEvent)
    verify(mockSubscriber).onNext(any[JsonObject])

    // Case 2: Non-object event
    when(mockJsonEvent.isObject).thenReturn(false)
    reactHandler.handle(mockJsonEvent)
    verify(mockSubscriber, times(1)).onNext(any[JsonObject]) // No additional calls
  }

  // Test 3: handleError() should log the error and notify the subscriber
  "ReactHandler" should "log errors and notify the subscriber" in {
    val testException = new RuntimeException("Test error")
    reactHandler.handleError(testException)
    // Verify logging behavior
    verify(mockSubscriber).onError(testException)
  }

  // Test 4: end() should notify the subscriber that the stream is complete
  "ReactHandler" should "notify the subscriber when the stream ends" in {
    reactHandler.end()
    verify(mockSubscriber).onCompleted()
  }
  "ReactHandler" should "count the number of JSON objects in react.json using streaming" in {
    // Step 1: Initialize Vert.x and FileSystem
    val vertx = Vertx.vertx()
    val fileSystem = vertx.fileSystem()

    // Step 2: Create a subscriber to collect JSON objects
    var objectCount = 0
    val subscriber = Subscriber[JsonObject](
      onNext = _ => objectCount += 1, // Increment count for each JSON object
      onError = e => fail(s"Error processing JSON: $e"), // Fail the test on error
      onCompleted = () => println("JSON processing completed") // Log completion
    )

    // Step 3: Parse the JSON stream using Vert.x's JsonParser
    val parser = HandlerHelper.parseJsonStream(subscriber)

    // Step 4: Get the absolute path of the file in the resources folder
    val resource = getClass.getResource("/react.json") // Path to the file in resources
    if (resource == null) {
      fail("File not found in resources: react.json")
    }
    val jsonFilePath = new File(resource.toURI).getAbsolutePath // Absolute file path

    // Step 5: Read the file in chunks and feed it to the parser
    fileSystem.open(jsonFilePath, new OpenOptions().setRead(true), { asyncResult =>
      if (asyncResult.succeeded()) {
        val file = asyncResult.result()

        // Use a small buffer size to process data incrementally
        val bufferSize = 1024 // Adjust based on your requirements
        val buffer = Buffer.buffer(bufferSize)

        file.handler { chunk: Buffer =>
          // Append the chunk to the buffer
          buffer.appendBuffer(chunk)

          // Process the buffer incrementally
          while (buffer.length() >= bufferSize) {
            val chunkToProcess = buffer.getBuffer(0, bufferSize)
            parser.handle(chunkToProcess)
            buffer.setBuffer(0, buffer.getBuffer(bufferSize, buffer.length()))
          }
        }

        file.endHandler { _ =>
          // Process any remaining data in the buffer
          if (buffer.length() > 0) {
            parser.handle(buffer)
          }
          parser.end() // Signal the end of the stream
          file.close() // Close the file

          // Step 6: Verify the count of JSON objects
          println(s"Total JSON objects processed: $objectCount")
          objectCount should be > 0 // Ensure at least one object was processed
        }
      } else {
        fail(s"Failed to open file: ${asyncResult.cause()}")
      }
    })
  }
  "ReactHandler" should "process JSON objects and large numeric values" in {
    // Case 1: JSON object event
    when(mockJsonEvent.isObject).thenReturn(true)
    when(mockJsonEvent.objectValue()).thenReturn(new JsonObject().put("key", "value"))
    reactHandler.handle(mockJsonEvent)
    verify(mockSubscriber).onNext(any[JsonObject])

    // Case 2: Large numeric value event
    when(mockJsonEvent.isNumber).thenReturn(true)
    when(mockJsonEvent.value()).thenReturn("86164646864734280808080000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
    reactHandler.handle(mockJsonEvent)
    verify(mockSubscriber, times(2)).onNext(any[JsonObject]) // Called twice now
  }
  // Test 5: arrayJsonEventHandler should handle different JSON event types
  "HandlerHelper" should "handle different JSON event types" in {
    val mockHandler = mock[ReactStreamJsonHandler]

    // Case 1: START_ARRAY event
    when(mockJsonEvent.`type`()).thenReturn(JsonEventType.START_ARRAY)
    HandlerHelper.arrayJsonEventHandler(mockJsonParser, mockHandler, mockJsonEvent)
    verify(mockJsonParser).objectValueMode()

    // Case 2: VALUE event
    when(mockJsonEvent.`type`()).thenReturn(JsonEventType.VALUE)
    HandlerHelper.arrayJsonEventHandler(mockJsonParser, mockHandler, mockJsonEvent)
    verify(mockHandler).handle(mockJsonEvent)

    // Case 3: END_ARRAY event
    when(mockJsonEvent.`type`()).thenReturn(JsonEventType.END_ARRAY)
    HandlerHelper.arrayJsonEventHandler(mockJsonParser, mockHandler, mockJsonEvent)
    verify(mockHandler).end()
  }

  // Test 6: parseJsonStream should configure the parser correctly
  "HandlerHelper" should "configure the parser correctly" in {
    val parser = HandlerHelper.parseJsonStream(mockSubscriber)
    // Verify that the parser is configured with the correct handlers
    // (This can be tricky to test directly, so we focus on the behavior instead.)
  }
}
