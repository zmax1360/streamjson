package com.example.jsonstream

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import rx.lang.scala.Subscriber

import java.io._
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import scala.concurrent.ExecutionContext

object JacksonStreamingParser {
  private val jsonFactory = new JsonFactory()
  private val executor = Executors.newSingleThreadExecutor()
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  def parseJsonStream(subscriber: Subscriber[JsonObject]): Buffer => Unit = {
    val queue = new LinkedBlockingQueue[Buffer]()
    val pipeOut = new PipedOutputStream()
    val pipeIn = new PipedInputStream(pipeOut, 64 * 1024)

    // Start background parser thread
    executor.execute(() => {
      val parser = jsonFactory.createParser(pipeIn)
      val mapper = io.vertx.core.json.jackson.DatabindCodec.mapper()

      try {
        if (parser.nextToken() != JsonToken.START_ARRAY)
          throw new IllegalArgumentException("Expected JSON array")

        while (parser.nextToken() != JsonToken.END_ARRAY) {
          val node = mapper.readTree(parser)
          val jsonObj = new JsonObject(mapper.writeValueAsString(node))
          subscriber.onNext(jsonObj)
        }

        subscriber.onCompleted()
      } catch {
        case e: Throwable =>
          subscriber.onError(e)
      } finally {
        parser.close()
        pipeIn.close()
      }
    })

    // Return function to feed data chunks into the pipe
    (buffer: Buffer) => {
      pipeOut.write(buffer.getBytes)
      pipeOut.flush()
    }
  }
}

