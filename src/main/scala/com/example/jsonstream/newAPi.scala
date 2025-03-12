import rx.lang.scala.Observable
import rx.lang.scala.Subscriber
import org.json.JSONObject
import scala.util.{Success, Try}
import java.util.function.Consumer
import java.util.concurrent.CompletableFuture

// Assuming you have your SurfaceMessageAdapter and MessageEnvelop classes defined
// Assuming you have your streamRatesFromReact Observable defined

def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
  Observable.create { subscriber =>
    val adapter = new SurfaceMessageAdapter() // Instantiate your adapter
    val consumer: Consumer[MessageEnvelop[String]] = message => {
      // Adapt the String MessageEnvelop to a SurfaceMessage MessageEnvelop
      if (message.success) {
        Try(adapter.adapt(Array(new JSONObject(message.data))).map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some(message.messageType), None, None, None)).get) match {
          case Success(messageEnvelop) => subscriber.onNext(messageEnvelop)
          case _ => subscriber.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some(message.messageType)))
        }
      } else {
        subscriber.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some(message.messageType)))
      }
    }

    val processor = new JsonChunkProcessor(consumer, adapter)

    streamRatesFromReact.subscribe(new Subscriber[String] {
      override def onNext(chunk: String): Unit = {
        processor.processChunk(chunk).join() // Process each chunk
        if(chunk == "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"){
          subscriber.onCompleted()
        }
      }

      override def onError(e: Throwable): Unit = {
        subscriber.onError(e)
      }

      override def onCompleted(): Unit = {}
    })
  }
}

import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import org.json.JSONObject
import scala.util.Try
import scala.util.Success


object JsonProcessingState extends Enumeration {
  type JsonProcessingState = Value
  val IDLE, IN_JSON, ERROR = Value
}

class JsonChunkProcessor(private var messageConsumer: Consumer[MessageEnvelop[String]], private var adapter: SurfaceMessageAdapter) {
  private var state = JsonProcessingState.IDLE
  private val currentJsonObject = new lang.StringBuilder
  final private val APPEND_JSON_START = "{\"surfaceResult\":{"
  final private val APPEND_JSON_END = "}"
  final private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"
  final private val COMPLETED_MESSAGE_TYPE = "CreditRiskJobDetails.COMPLETED"

  def processChunk(chunk: String): CompletableFuture[Void] = CompletableFuture.runAsync(() => {
    def foo() = {
      if (chunk == REACT_REST_API_EXCEPTION) {
        state = JsonProcessingState.ERROR
        messageConsumer.accept(new MessageEnvelop[String](false, null, null, "Error"))
        return
      }
      val builder = new lang.StringBuilder(chunk)
      var index = 0
      while (index < builder.length) if (builder.substring(index).startsWith(APPEND_JSON_START)) {
        state = JsonProcessingState.IN_JSON
        currentJsonObject.append(APPEND_JSON_START)
        index += APPEND_JSON_START.length
      }
      else if ((state eq JsonProcessingState.IN_JSON) && builder.substring(index).startsWith(APPEND_JSON_END)) {
        currentJsonObject.append(APPEND_JSON_END)
        state = JsonProcessingState.IDLE
        val jsonString = currentJsonObject.toString
        currentJsonObject.setLength(0) // Clear

        val tryResult = Try.apply(() => {
          val jsonObject = new JSONObject(jsonString)
          val adapted = adapter.adapt(Array[JSONObject](jsonObject))(0).toString
          new MessageEnvelop[String](true, adapted, null, COMPLETED_MESSAGE_TYPE)

        })
        if (tryResult.isSuccess) messageConsumer.accept(tryResult.get)
        else messageConsumer.accept(new MessageEnvelop[String](false, null, null, COMPLETED_MESSAGE_TYPE))
        index += APPEND_JSON_END.length
      }
      else if (state eq JsonProcessingState.IN_JSON) {
        currentJsonObject.append(builder.charAt(index))
        index += 1
      }
      else index += 1
    }

    foo()
  })
}

class MessageEnvelop[T](var success: Boolean, var data: T, var error: AnyRef, var messageType: String) {
}

class SurfaceMessageAdapter {
  def adapt(input: Array[JSONObject]): Array[JSONObject] = input
}