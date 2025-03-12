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
import scala.util.{Try, Success, Failure}

object JsonChunkProcessor {
  sealed trait JsonProcessingState
  case object IDLE extends JsonProcessingState
  case object IN_JSON extends JsonProcessingState
  case object ERROR extends JsonProcessingState
}

class JsonChunkProcessor(messageConsumer: Consumer[MessageEnvelop[String]], adapter: SurfaceMessageAdapter) {
  import JsonChunkProcessor._

  private var state: JsonProcessingState = IDLE
  private val currentJsonObject = new StringBuilder()

  private val APPEND_JSON_START = "{\"surfaceResult\":{"
  private val APPEND_JSON_END = "}"
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"
  private val COMPLETED_MESSAGE_TYPE = "CreditRiskJobDetails.COMPLETED"

  def processChunk(chunk: String): CompletableFuture[Void] = {
    CompletableFuture.runAsync(() => {
      if (chunk == REACT_REST_API_EXCEPTION) {
        state = ERROR
        messageConsumer.accept(MessageEnvelop(false, null, null, "Error"))
        return
      }

      val builder = new StringBuilder(chunk)
      var index = 0

      while (index < builder.length()) {
        if (builder.substring(index).startsWith(APPEND_JSON_START)) {
          state = IN_JSON
          currentJsonObject.append(APPEND_JSON_START)
          index += APPEND_JSON_START.length
        } else if (state == IN_JSON && builder.substring(index).startsWith(APPEND_JSON_END)) {
          currentJsonObject.append(APPEND_JSON_END)
          state = IDLE
          val jsonString = currentJsonObject.toString()
          currentJsonObject.setLength(0) // Clear

          Try {
            val jsonObject = new JSONObject(jsonString)
            val adapted = adapter.adapt(Array(jsonObject))(0).toString
            MessageEnvelop(true, adapted, null, COMPLETED_MESSAGE_TYPE)
          } match {
            case Success(result) => messageConsumer.accept(result)
            case Failure(_) => messageConsumer.accept(MessageEnvelop(false, null, null, COMPLETED_MESSAGE_TYPE))
          }

          index += APPEND_JSON_END.length
        } else if (state == IN_JSON) {
          currentJsonObject.append(builder.charAt(index))
          index += 1
        } else {
          index += 1
        }
      }
    })
  }
}

case class MessageEnvelop[T](success: Boolean, data: T, error: Any, messageType: String)

class SurfaceMessageAdapter {
  def adapt(input: Array[JSONObject]): Array[JSONObject] = {
    input
  }
}