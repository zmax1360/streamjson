import rx.lang.scala.Observable
import rx.lang.scala.Subscriber
import org.json.JSONObject
import scala.util.{Success, Try}
import scala.collection.mutable.Queue

def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
  Observable.create { subscriber =>
    val builder = new StringBuilder()
    var errorOccured = false
    var inJsonObject = false
    var currentJsonObject = new StringBuilder()
    val messageQueue = Queue[MessageEnvelop[SurfaceMessage]]() // Queue to buffer messages

    streamRatesFromReact.subscribe(new Subscriber[String] {
      override def onNext(next: String): Unit = {
        if (next == REACT_REST_API_EXCEPTION) {
          errorOccured = true
          subscriber.onNext(new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error")))
          subscriber.onCompleted()
        } else {
          builder.append(next)
          var index = 0
          while (index < builder.length()) {
            if (builder.substring(index).startsWith(APPEND_JSON_START)) {
              inJsonObject = true
              currentJsonObject.append(APPEND_JSON_START)
              index += APPEND_JSON_START.length()
            } else if (inJsonObject && builder.substring(index).startsWith(APPEND_JSON_END)) {
              currentJsonObject.append(APPEND_JSON_END)
              inJsonObject = false
              val jsonString = currentJsonObject.toString()
              currentJsonObject.clear()

              Observable.just(jsonString)
                .observeOn(reactEndpointTabLeLoaderScheduler)
                .map(ss => ("CreditRiskJobDetails.COMPLETED".toString, ss))
                .observeOn(reactEndpointTabLeLoaderScheduler)
                .map(b => {
                  Try(adaptor.adapt(Array(new JSONObject(b._2))).map(m => new MessageEnvelop[SurfaceMessage](true, Some(m), None, Some(b._1), None, None, None)).get) match {
                    case Success(messageEnvelop) => messageEnvelop
                    case failure => new MessageEnvelop[SurfaceMessage](false, None, None, Some(b._1))
                  }
                }).subscribe(message => {
                  messageQueue.enqueue(message)
                })
              index += APPEND_JSON_END.length()
            } else if (inJsonObject) {
              currentJsonObject.append(builder.charAt(index))
              index += 1
            } else {
              index += 1
            }
          }
          builder.delete(0, index)
          if (next == REACT_BOOTSTRAP_API_RESPONSE_COMPLETED) {
            messageQueue.foreach(m => subscriber.onNext(m))
            subscriber.onCompleted()
          }
        }
      }

      override def onError(e: Throwable): Unit = {
        subscriber.onError(e)
      }

      override def onCompleted(): Unit = {} // Nothing to do on stream completion
    })
  }
}