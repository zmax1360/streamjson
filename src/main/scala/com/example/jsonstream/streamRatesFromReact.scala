package com.example.jsonstream

import io.reactivex.Observable
import io.vertx.core.json.JsonParser
import io.vertx.core.buffer.Buffer

def streamRatesFromReact: Observable[MessageEnvelop[SurfaceMessage]] = {
  Observable.create[MessageEnvelop[SurfaceMessage]] { subscribe =>
    client.request().onComplete { reqAr =>
      if (reqAr.succeeded()) {
        val request = reqAr.result()
        request.send().onComplete { respAsr =>
          if (respAsr.succeeded()) {
            val response = respAsr.result()
            if (response.statusCode() == 200) {
              val parser = JsonParser.newParser()
              val handler = new ReactApiJsonHandler(subscribe, adaptor) // Use your custom handler
              parser.handler(handler)
              parser.exceptionHandler(handler.handleError)
              parser.endHandler(handler.end)

              response.handler { buffer: Buffer =>
                parser.handle(buffer)
              }
            } else {
              subscribe.onError(new RuntimeException(response.statusMessage()))
            }
          } else {
            subscribe.onError(respAsr.cause())
          }
        }
      } else {
        subscribe.onError(reqAr.cause())
      }
    }
  }.onErrorReturn(e => {
    logger.info("error", e)
    new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error"), Some(e.getMessage), None, None)
  })
}
def bootstrapReactApi: Observable[MessageEnvelop[SurfaceMessage]] = {
  streamRatesFromReact.observeOn(reactEndpointTabLeLoaderScheduler)
}
