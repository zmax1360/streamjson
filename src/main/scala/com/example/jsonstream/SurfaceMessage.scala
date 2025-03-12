
package com.example.jsonstream

import org.json.JSONObject
import scala.util.{Failure, Success, Try}



case class MessageEnvelop[T](isDate:Boolean, date:Option[T], Option[String] = None, message = option[String]= None, snapName:Option[String]=None,procutType: Option[String]= None, falconIngestionTime: Option[org.joda.time.DateTime] =None)
case class SurfaceMessage(j: JSONObject) extends JWrapper(j){
  lazy val surfaceResult: Option[RiskDto] = if (j.has("surfaceResult")) Some(RiskDto(jsonObject("surfaceResult"))) else None
 // lazy val errors: Option[SurfaceError] = if (j.has("error")) Some(SurfaceError(jsonObject("errors"))) else None
}


//case class SurfaceError(j: JSONObject){
//  def raw():List[JSONObject] = if (j.has("raw")) JavaConverters.iterableAsScalaIterable(j.getJSONArray("raw")).map(_.toString).toList
//}
case class RiskDto(j: JSONObject) extends JWrapper(j){
  def ccy():Option[String] = string("ccy")
}
class JWrapper(j:JSONObject) {
  def anyRef(name: String): Option[AnyRef] = if (j.has(name)) Some(j.get(name)) else None
  def jsonObject(name: String): JSONObject = j.optJSONObject(name)
  def string(name: String, convertToStringIfBoolean: Boolean = false): Option[String] =
    if(j.has(name)){
      if(j.isNull(name)){
        None
      }
      else {
        Try(j.getString(name)) match {
          case Failure(exception) =>
            if(exception.getMessage.startsWith(s""""JSONOBJECT["$name"] is not a string""") && convertToStringIfBoolean)
              Some(j.getBoolean(name).toString)
            else
              throw exception
          case Success(value)  => Some(value)
        }
      }
    } else None
}