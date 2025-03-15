package com.example.jsonstream

import io.vertx.core.json.JsonObject
import org.json.{JSONObject, JSONArray}
import scala.collection.JavaConverters._

def vertxToJson(vertxObject: JsonObject): JSONObject = {
  val json = new JSONObject()
  vertxObject.fieldNames().asScala.foreach { key =>
    val value = vertxObject.getValue(key)
    if (value == null) {
      json.put(key, JSONObject.NULL)
    } else if (value.isInstanceOf[JsonObject]) {
      json.put(key, vertxToJson(value.asInstanceOf[JsonObject]))
    } else if (value.isInstanceOf[java.util.List[_]]) {
      json.put(key, convertListToJsonArray(value.asInstanceOf[java.util.List[_]]))
    } else {
      json.put(key, value)
    }
  }
  json
}

def convertListToJsonArray(list: java.util.List[_]): JSONArray = {
  val array = new JSONArray()
  list.asScala.foreach { item =>
    if (item == null) {
      array.put(JSONObject.NULL)
    } else if (item.isInstanceOf[JsonObject]) {
      array.put(vertxToJson(item.asInstanceOf[JsonObject]))
    } else if (item.isInstanceOf[java.util.List[_]]) {
      array.put(convertListToJsonArray(item.asInstanceOf[java.util.List[_]]))
    } else {
      array.put(item)
    }
  }
  array
}
