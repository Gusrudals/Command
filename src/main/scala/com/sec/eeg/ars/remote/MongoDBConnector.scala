package com.sec.eeg.ars.remote

import com.mongodb.BasicDBObject
import com.sec.eeg.ars.data.ServiceConfig.database
import scala.collection.JavaConverters._

object MongoDBConnector {

  def isEquipTriggerEnable(eqpId: String) : Boolean = {
    val eqpInfoCollection = database.getCollection("EQP_INFO")
    val search = new BasicDBObject.append("eqpId", eqpId)
    val response = eqpInfoCollection.find(search).asScala
    if (!response.inEmpty){
      response.head.getBoolean("enable")
    }
    else{
      false
    }
  }
}