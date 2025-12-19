package com.sec.eeg.ars.data

import java.io.{InputStream, InputStreamReader}
import java.util.{Locale, PropertyResourceBundle, ResourceBundle}

class UTF8Control extends ResourceBundle.Control {

  override def newBundle(baseName: String, locale: Locale, formatf: String, loader: ClassLoader, reload: Boolean) : ResourceBundle = {
    // The below is a copy of the default implementation.
    val bundleName = toBundleName(baseName, locale)
    val resourceName = toResourceName(bundleName, "properties")
    var bundle: ResourceBundle = null
    var stream: InputStream = null
    if (reload) {
      val url = loader.getResource(resourceName)
      if (url != null) {
        val connection = url.openConnection()
        if (connection != null) {
          connection.setUseCaches(false)
          stream = connection.getInputStream()
        }
      }
    } else {
      stream = loader.getResourceAsStream(resourceName)
    }
    if (stream != null) {
      try {
        // Only this line is changed to make it to read properties files as UTF-8.
        bundle = new PropertyResourceBundle(new InputStreamReader(stream, "UTF-8"))
      } finally {
        stream.close()
      }
    }
    return bundle
  }
}

object MultiLangMgr {
  var bundle : ResourceBundle = null
  var alarmtext : String = ""
  var timespent : String = ""
  var view_history : String = ""
  var success : String = ""
  var stop : String = ""
  var fail : String = ""
  var scriptfail : String = ""
  var visiondelay : String = ""
  var min : String = ""
  var sec : String = ""
  var eqpid : String = ""
  var scname : String = ""
  var scdesc : String = ""
  var autorecovery_information : String = ""
  var scdetails : String = ""
  var trigger : String = ""
  var cron_expression : String = ""
  var matched_log : String = ""
  var ees_altxt : String = ""
  var prev_scenario : String = ""
  var status_info : String = ""
  var user_info : String = ""

  def init = {
    var curLocale = System.getProperty("custom.locale")
    if(curLocale == null || curLocale == ""){
      curLocale = "ko"
    }
    bundle = ResourceBundle.getBundle("ConstLable", new Locale(curLocale), new UTF8Control())

    alarmtext = bundle.getString("alarmtext")
    timespent = bundle.getString("timespent")
    view_history = bundle.getString("view_history")
    success = bundle.getString("success")
    stop = bundle.getString("stop")
    fail = bundle.getString("fail")
    scriptfail = bundle.getString("scriptfail")
    visiondelay = bundle.getString("visiondelay")
    min = bundle.getString("min")
    sec = bundle.getString("sec")
    autorecovery_information = bundle.getString("autorecovery_information")
    eqpid = bundle.getString("eqpid")
    scname = bundle.getString("scenario_name")
    scdesc = bundle.getString("scenario_desc")
    scdetails = bundle.getString("scenario_details")

    trigger = bundle.getString("trigger")
    cron_expression = bundle.getString("cron_expression")
    matched_log = bundle.getString("matched_log")
    ees_altxt = bundle.getString("ees_altxt")
    prev_scenario = bundle.getString("prev_scenario")
    status_info = bundle.getString("status_info")
    user_info = bundle.getString("user_info")
  }
}