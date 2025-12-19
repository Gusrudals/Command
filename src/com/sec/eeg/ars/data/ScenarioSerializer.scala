package com.sec.ee.ars.data

import org.json4s.JsonAST._
import org.json4s.ext.EnumSerializer
import org.json4s.{CustomSerializer, DefaultFormats}



class OcrParamSerializer extends CustomSerializer[OcrParam](format => (
  {
    case JObject(JField("Type", JInt(_Type))::JField("Operation", JInt(_Operation))::JField("TextValue", JString(_TextValue))::JField("NumberValue", JDouble(_NumberValue))
      ::JField("NumberRangeMin", JDouble(_NumberRangeMin))::JField("NumberRangeMax", JDouble(_NumberRangeMax))::JField("MinMatchingValue", JInt(_MinMatchingValue))::Nil)
    =>
      implicit val formats = DefaultFormats + new EnumSerializer(eOcrValueType) + new EnumSerializer(eOcrOperation)
      new OcrParam(eOcrValueType(_Type.toInt),eOcrOperation(_Operation.toInt),_TextValue,_NumberValue,_NumberRangeMin,_NumberRangeMax,_MinMatchingValue.toInt)
  },{
  case _ => JNull
}))

class ScreenSearchAreaSerializer extends CustomSerializer[ScreenSearchArea](format => (
  {
    case JObject(JField("X", JInt(_X))::JField("Y", JInt(_Y))::JField("Width", JInt(_Width))::JField("Height", JInt(_Height))::Nil)
    => new ScreenSearchArea(_X.toInt,_Y.toInt,_Width.toInt,_Height.toInt)
  },{
  case _ => JNull
}))

class TrainImageSerializer extends CustomSerializer[TrainImage](format => (
  {
    case JObject(JField("BinaryImage", JString(_BinaryImage))::JField("Type", JBool(_Type))::Nil)
    => new TrainImage(_BinaryImage.getBytes, _Type)
  },{
  case _ => JNull
}))

class ImageSearchingParamSerializer extends CustomSerializer[ImageSearchingParam](format => (
  {
    case JObject(JField("TrainImageMap", JObject(_TrainImageMap))::JField("AcceptThreshold", JDouble(_AcceptThreshold))::JField("MaximumNumberToFind", JInt(_MaximumNumberToFind))
      ::JField("TimeoutEnabled", JBool(_TimeoutEnabled))::JField("Timeout", JDouble(_Timeout))::Nil)
    =>
      implicit val formats = DefaultFormats + new TrainImageSerializer
      val ___TrainImageMap = JObject(_TrainImageMap).extract[Map[Int,TrainImage]]
      new ImageSearchingParam(___TrainImageMap, _AcceptThreshold, _MaximumNumberToFind.toInt, _TimeoutEnabled, _Timeout)
  },{
  case _ => JNull
}
))

class MetadataSerializer extends CustomSerializer[ScenarioMetadata](format => (
  {
    case JObject(
    JField("SearchArea", JObject(_SearchArea))::
      JField("ImageSearchParam", JObject(_ImageSearchParam))::
      JField("ConditionExpression", JString(_ConditionExpression))::
      JField("OcrParamInfo", JObject(_OcrParam))::
      Nil)

    => implicit val formats = DefaultFormats + new ImageSearchingParamSerializer + new ScreenSearchAreaSerializer + new OcrParamSerializer
      val __SearchArea = JObject(_SearchArea).extract[ScreenSearchArea]
      val __ImageSearchParam = JObject(_ImageSearchParam).extract[ImageSearchingParam]
      val __OcrParam = JObject(_OcrParam).extract[OcrParam]

      new ScenarioMetadata(__SearchArea, __ImageSearchParam, _ConditionExpression, __OcrParam)
  },{
  case _ => JNull
}
))

class NativeFunctionSerializer extends CustomSerializer[NativeFunction](format => (
  {
    case JObject(JField("FunctionName", JString(_FunctionName))::JField("Params", JArray(_Params))::JField("Outputs", JArray(_Output))::Nil)
    => val Params = _Params.map{
      case JString(value) => value
      case x => ""
    }.toArray
      val Outputs = _Output.map{
        case JString(value) => value
        case x => ""
      }.toArray
      new NativeFunction(_FunctionName,Params,Outputs)
  }, {
  case _ => JNull
}
))

class ScenarioScriptAreaSerializer extends CustomSerializer[ScenarioScript](format => (
  {
    case x: JObject =>
      implicit val _format = DefaultFormats + new NativeFunctionSerializer
      val _Type = (x \ "Type").extract[Int]
      val _Value = (x \ "Value").extract[String]
      val _NativeFunctionOpt = (x \ "NativeFunction").extractOpt[NativeFunction]
      new ScenarioScript(eScenarioScript(_Type.toInt),_Value,_NativeFunctionOpt)
  },{
  case _ => JNull
}))

class ScenarioStepSerializer extends CustomSerializer[ScenarioStep](format => (
  {
    case JObject(
    JField("Operation", JInt(_Operation))::
      JField("Metadata", JObject(_Metadata))::
      JField("Script", JObject(_Script))::
      JField("RetryInterval", JInt(_RetryInterval))::
      JField("RetryLimit", JInt(_RetryLimit))::
      JField("NextStepIfTrue", JInt(_NextStepIfTrue))::
      JField("NextStepIfFalse", JInt(_NextStepIfFalse))::
      JField("DescriptionToEmail", JString(_DescriptionToEmail))::
      JField("DescriptionToDialog", JString(_DescriptionToDialog))::
      JField("IsShowDialog", JBool(_IsShowDialog))::
      Nil)

    => implicit val formats = DefaultFormats + new MetadataSerializer + new ScenarioScriptAreaSerializer
      val __Metadata = JObject(_Metadata).extract[ScenarioMetadata]
      val __ScenarioScript = JObject(_Script).extract[ScenarioScript]

      new ScenarioStep(eScenarioOperation(_Operation.toInt),__Metadata,__ScenarioScript,_RetryInterval.toInt,_RetryLimit.toInt,_NextStepIfTrue.toInt,_NextStepIfFalse.toInt,
        _DescriptionToEmail,_DescriptionToDialog,_IsShowDialog)
  },{
  case _ => JNull
}
))