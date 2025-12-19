package com.sec.eeg.ars.data

import com.sec.eeg.ars.data.eOcrOperation.eOcrOperation
import com.sec.eeg.ars.data.eOcrValueType.eOcrValueType
import com.sec.eeg.ars.data.eScenarioOperation.eScenarioOperation
import com.sec.eeg.ars.data.eScenarioScript.eScenarioScript
import com.sec.eeg.ars.data.eScenarioTriggerType.eScenarioTriggerType
import com.sec.eeg.ars.data.eScenarioTriggering.eScenarioTriggering


object eScenarioTriggerType extends Enumeration {
  type eScenarioTriggerType = Value
  val USER_DEFINED, EES, RTM = Value
}

object eScenarioTriggering extends Enumeration {
  type eScenarioTriggering = Value
  val Polling, FromUser, FromServer, TimeSchedule = Value
}

object eScenarioOperation extends Enumeration {
  type eScenarioOperation = Value
  val None, ImageSearching, OCR, Variable, EqpState = Value
}

object eScenarioScript extends Enumeration {
  type eScenarioScript = Value
  val None0,
  MouseLeftClick,
  MouseLeftDoubleClick,
  MouseRightClick,
  MouseMove,
  Keyboard,
  Wait,            //일정 시간동안 대기
  Variable,
  ScenarioEvent,
  FindWindow,         //특정 윈도우 검색하여 지정된 위치로 이동
  SendEmail,
  RunCommand,
  SsbMessage,          //SSB를 위한 RV 메시지 전송 요청
  ACSCall,
  CustomScript,
  NativeFunction = Value
}

object eOcrValueType extends Enumeration 
{
  type eOcrValueType = Value
  val None,
  Text,
  Number,
  Regex = Value
}

object eOcrOperation extends Enumeration
{
  type eOcrOperation = Value
  val None,
  TextEqual,
  TextNotEqual,
  TextContain,
  TextNotContain,
  NumberEqual,
  NumberNotEqual,
  NumberInRange,
  NumberOutOfRange,
  NumberLessThan,
  NumberGreaterThan = Value
}

case class ScenarioProperty(
                             ID: String,
                             Owners: List[String],
                             ScenarioTriggerType: eScenarioTriggerType,
                             DoNotSendEmailWhenSuccess: Boolean,
                             DoNotSendEmailWhenStop: Boolean,
                             DoNotSendEmailWhenFail: Boolean,
                             StartDelay: Int,
                             RetryCnt: Int,
                             RetryInterval: Int,
                             NextScenarioIfSuccess: NextScenarioCondition,
                             NextScenarioIfFail: NextScenarioCondition,
                             IsEnabled: Boolean,
                             Priority: Int
                           )

case class NextScenarioCondition(Name: String, Text: String)

case class WindowsSearchCondition(Text: String, ClassName: String, IsCheckRectEqual: Boolean, Left: Int, Top: Int, Right: Int, Bottom:Int)

case class ScenarioStep(
                         Operation: eScenarioOperation,
                         Metadata: ScenarioMetadata,
                         Script: ScenarioScript,
                         RetryInterval: Int,
                         RetryLimit: Int,
                         NextStepIfTrue: Int,
                         NextStepIfFalse: Int,
                         DescriptionToEmail: String,
                         DescriptionToDialog: String,
                         IsShowDialog: Boolean)

case class ScenarioScript(Type: eScenarioScript, Value: String, Native: Option[NativeFunction])

case class ScenarioMetadata(SearchArea: ScreenSearchArea, ImageSearchParam: ImageSearchingParam, ConditionExpression: String, OcrParamInfo: OcrParam)

case class ImageSearchingParam(TrainImageMap: Map[Int, TrainImage], AcceptThreshold: Double, MaximumNumberToFind: Int, TimeoutEnabled: Boolean, Timeout: Double)

case class TrainImage(BinaryImage: Array[Byte], Type: Boolean)

case class OcrParam(
                     Type: eOcrValueType,
                     Operation: eOcrOperation,
                     TextValue: String,
                     NumberValue: Double,
                     NumberRangeMin: Double,
                     NumberRangeMax: Double,
                     MinMatchingValue: Int
                   )

case class ScreenSearchArea(X: Int, Y: Int, Width: Int, Height: Int)

case class XYcompensation(RefImage: Array[Byte], X: Int, Y: Int)

case class Scenario(
                     RevisionNo: Int,
                     TriggerType: eScenarioTriggering,
                     ID: String,
                     Text: String,
                     Description: String,
                     Step: List[ScenarioStep],
                     TargetWindowCondition: WindowSearchCondition,
                     HidingWindowConditionList: List[WindowSearchCondition],
                     Compensation: XYcompensation,
                     TimeoutLimit: Int)

case class NativeFunction(FuncName: String, Params: Array[String], Outputs: Array[String])