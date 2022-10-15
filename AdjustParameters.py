from enum import Enum
import numpy as np
import json


class AdjustableParameter(Enum):
    TotalBuyCount0 = "TotalBuyCount0"
    TotalSellCount0 = "TotalSellCount0"
    TotalBuyPower0 = "TotalBuyPower0"
    TotalSellPower0 = "TotalSellPower0"
    TotalBuyCount1 = "TotalBuyCount1"
    TotalSellCount1 = "TotalSellCount1"
    TotalBuyPower1 = "TotalBuyPower1"
    TotalSellPower1 = "TotalSellPower1"
    TotalBuyCount2 = "TotalBuyCount2"
    TotalSellCount2 = "TotalSellCount2"
    TotalBuyPower2 = "TotalBuyPower2"
    TotalSellPower2 = "TotalSellPower2"
    TotalBuyCount3 = "TotalBuyCount3"
    TotalSellCount3 = "TotalSellCount3"
    TotalBuyPower3 = "TotalBuyPower3"
    TotalSellPower3 = "TotalSellPower3"
    PowerRatio0 = "PowerRatio0"
    PowerRatio1 = "PowerRatio1"
    PowerRatio2 = "PowerRatio2"
    PowerRatio3 = "PowerRatio3"
    Price0 = "Price0"
    Price1 = "Price1"
    Price2 = "Price2"
    Price3 = "Price3"
    RPrice0 = "RPrice0"
    RPrice1 = "RPrice1"
    RPrice2 = "RPrice2"
    RPrice3 = "RPrice3"
    BuySellRatio0 = "BuySellRatio0"
    BuySellRatio1 = "BuySellRatio1"
    BuySellRatio2 = "BuySellRatio2"
    BuySellRatio3 = "BuySellRatio3"
    PeakLast0 = "PeakLast0"
    PeakLast1 = "PeakLast1"
    PeakLast2 = "PeakLast2"
    PeakLast3 = "PeakLast3"
    PeakLast4 = "PeakLast4"
    PeakLast5 = "PeakLast5"
    PeakTime0 = "PeakTime0"
    PeakTime1 = "PeakTime1"
    PeakTime2 = "PeakTime2"
    PeakTime3 = "PeakTime3"
    PeakTime4 = "PeakTime4"
    PeakTime5 = "PeakTime5"
    PeakCount15M = "PeakCount15M"
    PeakCount1H = "PeakCount1H"
    DetailLen = "DetailLen"
    MaxPowInDetail = "MaxPowInDetail"
    DayPriceAnalysis = "DayPriceAnalysis"
    DownPeakRatio0 = "DownPeakRatio0"
    DownPeakRatio1 = "DownPeakRatio1"
    UpPeakRatio0 = "UpPeakRatio0"
    UpPeakRatio1 = "UpPeakRatio1"
    DownPeakRatioRise0 = "DownPeakRatioRise0"
    DownPeakRatioRise1 = "DownPeakRatioRise1"
    UpPeakRatioRise0 = "UpPeakRatioRise0"
    UpPeakRatioRise1 = "UpPeakRatioRise1"
    HourPriceRatioMin6 = "HourPriceRatioMin6"
    HourPriceRatioMax6 = "HourPriceRatioMax6"
    HourPriceRatioMin24 = "HourPriceRatioMin24"
    HourPriceRatioMax24 = "HourPriceRatioMax24"
    HourPriceRatioMin72 = "HourPriceRatioMin72"
    HourPriceRatioMax72 = "HourPriceRatioMax72"
    HourPriceRatioMin144 = "HourPriceRatioMin144"
    HourPriceRatioMax144 = "HourPriceRatioMax144"
    HourPriceRatioMin288 = "HourPriceRatioMin288"
    HourPriceRatioMax288 = "HourPriceRatioMax288"
    FirstToLastRaio = "FirstToLastRaio"
    JumpCount1H = "JumpCount1H"
    JumpCount2H = "JumpCount2H"
    JumpCount4H = "JumpCount4H"
    JumpCount8H = "JumpCount8H"
    JumpCount24H = "JumpCount24H"
    JumpCount72H = "JumpCount72H"
    NetPrice1H = "NetPrice1H"
    NetPrice2H = "NetPrice2H"
    NetPrice4H = "NetPrice4H"
    NetPrice8H = "NetPrice8H"
    NetPrice24H = "NetPrice24H"
    NetPrice72H = "NetPrice72H"
class Tag(Enum):
    Transaction = "Transaction"


class CheckType(Enum):
    Small = "<"
    Big = ">"
    DivideSmall = "/<"
    DivideBig = "/>"


class Rule:
    def __init__(self, param, index, thresholdSmall, tags, checkType):
        self.adjustableParameter = param
        self.threshold = thresholdSmall
        self.tags = tags
        self.checkType = checkType
        self.index = index
        self.badCount = 0
        self.goodCount = 0
        self.isTuned = False
    def ToJson(self):
        return {"ruleName": self.adjustableParameter ,
                "threshold": self.threshold,
                "checkType": self.checkType.value,
                "tags": self.tags}

    def __init__(self, jsonIn):
        self.adjustableParameter = jsonIn["ruleName"]
        self.threshold = float(jsonIn["threshold"])
        self.tags = jsonIn["tags"]
        checkTypeStr = jsonIn["checkType"]
        if checkTypeStr == "<":
            self.checkType = CheckType.Small
        elif checkTypeStr == ">":
            self.checkType = CheckType.Big
        elif checkTypeStr == "/<":
            self.checkType = CheckType.DivideSmall
        elif checkTypeStr == "/>":
            self.checkType = CheckType.DivideBig
        self.badCount = 0
        self.goodCount = 0
        self.quantileVal = 0.0
        self.quitCount = 0
        self.isTuned = False
        self.tuneCount = 0
        self.isNonZero = "NonZero" in self.tags
        self.isTransaction = "Transaction" in self.tags
        if self.adjustableParameter == "PowerRatio0":
            self.isTuned = True

        if not self.isTuned:
            self.threshold = 100000000
            if self.IsSmall():
                self.threshold = -100000000

    def SetIndex(self, index):
        self.index = index

    def IsSmall(self):
        return self.checkType == CheckType.Small or self.checkType == CheckType.DivideSmall

    def GetThresHoldPcnt(self, val):
        if self.IsSmall() and val > 0.0:
            return 0.9999
        elif self.IsSmall() and val < 0.0:
            return 1.0001
        elif not self.IsSmall() and val > 0.0:
            return 1.0001
        else:
            return 0.9999
    def SetFromValue(self, list):
        self.SetThreshold(np.amin(list) if self.IsSmall() else np.amax(list))
    def SetThreshold(self, val):
        self.threshold = val * self.GetThresHoldPcnt(val)
    def SetEliminationCounts(self, list, compareList, quantilePcnt):
        goodValue = np.quantile(list, quantilePcnt if self.IsSmall() else 1.0 - quantilePcnt)

        if self.IsSmall():
            minVal = np.min(list)
            isGoodValueSmallest = goodValue <= minVal
            if isGoodValueSmallest:
                flattened1D = np.sort(list.flatten())
                biggerList = flattened1D[flattened1D > minVal]
                if biggerList.size != 0:
                    goodValue = biggerList[0]
            self.badCount = np.count_nonzero(compareList < goodValue) / compareList.size
            self.goodCount = np.count_nonzero(list < goodValue) / list.size
        else:
            maxVal = np.max(list)
            isGoodValueLargest = goodValue >= maxVal
            if isGoodValueLargest:
                flattened1D = np.sort(list.flatten())
                smallerList = flattened1D[flattened1D < maxVal]
                if smallerList.size != 0:
                    goodValue = smallerList[-1]
            self.badCount = np.count_nonzero(compareList > goodValue) / compareList.size
            self.goodCount = np.count_nonzero(list > goodValue) / list.size

        self.quantileVal = goodValue
    def GetValue(self):
        return self.threshold
    def Check(self, val):
        if val <= 0.0 and self.isNonZero:
            return True
        returnVal = val < self.threshold if self.IsSmall() else val > self.threshold
        if returnVal:
            self.quitCount += 1
        return returnVal
    def Print(self):
        print( str(self.adjustableParameter), "its value is: ", self.threshold, "small: ", self.IsSmall(),
               "badCount: ", self.badCount, "goodCount: ", self.goodCount, "quitCount: ", self.quitCount  )

    def CheckDivider(self, first, divider):
        if CheckType.Small == self.checkType or CheckType.Big == self.checkType:
            raise Exception("Check type must be divider")

        if divider == 0:
            return True if CheckType.DivideSmall == self.checkType else False
        return self.Check(first / divider)


class RuleList:
    def __init__(self):
        self.ruleList = []
        self.isTuned = False
        self.iterationCount = 0
        file = open("/home/erdem/Documents/RuleJsonList.json", "r")
        ruleDictionary = json.load(file)
        curIndex = 0
        for ruleJson in ruleDictionary["ruleList"]:
            rule = Rule(ruleJson)
            if bool(self.ruleList):
                adjParam = self.ruleList[-1].adjustableParameter
                if adjParam != rule.adjustableParameter:
                    curIndex += 1
            rule.SetIndex(curIndex)
            self.ruleList.append(rule)
        return

    def Control(self, parameter, checkType, val):
        rule = self.GetRule(parameter, checkType)
        return rule.Check(val)

    def ResetRules(self):
        for rule in self.ruleList:
            rule.badCount = 0
            rule.goodCount = 0
            rule.quantileVal = 0.0

    def SelectBestQuantile(self):
        selectedRule = None
        bestVal = -1000000.0
        for rule in self.ruleList:
            curVal = rule.badCount - (rule.goodCount*5)
            if curVal > bestVal and not rule.isTuned and rule.tuneCount <= 3 :
                bestVal = curVal
                selectedRule = rule
        selectedRule.tuneCount += 1
        print("Iteration done best seperator rule was", str(selectedRule.adjustableParameter), "its value is: ", selectedRule.threshold,
              "small: ", selectedRule.IsSmall(), "will be replaced: ", selectedRule.quantileVal, "best val:", bestVal)
        selectedRule.SetThreshold(selectedRule.quantileVal)
        return True #selectedRule.badCount > selectedRule.goodCount

    def Write(self, goodCount, badCount):
        self.iterationCount += 1
        jsonDict = { "ruleList":[] }
        for rule in self.ruleList:
            jsonDict["ruleList"].append(rule.ToJson())
        parsed = json.loads(json.dumps(jsonDict))
        fileName = "/home/erdem/Documents/RuleJsonList_" + str(self.iterationCount)  + "_" + str(goodCount) + "_" + str(badCount) + ".json"
        file = open(fileName, "w")
        file.write(json.dumps(parsed, indent=4))

        file = open("/home/erdem/Documents/RuleJsonList.json", "w")
        file.write(json.dumps(parsed, indent=4))

    def SetIndex(self, parameter, index ):
        for rule in self.ruleList:
            if rule.adjustableParameter == parameter.name:
                rule.index = index
    def ControlClamp(self, parameter, val):
        ruleSmall = self.GetRule(parameter, CheckType.Small)
        ruleBig = self.GetRule(parameter, CheckType.Big)

        return ruleSmall.Check(val) or ruleBig.Check(val)


    def ControlIndex(self, index, checkType, val):
        if checkType != CheckType.Small:
            index += 1
        rule = self.ruleList[index]
        return rule.Check(val)


    def ControlClampIndex(self, index, val):
        ruleSmall = self.ruleList[index]
        ruleBig = self.ruleList[index + 1]

        return ruleSmall.Check(val) or ruleBig.Check(val)


    def ControlClampIndexDivider(self, index, val, divider):
        ruleSmall = self.ruleList[index]
        ruleBig = self.ruleList[index + 1]

        return ruleSmall.CheckDivider(val, divider) or ruleBig.CheckDivider(val, divider)

    def GetRulesWithIndex(self, index):
        returnVal = []
        for rule in self.ruleList:
            if rule.index == index:
                returnVal.append( rule )
        return returnVal
    def GetRule(self, parameter, checkType):
        for rule in self.ruleList:
            if rule.adjustableParameter == parameter.value and rule.checkType == checkType :
                return rule
        return None
