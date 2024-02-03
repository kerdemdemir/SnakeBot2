import json
from datetime import datetime
import bisect
import copy
import TransactionBasics
import random
import AdjustParameters as AP
import sys
import multiprocessing
import Peaks
import Features
from typing import List

import time

IsMultiThreaded = False
rules = AP.RuleList()


def init_pool_processes(the_lock):
    global lock
    lock = the_lock


class EliminatedList:
    def __init__(self):
        if IsMultiThreaded:
            manager = multiprocessing.Manager()
            self.eliminatedList = manager.list()
            self.lock = multiprocessing.Lock()
        else:
            self.eliminatedList = []

    def AddEliminated(self, name, reportTime):
        key = str(name) + str(reportTime)
        if IsMultiThreaded:
            with self.lock:
                self.eliminatedList.append(key)
        else:
            self.eliminatedList.append(key)

    def AddEliminatedBool(self, name, isExtra, reportTime):
        newName = str(name) + str(isExtra)
        return self.AddEliminated(newName, reportTime)

    def IsEliminated(self, name, reportTime):
        key = str(name) + str(reportTime)
        if IsMultiThreaded:
            with self.lock:
                return key in self.eliminatedList
        else:
            return key in self.eliminatedList

    def IsEliminatedBool(self, name, isExtra, reportTime):
        if isExtra:
            newName = str(name) + str(False)
            if self.IsEliminated(newName, reportTime):
                return True

        newName = str(name) + str(isExtra)
        return self.IsEliminated(newName, reportTime)


eliminatedList = EliminatedList()
eliminatedList2 = EliminatedList()


class FileHandler:
    def __init__(self, jsonIn, transactionParam, marketState, fileName, isTest):
        self.marketState = marketState
        self.jumpTimeInSeconds = 0
        self.reportTimeInSeconds = 0
        self.reportPrice = 0.0
        self.jumpPrice = 0.0
        self.transactions = []
        self.currencyName = ""
        self.isRise = False
        self.candleDataList = Peaks.CandleDataList()
        self.transactionParam = transactionParam
        self.patternList: List[Features.Features] = []
        self.mustBuyList: List[Features.Features] = []
        self.badPatternList: List[Features.Features] = []
        self.addedCount = 0
        self.isAfterBuyRecord = fileName.split("/")[-1].startswith("Positive")
        self.smallestStrikeCount = 1000
        self.bestPattern = None
        self.averageVolume = 0.0
        self.isTest = isTest
        self.isExtra = fileName.split("/")[-1].startswith("learningSuddenChange_")
        if self.__Parse(jsonIn) == -1:
            return

        self.lowestTransaction = TransactionBasics.TransactionCountPerSecBase
        self.acceptedTransLimit = TransactionBasics.TransactionLimitPerSecBase
        self.dataList = []
        tempTransaction = json.loads(jsonIn["transactions"])
        if len(tempTransaction) == 0:
            return
        lastTimeInSeconds = int(tempTransaction[-1]["T"]) // 1000
        diffTime = self.reportTimeInSeconds - lastTimeInSeconds
        if diffTime > 60 * 65:
            print("2 hour diff")
            self.jumpTimeInSeconds -= 60 * 60 * 2
            self.reportTimeInSeconds -= 60 * 60 * 2
        elif diffTime > 60 * 35:
            print("1 hour diff")
            self.jumpTimeInSeconds -= 60 * 60
            self.reportTimeInSeconds -= 60 * 60

        if eliminatedList.IsEliminated(self.currencyName, self.reportTimeInSeconds):
            return

        self.__DivideDataInSeconds(tempTransaction, self.transactionParam.msec, self.dataList, 0,
                                   len(tempTransaction))  # populates the dataList with TransactionData
        self.__AppendToPatternList(
            tempTransaction)  # deletes dataList and populates mustBuyList, patternList badPatternList

    def GetFeatures(self):
        return []

    def __Parse(self, jsonIn):
        epoch = datetime.utcfromtimestamp(0)

        self.isRise = bool(jsonIn["isRise"])
        self.jumpPrice = float(jsonIn["jumpPrice"])
        self.reportPrice = float(jsonIn["reportPrice"])
        tempReportTime = jsonIn["reportTime"].split(".")[0]
        if tempReportTime.endswith("Z"):
            tempReportTime = tempReportTime[:-1]
        datetime_object = datetime.strptime(tempReportTime, '%Y-%b-%d %H:%M:%S')
        now = datetime.now()

        self.reportTimeInSeconds = (datetime_object - epoch).total_seconds()
        if "avarageVolume" in jsonIn:
            self.averageVolume = jsonIn["avarageVolume"]

        candleSticks = jsonIn["candleStickData"]
        self.candleDataList.feed(candleSticks)
        tempJumpTime = jsonIn["time"].split(".")[0]
        if tempJumpTime.endswith("Z"):
            tempJumpTime = tempReportTime[:-1]
        datetime_object = datetime.strptime(tempJumpTime, '%Y-%b-%d %H:%M:%S')
        self.jumpTimeInSeconds = (datetime_object - epoch).total_seconds()
        self.currencyName = jsonIn["name"]

    def __DivideDataInSeconds(self, jsonIn, msecs, datalist, startIndex, endIndex):
        transactionData = TransactionBasics.TransactionData()
        lastEndTime = 0
        stopMiliSecs = int(jsonIn[endIndex - 1]["T"]) + msecs
        for x in range(startIndex, endIndex):
            curElement = jsonIn[x]
            curMiliSecs = int(curElement["T"])
            if x == startIndex:
                lastEndTime = curMiliSecs // msecs * msecs + msecs
                transactionData.SetTime(curMiliSecs // msecs * msecs / 1000)

            if curMiliSecs > lastEndTime:
                copyData = copy.deepcopy(transactionData)
                datalist.append(copyData)
                transactionData.Reset()
                transactionData.AddData(curElement)

                while True:
                    if curMiliSecs > (lastEndTime + msecs) and lastEndTime < stopMiliSecs:
                        emptyData = TransactionBasics.TransactionData()
                        emptyData.SetTime(lastEndTime // msecs * msecs / 1000)
                        emptyData.SetIndex(x)
                        emptyData.firstPrice = copyData.lastPrice
                        emptyData.lastPrice = copyData.lastPrice
                        lastEndTime += msecs
                        if startIndex == 0:
                            datalist.append(emptyData)
                    else:
                        transactionData.SetTime(curMiliSecs // msecs * msecs / 1000)
                        transactionData.SetIndex(x)
                        lastEndTime += msecs
                        break
            else:
                transactionData.AddData(curElement)
                transactionData.SetIndex(x)
        copyData = copy.deepcopy(transactionData)
        datalist.append(copyData)

    def __AppendToPatternList(self, jsonIn):
        lenArray = len(self.dataList)
        if lenArray == 0:
            return
        # print(lenArray, self.dataList)
        maxTradeVal = 0
        for x in range(lenArray):
            # curTimeInSeconds = self.dataList[x].timeInSecs
            # if  curTimeInSeconds > self.reportTimeInSeconds+10:
            #    continue
            lastTotalTradePower = self.dataList[x].totalBuy + self.dataList[x].totalSell
            if lastTotalTradePower > maxTradeVal:
                maxTradeVal = lastTotalTradePower
            self.__AppendToPatternListImpl(x, lenArray, jsonIn)
        if len(self.patternList) == 0:
            self.dataList.reverse()

        limit = TransactionBasics.MaximumSampleSizeFromGoodPattern
        badLimit = TransactionBasics.MaximumSampleSizeFromPattern

        if self.isTest:
            limit = 1
            badLimit = 1
        if len(self.patternList) > limit:
            self.patternList = sorted(self.patternList, key=lambda x: int(x.lastPrice))[:limit]

        if len(self.badPatternList) > badLimit:
            randomSampleList = random.sample(self.badPatternList, badLimit)
            self.badPatternList = randomSampleList

        if len(self.badPatternList) == 0 and len(self.patternList) == 0:
            eliminatedList.AddEliminated(self.currencyName, self.reportTimeInSeconds)

        del self.dataList
        del self.candleDataList

    def __GetWithTime(self, jsonIn, startIndex, startTime, stopTime, divider):
        currentData = TransactionBasics.TransactionData()

        isFirst = True
        if startIndex == 0:
            startIndex = bisect.bisect_right(jsonIn, startTime, key=lambda x: int(x["T"]))
            startIndex -= 1
            if startIndex >= len(jsonIn) or startIndex < 0:
                startIndex = 0

        for index in range(startIndex, len(jsonIn)):
            elem = jsonIn[index]
            curTime = int(elem["T"])
            currentData.SetCurPrice(elem)
            if curTime < startTime:
                continue
            if isFirst:
                startTime = curTime
                isFirst = False
            if curTime > stopTime:
                break
            currentData.AddData(elem)
            currentData.endIndex = index

        currentData.NormalizePrice()
        if startTime < int(jsonIn[0]["T"]):
            dividorTemp = stopTime - int(jsonIn[0]["T"])
            currentData.Divide(dividorTemp / 1000)
        else:
            currentData.Divide(divider)
        return currentData

    def FindInterestIndexWithPower(self, curPattern, jsonIn, powerLimit):
        currentPowSum = 0.0
        for x in range(curPattern.startIndex, curPattern.endIndex):
            currentPowSum += float(jsonIn[x]["q"]) * float(jsonIn[x]["p"])
            if currentPowSum > powerLimit:
                return float(jsonIn[x]["p"]), float(jsonIn[x]["T"]), x
        return float(jsonIn[curPattern.endIndex]["p"]), float(jsonIn[curPattern.endIndex]["T"]), curPattern.endIndex

    def FindInterestIndexWithTime(self, curPattern, jsonIn, time):
        for x in range(curPattern.startIndex, curPattern.endIndex):
            if jsonIn[x]["T"] >= time:
                return float(jsonIn[x]["p"]), time, x
        return float(jsonIn[curPattern.endIndex]["p"]), float(jsonIn[curPattern.endIndex]["T"]), curPattern.endIndex

    def FindInterestIndexWithPrice(self, curPattern, jsonIn, price):
        for x in range(curPattern.startIndex, curPattern.endIndex):
            if float(jsonIn[x]["p"]) >= price:
                return float(jsonIn[x]["p"]), float(jsonIn[x]["T"]), x
        return float(jsonIn[curPattern.endIndex]["p"]), float(jsonIn[curPattern.endIndex]["T"]), curPattern.endIndex

    def CalculateActualVolume(self, jsonIn, actualEndIndex, curTimeInMiliSecs):
        restPowerSum = 0.0
        for x in range(actualEndIndex, len(jsonIn)):
            restPowerSum += float(jsonIn[x]["q"]) * float(jsonIn[x]["p"])
        lastTimeInSeconds = int(jsonIn[-1]["T"]) // 1000
        timeDiffInSeconds = (lastTimeInSeconds - curTimeInMiliSecs // 1000)
        return (self.averageVolume * 60 * 60 * 6 - restPowerSum) / (60 * 60 * 6 - timeDiffInSeconds)

    def __AppendToPatternListImpl(self, curIndex, lenArray, jsonIn):
        totalCount = 30
        startBin = curIndex + 1 - totalCount
        if curIndex > lenArray:
            return
        curPattern = self.dataList[curIndex]
        curTimeInMiliSecs = jsonIn[curPattern.endIndex]["T"]
        interestTime = 0
        # if interestTime - curTimeInMiliSecs < 10000 and self.currencyName == "IOTA" and self.isRise:
        #    print("Alert")
        if startBin < 0:
            return
        powerLimit = 0.03
        if curPattern.totalBuy + curPattern.totalSell < powerLimit:
            return
        if eliminatedList2.IsEliminatedBool(self.currencyName, self.isExtra, curPattern.endTimeInSecs):
            return
        features = Features.Features()

        if interestTime != 0 and interestTime - curTimeInMiliSecs < 10000:
            lastPrice, curTimeInMiliSecs, actualEndIndex = self.FindInterestIndexWithTime(curPattern, jsonIn,
                                                                                          interestTime)
        else:
            lastPrice, curTimeInMiliSecs, actualEndIndex = self.FindInterestIndexWithPower(curPattern, jsonIn,
                                                                                           powerLimit)

        isUpOrDownTrend = features.SetPeaks(lastPrice, curTimeInMiliSecs // 1000, self.candleDataList, self.dataList)
        if AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
            if isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
                targetUpPrice = features.priceList[-2] * 1.032
            if curPattern.maxPrice > targetUpPrice:
                lastPrice, curTimeInMiliSecs, actualEndIndex = self.FindInterestIndexWithPrice(curPattern, jsonIn,
                                                                                               targetUpPrice)
                isUpOrDownTrend = features.SetPeaks(lastPrice, curTimeInMiliSecs // 1000, self.candleDataList,
                                                    self.dataList)
                if isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
                    return
            else:
                return
        if not AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.UP:
            return

        if features.peaks[-1] < 0.995:
            targetTime = curPattern.maxTimeInSecs * 1000
            lastPrice, curTimeInMiliSecs, actualEndIndex = self.FindInterestIndexWithTime(curPattern, jsonIn,
                                                                                          targetTime)
            features.SetPeaks(lastPrice, curTimeInMiliSecs // 1000, self.candleDataList, self.dataList)
            if features.peaks[-1] < 0.995:
                return
            # else:
            #    print("Recovered")

        if len(features.timeList) < 7:
            return

        features.firstToLastRatio = self.dataList[0].firstPrice / lastPrice
        # self.__GetWithTime(jsonIn, curTimeInMiliSecs - 10000, curTimeInMiliSecs, 10)
        firstData = self.__GetWithTime(jsonIn, 0, curTimeInMiliSecs - 900000, curTimeInMiliSecs - 380000, 520)
        secondData = self.__GetWithTime(jsonIn, firstData.endIndex - 1, curTimeInMiliSecs - 380000,
                                        curTimeInMiliSecs - 60000, 320)
        lastData = self.__GetWithTime(jsonIn, secondData.endIndex - 1, curTimeInMiliSecs - 60000, curTimeInMiliSecs, 60)
        dataRange = [firstData, secondData, lastData]

        basePrice = lastPrice
        features.lastPrice = lastPrice

        features.timeToJump = self.reportTimeInSeconds - self.dataList[curIndex].timeInSecs

        lastMiniData = self.__GetWithTime(jsonIn, secondData.endIndex - 1, curTimeInMiliSecs - 1000, curTimeInMiliSecs,
                                          1)
        if lastMiniData.totalBuy < 0.004:
            return
        features.SetDetailedTransaction(lastMiniData)
        actualAvarageVolume = self.CalculateActualVolume(jsonIn, actualEndIndex, curTimeInMiliSecs)
        features.Append(dataRange, actualAvarageVolume, self.jumpTimeInSeconds, self.jumpPrice)

        if self.marketState:
            if self.marketState.getState(self.jumpTimeInSeconds)[1] > 6:
                return

        k = 0
        rules.strikeCount = 0
        for i in range(len(features.transactionBuyList)):
            if rules.ControlClampIndex(k, features.transactionBuyList[i] + features.transactionSellList[i]):
                return
            k += 2
            if rules.ControlClampIndex(k, features.transactionBuyPowerList[i]):
                return
            k += 2
            if rules.ControlClampIndex(k, features.transactionSellPowerList[i]):
                return
            k += 2
            if rules.ControlClampIndex(k, features.firstLastPriceList[i]):
                return
            k += 2

        # if rules.ControlClamp(AP.AdjustableParameter.MaxPowInDetail, pattern.maxDetailBuyPower):
        #    return

        if rules.ControlClamp(AP.AdjustableParameter.AverageVolume, features.averageVolume):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount10M, features.jumpCountList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount1H, features.jumpCountList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount12H, features.jumpCountList[2]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.NetPrice1H, features.netPriceList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice8H, features.netPriceList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice24H, features.netPriceList[2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice168H, features.netPriceList[3]):
            return

        # if rules.ControlClamp(AP.AdjustableParameter.PeakTime0, pattern.timeList[-1]):
        #    return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime1, features.timeList[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime2, features.timeList[-3]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakLast1, features.peaks[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast2, features.peaks[-3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast3, features.peaks[-4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast4, features.peaks[-5]):
            return

        self.smallestStrikeCount = min(self.smallestStrikeCount, rules.strikeCount)
        # print(pattern.marketStateList)
        category = self.__GetCategory(curIndex, basePrice, features)
        if category == 0:
            self.mustBuyList.append(features)
        elif category == 1:
            self.addedCount += 1
            # if self.isRise:
            #    print("name: ", self.currencyName, " time: ", curTimeInMiliSecs, " curIndex: ", curIndex, " all vals: ", pattern.GetFeatures(rules))
            self.patternList.append(features)
            eliminatedList2.AddEliminatedBool(self.currencyName, self.isExtra, curPattern.endTimeInSecs)
            # print(basePrice, self.currencyName)
        elif category == 2:
            self.badPatternList.append(features)
            self.addedCount += 1
            eliminatedList2.AddEliminatedBool(self.currencyName, self.isExtra, curPattern.endTimeInSecs)

        features.isExtra = self.isExtra
        features.patternTime = curPattern.endTimeInSecs
        features.name = self.currencyName

    def __GetCategory(self, curIndex, priceIn, pattern):
        if self.isRise:
            isDropped = False
            for i in range(curIndex + 2, len(self.dataList)):
                ratio = self.dataList[i].lastPrice / priceIn
                timeDiff = self.dataList[i].endTimeInSecs - self.dataList[curIndex].endTimeInSecs
                if timeDiff > 900:
                    return -1
                if ratio < 0.98:
                    isDropped = True
                if ratio < 0.97:
                    return 2
                if ratio > 1.07 and not isDropped:
                    pattern.GoalReached(timeDiff)
                    return 1
            return -1
        else:
            for i in range(curIndex, len(self.dataList)):
                timeDiff = self.dataList[i].endTimeInSecs - self.dataList[curIndex].endTimeInSecs
                if self.dataList[i].lastPrice / priceIn < 0.99:
                    pattern.GoalReached(timeDiff)
                    return 2
                if timeDiff > 900:
                    if self.dataList[i].lastPrice / priceIn > 1.005:
                        return -1
                    else:
                        return 2
                if self.dataList[i].lastPrice / priceIn > 1.08:
                    return 1
            return 2

        return -1
