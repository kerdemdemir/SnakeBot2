import json
from builtins import list
from datetime import datetime
import bisect
import copy
import numpy as np
import os
from os import listdir
from os.path import isfile, join
import MarketStateManager
import TransactionBasics
import random
import AdjustParameters as AP
import sys
import multiprocessing
import Peaks

import time

PeakFeatureCount = TransactionBasics.PeakFeatureCount
percent = 0.01
IsMultiThreaded = True
IsOneFileOnly = False
totalCounter = 0
rules = AP.RuleList()

def init_pool_processes(the_lock):
    global lock
    lock = the_lock

class EliminatedList:
    def __init__(self):
        if IsMultiThreaded :
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

    def IsEliminated(self, name, reportTime):
        key = str(name) + str(reportTime)
        if IsMultiThreaded:
            with self.lock:
                return key in self.eliminatedList
        else:
            return key in self.eliminatedList

eliminatedList = EliminatedList()

class SuddenChangeHandler:
    def __init__(self, jsonIn, transactionParam,marketState, fileName, isTest):
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
        self.patternList = []
        self.mustBuyList = []
        self.badPatternList = []
        self.addedCount  = 0
        self.isAfterBuyRecord = fileName.split("/")[-1].startswith("Positive")
        self.smallestStrikeCount = 1000
        self.bestPattern = None
        self.averageVolume = 0.0
        self.isTest =  isTest
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
        if diffTime > 60*65:
            print("2 hour diff")
            self.jumpTimeInSeconds -= 60*60*2
            self.reportTimeInSeconds -= 60*60*2
        elif diffTime > 60*35:
            print("1 hour diff")
            self.jumpTimeInSeconds -= 60*60
            self.reportTimeInSeconds -= 60*60

        if eliminatedList.IsEliminated(self.currencyName, self.reportTimeInSeconds):
            return

        self.__DivideDataInSeconds(tempTransaction, self.transactionParam.msec, self.dataList, 0, len(tempTransaction)) #populates the dataList with TransactionData
        self.__AppendToPatternList(tempTransaction) # deletes dataList and populates mustBuyList, patternList badPatternList

    def GetFeatures(self):
        #maxRise = [self.GetPeakFeatures(60 * 6), self.GetPeakFeatures(60 * 24), self.GetPeakFeatures(60 * 24 * 3), self.GetPeakFeatures(60 * 24 * 7 ), self.GetPeakFeatures(60 * 24 * 30 ), self.GetPeakFeatures(60 * 24 * 90 )]
        #maxRise = [self.GetCount(60 * 6), self.GetCount(60 * 24), self.GetCount(60 * 24 * 3), self.GetCount(60 * 24 * 7 ), self.GetCount(60 * 24 * 30 ), self.GetCount(60 * 24 * 90 )]
        #return maxRise
        #return TransactionBasics.GetMaxMinList( self.maxMinList ) #+ maxRise
        return []
       #return self.timeList[-PeakFeatureCount:] + self.riseList[-PeakFeatureCount:]
       #return [self.riseList[-1] / self.riseList[-3], self.riseList[-2] / self.riseList[-4], self.riseList[-3] / self.riseList[-5], self.riseList[-4] / self.riseList[-6]]
        #return []

    def __Parse(self, jsonIn):
        epoch = datetime.utcfromtimestamp(0)

        self.isRise = bool(jsonIn["isRise"])
        self.jumpPrice = float(jsonIn["jumpPrice"])
        self.reportPrice = float(jsonIn["reportPrice"])
        tempReportTime =  jsonIn["reportTime"].split(".")[0]
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




    def __DivideDataInSeconds(self, jsonIn, msecs, datalist, startIndex, endIndex ):
        transactionData = TransactionBasics.TransactionData()
        lastEndTime = 0
        stopMiliSecs = int(jsonIn[endIndex-1]["T"])+msecs
        for x in range(startIndex,endIndex):
            curElement = jsonIn[x]
            curMiliSecs = int(curElement["T"])
            if x == startIndex:
                lastEndTime = curMiliSecs//msecs*msecs + msecs
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
            #curTimeInSeconds = self.dataList[x].timeInSecs
            #if  curTimeInSeconds > self.reportTimeInSeconds+10:
            #    continue
            lastTotalTradePower = self.dataList[x].totalBuy + self.dataList[x].totalSell
            if lastTotalTradePower > maxTradeVal:
                maxTradeVal = lastTotalTradePower
            self.__AppendToPatternListImpl(self.transactionParam.gramCount, x, lenArray, jsonIn)
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

        if len(self.badPatternList) == 0 and len(self.patternList) == 0 :
            eliminatedList.AddEliminated(self.currencyName, self.reportTimeInSeconds)

        del self.dataList
        del self.candleDataList

    def __GetWithTime(self, jsonIn, startIndex, startTime, stopTime, divider):
        currentData = TransactionBasics.TransactionData()

        isFirst = True
        if startIndex == 0 :
            startIndex = bisect.bisect_right(jsonIn, startTime, key=lambda x: int(x["T"]))
            startIndex -= 1
            if startIndex >= len(jsonIn) or startIndex < 0 :
                startIndex = 0

        for index in range(startIndex,len(jsonIn)):
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
        if startTime < int(jsonIn[0]["T"]) :
            dividorTemp = stopTime - int(jsonIn[0]["T"])
            currentData.Divide(dividorTemp / 1000)
        else:
            currentData.Divide( divider )
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
        timeDiffInSeconds = (lastTimeInSeconds - curTimeInMiliSecs//1000)
        return (self.averageVolume * 60 * 60 * 6 - restPowerSum)/(60 * 60 * 6 - timeDiffInSeconds)
    def __AppendToPatternListImpl(self, ngramCount, curIndex, lenArray, jsonIn):
        totalCount = 30
        startBin = curIndex + 1 - totalCount
        if curIndex > lenArray:
             return
        curPattern = self.dataList[curIndex]
        curTimeInMiliSecs = jsonIn[curPattern.endIndex]["T"]
        interestTime = 0
        #if interestTime - curTimeInMiliSecs < 10000 and self.currencyName == "MBOX" and self.isRise and curIndex > 380:
        #    print("Alert")
        if startBin < 0:
            return
        powerLimit = 0.03
        if curPattern.totalBuy + curPattern.totalSell < powerLimit:
            return

        pattern = TransactionBasics.TransactionPattern()

        if interestTime != 0 and interestTime - curTimeInMiliSecs < 10000 :
            lastPrice,curTimeInMiliSecs,actualEndIndex = self.FindInterestIndexWithTime(curPattern, jsonIn, powerLimit)
        else:
            lastPrice,curTimeInMiliSecs,actualEndIndex = self.FindInterestIndexWithPower(curPattern, jsonIn, powerLimit)

        isUpOrDownTrend = pattern.SetPeaks(lastPrice, curTimeInMiliSecs//1000, self.candleDataList, self.dataList)
        if AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
            targetUpPrice = pattern.priceList[-2]*1.035
            if curPattern.lastPrice > targetUpPrice:
                lastPrice,curTimeInMiliSecs,actualEndIndex  = self.FindInterestIndexWithPrice(curPattern, jsonIn, targetUpPrice)
                isUpOrDownTrend = pattern.SetPeaks(lastPrice, curTimeInMiliSecs // 1000, self.candleDataList,self.dataList)
                if isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
                    return
            else:
                return
        if not AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.UP:
            return

        if pattern.peaks[-1] < 0.995:
           return

        if len(pattern.timeList) < 7:
            return
        pattern.firstToLastRatio = self.dataList[0].firstPrice / lastPrice
        #self.__GetWithTime(jsonIn, curTimeInMiliSecs - 10000, curTimeInMiliSecs, 10)
        firstData = self.__GetWithTime(jsonIn, 0, curTimeInMiliSecs - 610000, curTimeInMiliSecs - 130000, 480)
        secondData = self.__GetWithTime(jsonIn, firstData.endIndex - 1, curTimeInMiliSecs - 130000, curTimeInMiliSecs - 10000, 120)
        lastData = self.__GetWithTime(jsonIn, secondData.endIndex - 1, curTimeInMiliSecs - 10000, curTimeInMiliSecs, 10)
        dataRange = [firstData, secondData, lastData]

        basePrice = lastPrice
        pattern.lastPrice = lastPrice

        pattern.timeToJump = self.reportTimeInSeconds - self.dataList[curIndex].timeInSecs

        lastMiniData = self.__GetWithTime(jsonIn, secondData.endIndex - 1, curTimeInMiliSecs - 1000, curTimeInMiliSecs, 1)
        if lastMiniData.totalBuy < 0.004:
            return
        pattern.SetDetailedTransaction(lastMiniData)
        actualAvarageVolume = self.CalculateActualVolume(jsonIn, actualEndIndex, curTimeInMiliSecs)
        pattern.Append( dataRange, actualAvarageVolume, self.jumpTimeInSeconds, self.jumpPrice, self.marketState)
        if pattern.marketStateList[1] > 6:
            return

        k = 0
        rules.strikeCount = 0
        for i in range(len(pattern.transactionBuyList)):
            if rules.ControlClampIndex(k,pattern.transactionBuyList[i]+pattern.transactionSellList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.transactionBuyPowerList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.transactionSellPowerList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.firstLastPriceList[i]):
                return
            k+=2

        if rules.ControlClamp(AP.AdjustableParameter.MaxPowInDetail, pattern.maxDetailBuyPower):
            return

        if rules.ControlClamp(AP.AdjustableParameter.AverageVolume, pattern.averageVolume):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount10M, pattern.jumpCountList[0]):
           return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount1H, pattern.jumpCountList[1]):
           return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount12H, pattern.jumpCountList[2]):
           return

        if rules.ControlClamp(AP.AdjustableParameter.NetPrice1H, pattern.netPriceList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice8H, pattern.netPriceList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice24H, pattern.netPriceList[2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice168H, pattern.netPriceList[3]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakTime0, pattern.timeList[-1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime1, pattern.timeList[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime2, pattern.timeList[-3]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakLast1, pattern.peaks[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast2, pattern.peaks[-3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast3, pattern.peaks[-4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast4, pattern.peaks[-5]):
            return

        self.smallestStrikeCount = min(self.smallestStrikeCount, rules.strikeCount)
        #print(pattern.marketStateList)
        category = self.__GetCategory(curIndex,basePrice,pattern)
        if category == 0:
            self.mustBuyList.append(pattern)
        elif category == 1:
            self.addedCount += 1
            #if self.isRise:
            #    print("name: ", self.currencyName, " time: ", curTimeInMiliSecs, " curIndex: ", curIndex, " all vals: ", pattern.GetFeatures(rules))
            self.patternList.append(pattern)
            #print(basePrice, self.currencyName)
        elif category == 2:
            self.badPatternList.append(pattern)
            self.addedCount += 1

    def __GetCategory(self, curIndex, priceIn, pattern):
        if self.isRise:
            isDropped = False
            for i in range(curIndex+1, len(self.dataList)):
                ratio = self.dataList[i].lastPrice / priceIn
                timeDiff = self.dataList[i].endTimeInSecs - self.dataList[curIndex].endTimeInSecs
                if timeDiff > 900:
                    return -1
                if ratio<0.98:
                    isDropped = True
                if ratio<0.97:
                    return 2
                if ratio>1.08 and not isDropped:
                    pattern.GoalReached(timeDiff, 1.08)
                    return 1
            return -1
        else:
            for i in range(curIndex, len(self.dataList)):
                timeDiff = self.dataList[i].endTimeInSecs - self.dataList[curIndex].endTimeInSecs
                if self.dataList[i].lastPrice/priceIn<0.99:
                    pattern.GoalReached(timeDiff, 1.025)
                    return 2
                if timeDiff > 900:
                    if self.dataList[i].lastPrice/priceIn > 1.005:
                        return -1
                    else:
                        return 2
                if self.dataList[i].lastPrice/priceIn>1.08:
                    return 1
            return 2

        return -1

class SuddenChangeMerger:

    def __init__(self, transactionParam, marketState):
        self.mustBuyList = []

        if IsMultiThreaded :
            manager = multiprocessing.Manager()
            self.patternList = manager.list()
            self.badPatternList = manager.list()
        else:
            self.patternList = []
            self.badPatternList = []

        self.handlerList = []
        self.peakHelperList = []
        self.transactionParam = transactionParam
        self.marketState = marketState
        print("Alert2: ")


    def AddFile(self, jsonIn, fileName, isTest):
        tempHandlers = []
        if isinstance(jsonIn, dict):
            handler = SuddenChangeHandler(jsonIn, self.transactionParam, self.marketState, fileName, isTest)
            return [handler]
        else:
            for index in range(len(jsonIn)):
                if not jsonIn[index]:
                    continue

                jsonPeakTrans = jsonIn[index]

                handler = SuddenChangeHandler(jsonPeakTrans,self.transactionParam,self.marketState,fileName, isTest)
                tempHandlers.append(handler)
            return tempHandlers

    def Finalize(self, isPrint):
        if isPrint:
            self.Print()

    def toTransactionFeaturesNumpy(self):
        badCount = len(self.badPatternList)
        goodCount = len(self.patternList)
        #self.Print()
        #mustBuyCount = len(self.mustBuyList)
        print("Good count: ", goodCount, " Bad Count: ", badCount)

        if goodCount > 0 :
            allData = np.concatenate( (self.patternList, self.badPatternList), axis=0)
        else:
            allData = self.badPatternList
        return allData

    def toTransactionResultsNumpy(self):
        badCount = len(self.badPatternList)
        goodCount = len(self.patternList)
        #mustBuyCount = len(self.mustBuyList)
        print("Good count: ", goodCount, " Bad Count: ", badCount)
        #mustBuyResult = [2] * mustBuyCount
        goodResult = [1] * goodCount
        badResult = [0] * badCount
        returnPatternList = goodResult + badResult
        return returnPatternList

    def Print(self):

        buyList = np.array(self.patternList)
        badList = np.array(self.badPatternList)
        print("Good count: ", len(self.patternList), "Bad Count", len(self.badPatternList))


        for i in range(len(self.patternList[0])):
            if not AP.IsTraining:
                curRules = rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.SetFromValue(buyList[:, i], False)#not rules.isTuned
                    rule.Print()

            buyLegend = str(np.quantile(buyList[:, i], 0.0)) + "," + str(np.quantile(buyList[:, i], 0.1)) + "," + str(
                np.quantile(buyList[:, i], 0.25)) + "," + str(np.quantile(buyList[:, i], 0.5)) + "," + str(
                np.quantile(buyList[:, i], 0.75)) + "," + str(np.quantile(buyList[:, i], 0.9)) + "," + str(
                np.quantile(buyList[:, i], 1.0))
            badLegend = str(np.quantile(badList[:, i], 0.0)) + "," + str(
                np.quantile(badList[:, i], 0.1)) + "," + str(np.quantile(badList[:, i], 0.25)) + "," + str(
                np.quantile(badList[:, i], 0.5)) + "," + str(np.quantile(badList[:, i], 0.75)) + "," + str(
                np.quantile(badList[:, i], 0.9)) + "," + str(np.quantile(badList[:, i], 1.0))
            print(buyLegend)
            print(badLegend)
            print("*******************************")

        if AP.IsTraining and not AP.IsTrained:
            for i in range(len(self.patternList[0])):
                curRules = rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.RelaxTheRules(buyList[:, i])
            AP.IsTrained = True
            AP.TotalStrikeCount = 0


        if rules.isTuned :
            for i in range(len(self.patternList[0])):
                curRules = rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.SetEliminationCounts(buyList[:, i], badList[:, i], 0.05)
                    rule.Print()

            isBadCountBigger = rules.SelectBestQuantile()
            if not isBadCountBigger:
                sys.exit()

        #if AP.IsTeaching:
        rules.Write(len(self.patternList), len(self.badPatternList))


        rules.ResetRules()
        rules.isTuned = True
        #plt.close()


    def MergeInTransactions(self, handler):
        for pattern in handler.patternList:
            if pattern is not None:
                features = pattern.GetFeatures(rules)
                if features is not None:
                    if IsMultiThreaded:
                        with lock:
                            self.patternList.append(features)
                    else:
                        self.patternList.append(features)

        for pattern in handler.badPatternList:
            if pattern is not None:
                features = pattern.GetFeatures(rules)
                if features is not None:
                    if IsMultiThreaded:
                        with lock:
                            self.badPatternList.append(features)
                    else:
                        self.badPatternList.append(features)

class SuddenChangeManager:

    def __init__(self, transactionParamList):
        self.marketState = MarketStateManager.MarketStateManager()
        self.FeedMarketState()
        self.isTest = False
        self.transParamList = transactionParamList
        self.suddenChangeMergerList = []
        self.CreateSuddenChangeMergers()
        print(self.suddenChangeMergerList)
        self.FeedChangeMergers()
        self.suddenChangeMergerList[0].Finalize(True)

        if AP.IsTeaching:
            for i in range(1000):
                self.suddenChangeMergerList = []
                self.CreateSuddenChangeMergers()
                self.FeedChangeMergers()
                self.suddenChangeMergerList[0].Finalize(True)
        else:
            self.isTest = True
            self.FeedChangeMergers()
            self.suddenChangeMergerList[1].Finalize(False)


    def FeedMarketState(self):
        jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/JumpData/"
        onlyJumpFiles = [f for f in listdir(jumpDataFolderPath) if isfile(join(jumpDataFolderPath, f))]
        riseCount = 0
        downCount = 0
        for fileName in onlyJumpFiles:
            print("Reading market state", jumpDataFolderPath + fileName, " ")
            sys.stdout.flush()
            file = open(jumpDataFolderPath + fileName, "r")
            epoch = datetime.utcfromtimestamp(0)
            try:
                jsonDictionary = json.load(file)
                for jsonIn in jsonDictionary:
                    if not jsonIn:
                        continue

                    tempTransaction = json.loads(jsonIn["transactions"])
                    if len(tempTransaction) == 0:
                        continue
                    timeStr = jsonIn["reportTime"]
                    datetime_object = datetime.strptime(timeStr.split(".")[0], '%Y-%b-%d %H:%M:%S')
                    reportTimeInSeconds = (datetime_object - epoch).total_seconds()
                    isRise = bool(jsonIn["isRise"])
                    if isRise:
                        riseCount += 1
                    else:
                        downCount += 1
                    self.marketState.add(isRise, reportTimeInSeconds)
            except Exception as e:
                print("There was a exception in ", fileName, e)
            if IsOneFileOnly:
                break
        self.marketState.sort()
        print("Total rise: ", riseCount, " total down: ", downCount)

    def ReadFile(self, fileName):
        file = open(fileName, "r")
        #print("Reading file: ", fileName)
        jsonDictionary = json.load(file)
        index = 1 if self.isTest else 0
        handlerList = self.suddenChangeMergerList[index].AddFile(jsonDictionary, fileName, self.isTest)
        for peak in handlerList:
            self.suddenChangeMergerList[index].MergeInTransactions(peak)
            del peak
        return

    def GetAllFiles(self, path, isSort):
        jumpDataFolderPath = os.path.abspath(os.getcwd()) + path
        onlyJumpFiles = [join(jumpDataFolderPath, f) for f in listdir(jumpDataFolderPath) if
                         isfile(join(jumpDataFolderPath, f))]
        if isSort:
            onlyJumpFiles.sort(key=lambda f: os.path.getsize(f), reverse=True)
        return onlyJumpFiles

    def FeedChangeMergers(self):
        allJumpFiles = self.GetAllFiles("/Data/JumpData/", True)
        if self.isTest or (AP.IsTraining and not AP.IsMachineLearning):
            allJumpFiles = self.GetAllFiles("/Data/TestData/", False)
        allExtraFiles = self.GetAllFiles("/Data/ExtraData/", False)
        allFiles = allJumpFiles
        if not self.isTest:
            allFiles = allJumpFiles + allExtraFiles
        if IsMultiThreaded:
            lock = multiprocessing.Lock()
            pool_obj = multiprocessing.Pool(initializer=init_pool_processes, initargs=(lock,), processes=32,maxtasksperchild=500)
            pool_obj.map(self.ReadFile, allFiles)
        else:
            for fileName in allFiles:
                self.ReadFile(fileName)




    def toTransactionFeaturesNumpy(self, isTest):
        if isTest:
            return self.suddenChangeMergerList[1].toTransactionFeaturesNumpy()
        else:
            return self.suddenChangeMergerList[0].toTransactionFeaturesNumpy()

    def toTransactionResultsNumpy(self, isTest):
        if isTest:
            return self.suddenChangeMergerList[1].toTransactionResultsNumpy()
        else:
            return self.suddenChangeMergerList[0].toTransactionResultsNumpy()

        #for transactionIndex in range(len(self.transParamList)):
        #    self.suddenChangeMergerList[transactionIndex].Finalize()

    def CreateSuddenChangeMergers(self):
        for transactionIndex in range(len(self.transParamList)):
            newMerger = SuddenChangeMerger(self.transParamList[transactionIndex], self.marketState)
            self.suddenChangeMergerList.append(newMerger)
            testMerger = SuddenChangeMerger(self.transParamList[transactionIndex], self.marketState)
            self.suddenChangeMergerList.append(testMerger)