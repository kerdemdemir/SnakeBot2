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
IsMultiThreaded = True
percent = 0.01
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
    def __init__(self, jsonIn, transactionParam,marketState, fileName):
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
        self.isAfterBuyRecord = fileName.startswith("Positive")
        self.smallestStrikeCount = 1000
        self.bestPattern = None
        self.averageVolume = 0.0

        if self.__Parse(jsonIn) == -1:
            return

        buySellPriceRatio = self.reportPrice / self.jumpPrice
        if self.isAfterBuyRecord:
            if self.isRise:
                if buySellPriceRatio < 1.02:
                    return
            else:
                if buySellPriceRatio > 1.0:
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
            print("1 hour diff")
            self.jumpTimeInSeconds -= 60*60*2
            self.reportTimeInSeconds -= 60*60*2
        elif diffTime > 60*25:
            print("2 hour diff")
            self.jumpTimeInSeconds -= 60*60
            self.reportTimeInSeconds -= 60*60
            return

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
        datetime_object = datetime.strptime(jsonIn["reportTime"].split(".")[0], '%Y-%b-%d %H:%M:%S')
        now = datetime.now()

        self.reportTimeInSeconds = (datetime_object - epoch).total_seconds()
        if "avarageVolume" in jsonIn:
            self.averageVolume = jsonIn["avarageVolume"]

        candleSticks = jsonIn["candleStickData"]
        self.candleDataList.feed(candleSticks)

        datetime_object = datetime.strptime(jsonIn["time"].split(".")[0], '%Y-%b-%d %H:%M:%S')
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
            curTimeInSeconds = self.dataList[x].timeInSecs
            if  curTimeInSeconds > self.reportTimeInSeconds+10:
                continue
            lastTotalTradePower = self.dataList[x].totalBuy + self.dataList[x].totalSell
            if lastTotalTradePower > maxTradeVal:
                maxTradeVal = lastTotalTradePower
            self.__AppendToPatternListImpl(self.transactionParam.gramCount, x, lenArray, jsonIn)
        if len(self.patternList) == 0:
            self.dataList.reverse()

        limit = 0
        if AP.IsTeaching or AP.IsMachineLearning:
            limit = TransactionBasics.MaximumSampleSizeFromGoodPattern
        if len(self.patternList) > limit:
            if AP.IsTraining:
                self.patternList.clear()
                self.patternList.append(self.bestPattern)
            else:
                self.patternList = random.sample(self.patternList,
                                                 TransactionBasics.MaximumSampleSizeFromGoodPattern - 1)

        if len(self.badPatternList) > TransactionBasics.MaximumSampleSizeFromPattern:
            randomSampleList = random.sample(self.badPatternList, TransactionBasics.MaximumSampleSizeFromPattern-1)
            self.badPatternList = randomSampleList

        buySellPriceRatio = self.reportPrice / self.jumpPrice

        timeDiff = time.time() - self.reportTimeInSeconds
        if AP.IsTeaching and self.isAfterBuyRecord and timeDiff < (60*60*24*7):
            print("Extending the list because it happened very soon")
            if len(self.badPatternList) < 15:
                self.badPatternList.extend(self.badPatternList)
            if buySellPriceRatio < 0.97 :
                print("Double Extending the list because it happened very soon")
                self.badPatternList.extend(self.badPatternList)

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

    def __AppendToPatternListImpl(self, ngramCount, curIndex, lenArray, jsonIn):
        totalCount = 30
        startBin = curIndex + 1 - totalCount
        if curIndex > lenArray:
             return
        curPattern = self.dataList[curIndex]
        curTimeInMiliSecs = jsonIn[curPattern.endIndex]["T"]
        interestTime = 0
        #if interestTime - curTimeInMiliSecs < 10000 and self.currencyName == "SCRT":
        #    print("ALERT")

        if startBin < 0 :
            return
        if curPattern.totalBuy < 0.03:
            return

        pattern = TransactionBasics.TransactionPattern()


        lastPrice = curPattern.lastPrice
        currentPowSum = 0.0

        actualEndIndex = curPattern.endIndex
        if interestTime != 0 and interestTime - curTimeInMiliSecs < 10000 :
            curTimeInMiliSecs = interestTime
            for x in range(curPattern.startIndex, curPattern.endIndex):
                if jsonIn[x]["T"] > curTimeInMiliSecs:
                    actualEndIndex = x
                    lastPrice = float(jsonIn[x]["p"])
                    break
        else:
            for x in range(curPattern.startIndex,curPattern.endIndex):
                if not bool(jsonIn[x]["m"]):
                    currentPowSum += float(jsonIn[x]["q"]) * float(jsonIn[x]["p"])
                    lastPrice = float(jsonIn[x]["p"])
                if currentPowSum > 0.03:
                    actualEndIndex = x
                    curTimeInMiliSecs = jsonIn[x]["T"]
                    break

        restPowerSum = 0.0
        for x in range(actualEndIndex, len(jsonIn)):
            restPowerSum += float(jsonIn[x]["q"]) * float(jsonIn[x]["p"])
        timeDiffInSeconds = (self.reportTimeInSeconds - curTimeInMiliSecs//1000 )
        actualAvarageVolume = (self.averageVolume * 60 * 60 * 6 - restPowerSum) / (60 * 60 * 6 - timeDiffInSeconds)

        #if AP.IsTraningUpPeaks :
        #    if AP.IsWorkingLowVolumes and actualAvarageVolume > 0.0003:
        #        return
        #    if not AP.IsWorkingLowVolumes and actualAvarageVolume < 0.0003:
        #        return

        isUpOrDownTrend = pattern.SetPeaks(lastPrice, curTimeInMiliSecs//1000, self.candleDataList, self.dataList)
        #if AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.DOWN:
        #    return
        #if not AP.IsTraningUpPeaks and isUpOrDownTrend == Peaks.PriceTrendSide.UP:
        #    return

        pattern.firstToLastRatio = self.dataList[0].firstPrice / lastPrice
        #self.__GetWithTime(jsonIn, curTimeInMiliSecs - 10000, curTimeInMiliSecs, 10)
        firstData = self.__GetWithTime(jsonIn, 0, curTimeInMiliSecs - 610000, curTimeInMiliSecs - 130000, 480)
        secondData = self.__GetWithTime(jsonIn, firstData.endIndex - 1, curTimeInMiliSecs - 130000, curTimeInMiliSecs - 10000, 120)
        lastData = self.__GetWithTime(jsonIn, secondData.endIndex - 1, curTimeInMiliSecs - 10000, curTimeInMiliSecs, 10)
        dataRange = [firstData, secondData, lastData]

        basePrice = lastPrice
        pattern.lastPrice = lastPrice

        pattern.timeToJump = self.reportTimeInSeconds - self.dataList[curIndex].timeInSecs

        moreDetailDataList = []
        self.__DivideDataInSeconds(jsonIn, 250, moreDetailDataList, secondData.endIndex, lastData.endIndex)
        pattern.SetDetailedTransaction(moreDetailDataList, dataRange)
        pattern.Append( dataRange, actualAvarageVolume, self.jumpTimeInSeconds, self.jumpPrice, self.marketState)
        if pattern.marketStateList[1] > 3:
            return

        k = 0
        rules.strikeCount = 0
        for i in range(len(pattern.transactionBuyList)):
            if rules.ControlClampIndex(k,pattern.transactionBuyList[i]+pattern.transactionSellList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.TotalPower(i)):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.buySellRatio[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.firstLastPriceList[i]):
                return
            k+=2

        #if rules.ControlClamp(AP.AdjustableParameter.DetailLen, pattern.detailLen):
        #    return

        if rules.ControlClamp(AP.AdjustableParameter.MaxPowInDetail, pattern.maxDetailBuyPower):
            return

        if rules.ControlClamp(AP.AdjustableParameter.BuyWall, pattern.buyWall):
            return
        if rules.ControlClamp(AP.AdjustableParameter.SellWall, pattern.sellWall):
            return
        if rules.ControlClamp(AP.AdjustableParameter.BuyLongWall, pattern.buyLongWall):
            return
        if rules.ControlClamp(AP.AdjustableParameter.SellLongWall, pattern.sellLongWall):
            return

        if rules.ControlClamp(AP.AdjustableParameter.AverageVolume, pattern.averageVolume):
            return

        if rules.ControlClamp(AP.AdjustableParameter.JumpCount8H, pattern.jumpCountList[1]):
           return

        if rules.ControlClamp(AP.AdjustableParameter.NetPrice1H, pattern.netPriceList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice8H, pattern.netPriceList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice24H, pattern.netPriceList[2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice72H, pattern.netPriceList[3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice168H, pattern.netPriceList[4]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakTime0, pattern.timeList[-1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime1, pattern.timeList[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime2, pattern.timeList[-3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime3, pattern.timeList[-4]):
            return

        #if rules.ControlClamp(AP.AdjustableParameter.MarketState, pattern.marketStateList[1]):
        #    return
        if rules.strikeCount < self.smallestStrikeCount:
            self.bestPattern = pattern

        self.smallestStrikeCount = min(self.smallestStrikeCount, rules.strikeCount)
        #print(pattern.marketStateList)
        category = self.__GetCategory(curIndex,basePrice,pattern)
        if category == 0:
            self.mustBuyList.append(pattern)
        elif category == 1:
            self.addedCount += 1
            self.patternList.append(pattern)
            #print(basePrice, self.currencyName)
        elif category == 2:
            self.badPatternList.append(pattern)
            self.addedCount += 1

    def __GetCategory(self, curIndex, priceIn, pattern):
        if self.isRise:
            if self.isAfterBuyRecord:
                return 1
            if priceIn < self.reportPrice * 0.97:
                for i in range(curIndex+1, len(self.dataList)):
                    ratio = self.dataList[i].lastPrice / priceIn
                    timeDiff = self.dataList[i].endIndex - self.dataList[curIndex].endIndex
                    #pattern.UpdatePrice(timeDiff, ratio)
                    if ratio<0.98:
                        return -1
                    if ratio>1.025:
                        pattern.GoalReached(timeDiff, 1.025)
                        return 1
                return -1
        else:
            if self.isAfterBuyRecord:
                return 2
            for i in range(curIndex, len(self.dataList)):
                if self.dataList[i].lastPrice/priceIn<0.985:
                    timeDiff = self.dataList[i].endIndex - self.dataList[curIndex].endIndex
                    pattern.GoalReached(timeDiff, 1.025)
                    return 2
                if self.dataList[i].lastPrice/priceIn>1.03:
                    return -1
            return 2

        return -1

class SuddenChangeMerger:

    def __init__(self, transactionParam, marketState):
        self.mustBuyList = []
        self.patternList = []
        self.badPatternList = []

        self.handlerList = []
        self.peakHelperList = []
        self.transactionParam = transactionParam
        self.marketState = marketState
        print("Alert2: ")


    def AddFile(self, jsonIn, fileName):
        tempHandlers = []
        for index in range(len(jsonIn)):
            if not jsonIn[index]:
                continue

            jsonPeakTrans = jsonIn[index]

            handler = SuddenChangeHandler(jsonPeakTrans,self.transactionParam,self.marketState,fileName)
            tempHandlers.append(handler)
        return tempHandlers


    def Finalize(self, isPrint):
        for peak in self.handlerList:
            self.__MergeInTransactions(peak)
            del peak
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
                    rule.SetFromValue(buyList[:, i], False)
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
                    rule.SetEliminationCounts(buyList[:, i], badList[:, i], 0.1)
                    rule.Print()

            isBadCountBigger = rules.SelectBestQuantile()
            if not isBadCountBigger:
                sys.exit()

        #if AP.IsTeaching:
        rules.Write(len(self.patternList), len(self.badPatternList))


        rules.ResetRules()
        rules.isTuned = True
        #plt.close()


    def __MergeInTransactions(self, handler):
        for pattern in handler.patternList:
            self.patternList.append(pattern.GetFeatures(rules) + handler.GetFeatures())

        for pattern in handler.badPatternList:
            self.badPatternList.append(pattern.GetFeatures(rules) + handler.GetFeatures())

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
        jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/JumpData/"
        if self.isTest or (AP.IsTraining and not AP.IsMachineLearning):
            jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/TestData/"
        print("Reading Jump", jumpDataFolderPath + fileName, " ")
        file = open(jumpDataFolderPath + fileName, "r")
        # try:
        jsonDictionary = json.load(file)
        return self.suddenChangeMergerList[0].AddFile(jsonDictionary, fileName)

        # except Exception as e:
        #    print("There was a exception in ", fileName, e )
        #if IsOneFileOnly:
        #    break
    def FeedChangeMergers(self):
        jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/JumpData/"
        if self.isTest or (AP.IsTraining and not AP.IsMachineLearning):
            jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/TestData/"
        onlyJumpFiles = [f for f in listdir(jumpDataFolderPath) if isfile(join(jumpDataFolderPath, f))]
        if IsMultiThreaded:
            lock = multiprocessing.Lock()
            pool_obj = multiprocessing.Pool(initializer=init_pool_processes, initargs=(lock,), processes=16,maxtasksperchild=500)
            for handlerList in pool_obj.map(self.ReadFile, onlyJumpFiles):
                if self.isTest:
                    self.suddenChangeMergerList[1].handlerList.extend(handlerList)
                else:
                    self.suddenChangeMergerList[0].handlerList.extend(handlerList)
        else:
            for fileName in onlyJumpFiles:
                if self.isTest:
                    self.suddenChangeMergerList[1].handlerList.extend(self.ReadFile(fileName))
                else:
                    self.suddenChangeMergerList[0].handlerList.extend(self.ReadFile(fileName))



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