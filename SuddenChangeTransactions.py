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
from matplotlib import pyplot as plt
import sys

PeakFeatureCount = TransactionBasics.PeakFeatureCount
percent = 0.01
IsOneFileOnly = False
totalCounter = 0
rules = AP.RuleList()

class SuddenChangeHandler:

    def __init__(self, jsonIn, transactionParam,marketState, fileName):
        self.marketState = marketState
        self.jumpTimeInSeconds = 0
        self.reportTimeInSeconds = 0
        self.reportPrice = 0.0
        self.jumpPrice = 0.0
        self.transactions = []
        self.maxMinList = []
        self.riseList = []
        self.timeList = []
        self.currencyName = ""
        self.isRise = False
        self.downUpList = []
        self.transactionParam = transactionParam
        self.extraControlDataList = []
        self.priceList = []
        self.patternList = []
        self.mustBuyList = []
        self.badPatternList = []

        self.mustSellList = []
        self.keepList = []
        self.addedCount  = 0
        self.isAfterBuyRecord = fileName.startswith("Positive")

        self.jumpState = []
        self.__Parse(jsonIn)

        if self.isAfterBuyRecord and self.isRise :
            return
        if self.isAfterBuyRecord :
            if self.jumpPrice/self.reportPrice>1.0:
                return

        self.lowestTransaction = TransactionBasics.TransactionCountPerSecBase
        self.acceptedTransLimit = TransactionBasics.TransactionLimitPerSecBase
        self.dataList = []
        tempTransaction = json.loads(jsonIn["transactions"])
        if len(tempTransaction) == 0:
            return
        lastTimeInSeconds = int(tempTransaction[-1]["T"]) // 1000
        if self.reportTimeInSeconds - lastTimeInSeconds > 1500:
            self.jumpTimeInSeconds -= 7200
            self.reportTimeInSeconds -= 7200

        self.minIndex = 0
        self.maxIndex = 0
        self.maxPrice = 0
        self.minPrice = 1000
        for index in range(len(tempTransaction)):
            transaction = tempTransaction[index]
            curTimeInSeconds = int(transaction["T"]) // 1000
            curPrice = float(transaction["p"])
            if curTimeInSeconds < self.jumpTimeInSeconds - 10 or curTimeInSeconds > self.reportTimeInSeconds+10:
                continue
            if curPrice <= self.minPrice:
                self.minPrice = curPrice
                self.minIndex = index
                self.minTime = curTimeInSeconds
            if curPrice >= self.maxPrice:
                self.maxPrice = curPrice
                self.maxIndex = index
                self.maxTime = curTimeInSeconds

        if self.isRise:
            self.peakIndex = self.minIndex
        else:
            self.peakIndex = self.maxIndex

        self.peakTime = int(tempTransaction[self.peakIndex]["T"])//1000
        self.peakVal = float(tempTransaction[self.peakIndex]["p"])

        if self.timeList[-1] < 2.0:
            self.timeList = self.timeList[:-1]
            self.riseList = self.riseList[:-1]
            if self.riseList[-1] > 0.0 :
                self.riseList[-1] = (self.jumpPrice/self.priceList[-3]-1.0 )*100.0
            else:
                self.riseList[-1] = (-self.jumpPrice/self.priceList[-3]+1.0)*100.0


        self.__DivideDataInSeconds(tempTransaction, self.transactionParam.msec, self.dataList, 0, len(tempTransaction)) #populates the dataList with TransactionData
        self.__AppendToPatternList(tempTransaction) # deletes dataList and populates mustBuyList, patternList badPatternList

    def GetPeakFeatures(self, time):
        returnVal = 0
        counter = 0
        totalTime = 0
        for x in reversed(self.timeList):
            totalTime += x
            counter -= 1
            curVal = self.riseList[counter]
            returnVal = min(curVal, returnVal)
            if time < totalTime:
                return returnVal
        return returnVal

    def GetCount(self, time):
        counter = 0
        totalTime = 0
        for x in reversed(self.timeList):
            totalTime += x
            counter += 1
            if time < totalTime:
                return counter
        return counter

    def GetFeatures(self):
        return self.downUpList
        #maxRise = [self.GetPeakFeatures(60 * 6), self.GetPeakFeatures(60 * 24), self.GetPeakFeatures(60 * 24 * 3), self.GetPeakFeatures(60 * 24 * 7 ), self.GetPeakFeatures(60 * 24 * 30 ), self.GetPeakFeatures(60 * 24 * 90 )]
        #maxRise = [self.GetCount(60 * 6), self.GetCount(60 * 24), self.GetCount(60 * 24 * 3), self.GetCount(60 * 24 * 7 ), self.GetCount(60 * 24 * 30 ), self.GetCount(60 * 24 * 90 )]
        #return maxRise
        #return TransactionBasics.GetMaxMinList( self.maxMinList ) #+ maxRise
        #return []
       #return self.timeList[-PeakFeatureCount:] + self.riseList[-PeakFeatureCount:]
       #return [self.riseList[-1] / self.riseList[-3], self.riseList[-2] / self.riseList[-4], self.riseList[-3] / self.riseList[-5], self.riseList[-4] / self.riseList[-6]]
        #return []

    def __Parse(self, jsonIn):
        epoch = datetime.utcfromtimestamp(0)

        self.isRise = bool(jsonIn["isRise"])
        self.jumpPrice = float(jsonIn["jumpPrice"])
        self.reportPrice = float(jsonIn["reportPrice"])
        datetime_object = datetime.strptime(jsonIn["reportTime"].split(".")[0], '%Y-%b-%d %H:%M:%S')
        self.reportTimeInSeconds = (datetime_object - epoch).total_seconds()
        self.riseList = jsonIn["riseList"]
        self.timeList = jsonIn["timeList"]
        self.priceList = jsonIn["priceList"]
        #extraString = jsonIn["extraRecordData"]
        #self.extraControlDataList = extraString.split("\n")
        for i in range(len(self.riseList) - 1 ):
            if self.riseList[i]*self.riseList[i+1] > 0.0:
                self.riseList.pop(i)
                self.timeList.pop(i)
                #TransactionBasics.RiseListSanitizer(self.riseList, self.timeList)


        self.maxMinList = jsonIn["maxMin"]
        datetime_object = datetime.strptime(jsonIn["time"].split(".")[0], '%Y-%b-%d %H:%M:%S')
        self.jumpTimeInSeconds = (datetime_object - epoch).total_seconds()
        self.downUpList = jsonIn["downUps"]
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
            if lastTotalTradePower > maxTradeVal  :
                maxTradeVal = lastTotalTradePower
            self.__AppendToPatternListImpl(self.transactionParam.gramCount, x, lenArray, jsonIn)
        if len(self.patternList) == 0:
            self.dataList.reverse()

        if len(self.patternList) > TransactionBasics.MaximumSampleSizeFromGoodPattern:
            sorted(self.patternList, key=lambda l: l.lastPrice)
            self.patternList = [self.patternList[0]]

        if len(self.badPatternList) > TransactionBasics.MaximumSampleSizeFromPattern:
            sorted(self.badPatternList, key=lambda l: l.lastPrice)
            lastPrice = self.badPatternList[-1].lastPrice
            self.badPatternList = [self.badPatternList[-1]]
            if TransactionBasics.MaximumSampleSizeFromPattern > 1 and len(self.badPatternList) > 1 :
                randomSampleList = random.sample(self.badPatternList, TransactionBasics.MaximumSampleSizeFromPattern-1)
                for elem in randomSampleList:
                    if elem.lastPrice != lastPrice:
                        self.badPatternList.append(elem)

        del self.dataList

    def __GetWithTime(self, jsonIn, startTime, stopTime, divider):
        currentData = TransactionBasics.TransactionData()

        isFirst = True
        for elem in jsonIn :
            curTime = float(elem["T"])
            currentData.SetCurPrice(elem)
            if curTime < startTime:
                continue
            if isFirst:
                startTime = curTime
                isFirst = False
            if curTime > stopTime:
                break
            currentData.AddData(elem)

        currentData.NormalizePrice()
        currentData.Divide( (stopTime- startTime)/1000)
        return currentData





    def __AppendToPatternListImpl(self, ngramCount, curIndex, lenArray, jsonIn):
        totalCount = 71
        startBin = curIndex + 1 - totalCount
        endBin = curIndex + 1
        if startBin < 0 or curIndex > lenArray:
             return

        curPattern = self.dataList[curIndex]
        if curPattern.totalBuy < 0.1:
            return
        if self.dataList[0].firstPrice == 0.0:
            print("wierd")
        pattern = TransactionBasics.TransactionPattern()


        lastPrice = curPattern.lastPrice
        currentPowSum = 0.0
        curTimeInMiliSecs = jsonIn[curPattern.endIndex]["T"]
        for x in range(curPattern.startIndex,curPattern.endIndex):
            if not bool(jsonIn[x]["m"]):
                currentPowSum += float(jsonIn[x]["q"]) * float(jsonIn[x]["p"])
                lastPrice = float(jsonIn[x]["p"])
            if currentPowSum > 0.1:
                curTimeInMiliSecs = jsonIn[x]["T"]
                break

        pattern.firstToLastRatio = self.dataList[0].firstPrice / lastPrice

        dataRange = [self.__GetWithTime(jsonIn, curTimeInMiliSecs - 453000, curTimeInMiliSecs - 93000, 360.0),
                     self.__GetWithTime(jsonIn, curTimeInMiliSecs - 93000, curTimeInMiliSecs - 33000, 60.0),
                     self.__GetWithTime(jsonIn, curTimeInMiliSecs - 33000, curTimeInMiliSecs - 3000, 30.0),
                     self.__GetWithTime(jsonIn, curTimeInMiliSecs - 3000, curTimeInMiliSecs+1, 3.0)]

        basePrice = lastPrice
        pattern.lastPrice = lastPrice
        ratio = basePrice/self.jumpPrice
        curTimeDiff = (self.dataList[curIndex].timeInSecs - self.jumpTimeInSeconds)//60
        pattern.timeToJump = self.reportTimeInSeconds - self.dataList[curIndex].timeInSecs
        pattern.SetPeaks(lastPrice, self.priceList, self.riseList, self.timeList, self.maxMinList, ratio, curTimeDiff)

        moreDetailDataList = []
        self.__DivideDataInSeconds(jsonIn, 100, moreDetailDataList, self.dataList[curIndex].startIndex, self.dataList[curIndex].endIndex+1)
        pattern.SetDetailedTransaction(moreDetailDataList, dataRange)
        pattern.Append( dataRange, self.jumpTimeInSeconds, self.jumpPrice, self.marketState)

        k = 0
        for i in range(len(pattern.transactionBuyList)):
            if rules.ControlClampIndex(k,pattern.transactionBuyList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.transactionSellList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.transactionBuyPowerList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.transactionSellPowerList[i]):
                return
            k+=2
            if rules.ControlClampIndexDivider(k, pattern.transactionBuyList[i], pattern.transactionBuyList[0]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.minMaxPriceList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.firstLastPriceList[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.ratioFirstToJump[i]):
                return
            k+=2
            if rules.ControlClampIndex(k, pattern.buySellRatio[i]):
                return
            k+=2

        if rules.ControlClamp(AP.AdjustableParameter.DetailLen, pattern.detailLen):
            return
        if rules.ControlClamp(AP.AdjustableParameter.MaxPowInDetail, pattern.maxDetailBuyPower):
            return
        if rules.ControlClamp(AP.AdjustableParameter.DownPeakRatio0, pattern.lastDownRatio):
            return
        if rules.ControlClamp(AP.AdjustableParameter.UpPeakRatio0, pattern.lastUpRatio):
            return
        if rules.ControlClamp(AP.AdjustableParameter.DownPeakRatio1, pattern.lastDownRatio2):
            return
        if rules.ControlClamp(AP.AdjustableParameter.UpPeakRatio1, pattern.lastUpRatio2):
            return
        if rules.ControlClamp(AP.AdjustableParameter.DownPeakRatioRise0, pattern.lastDownRatioRise):
            return
        if rules.ControlClamp(AP.AdjustableParameter.UpPeakRatioRise0, pattern.lastUpRatioRise):
            return
        if rules.ControlClamp(AP.AdjustableParameter.DownPeakRatioRise1, pattern.lastDownRatioRise2):
            return
        if rules.ControlClamp(AP.AdjustableParameter.UpPeakRatioRise1, pattern.lastUpRatioRise2):
            return


        if rules.Control(AP.AdjustableParameter.HourPriceRatioMin6, AP.CheckType.Small,  pattern.dayPriceArray[0]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMax6, AP.CheckType.Big  , pattern.dayPriceArray[1]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMin24, AP.CheckType.Small, pattern.dayPriceArray[2]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMax24, AP.CheckType.Big , pattern.dayPriceArray[3]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMin72, AP.CheckType.Small , pattern.dayPriceArray[6]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMax72, AP.CheckType.Big, pattern.dayPriceArray[7]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMin144, AP.CheckType.Small, pattern.dayPriceArray[8]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMax144, AP.CheckType.Big, pattern.dayPriceArray[9]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMin288, AP.CheckType.Small, pattern.dayPriceArray[10]):
            return
        if rules.Control(AP.AdjustableParameter.HourPriceRatioMax288, AP.CheckType.Big, pattern.dayPriceArray[11]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.FirstToLastRaio, pattern.firstToLastRatio):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakLast0, pattern.peaks[-1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast1, pattern.peaks[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast2, pattern.peaks[-3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast3, pattern.peaks[-4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakLast4, pattern.peaks[-5]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.PeakTime0, pattern.timeList[-1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime1, pattern.timeList[-2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime2, pattern.timeList[-3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime3, pattern.timeList[-4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.PeakTime4, pattern.timeList[-5]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.JumpCount1H, pattern.jumpCountList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount2H, pattern.jumpCountList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount4H, pattern.jumpCountList[2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount8H, pattern.jumpCountList[3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount24H, pattern.jumpCountList[4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.JumpCount72H, pattern.jumpCountList[5]):
            return

        if rules.ControlClamp(AP.AdjustableParameter.NetPrice1H, pattern.netPriceList[0]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice2H, pattern.netPriceList[1]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice4H, pattern.netPriceList[2]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice8H, pattern.netPriceList[3]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice24H, pattern.netPriceList[4]):
            return
        if rules.ControlClamp(AP.AdjustableParameter.NetPrice72H, pattern.netPriceList[5]):
            return

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
            if priceIn < self.reportPrice * 0.9:
                for i in range(curIndex+1, len(self.dataList)):
                    ratio = self.dataList[i].lastPrice / priceIn
                    timeDiff = self.dataList[i].endIndex - self.dataList[curIndex].endIndex
                    #pattern.UpdatePrice(timeDiff, ratio)
                    if ratio<0.98:
                        return -1
                    if ratio>1.03:
                        pattern.GoalReached(timeDiff, 1.03)
                        return 1
                return -1
        else:
            for i in range(curIndex, len(self.dataList)):
                if self.dataList[i].lastPrice/priceIn<0.985:
                    return 2
                if self.dataList[i].lastPrice/priceIn>1.03:
                    return -1
            return 2

        return -1

    def __GetCategorySell(self, curIndex):
        price = self.dataList[curIndex].lastPrice
        minVal = self.dataList[curIndex].minPrice
        maxVal = self.dataList[curIndex].maxPrice
        time = self.dataList[curIndex].timeInSecs

        if self.isRise:
            if minVal < self.peakVal * 1.03:
                return 2  # We can keep

        else:
            if maxVal > self.peakVal * 0.995:
                return 1 # We need to sell now
        return -1

class SuddenChangeMerger:

    def __init__(self, transactionParam, marketState):
        self.mustBuyList = []
        self.patternList = []
        self.badPatternList = []

        self.mustSellList = []
        self.keepList = []

        self.handlerList = []
        self.peakHelperList = []
        self.transactionParam = transactionParam
        self.marketState = marketState

    def AddFile(self, jsonIn, fileName):
        for index in range(len(jsonIn)):
            if not jsonIn[index]:
                continue

            jsonPeakTrans = jsonIn[index]

            handler = SuddenChangeHandler(jsonPeakTrans,self.transactionParam,self.marketState,fileName)
            self.handlerList.append(handler)

    def Finalize(self):
        for peak in self.handlerList:
            self.__MergeInTransactions(peak)
            del peak
        self.Print()

    def toTransactionFeaturesNumpy(self):
        badCount = len(self.badPatternList)
        goodCount = len(self.patternList)
        #self.Print()
        #mustBuyCount = len(self.mustBuyList)
        print("Good count: ", goodCount, " Bad Count: ", badCount)

        allData = np.concatenate( (self.patternList, self.badPatternList), axis=0)
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

    def toSellTransactions(self):
        mustSellCount = len(self.mustSellList)
        keepCount = len(self.keepList)
        #self.Print()
        #mustBuyCount = len(self.mustBuyList)
        allData = np.concatenate( (self.mustSellList, self.keepList), axis=0)
        #print(allData)
        print("Must sell count: ", mustSellCount, " Keep count: ", keepCount)
        return allData

    def toSellResultsNumpy(self):
        mustSellCount = len(self.mustSellList)
        keepCount = len(self.keepList)

        print("Must sell count: ", mustSellCount, " Keep count: ", keepCount)
        mustSellResult = [0] * keepCount
        keepResult  = [1] * mustSellCount
        returnPatternList = mustSellResult + keepResult
        return returnPatternList

    def Print(self):

        buyList = np.array(self.patternList)
        badList = np.array(self.badPatternList)
        print("Good count: ", len(self.patternList), "Bad Count" , len(self.badPatternList))

        for i in range(len(self.patternList[0])):
            curRules = rules.GetRulesWithIndex(i)
            for rule in curRules:
                if not rules.isTuned :
                    rule.SetFromValue(buyList[:, i])
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
        if rules.isTuned :
            for i in range(len(self.patternList[0])):
                curRules = rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.SetEliminationCounts(buyList[:, i], badList[:, i], 0.02)
                    rule.Print()

            isBadCountBigger = rules.SelectBestQuantile()
            if not isBadCountBigger:
                sys.exit()

        rules.Write(len(self.patternList), len(self.badPatternList))
        rules.ResetRules()
        rules.isTuned = True
        #plt.close()


    def __MergeInTransactions(self, handler):
        for pattern in handler.patternList:
            self.patternList.append(pattern.GetFeatures(rules) + handler.GetFeatures())

        for pattern in handler.mustBuyList:
            self.mustBuyList.append(pattern.GetFeatures(rules) + handler.GetFeatures())

        for pattern in handler.badPatternList:
            self.badPatternList.append(pattern.GetFeatures(rules) + handler.GetFeatures())

        for pattern in handler.mustSellList:
            self.mustSellList.append(pattern.GetFeatures(rules) )

        for pattern in handler.keepList:
            self.keepList.append(pattern.GetFeatures(rules))


class SuddenChangeManager:

    def __init__(self, transactionParamList):
        self.marketState = MarketStateManager.MarketStateManager()
        #self.FeedMarketState()

        self.transParamList = transactionParamList
        self.suddenChangeMergerList = []
        self.CreateSuddenChangeMergers()
        print(self.suddenChangeMergerList)
        self.FeedChangeMergers()
        self.FinalizeMergers()

        for i in range(1000):
            self.suddenChangeMergerList = []
            self.CreateSuddenChangeMergers()
            self.FeedChangeMergers()
            self.FinalizeMergers()

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

                    lastTimeInSeconds = int(tempTransaction[-1]["T"]) // 1000
                    if reportTimeInSeconds - lastTimeInSeconds > 1500:
                        reportTimeInSeconds -= 3600
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

    def FeedChangeMergers(self):
        jumpDataFolderPath = os.path.abspath(os.getcwd()) + "/Data/JumpData/"
        onlyJumpFiles = [f for f in listdir(jumpDataFolderPath) if isfile(join(jumpDataFolderPath, f))]
        for fileName in onlyJumpFiles:
            print("Reading Jump", jumpDataFolderPath + fileName, " ")
            file = open(jumpDataFolderPath + fileName, "r")
            #try:
            jsonDictionary = json.load(file)
            for merger in self.suddenChangeMergerList:
                merger.AddFile(jsonDictionary, fileName)
            #except Exception as e:
            #    print("There was a exception in ", fileName, e )
            if IsOneFileOnly:
                break

    def toTransactionFeaturesNumpy(self, index):
        return self.suddenChangeMergerList[index].toTransactionFeaturesNumpy()

    def toTransactionResultsNumpy(self, index):
        return self.suddenChangeMergerList[index].toTransactionResultsNumpy()

    def toSellTransactions(self, index):
        return self.suddenChangeMergerList[index].toSellTransactions()

    def toSellResultsNumpy(self, index):
        return self.suddenChangeMergerList[index].toSellResultsNumpy()

    def FinalizeMergers(self):
        for transactionIndex in range(len(self.transParamList)):
            self.suddenChangeMergerList[transactionIndex].Finalize()

    def CreateSuddenChangeMergers(self):
        for transactionIndex in range(len(self.transParamList)):
            newMerger = SuddenChangeMerger(self.transParamList[transactionIndex], self.marketState)
            self.suddenChangeMergerList.append(newMerger)