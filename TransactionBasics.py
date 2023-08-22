import AdjustParameters as AP
import Peaks

PeakFeatureCount = 6
MaximumSampleSizeFromPattern = 100000
MaximumSampleSizeFromGoodPattern = 8
TransactionCountPerSecBase = 3
TransactionLimitPerSecBase = 0.1
TotalPowerLimit = 0.5
TotalElementLimitMsecs = 10000
MaxMinListTimes = [60*60*6,60*60*24,60*60*36]
IsUseMaxInList = True
MergeLenghtSize = 2
TransactionFeatureCount = 9

def GetMaxMinList(maxMinList):
    extraCount = len(MaxMinListTimes)
    if extraCount == 0:
        return []

    returnVal = []
    for index in range(extraCount * 2):
        if index % 2 == 0:
            returnVal.append(maxMinList[index])
        elif IsUseMaxInList:
            returnVal.append(maxMinList[index])
    return returnVal

def LastNElementsTransactionPower(list, index, elementCount):
    totalTradePower = 0
    for curIndex in range(index-elementCount, index+1):
        lastTotalTradePower = list[curIndex].totalBuy + list[curIndex].totalSell
        totalTradePower += lastTotalTradePower
    return totalTradePower

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def CreateTransactionList(inList):
    transPatternReturnVal = []
    newList = list(chunks(inList, 4))
    for listElem in newList:
        elem = BasicTransactionData(listElem)
        transPatternReturnVal.append(elem)
    return transPatternReturnVal

def GetListFromBasicTransData( inBasicTransactionDataList ):
    returnList = []
    for i in range(len(inBasicTransactionDataList)):
        returnList.append(inBasicTransactionDataList[i].transactionBuyCount)
        returnList.append(inBasicTransactionDataList[i].transactionSellCount)
        returnList.append(inBasicTransactionDataList[i].totalBuy)
        returnList.append(inBasicTransactionDataList[i].totalSell)
    return returnList

class TransactionParam:
    def __init__ ( self, msec, gramCount ):
        self.msec = msec
        self.gramCount = gramCount

    def __repr__(self):
        return "MSec:%d,GramCount:%d" % (self.msec, self.gramCount)

class BasicTransactionData:
    def __init__(self, list):
        self.totalBuy = list[2]
        self.totalSell = list[3]
        self.transactionBuyCount = list[0]
        self.transactionSellCount = list[1]

    def CombineData(self, otherData):
        self.transactionSellCount += otherData.transactionSellCount
        self.transactionBuyCount += otherData.transactionBuyCount
        self.totalBuy += otherData.totalBuy
        self.totalSell += otherData.totalSell

class TransactionData:
    def __init__(self):
        self.totalBuy = 0.0
        self.totalSell = 0.0
        self.transactionBuyCount = 0.0
        self.totalTransactionCount = 0.0
        self.score = 0
        self.timeInSecs = 0
        self.endTimeInSecs = 0
        self.minTimeInSecs = 0
        self.maxTimeInSecs = 0
        self.firstPrice = 0.0
        self.lastPrice = 0.0
        self.maxPrice = 0.0
        self.minPrice = 1000.0
        self.startIndex = 0
        self.endIndex = 0
        self.buyWall = 0.0
        self.sellWall = 0.0
        self.buyLongWall = 0.0
        self.sellLongWall = 0.0
        self.lastBuyWall = 0.0
        self.lastSellWall = 0.0
        self.lastBuyLongWall = 0.0
        self.lastSellLongWall = 0.0
    def __repr__(self):
        return "TotalBuy:%f,TotalSell:%f,TransactionCount:%f,Score:%f,LastPrice:%f,Time:%d" % (
        self.totalBuy, self.totalSell,
        self.transactionBuyCount, self.score, self.lastPrice, self.timeInSecs)

    def SellCount(self):
        return self.totalTransactionCount - self.transactionBuyCount

    def SetCurPrice(self, jsonIn):
        self.lastPrice = float(jsonIn["p"])

    def NormalizePrice(self):
        if self.firstPrice == 0.0:
            self.firstPrice = self.lastPrice
            self.maxPrice = self.firstPrice
            self.minPrice = self.firstPrice
    # "m": true, "l": 6484065,"M": true,"q": "44113.00000000","a": 5378484,"T": 1591976004949,"p": "0.00000225","f": 6484064
    def AddData(self, jsonIn):
        isSell = bool(jsonIn["m"])
        power = float(jsonIn["q"]) * float(jsonIn["p"])
        if self.firstPrice == 0.0:
            self.firstPrice = float(jsonIn["p"])
            self.minPrice = self.firstPrice
            self.maxPrice = self.firstPrice
        self.SetCurPrice(jsonIn)
        curTime = int(jsonIn["T"]) // 1000
        if self.lastPrice > self.maxPrice:
            self.maxPrice = self.lastPrice
            self.maxTimeInSecs = curTime
        if self.lastPrice < self.minPrice:
            self.minPrice = self.lastPrice
            self.minTimeInSecs = curTime
        self.endTimeInSecs = curTime

        self.totalTransactionCount += 1
        if "d" in jsonIn :
            buyList = jsonIn["d"].split(",")
            self.buyWall += float(buyList[0])
            self.sellWall += float(buyList[1])
            self.buyLongWall += float(buyList[2])
            self.sellLongWall += float(buyList[3])
            self.lastBuyWall = float(buyList[0])
            self.lastSellWall = float(buyList[1])
            self.lastBuyLongWall = float(buyList[2])
            self.lastSellLongWall = float(buyList[3])


        if not isSell:
            self.transactionBuyCount += 1
            self.totalBuy += power
        else:
            self.totalSell += power
            
    def Divide(self, dividor):

        if self.totalTransactionCount > 0 :
            self.buyWall /= self.totalTransactionCount
            self.sellWall /= self.totalTransactionCount
            self.buyLongWall /= self.totalTransactionCount
            self.sellLongWall /= self.totalTransactionCount

        self.transactionBuyCount /= dividor
        self.totalTransactionCount /= dividor
        self.totalSell /= dividor
        self.totalBuy /= dividor

    def SetTime(self, timeInSecs):
        self.timeInSecs = timeInSecs
        self.maxTimeInSecs = timeInSecs
        self.minTimeInSecs = timeInSecs
        self.endTimeInSecs = timeInSecs

    def SetIndex(self, index):
        if self.startIndex == 0:
            self.startIndex = index
        self.endIndex = index

    def Reset(self):
        self.totalBuy = 0.0
        self.totalSell = 0.0
        self.transactionBuyCount = 0.0
        self.totalTransactionCount = 0.0
        self.score = 0
        self.timeInSecs = 0
        self.endTimeInSecs = 0
        self.minTimeInSecs = 0
        self.maxTimeInSecs = 0
        self.firstPrice = 0.0
        self.lastPrice = 0.0
        self.maxPrice = 0.0
        self.minPrice = 1000.0
        self.startIndex = 0
        self.endIndex = 0

class TransactionPattern:
    def __init__(self):
        self.transactionBuyList = []
        self.transactionSellList = []
        self.transactionBuyPowerList = []
        self.transactionSellPowerList = []
        self.firstLastPriceList = []
        self.minMaxPriceList = []
        self.ratioFirstToJump = []
        self.buySellRatio = []
        self.dayPriceArray = []
        self.jumpCountList = []
        self.netPriceList = []
        self.upDownRangeBuyRatio = 10000000.0
        self.upDownRangeSellRatio = 10000000.0
        self.upDownRangeBuyRatioCount = 10000000.0
        self.upDownRangeSellRatioCount = 10000000.0
        self.firstToLastRatio = 1.0
        self.lastPrice = 0.0

        self.detailedTransactionList = []
        self.detailedHighestPowerNumber = 0.0
        self.detailedHighestCountNumber = 0
        self.detailLen = 0.0
        self.detailedHighestSellCountNumber = 0.0
        self.isMaxBuyLater = False

        self.maxDetailBuyCount = 0.0
        self.maxDetailBuyPower = 0.0

        self.lastDownRatio = 0.0
        self.lastUpRatio = 0.0
        self.lastDownRatioRise = 0.0
        self.lastUpRatioRise = 0.0
        self.lastRatio = 0.0
        self.lastDownRatio2 = 0.0
        self.lastUpRatio2 = 0.0
        self.lastDownRatioRise2 = 0.0
        self.lastUpRatioRise2 = 0.0
        self.upDownRangeRatio = 0.0

        self.totalBuy = 0.0
        self.totalSell = 0.0
        self.transactionCount = 0.0
        self.score = 0.0
        self.totalTransactionCount = 0
        self.timeDiffInSeconds = 0
        self.priceMaxRatio = 1.0
        self.priceDiff = 1.0
        self.priceMinRatio = 1.0
        self.marketStateList = []
        self.peaks = []
        self.timeList = []
        self.priceList = []
        self.isAvoidPeaks = True
        self.buyRatio = 1.0
        self.buyTimeDiffInSecs = 0
        self.buyInfoEnabled  = False

        self.totalPeakCount15M = 0
        self.totalPeakCount1Hour = 0
        self.totalPeakCount6Hour = 0
        self.totalPeakCount24Hour = 0
        self.timeToJump = 0

        self.after1MinMin = 1.0
        self.after1MinLast = 1.0

        self.after3MinMin = 1.0
        self.after3MinLast = 1.0

        self.after5MinMin = 1.0
        self.after5MinLast = 1.0

        self.after10MinMin = 1.0
        self.after10MinLast = 1.0

        self.buyWall = 0.0
        self.sellWall = 0.0
        self.buyLongWall = 0.0
        self.sellLongWall = 0.0
        self.buyWallRatio = 1.0
        self.sellWallRatio = 1.0
        self.averageVolume = 0.0
    def UpdatePrice(self, timeDiff, priceRatio ):
        if timeDiff < 60 :
            self.after1MinMin = min(self.after1MinMin, priceRatio)
            self.after3MinMin = min(self.after3MinMin, priceRatio)
            self.after5MinMin = min(self.after5MinMin, priceRatio)
            self.after10MinMin = min(self.after10MinMin, priceRatio)
        elif timeDiff < 180:
            self.after3MinMin = min(self.after3MinMin, priceRatio)
            self.after5MinMin = min(self.after5MinMin, priceRatio)
            self.after10MinMin = min(self.after10MinMin, priceRatio)
        elif timeDiff < 300:
            self.after5MinMin = min(self.after5MinMin, priceRatio)
            self.after10MinMin = min(self.after10MinMin, priceRatio)
        elif timeDiff < 600:
            self.after10MinMin = min(self.after10MinMin, priceRatio)

        if timeDiff > 58 and timeDiff < 62 :
            self.after1MinLast = priceRatio
        elif timeDiff > 178 and timeDiff < 182 :
            self.after3MinLast = priceRatio
        elif timeDiff > 298 and timeDiff < 302:
            self.after5MinLast = priceRatio
        elif timeDiff > 598 and timeDiff < 602:
            self.after10MinLast = priceRatio

    def GoalReached(self, timeDiff, goal):
        self.timeToJump = timeDiff
        if timeDiff < 60 :
            self.after1MinLast = goal
            self.after3MinLast = goal
            self.after5MinLast = goal
            self.after10MinLast = goal
        elif timeDiff < 180:
            self.after3MinLast = goal
            self.after5MinLast = goal
            self.after10MinLast = goal
        elif timeDiff < 300:
            self.after5MinLast = goal
            self.after10MinLast = goal
        elif timeDiff < 600:
            self.after10MinLast = goal

    def SetDetailedTransaction(self, detailedTransactionList, dataRange):
        self.detailedTransactionList = detailedTransactionList

        buyPowerList = list(map(lambda x: x.totalBuy, detailedTransactionList))
        buyCountList = list(map(lambda x: x.transactionBuyCount, detailedTransactionList))
        buySellList = list(map(lambda x: x.totalSell, detailedTransactionList))

        self.detailLen = len(buyPowerList)

        self.maxDetailBuyCount = max(buyCountList)
        self.maxDetailBuyPower = max(buyPowerList)
        maxSellVal = max(buySellList)
        if maxSellVal < 0.05:
            self.isMaxBuyLater = True
        else:
            self.isMaxBuyLater = buyPowerList.index(max(buyPowerList)) >= buySellList.index(maxSellVal)


    def GetCount(self, timeList, time):
        counter = 0
        totalTime = 0
        for x in reversed(timeList):
            totalTime += x
            counter += 1
            if time < totalTime:
                return counter
        return counter

    def GetPrice(self, curPrice, priceList, timeList, time):
        counter = 0
        totalTime = 0
        for x in reversed(timeList):
            totalTime += x
            counter += 1

            if time < totalTime:
                return curPrice/priceList[-counter]
        return curPrice/priceList[-counter]

    def SetPeaks(self, curPrice, curTime, candleSticks, dataList ):
        peakListAndTimeList = candleSticks.GetPeaks(curPrice, curTime, dataList)
        self.netPriceList = [curPrice / candleSticks.GetPrice(curTime, 60*60), curPrice / candleSticks.GetPrice(curTime, 60*60*8),
                             curPrice / candleSticks.GetPrice(curTime, 60*60*24), curPrice / candleSticks.GetPrice(curTime, 60*60*72),
                             curPrice / candleSticks.GetPrice(curTime, 60*60*168)]

        self.jumpCountList = [candleSticks.CountPeaks(curTime, 60*60*24),candleSticks.CountPeaks(curTime, 60*60*24*7)]
        self.peaks = peakListAndTimeList[0]
        self.timeList = peakListAndTimeList[1]
        self.priceList = peakListAndTimeList[2]
        if peakListAndTimeList[3] == Peaks.PriceTrendSide.DOWN:
            if len(self.priceList) > 3:
                self.lastDownRatio = self.priceList[-2]/self.priceList[-4]
            else:
                self.lastDownRatio = 1.0

            if len(self.priceList) > 4:
                self.lastUpRatio = self.priceList[-3] /self.priceList[-5]
            else:
                self.lastUpRatio = 1.0

            if len(self.priceList) > 5:
                self.lastDownRatio2 = self.priceList[-4]/self.priceList[-6]
            else:
                self.lastDownRatio2 = 1.0

            if len(self.priceList) > 6:
                self.lastUpRatio2 = self.priceList[-5] /self.priceList[-7]
            else:
                self.lastUpRatio2 = 1.0
        else:
            if len(self.priceList) > 4:
                self.lastDownRatio = self.priceList[-3]/self.priceList[-5]
            else:
                self.lastDownRatio = 1.0

            if len(self.priceList) > 3:
                self.lastUpRatio = self.priceList[-2] / self.priceList[-4]
            else:
                self.lastUpRatio = 1.0

            if len(self.priceList) > 6 :
                self.lastDownRatio2 = self.priceList[-5]/self.priceList[-7]
            else:
                self.lastDownRatio2 = 1.0

            if len(self.priceList) > 5:
                self.lastUpRatio2 = self.priceList[-4] / self.priceList[-6]
            else:
                self.lastUpRatio2 = 1.0


        self.lastRatio = self.peaks[-1]
        self.isAvoidPeaks = False
        return peakListAndTimeList[4]

    def Append(self, dataList, averageVolume, peakTime, jumpPrice, marketState):

        lastTime = dataList[-1].timeInSecs
        if marketState:
            self.marketStateList = marketState.getState(peakTime)
        else:
            self.marketStateList = []

        self.buyWall = dataList[-1].lastBuyWall
        self.sellWall = dataList[-1].lastSellWall
        self.buyLongWall = dataList[-1].lastBuyLongWall
        self.sellLongWall = dataList[-1].lastSellLongWall

        if dataList[1].buyLongWall != 0.0:
            self.buyWallRatio = dataList[-1].buyLongWall/dataList[1].buyLongWall
        if dataList[1].sellLongWall != 0.0:
            self.sellWallRatio = dataList[-1].sellLongWall/dataList[1].sellLongWall
        self.averageVolume = averageVolume


        for elem in dataList:
            self.transactionBuyList.append(elem.transactionBuyCount)
            self.transactionSellList.append(elem.totalTransactionCount - elem.transactionBuyCount)
            self.transactionBuyPowerList.append(elem.totalBuy)
            self.transactionSellPowerList.append(elem.totalSell)
            self.minMaxPriceList.append(elem.maxPrice/elem.minPrice)
            self.ratioFirstToJump.append(elem.firstPrice/dataList[-1].lastPrice)
            if elem.totalTransactionCount == 0:
                self.buySellRatio.append(0.0)
            else:
                self.buySellRatio.append(elem.transactionBuyCount / elem.totalTransactionCount )


            if elem.firstPrice == 0.0:
                self.firstLastPriceList.append(0.0)
            else:
                self.firstLastPriceList.append(elem.lastPrice/elem.firstPrice)
            self.totalBuy += elem.totalBuy
            self.totalSell += elem.totalSell
            self.transactionCount += elem.transactionBuyCount
            self.totalTransactionCount += elem.totalTransactionCount
            self.timeDiffInSeconds = lastTime - peakTime
            self.priceDiff = dataList[-1].lastPrice/jumpPrice


    def AppendWithOutPeaks(self, dataList, marketState, buyPrice, buyTimeInSecs):

        lastTime = dataList[-1].timeInSecs
        self.buyTimeDiffInSecs = lastTime - buyTimeInSecs
        self.buyRatio = dataList[-1].lastPrice/buyPrice
        self.buyInfoEnabled = False
        if marketState:
            self.marketStateList = marketState.getState(lastTime)[0:2]
            self.marketStateList.extend(marketState.getState(buyTimeInSecs)[0:2])
        else:
            self.marketStateList = []
        for elem in dataList:
            self.transactionBuyList.append(elem.transactionBuyCount)
            self.transactionSellList.append(elem.totalTransactionCount - elem.transactionBuyCount)
            self.transactionBuyPowerList.append(elem.totalBuy)
            self.transactionSellPowerList.append(elem.totalSell)
            self.totalBuy += elem.totalBuy
            self.totalSell += elem.totalSell
            self.transactionCount += elem.transactionBuyCount
            self.totalTransactionCount += elem.totalTransactionCount

    def TotalPower(self, i):
        return self.transactionBuyPowerList[i]+self.transactionSellPowerList[i]

    def GetFeatures(self, ruleList):
        returnList = []

        for i in range(len(self.transactionBuyList)):
            returnList.append(self.transactionBuyList[i]+self.transactionSellList[i])
            returnList.append(self.TotalPower(i))
            returnList.append(self.TotalPower(i)/self.averageVolume)
            returnList.append(self.buySellRatio[i])
            returnList.append(self.firstLastPriceList[i])

        index = len(self.transactionBuyList)*5

        returnList.append(self.maxDetailBuyPower)
        ruleList.SetIndex(AP.AdjustableParameter.MaxPowInDetail, index)
        index += 1

        returnList.append(self.buyWall)
        ruleList.SetIndex(AP.AdjustableParameter.BuyWall, index)
        index += 1

        returnList.append(self.sellWall)
        ruleList.SetIndex(AP.AdjustableParameter.SellWall, index)
        index += 1

        returnList.append(self.buyLongWall)
        ruleList.SetIndex(AP.AdjustableParameter.BuyLongWall, index)
        index += 1

        returnList.append(self.sellLongWall)
        ruleList.SetIndex(AP.AdjustableParameter.SellLongWall, index)
        index += 1

        returnList.append(self.averageVolume)
        ruleList.SetIndex(AP.AdjustableParameter.AverageVolume, index)
        index += 1

        returnList.append(self.jumpCountList[-2])
        ruleList.SetIndex(AP.AdjustableParameter.JumpCount24H, index)
        index += 1
        returnList.append(self.netPriceList[0])
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice1H, index)
        index += 1
        returnList.append(self.netPriceList[1])
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice8H, index)
        index += 1
        returnList.append(self.netPriceList[2])
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice24H, index)
        index += 1
        returnList.append(self.netPriceList[3])
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice72H, index)
        index += 1
        returnList.append(self.netPriceList[4])
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice168H, index)

        index += 1
        returnList.append(self.peaks[-1])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast0, index)
        index += 1
        returnList.append(self.timeList[-1])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime0, index)
        index += 1
        returnList.append(self.timeList[-2])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime1, index)
        index += 1
        returnList.append(self.lastDownRatio)
        ruleList.SetIndex(AP.AdjustableParameter.DownPeakRatio0, index)
        index += 1
        returnList.append(self.lastUpRatio)
        ruleList.SetIndex(AP.AdjustableParameter.UpPeakRatio0, index)

        returnList.append(self.peaks[-2])
        if len(self.timeList) > 3:
            returnList.append(self.timeList[-3])
            returnList.append(self.timeList[-4])
        else:
            returnList.append(self.timeList[-1])
            returnList.append(self.timeList[-1])
        returnList.append(self.lastDownRatio2)
        returnList.append(self.lastUpRatio2)

        #returnList.append(self.timeToJump

        return returnList

    def __repr__(self):
        return "list:%s,timeDiff:%d,totalBuy:%f,totalSell:%f,transactionCount:%f,score:%f" % (
            str(self.transactionBuyList), self.timeDiffInSeconds,
            self.totalBuy, self.totalSell,
            self.transactionCount, self.score)

