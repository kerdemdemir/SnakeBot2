import AdjustParameters as AP
import Peaks

PeakFeatureCount = 6
MaximumSampleSizeFromPattern = 100000
MaximumSampleSizeFromGoodPattern = 6
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
        return "TotalBuy:%f,TotalSell:%f,TransactionCount:%f,LastPrice:%f,Time:%d" % (
        self.totalBuy, self.totalSell,
        self.transactionBuyCount, self.lastPrice, self.timeInSecs)

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
        self.lastPrice = float(jsonIn["p"])
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
        self.   transactionBuyPowerList = []
        self.transactionSellPowerList = []
        self.firstLastPriceList = []
        self.buySellRatio = []
        self.jumpCountList = []
        self.jumpCountList2 = []
        self.netPriceList = []
        self.firstToLastRatio = 1.0
        self.lastPrice = 0.0
        self.detailLen = 0.0
        self.maxDetailBuyPower = 0.0
        self.totalBuy = 0.0
        self.totalSell = 0.0
        self.transactionCount = 0.0
        self.totalTransactionCount = 0
        self.timeDiffInSeconds = 0
        self.priceDiff = 1.0
        self.marketStateList = []
        self.peaks = []
        self.timeList = []
        self.priceList = []
        self.timeToJump = 0

        self.longPeaks = []
        self.longTimeList = []
        self.longPrices = []
        self.buyWall = 0.0
        self.sellWall = 0.0
        self.buyLongWall = 0.0
        self.sellLongWall = 0.0
        self.averageVolume = 0.0

    def GoalReached(self, timeDiff, goal):
        self.timeToJump = timeDiff

    def SetDetailedTransaction(self, lastPattern):
        #TransactionBasics.TransactionData
        self.detailLen = lastPattern.transactionBuyCount
        self.maxDetailBuyPower = lastPattern.totalBuy


    def SetPeaks(self, curPrice, curTime, candleSticks, dataList ):
        peakListAndTimeList = candleSticks.GetPeaks(curPrice, curTime, dataList, False)
        #longPeakListAndTimeList = candleSticks.GetPeaks(curPrice, curTime, dataList, True)
        self.netPriceList = [curPrice / candleSticks.GetPrice(curTime, 60*60),
                             curPrice / candleSticks.GetPrice(curTime, 60 * 60 * 8),
                             curPrice / candleSticks.GetPrice(curTime, 60*60*24),
                             curPrice / candleSticks.GetPrice(curTime, 60*60*24*10)]

        self.jumpCountList = [candleSticks.CountPeaks(curTime, 60*10), candleSticks.CountPeaks(curTime, 60*60), candleSticks.CountPeaks(curTime, 60*60*12)]

        self.peaks = peakListAndTimeList[0][-10:]
        self.timeList = peakListAndTimeList[1][-10:]
        self.priceList = peakListAndTimeList[2][-10:]
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
        self.averageVolume = averageVolume


        for elem in dataList:
            self.transactionBuyList.append(elem.transactionBuyCount)
            self.transactionSellList.append(elem.totalTransactionCount - elem.transactionBuyCount)
            self.transactionBuyPowerList.append(elem.totalBuy)
            self.transactionSellPowerList.append(elem.totalSell)
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

    def TotalPower(self, i):
        return self.transactionBuyPowerList[i]+self.transactionSellPowerList[i]

    def GetFeatures(self, ruleList):
        returnList = []

        for i in range(len(self.transactionBuyList)):
            returnList.append(self.transactionBuyList[i]+self.transactionSellList[i])
            returnList.append(self.transactionBuyPowerList[i])
            returnList.append(self.transactionSellPowerList[i])
            returnList.append(self.firstLastPriceList[i])

        index = len(self.transactionBuyList)*4

        returnList.append(self.maxDetailBuyPower)
        ruleList.SetIndex(AP.AdjustableParameter.MaxPowInDetail, index)
        index += 1

        returnList.append(self.averageVolume)
        ruleList.SetIndex(AP.AdjustableParameter.AverageVolume, index)
        index += 1

        returnList.append(self.jumpCountList[1])
        ruleList.SetIndex(AP.AdjustableParameter.JumpCount1H, index)
        index += 1
        returnList.append(self.jumpCountList[2])
        ruleList.SetIndex(AP.AdjustableParameter.JumpCount12H, index)
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
        ruleList.SetIndex(AP.AdjustableParameter.NetPrice168H, index)
        index += 1
        returnList.append(self.timeList[-1])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime0, index)
        index += 1
        returnList.append(self.peaks[-1])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast0, index)
        index += 1
        returnList.append(self.peaks[-2])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast1, index)
        index += 1

        downPeakRatioLast = self.priceList[-3] / self.priceList[-5]
        upPeakRatioLast = self.priceList[-2] / self.priceList[-4]
        returnList.append(upPeakRatioLast/downPeakRatioLast)
        ruleList.SetIndex(AP.AdjustableParameter.DownPeakRatio0, index)
        index += 1
        returnList.append(upPeakRatioLast)
        ruleList.SetIndex(AP.AdjustableParameter.UpPeakRatio0, index)
        index += 1
        downPeakRatioLast1 = self.priceList[-5] / self.priceList[-7]
        upPeakRatioLast1 = self.priceList[-4] / self.priceList[-6]
        #returnList.append(upPeakRatioLast1)
        #ruleList.SetIndex(AP.AdjustableParameter.UpPeakRatio1, index)
        #index += 1

        return returnList

    def __repr__(self):
        return "list:%s,timeDiff:%d,totalBuy:%f,totalSell:%f,transactionCount:%f" % (
            str(self.transactionBuyList), self.timeDiffInSeconds,
            self.totalBuy, self.totalSell,
            self.transactionCount)

