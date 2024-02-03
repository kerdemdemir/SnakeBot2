import numpy as np

import AdjustParameters as AP
import Peaks
import TransactionBasics as TB
from typing import List


class Features:
    def __init__(self):
        self.transactionBuyList = []
        self.transactionSellList = []
        self.transactionBuyPowerList = []
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
        self.peaks = []
        self.timeList = []
        self.priceList = []
        self.timeToJump = 0

        self.longPeaks = []
        self.longTimeList = []
        self.longPrices = []
        self.buyWall = np.nan
        self.sellWall = np.nan
        self.buyLongWall = np.nan
        self.sellLongWall = np.nan

        self.averageBuyWall = np.nan
        self.averageSellWall = np.nan
        self.averageBuyLongWall = np.nan
        self.averageSellLongWall = np.nan

        self.averageVolume = 0.0
        self.isExtra = False
        self.patternTime = 0
        self.name = ""

    def GoalReached(self, timeDiff: int) -> None:
        self.timeToJump = timeDiff

    def SetDetailedTransaction(self, lastPattern: TB.TransactionData) -> None:
        self.detailLen = lastPattern.transactionBuyCount
        self.maxDetailBuyPower = lastPattern.totalBuy

    def SetPeaks(self, curPrice: float, curTime: int, candleSticks: Peaks.CandleDataList,
                     dataList: List[TB.TransactionData]) -> Peaks.PriceTrendSide:
        peakListAndTimeList = candleSticks.GetPeaks(curPrice, curTime, dataList, False)
        longPeakListAndTimeList = candleSticks.GetPeaks(curPrice, curTime, dataList, True)
        self.netPriceList = [curPrice / candleSticks.GetPrice(curTime, 60 * 60),
                             curPrice / candleSticks.GetPrice(curTime, 60 * 60 * 8),
                             curPrice / candleSticks.GetPrice(curTime, 60 * 60 * 24),
                             curPrice / candleSticks.GetPrice(curTime, 60 * 60 * 24 * 10)]

        self.jumpCountList = [candleSticks.CountPeaks(curTime, 60 * 10), candleSticks.CountPeaks(curTime, 60 * 60),
                              candleSticks.CountPeaks(curTime, 60 * 60 * 12)]
        self.jumpCountList2 = [candleSticks.CountPeaks(curTime, 60 * 60 * 6, True),
                               candleSticks.CountPeaks(curTime, 60 * 60 * 24, True),
                               candleSticks.CountPeaks(curTime, 60 * 60 * 72, True)]

        self.peaks = peakListAndTimeList[0][-10:]
        self.timeList = peakListAndTimeList[1][-10:]
        self.priceList = peakListAndTimeList[2][-10:]

        self.longPeaks = longPeakListAndTimeList[0][-10:]
        self.longTimeList = longPeakListAndTimeList[1][-10:]
        self.longPrices = longPeakListAndTimeList[2][-10:]

        if len(self.longTimeList) < 4:
            self.timeList = self.timeList[:2]

        return peakListAndTimeList[4]

    def Append(self, dataList: List[TB.TransactionData], averageVolume: float, peakTime: int, jumpPrice: float) -> None:
        lastTime = dataList[-1].timeInSecs
        self.averageVolume = averageVolume

        if dataList[-1].lastBuyLongWall != 0.0:
            self.buyWall = dataList[-1].lastBuyWall
            self.sellWall = dataList[-1].lastSellWall
            self.buyLongWall = dataList[-1].lastBuyLongWall
            self.sellLongWall = dataList[-1].lastSellLongWall

            self.averageBuyWall = dataList[-2].lastBuyWall / dataList[-1].lastBuyWall
            self.averageSellWall = dataList[-2].lastSellWall / dataList[-1].lastSellWall
            self.averageBuyLongWall = dataList[-2].lastBuyLongWall / dataList[-1].lastBuyLongWall
            self.averageSellLongWall = dataList[-2].lastSellLongWall / dataList[-1].lastSellLongWall

        for elem in dataList:
            self.transactionBuyList.append(elem.transactionBuyCount)
            self.transactionSellList.append(elem.totalTransactionCount - elem.transactionBuyCount)
            self.transactionBuyPowerList.append(elem.totalBuy)
            self.transactionSellPowerList.append(elem.totalSell)
            if elem.totalTransactionCount == 0:
                self.buySellRatio.append(0.0)
            else:
                self.buySellRatio.append(elem.transactionBuyCount / elem.totalTransactionCount)

            if elem.firstPrice == 0.0:
                self.firstLastPriceList.append(0.0)
            else:
                self.firstLastPriceList.append(elem.lastPrice / elem.firstPrice)
            self.totalBuy += elem.totalBuy
            self.totalSell += elem.totalSell
            self.transactionCount += elem.transactionBuyCount
            self.totalTransactionCount += elem.totalTransactionCount
            self.timeDiffInSeconds = lastTime - peakTime
            self.priceDiff = dataList[-1].lastPrice / jumpPrice

    def TotalPower(self, i: int) -> None:
        return self.transactionBuyPowerList[i] + self.transactionSellPowerList[i]

    def GetFeatures(self, ruleList: AP.RuleList) -> List[float]:
        returnList = []

        for i in range(len(self.transactionBuyList)):
            returnList.append(self.transactionBuyList[i] + self.transactionSellList[i])
            returnList.append(self.transactionBuyPowerList[i])
            returnList.append(self.transactionSellPowerList[i])
            returnList.append(self.firstLastPriceList[i])

        index = len(self.transactionBuyList) * 4

        returnList.append(self.TotalPower(0) / self.averageVolume)
        ruleList.SetIndex(AP.AdjustableParameter.MaxPowInDetail, index)
        index += 1

        returnList.append(self.averageVolume)
        ruleList.SetIndex(AP.AdjustableParameter.AverageVolume, index)
        index += 1
        returnList.append(self.jumpCountList[0])
        ruleList.SetIndex(AP.AdjustableParameter.JumpCount10M, index)
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
        returnList.append(self.timeList[-4])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime0, index)
        index += 1
        returnList.append(self.timeList[-2])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime1, index)
        index += 1
        returnList.append(self.timeList[-3])
        ruleList.SetIndex(AP.AdjustableParameter.PeakTime2, index)
        index += 1
        returnList.append(self.peaks[-2])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast1, index)
        index += 1
        returnList.append(self.peaks[-3])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast2, index)
        index += 1
        returnList.append(self.peaks[-4])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast3, index)
        index += 1
        returnList.append(self.peaks[-5])
        ruleList.SetIndex(AP.AdjustableParameter.PeakLast4, index)
        index += 1

        downPeakRatioLast = self.priceList[-3] / self.priceList[-5]
        upPeakRatioLast = self.priceList[-2] / self.priceList[-4]
        returnList.append(downPeakRatioLast)
        ruleList.SetIndex(AP.AdjustableParameter.DownPeakRatio0, index)
        index += 1
        returnList.append(upPeakRatioLast)
        ruleList.SetIndex(AP.AdjustableParameter.UpPeakRatio0, index)
        index += 1
        returnList.append(upPeakRatioLast / downPeakRatioLast)
        ruleList.SetIndex(AP.AdjustableParameter.DownPeakRatio1, index)
        index += 1
        ###########
        downPeakRatioLast1 = self.priceList[-5] / self.priceList[-7]
        upPeakRatioLast1 = self.priceList[-4] / self.priceList[-6]
        returnList.append(downPeakRatioLast1)
        returnList.append(upPeakRatioLast1)
        returnList.append(upPeakRatioLast / downPeakRatioLast)
        #################################
        returnList.append(self.longPeaks[-1])
        returnList.append(self.longPeaks[-2])
        returnList.append(self.longPeaks[-3])
        returnList.append(self.longPeaks[-4])
        returnList.append(self.TotalPower(1) / self.averageVolume)
        returnList.append(self.longTimeList[-1])
        returnList.append(self.longTimeList[-2])
        returnList.append(self.longTimeList[-3])

        downPeakRatioLast = self.longPrices[-2] / self.priceList[-3]
        upPeakRatioLast = self.longPrices[-2] / self.priceList[-4]
        returnList.append(downPeakRatioLast)
        returnList.append(upPeakRatioLast)
        # returnList.append(self.TotalPower(1)/self.averageVolume)
        returnList.append(self.longPrices[-2] / self.priceList[-5])
        downPeakRatioLast = self.longPrices[-2] / self.priceList[-2]
        upPeakRatioLast = self.longPrices[-3] / self.priceList[-3]
        returnList.append(downPeakRatioLast)
        returnList.append(upPeakRatioLast)
        returnList.append(self.longPrices[-4] / self.priceList[-4])

        returnList.append(self.buyWall)
        returnList.append(self.sellWall)
        returnList.append(self.buyLongWall)
        returnList.append(self.sellLongWall)
        returnList.append(self.buyWall / self.sellWall)
        returnList.append(self.buyLongWall / self.sellLongWall)
        returnList.append(self.buyLongWall / self.buyWall)
        returnList.append(self.sellLongWall / self.sellWall)
        # returnList.append(self.averageBuyWall)
        # returnList.append(self.averageSellWall)
        # returnList.append(self.averageBuyLongWall)
        # returnList.append(self.averageSellLongWall)
        returnList.append(self.buyWall / self.TotalPower(2))
        returnList.append(self.sellWall / self.TotalPower(2))
        returnList.append(self.buyLongWall / self.TotalPower(2))
        returnList.append(self.sellLongWall / self.TotalPower(2))

        # ruleList.SetIndex(AP.AdjustableParameter.UpPeakRatio1, index)
        # index += 1
        return returnList

    def __repr__(self):
        return "list:%s,timeDiff:%d,totalBuy:%f,totalSell:%f,transactionCount:%f" % (
            str(self.transactionBuyList), self.timeDiffInSeconds,
            self.totalBuy, self.totalSell,
            self.transactionCount)
