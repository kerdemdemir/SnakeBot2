import copy

from multipledispatch import dispatch
from enum import Enum
import bisect


class TimePriceBasic:
    def __init__( self, timeInSeconds, priceIn ) :
        self.timeInSec = timeInSeconds
        self.price = priceIn

    def __lt__(self, other):
        return self.timeInSec < other.timeInSec

class PriceTrendSide(Enum):
    UP = "UP",
    DOWN = "DOWN",
    NO_TREND = "NOT INITED",
class PeakData:
    def __init__(self, price, time):
        self.minVal = TimePriceBasic(price, time)
        self.maxVal = TimePriceBasic(price, time)

    @dispatch()
    def GetChange(self):
        returnVal = self.maxVal.price / self.minVal.price
        currentTrend = self.IsUpOrDownPeak()
        if currentTrend == PriceTrendSide.UP:
            return (returnVal - 1.0) * 100.0
        else:
            return (-returnVal + 1.0) * 100.0

    @dispatch(float)
    def GetChange(self, val):
        currentTrend = self.IsUpOrDownPeak()
        if currentTrend == PriceTrendSide.UP:
            returnVal = val/self.minVal.price
            return (returnVal - 1.0) * 100.0
        else:
            returnVal = self.maxVal.price / val
            return (-returnVal + 1.0) * 100.0

    @dispatch(float,float)
    def GetChange(self, valCompare, valPeak):
        currentTrend = self.IsUpOrDownPeak()
        if currentTrend == PriceTrendSide.DOWN:
            returnVal = valCompare / valPeak
            return (-returnVal + 1.0) * 100.0
        else:
            returnVal = valPeak / valCompare
            return (returnVal - 1.0) * 100.0

    @dispatch()
    def GetTimeDiff(self):
        if self.IsUpOrDownPeak() == PriceTrendSide.UP:
            return self.maxVal.timeInSec - self.minVal.timeInSec
        else:
            return self.minVal.timeInSec - self.maxVal.timeInSec

    @dispatch(int)
    def GetTimeDiff(self, time):
        return time-self.GetStartTime()


    def GetStartTime(self):
        return min(self.minVal.timeInSec, self.maxVal.timeInSec)

    def GetEndTime(self):
        return max(self.minVal.timeInSec, self.maxVal.timeInSec)

    def GetLastPrice(self):
        if self.IsUpOrDownPeak() == PriceTrendSide.UP:
            return self.maxVal.price
        else:
            return self.minVal.price

    def GetFirstPrice(self):
        if self.IsUpOrDownPeak() == PriceTrendSide.UP:
            return self.minVal.price
        else:
            return self.maxVal.price

    def IsTimeBetween(self, timeSec):
        return self.GetStartTime() < timeSec < self.GetEndTime()
    def IsTimeAfter(self, timeSec):
        return self.GetStartTime() < timeSec and self.GetEndTime() < timeSec
    def IsTimeBefore(self, timeSec):
        return self.GetStartTime() > timeSec and self.GetEndTime() > timeSec
    def Update(self, price, time):
        if price < self.minVal.price:
            self.minVal.price = price
            self.minVal.timeInSec = time

        if price > self.maxVal.price:
            self.maxVal.price = price
            self.maxVal.timeInSec = time

    def UpdateAfterPeak(self, price, time):
        if self.IsUpOrDownPeak() == PriceTrendSide.UP:
            self.minVal.price = price
            self.minVal.timeInSec = time
        else:
            self.maxVal.price = price
            self.maxVal.timeInSec = time

    def UpdateAndSetThePeak(self, price, time, peakSize):
        if self.IsPeakFinished(price, peakSize):
            self.UpdateAfterPeak(price, time)
        else:
            self.Update(price, time)
    def IsPeakFinished(self, price, peakSize):
        currentTrend = self.IsUpOrDownPeak()
        if currentTrend == PriceTrendSide.NO_TREND:
            return False
        elif currentTrend == PriceTrendSide.UP:
            return price < self.maxVal.price * (1.0 - peakSize) or price < self.minVal.price
        else:
            return price > self.minVal.price * (1.0 + peakSize) or price > self.maxVal.price
    def IsUpOrDownPeak(self):
        if self.maxVal.price / self.minVal.price < 1.015:
            return PriceTrendSide.NO_TREND
        elif self.maxVal.timeInSec > self.minVal.timeInSec:
            return PriceTrendSide.UP
        else:
            return PriceTrendSide.DOWN



class PeakList:
    def __init__(self):
        self.peakDataList = []
        self.curStream = PeakData(0.0000000000001, 0.0000000000001)
        self.peakSize = 0.05

    def CurveUpdate(self, price, time):
        if self.curStream.IsPeakFinished(price, self.peakSize):
            self.peakDataList.append(copy.deepcopy(self.curStream))
            self.curStream.UpdateAfterPeak(price, time)
        else:
            self.curStream.Update(price, time)

    def IsPeakFinished(self, price):
        return self.curStream.IsPeakFinished(price, self.peakSize)

    def IsUpOrDownPeak(self):
        return self.curStream.IsUpOrDownPeak()

class CandleDataBasic:
    def __init__( self, startTimeSec, endTimeSec, openPrice, closePrice, minPrice, maxPrice ) :
        self.startTimeSec = startTimeSec
        self.endTimeSec = endTimeSec
        self.openPrice = openPrice
        self.closePrice = closePrice
        self.minPrice = minPrice
        self.maxPrice = maxPrice

    def __init__(self, jsonStr):
        jsonStrList = jsonStr.split(",")
        self.startTimeSec = int(jsonStrList[0])
        self.endTimeSec = int(jsonStrList[1])
        self.openPrice = float(jsonStrList[2])
        self.closePrice = float(jsonStrList[3])
        self.minPrice = float(jsonStrList[4])
        self.maxPrice = float(jsonStrList[5])

    def DurationInSecs(self):
        return self.endTimeSec - self.startTimeSec
    def IsTimeBefore(self, timeInSec):
        return self.endTimeSec > timeInSec and self.startTimeSec > timeInSec
    def IsTimeAfter(self, timeInSec):
        return self.endTimeSec < timeInSec and self.startTimeSec < timeInSec
    def IsTimeBetween(self, timeInSec):
        return self.endTimeSec > timeInSec > self.startTimeSec


class CandleDataList:

    def __init__(self):
        self.candleStickDataList = []
        self.peaks = PeakList()
    def feed(self, jsonIn):
        self.candleStickDataList = []
        self.peaks = PeakList()
        for candleStick in jsonIn:
            candleData = CandleDataBasic(str(candleStick))
            self.candleStickDataList.append(candleData)
            maxDiff = candleData.closePrice-candleData.maxPrice
            minDiff = candleData.closePrice-candleData.minPrice
            middleTime = (candleData.endTimeSec+candleData.startTimeSec)//2
            if abs(minDiff) > abs(maxDiff) :
                self.peaks.CurveUpdate(candleData.minPrice, middleTime)
                self.peaks.CurveUpdate(candleData.maxPrice, candleData.endTimeSec)
            else:
                self.peaks.CurveUpdate(candleData.maxPrice, middleTime)
                self.peaks.CurveUpdate(candleData.minPrice, candleData.endTimeSec)
        self.peaks.peakDataList.pop(0)
    def CountPeaks(self, timeSec, durationSec ):
        counter = 0
        tempList = self.peaks.peakDataList + [self.peaks.curStream]
        for peak in reversed(tempList):
            if peak.IsTimeBefore(timeSec):
                continue
            if peak.IsTimeAfter(timeSec-durationSec):
                break
            counter += 1
        return counter

    def GetPrice(self, timeSec, durationSec):
        startIndex = bisect.bisect_left(self.candleStickDataList, timeSec-durationSec,
                                        key=lambda candleData: candleData.endTimeSec)
        return self.candleStickDataList[startIndex].closePrice


    def GetMaxPriceTime(self, startTime, endTime, endPrice):
        outputPrice = endPrice
        outputTime = endTime
        for candleData in reversed(self.candleStickDataList):
            if not candleData.IsTimeAfter(endTime):
                continue
            if candleData.maxPrice > outputPrice:
                outputPrice = candleData.maxPrice
                outputTime = candleData.startTimeSec
            if candleData.IsTimeAfter(startTime):
                break
        return outputPrice, outputTime

    def GetMinPriceTime(self, startTime, endTime, endPrice):
        outputPrice = endPrice
        outputTime = endTime
        for candleData in reversed(self.candleStickDataList):
            if not candleData.IsTimeAfter(endTime):
                continue
            if candleData.minPrice < outputPrice:
                outputPrice = candleData.minPrice
                outputTime = candleData.startTimeSec
            if candleData.IsTimeAfter(startTime):
                break
            return outputPrice, outputTime

    def FeedCandleSticks(self, peak, lastTime, dividedTimeList):
        startTime = peak.GetEndTime()
        startIndex = bisect.bisect_left(self.candleStickDataList, lastTime, key=lambda candleData: candleData.endTimeSec)
        for index in range(startIndex-1, len(self.candleStickDataList)):
            candleData = self.candleStickDataList[index]
            if candleData.endTimeSec < startTime:
                continue
            if candleData.IsTimeBetween(lastTime):
                startIndex = bisect.bisect_right(dividedTimeList, candleData.startTimeSec, key=lambda x: x.timeInSecs)
                for indexInner in range(startIndex, len(dividedTimeList)):
                    curData = dividedTimeList[indexInner]
                    if curData.timeInSecs > lastTime:
                        break
                    peak.UpdateAndSetThePeak(curData.lastPrice, curData.timeInSecs, self.peaks.peakSize)

            if candleData.endTimeSec > lastTime:
                break
            peak.UpdateAndSetThePeak(candleData.closePrice, candleData.endTimeSec, self.peaks.peakSize)

    def GetPeaks(self, curPrice, curTime, dividedTimeList ):
        startStream = self.peaks.curStream
        tempList = self.peaks.peakDataList + [startStream]
        for peak in reversed(tempList):
            if peak.IsTimeAfter(curTime):
                if not peak.IsPeakFinished(curPrice, self.peaks.peakSize):
                    startStream = peak
                    break
                else:
                    tempPeakData = copy.deepcopy(peak)
                    self.FeedCandleSticks(tempPeakData, curTime, dividedTimeList)
                    tempPeakData.Update(curPrice, curTime)
                    startStream = tempPeakData
                break

        curEndTime = startStream.GetEndTime()
        curLastPrice = startStream.GetLastPrice()
        curTrend = startStream.IsUpOrDownPeak()

        changeList = []
        timeList = []
        newPriceList = []
        #if curTrend == PriceTrendSide.DOWN and (curPrice/curLastPrice >§ 1.08 or curPrice/curLastPrice < 0.99):
        #    print("Alert")
        #elif curTrend == PriceTrendSide.UP and (curPrice/curLastPrice < 0.92 or curPrice/curLastPrice > 1.01):
        #    print("Alert2")
        changeList.append(curPrice/curLastPrice)
        timeList.append(curTime-curEndTime)
        newPriceList.append(curPrice)
        changeList.append(startStream.GetChange())
        timeList.append(startStream.GetTimeDiff())
        newPriceList.append(curLastPrice)

        for peak in reversed(self.peaks.peakDataList):
            if peak.IsTimeAfter(startStream.GetStartTime()+1):
                changeList.append(peak.GetChange())
                timeList.append(peak.GetTimeDiff())
                newPriceList.append(peak.GetLastPrice())
        changeList.reverse()
        timeList.reverse()
        newPriceList.reverse()

        return changeList, timeList, newPriceList, startStream.IsUpOrDownPeak()
