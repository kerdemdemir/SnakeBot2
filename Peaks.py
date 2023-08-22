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
    def __init__(self, peakSize):
        self.peakDataList = []
        self.curStream = PeakData(0.0000000000001, 0.0000000000001)
        self.peakSize = peakSize


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
        self.peaks = PeakList(0.08)
        self.smallPeaks = PeakList(0.035)

    def feed(self, jsonIn):
        self.peaks = PeakList(0.08)
        self.feedPeaks(jsonIn, self.peaks)
        self.smallPeaks = PeakList(0.03)
        self.feedPeaks(jsonIn, self.smallPeaks)
    def feedPeaks(self, jsonIn, peakList):
        self.candleStickDataList = []
        for candleStick in jsonIn:
            candleData = CandleDataBasic(str(candleStick))
            self.candleStickDataList.append(candleData)
            maxDiff = candleData.closePrice-candleData.maxPrice
            minDiff = candleData.closePrice-candleData.minPrice
            middleTime = (candleData.endTimeSec+candleData.startTimeSec)//2
            if abs(minDiff) > abs(maxDiff) :
                peakList.CurveUpdate(candleData.minPrice, middleTime)
                peakList.CurveUpdate(candleData.maxPrice, candleData.endTimeSec)
            else:
                peakList.CurveUpdate(candleData.maxPrice, middleTime)
                peakList.CurveUpdate(candleData.minPrice, candleData.endTimeSec)
        if len(peakList.peakDataList)>0:
            peakList.peakDataList.pop(0)
    def CountPeaks(self, timeSec, durationSec ):
        counter = 0
        tempList = self.smallPeaks.peakDataList + [self.smallPeaks.curStream]
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

    def FeedCandleSticks(self, peak, lastTime, dividedTimeList, peakSize):
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
                    peak.UpdateAndSetThePeak(curData.firstPrice, curData.timeInSecs, peakSize)

            if candleData.endTimeSec > lastTime:
                break
            peak.UpdateAndSetThePeak(candleData.closePrice, candleData.endTimeSec, self.peaks.peakSize)

    def GetStartPeak(self, curPrice, curTime, dividedTimeList, peakList):
        startStream = peakList.curStream
        tempList = peakList.peakDataList + [startStream]
        for peak in reversed(tempList):
            if peak.IsTimeAfter(curTime):
                if not peak.IsPeakFinished(curPrice, peakList.peakSize):
                    startStream = peak
                    break
                else:
                    tempPeakData = copy.deepcopy(peak)
                    self.FeedCandleSticks(tempPeakData, curTime, dividedTimeList, peakList.peakSize)
                    tempPeakData.Update(curPrice, curTime)
                    startStream = tempPeakData
                break
        return startStream

    def GetPeaks(self, curPrice, curTime, dividedTimeList ):
        startStream = self.GetStartPeak(curPrice, curTime, dividedTimeList, self.peaks)

        curEndTime = startStream.GetEndTime()
        curLastPrice = startStream.GetLastPrice()

        changeList = []
        timeList = []
        newPriceList = []

        changeList.append(curPrice/curLastPrice)
        timeList.append(curTime-curEndTime)
        newPriceList.append(curPrice)
        longTermTrend = startStream.IsUpOrDownPeak()
        startStream = self.GetStartPeak(curPrice, curTime, dividedTimeList, self.smallPeaks)
        changeList.append(startStream.GetChange())
        timeList.append(startStream.GetTimeDiff())
        newPriceList.append(curLastPrice)

        for peak in reversed(self.smallPeaks.peakDataList):
            if peak.IsTimeAfter(startStream.GetStartTime()+1):
                changeList.append(peak.GetChange())
                timeList.append(peak.GetTimeDiff())
                newPriceList.append(peak.GetLastPrice())
        changeList.reverse()
        timeList.reverse()
        newPriceList.reverse()

        return changeList, timeList, newPriceList, startStream.IsUpOrDownPeak(), longTermTrend
