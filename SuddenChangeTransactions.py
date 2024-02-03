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
import TransactionFileHandler as FH

IsOneFileOnly = False

class SuddenChangeMerger:
    def __init__(self, transactionParam, marketState):
        self.mustBuyList = []

        if FH.IsMultiThreaded :
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
            handler = FH.FileHandler(jsonIn, self.transactionParam, self.marketState, fileName, isTest)
            return [handler]
        else:
            for index in range(len(jsonIn)):
                if not jsonIn[index]:
                    continue

                jsonPeakTrans = jsonIn[index]

                handler = FH.FileHandler(jsonPeakTrans,self.transactionParam,self.marketState,fileName, isTest)
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
                curRules = FH.rules.GetRulesWithIndex(i)
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
                curRules = FH.rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.RelaxTheRules(buyList[:, i])
            AP.IsTrained = True
            AP.TotalStrikeCount = 0


        if FH.rules.isTuned :
            for i in range(len(self.patternList[0])):
                curRules = FH.rules.GetRulesWithIndex(i)
                for rule in curRules:
                    rule.SetEliminationCounts(buyList[:, i], badList[:, i], 0.05)
                    rule.Print()

            isBadCountBigger = FH.rules.SelectBestQuantile()
            if not isBadCountBigger:
                sys.exit()

        #if AP.IsTeaching:
        FH.rules.Write(len(self.patternList), len(self.badPatternList))


        FH.rules.ResetRules()
        FH.rules.isTuned = True
        #plt.close()


    def MergeInTransactions(self, handler):
        for pattern in handler.patternList:
            if pattern is not None:
                features = pattern.GetFeatures(FH.rules)
                if features is not None:
                    if FH.IsMultiThreaded:
                        with FH.lock:
                            self.patternList.append(features)
                    else:
                        self.patternList.append(features)

        for pattern in handler.badPatternList:
            if pattern is not None:
                features = pattern.GetFeatures(FH.rules)
                if features is not None:
                    if FH.IsMultiThreaded:
                        with FH.lock:
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
                for _ in range(len(FH.eliminatedList2.eliminatedList)):
                    FH.eliminatedList2.eliminatedList.pop()
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
        print("Reading file: ", fileName)
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
        #allJumpFiles = self.GetAllFiles("/Data/TestData/", True)
        #allExtraFiles = []

        allFiles = allJumpFiles
        if not self.isTest:
            allFiles = allJumpFiles + allExtraFiles
        if FH.IsMultiThreaded:
            lock = multiprocessing.Lock()
            pool_obj = multiprocessing.Pool(initializer=FH.init_pool_processes, initargs=(lock,), processes=30,maxtasksperchild=500)
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