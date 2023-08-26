import MarketStateManager as marketState

import zmq
import numpy as np
import sys
import os

from sklearn.neural_network import MLPClassifier
from sklearn import preprocessing
# Import necessary modules
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report,confusion_matrix
from sklearn.model_selection import GridSearchCV

import SuddenChangeTransactions
import TransactionBasics

transactionBinCountList = [6,8]
totalTimeCount = 6
isUsePeaks = False
totalUsedCurveCount = 4
isUseExtraData = False
acceptedProbibilty = 0.7
testRatio = 4
transParamList = [TransactionBasics.TransactionParam(10000, 3)]

mlpTransactionList = []
mlpTransactionScalerList = []

currentProbs = []
parameter_space = {
    'hidden_layer_sizes': [ (36,36,36),(36,36,36,36,36),(36,36,36,36),(48,48,48),(48,48,48,48) ],
    'solver': ['sgd', 'adam'],
    'alpha': [0.0001, 0.001, 0.01],
}

suddenChangeManager = SuddenChangeTransactions.SuddenChangeManager(transParamList)

def Predict( messageChangeTimeTransactionStrList, mlpTransactionScalerListIn, mlpTransactionListIn, isBuySell, isAvoidPeaks ):

    priceStrList = messageChangeTimeTransactionStrList[0].split(",")
    timeStrList = messageChangeTimeTransactionStrList[1].split(",")
    transactionStrList = messageChangeTimeTransactionStrList[2].split(",")
    extrasStrList = messageChangeTimeTransactionStrList[3].split(",")
    resultsChangeFloat = [float(messageStr) for messageStr in priceStrList]
    resultsTimeFloat = [float(timeStr) for timeStr in timeStrList]
    resultsExtraFloat = [float(extraStr) for extraStr in extrasStrList]

    resultsTransactionFloat = [float(transactionStr) for transactionStr in transactionStrList]

    extraMaxMinList = []
    resultStr = ""

    for transactionIndex in range(len(transParamList)):
        transParam = transParamList[transactionIndex]
        justTransactions = resultsTransactionFloat
        multipliedGramCount = TransactionBasics.GetTotalPatternCount(transParam.gramCount)
        basicList = TransactionBasics.CreateTransactionList(currentTransactionList)
        basicList = TransactionBasics.ReduceToNGrams(basicList, transParam.gramCount)
        currentTransactionList = TransactionBasics.GetListFromBasicTransData(basicList)
        # + marketStateList market state is cancelled for now
        #totalFeatures = currentTransactionList  + resultsChangeFloat[-TransactionBasics.PeakFeatureCount:] + resultsTimeFloat[-TransactionBasics.PeakFeatureCount:]
        if TransactionBasics.PeakFeatureCount == 0 or isBuySell :
            totalFeatures = currentTransactionList #+ resultsExtraFloat
        else:
            totalFeatures = currentTransactionList +\
                            resultsChangeFloat[-TransactionBasics.PeakFeatureCount:] + \
                            resultsTimeFloat[-TransactionBasics.PeakFeatureCount:] + extraMaxMinList


        totalFeaturesNumpy = np.array(totalFeatures).reshape(1, -1)
        totalFeaturesScaled = mlpTransactionScalerListIn[transactionIndex].transform(totalFeaturesNumpy)
        print("I will predict: ", totalFeatures, " scaled: ", totalFeaturesScaled)
        npTotalFeatures = np.array(totalFeaturesScaled)
        npTotalFeatures = npTotalFeatures.reshape(1, -1)
        predict_test = mlpTransactionListIn[transactionIndex].predict_proba(npTotalFeatures)
        curResultStr = str(predict_test) + ";"
        resultStr += curResultStr

    resultStr = resultStr[:-1]
    print("Results are: ", resultStr)
    return resultStr

def Learn():
    mlpTransaction = MLPClassifier(hidden_layer_sizes=(36, 36, 36), activation='relu',
                                   solver='sgd', learning_rate='adaptive', alpha=0.01, max_iter=500)
    global suddenChangeManager
    numpyArr = suddenChangeManager.toTransactionFeaturesNumpy(False)
    transactionScaler = preprocessing.StandardScaler().fit(numpyArr)
    X = transactionScaler.transform(numpyArr)
    y = suddenChangeManager.toTransactionResultsNumpy(False) #+ extraDataManager.getResult(transactionIndex)
    numpyArrTest = suddenChangeManager.toTransactionFeaturesNumpy(True)
    X_test = transactionScaler.transform(numpyArrTest)
    y_test = suddenChangeManager.toTransactionResultsNumpy(True)
    del suddenChangeManager

    mlpTransaction.fit(X, y)

    #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=40)



    predict_test = mlpTransaction.predict_proba(X_test)
    #print(predict_test)

    finalResult = predict_test[:, 1] >= 0.5
    returnResult = confusion_matrix(y_test, finalResult)
    print("50 ", returnResult)

    finalResult = predict_test[:, 1] >= 0.6
    returnResult = confusion_matrix(y_test, finalResult)
    print("60 ", returnResult)

    finalResult = predict_test[:, 1] >= 0.7
    returnResult = confusion_matrix(y_test, finalResult)
    print("70 ", returnResult)

    finalResult = predict_test[:, 1] >= 0.8
    returnResult = confusion_matrix(y_test, finalResult)
    print("80 ", returnResult)

    finalResult = predict_test[:, 1] >= 0.9
    returnResult = confusion_matrix(y_test, finalResult)
    print("90 ", returnResult)
    #print(predict_test)
    #predict_test = np.delete(finalResult, 0, 1)

    print(" Transactions learning done")

    sys.stdout.flush()
    return returnResult


Learn()

print("Memory cleaned")
sys.stdout.flush()
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("ipc:///tmp/peakLearner")
while True:
    #  Wait for next request from client
    message = socket.recv_string(0, encoding='ascii')
    print("Received request: %s" % message)
    messageChangeTimeTransactionStrList = message.split(";")
    command = messageChangeTimeTransactionStrList[0]

    if command == "Predict":
        messageChangeTimeTransactionStrList = messageChangeTimeTransactionStrList[1:]
        resultStr = Predict(messageChangeTimeTransactionStrList, mlpTransactionScalerList, mlpTransactionList, False, False)
        print("Results are: ", resultStr)
        #  Send reply back to client
        socket.send_string(resultStr, encoding='ascii')
        sys.stdout.flush()

