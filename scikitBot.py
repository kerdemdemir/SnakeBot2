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
isUseTest = False

transactionScaler = None
mlpTransaction = None

currentProbs = []
parameter_space = {
    'hidden_layer_sizes': [ (36,36,36),(36,36,36,36,36),(36,36,36,36),(48,48,48),(48,48,48,48) ],
    'solver': ['sgd', 'adam'],
    'alpha': [0.0001, 0.001, 0.01],
}

suddenChangeManager = SuddenChangeTransactions.SuddenChangeManager(transParamList)

parameterHeaders = ["TotalCount0", "TotalPower0", "BuySellRatio0", "Price0",
                    "TotalCount1","TotalPower1","BuySellRatio1","Price1",
                    "TotalCount2", "TotalPower2", "BuySellRatio2", "Price2",
                    "MaxPowInDetail", "AverageVolume", "JumpCount8H", "NetPrice1H", "NetPrice8H", "NetPrice24H",
                    "NetPrice72H", "NetPrice168H","PeakTime0", "PeakTime1", "PeakTime2", "PeakTime3"]
def Predict( messageChangeTimeTransactionStrList):

    priceStrList = messageChangeTimeTransactionStrList[1].split(",")
    parameterKeyValues = messageChangeTimeTransactionStrList[2].split(":")[1].split("|")

    resultsChangeFloat = [float(messageStr) for messageStr in priceStrList]
    dictionaryParams = {}

    for parameterKeyVal in parameterKeyValues:
        keyValList = parameterKeyVal.split(",")
        dictionaryParams[keyValList[0]] = float(keyValList[1])

    parameterList = []
    for key in parameterHeaders:
        parameterList.append(dictionaryParams[key])

    totalFeatures = parameterList + resultsChangeFloat[-5:]

    totalFeaturesNumpy = np.array(totalFeatures).reshape(1, -1)
    totalFeaturesScaled = transactionScaler.transform(totalFeaturesNumpy)
    print("I will predict: ", totalFeatures, " scaled: ", totalFeaturesScaled)
    npTotalFeatures = np.array(totalFeaturesScaled)
    npTotalFeatures = npTotalFeatures.reshape(1, -1)
    predict_test = mlpTransaction.predict_proba(npTotalFeatures)
    return str(predict_test[0][1])

def Learn():
    global suddenChangeManager
    global transactionScaler
    global mlpTransaction

    mlpTransaction = MLPClassifier(hidden_layer_sizes=(24, 24, 24), activation='relu',
                                   solver='sgd', learning_rate='adaptive', alpha=0.01, max_iter=750)
    numpyArr = suddenChangeManager.toTransactionFeaturesNumpy(False)
    transactionScaler = preprocessing.StandardScaler().fit(numpyArr)
    X = transactionScaler.transform(numpyArr)
    y = suddenChangeManager.toTransactionResultsNumpy(False) #+ extraDataManager.getResult(transactionIndex)
    if isUseTest:
        numpyArrTest = suddenChangeManager.toTransactionFeaturesNumpy(True)
        X_test = transactionScaler.transform(numpyArrTest)
        y_test = suddenChangeManager.toTransactionResultsNumpy(True)
    del suddenChangeManager

    mlpTransaction.fit(X, y)

    #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=40)

    if isUseTest:
        predict_test = mlpTransaction.predict_proba(X_test)
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
    return


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
        resultStr = Predict(messageChangeTimeTransactionStrList)
        print("Results are: ", resultStr)
        #  Send reply back to client
        socket.send_string(resultStr, encoding='ascii')
        sys.stdout.flush()

