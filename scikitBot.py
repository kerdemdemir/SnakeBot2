import MarketStateManager as marketState

import zmq
import numpy as np
import sys
import os
#import pydot

from sklearn.neural_network import MLPClassifier

from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV

from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.feature_selection import SelectFromModel
from sklearn.impute import SimpleImputer

from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.metrics import f1_score, make_scorer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier

import SuddenChangeTransactions
import TransactionBasics

import pydotplus

transactionBinCountList = [6,8]
totalTimeCount = 6
isUsePeaks = False
totalUsedCurveCount = 4
isUseExtraData = False
acceptedProbibilty = 0.7
testRatio = 4
transParamList = [TransactionBasics.TransactionParam(10000, 3)]
isUseTest = True
transactionScaler = None
inputTransform = None
mlpTransaction = None
IsPCA = False
IsKMeans = False
IsRandomForest = False
IsCorrelationMatrix = True
IsDecisionTree = True



currentProbs = []
if not IsDecisionTree:
    parameter_space = {
        'hidden_layer_sizes': [ (24,24, 24),(36,36,36) ]
    }
else:
    parameter_space = {
        'max_depth': [ 35],
        'min_samples_split': [100],
        'min_samples_leaf': [25]
    }
suddenChangeManager = SuddenChangeTransactions.SuddenChangeManager(transParamList)

parameterHeaders = ["TotalCount0", "TotalBuyPower0", "TotalSellPower0", "Price0",
                    "TotalCount1","TotalBuyPower1","TotalSellPower1","Price1",
                    "TotalCount2", "TotalBuyPower2", "TotalSellPower2", "Price2",
                    "MaxPowInDetail", "AverageVolume", "JumpCount10M", "JumpCount1H", "JumpCount12H",
                    "NetPrice1H", "NetPrice8H", "NetPrice24H", "NetPrice168H","PeakTime0","PeakTime1","PeakTime2",
                    "PeakLast1","PeakLast2","PeakLast3","PeakLast4", "DownPeakRatio0", "UpPeakRatio0", "DownPeakRatio1",
                    "DownPeak1", "UpPeak1", "UpPeakRatio1", "LongPeak1",
                    "LongPeak2", "LongPeak3", "LongPeak4", "LongPeak5",
                    "LongTime1", "LongTime2", "LongTime3", "LongDownPeakRatio0",
                    "LongUpPeakRatio0", "LongUpDownPeakRatio0", "SmallLongRatio0", "SmallLongRatio1", "SmallLongRatio2",
                    "BuyWall", "SellWall", "BuyLongWall", "SellLongWall",
                    "BuyVsSellWall", "LongBuyVsSellWall", "LongBuyVsBuyWall", "LongSellVsSellWall",
                    "AverageBuyWallVsWall", "AverageSellWallVsWall", "AverageLongBuyWallVsWall", "AverageLongSellWallVsWall",
                    "BuyWallVsTransaction", "SellWallVsTransaction", "LongBuyWallVsTransaction", "LongSellWallVsTransaction"
                    ]


extraHeaders =[]
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

    totalFeatures = parameterList #+ resultsChangeFloat[-5:]

    totalFeaturesNumpy = np.array(totalFeatures).reshape(1, -1)
    if inputTransform != None:
        totalFeaturesScaled = inputTransform.transform(totalFeaturesNumpy)
    else:
        totalFeaturesScaled = totalFeaturesNumpy
    print("I will predict: ", totalFeatures, " scaled: ", totalFeaturesScaled)
    npTotalFeatures = np.array(totalFeaturesScaled)
    npTotalFeatures = npTotalFeatures.reshape(1, -1)
    predict_test = mlpTransaction.predict_proba(npTotalFeatures)
    return str(predict_test[0][1])

def Learn():
    global suddenChangeManager
    global transactionScaler
    global mlpTransaction
    global inputTransform
    inputTransform = None

    if IsDecisionTree:
        mlpTransaction = RandomForestClassifier()
    else:
        mlpTransaction = MLPClassifier(hidden_layer_sizes=(24, 24, 24), activation='relu',
                                      solver='adam', learning_rate='adaptive', alpha=0.001, max_iter=500)
    #scoring = make_scorer(f1_score, pos_label=1)
    mlpTransaction = GridSearchCV(mlpTransaction, param_grid=parameter_space, cv=5, refit=True)

    #parameterHeaders = ["TotalCount0", "TotalBuyPower0", "TotalSellPower0", "Price0",
    #                    "TotalCount1", "TotalBuyPower1", "TotalSellPower1", "Price1",
    #                    "TotalCount2", "TotalBuyPower2", "TotalSellPower2", "Price2",
    #                    "MaxPowInDetail", "AverageVolume", "JumpCount10M", "JumpCount1H", "JumpCount12H",
    #                    "NetPrice1H", "NetPrice168H", "DownPeakRatio0", "PowerRatio0", "PowerRatio1"]
    # Get the selected feature indices and names
    #returnList.append(self.longPeaks[-5] / self.longPeaks[-3])
    #returnList.append(self.longPeaks[-4] / self.longPeaks[-2])
    #returnList.append(self.longPeaks[-1] / self.longPeaks[-2])
    #returnList.append(self.longPeaks[-0])
    #returnList.append(self.longPeaks[-1])
    #returnList.append(self.longPeaks[-2])
    #returnList.append(self.longPeaks[-3])

    feature_names = parameterHeaders+extraHeaders
    numpyArr = suddenChangeManager.toTransactionFeaturesNumpy(False)
    imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
    numpyArr = imputer.fit_transform(numpyArr)

    y = suddenChangeManager.toTransactionResultsNumpy(False)
    X = numpyArr
    if not IsDecisionTree:
        inputTransform = preprocessing.StandardScaler().fit(numpyArr)
        X = inputTransform.transform(X)


    if IsPCA:
        pca = PCA(n_components=15)
        X = pca.fit_transform(X)
        pca_components = pca.components_
        print("PCA components:")
        explained_variance_ratio = pca.explained_variance_ratio_
        inputTransform = pca
        for i, component in enumerate(pca_components):
            print(f"PC{i + 1}: {', '.join([f'{feature_names[j]}={component[j]:.4f}' for j in range(len(feature_names))])}")
    elif IsRandomForest:
        clf = RandomForestClassifier(max_depth=8, min_samples_split=50)
        clf.fit(X, y)
        #sfm = SelectFromModel(clf, max_features=20)
        sfm = SelectFromModel(clf, threshold='median', prefit=True, max_features=20, norm_order=1,
                                    importance_getter='auto')

        X = sfm.fit_transform(X, y)
        num_selected_features = X.shape[1]
        print(f"Number of selected features: {num_selected_features}")
        importances = clf.feature_importances_
        indices = np.argsort(importances)[::-1]

        # Write the feature importances to the standard output
        for i in indices:
            print(f"{feature_names[i]}: {importances[i]}")
        selected_names = [feature_names[i] for i in indices[:num_selected_features]]
        inputTransform = sfm
    elif IsKMeans:
        selector = SelectKBest(f_classif, k='all')
        X = selector.fit_transform(X, y)
        selected_indices = selector.get_support(indices=True)
        selected_names = [feature_names[i] for i in selected_indices]
        scores = selector.scores_

        #Sort the features by importance
        sorted_indices = np.argsort(scores)[::-1]
        sorted_names = [feature_names[i] for i in sorted_indices]

        corr_matrix = np.corrcoef(X, rowvar=False)

        #Print the sorted feature names and scores
        print("Sorted feature names and scores:")
        for name, score in zip(sorted_names, scores[sorted_indices]):
            print(f"{name}: {score:.4f}")

        avoid_indices = []
        for i in range(len(sorted_names)):
            if sorted_indices[i] not in avoid_indices:
                print(f"Taking feature {sorted_names[i]} will check correlated features")
                for j in range(i + 1, len(sorted_names)):
                    if corr_matrix[sorted_indices[i], sorted_indices[j]] > 0.7:
                        print(
                            f"Removing feature {sorted_names[j]} due to high correlation with feature {sorted_names[i]}")
                        avoid_indices.append(sorted_indices[j])

        removed_names = [selected_names[i] for i in avoid_indices]
        print(f"total removed names are:  {removed_names}")
        # Print the final selected feature names
        selected_names = [selected_names[i] for i in selected_indices]

        inputTransform = selector



    if isUseTest:
        numpyArrTest = suddenChangeManager.toTransactionFeaturesNumpy(True)
        #X_test = transactionScaler.transform(numpyArrTest)
        if inputTransform != None:
            X_test = inputTransform.transform(numpyArrTest)
        else:
            X_test = numpyArrTest
        y_test = suddenChangeManager.toTransactionResultsNumpy(True)
    del suddenChangeManager

    mlpTransaction.fit(X, y)

    print("Best parameters:", mlpTransaction.best_params_)

    #if IsDecisionTree:
    #   dot_data = export_graphviz(mlpTransaction.best_estimator_, out_file=None,
    #                              feature_names=feature_names,
    #                              class_names=["Fail","Good"],
    #                              filled=True, rounded=True,
    #                              special_characters=True)
#
    #   graph = pydotplus.graph_from_dot_data(dot_data)
    #   graph.write_png('decision_tree.png')
    #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=40)

    if isUseTest:

        thresholds = np.arange(0.1, 0.91, 0.03)

        predict_test = mlpTransaction.predict_proba(X_test)

        for threshold in thresholds:
            finalResult = predict_test[:, 1] >= threshold
            returnResult = confusion_matrix(y_test, finalResult)
            print(f"{threshold * 100:.0f} ", returnResult)

    for feature_name, importance in zip(feature_names, mlpTransaction.best_estimator_.feature_importances_):
        print(f"{feature_name}: {importance}")
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

