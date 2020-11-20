import pandas as pd
import numpy as np
from py2neo import Graph, Node, Relationship, Subgraph, NodeMatcher
import os
import tqdm


def main(
        dataDir="data",
        host="http://localhost:7474",
        username="neo4j",
        password="neo4j",
        checkExist=False):

    myGraph = Graph(host, username=username, password=password)

    triFileList = os.listdir(dataDir)

    nodeMatcher = NodeMatcher(myGraph)
    nodeDict = {}
    attrIdList = []
    for triFileItem in tqdm.tqdm(triFileList):  # 获取Node
        if triFileItem.split(".")[0].endswith("Nodes"):
            nodeDf = pd.read_csv(os.path.join(dataDir, triFileItem), dtype=str)
            nodeDf.fillna("", inplace=True)
            labelItem = list(set(nodeDf[":LABEL"].values.tolist()))[0]
            attrList = nodeDf.columns
            for attrI in range(len(attrList)):
                if ":ID" in attrList[attrI]:
                    if not myGraph.schema.get_indexes(labelItem):
                        myGraph.schema.create_index(labelItem, attrList[attrI])
                    idIndex = attrI
                    break
            nodeAttrList = nodeDf.values.tolist()

            nodeDict[labelItem] = []
            for nodeIndex in tqdm.tqdm(range(len(nodeAttrList))):
                nodeItem = Node(labelItem)
                for attrIndex in range(len(attrList)):
                    if attrIndex == idIndex:
                        nodeItem[attrList[attrIndex]] = labelItem + \
                            "."+str(nodeAttrList[nodeIndex][attrIndex])
                    else:
                        nodeItem[attrList[attrIndex]
                                 ] = nodeAttrList[nodeIndex][attrIndex]

                tmpSearchList = list(
                    zip(list(nodeItem.keys()), list(nodeItem.values())))
                tmpSearchStr = " and ".join(
                    ["_.`{}` = '{}'".format(row[0].replace("\\", "/"), row[1].replace("\\", " /").replace("'", ""))
                        if type(row[1]) == str else "_.`{}` = '{}'".format(row[0].replace("\\", "/"), row[1])
                     for row in tmpSearchList])
                tmpNode = nodeMatcher.match(
                    labelItem).where(tmpSearchStr).first()

                if tmpNode is not None:
                    nodeItem = tmpNode
                    with open("log.txt", "a+", encoding="utf8") as logFile:
                        logFile.write("{}已存在，取已有实体进行替换\n".format(
                            nodeItem[attrList[idIndex]]))
                nodeDict[labelItem].append(
                    (labelItem+"."+str(nodeAttrList[nodeIndex][idIndex]), nodeItem))
            nodeDict[labelItem] = dict(nodeDict[labelItem])

    for triFileItem in tqdm.tqdm(triFileList):  # 获取关系
        if triFileItem.split(".")[0].endswith("Relationships"):
            triDf = pd.read_csv(os.path.join(
                dataDir, triFileItem), dtype=str,nrows=50)
            triColumnList = list(triDf.columns)
            triColumnList.remove(":START_ID")
            triColumnList.remove(":TYPE")
            triColumnList.remove(":END_ID")
            triColumnList = [":START_ID", ":TYPE", ":END_ID"]+triColumnList
            triDf = triDf.loc[:, triColumnList]
            triList = triDf.values.tolist()

            batchSize = 100000
            batchI = 0
            while batchI*batchSize < len(triList):
                triRelList = []
                for triRow in tqdm.tqdm(triList[batchI*batchSize:min((batchI+1)*batchSize, len(triList))]):
                    headLabelItem = triRow[0].split(".")[0]
                    headNode = nodeDict[headLabelItem][triRow[0]]
                    tailLabelItem = triRow[2].split(".")[0]
                    tailNode = nodeDict[tailLabelItem][triRow[2]]
                    relName = triRow[1]

                    if checkExist == True:  # 具有重复值
                        rItemCypherData = pd.DataFrame(myGraph.run(
                            "match (p1:{p1label})-[r]->(p2:{p2label}) where type(r)='{rtype}' return distinct r limit 1".format(p1label=headNode[":LABEL"], rtype=relName, p2label=tailNode[":LABEL"])).data())

                        if rItemCypherData.shape[0] == 0:
                            relOldAttrKVDict = {}
                        else:
                            rItem = rItemCypherData.values[:, 0].tolist()[0]
                            relOldAttrKey = list(rItem.keys())
                            relOldAttrVal = list(rItem.values())
                            relOldAttrKVDict = dict(
                                list(zip(relOldAttrKey, relOldAttrVal)))

                        relNewAttrKey = triColumnList[3:]
                        relNewAttrVal = triRow[3:]
                        relNewAttrKVDict = dict(
                            list(zip(relNewAttrKey, relNewAttrVal)))

                        relAttrKVDict = {}
                        keyList = list(relOldAttrKVDict.keys()) + \
                            list(relNewAttrKVDict.keys())
                    else:  # 不具有重复值
                        relOldAttrKVDict={}
                        relNewAttrKey = triColumnList[3:]
                        relNewAttrVal = triRow[3:]
                        relNewAttrKVDict = dict(
                            list(zip(relNewAttrKey, relNewAttrVal)))

                        relAttrKVDict = {}
                        keyList = list(relNewAttrKVDict.keys())

                    for keyItem in keyList:
                        if keyItem in relOldAttrKVDict.keys() and keyItem not in relNewAttrKVDict.keys():
                            relAttrKVDict[keyItem] = relOldAttrKVDict[keyItem]
                        elif keyItem in relNewAttrKVDict.keys() and keyItem not in relOldAttrKVDict.keys():
                            relAttrKVDict[keyItem] = relNewAttrKVDict[keyItem]
                            with open("log.txt", "a+", encoding="utf8") as logFile:
                                logFile.write("新增关系属性：{}\n".format(keyItem))
                        elif keyItem in relNewAttrKVDict.keys() and keyItem in relOldAttrKVDict.keys():
                            with open("log.txt", "a+", encoding="utf8") as logFile:
                                logFile.write("更新关系属性：{}\n".format(keyItem))
                            relAttrKVDict[keyItem] = relOldAttrKVDict[keyItem] + \
                                ";" + relNewAttrKVDict[keyItem]
                        relAttrKVDict[keyItem] = ";".join(
                            list(set(relAttrKVDict[keyItem].split(";"))))
                    triRelation = Relationship(
                        headNode, relName, tailNode, **relAttrKVDict)

                    triRelList.append(triRelation)
                myGraph.create(Subgraph(relationships=triRelList))
                batchI += 1

    print("数据导入完成")


if __name__ == "__main__":
    main(dataDir="graphStructure_neo4j")
