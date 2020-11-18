import pandas as pd
import numpy as np
from py2neo import Graph, Node, Relationship, Subgraph
import os
import tqdm


def main(
        dataDir="data",
        host="http://localhost:7474",
        username="neo4j",
        password="neo4j"):

    myGraph = Graph(host, username=username, password=password)

    triFileList = os.listdir(dataDir)

    nodeMatcher = NodeMatcher(myGraph)
    nodeDict = {}
    for triFileItem in tqdm.tqdm(triFileList):  # 获取Node
        if triFileItem.split(".")[0].endswith("Nodes"):
            nodeDf = pd.read_csv(os.path.join(dataDir, triFileItem))
            nodeDf.fillna("", inplace=True)
            attrList = nodeDf.columns
            for attrI in range(len(attrList)):
                if ":ID" in attrList[attrI]:
                    idIndex = attrI
                    break
            labelItem = list(set(nodeDf[":LABEL"].values.tolist()))[0]
            nodeAttrList = nodeDf.values.tolist()

            nodeDict[labelItem] = []
            for nodeIndex in range(len(nodeAttrList)):
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
                    ["_.`{}` = '{}'".format(row[0], row[1]) for row in tmpSearchList])
                tmpNode = nodeMatcher.match(
                    labelItem).where(tmpSearchStr).first()

                if tmpNode is not None:
                    nodeItem = tmpNode
                    print("{}已存在，取已有实体进行替换".format(
                        nodeItem[attrList[idIndex]]))
                nodeDict[labelItem].append(
                    (labelItem+"."+str(nodeAttrList[nodeIndex][idIndex]), nodeItem))
            nodeDict[labelItem] = dict(nodeDict[labelItem])

    for triFileItem in tqdm.tqdm(triFileList):  # 获取关系
        if triFileItem.split(".")[0].endswith("Relationships"):
            triDf = pd.read_csv(os.path.join(dataDir, triFileItem))
            triColumnList = list(triDf.columns)
            triColumnList.remove(":START_ID")
            triColumnList.remove(":TYPE")
            triColumnList.remove(":END_ID")
            triColumnList = [":START_ID", ":TYPE", ":END_ID"]+triColumnList
            triDf = triDf.loc[:, triColumnList]
            triList = triDf.values.tolist()

            triRelList = []
            for triRow in triList:
                headLabelItem = triRow[0].split(".")[0]
                headNode = nodeDict[headLabelItem][triRow[0]]
                tailLabelItem = triRow[2].split(".")[0]
                tailNode = nodeDict[tailLabelItem][triRow[2]]
                relName = triRow[1]

                rItemCypherData = pd.DataFrame(myGraph.run(
                    "match (p1:{p1label})-[r]->(p2:{p2label}) where type(r)='{rtype}' return p1,r,p2".format(p1label=headNode[":LABEL"], rtype=relName, p2label=tailNode[":LABEL"])).data())

                if rItemCypherData.shape[0] == 0:
                    relOldAttrKVDict = {}
                else:
                    rItem = rItemCypherData.values[:, 1].tolist()[0]
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
                for keyItem in keyList:
                    if keyItem in relOldAttrKVDict.keys() and keyItem not in relNewAttrKVDict.keys():
                        relAttrKVDict[keyItem] = relOldAttrKVDict[keyItem]
                    elif keyItem in relNewAttrKVDict.keys() and keyItem not in relOldAttrKVDict.keys():
                        relAttrKVDict[keyItem] = relNewAttrKVDict[keyItem]
                        print("新增关系属性：{}".format(keyItem))
                    elif keyItem in relNewAttrKVDict.keys() and keyItem in relOldAttrKVDict.keys():
                        print("更新关系属性：{}".format(keyItem))
                        relAttrKVDict[keyItem] = relOldAttrKVDict[keyItem] + \
                            ";" + relNewAttrKVDict[keyItem]
                    relAttrKVDict[keyItem] = ";".join(
                        list(set(relAttrKVDict[keyItem].split(";"))))
                triRelation = Relationship(
                    headNode, relName, tailNode, **relAttrKVDict)
                triRelList.append(triRelation)
            myGraph.create(Subgraph(relationships=triRelList))

    print("数据导入完成")


if __name__ == "__main__":
    main(dataDir="graphStructure_neo4j")
