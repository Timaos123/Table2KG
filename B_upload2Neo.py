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

                relAttrKey = triColumnList[3:]
                relAttrVal = triRow[3:]
                relAttrKVDict = dict(list(zip(relAttrKey, relAttrVal)))

                triRelation = Relationship(
                    headNode, relName, tailNode, **relAttrKVDict)
                triRelList.append(triRelation)
            myGraph.create(Subgraph(relationships=triRelList))

    print("数据导入完成")


if __name__ == "__main__":
    main(dataDir="graphStructure_neo4j")
