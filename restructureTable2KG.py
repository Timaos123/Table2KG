import pandas as pd
import numpy as np
import tqdm

def main(params):
    
    fileName=params["fileName"]
    entityList=params["entityList"]
    attrList=params["attrList"]
    relationDict=params["relationDict"]

    fileType=fileName.split(".")[-1]
    if fileType=="csv":
        preDf=pd.read_csv(fileName)
    
    entityAttDict=dict(list(zip(entityList,attrList)))

    #构建nodes文件
    print("生成nodes文件...")
    for entityItem in entityAttDict.keys():
        fileName=entityItem+"Nodes.csv"
        try:
            columnList=[entityItem]+entityAttDict[entityItem]
            preItemDf=preDf.loc[:,columnList]
            preItemDf.drop_duplicates(subset=entityItem,inplace=True)

            preItemDf.rename({entityItem:entityItem+":ID"},axis=1,inplace=True)
            preItemDf[":LABEL"]=entityItem

            preItemDf.to_csv(fileName,index=None)
        except KeyError as ex:
            print(fileName,"生成失败，可能原因：列名设置错误")

        print("生成文件:",fileName)
    
    print("生成relationships文件...")
    for relationKeyItem in relationDict.keys():
        # 提取实体与关系
        startId=relationKeyItem.split("-")[0]
        endId=relationKeyItem.split("-")[1]
        relationName=relationDict[relationKeyItem]
        fileName=relationKeyItem+"_"+relationName+"Relationships.csv"
        if "-" not in relationKeyItem:
            print(fileName,"生成失败，可能原因：关系符号'-'设置错误")
        else:
            if startId not in entityList or endId not in entityList:
                print(fileName,"生成失败，可能原因：不存在输入的节点")
            
            #构建关系结构
            relDf=preDf.loc[:,[startId,endId]]
            relDf.dropna(inplace=True,how="any")

            #重命名
            relDf.rename({startId:":START_ID",endId:":END_ID"},axis=1,inplace=True)
            relDf[":TYPE"]=relationName
            relDf.to_csv(fileName,index=None)

            print("生成文件:",fileName)
    


if __name__ == "__main__":
    parameters={
        "fileName":"preRestructure.csv",# 文件名尽量遵守windows命名规范，减少“.”的使用，最好csv
        "entityList":["实体_a","实体_b","实体_c"],# 实体名：一维-list,作为ID
        "attrList":[["a属性_a1","a属性_a2","a属性_a3","a和b属性_ab"],["b属性_b1","b属性_b2","a和b属性_ab"],["c属性_c1"]],# 属性名：二维-list
        "relationDict":{"实体_a-实体_b":"关系1","实体_b-实体_c":"关系2","实体_b-实体_a":"关系4"},#实体间的关系,格式：{"实体1-实体2":"关系名"}
        "sheet":"",#当文件时xls时使用
    }
    main(parameters)

    # neo4j-admin import --database try1 
    #                     --nodes e_aNodes.csv 
    #                     --nodes e_bNodes.csv 
    #                     --nodes e_cNodes.csv 
    #                     --relationships r1Relationships.csv 
    #                     --relationships r4Relationships.csv 
    #                     --relationships r2Relationships.csv

    # neo4j-admin import --database try1  --nodes e_aNodes.csv --nodes e_bNodes.csv --nodes e_cNodes.csv --relationships r1Relationships.csv --relationships r4Relationships.csv --relationships r2Relationships.csv