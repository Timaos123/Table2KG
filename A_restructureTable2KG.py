import pandas as pd
import numpy as np
import tqdm
import os

def main(params):
    
    srcFileName=params["fileName"]
    entityList=params["entityList"]
    attrList=params["attrList"]
    relationDict=params["relationDict"]
    genFilePath=params["genFilePath"]
    
    fileType=srcFileName.split(".")[-1]
    if fileType=="csv":
        preDf=pd.read_csv(srcFileName)
    
    entityAttDict=dict(list(zip(entityList,attrList)))

    #构建nodes文件
    print("生成nodes文件...")
    for entityItem in entityAttDict.keys():
        fileName=os.path.join(genFilePath,entityItem+"Nodes.csv")
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
        fileName=os.path.join(genFilePath,relationKeyItem+"_"+relationName+"Relationships.csv")
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
            relDf[":START_ID"] = relDf[":START_ID"].apply(
                lambda row: startId+"."+str(row))
            relDf[":END_ID"] = relDf[":END_ID"].apply(
                lambda row: endId+"."+str(row))
            relDf[":TYPE"]=relationName
            relDf.to_csv(fileName,index=None)

            print("生成文件:",fileName)
    


if __name__ == "__main__": 
    parameters={
        "fileName":"data/table2KG.csv",# 文件名尽量遵守windows命名规范，减少“.”的使用，最好csv
        "entityList":["student","mentor","highSchool","middleSchool"],# 实体名：一维-list,作为ID
        "attrList":[["学生姓名","语文","数学","英语","班级"],["班主任姓名","班主任年龄","班级"],["高中名","是否是示范性高中"],["初中名","是否是私立学校"]],# 属性名：二维-list
        "relationDict":{"student-highSchool":"__highSchool__","student-middleSchool":"__middleSchool__","mentor-student":"__monitor__"},#实体间的关系,格式：{"实体1-实体2":"关系名"}
        "sheet":"",#当文件时xls时使用
        "genFilePath":"data"
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
