import json
import os
import matplotlib.pyplot as plt
import numpy as np


def convertByteToGB(byte: str):
    return int(byte[:-1]) / 1074000000


def printEingerückt(string: str, tabs: int):
    space = ""
    for i in range(0, tabs):
        space += "\t"
    print(space + string)

def CollectOPData(i, tabs):
    walltemp = i["blockedWall"]
    printEingerückt("Wall time: " + walltemp, tabs)
    spilltemp = convertByteToGB(i["spilledDataSize"])
    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


def JoinBuildTable():
    qspillSize = np.empty([3, 4], dtype=float)
    qexecTime = np.empty([3, 4], dtype=float)
    hspillSize = np.empty([3, 4], dtype=float)
    hexecTime = np.empty([3, 4], dtype=float)
    memcounter = 0
    tabs = 0
    for k in np.array([4, 5, 6]):
        threshcounter = 0
        tabs = 0
        printEingerückt(str(k) + ":", tabs)
        tabs += 1
        for i in np.array([0, 1150, 1350, 1550]):
            printEingerückt(str(i) + ":", tabs)
            tabs += 1
            name = "Join_" + str(k) + "_1_1_" + str(i) + ".txt"

            try:
                directory = "BuildingTable"
                f = open(os.path.join("Join", directory, name))
            except:
                break
            data = json.load(f)

            qTime = data["queryStats"]["executionTime"]
            printEingerückt("ExecutionTime: " + qTime, tabs)

            qData = convertByteToGB(data["queryStats"]["spilledDataSize"])
            printEingerückt("Spilled Data: " + str(qData) + " GB", tabs)
            qspillSize[memcounter][threshcounter] = qData
            qexecTime[memcounter][threshcounter] = float(qTime[:-1])
            for i in data["queryStats"]["operatorSummaries"]:
                if i["operatorType"] == "HashBuilderOperator":
                    printEingerückt("Hash:", tabs)
                    tabs += 1
                    walltemp = i["blockedWall"]
                    printEingerückt("Wall time: " + walltemp, tabs)
                    spilltemp = convertByteToGB(i["spilledDataSize"])
                    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
                    hspillSize[memcounter][threshcounter] = spilltemp
                    hexecTime[memcounter][threshcounter] = float(walltemp[:-1])
                    tabs -= 1
                if i["operatorType"] == "LookupJoinOperator":
                    printEingerückt("Join:", tabs)
                    tabs += 1
                    walltemp = i["blockedWall"]
                    printEingerückt("Wall time: " + walltemp, tabs)
                    spilltemp = convertByteToGB(i["spilledDataSize"])
                    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
                    tabs -= 1
            f.close()
            threshcounter += 1
            tabs -= 1
        memcounter += 1

    x = 0
    colors = np.array(["r", "g", "b"])
    labels = np.array(["2,8", "3,5", "4,2"])
    fig, axis = plt.subplots(1, 2)
    for i in hspillSize:
        axis[0].plot(i, hexecTime[x], color=colors[x], label=labels[x], marker="o")
        axis[1].plot(
            qspillSize[x], qexecTime[x], color=colors[x], label=labels[x], marker="o"
        )
        x += 1
    axis[0].set_xlabel("Spilled Datasize of the Query in GB")
    axis[0].set_ylabel("Query Execution Time")
    axis[0].set_title("Query execution time dependence of amount of spilled data")
    axis[0].grid(visible=True)
    axis[0].legend()
    axis[1].set_xlabel("Spilled Datasize of the Hash Operator in GB")
    axis[1].set_ylabel("Blocked Wall Time of the Hash Operator")
    axis[1].set_title(
        "Hash Operation execution time dependence of amount of spilled data"
    )
    axis[1].grid(visible=True)
    axis[1].legend()
    plt.show()


def JoinOtherTable():
    qspillSize = np.empty([2, 4], dtype=float)
    qexecTime = np.empty([2, 4], dtype=float)
    hspillSize = np.empty([2, 4], dtype=float)
    hexecTime = np.empty([2, 4], dtype=float)
    memcounter = 0
    tabs = 0
    for k in np.array([5, 6]):
        threshcounter = 0
        tabs = 0
        printEingerückt(str(k) + ":", tabs)
        tabs += 1
        for i in np.array([3000, 5000, 7000, 9000]):
            printEingerückt(str(i) + ":", tabs)
            tabs += 1
            name = "Join_" + str(k) + "_1_1_" + str(i) + ".txt"

            try:
                directory = "OtherTable"
                f = open(os.path.join("Join", directory, name))
            except:
                break
            data = json.load(f)

            qTime = data["queryStats"]["executionTime"]
            printEingerückt("ExecutionTime: " + qTime, tabs)

            qData = convertByteToGB(data["queryStats"]["spilledDataSize"])
            printEingerückt("Spilled Data: " + str(qData) + " GB", tabs)
            qspillSize[memcounter][threshcounter] = qData
            time = float(qTime[:-1]) if qTime[-1] == "m" else float(qTime[:-1]) * 60
            qexecTime[memcounter][threshcounter] = time
            for i in data["queryStats"]["operatorSummaries"]:
                if i["operatorType"] == "HashBuilderOperator":
                    printEingerückt("Hash:", tabs)
                    tabs += 1
                    walltemp = i["blockedWall"]
                    printEingerückt("Wall time: " + walltemp, tabs)
                    spilltemp = convertByteToGB(i["spilledDataSize"])
                    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
                    time = (
                        float(walltemp[:-1])
                        if walltemp[-1] == "m"
                        else float(walltemp[:-1]) * 60
                    )
                    hexecTime[memcounter][threshcounter] = time
                    tabs -= 1
                if i["operatorType"] == "LookupJoinOperator":
                    printEingerückt("Join:", tabs)
                    tabs += 1
                    walltemp = i["blockedWall"]
                    printEingerückt("Wall time: " + walltemp, tabs)
                    spilltemp = convertByteToGB(i["spilledDataSize"])
                    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
                    hspillSize[memcounter][threshcounter] = spilltemp
                    tabs -= 1
            f.close()
            threshcounter += 1
            tabs -= 1
        memcounter += 1

    x = 0
    colors = np.array(["r", "g", "b"])
    labels = np.array(["2,8", "3,5", "4,2"])
    fig, axis = plt.subplots(1, 2)
    for i in hspillSize:
        axis[0].plot(i, hexecTime[x], color=colors[x], label=labels[x], marker="o")
        axis[1].plot(
            qspillSize[x], qexecTime[x], color=colors[x], label=labels[x], marker="o"
        )
        x += 1
    axis[0].set_xlabel("Spilled Datasize of the Query in GB")
    axis[0].set_ylabel("Query Execution Time")
    axis[0].set_title("Query execution time dependence of amount of spilled data")
    axis[0].grid(visible=True)
    axis[0].legend()
    axis[1].set_xlabel("Spilled Datasize of the Hash Operator in GB")
    axis[1].set_ylabel("Blocked Wall Time of the Hash Operator")
    axis[1].set_title(
        "Hash Operation execution time dependence of amount of spilled data"
    )
    axis[1].grid(visible=True)
    axis[1].legend()
    plt.show()



def TPC():
    qspillSize = np.empty([3, 4], dtype=float)
    qexecTime = np.empty([3, 4], dtype=float)
    hspillSize = np.empty([3, 4], dtype=float)
    hexecTime = np.empty([3, 4], dtype=float)
    memcounter = 0
    tabs = 0
    for k in range(1, 23):
        threshcounter = 0
        tabs = 0
        printEingerückt(str(k) + ":", tabs)
        tabs += 1
        name = "tpc_" + str(k) + ".json"
        try:
            directory = "tpc"
            f = open(os.path.join(directory, name))
        except:
            print("File " + name + " not found.")
            continue
        data = json.load(f)

        qTime = data["queryStats"]["executionTime"]
        printEingerückt("ExecutionTime: " + qTime, tabs)

        qData = convertByteToGB(data["queryStats"]["spilledDataSize"])
        printEingerückt("Spilled Data: " + str(qData) + " GB", tabs)
        #qspillSize[memcounter][threshcounter] = qData
        #qexecTime[memcounter][threshcounter] = float(qTime[:-1])
        for i in data["queryStats"]["operatorSummaries"]:
            if i["operatorType"] == "HashBuilderOperator":
                printEingerückt("Hash:", tabs)
                CollectOPData(i, tabs + 1)
                #if walltemp[-1] == "d":
               #     time =  float(walltemp[:-1]) * 24 * 60
              #  elif walltemp[-1] == "h":
             #       time =  float(walltemp[:-2]) * 60
            #    elif walltemp[-1] == "m":
                #    time = float(walltemp[:-1])
               # elif walltemp[-2] == "ms":
              #      time =  float(walltemp[:-2]) / 3600
             #   elif walltemp[-2] == "ns":
            #        time =  float(walltemp[:-2]) / (60 * 60 * 60)
           #     elif walltemp[-1] == "s":
          #          time =  float(walltemp[:-1]) / 60
                #hexecTime[memcounter][threshcounter] = time
            elif i["operatorType"] == "LookupJoinOperator":
                printEingerückt("Join:", tabs)
                CollectOPData(i, tabs + 1)
            elif i["operatorType"] == "HashAggregationOperator":
                printEingerückt("HashAgg:", tabs)
                CollectOPData(i, tabs + 1)
            elif i["operatorType"] == "AggregationOperator":
                printEingerückt("Agg:", tabs)
                CollectOPData(i, tabs + 1) 
        f.close()
        threshcounter += 1
        tabs -= 1
    memcounter += 1

    x = 0
    colors = np.array(["r", "g", "b"])
    labels = np.array(["2,8", "3,5", "4,2"])
    fig, axis = plt.subplots(1, 2)
    for i in hspillSize:
        axis[0].plot(i, hexecTime[x], color=colors[x], label=labels[x], marker="o")
        axis[1].plot(
            qspillSize[x], qexecTime[x], color=colors[x], label=labels[x], marker="o"
        )
        x += 1
    axis[0].set_xlabel("Spilled Datasize of the Query in GB")
    axis[0].set_ylabel("Query Execution Time")
    axis[0].set_title("Query execution time dependence of amount of spilled data")
    axis[0].grid(visible=True)
    axis[0].legend()
    axis[1].set_xlabel("Spilled Datasize of the Hash Operator in GB")
    axis[1].set_ylabel("Blocked Wall Time of the Hash Operator")
    axis[1].set_title(
        "Hash Operation execution time dependence of amount of spilled data"
    )
    axis[1].grid(visible=True)
    axis[1].legend()
    plt.show()

TPC()
