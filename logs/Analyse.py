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


def transTimes(time):
    res = 0
    if time[-1] == "d":
        res = float(time[:-1]) * 24 * 60
    elif time[-1] == "h":
        res = float(time[:-1]) * 60
    elif time[-1] == "m":
        res = float(time[:-1])
    elif time[-2:] == "ms":
        res = float(time[:-2]) / (60 * 1000)
    elif time[-2:] == "ns":
        res = float(time[:-2]) / (60 * 1000000)
    elif time[-2:] == "us":
        res = float(time[:-2]) / (60 * 1000000000)
    elif time[-1] == "s":
        res = float(time[:-1]) / 60
    return res


def CollectOPData(i, tabs):
    blockedwall = i["blockedWall"]

    spilltemp = convertByteToGB(i["spilledDataSize"])
    printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
    cpu_time = (
        transTimes(i["addInputCpu"])
        + transTimes(i["getOutputCpu"])
        + transTimes(i["finishCpu"])
    )
    wall_time = (
        transTimes(i["addInputWall"])
        + transTimes(i["getOutputWall"])
        + transTimes(i["finishWall"])
    )
    printEingerückt("Wall time: " + str(wall_time), tabs)
    return wall_time, cpu_time, spilltemp


def makeBarFig(data, xlabels, ylabel, show_bar_label: bool = True):
    fig, ax = plt.subplots()
    x = np.arange(len(xlabels))
    width = 0.4

    bottom = np.zeros(len(xlabels))
    for label, data in data.items():
        rects = ax.bar(x, data, width, label=label, bottom=bottom)
        bottom += data
        if show_bar_label:
            ax.bar_label(rects, padding=2, fontsize=20)

    ax.set_xticks(x)
    ax.set_xticklabels(xlabels, fontsize=20)
    ax.legend(loc="upper right", fontsize=20)
    ax.set_ylabel(ylabel, fontsize=20)
    ax.grid(visible=True, linestyle="dashed")
    ax.set_axisbelow(True)
    plt.show()


def makeScatterFig(xdata, ydata, labels, xlabel, ylabel):
    fig, ax = plt.subplots()

    ax.scatter(xdata, ydata, marker="o", s=70)
    for i, txt in enumerate(labels):
        ax.annotate(txt, (xdata[i], ydata[i]))
    ax.set_xlabel(xlabel, fontsize=20)
    ax.set_ylabel(ylabel, fontsize=20)
    ax.grid(visible=True, linestyle="dashed")
    ax.set_axisbelow(True)
    plt.show()


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
    spilledData = []
    query_times = []
    query_names = []
    query_p_user_mem = []
    query_joins = []
    query_aggs = []
    query_join_spills = []
    query_agg_spills = []
    query_hash_spills = []
    query_times_join = []
    query_times_agg = []
    query_times_ex = []
    query_times_hash = []
    query_times_cpu_join = []
    query_times_cpu_agg = []
    query_times_cpu_ex = []
    query_times_cpu_hash = []
    query_all_time = []
    query_all_time_cpu = []
    all_join_spills = []
    all_join_times = []
    all_join_times_cpu = []
    all_hash_spills = []
    all_hash_times = []
    all_hash_times_cpu = []
    all_agg_spills = []
    all_agg_times = []
    all_agg_times_cpu = []
    memcounter = 0
    tabs = 0
    for k in range(1, 23):
        join_spills = 0
        agg_spills = 0
        hash_spills = 0
        join_counter = 0
        agg_counter = 0
        join_time = 0
        agg_time = 0
        ex_time = 0
        hash_time = 0
        all_time = 0
        join_time_cpu = 0
        agg_time_cpu = 0
        ex_time_cpu = 0
        hash_time_cpu = 0
        all_time_cpu = 0
        tabs = 0
        printEingerückt(str(k) + ":", tabs)
        tabs += 1
        name = "tpc_" + str(k) + ".json"
        try:
            directory = "tpc"
            f = open(os.path.join(directory, name))
        except:
            print("File " + name + " not found.")
            try:
                name = "tpc_" + str(k) + "_6_1000.json"
                f = open(os.path.join(directory, name))
            except:
                # continue
                try:
                    name = "tpc_" + str(k) + "(failed).json"
                    f = open(os.path.join(directory, name))
                except:
                    print("failed to open failed!")
                    continue

        query_names.append("Query " + str(k))
        data = json.load(f)

        qTime = transTimes(data["queryStats"]["executionTime"])
        printEingerückt("ExecutionTime: " + str(qTime), tabs)
        query_times.append(qTime)

        qData = convertByteToGB(data["queryStats"]["spilledDataSize"])
        printEingerückt("Spilled Data: " + str(qData) + " GB", tabs)
        spilledData.append(qData)

        p_user_mem = convertByteToGB(data["queryStats"]["peakUserMemoryReservation"])
        printEingerückt("Peak User Memory: " + str(p_user_mem) + " GB", tabs)
        query_p_user_mem.append(p_user_mem)
        # qspillSize[memcounter][threshcounter] = qData
        # qexecTime[memcounter][threshcounter] = float(qTime[:-1])
        for i in data["queryStats"]["operatorSummaries"]:
            printEingerückt(i["operatorType"], tabs)
            if i["operatorType"] == "HashBuilderOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                hash_spills += spill
                hash_time += wtime
                hash_time_cpu += cputime
                all_hash_spills.append(spill)
                all_hash_times.append(wtime)
                all_hash_times_cpu.append(cputime)
                join_counter += 1
            elif i["operatorType"] == "LookupJoinOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                join_spills += spill
                join_time += wtime
                join_time_cpu += cputime
                all_join_spills.append(spill)
                all_join_times.append(wtime)
                all_join_times_cpu.append(cputime)
                join_counter += 1
            elif i["operatorType"] == "HashAggregationOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                agg_spills += spill
                agg_time += wtime
                agg_time_cpu += cputime
                all_agg_spills.append(spill)
                all_agg_times.append(wtime)
                all_agg_times_cpu.append(cputime)
                agg_counter += 1
            elif i["operatorType"] == "AggregationOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                agg_spills += spill
                agg_time += wtime
                agg_time_cpu += cputime
                all_agg_spills.append(spill)
                all_agg_times.append(wtime)
                all_agg_times_cpu.append(cputime)
                agg_counter += 1
            elif "Exchange" in i["operatorType"]:
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                ex_time += wtime
                ex_time_cpu += cputime
            else:
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                all_time += wtime
                all_time_cpu += cputime
        f.close()
        tabs -= 1
        query_aggs.append(agg_counter)
        query_joins.append(join_counter)
        query_agg_spills.append(agg_spills)
        query_join_spills.append(join_spills)
        query_hash_spills.append(hash_spills)
        query_times_join.append(join_time)
        query_times_agg.append(agg_time)
        query_times_ex.append(ex_time)
        query_times_hash.append(hash_time)
        query_all_time.append(all_time)
        query_times_cpu_join.append(join_time_cpu)
        query_times_cpu_agg.append(agg_time_cpu)
        query_times_cpu_ex.append(ex_time_cpu)
        query_times_cpu_hash.append(hash_time_cpu)
        query_all_time_cpu.append(all_time_cpu)

    memcounter += 1

    filter = np.array(spilledData) != 0

    other = np.subtract(
        spilledData,
        np.add(np.add(query_agg_spills, query_join_spills), query_hash_spills),
    )
    spills = {
        "Join": np.array(query_join_spills)[filter],
        "Aggregation": np.array(query_agg_spills)[filter],
        "Hash": np.array(query_hash_spills)[filter],
        "Other": np.array(other)[filter],
    }
    makeBarFig(spills, np.array(query_names)[filter], "Spilled Data in GB")
    makeScatterFig(
        query_times,
        spilledData,
        query_names,
        "Execution time in min",
        "Data spilled in GB",
    )

    # fig, ax = plt.subplots()
    # op_names = ["Joins", "Aggregations"]
    # rects = ax.bar(x, query_joins, width, label="Joins")
    # ax.bar_label(rects, padding=2)
    # rects = ax.bar(x + width, query_aggs, width, label="Aggregations")
    # ax.bar_label(rects, padding=2)
    # ax.set_xticks(x + width / 2)
    # ax.set_xticklabels(query_names, fontsize=15)
    # ax.legend(loc="upper right")
    # ax.set_ylabel("Number of Operations in Query")
    # plt.show()

    other_times = np.subtract(
        query_times,
        np.add(
            np.add(np.add(query_times_join, query_times_agg), query_times_ex),
            query_times_hash,
        ),
    )
    times = {
        "Join": query_times_cpu_join,
        "Aggregation": query_times_cpu_agg,
        # "Exchange": query_times_ex,
        "Hash": query_times_cpu_hash,
        # "Other": all_time,
    }
    makeBarFig(times, query_names, "Wall time in min", show_bar_label=False)

    fig, ax = plt.subplots(1, 2)
    ax31 = ax[0]
    ax31.scatter(all_hash_times, all_hash_spills, marker="o", label="Hash", s=80)
    ax31.scatter(all_join_times, all_join_spills, marker="s", label="Join", s=80)
    ax31.scatter(all_agg_times, all_agg_spills, marker="^", label="Aggregation", s=80)
    ax31.legend(loc="upper right")
    ax31.set_xlabel("Wall time of Operation in min")
    ax31.set_ylabel("Data spilled by Operation in GB")

    ax32 = ax[1]
    ax32.scatter(all_hash_times_cpu, all_hash_spills, marker="o", label="Hash", s=80)
    ax32.scatter(all_join_times_cpu, all_join_spills, marker="s", label="Join", s=80)
    ax32.scatter(
        all_agg_times_cpu, all_agg_spills, marker="^", label="Aggregation", s=80
    )
    ax32.legend(loc="upper right")
    ax32.set_xlabel("CPU time of Operation in min")
    ax32.set_ylabel("Data spilled by Operation in GB")

    plt.show()


def analyse_Query(number):
    all_hash_times = []
    all_hash_spills = []
    all_hash_times_cpu = []
    hash_names = []
    all_wtimes = []
    all_cputimes = []
    all_names = []
    all_wtimes_operators = {}
    all_cputimes_operators = {}

    name = "tpc_" + number + ".json"
    try:
        directory = "tpc"
        f = open(os.path.join(directory, name))
    except:
        print("File " + name + " not found.")
    data = json.load(f)
    for i in data["queryStats"]["operatorSummaries"]:
        wtime, cputime, spill = CollectOPData(i, 0)
        # if wtime > 0:
        if i["operatorType"][:-8] in all_wtimes_operators:
            all_wtimes_operators[i["operatorType"][:-8]] += wtime
            all_cputimes_operators[i["operatorType"][:-8]] += cputime
        else:
            all_wtimes_operators[i["operatorType"][:-8]] = wtime
            all_cputimes_operators[i["operatorType"][:-8]] = cputime
        all_names.append(i["operatorType"][:-8] + " " + str(i["stageId"]))
        all_wtimes.append(wtime)
        all_cputimes.append(cputime)
        if i["operatorType"] == "HashBuilderOperator":
            all_hash_spills.append(spill)
            all_hash_times.append(wtime)
            all_hash_times_cpu.append(cputime)
            hash_names.append(i["stageId"])

    fig, ax = plt.subplots()
    width = 0.25
    x = np.arange(len(hash_names))
    rects = ax.bar(hash_names, all_hash_spills, width)
    ax.legend(loc="upper right")
    ax.set_xlabel("CPU time of Operation in min")
    ax.set_ylabel("Data spilled by Operation in GB")
    ax.bar_label(rects)
    plt.show()

    # fig, ax = plt.subplots(2, 1)
    # width = 0.25
    # ax1 = ax[0]
    # data = list(zip(all_names, all_wtimes))
    # sorted_data = sorted(data, key=lambda item: item[1], reverse=True)
    # x_sorted, y_sorted = zip(*sorted_data)
    # rects = ax1.bar(x_sorted, y_sorted, width)
    # ax1.legend(loc="upper right")
    # ax1.set_ylabel("Wall time of Operation in min")
    # ax1.bar_label(rects)
    # width = 0.25
    # ax2 = ax[1]
    # data = list(zip(all_names, all_cputimes))
    # sorted_data = sorted(data, key=lambda item: item[1], reverse=True)
    # x_sorted, y_sorted = zip(*sorted_data)
    # rects = ax2.bar(x_sorted, y_sorted, width)
    # ax2.legend(loc="upper right")
    # ax2.set_ylabel("CPU time of Operation in min")
    # ax2.bar_label(rects)

    fig, ax = plt.subplots(2, 1)
    width = 0.25
    ax1 = ax[0]
    wtimes_temp = list(all_wtimes_operators.values())
    names_temp = list(all_wtimes_operators.keys())
    data = list(zip(names_temp, wtimes_temp))
    sorted_data = sorted(data, key=lambda item: item[0], reverse=True)
    x_sorted, y_sorted = zip(*sorted_data)
    rects = ax1.bar(x_sorted, y_sorted, width)
    ax1.set_ylabel("Wall time of Operation in min")
    ax1.bar_label(rects)

    cputimes_temp = list(all_cputimes_operators.values())
    names_temp = list(all_cputimes_operators.keys())
    ax2 = ax[1]
    data = list(zip(names_temp, cputimes_temp))
    sorted_data = sorted(data, key=lambda item: item[0], reverse=True)
    x_sorted, y_sorted = zip(*sorted_data)
    rects = ax2.bar(x_sorted, y_sorted, width)
    ax2.set_ylabel("CPU time of Operation in min")
    ax2.bar_label(rects)

    plt.show()


def analyse_1_6_13():
    memcounter = 0
    tabs = 0
    query_ids = [1, 6, 13]
    data_scale = [100, 300, 1000]
    worker_size = [4, 5, 6]
    query_names = []
    query_times = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    spilledData = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    wall_time_agg = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    wall_time_exc = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    wall_time_hash = np.zeros(
        (len(query_ids), len(worker_size), len(data_scale)), float
    )
    wall_time_join = np.zeros(
        (len(query_ids), len(worker_size), len(data_scale)), float
    )
    for qid in range(len(query_ids)):
        query_names.append("Query " + str(query_ids[qid]))
        tabs = 0
        printEingerückt(str(qid) + ":", tabs)
        tabs += 1
        for scale in range(len(data_scale)):
            for worker in range(len(worker_size)):
                name = (
                    "tpc_"
                    + str(query_ids[qid])
                    + "_"
                    + str(worker_size[worker])
                    + "_"
                    + str(data_scale[scale])
                    + ".json"
                )
                print(name)
                try:
                    directory = "tpc"
                    f = open(os.path.join(directory, name))
                except:
                    print("File " + name + " not found.")
                    continue

                data = json.load(f)

                qTime = transTimes(data["queryStats"]["executionTime"])
                printEingerückt("ExecutionTime: " + str(qTime), tabs)
                query_times[qid][worker][scale] = qTime

                qData = convertByteToGB(data["queryStats"]["spilledDataSize"])
                printEingerückt("Spilled Data: " + str(qData) + " GB", tabs)
                spilledData[qid][scale][worker] = qData

                # p_user_mem = convertByteToGB(data["queryStats"]["peakUserMemoryReservation"])
                # printEingerückt("Peak User Memory: " + str(p_user_mem) + " GB", tabs)
                # query_p_user_mem[worker][qid].append(p_user_mem)
                # qspillSize[memcounter][threshcounter] = qData
                # qexecTime[memcounter][threshcounter] = float(qTime[:-1])
                for i in data["queryStats"]["operatorSummaries"]:
                    printEingerückt(i["operatorType"], tabs)
                    if i["operatorType"] == "HashBuilderOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_hash[qid][scale][worker] += wtime

                    elif i["operatorType"] == "LookupJoinOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_join[qid][scale][worker] += wtime

                    elif i["operatorType"] == "HashAggregationOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_agg[qid][scale][worker] += wtime

                    elif i["operatorType"] == "AggregationOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_agg[qid][scale][worker] += wtime

                    elif "Exchange" in i["operatorType"]:
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_exc[qid][scale][worker] += wtime
                    else:
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_exc[qid][scale][worker] += wtime
                f.close()
        tabs -= 1

    for h in range(1):
        data = query_times if h == 0 else spilledData

        x = np.arange(len(query_ids))
        width = 0.25  # the width of the bars
        labels = ["4GB Heapspace", "5GB Heapspace", "6GB Heapspace"]
        for k in range(3):
            fig, axs = plt.subplots()
            ax = axs
            bottom = np.zeros(len(query_ids))
            for i in range(3):
                rects = ax.bar(x + width * i, data[k][i], width, label=labels[i])
                ax.bar_label(rects, padding=2, fontsize=20)
            ax.set_xticks(x + width)
            ax.set_xticklabels(data_scale, fontsize=20)
            ax.legend(loc="upper right", fontsize=20)
            ax.set_ylabel("Query Execution-time in min", fontsize=20)
            ax.grid(visible=True, linestyle="dashed")
            ax.set_axisbelow(True)

    for k in range(3):
        times = {
            "Join": np.array(wall_time_join[2][k]),
            "Aggregation": np.array(wall_time_agg[2][k]),
            "Hash": np.array(wall_time_hash[2][k]),
            "Exchange": np.array(wall_time_exc[2][k]),
        }
        # makeBarFig(times, np.array(labels), "Wall time in min")

    makeBarFig(
        {"Spilled Data": np.array(spilledData[2][2])},
        labels,
        "Spilled data in GB",
    )


# TPC()
# analyse_Query("8")
analyse_1_6_13()
