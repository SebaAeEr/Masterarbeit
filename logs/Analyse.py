import json
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


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


def makeBarFig(
    data,
    xlabels,
    ylabel,
    show_bar_label: bool = True,
    xaxis_label: str = "",
    markings: bool = False,
    marking_labels: list = [],
):

    fig, ax = plt.subplots()
    width = 0.18
    space = 0.05
    big_space = 0.5
    x = np.arange(len(xlabels))
    x = x * big_space
    colors = plt.cm.viridis.colors
    nth = int(len(colors) / len(list(data[0].keys())))
    colors = colors[nth - 1 :: nth]
    for i in range(len(data)):
        bottom = np.zeros(len(xlabels))
        counter = 0
        for label, datum in data[i].items():
            rects = ax.bar(
                x + (space + width) * i,
                datum,
                width,
                bottom=bottom,
                color=colors[counter],
            )
            bottom += datum
            counter += 1
            if show_bar_label:
                ax.bar_label(rects, padding=2)
    ax.legend(list(data[0].keys()), loc="upper right")
    if markings:
        for it in range(2):
            if it % 2 == 0:
                color = "blue"
                xdiff = space + width
                ydiff = 0
            else:
                color = "red"
                xdiff = 0
                ydiff = space + width
            for xth in x:
                xth = xth + (space + width) * (len(data) - 1) * 0.5
                ax.axvspan(xth - xdiff, xth + ydiff, color=color, alpha=0.15)
                plt.text(
                    xth - (xdiff - ydiff) / 2,
                    max(bottom) * 1.2,
                    marking_labels[it],
                    color=color,
                    ha="center",
                    # bbox=dict(facecolor="white", edgecolor="none", alpha=1.0),
                )

    ax.set_xticks(x + (space + width) * (len(data) - 1) * 0.5)
    ax.set_xticklabels(xlabels)

    ax.set_ylabel(ylabel)
    ax.set_xlabel(xaxis_label)
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
                print("File " + name + " not found.")
                # continue
                try:
                    name = "tpc_" + str(k) + "_9_1000.json"
                    f = open(os.path.join(directory, name))
                except:
                    print("failed to open failed!")
                    continue

        query_names.append(str(k))
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
    spills = [
        {
            "Join": np.array(query_join_spills)[filter],
            "Aggregation": np.array(query_agg_spills)[filter],
            "Hash": np.array(query_hash_spills)[filter],
            # "Other": np.array(other)[filter],
        }
    ]
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
    times = [
        {
            "Join": query_times_cpu_join,
            "Aggregation": query_times_cpu_agg,
            # "Exchange": query_times_ex,
            "Hash": query_times_cpu_hash,
            # "Other": all_time,
        }
    ]
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
    query_ids = [13]  # [4, 17, 20]  # [1, 6, 13]
    data_scale = [1000]  # [300, 1000]  # [100, 300, 1000]
    worker_size = [4, 45, 5, 6, 10]  # [8, 9, 10]  # [4, 5, 6]
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
    spill_agg = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    spill_hash = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    spill_join = np.zeros((len(query_ids), len(worker_size), len(data_scale)), float)
    for qid in range(len(query_ids)):
        query_names.append("Query " + str(query_ids[qid]))
        tabs = 0
        printEingerückt(str(query_ids[qid]) + ":", tabs)
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
                spilledData[qid][worker][scale] = qData

                # p_user_mem = convertByteToGB(data["queryStats"]["peakUserMemoryReservation"])
                # printEingerückt("Peak User Memory: " + str(p_user_mem) + " GB", tabs)
                # query_p_user_mem[worker][qid].append(p_user_mem)
                # qspillSize[memcounter][threshcounter] = qData
                # qexecTime[memcounter][threshcounter] = float(qTime[:-1])
                for i in data["queryStats"]["operatorSummaries"]:
                    printEingerückt(i["operatorType"], tabs)
                    if i["operatorType"] == "HashBuilderOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_hash[qid][worker][scale] += wtime
                        spill_hash[qid][worker][scale] += spill
                    elif i["operatorType"] == "LookupJoinOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_join[qid][worker][scale] += wtime
                        spill_join[qid][worker][scale] += spill

                    elif i["operatorType"] == "HashAggregationOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_agg[qid][worker][scale] += wtime
                        spill_agg[qid][worker][scale] += spill

                    elif i["operatorType"] == "AggregationOperator":
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_agg[qid][worker][scale] += wtime
                        spill_agg[qid][worker][scale] += spill

                    elif "Exchange" in i["operatorType"]:
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_exc[qid][worker][scale] += wtime
                    else:
                        wtime, cputime, spill = CollectOPData(i, tabs + 1)
                        wall_time_exc[qid][worker][scale] += wtime
                f.close()
        tabs -= 1

    # labels = ["100GB", "300GB", "1000GB"]
    labels = ["1000GB"]
    # for h in range(2):
    #     data = query_times if h == 0 else spilledData
    #     for k in range(3):
    #         times = [
    #             {"": np.array(data[k][0])},
    #             {"": np.array(data[k][1])},
    #             {"": np.array(data[k][2])},
    #         ]
    #         makeBarFig(times, np.array(labels), "Wall time in min", False)

    # labels = [
    #     str(worker_size[0]) + "GB Heapspace",
    #     str(worker_size[1]) + "GB Heapspace",
    #     str(worker_size[2]) + "GB Heapspace",
    # ]

    makeBarFig(
        [
            {
                "Join": np.array(spill_join[0][0]),
                "Aggregation": np.array(spill_agg[0][0]),
                "Hash": np.array(spill_hash[0][0]),
            },
            {
                "Join": np.array(spill_join[0][1]),
                "Aggregation": np.array(spill_agg[0][1]),
                "Hash": np.array(spill_hash[0][1]),
            },
            {
                "Join": np.array(spill_join[0][2]),
                "Aggregation": np.array(spill_agg[0][2]),
                "Hash": np.array(spill_hash[0][2]),
            },
            {
                "Join": np.array(spill_join[0][3]),
                "Aggregation": np.array(spill_agg[0][3]),
                "Hash": np.array(spill_hash[0][3]),
            },
        ],
        labels,
        "Spilled data in GB",
    )

    data = [
        {
            "Join": np.array(wall_time_join.squeeze()),
            "Aggregation": np.array(wall_time_agg.squeeze()),
            "Hash": np.array(wall_time_hash.squeeze()),
        }
    ]
    x_positions = spilledData.squeeze()

    fig, ax = plt.subplots()
    width = 0.1
    space = 0.05
    colors = plt.cm.viridis.colors
    nth = int(len(colors) / len(list(data[0].keys())))
    colors = colors[nth - 1 :: nth]
    for i in range(len(data)):
        bottom = np.zeros(len(x_positions))
        counter = 0
        for label, datum in data[i].items():
            rects = ax.bar(
                x_positions,
                datum,
                width,
                bottom=bottom,
                color=colors[counter],
                label=label,
            )
            bottom += datum
            counter += 1
            ax.bar_label(rects, padding=2, fontsize=20)

    ax.set_xlabel("Spilled data in GB", fontsize=20)
    ax.legend(list(data[0].keys()), loc="upper right", fontsize=20)
    ax.set_ylabel("Wall time in min", fontsize=20)
    ax.grid(visible=True, linestyle="dashed")
    ax.set_axisbelow(True)
    plt.show()
    # for k in range(3):
    #     times = [
    #         {
    #             "Join": np.array(wall_time_join[k][0]),
    #             "Aggregation": np.array(wall_time_agg[k][0]),
    #             "Hash": np.array(wall_time_hash[k][0]),
    #             #   "Exchange": np.array(wall_time_exc[k][0]),
    #         },
    #         {
    #             "Join": np.array(wall_time_join[k][1]),
    #             "Aggregation": np.array(wall_time_agg[k][1]),
    #             "Hash": np.array(wall_time_hash[k][1]),
    #             #   "Exchange": np.array(wall_time_exc[k][0]),
    #         },
    #         {
    #             "Join": np.array(wall_time_join[k][2]),
    #             "Aggregation": np.array(wall_time_agg[k][2]),
    #             "Hash": np.array(wall_time_hash[k][2]),
    #             #   "Exchange": np.array(wall_time_exc[k][0]),
    #         },
    #         {
    #             "Join": np.array(wall_time_join[k][3]),
    #             "Aggregation": np.array(wall_time_agg[k][3]),
    #             "Hash": np.array(wall_time_hash[k][3]),
    #             #   "Exchange": np.array(wall_time_exc[k][0]),
    #         },
    #     ]
    #     makeBarFig(times, np.array(labels), "Wall time in min")
    #     makeBarFig(
    #         [
    #             {
    #                 "Join": np.array(spill_join[k][0]),
    #                 "Aggregation": np.array(spill_agg[k][0]),
    #                 "Hash": np.array(spill_hash[k][0]),
    #             },
    #             {
    #                 "Join": np.array(spill_join[k][1]),
    #                 "Aggregation": np.array(spill_agg[k][1]),
    #                 "Hash": np.array(spill_hash[k][1]),
    #             },
    #             {
    #                 "Join": np.array(spill_join[k][2]),
    #                 "Aggregation": np.array(spill_agg[k][2]),
    #                 "Hash": np.array(spill_hash[k][2]),
    #             },
    #         ],
    #         labels,
    #         "Spilled data in GB",
    #     )


def c_size_by_time():
    plt.rcParams.update({"font.size": 35})
    tpc_4_shuffled = False
    helpers = {}
    # first analyses
    # names = [
    #     "logfile_0_6_0_4_22-42.json",
    #     "logfile_4_6_100_4_21-55.json",
    #     "logfile_4_6_0_4_20-57.json",
    # ]
    # labels = np.array(["3W", "Local", "S3"])

    # partition analyses dyn
    # names = [
    #     "logfile_4_6_0_4_09-09.json",
    #     "logfile_4_6_0_4_09-33.json",
    #     "logfile_4_6_0_4_09-53.json",
    #     "logfile_4_6_0_4_10-14.json",
    # ]
    # labels = np.array(["25; 1T", "50; 2T", "75; 5T", "100; 5T"])

    # names = [
    #     "logfile_4_6_0_4_09-03.json",
    #     "logfile_4_6_0_4_09-09.json",
    #     "logfile_4_6_0_4_09-20.json",
    #     "logfile_4_6_0_4_09-33.json",
    #     "logfile_4_6_0_4_09-39.json",
    #     "logfile_4_6_0_4_09-53.json",
    #     "logfile_4_6_0_4_10-01.json",
    #     "logfile_4_6_0_4_10-14.json",
    # ]
    # labels = np.array(
    #     [
    #         "25",
    #         "50",
    #         "75",
    #         "100",
    #     ]
    # )

    # part only 6 merge Threads
    # names = [
    #     "logfile_4_6_0_4_13-41.json",
    #     "logfile_4_6_0_4_09-02.json",
    #     "logfile_4_6_0_4_09-19.json",
    #     "logfile_4_6_0_4_09-37.json",
    #     "logfile_4_6_0_4_09-57.json",
    # ]
    # labels = np.array(
    #     [
    #         "10",
    #         "25",
    #         "50",
    #         "75",
    #         "100",
    #     ]
    # )

    # deencode analyses
    # names = [
    #     "logfile_4_6_0_4_12-50.json",
    #     "logfile_4_6_0_4_12-22.json",
    #     # "logfile_4_6_100_4_12-12.json",
    #     # "logfile_4_6_100_4_19-32.json",
    # ]
    # labels = np.array(
    #     [
    #         "compression 0BM",
    #         "no compression 0BM",
    #         #   "compression 100BM",
    #         # "no compression 100BM",
    #     ]
    # )

    # with/out part
    # names = [
    #     "logfile_4_6_0_8_17-14.json",
    #     "logfile_4_6_0_8_14-43.json",
    #     "logfile_4_40_0_10_18-01.json",
    #     "logfile_4_40_0_10_17-40.json",
    # ]
    # labels = np.array(
    #     [
    #         "1; 0.9",
    #         "30; 0.9",
    #         "1; 0.5",
    #         "30; 0.5",
    #         # "30 partitions 2W",
    #         #   "compression 100BM",
    #         # "no compression 100BM",
    #     ]
    # )

    # Mapping size
    # names = [
    #     "logfile_4_6_0_4_15-59.json",
    #     "logfile_4_6_0_4_16-25.json",
    #     "logfile_4_6_0_4_16-51.json",
    #     "logfile_4_6_0_4_17-16.json",
    #     "logfile_4_6_0_4_17-56.json",
    #     "logfile_4_6_0_4_18-22.json",
    #     "logfile_4_6_0_4_18-50.json",
    # ]
    # labels = np.array(["0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6"])

    # split_mana
    # names = [
    #     "logfile_4_6_0_6_19-02.json",
    #     "logfile_4_6_0_6_19-28.json",
    #     "logfile_4_6_0_6_19-54.json",
    #     "logfile_4_6_0_6_20-39.json",
    #     "logfile_4_6_0_6_18-55.json",
    #     "logfile_4_6_0_6_21-38.json",
    # ]
    # labels = np.array(
    #     [
    #         "1 worker; no split",
    #         "1 worker; split",
    #         "2 worker; no split",
    #         "2 worker; split",
    #         "3 worker; no split",
    #         "3 worker; split",
    #     ]
    # )
    # split_mana shuffled
    # names = [
    #     "logfile_4_6_0_10_10-44.json",
    #     # "logfile_4_6_0_10_16-17.json",
    #     "logfile_4_6_0_10_11-32.json",
    #     # "logfile_4_6_0_10_16-21.json",
    #     # "logfile_4_6_0_10_20-58.json",  # worker logfile: logfile_4_6_0_4_20-58.json
    #     "logfile_4_6_0_10_14-39.json",  # worker logfile: logfile_4_6_0_4_14-39.json
    #     # "logfile_4_6_0_10_15-17.json",  # worker logfile: logfile_4_6_0_4_15-17.json
    #     "logfile_4_6_0_10_14-00.json",  # worker logfile: logfile_4_6_0_4_12-27.json
    # ]
    # labels = np.array(
    #     [
    #         "1 worker; no split",
    #         #  "1 worker; no split(2)",
    #         "1 worker; split",
    #         # "1 worker; split(2)",
    #         "2 worker; no split",
    #         "2 worker; split",
    #         # "2 worker; split(2)",
    #         # "3 worker; no split",
    #         # "3 worker; split",
    #     ]
    # )
    # helpers = {
    #     "no split": "logfile_4_6_0_4_14-39.json",
    #     "split": "logfile_4_6_0_4_14-00.json",
    # }

    # split_mana shuffled local + size
    # names = [
    #     "logfile_4_6_0_10_21-11.json",
    #     # "logfile_4_6_0_10_16-17.json",
    #     "logfile_4_6_0_10_20-24.json",
    #     # "logfile_4_6_0_10_16-21.json",
    #     # "logfile_4_6_0_10_20-58.json",  # worker logfile: logfile_4_6_0_4_20-58.json
    #     "logfile_4_6_0_10_22-34.json",  # worker logfile: logfile_4_6_0_4_14-39.json
    #     # "logfile_4_6_0_10_15-17.json",  # worker logfile: logfile_4_6_0_4_15-17.json
    #     "logfile_4_6_0_10_22-04.json",  # worker logfile: logfile_4_6_0_4_12-27.json
    # ]
    # labels = np.array(
    #     [
    #         "1 worker; no split",
    #         #  "1 worker; no split(2)",
    #         "1 worker; split",
    #         # "1 worker; split(2)",
    #         "2 worker; no split",
    #         "2 worker; split",
    #         # "2 worker; split(2)",
    #         # "3 worker; no split",
    #         # "3 worker; split",
    #     ]
    # )
    # helpers = {
    #     "no split": "logfile_4_6_0_4_22-34.json",
    #     "split": "logfile_4_6_0_4_22-04.json",
    # }

    # mem buffer
    # names = [
    #     "logfile_4_16_0_4_16-56.json",
    #     "logfile_4_16_0_4_17-23.json",
    #     "logfile_4_16_0_4_17-50.json",
    #     # "logfile_4_6_0_4_21-44.json",
    #     # "logfile_4_6_0_4_22-18.json",
    # ]
    # labels = np.array(
    #     [
    #         "4000000",
    #         "2000000",
    #         "1000000",
    #     ]
    # )

    # local vs local + s3 vs. s3
    names = [
        # "logfile_4_6_0_6_11-47.json",
        # "logfile_4_6_0_6_12-11.json",
        # "logfile_4_6_0_6_14-41.json",
        # "logfile_4_6_0_8_20-47.json",
        # "logfile_4_6_0_8_17-38.json",
        # "logfile_4_6_0_8_01-22.json",
        "logfile_4_6_0_8_20-47.json",
        "logfile_4_6_0_8_17-38.json",
        "logfile_4_6_0_8_12-03.json",
    ]
    labels = np.array(
        [
            "S3",
            "local + S3",
            "local",
        ]
    )

     # local vs local + s3 vs. s3 shuffled
    # names = [
    #     "logfile_4_6_0_10_21-06.json",
    #            "logfile_4_6_0_10_12-14.json",
    #         "logfile_4_6_0_10_22-06.json",
    # ]
    # labels = np.array(
    #     [
    #         "S3",
    #         "local + S3",
    #         "local",
    #     ]
    # )

    # threadNumber s3 + local
    # names = [
    #     "logfile_4_6_0_1_15-40.json",
    #     "logfile_4_6_0_2_16-28.json",
    #     "logfile_4_6_0_4_16-57.json",
    #     "logfile_4_6_0_6_17-19.json",
    #     "logfile_4_6_0_8_17-38.json",
    #     "logfile_4_6_0_12_22-38.json",
    #     "logfile_4_6_0_16_22-55.json",
    #     "logfile_4_6_0_20_23-13.json",
    # ]
    # labels = np.array(["1", "2", "4", "6", "8", "12", "16", "20"])

    # # threadNumber s3
    # names = [
    #     "logfile_4_6_0_1_18-27.json",
    #     "logfile_4_6_0_2_19-25.json",
    #     "logfile_4_6_0_4_20-01.json",
    #     "logfile_4_6_0_6_20-26.json",
    #     "logfile_4_6_0_8_20-47.json",
    #     "logfile_4_6_0_12_10-53.json",
    #     "logfile_4_6_0_16_11-12.json",
    #     "logfile_4_6_0_20_11-33.json",
    # ]
    # labels = np.array(["1", "2", "4", "6", "8", "12", "16", "20"])

    # threaNumber local
    # names = [
    #     "logfile_4_6_0_1_23-41.json",
    #     "logfile_4_6_0_2_00-30.json",
    #     "logfile_4_6_0_4_01-00.json",
    #     # "logfile_4_6_0_6_20-26.json",
    #     "logfile_4_6_0_8_01-22.json",
    #     "logfile_4_6_0_12_01-40.json",
    #     "logfile_4_6_0_16_01-58.json",
    #     "logfile_4_6_0_20_02-16.json",
    # ]
    # labels = np.array(["1", "2", "4", "8", "12", "16", "20"])

    thread_number_anal = False

    # thread number s3 + se + local
    # names = [
    #     "logfile_4_6_0_1_18-27.json",
    #     "logfile_4_6_0_1_15-40.json",
    #     "logfile_4_6_0_1_10-25.json",
    #     "logfile_4_6_0_2_19-25.json",
    #     "logfile_4_6_0_2_16-28.json",
    #     "logfile_4_6_0_2_11-14.json",
    #     "logfile_4_6_0_4_20-01.json",
    #     "logfile_4_6_0_4_16-57.json",
    #     "logfile_4_6_0_4_11-43.json",
    #     #  "logfile_4_6_0_4_01-00.json",
    #     # "logfile_4_6_0_6_20-26.json",
    #     # "logfile_4_6_0_6_17-19.json",
    #     "logfile_4_6_0_8_20-47.json",
    #     "logfile_4_6_0_8_17-38.json",
    #     "logfile_4_6_0_8_12-03.json",
    #     "logfile_4_6_0_12_10-53.json",
    #     "logfile_4_6_0_12_22-38.json",
    #     "logfile_4_6_0_12_12-17.json",
    #     "logfile_4_6_0_16_11-12.json",
    #     "logfile_4_6_0_16_22-55.json",
    #     "logfile_4_6_0_16_12-31.json",
    #     "logfile_4_6_0_20_11-33.json",
    #     "logfile_4_6_0_20_23-13.json",
    #     "logfile_4_6_0_20_12-44.json",
    # ]
    # labels = np.array(["1", "2", "4", "8", "12", "16", "20"])
    # thread_number_anal = True

    # merge helpe 4
    # names = [
    #     # "logfile_4_6_0_10_21-06.json",
    #     "logfile_4_6_0_10_12-14.json",
    #     # "logfile_4_6_0_10_22-06.json",
    #     "logfile_4_6_0_10_09-04.json",
    #     # "logfile_4_6_0_10_16-49.json",
    #     "logfile_4_6_0_10_12-50.json",
    # ]
    # labels = np.array(
    #     [
    #         # "local",
    #         "1 Worker",
    #         # "S3",
    #         "2 Worker",
    #         "3 Worker",
    #     ]
    # )

    # # # merge helpe 4 shuffled

    names = [
        #   "logfile_4_4_0_10_11-42.json",
              "logfile_4_4_0_10_18-53.json",
        #    "logfile_4_4_0_10_13-21.json",
        "logfile_4_4_0_10_14-24.json",
        # "logfile_4_4_0_10_14-54.json",
               "logfile_4_4_0_10_09-57.json",
        #    "logfile_4_6_0_10_21-06.json",
               "logfile_4_6_0_10_12-14.json",
        #     "logfile_4_6_0_10_22-06.json",
        "logfile_4_6_0_10_09-04.json",
        # "logfile_4_6_0_10_16-49.json",
              "logfile_4_6_0_10_12-50.json",
        #    "logfile_4_10_0_10_23-14.json",
               "logfile_4_10_0_10_23-33.json",
        #     "logfile_4_10_0_10_00-10.json",
        "logfile_4_10_0_10_09-14.json",
        # "logfile_4_10_0_10_17-14.json",
        #      "logfile_4_10_0_10_13-21.json",
             "logfile_4_15_0_10_07-57.json",
        #       "logfile_4_15_0_10_08-13.json",
              "logfile_4_15_0_10_08-36.json",
        # "logfile_4_15_0_10_08-36.json",
        "logfile_4_10_0_10_09-14.json",
        #        "logfile_4_15_0_10_20-28.json",
              "logfile_4_15_0_10_20-56.json",
        #      "logfile_4_20_0_10_10-32.json",
               "logfile_4_20_0_10_10-45.json",
        #       "logfile_4_20_0_10_11-03.json",
        "logfile_4_20_0_10_11-23.json",
               "logfile_4_20_0_10_21-43.json",
    ]
    labels = np.array(
        [
            #  "local",
            "S3 + local",
            # "S3",
            "2 Worker 1",
            "3 Worker",
            # "local",
            "S3 + local",
            # "S3",
            "2 Worker 2",
            "3 Worker",
            #  "local",
            "S3 + local",
            #   "S3",
            "2 Worker 3",
            "3 Worker",
            #   "local",
             "S3 + local",
            #   "S3",
            "2 Worker 4",
            "3 Worker",
            #   "local",
            "S3 + local",
            #   "S3",
            "2 Worker 5",
            "3 Worker",
        ]
    )
    runtimes = {
        #   "local": np.zeros(5),
        # "local + S3": np.zeros(5),
        #   "S3": np.zeros(5),
          "1 Worker": np.zeros(5),
        "2 Worker": np.zeros(5),
          "3 Worker": np.zeros(5),
    }
    runtime_x = [4, 6, 10, 15, 20]
    tpc_4_shuffled = True
    helpers = {
        "2 Worker": "logfile_4_6_0_4_22-04.json",
        "3 Worker (1)": "logfile_4_6_0_4_13-50.json",
        "3 Worker (2)": "logfile_4_6_0_4_13-50(2).json",
    }
    subplot = 0
    subruntimes = {
        #   "local": np.zeros(5),
        # "local + S3": np.zeros(5),
        #   "S3": np.zeros(5),
        "Write time of spill files": np.zeros(5),
        "Scan duration": np.zeros(5),
        "Merge duration": np.zeros(5),
    }

    # merge help 20
    # names = [
    #     # "logfile_20_3_0_10_16-02.json",
    #     # "logfile_20_3_0_10_16-06.json",
    #     # "logfile_20_3_0_10_16-17.json",
    #     # "logfile_20_3_0_10_16-37.json",
    #     "logfile_20_6_0_10_13-52.json",
    #     "logfile_20_6_0_10_13-55.json",
    #     "logfile_20_6_0_10_14-04.json",
    #     # "logfile_20_6_0_10_14-17.json",
    #     "logfile_20_6_0_10_14-55.json",
    #     # "logfile_20_10_0_10_15-08.json",
    #     # "logfile_20_10_0_10_15-11.json",
    #     # "logfile_20_10_0_10_15-19.json",
    #     # "logfile_20_10_0_10_15-47.json",
    # ]
    # labels = np.array(
    #     [
    #         # "local; 3",
    #         # "S3 + local; 3",
    #         # "S3; 3",
    #         # "2 Worker; 3",
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker",
    #         # "local; 10",
    #         # "S3 + local; 10",
    #         # "S3; 10",
    #         # "2 Worker; 10",
    #     ]
    # )

    trino = False


    #Trino 20 4.2 spill: 16.32 GiB
    # t_scan_dur = 241
    # t_query_dur = 1080
    # trino = True




    for label, helper in helpers.items():
        jf = open(os.path.join("c++_logs", helper))
        jf_data = json.load(jf)
        print(helper + ":")
        printEingerückt(
            "Number of written files: " + str(len(jf_data["writeCall_s3_file_dur"])), 1
        )
        printEingerückt(
            "Sum of size of written files: "
            + str(sum(jf_data["writeCall_s3_file_size"]) / 2**20),
            1,
        )
        printEingerückt(
            "Number of written mana files: " + str(len(jf_data["get_mana_dur"])), 1
        )
        merge_file_num = np.cumsum(np.array(jf_data["mergeFiles_num"]))
        merge_file_times = np.array(jf_data["mergeFiles_time"]) / 1000000
        plt.figure(9)
        plt.plot(merge_file_times, merge_file_num, label=label, linewidth=3)
        plt.legend()
        plt.xlabel("Time in s")
        plt.ylabel("Number of files merged")
        printEingerückt(
            "merged files per second: "
            + str(merge_file_num[-1] / merge_file_times[-1]),
            1,
        )

    thread_number_x = np.array([1, 2, 4, 8, 12, 16, 20])
    thread_number_y_sl = np.empty(len(labels))
    thread_number_y_s = np.empty(len(labels))
    thread_number_y_l = np.empty(len(labels))

    thread_number_y_sl_scan = np.empty(len(labels))
    thread_number_y_s_scan = np.empty(len(labels))
    thread_number_y_l_scan = np.empty(len(labels))

    thread_number_y_sl_write = np.empty(len(labels))
    thread_number_y_s_write = np.empty(len(labels))
    thread_number_y_l_write = np.empty(len(labels))

    try:
        directory = "c++_logs"
        f = open(os.path.join(directory, "times_20_3_0_10_16-02.csv"))
        jf = open(os.path.join(directory, "logfile_20_3_0_10_16-02.json"))
    except:
        print("File not found.")
        return
    df = pd.read_csv(f)
    jf_data = json.load(jf)
    # Step 2: Extract the columns you want to plot
    # Assuming the columns are named 'Column1' and 'Column2' (change these to match your CSV)
    scale = 2**30
    x = df["time"] / 1000
    mes_y = df["mes_size"] / scale
    hmap_y = df["hmap_size"] / scale
    base_y = df["base_size"] / scale
    map_y = df["map_size"] / scale
    bit_y = df["bit_size"] / scale
    avg_y = df["avg"]
    calc_y = hmap_y + base_y + map_y + bit_y

    # Step 3: Create the plot
    plt.figure(1)

    plt.plot(x, mes_y, label="measured size", linewidth=3)
    plt.plot(x, hmap_y, label="Hashmap size", linewidth=3)
    plt.plot(x, base_y, label="base size")
    plt.plot(x, map_y, label="mapping size")
    plt.plot(x, bit_y, label="bitmap size")
    plt.plot(
        x, calc_y, label="calc overall size"
    )  # Line plot (you can change to scatter plot or others)

    try:
        keys = ["scanTime", "mergeHashTime", "mergeTime"]
        phase_labels = ["Scan", "", "Merge"]
        colors = ["blue", "white", "blue"]
        old_value = 0
        counter = 0
        for key in keys:
            x_value = jf_data[key] / 1000000
            plt.axvline(x=x_value, color="blue", linestyle="--", linewidth=2)
            plt.axvspan(old_value, x_value, color=colors[counter], alpha=0.15)
            plt.text(
                old_value + (x_value - old_value) / 2,
                -0.005,
                phase_labels[counter],
                color="blue",
                ha="center",
                # bbox=dict(facecolor="white", edgecolor="none", alpha=1.0),
            )
            counter += 1
            old_value = x_value

        plt.axline([0, 6], [x.max(), 6], color="red", linestyle="--", linewidth=2)
    except:
        print("no scan/merge/mergeHash-time")

    plt.xlabel("time in s")  # Label for x-axis
    plt.ylabel("size in GiB")  # Label for y-axis
    # plt.title("size over time")  # Title of the plot
    plt.legend(loc="upper right")  # Show the legend

    plt.figure(2)
    plt.plot(x, avg_y, label="Average")
    counter = 0
    arr_len = len(names)
    if trino:
        arr_len += 1

    times = [
        {
            "Write time of spill files": np.empty(arr_len),
            "Scan duration": np.empty(arr_len),
            #  "Spill rest duration":np.empty(len(names)),
            # "merge_hash_dur": np.empty(),
            # "get_file_sum": np.empty(),
            # "Write time of the output": np.empty(len(names)),
            "Merge duration": np.empty(arr_len),
            # "read_tuple_sum": np.empty(),
            # "Exchange": np.empty(),
        }
    ]
    if trino:
        times[0]["Write time of spill files"][0] = 0
        times[0]["Scan duration"][0] = t_scan_dur
        times[0]["Merge duration"][0] = t_query_dur - t_scan_dur
        counter = 1
        labels = np.insert(labels, 0, "Trino")

    # times = [
    #     {
    #         "Write time of spill files": np.empty(int(len(names) / 2)),
    #         "Scan duration": np.empty(int(len(names) / 2)),
    #         # "merge_hash_dur": np.empty(),
    #         # "get_file_sum": np.empty(),
    #         "Write time of the output": np.empty(int(len(names) / 2)),
    #         "Merge duration": np.empty(int(len(names) / 2)),
    #         # "read_tuple_sum": np.empty(),
    #         # "Exchange": np.empty(),
    #     },
    #     {
    #         "Write time of spill files": np.empty(int(len(names) / 2)),
    #         "Scan duration": np.empty(int(len(names) / 2)),
    #         # "merge_hash_dur": np.empty(),
    #         # "get_file_sum": np.empty(),
    #         "Write time of the output": np.empty(int(len(names) / 2)),
    #         "Merge duration": np.empty(int(len(names) / 2)),
    #         # "read_tuple_sum": np.empty(),
    #         # "Exchange": np.empty(),
    #     },
    # ]
    merge_thread_num = []
    sub_counter = 0
    for name in names:
        jf = open(os.path.join(directory, name))
        jf_data = json.load(jf)
        print(name + ":")
        tabs = 1
        printEingerückt("lines read: " + str(jf_data["linesRead"]), tabs)
        spill_times = np.array(jf_data["Threads"][0]["spillTimes"]) / 1000000
        differences = [
            abs(spill_times[i + 1] - spill_times[i])
            for i in range(len(spill_times) - 1)
        ]
        printEingerückt(
            "average spill frequency: " + str(sum(differences) / len(differences)), tabs
        )
        printEingerückt(
            "Post Scan selectivity: " + str(jf_data["selectivityPostScan"]), tabs
        )
        # plt.figure(3)
        # plt.hist(get_mana_dur, bins=30, label="get_mana_dur")
        # plt.title("get_mana_dur")
        get_mana_dur = jf_data["get_mana_dur"]
        average = sum(get_mana_dur) / len(get_mana_dur)
        printEingerückt("get_mana_dur avg: " + str(average), tabs)

        write_mana_dur = np.array(jf_data["write_mana_dur"]) / 1000000
        # plt.figure(4)
        # plt.hist(write_mana_dur, bins=30, label="write_mana_dur")
        # plt.title("write_mana_dur")
        average = sum(write_mana_dur) / len(write_mana_dur)
        printEingerückt("write_mana_dur avg: " + str(average), tabs)
        printEingerückt("write_mana_dur sum: " + str(sum(write_mana_dur)), tabs)
        printEingerückt("write_mana_dur times: " + str(len(write_mana_dur)), tabs)

        get_lock_dur = np.array(jf_data["get_lock_dur"]) / 1000000
        # plt.figure(3)
        # plt.hist(get_lock_dur, bins=30, label="get_lock_dur")
        # plt.title("get_lock_dur")
        average = sum(get_lock_dur) / len(get_lock_dur)
        printEingerückt("get_lock_dur avg: " + str(average), tabs)
        printEingerückt("get_lock_dur sum: " + str(sum(get_lock_dur)), tabs)

        write_file_dur = np.array(jf_data["writeCall_s3_file_dur"]) / 1000000
        write_file_size = np.array(jf_data["writeCall_s3_file_size"]) / 2**20
        write_file_dur.sort()
        write_file_size.sort()
        # if len(names) == len(labels):
        #     plt.figure(4)
        #     plt.scatter(write_file_dur, write_file_size, label=labels[counter])
        #     plt.legend()
        #     plt.xlabel("Time in s")
        #     plt.ylabel("Size in MiB")
        #     # plt.plot(write_file_dur, write_file_size, label="write file duration")
        #     plt.title("write file dur per size")

        # if len(names) == len(labels):
        #     plt.figure(5)
        #     plt.hist(
        #         write_file_dur, bins=100, label=labels[counter], alpha=1, rwidth=0.4
        #     )
        #     plt.legend()
        #     plt.xlabel("Time in s")
        #     plt.ylabel("Size in MiB")
        #     plt.title("write file dur")

        if len(names) == len(labels):
            plt.figure(6)
            ecdf_values = np.arange(1, len(write_file_dur) + 1) / len(write_file_dur)
            plt.step(write_file_dur, ecdf_values, label=labels[counter], linewidth=3)
            plt.grid(visible=True, linestyle="dashed")
            plt.legend()
            plt.xlabel("Time in s")
            plt.ylabel("ECDF")

        if len(names) == len(labels):
            plt.figure(8)
            write_spill_dur = np.array(jf_data["write_spill_durs"]) / 1000000
            write_spill_dur.sort()
            ecdf_values = np.arange(1, len(write_spill_dur) + 1) / len(write_spill_dur)
            plt.step(write_spill_dur, ecdf_values, label=labels[counter], linewidth=3)
            plt.grid(visible=True, linestyle="dashed")
            plt.legend()
            plt.xlabel("Time in s")
            plt.ylabel("ECDF")

        if len(names) == len(labels):
            plt.figure(7)
            get_lock_dur.sort()
            ecdf_values = np.arange(1, len(get_lock_dur) + 1) / len(get_lock_dur)
            plt.step(get_lock_dur, ecdf_values, label=labels[counter], linewidth=3)
            plt.grid(visible=True, linestyle="dashed")
            plt.legend()
            plt.xlabel("Time in s")
            plt.ylabel("ECDF")

        # plt.title("write file dur")
        # axs.ecdf(df_reads0.latency+10, label="Lvl 1 - Reads (exp.)")
        if len(write_file_dur) > 0:
            average_1 = sum(write_file_dur) / len(write_file_dur)
            printEingerückt("write_file_dur avg: " + str(average_1), tabs)

            average_2 = sum(write_file_size) / len(write_file_size)
            printEingerückt("write_file_size avg: " + str(average_2), tabs)
            printEingerückt("number of spills: " + str(len(write_file_dur)), tabs)
            printEingerückt(
                "write_file_size / write_file_dur  avg: " + str(average_2 / average_1),
                tabs,
            )
            printEingerückt("write_file_size sum: " + str(sum(write_file_size)), tabs)

        # plt.figure(8)
        get_file_dur = jf_data["getCall_s3_file_dur"]
        # plt.hist(get_file_dur, bins=30, label="get_file_dur")
        # plt.title("get_file_dur")
        if len(get_file_dur) > 0:
            average = sum(get_file_dur) / len(get_file_dur)
            printEingerückt("get_file_dur avg: " + str(average), tabs)

        # plt.figure(9)
        # plt.plot(x, hmap_y * scale / avg_y, label="hmap_size")

        write_file_sum = 0
        for thread in jf_data["Threads"]:
            write_file_sum += thread["write_file_dur"]

        write_file_sum /= jf_data["threadNumber"] * 1000000

        get_file_sum = sum(jf_data["getCall_s3_file_dur"]) / (
            jf_data["mergeThread_number"]
            * 1000000
            # jf_data["threadNumber"]
            * 1000000
        )
        printEingerückt("get_file_sum: " + str(get_file_sum), tabs)
        # read_tuple_sum = jf_data["get_tuple_dur"] / 1000000
        write_output_sum = jf_data["write_output_dur"] / (
            1000000 * jf_data["mergeThread_number"]
        )
        printEingerückt("Write output sum: " + str(write_output_sum), tabs)
        merge_dur = jf_data[
            "mergeDuration"
        ]  # - write_output_sum  # (get_file_sum + write_output_sum)
        scan_dur = jf_data["scanDuration"] - write_file_sum
        merge_hash_dur = jf_data["mergeHashDuration"]

        adding = scan_dur + merge_dur + write_file_sum
        # adding = scan_dur + merge_dur
        # adding = (scan_dur + merge_dur ) / (write_file_sum + scan_dur + merge_dur)
        if tpc_4_shuffled:
            rest = counter % len(runtimes)
            if rest == 0:
                runtimes["1 Worker"][int((counter) / len(runtimes))] = adding
            elif rest == 1:
                runtimes["2 Worker"][int((counter - 1) / len(runtimes))] = adding
            elif rest == 2:
                runtimes["3 Worker"][int((counter - 2) / len(runtimes))] = adding
            if rest == subplot:
                subruntimes["Merge duration"][
                    int((counter - subplot) / len(runtimes))
                ] = (merge_dur + scan_dur + write_file_sum)
                subruntimes["Scan duration"][
                    int((counter - subplot) / len(runtimes))
                ] = (scan_dur + write_file_sum)
                subruntimes["Write time of spill files"][
                    int((counter - subplot) / len(runtimes))
                ] = write_file_sum
            # if rest == 0:
            #     runtimes["local"][int(counter / len(runtimes))] = adding
            # elif rest == 1:
            #     runtimes["local + S3"][int((counter - 1) / len(runtimes))] = adding
            # elif rest == 2:
            #     runtimes["S3"][int((counter - 2) / len(runtimes))] = adding
            # elif rest == 3:
            #     runtimes["2 Worker"][int((counter - 3) / len(runtimes))] = adding
            # elif rest == 4:
            #     runtimes["3 Worker"][int((counter - 3) / len(runtimes))] = adding

        times[0]["Write time of spill files"][counter] = write_file_sum
        times[0]["Scan duration"][counter] = scan_dur
        # times[0]["Spill rest duration"][counter] = merge_hash_dur
        # times[0]["Write time of the output"][counter] = write_output_sum
        times[0]["Merge duration"][counter] = merge_dur
        # times[counter % 2]["Write time of spill files"][sub_counter] = write_file_sum
        # times[counter % 2]["Scan duration"][sub_counter] = scan_dur
        # times[counter % 2]["Write time of the output"][sub_counter] = write_output_sum
        # times[counter % 2]["Merge duration"][sub_counter] = merge_dur

        if counter % 3 == 0:
            thread_number_y_s[sub_counter] = jf_data[
                "scanDuration"
            ]  # jf_data["queryDuration"]
            thread_number_y_s_scan[sub_counter] = jf_data["scanDuration"]
            thread_number_y_s_write[sub_counter] = write_file_sum
        elif counter % 3 == 1:
            thread_number_y_sl[sub_counter] = jf_data[
                "scanDuration"
            ]  # jf_data["queryDuration"]
            thread_number_y_sl_scan[sub_counter] = jf_data["scanDuration"]
            thread_number_y_sl_write[sub_counter] = write_file_sum
        else:
            thread_number_y_l[sub_counter] = jf_data[
                "scanDuration"
            ]  # jf_data["queryDuration"]
            thread_number_y_l_scan[sub_counter] = jf_data["scanDuration"]
            thread_number_y_l_write[sub_counter] = write_file_sum

        if counter % 3 == 2:
            sub_counter += 1
        counter += 1

        # times +=   [ {
        # "write_file_sum": np.array([write_file_sum]),
        # "scan_dur": np.array([scan_dur]),
        # "merge_hash_dur": np.array([merge_hash_dur]),
        # "get_file_sum": np.array([get_file_sum]),
        # "write_output_sum": np.array([write_output_sum]),
        # "merge_dur": np.array([merge_dur]),
        # "read_tuple_sum": np.array([read_tuple_sum]),
        #   "Exchange": np.array(wall_time_exc[k][0]),
        #     }
        # ]
        # merge_thread_num.append(jf_data["mergeThread_number"])

    if tpc_4_shuffled:
        plt.figure(10)
        for label, value in runtimes.items():
            plt.plot(runtime_x, value, label=label, linewidth=3)
        plt.legend()
        plt.xlabel("Size of Main Memory in GiB")
        plt.ylabel("Query runtime in s")
        plt.grid(visible=True, linestyle="dashed")

        plt.figure(11)
        colors = plt.cm.viridis.colors
        nth = int(len(colors) / 3)
        colors = colors[nth - 1 :: nth]
        downy = np.zeros(5)
        counter = 0
        for label, value in subruntimes.items():
            plt.plot(runtime_x, value, label=label, linewidth=3, color=colors[counter])
            plt.fill_between(runtime_x, value, downy, color=colors[counter], alpha=0.2)
            downy = value
            counter += 1
        plt.legend()
        plt.xlabel("Size of Main Memory in GiB")
        plt.ylabel("Query runtime in s")
        plt.grid(visible=True, linestyle="dashed")

    if thread_number_anal:
        plt.figure(4)
        plt.plot(thread_number_x, thread_number_y_s, label="S3", linewidth=3)
        plt.plot(thread_number_x, thread_number_y_sl, label="S3+local", linewidth=3)
        plt.plot(thread_number_x, thread_number_y_l, label="local", linewidth=3)

        # plt.plot(thread_number_x, thread_number_y_s_scan, label="S3 scan")
        # plt.plot(thread_number_x, thread_number_y_sl_scan, label="S3+local scan")
        # plt.plot(thread_number_x, thread_number_y_l_scan, label="local scan")

        # plt.plot(thread_number_x, thread_number_y_s_write, label="S3 write")
        # plt.plot(thread_number_x, thread_number_y_sl_write, label="S3+local write")
        # plt.plot(thread_number_x, thread_number_y_l_write, label="local write")
        plt.legend()
        plt.xlabel("Number of Threads")
        plt.ylabel("Network throughput in MiB/s")
        plt.grid(visible=True, linestyle="dashed")

        plt.figure(5)
        perc_s = [
            (
                (thread_number_y_s[i - 1] - thread_number_y_s[i])
                * 100
                / thread_number_y_s[i - 1]
            )
            for i in range(1, len(thread_number_y_s))
        ]
        smoothed_perc_s = pd.Series(perc_s).rolling(window=2, min_periods=1).mean()
        perc_sl = [
            (
                (thread_number_y_sl[i - 1] - thread_number_y_sl[i])
                * 100
                / thread_number_y_sl[i - 1]
            )
            for i in range(1, len(thread_number_y_sl))
        ]
        smoothed_perc_sl = pd.Series(perc_sl).rolling(window=2, min_periods=1).mean()
        perc_l = [
            (
                (thread_number_y_l[i - 1] - thread_number_y_l[i])
                * 100
                / thread_number_y_l[i - 1]
            )
            for i in range(1, len(thread_number_y_l))
        ]
        smoothed_perc_l = pd.Series(perc_l).rolling(window=2, min_periods=1).mean()
        # plt.plot(thread_number_x[1::], perc_s, label="S3", linewidth=3)
        # plt.plot(thread_number_x[1::], perc_sl, label="S3+local", linewidth=3)
        # plt.plot(thread_number_x[1::], perc_l, label="local", linewidth=3)
        plt.plot(thread_number_x[1::], smoothed_perc_s / 100, label="S3", linewidth=3)
        plt.plot(
            thread_number_x[1::], smoothed_perc_sl / 100, label="S3+local", linewidth=3
        )
        plt.plot(
            thread_number_x[1::], smoothed_perc_l / 100, label="local", linewidth=3
        )
        plt.legend()
        plt.xlabel("Number of Threads")
        plt.ylabel("Proportional runtime improvement")
        plt.grid(visible=True, linestyle="dashed")
    else:
        if len(names) > 0:
            # s1 = "dynamic"
            # s2 = "static"
            s1 = "S3+local"
            s2 = "S3"
            marking_labels = []
            for n in range(len(names)):
                if n % 2 == 0:
                    s = s2
                else:
                    s = s1
                marking_labels.append(s)
            makeBarFig(
                times,
                labels,
                "Time in s",
                True,
                #"Number of Partitions",
                # markings=True,
                # marking_labels=marking_labels,
            )
            print(str(times))
    # bottom = 0
    # for datum in dates:
    #     rects = plt.bar(
    #         0,
    #         datum,
    #         bottom=bottom,
    #     )
    #     bottom += datum

    # Step 4: Show the plot
    plt.show()


# TPC()
# analyse_Query("8")
c_size_by_time()
# analyse_1_6_13()
