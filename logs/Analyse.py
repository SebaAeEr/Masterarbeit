import json
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.lines as mlines
from Model import main
import math


def convertByteToGB(byte: str):
    return int(byte[:-1]) / 1074000000


def printEingerückt(string: str, tabs: int):
    space = ""
    for i in range(0, tabs):
        space += "\t"
    print(space + string)


def combinemergeFiles(name1, name2):
    jf = open(os.path.join("c++_logs", name1))
    jf_data1 = json.load(jf)
    jf = open(os.path.join("c++_logs", name2))
    jf_data2 = json.load(jf)

    merge_file_num1 = np.cumsum(np.array(jf_data1["mergeFiles_num"]))
    merge_file_times1 = np.array(jf_data1["mergeFiles_time"]) / 1000000
    merge_file_num2 = np.cumsum(np.array(jf_data2["mergeFiles_num"]))
    merge_file_times2 = np.array(jf_data2["mergeFiles_time"]) / 1000000
    # Combine all timestamps from both arrays
    all_timestamps = np.unique(np.concatenate((merge_file_times1, merge_file_times2)))

    # Interpolate the counters to match the common timestamps
    counters1_interp = np.interp(all_timestamps, merge_file_times1, merge_file_num1)
    counters2_interp = np.interp(all_timestamps, merge_file_times2, merge_file_num2)

    # Calculate the sum of the interpolated counters
    return counters1_interp + counters2_interp, all_timestamps


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
    drivers = i["totalDrivers"]

    spilltemp = convertByteToGB(i["spilledDataSize"])
    # printEingerückt("Spill Data: " + str(spilltemp) + " GB", tabs)
    cpu_time = (
        transTimes(i["addInputCpu"])
        + transTimes(i["getOutputCpu"])
        + transTimes(i["finishCpu"])
    )  # / drivers
    wall_time = (
        transTimes(i["addInputWall"])
        + transTimes(i["getOutputWall"])
        + transTimes(i["finishWall"])
    )  # / drivers
    printEingerückt("Wall time: " + str(wall_time), tabs)
    return wall_time, cpu_time, spilltemp


def getOpTimes(op, kind, prev_time):
    printEingerückt(op["operatorType"], 1)
    drivers = op["totalDrivers"]
    in_out_time = 0
    prev_out_time = 0
    wall_time = True
    if kind == 0:
        trans_time = transTimes(op["finishCpu"])
        prev_out_time = transTimes(op["addInputCpu"])
        in_out_time = transTimes(op["getOutputCpu"])
        add_time = transTimes(op["addInputCpu"]) + transTimes(op["finishCpu"])

        if wall_time:
            trans_time = transTimes(op["finishWall"])
            prev_out_time = transTimes(op["addInputWall"])
            in_out_time = transTimes(op["getOutputWall"])
            add_time = max((
                transTimes(op["addInputWall"])
                + transTimes(op["finishWall"])
                - transTimes(op["blockedWall"]) / drivers
            ), 0)
    elif kind == 1:
        trans_time = max(prev_time, transTimes(op["getOutputCpu"])) + transTimes(
            op["finishCpu"]
        )
        prev_out_time = transTimes(op["addInputCpu"])
        add_time = (
            transTimes(op["addInputCpu"])
            + transTimes(op["finishCpu"])
            + transTimes(op["getOutputCpu"])
        )
        if wall_time:
            add_time = max((
                transTimes(op["addInputWall"])
                + transTimes(op["finishWall"])
                + transTimes(op["getOutputWall"])
                - transTimes(op["blockedWall"]) / drivers
            ), 0)
            prev_out_time = transTimes(op["addInputWall"])
            trans_time = max(prev_time, transTimes(op["getOutputWall"])) + transTimes(
                op["finishWall"]
            )

    if kind == 2:
        trans_time = max(prev_time, transTimes(op["getOutputCpu"])) + transTimes(
            op["finishCpu"]
        )
        prev_out_time = 0
        in_out_time = transTimes(op["addInputCpu"])
        add_time = transTimes(op["getOutputCpu"]) + transTimes(op["finishCpu"])
        if wall_time:
            add_time = max((
                transTimes(op["getOutputWall"])
                + transTimes(op["finishWall"])
                - transTimes(op["blockedWall"]) / drivers
            ), 0)
            in_out_time = transTimes(op["addInputWall"])
            prev_out_time = 0
            trans_time = max(prev_time, transTimes(op["getOutputWall"])) + transTimes(
                op["finishWall"]
            )
    printEingerückt(
        "Spill Data: " + str(convertByteToGB(op["spilledDataSize"])) + " GB", 2
    )
    printEingerückt(
        "input: " + str(transTimes(op["addInputWall"])),
        2,
    )
    printEingerückt(
        "output: " + str(transTimes(op["getOutputWall"])),
        2,
    )
    printEingerückt(
        "finish: " + str(transTimes(op["finishWall"])),
        2,
    )
    printEingerückt("trans time: " + str(trans_time) + " add_time: " + str(add_time), 2)
    # if wall_time:
    #    return trans_time, add_time, in_out_time, prev_out_time
    return trans_time / 2, add_time / 2, in_out_time / 2, prev_out_time
    


def getTrinoAggStats(filename, tpc):
    if tpc == 13:
        sid = 2
        pipid = 4
        op_id = [1, 0, 3, 2]
    elif tpc == 17:
        sid = 1
        pipid = 6
        op_id = [1, 0, 6, 5]
    elif tpc == 20:
        sid = 2
        pipid = 6
        op_id = [1, 0, 6, 5]
    elif tpc == 4:
        sid = 2
        pipid = 3
        op_id = [1, 0, 2, 1]
    jf = open(os.path.join("tpc", filename))
    jf_data = json.load(jf)
    ops = jf_data["queryStats"]["operatorSummaries"]
    col_run = 0
    in_dur = 0
    out_dur = 0
    prev_out_time = 0
    add_times = []
    trans_times = []
    spill_size = 0
    print(filename + ":")
    for op in ops:
        if op["stageId"] == sid:
            if op["pipelineId"] == pipid:
                if op["operatorId"] == op_id[0]:
                    # Main Agg
                    trans_time, add_time, out_dur, prev_out_time = getOpTimes(
                        op, 2, prev_out_time
                    )
                    trans_times.append(trans_time)
                    add_times.append(add_time)
                    spill_size += convertByteToGB(op["spilledDataSize"])
                elif op["operatorId"] == op_id[1]:
                    # Exchange source
                    trans_time, add_time, in_out_time, prev_out_time = getOpTimes(
                        op, 1, prev_out_time
                    )
                    trans_times.append(trans_time)
                    add_times.append(add_time)
                    spill_size += convertByteToGB(op["spilledDataSize"])
            elif op["pipelineId"] == pipid - 1:
                if op["operatorId"] == op_id[2]:
                    # exhcangeSink
                    trans_time, add_time, in_out_time, prev_out_time = getOpTimes(
                        op, 1, prev_out_time
                    )
                    trans_times.append(trans_time)
                    add_times.append(add_time)
                    spill_size += convertByteToGB(op["spilledDataSize"])
                elif op["operatorId"] == op_id[3]:
                    # pre Agg
                    trans_time, add_time, in_dur, prev_out_time = getOpTimes(
                        op, 0, prev_out_time
                    )
                    trans_times.append(trans_time)
                    add_times.append(add_time)
                    spill_size += convertByteToGB(op["spilledDataSize"])

    # add_times = np.cumsum(np.array(add_times))
    # trans_times = np.cumsum(np.array(trans_times))
    return (
        np.array(add_times) * 60,
        np.array(trans_times) * 60,
        in_dur * 60,
        out_dur * 60,
        spill_size,
    )


def makeBarFig(
    data,
    xlabels,
    ylabel,
    show_bar_label: bool = True,
    xaxis_label: str = "",
    markings: bool = False,
    marking_labels: list = [],
    divide: int = 1,
    titles: list = [],
):
    # plt.tight_layout()
    fig, axs = plt.subplots(1, divide, sharex=True, sharey=True)
    width = 0.18
    space = 0.05
    big_space = 0.5
    x = np.arange(len(xlabels))
    x = x * big_space
    colors = plt.cm.viridis.colors
    nth = int(len(colors) / len(list(data[0].keys())))
    colors = colors[nth - 1 :: nth]
    i_bottom = 0
    for ax_counter in range(divide):
        if divide > 1:
            ax = axs[ax_counter]
        else:
            ax = axs
        if ax_counter > 0:
            ax.yaxis.set_ticks_position("left")
        for i in range(int(len(data) / divide)):
            bottom = np.zeros(len(xlabels))
            counter = 0
            for label, datum in data[i + ax_counter].items():
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
        i_bottom = int(len(data) / divide)
        ax.legend(list(data[0].keys()), loc="upper right")
        ax.set_xticks(x + (space + width) * (int(len(data) / divide) - 1) * 0.5)
        ax.set_xticklabels(xlabels)
        if len(titles) > 0:
            ax.set_title(titles[ax_counter])
        if ax_counter == 0:
            ax.set_ylabel(ylabel)
        ax.set_xlabel(xaxis_label)
        ax.grid(visible=True, linestyle="dashed")
        ax.set_axisbelow(True)
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

    # if divide != -1:
    #     num = int(len(x) / divide)
    #     counter = 1
    #     xdiff = (x[1] - x[0]) / 2
    #     for xth in x:
    #         if counter % num == 0:
    #             xth = xth + (space + width) * (len(data) - 1) * 0.5
    #             if counter != len(x):
    #                 plt.axvline(
    #                     x=xth + xdiff, linestyle="--", color="black", linewidth=3
    #                 )
    #             plt.text(
    #                 xth - (xdiff) * (num - 1),
    #                 max(bottom) * 1.2,
    #                 marking_labels[int(counter / num) - 1],
    #                 ha="center",
    #                 # bbox=dict(facecolor="white", edgecolor="none", alpha=1.0),
    #             )
    #         counter += 1


def makeScatterFig(xdata, ydata, labels, xlabel, ylabel):
    fig, ax = plt.subplots()
    ax.scatter(xdata, ydata, marker="o", s=200)
    # for i, txt in enumerate(labels):
    #     ax.annotate(txt, (xdata[i], ydata[i]))
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(visible=True, linestyle="dashed")
    # plt.set_axisbelow(True)


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
    join_input_size = []
    hash_input_size = []
    agg_input_size = []
    join_spill_size = []
    hash_spill_size = []
    agg_spill_size = []
    join_length = []
    hash_length = []
    agg_length = []
    memcounter = 0
    tabs = 0
    for k in range(1, 23):
        if k == 7:
            continue
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
                if spill > 0:
                    hash_input_size.append(convertByteToGB(i["inputDataSize"]))
                    hash_spill_size.append(spill)
                    hash_length.append(cputime)
            elif i["operatorType"] == "LookupJoinOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                join_spills += spill
                join_time += wtime
                join_time_cpu += cputime
                all_join_spills.append(spill)
                all_join_times.append(wtime)
                all_join_times_cpu.append(cputime)
                join_counter += 1
                if spill > 0:
                    join_input_size.append(convertByteToGB(i["inputDataSize"]))
                    join_spill_size.append(spill)
                    join_length.append(cputime)
            elif i["operatorType"] == "HashAggregationOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                agg_spills += spill
                agg_time += wtime
                agg_time_cpu += cputime
                all_agg_spills.append(spill)
                all_agg_times.append(wtime)
                all_agg_times_cpu.append(cputime)
                agg_counter += 1
                if spill > 0:  # and convertByteToGB(i["inputDataSize"]) < 100:
                    agg_input_size.append(convertByteToGB(i["inputDataSize"]))
                    agg_spill_size.append(spill)
                    agg_length.append(cputime)
            elif i["operatorType"] == "AggregationOperator":
                wtime, cputime, spill = CollectOPData(i, tabs + 1)
                agg_spills += spill
                agg_time += wtime
                agg_time_cpu += cputime
                all_agg_spills.append(spill)
                all_agg_times.append(wtime)
                all_agg_times_cpu.append(cputime)
                agg_counter += 1
                if spill > 0:  # and convertByteToGB(i["inputDataSize"]) < 100:
                    agg_input_size.append(convertByteToGB(i["inputDataSize"]))
                    agg_spill_size.append(spill)
                    agg_length.append(cputime)
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
    plt.rcParams.update({"font.size": 35})
    makeBarFig(
        spills,
        np.array(query_names)[filter],
        "Spilled Data in GB",
        False,
        xaxis_label="TPC Query",
    )
    makeScatterFig(
        query_times,
        spilledData,
        query_names,
        "Execution time in min",
        "Data spilled in GB",
    )

    plt.figure(5)
    plt.rcParams.update({"font.size": 35})

    plt.scatter(join_length, join_spill_size, marker="o", s=200, label="Join")
    plt.scatter(agg_length, agg_spill_size, marker="X", s=200, label="Aggregator")
    plt.scatter(hash_length, hash_spill_size, marker="P", s=200, label="Hash")
    plt.xlabel("CPU-Time in s")
    plt.ylabel("Spill size in GiB")
    plt.legend()
    plt.grid(visible=True, linestyle="dashed")

    plt.figure(3)

    plt.scatter(join_input_size, join_spill_size, marker="o", s=200, label="Join")
    plt.scatter(agg_input_size, agg_spill_size, marker="X", s=200, label="Aggregator")
    plt.scatter(hash_input_size, hash_spill_size, marker="P", s=200, label="Hash")
    plt.xlabel("Input size in GiB")
    plt.ylabel("Spill size in GiB")
    plt.legend()
    plt.grid(visible=True, linestyle="dashed")
    # plt.set_axisbelow(True)

    total_difference = 0
    for a, b in zip(join_input_size, join_spill_size):
        if b / a < 3:
            total_difference += b / a
    printEingerückt(
        "Join spill proportion:" + str(total_difference / len(join_input_size)), 1
    )
    total_difference = 0
    for a, b in zip(agg_input_size, agg_spill_size):
        if b / a < 3:
            total_difference += b / a
    printEingerückt(
        "Aggregator spill proportion:" + str(total_difference / len(agg_input_size)), 1
    )
    total_difference = 0
    for a, b in zip(hash_input_size, hash_spill_size):
        if b / a < 3:
            total_difference += b / a
    printEingerückt(
        "Hash spill proportion:" + str(total_difference / len(hash_input_size)), 1
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
            "Join": query_times_join,
            "Aggregation": query_times_agg,
            # "Exchange": query_times_ex,
            "Hash": query_times_hash,
            # "Other": query_all_time,
        }
    ]
    makeBarFig(
        times,
        query_names,
        "Wall time in min",
        show_bar_label=False,
        xaxis_label="TPC Query",
    )

    times = [{"Time": query_times}]
    makeBarFig(
        times,
        query_names,
        "Wall time in min",
        show_bar_label=False,
        xaxis_label="TPC Query",
    )

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
    plt.rcParams.update({"font.size": 35})
    tabs = 0
    query_ids = [4, 13, 17, 20]  # [4, 17, 20]  # [1, 6, 13]
    data_scale = [1000]  # [300, 1000]  # [100, 300, 1000]
    worker_size = [
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        15,
        20,
        50,
        60,
    ]  # [4, 45, 5, 6, 10]  # [8, 9, 10]  # [4, 5, 6]
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

    agg_times = np.zeros((len(query_ids), len(worker_size)), float)
    agg_spill = np.zeros((len(query_ids), len(worker_size)), float)

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

                add_t, tans_t, in_t, out_t, spill_t = getTrinoAggStats(
                    name, query_ids[qid]
                )
                agg_times[qid][worker] = (np.sum(tans_t) + in_t + out_t) / 60
                agg_spill[qid][worker] = spill_t

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
    plt.rcParams.update({"font.size": 35})
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

    plt.figure(1)
    labels = ["4", "13", "17", "20"]
    for x in range(len(spilledData)):
        x_data = spilledData[x].flatten()
        x_ind = np.argsort(x_data)
        y_data = query_times[x].flatten()
        cond = y_data[x_ind] > 0
        plt.plot(x_data[x_ind][cond], y_data[x_ind][cond], linewidth=3, label=labels[x])
    plt.legend()
    plt.xlabel("Spilled Data in GiB")
    plt.ylabel("Runtime in min")
    plt.grid(visible=True, linestyle="dashed")

    plt.figure(3)
    labels = ["4", "13", "17", "20"]
    colors = plt.cm.viridis.colors
    nth = int(len(colors) / len(spilledData))
    colors = colors[nth - 1 :: nth]
    for x in range(len(spilledData)):
        y_data = spilledData[x].flatten()
        y_data2 = agg_spill[x]
        x_data = worker_size
        cond = query_times[x].flatten() > 0
        plt.plot(
            np.array(x_data)[cond],
            np.array(y_data)[cond],
            linewidth=3,
            label=labels[x],
            color=colors[x],
        )
        plt.plot(
            np.array(x_data)[cond],
            np.array(y_data2)[cond],
            linewidth=3,
            label=labels[x],
            color=colors[x],
            linestyle="dashed",
        )
        line_length = 0.5
        plt.plot(
            [np.array(x_data)[cond][0], np.array(x_data)[cond][0]],
            [
                np.array(y_data)[cond][0] + line_length,
                np.array(y_data)[cond][0] - line_length,
            ],
            color=colors[x],
            lw=3,
            zorder=10,
        )
        plt.text(
            np.array(x_data)[cond][0] - 0.02,
            np.array(y_data)[cond][0],
            "⚡",
            ha="right",
            va="center",
            fontsize=35,
            zorder=11,
            color="red",
        )
    plt.legend()
    # plt.xlim(0, 30)
    plt.xlabel("Worker Main Memory in GiB")
    plt.ylabel("Spilled Data in GiB")
    plt.grid(visible=True, linestyle="dashed")

    plt.figure(4)
    leg_handles = [
        mlines.Line2D(
            [], [], color="black", label="Aggregation", linewidth=3, linestyle="dashed"
        ),
        mlines.Line2D([], [], color="black", label="Query", linewidth=3),
    ]
    labels = ["4", "13", "17", "20"]
    for x in range(len(spilledData)):
        x_data = worker_size
        y_data = query_times[x].flatten()
        y_data2 = agg_times[x]
        cond = y_data > 0
        plt.plot(
            np.array(x_data)[cond],
            np.array(y_data)[cond],
            linewidth=3,
            label=labels[x],
            color=colors[x],
        )
        leg_handles.append(
            mlines.Line2D([], [], color=colors[x], label=labels[x], linewidth=3)
        )
        plt.plot(
            np.array(x_data)[cond],
            np.array(y_data2)[cond],
            linewidth=3,
            label=labels[x],
            color=colors[x],
            linestyle="dashed",
        )
        line_length = 0.5
        plt.plot(
            [np.array(x_data)[cond][0], np.array(x_data)[cond][0]],
            [
                np.array(y_data)[cond][0] + line_length,
                np.array(y_data)[cond][0] - line_length,
            ],
            color=colors[x],
            lw=3,
            zorder=10,
        )
        plt.text(
            np.array(x_data)[cond][0] - 0.02,
            np.array(y_data)[cond][0],
            "⚡",
            ha="right",
            va="center",
            fontsize=35,
            zorder=11,
            color="red",
        )
    plt.legend(handles=leg_handles)
    # plt.xlim(0, 25)
    plt.xlabel("Main Memory in GiB")
    plt.ylabel("Runtime in min")
    plt.grid(visible=True, linestyle="dashed")

    plt.figure(5)
    for x in range(len(spilledData)):
        cond = query_times[x].flatten() > 0
        x_data = agg_spill[x][cond]
        if x == 0:
            y_data = agg_times[x][cond] - query_times[x].flatten()[cond][-1]
        else:
            y_data = agg_times[x][cond] - agg_times[x][cond][-1]
        if x == 2:
            x_data = np.sort(x_data)[::-1]
        y_data = y_data / x_data
        plt.plot(
            x_data,
            y_data,
            linewidth=3,
            label=labels[x],
            color=colors[x],
        )
    plt.legend()
    # plt.xlim(0, 25)
    plt.xlabel("Spilled Data in GiB")
    plt.ylabel("Execution time per Spille Data in min/GiB")
    plt.grid(visible=True, linestyle="dashed")
    # fig, ax = plt.subplots()
    # width = 0.1
    # space = 0.05
    # colors = plt.cm.viridis.colors
    # nth = int(len(colors) / len(list(data[0].keys())))
    # colors = colors[nth - 1 :: nth]
    # for i in range(len(data)):
    #     bottom = np.zeros(len(x_positions))
    #     counter = 0
    #     for label, datum in data[i].items():
    #         rects = ax.bar(
    #             x_positions,
    #             datum,
    #             width,
    #             bottom=bottom,
    #             color=colors[counter],
    #             label=label,
    #         )
    #         bottom += datum
    #         counter += 1
    #         ax.bar_label(rects, padding=2, fontsize=20)

    # ax.set_xlabel("Spilled data in GB", fontsize=20)
    # ax.legend(list(data[0].keys()), loc="upper right", fontsize=20)
    # ax.set_ylabel("Wall time in min", fontsize=20)
    # ax.grid(visible=True, linestyle="dashed")
    # ax.set_axisbelow(True)
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


def printhelperStats(jf_data):
    printEingerückt(
        "Number of written files: " + str(len(jf_data["writeCall_s3_file_dur"])),
        1,
    )
    printEingerückt(
        "Sum of size of written files: "
        + str(sum(jf_data["writeCall_s3_file_size"]) / 2**20),
        1,
    )
    printEingerückt(
        "Number of written mana files: " + str(len(jf_data["get_mana_dur"])), 1
    )

    printEingerückt(
        "merge speed: "
        + str(jf_data["linesRead"] / jf_data["mergeHelp_merging_Duration"]),
        1,
    )


def c_size_by_time():
    plt.rcParams.update({"font.size": 35})
    tpc_4_shuffled = False
    trino = False
    only_trino = -1
    divide = 1
    helpers = {}
    titles = []
    trino_labels = []
    runtime_x = []
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

    # part only 8 merge Threads
    # names = [
    #     "logfile_4_6_0_4_17-25.json",
    #     "logfile_4_6_0_4_17-47.json",
    #     "logfile_4_6_0_4_18-06.json",
    #     "logfile_4_6_0_4_18-25.json",
    #     "logfile_4_6_0_4_18-49.json",
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

    # part only 4  merge Threads
    # names = [
    #     "logfile_4_6_0_4_15-38.json",
    #     "logfile_4_6_0_4_15-56.json",
    #     "logfile_4_6_0_4_16-13.json",
    #     "logfile_4_6_0_4_16-30.json",
    #     "logfile_4_6_0_4_16-49.json",
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

    # #4,6,8 threads
    # names = [
    #     # "logfile_4_6_0_4_20-58.json",  # 2
    #     "logfile_4_6_0_4_10-48.json",  # 2
    #     "logfile_4_6_0_4_19-22.json",  # 3
    #     "logfile_4_6_0_4_15-38.json",  # 4
    #     "logfile_4_6_0_4_16-40.json",  # 5
    #     "logfile_4_6_0_4_13-41.json",  # 6
    #     "logfile_4_6_0_4_17-25.json",  # 8
    #     # "logfile_4_6_0_4_21-18.json",  # 2
    #     "logfile_4_6_0_4_11-06.json",  # 2
    #     "logfile_4_6_0_4_19-40.json",  # 3
    #     "logfile_4_6_0_4_15-56.json",  # 4
    #     "logfile_4_6_0_4_17-02.json",  # 5
    #     "logfile_4_6_0_4_09-02.json",  # 6
    #     "logfile_4_6_0_4_17-47.json",  # 8
    #     # "logfile_4_6_0_4_21-36.json",  # 2
    #     "logfile_4_6_0_4_11-25.json",  # 2
    #     "logfile_4_6_0_4_19-57.json",  # 3
    #     "logfile_4_6_0_4_16-13.json",  # 4
    #     "logfile_4_6_0_4_17-19.json",  # 5
    #     "logfile_4_6_0_4_09-19.json",  # 6
    #     "logfile_4_6_0_4_18-06.json",  # 8
    #     # "logfile_4_6_0_4_21-57.json",  # 2
    #     "logfile_4_6_0_4_11-45.json",  # 2
    #     "logfile_4_6_0_4_20-16.json",  # 3
    #     "logfile_4_6_0_4_16-30.json",  # 4
    #     "logfile_4_6_0_4_17-40.json",  # 5
    #     "logfile_4_6_0_4_09-37.json",  # 6
    #     "logfile_4_6_0_4_18-25.json",  # 8
    #     # "logfile_4_6_0_4_22-21.json",  # 2
    #     "logfile_4_6_0_4_12-08.json",  # 2
    #     "logfile_4_6_0_4_20-37.json",  # 3
    #     "logfile_4_6_0_4_16-49.json",  # 4
    #     "logfile_4_6_0_4_18-07.json",  # 4
    #     "logfile_4_6_0_4_09-57.json",  # 6
    #     "logfile_4_6_0_4_18-49.json",  # 8
    # ]
    # labels = np.array(
    #     [
    #         "10",
    #         "10",
    #         "10",
    #         "10",
    #         "10",
    #         "10",
    #         "25",
    #         "25",
    #         "25",
    #         "25",
    #         "25",
    #         "25",
    #         "50",
    #         "50",
    #         "50",
    #         "50",
    #         "50",
    #         "50",
    #         "75",
    #         "75",
    #         "75",
    #         "75",
    #         "75",
    #         "75",
    #         "100",
    #         "100",
    #         "100",
    #         "100",
    #         "100",
    #         "100",
    #     ]
    # )
    # runtimes = {
    #     "2": np.zeros(5),
    #     "3": np.zeros(5),
    #     "4": np.zeros(5),
    #     "5": np.zeros(5),
    #     "6": np.zeros(5),
    #     "8": np.zeros(5),
    # }
    # runtime_keys = ["2","3", "4", "5","6", "8"]
    # runtime_x = [10, 25, 50, 75, 100]
    # tpc_4_shuffled = True
    # subplot = 0
    # subruntimes = {
    #     #   "local": np.zeros(5),
    #     # "local + S3": np.zeros(5),
    #     #   "S3": np.zeros(5),
    #     "Write time of spill files": np.zeros(5),
    #     "Scan duration": np.zeros(5),
    #     "Merge duration": np.zeros(5),
    # }

    # deencode analyses
    # names = [
    #     "logfile_4_6_0_4_12-50.json",
    #     "logfile_4_6_0_4_12-22.json",
    #     # "logfile_4_6_100_4_12-12.json",
    #     # "logfile_4_6_100_4_19-32.json",
    # ]
    # labels = np.array(
    #     [
    #         "compression",
    #         "no compression",
    #         # "compression",
    #         # "no compression",
    #     ]
    # )
    # marking_labels = ["S3", "Local"]
    # divide = 2
    # titles = ["S3", "Local"]

    # with/out part
    # names = [
    #     "logfile_4_6_0_8_17-14.json",
    #     "logfile_4_6_0_8_14-43.json",
    #     "logfile_4_40_0_10_18-01.json",
    #     "logfile_4_40_0_10_17-40.json",
    # ]
    # labels = np.array(
    #     [
    #         "1",
    #         "30",
    #         # "30 partitions 2W",
    #         #   "compression 100BM",
    #         # "no compression 100BM",
    #     ]
    # )
    # divide = 2
    # titles = ["6GiB", "40GiB"]

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
    #     "logfile_4_6_0_10_13-36.json",
    #     "logfile_4_6_0_10_14-17.json",
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
    #         "3 worker; no split",
    #         "3 worker; split",
    #     ]
    # )
    # helpers = {
    #     "no split": "logfile_4_6_0_4_14-39.json",
    #     "split": "logfile_4_6_0_4_14-00.json",
    #     "3W (1), no split": "logfile_4_4_0_4_13-36.json",
    #     "3W (2), no split": "logfile_4_6_0_4_12-37.json",
    #     "3W (1), split": "logfile_4_4_0_4_14-17.json",
    #     "3W (2), split": "logfile_4_6_0_4_13-17.json",
    # }

    # # split_mana shuffled local + size
    # names = [
    #     "logfile_4_6_0_10_21-11.json",
    #     # "logfile_4_6_0_10_16-17.json",
    #     "logfile_4_6_0_10_20-24.json",
    #     # "logfile_4_6_0_10_16-21.json",
    #     # "logfile_4_6_0_10_20-58.json",  # worker logfile: logfile_4_6_0_4_20-58.json
    #     "logfile_4_6_0_10_22-34.json",  # worker logfile: logfile_4_6_0_4_14-39.json
    #     # "logfile_4_6_0_10_15-17.json",  # worker logfile: logfile_4_6_0_4_15-17.json
    #     "logfile_4_6_0_10_22-04.json",  # worker logfile: logfile_4_6_0_4_12-27.json
    #     # "logfile_4_6_0_10_15-40.json",
    #     "logfile_4_6_0_10_21-32.json",
    #     "logfile_4_6_0_10_15-12.json",
    # ]
    # labels = np.array(
    #     [
    #         # "no split",
    #         # "split",
    #         "1 worker; no split",
    #         "1 worker; split",
    #         "2 worker; no split",
    #         "2 worker; split",
    #         "3 worker; no split",
    #         "3 worker; split",
    #     ]
    # )
    # labels = np.array(
    #     [
    #         "1 Worker",
    #         "2 Worker",
    #         "3 Worker",
    #     ]
    # )
    # helpers = {
    #     "2 Worker ": "logfile_4_6_0_4_22-34.json",
    #     "2 Worker": "logfile_4_6_0_4_22-04.json",
    #     "3 Worker ": ["logfile_4_6_0_4_15-40.json", "logfile_4_6_0_4_14-40.json"],
    #     "3 Worker": ["logfile_4_6_0_4_14-12.json", "logfile_4_6_0_4_15-12.json"],
    # }
    # titles = ["1 Worker", "2 Worker", "3 Worker"]
    # divide = 3

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
    # names = [
    #     # "logfile_4_6_0_6_11-47.json",
    #     # "logfile_4_6_0_6_12-11.json",
    #     # "logfile_4_6_0_6_14-41.json",
    #     # "logfile_4_6_0_8_20-47.json",
    #     # "logfile_4_6_0_8_17-38.json",
    #     # "logfile_4_6_0_8_01-22.json",
    #         "logfile_4_6_0_8_12-03.json",
    #     "logfile_4_6_0_8_17-38.json",
    #     "logfile_4_6_0_8_20-47.json",

    # ]
    # labels = np.array(
    #     [
    #         "local",
    #         "local + S3",
    #         "S3",
    #     ]
    # )

    # local vs local + s3 vs. s3 shuffled
    # names = [
    #     "logfile_4_6_0_10_21-06.json",
    #            "logfile_4_6_0_10_12-14.json",
    #         "logfile_4_6_0_10_22-06.json",
    # ]
    # labels = np.array(
    #     [
    #         "local",
    #         "local + S3",
    #         "S3",
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
    #     # "logfile_4_6_0_8_12-03.json",
    #     # "logfile_4_6_0_8_17-38.json",
    #     # "logfile_4_6_0_8_20-47.json",
    #     "logfile_4_6_0_10_21-06.json",
    #     "logfile_4_6_0_10_12-14.json",
    #     "logfile_4_15_0_10_09-25.json",
    #     "logfile_4_6_0_10_22-06.json",
    #     # "logfile_4_6_0_10_09-04.json",
    #     "logfile_4_6_0_10_16-49.json",
    #     # "logfile_4_6_0_10_12-50.json",
    # ]
    # labels = np.array(
    #     [
    #         "local",
    #         "local + S3",
    #         "local + S3(2)",
    #         "S3",
    #         "2 Worker",
    #         # "3 Worker",
    #     ]
    # )

    names = [
        "logfile_4_15_0_10_11-17.json",
        "logfile_4_20_0_10_11-35.json",
        "logfile_4_30_0_10_11-53.json",
        "logfile_4_40_0_10_12-11.json",
    ]
    labels = np.array(["15", "20", "30", "40"])

    runtimes = {
        "local + S3": np.zeros(4),
    }
    runtime_keys = ["local + S3"]  # , "3 Worker"]
    runtime_x = [15, 20, 30, 40]
    tpc_4_shuffled = True
    subplot = 0
    subruntimes = {
        "Write time of spill files": np.zeros(4),
        "Scan duration": np.zeros(4),
        "Merge duration": np.zeros(4),
    }

    # # # merge helpe 4 shuffled

    # names = [
    #     "logfile_4_4_0_10_11-42.json",  # local
    #     "logfile_4_4_0_10_18-53.json",  # s3 + local
    #     "logfile_4_4_0_10_13-21.json",  # s3
    #     "logfile_4_4_0_10_14-24.json",  # 1 worker
    #     # "logfile_4_4_0_10_09-57.json",  # 2 worker
    #     "logfile_4_6_0_10_21-06.json",  # local
    #     "logfile_4_6_0_10_12-14.json",  # s3 + local
    #     "logfile_4_6_0_10_22-06.json",  # s3
    #     "logfile_4_6_0_10_09-04.json",  # 1 w
    #     # "logfile_4_6_0_10_12-50.json",  # 2 w
    #     "logfile_4_10_0_10_23-14.json",  # l
    #     "logfile_4_10_0_10_23-33.json",  # sl
    #     "logfile_4_10_0_10_00-10.json",  # s
    #     "logfile_4_10_0_10_09-14.json",  # 1w
    #     # "logfile_4_10_0_10_13-21.json",  # 2w
    #     "logfile_4_15_0_10_07-57.json",  # l
    #     "logfile_4_15_0_10_08-13.json",  # sl
    #     "logfile_4_15_0_10_08-36.json",  # s
    #     "logfile_4_15_0_10_20-28.json",  # 1w
    #     # "logfile_4_15_0_10_20-56.json",  # 2w
    #     "logfile_4_20_0_10_10-32.json",  # l
    #     "logfile_4_20_0_10_10-45.json",  # sl
    #     "logfile_4_20_0_10_11-03.json",  # s
    #     "logfile_4_20_0_10_11-23.json",  # 1w
    #     # "logfile_4_20_0_10_21-43.json",  # 2w
    # ]
    # labels = np.array(
    #     [
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker 1",
    #         # "3 Worker",
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker 2",
    #         # "3 Worker",
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker 3",
    #         # "3 Worker",
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker 4",
    #         # "3 Worker",
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker 5",
    #         # "3 Worker",
    #     ]
    # )
    # runtimes = {
    #     "local": np.zeros(5),
    #     "local + S3": np.zeros(5),
    #     "S3": np.zeros(5),
    #     # "1 Worker": np.zeros(5),
    #     "2 Worker": np.zeros(5),
    #     # "3 Worker": np.zeros(5),
    # }
    # runtime_keys = ["local", "local + S3", "S3", "2 Worker"]#, "3 Worker"]
    # runtime_x = [4, 6, 10, 15, 20]
    # tpc_4_shuffled = True
    # helpers = {
    #     "2 Worker": "logfile_4_6_0_4_22-04.json",
    #     "3 Worker": [
    #         "logfile_4_6_0_4_13-50.json",
    #         "logfile_4_6_0_4_13-50(2).json",
    #     ],
    # }
    # subplot = 2
    # subruntimes = {
    #     #   "local": np.zeros(5),
    #     # "local + S3": np.zeros(5),
    #     #   "S3": np.zeros(5),
    #     "Write time of spill files": np.zeros(5),
    #     "Scan duration": np.zeros(5),
    #     "Merge duration": np.zeros(5),
    # }

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

    # merge help 17
    # names = [
    #     "logfile_17_20_0_10_10-33.json",
    #     "logfile_17_20_0_10_15-33.json",
    #     "logfile_17_20_0_10_16-42.json",
    # ]
    # labels = np.array(
    #     [
    #         # "local",
    #         "S3 + local",
    #         # "S3",
    #         "2 Worker",
    #         "2 Worker 2",
    #     ]
    # )

    # tpc 17
    # names = [
    #  #   "logfile_17_12.5_0_10_09-11.json",  # local
    #     "logfile_17_12.5_0_10_22-43.json",  # S3 + local
    #  #   "logfile_17_16_0_10_10-15.json",  # local
    #     "logfile_17_16_0_10_23-50.json",  # S3 + local
    #      "logfile_17_20_0_10_10-33.json", # S3 + local
    # #    "logfile_17_22_0_10_11-25.json",  # local
    #     "logfile_17_22_0_10_00-59.json",  # S3 + local
    # ]
    # labels = np.array(
    #     [
    #         "12.5",
    #       #  "12.5",
    #         "16",
    #         #"16",
    #         "20",
    #         # "20",
    #         "22",
    #        # "22",
    #     ]
    # )
    # runtimes = {
    #   #  "local": np.zeros(3),
    #     "local + S3": np.zeros(4),
    # }
    # runtime_keys = ["local + S3"]
    # runtime_x = [12.5, 16,20, 22]
    # tpc_4_shuffled = True
    # helpers = {}
    # subplot = 0
    # subruntimes = {
    #     #   "local": np.zeros(5),
    #     # "local + S3": np.zeros(5),
    #     #   "S3": np.zeros(5),
    #     "Write time of spill files": np.zeros(4),
    #     "Scan duration": np.zeros(4),
    #     "Merge duration": np.zeros(4),
    # }

    # merge help 13 shuffled
    # names = [
    #     # "logfile_13_4_0_10_12-17.json",
    #     # "logfile_13_4_0_10_12-20.json",
    #     # "logfile_13_4_0_10_12-24.json",
    #     "logfile_13_6_0_10_15-59.json",
    #     "logfile_13_6_0_10_16-05.json",
    #     "logfile_13_6_0_10_16-15.json",
    #     "logfile_13_6_0_10_17-37.json",
    # ]
    # labels = np.array(
    #     [
    #         "local",
    #         "S3 + local",
    #         "S3",
    #         "2 Worker",
    #     ]
    # )

    # tpc 13 all shuffled
    # names = [
    #     "logfile_13_6_0_10_16-05.json",
    #     "logfile_13_10_0_10_16-56.json",
    #     "logfile_13_15_0_10_17-05.json",
    #     "logfile_13_20_0_10_17-14.json",
    #     "logfile_13_25_0_10_16-47.json",
    # ]
    # labels = np.array(["6", "10", "15", "20", "25"])

    # runtimes = {
    #     "local + S3": np.zeros(5),
    # }
    # runtime_keys = ["local + S3"]
    # runtime_x = [6, 10, 15, 20, 25]
    # tpc_4_shuffled = True

    # subplot = 0
    # subruntimes = {
    #     #   "local": np.zeros(5),
    #     # "local + S3": np.zeros(5),
    #     #   "S3": np.zeros(5),
    #     "Write time of spill files": np.zeros(5),
    #     "Scan duration": np.zeros(5),
    #     "Merge duration": np.zeros(5),
    # }

    # Trino 20 4.2 spill: 16.32 GiB
    # t_scan_dur = 241
    # t_query_dur = 1080
    # trino = True

    # Trino 13
    # trino_names = [
    #     "tpc_13_4_1000.json",
    #     "tpc_13_5_1000.json",
    #     "tpc_13_6_1000.json",
    #     "tpc_13_7_1000.json",
    #     "tpc_13_10_1000.json",
    # ]
    # trino_labels = np.array(
    #     [
    #         "4",
    #         "5",
    #         "6",
    #         "7",
    #         "10",
    #     ]
    # )
    # only_trino = 13
    # trino_x = [8, 10, 12, 14, 20]

    # Trino 17
    # trino_names = [
    #     "tpc_17_8_1000.json",
    #     "tpc_17_9_1000.json",
    #     "tpc_17_10_1000.json",
    #     "tpc_17_11_1000(2).json",
    # ]
    # trino_labels = np.array(
    #     [
    #         "8",
    #         "9",
    #         "10",
    #         "11",
    #     ]
    # )
    # only_trino = 17
    # trino_x = [16, 18, 20, 22]

    # Trino 20
    # trino_names = [
    #     "tpc_20_8_1000.json",
    #     "tpc_20_9_1000.json",
    #     "tpc_20_10_1000.json",
    #     "tpc_20_11_1000.json",
    # ]
    # trino_labels = np.array(
    #     [
    #         "8",
    #         "9",
    #         "10",
    #         "11",
    #     ]
    # )
    # only_trino = 20
    # trino_x = [8, 9, 10, 11]

    # Trino 4
    trino_names = [
        "tpc_4_9_1000.json",
        "tpc_4_10_1000.json",
        "tpc_4_15_1000.json",
        "tpc_4_20_1000.json",
    ]
    trino_labels = np.array(["9", "10", "15", "20"])
    trino_x = [18, 20, 30, 40]
    only_trino = 4

    linestyles = ["dashed", "solid"]
    counter = 0
    leg_handles = [
        mlines.Line2D([], [], color="black", label="Split", linewidth=3),
        mlines.Line2D(
            [], [], color="black", label="No Split", linewidth=3, linestyle="dashed"
        ),
    ]
    colors = plt.cm.get_cmap("Dark2").colors
    for label, helper in helpers.items():
        try:
            jf = open(os.path.join("c++_logs", helper))
            jf_data = json.load(jf)
            print(helper + ":")
            printhelperStats(jf_data)
            try:
                printEingerückt(
                    "Number of merge mana files: "
                    + str(sum(jf_data["mergeFiles_num"])),
                    1,
                )
                printEingerückt(
                    "exec Time: " + str(jf_data["mergeFiles_time"][-1] / 1000000), 1
                )
                merge_file_num = np.cumsum(np.array(jf_data["mergeFiles_num"]))
                merge_file_times = np.array(jf_data["mergeFiles_time"]) / 1000000
                plt.figure(9)
                plt.plot(
                    merge_file_times,
                    merge_file_num,
                    label=label,
                    linewidth=3,
                    linestyle=linestyles[counter % 2],
                    color=colors[int(counter / 2)],
                )

                printEingerückt(
                    "merged files per second: "
                    + str(merge_file_num[-1] / merge_file_times[-1]),
                    1,
                )
            except:
                continue
        except:
            for h in helper:
                jf = open(os.path.join("c++_logs", h))
                jf_data = json.load(jf)
                print(h + ":")
                printhelperStats(jf_data)
            combine_y, combine_x = combinemergeFiles(helper[0], helper[1])
            plt.figure(9)
            plt.plot(
                combine_x,
                combine_y,
                label=label,
                linewidth=3,
                linestyle=linestyles[counter % 2],
                color=colors[int(counter / 2)],
            )
            printEingerückt(
                "merged files per second: "
                + str(merge_file_num[-1] / merge_file_times[-1]),
                1,
            )
        if counter % 2:
            leg_handles.append(
                mlines.Line2D(
                    [], [], color=colors[int(counter / 2)], label=label, linewidth=3
                )
            )
        plt.legend(handles=leg_handles)
        plt.xlabel("Time in s")
        plt.ylabel("Number of files merged")
        counter += 1

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

    prot_spill_size = np.empty(len(runtime_x))
    trino_spill_size = np.empty(len(trino_labels))

    try:
        directory = "c++_logs"
        f = open(os.path.join(directory, "times_4_15_0_10_09-25.csv"))
        jf = open(os.path.join(directory, "logfile_4_15_0_10_09-25.json"))
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
            "Write time of spill files": np.empty(int(arr_len / divide)),
            "Scan duration": np.empty(int(arr_len / divide)),
            #  "Spill rest duration":np.empty(len(names)),
            # "merge_hash_dur": np.empty(),
            # "get_file_sum": np.empty(),
            # "Write time of the output": np.empty(len(names)),
            "Merge duration": np.empty(int(arr_len / divide)),
            # "read_tuple_sum": np.empty(),
            # "Exchange": np.empty(),
        }
    ]
    for i in range(divide - 1):
        times.append(
            {
                "Write time of spill files": np.empty(int(arr_len / divide)),
                "Scan duration": np.empty(int(arr_len / divide)),
                #  "Spill rest duration":np.empty(len(names)),
                # "merge_hash_dur": np.empty(),
                # "get_file_sum": np.empty(),
                #  "Write time of the output": np.empty(len(names)),
                "Merge duration": np.empty(int(arr_len / divide)),
                # "read_tuple_sum": np.empty(),
                # "Exchange": np.empty(),
            }
        )
    if trino:
        times[0]["Write time of spill files"][0] = 0
        times[0]["Scan duration"][0] = t_scan_dur
        times[0]["Merge duration"][0] = t_query_dur - t_scan_dur
        counter = 1
        labels = np.insert(labels, 0, "Trino")

    if only_trino != -1:
        counter = 0
        trino_times = [
            {
                "Write time of spill files": np.empty(len(trino_labels)),
                "Scan duration": np.empty(len(trino_labels)),
                #  "Spill rest duration":np.empty(len(names)),
                # "merge_hash_dur": np.empty(),
                # "get_file_sum": np.empty(),
                "Write time of the output": np.empty(len(trino_labels)),
                "Merge duration": np.empty(len(trino_labels)),
                # "read_tuple_sum": np.empty(),
                # "Exchange": np.empty(),
            }
        ]
        trino_runtimes_add = {
            "Trino input": np.zeros(len(trino_names)),
            "pre Agg": np.zeros(len(trino_names)),
            "ex sink": np.zeros(len(trino_names)),
            "ex source": np.zeros(len(trino_names)),
            "main Agg": np.zeros(len(trino_names)),
            # "Trino Aggregation": np.zeros(len(trino_names)),
            "Trino output": np.zeros(len(trino_names)),
        }
        trino_runtimes_trans = {
            "Trino input": np.zeros(len(trino_names)),
            "pre Agg": np.zeros(len(trino_names)),
            "ex sink": np.zeros(len(trino_names)),
            "ex source": np.zeros(len(trino_names)),
            "main Agg": np.zeros(len(trino_names)),
            # "Trino Aggregation": np.zeros(len(trino_names)),
            "Trino output": np.zeros(len(trino_names)),
        }

        downy = np.zeros(len(trino_names))
        for name in trino_names:
            add_times, trans_times, in_dur, out_dur, spill_size = getTrinoAggStats(
                name, only_trino
            )
            trino_times[0]["Write time of spill files"][counter] = 0
            trino_times[0]["Scan duration"][counter] = in_dur
            trino_times[0]["Merge duration"][counter] = add_times[3]
            trino_times[0]["Write time of the output"][counter] = out_dur

            time_sum = in_dur
            trino_runtimes_add["Trino input"][counter] = time_sum
            time_sum += add_times[0]
            trino_runtimes_add["pre Agg"][counter] = time_sum
            time_sum += add_times[1]
            trino_runtimes_add["ex sink"][counter] = time_sum
            time_sum += add_times[2]
            trino_runtimes_add["ex source"][counter] = time_sum
            time_sum += add_times[3]
            trino_runtimes_add["main Agg"][counter] = time_sum
            time_sum += out_dur
            trino_runtimes_add["Trino output"][counter] = time_sum
            # trino_runtimes_add["Trino Aggregation"][counter] = add_times[3]
            time_sum = in_dur
            trino_runtimes_trans["Trino input"][counter] = time_sum
            time_sum += trans_times[0]
            trino_runtimes_trans["pre Agg"][counter] = time_sum
            time_sum += trans_times[1]
            trino_runtimes_trans["ex sink"][counter] = time_sum
            time_sum += trans_times[2]
            trino_runtimes_trans["ex source"][counter] = time_sum
            time_sum += trans_times[3]
            trino_runtimes_trans["main Agg"][counter] = time_sum
            time_sum += out_dur
            trino_runtimes_trans["Trino output"][counter] = time_sum
            # trino_runtimes_trans["Trino Aggregation"][counter] = add_times[3]

            trino_spill_size[counter] = spill_size

            counter += 1

        plt.figure(12)
        makeBarFig(
            trino_times,
            trino_labels,
            "Time in s",
            True,
            # marking_labels=marking_labels,
            divide=divide,
            titles=titles,
            # "Number of Partitions",
            # markings=True,
            # marking_labels=marking_labels,
        )
        if tpc_4_shuffled:
            colors = plt.cm.viridis.colors
            nth = int(len(colors) / len(trino_runtimes_add))
            colors = colors[nth - 1 :: nth]
            counter = 0
            plt.figure(10)
            for label, value in trino_runtimes_add.items():
                plt.plot(
                    trino_x,
                    value,
                    label=label,
                    linewidth=3,
                    color=colors[counter],
                )
                plt.fill_between(
                    trino_x, value, downy, color=colors[counter], alpha=0.2
                )
                downy = value
                counter += 1

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
    counter = 0
    scan_hashmap_size = 0
    lin_colors = plt.cm.get_cmap("Dark2").colors
    leg_handles = [
        mlines.Line2D([], [], color="black", label="Split", linewidth=3),
        mlines.Line2D(
            [], [], color="black", label="No Split", linewidth=3, linestyle="dashed"
        ),
    ]
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
        if len(differences) > 0:
            printEingerückt(
                "average spill frequency: " + str(sum(differences) / len(differences)),
                tabs,
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
        if len(write_file_size) > 0:
            scan_hashmap_size = max(write_file_size) * jf_data["partitionNumber"]
            printEingerückt("scan_hashmap_size: " + str(scan_hashmap_size), tabs)
            printEingerückt(
                "scan_hashmap_size prop: "
                + str(scan_hashmap_size / (jf_data["mainLimit"])),
                tabs,
            )
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
            plt.step(
                write_file_dur,
                ecdf_values,
                # label=labels[counter],
                linewidth=3,
                linestyle=linestyles[counter % 2],
                #   color=lin_colors[int(counter / 2)],
            )
            plt.grid(visible=True, linestyle="dashed")
            plt.legend()  # handles=leg_handles)
            plt.xlabel("Time in s")
            plt.ylabel("ECDF")
            # if counter % 2 == 0:
            #     leg_handles.append(
            #         mlines.Line2D(
            #             [],
            #             [],
            #             color=lin_colors[int(counter / 2)],
            #             label=labels[int(counter / 2)],
            #             linewidth=3,
            #         )
            #     )

        # uncomment
        # if len(names) == len(labels):
        #     plt.figure(8)
        #     write_spill_dur = np.array(jf_data["write_spill_durs"]) / 1000000
        #     write_spill_dur.sort()
        #     ecdf_values = np.arange(1, len(write_spill_dur) + 1) / len(
        #         write_spill_dur
        #     )
        #     plt.step(
        #         write_spill_dur, ecdf_values, label=labels[counter], linewidth=3
        #     )
        #     plt.grid(visible=True, linestyle="dashed")
        #     plt.legend()
        #     plt.xlabel("Time in s")
        #     plt.ylabel("ECDF")

        if len(names) == len(labels):
            plt.figure(7)
            get_lock_dur.sort()
            ecdf_values = np.arange(1, len(get_lock_dur) + 1) / len(get_lock_dur)
            plt.step(
                get_lock_dur,
                ecdf_values,
                linewidth=3,
                linestyle=linestyles[counter % 2],
                #  color=lin_colors[int(counter / 2)],
            )  # label=labels[counter], )
            plt.grid(visible=True, linestyle="dashed")
            plt.legend()  # handles=leg_handles)
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
        get_file_dur = []
        try:
            get_file_dur = np.array(jf_data["getCall_s3_file_dur"]) / (
                1000000 * jf_data["mergeThread_number"]
            )
            # plt.hist(get_file_dur, bins=30, label="get_file_dur")
            # plt.title("get_file_dur")
            if len(get_file_dur) > 0:
                average = sum(get_file_dur) / len(get_file_dur)
                printEingerückt("get_file_dur avg: " + str(average), tabs)
                printEingerückt("get_file_dur sum: " + str(sum(get_file_dur)), tabs)
                printEingerückt("get_file_dur num: " + str(len(get_file_dur)), tabs)
        except:
            printEingerückt("No mergeThread_number", tabs)
        # plt.figure(9)
        # plt.plot(x, hmap_y * scale / avg_y, label="hmap_size")

        write_file_sum = 0
        for thread in jf_data["Threads"]:
            write_file_sum += thread["write_file_dur"]

        write_file_sum /= jf_data["threadNumber"] * 1000000
        # uncomment
        # get_file_sum = sum(jf_data["getCall_s3_file_dur"]) / (
        #     jf_data["mergeThread_number"]
        #     * 1000000
        #     # jf_data["threadNumber"]
        #     * 1000000
        # )
        # printEingerückt("get_file_sum: " + str(get_file_sum), tabs)
        # # read_tuple_sum = jf_data["get_tuple_dur"] / 1000000
        try:
            write_output_sum = jf_data["write_output_dur"] / (
                1000000 * jf_data["mergeThread_number"]
            )
            printEingerückt("Write output sum: " + str(write_output_sum), tabs)
            printEingerückt(
                "Merge speed: "
                + str(
                    jf_data["linesRead"]
                    / (
                        jf_data["mergeDuration"] * jf_data["mergeThread_number"]
                        - sum(get_file_dur)
                    )
                ),
                tabs,
            )
        except:
            printEingerückt("No mergeThread_number", tabs)

        merge_dur = jf_data[
            "mergeDuration"
        ]  # - write_output_sum  # (get_file_sum + write_output_sum)
        scan_dur = jf_data["scanDuration"] - write_file_sum
        merge_hash_dur = jf_data["mergeHashDuration"]
        # printEingerückt("get_file_dur avg: " + str(average), tabs)

        adding = merge_dur + scan_dur + write_file_sum
        # adding = scan_dur + merge_dur
        # adding = (scan_dur + merge_dur ) / (write_file_sum + scan_dur + merge_dur)
        if tpc_4_shuffled:
            rest = counter % len(runtimes)
            if rest == 0:
                prot_spill_size[int((counter - rest) / len(runtimes))] = (
                    jf_data["colS3Spill"] + jf_data["colBackSpill"]
                ) / 2**30
            # if rest == 0:
            #     runtimes["1 Worker"][int((counter) / len(runtimes))] = adding
            # elif rest == 1:
            #     runtimes["2 Worker"][int((counter - 1) / len(runtimes))] = adding
            # elif rest == 2:
            #     runtimes["3 Worker"][int((counter - 2) / len(runtimes))] = adding
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
            runtimes[runtime_keys[rest]][int((counter - rest) / len(runtimes))] = adding

        times_counter = int(counter / int(arr_len / divide))
        times_sub_counter = counter - times_counter * int(arr_len / divide)
        times[times_counter]["Write time of spill files"][
            times_sub_counter
        ] = write_file_sum
        times[times_counter]["Scan duration"][times_sub_counter] = scan_dur
        # times[0]["Spill rest duration"][counter] = merge_hash_dur
        # times[0]["Write time of the output"][counter] = write_output_sum
        times[times_counter]["Merge duration"][times_sub_counter] = merge_dur
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
        linestyles = ["solid", "dashed", "dotted"]
        counter = 0
        for label, value in runtimes.items():
            plt.plot(
                runtime_x,
                value,
                label=label,
                linewidth=5,
                #                linestyle=linestyles[counter],
                #               color=plt.cm.get_cmap("Dark2").colors[0],
            )
            counter += 1
        plt.legend()
        plt.xlabel("Size of Main Memory in GiB")
        plt.ylabel("Query runtime in s")
        plt.grid(visible=True, linestyle="dashed")

        plt.figure(11)
        colors = plt.cm.viridis.colors
        nth = int(len(colors) / 3)
        colors = colors[nth - 1 :: nth]
        downy = np.zeros(len(subruntimes["Merge duration"]))
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

    if only_trino != -1:
        plt.figure(14)
        plt.plot(trino_x, trino_spill_size, label="Trino", linewidth=3)
        plt.plot(runtime_x, prot_spill_size, label="Prototype", linewidth=3)
        plt.legend()
        plt.xlabel("Size of Main Memory in GiB")
        plt.ylabel("Spilled Data in GiB")
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
                # marking_labels=marking_labels,
                divide=divide,
                titles=titles,
                # "Number of Partitions",
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


def model():
    plt.rcParams.update({"font.size": 35})
    # model init
    q = 4
    wn = [0, 1, 2]
    sT = 10
    mm = [
        4,
        6,
        10,
        15,
        16,
        17.5,
        19,
        20,
        30,
    ]  # 60, 80, 100]
    # mm = [4, 6, 10, 15, 20]
    # mm = [6]
    pn = 30
    mt = 8
    sm = 1
    b = [25]  # [10, 15, 20, 25, 30, 40, 50]
    ds = [0.5, 0.75, 1]
    # ifs = #[False, False,False, True, False, True]
    linestyles = ["solid", "dashed", "dotted"]

    leg_handles = [
        mlines.Line2D([], [], color="black", label="1 Worker", linewidth=3),
        mlines.Line2D(
            [], [], color="black", label="2 Worker", linewidth=3, linestyle="dashed"
        ),
        mlines.Line2D(
            [], [], color="black", label="3 Worker", linewidth=3, linestyle="dotted"
        ),
        # mlines.Line2D(
        #     [], [], color=plt.cm.get_cmap("Dark2").colors[0], label="Real", linewidth=3, linestyle="solid"
        # ),
    ]

    colors = plt.cm.viridis.colors
    colors = plt.cm.get_cmap("Dark2").colors
    # nth = int(len(colors) / (len(ds) ))
    # colors = colors[nth - 1 :: nth]

    d_counter = 0
    for d in ds:
        leg_handles.append(
            mlines.Line2D([], [], color=colors[d_counter], label=str(d), linewidth=3),
        )
        base_line = []
        counter = 0
        for worker_number in wn:
            model_vs = []
            network_loads = []
            for bw in b:
                for m in mm:
                    print(
                        "\n" + str(d) + ", " + str(worker_number) + ", " + str(m) + ":"
                    )
                    stats = main(
                        q, worker_number, sT, m, pn, mt, sm, bw, d
                    )  # ifs[counter])
                    model_vs.append(stats)
                    network_loads.append((stats[4] + stats[3]) / 2**20)

            plt.figure(10)
            y = np.array([row[0] for row in model_vs])
            # plt.plot(
            #     mm,
            #     y,
            #     label=str("write spill dur"),
            #     linewidth=3,
            # )
            y1 = np.array([row[1] for row in model_vs])
            # plt.plot(
            #     mm,
            #     y1 + y,
            #     label=str("scan dur"),
            #     linewidth=3,
            # )
            y2 = np.array([row[2] for row in model_vs])
            if len(mm) > 1:
                plt.plot(
                    mm,
                    y + y1 + y2,
                    label=str(worker_number + 1) + " Worker",  # + " " + str(d),
                    linewidth=5,
                    linestyle=linestyles[counter],
                    color=colors[d_counter],
                    # color=colors[d_counter],
                )
            else:
                plt.figure(4)
                plt.plot(
                    b,
                    y + y1 + y2,
                    label=str(worker_number + 1) + " Worker",  # + " " + str(d),
                    linewidth=5,
                    linestyle=linestyles[counter],
                    color=colors[counter],
                )
            # plt.figure(22)
            # plt.plot(
            #     mm,
            #     network_loads,
            #     label=str(worker_number + 1) + " Worker",  # + " " + str(d),
            #     linewidth=5,
            #     color=colors[counter],
            #     linestyle=linestyles[counter],
            # )
            if len(base_line) == 0:
                base_line = y + y1 + y2
            else:
                plt.figure(3)
                if len(mm) > 1:
                    plt.plot(
                        mm,
                        base_line - (y + y1 + y2),
                        label=str(worker_number),  # + " " + str(d),
                        linewidth=4,
                    )
                else:
                    plt.plot(
                        b,
                        base_line - (y + y1 + y2),
                        label=str(worker_number),  # + " " + str(d),
                        linewidth=4,
                    )
            selctivity = np.array([row[7] for row in model_vs])
            # plt.figure(25)
            # plt.plot(
            #     mm,
            #     selctivity,
            #     label=str(worker_number + 1) + " Worker",  # + " " + str(d),
            #     linewidth=5,
            #     color=colors[counter],
            #     linestyle=linestyles[counter],
            # )
            counter += 1
        d_counter += 1

    plt.figure(10)
    plt.legend(handles=leg_handles)
    plt.xlabel("Size of Main Memory in GiB")
    plt.ylabel("Runtime in s")
    plt.grid(visible=True, linestyle="dashed")

    plt.figure(1)
    plt.legend()
    plt.xlabel("Runtime in s")
    plt.ylabel("Network Load")
    plt.grid(visible=True, linestyle="dashed")
    plt.figure(22)
    plt.legend()
    plt.xlabel("Size of Main Memory in GiB")
    plt.ylabel("Network load in MiB/s")
    plt.grid(visible=True, linestyle="dashed")
    plt.figure(3)
    plt.legend()
    plt.xlabel("Size of Main Memory in GiB")
    plt.ylabel("Timegain in s")
    plt.grid(visible=True, linestyle="dashed")
    plt.figure(4)
    plt.legend()
    plt.xlabel("Network Bandwidth in MiB/s")
    plt.ylabel("Runtime in s")
    plt.grid(visible=True, linestyle="dashed")
    plt.figure(25)
    plt.legend()
    plt.xlabel("Size of Main Memory in GiBy")
    plt.ylabel("Merge Help selecitivity")
    plt.grid(visible=True, linestyle="dashed")
    plt.show()


# TPC()
# analyse_Query("8")
#c_size_by_time()
analyse_1_6_13()
# model()
