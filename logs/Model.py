import math

# tpc = 4
# tuple_size = 2

split = [1]
worker_number = 0
scanThreads = 6
mainMemory = 4
partition_number = 30
merge_threads = 6
merge_subThreads = 4
spill_mode = 1
bandwith = 30
late_start = 0
if bandwith != -1:
    bandwith *= 2**20
inf_spill_freq = False

hashmap_avg = 55  # avg size of hashmap entry
# read_speed = 7  # mio lines / s
read_speed = 8
# merge_speed = 1.1  # 3.5  # merge mio tuples /s
merge_speed = 1.5
# if spill_mode == 0:
#     file_get = 0.0053  # speed of get file
# else:
#     file_get = 0.11
buffer_fac = 8.53
tuple_size = 0
comp_fac = 5 * tuple_size  # size of compressed tuple
merge_help_speed = 7.7

scan_hashmapSize = 0
scan_dur = 0
write_spill_dur = 0

post_scan_selectivity = -1
scan_time_per_spill = 0

upload_network_load_scan = 0
upload_network_load_merge_help = 0
download_network_load_merge_help = 0
download_network_load_merge = 0

merge_hashmapSize = 0
merge_time_per_partition = 0
merge_selectivity = 0
merge_help_selectivity = 0
help_merged_tuples = 0
spill_file_merge_speed = 0
postScan_tuple_number = 0
merge_dur = 0
write_time_per_spill = 0
congestion_factor = 1
merge_help_write_tuple = 0
merge_help_get_tuple = 0
file_get_number = 0
biggest_file = 0


def reset():
    global biggest_file, scan_hashmapSize, scan_dur, write_spill_dur, post_scan_selectivity, scan_time_per_spill, upload_network_load_scan, upload_network_load_merge_help, download_network_load_merge_help, download_network_load_merge, merge_hashmapSize, merge_time_per_partition, merge_selectivity, merge_help_selectivity, help_merged_tuples, spill_file_merge_speed, postScan_tuple_number, merge_dur, write_time_per_spill, congestion_factor, merge_help_write_tuple, merge_help_get_tuple, file_get_number

    scan_hashmapSize = 0
    scan_dur = 0
    write_spill_dur = 0
    post_scan_selectivity = -1
    scan_time_per_spill = 0

    upload_network_load_scan = 0
    upload_network_load_merge_help = 0
    download_network_load_merge_help = 0
    download_network_load_merge = 0

    merge_hashmapSize = 0
    merge_time_per_partition = 0
    merge_selectivity = 0
    merge_help_selectivity = 0
    help_merged_tuples = 0
    spill_file_merge_speed = 0
    postScan_tuple_number = 0
    merge_dur = 0
    write_time_per_spill = 0
    congestion_factor = 1
    merge_help_write_tuple = 0
    merge_help_get_tuple = 0
    file_get_number = 0
    biggest_file = 0


def calc_scan_hashmapSize():
    global scan_hashmapSize
    thread_mem = (mainMemory) * 2**30 / scanThreads
    scan_hashmapSize = int(thread_mem / hashmap_avg)
    # print("scan hashmap size: " + str(scan_hashmapSize))


def calc_postScan_selectivity():
    global post_scan_selectivity, selectivity, scan_hashmapSize
    num_same = 1 / selectivity
    num_of_matches = (
        num_same * (scan_hashmapSize * (scan_hashmapSize + 1) / 2) / (input_lines)
    )
    post_scan_selectivity = (scan_hashmapSize - num_of_matches) / scan_hashmapSize


def calc_scan_time_per_spill():
    global scan_time_per_spill
    scan_time_per_spill = scan_hashmapSize / (
        post_scan_selectivity * read_speed * 10**6
    )


def calc_write_spill_dur():
    global write_spill_dur
    write_spill_dur = write_time_per_spill * math.ceil(
        input_lines * post_scan_selectivity / scan_hashmapSize
    )


def calc_scan_dur():
    global scan_dur
    scan_dur = input_lines / (read_speed * 10**6)


def calc_network_load_spill():
    global scan_time_per_spill, upload_network_load_scan
    spill_size = scan_hashmapSize * scanThreads * comp_fac
    # upload_network_load_scan += spill_size / scan_time_per_spill
    upload_network_load_scan = input_lines * post_scan_selectivity / scan_dur
    # print(
    #     "spill_size: "
    #     + str(spill_size)
    #     + " scan_time_per_second: "
    #     + str(scan_time_per_spill)
    #     + " upload_network_load_scan / s: "
    #     + str(upload_network_load_scan / 2**20)
    # )
    # print(
    #     "spill_size: "
    #     + str(input_lines * post_scan_selectivity)
    #     + " scan_time_per_second: "
    #     + str(scan_dur)
    #     + " upload_network_load_scan / s: "
    #     + str(upload_network_load_scan2 / 2**20)
    # )


# def calc_congestion_factor():
#     global congestion_factor
#     congestion_factor =


def calc_merge_help_selectivity():
    global merge_help_selectivity
    num_same = post_scan_selectivity / selectivity
    # print("num_same: " + str(num_same))
    spill_tuples = scan_hashmapSize / partition_number
    # print("spill_tuples: " + str(spill_tuples))
    hmap_size_base = min(
        mainMemory * 2**30 / (4 * hashmap_avg * 3), input_lines / partition_number
    )
    print("hmap_size_base: " + str(hmap_size_base))
    num_of_matches = 0
    for i in range(1, 8):
        hmsap_size = spill_tuples * i - num_of_matches + hmap_size_base
        num_of_matches += (
            num_same * partition_number * (hmap_size_base * spill_tuples) / input_lines
        )
    print("num_of_matches: " + str(num_of_matches))
    merge_help_selectivity = max(
        (spill_tuples * 8 - num_of_matches) / (spill_tuples * 8),
        selectivity / post_scan_selectivity,
    )
    print("merge_help_selectivity: " + str(merge_help_selectivity))


def calc_merge_help_write_get_tuple():
    global merge_help_write_tuple, merge_help_get_tuple
    if worker_number > 0:
        io_dur = (
            (
                scan_hashmapSize * 8 * (1 + merge_help_selectivity)
            )  # download + upload size
        ) / (bandwith * 4)
        merge_dur = scan_hashmapSize * 8 / (partition_number * merge_help_speed * 10**6)
        # print("merge_dur: " + str(merge_dur))
        # if inf_spill_freq:
        #     spill_freq = worker_number * 8 * 4 / (merge_dur + io_dur)
        #     print("inf, spill_freq: " + str(spill_freq))
        # else:
        spill_freq = min(
            scanThreads * partition_number / (worker_number * scan_time_per_spill),
            worker_number * 8 * 4 / (merge_dur + io_dur),
        )
        print(
            "spill_freq: "
            + str(
                scanThreads * partition_number / (worker_number * scan_time_per_spill)
            )
            + " vs "
            + str(worker_number * 8 * 4 / (merge_dur + io_dur))
        )
        # print("min spill_freq: " + str(spill_freq))
        merge_help_write_tuple = (
            scan_hashmapSize * spill_freq * merge_help_selectivity / partition_number
        )
        # print("merge_help_write_tuple: " + str(merge_help_write_tuple))
        # print("scan_hashmapSize: " + str(scan_hashmapSize))
        print("merge_help_selectivity: " + str(merge_help_selectivity))
        merge_help_get_tuple = scan_hashmapSize * spill_freq / partition_number


def calc_biggest_file():
    global biggest_file
    if worker_number > 0:
        tuple_diff = input_lines * post_scan_selectivity - postScan_tuple_number
        merge_help_dur = tuple_diff / (merge_help_get_tuple - merge_help_write_tuple)
        biggest_file = merge_help_dur * merge_help_write_tuple / partition_number
        print("bigges file: " + str(biggest_file))


def calc_network_load_merge_help():
    global upload_network_load_merge_help, download_network_load_merge_help
    merge_help_write_tuple_temp = merge_help_write_tuple
    merge_help_get_tuple_temp = merge_help_get_tuple 
    if postScan_tuple_number == input_lines * selectivity:  
        merge_help_dur = max(0, scan_dur - scan_time_per_spill * 2 - late_start + merge_dur)
        merge_help_write_tuple_temp = postScan_tuple_number / merge_help_dur
        merge_help_get_tuple_temp = input_lines * post_scan_selectivity / merge_help_dur
    
    upload_network_load_merge_help = merge_help_write_tuple_temp * comp_fac
    download_network_load_merge_help = merge_help_get_tuple_temp * comp_fac
    # print(
    #     "merge_help_write_tuple: "
    #     + str(merge_help_write_tuple)
    #     + " merge_help_write_tuple * comp_fac: "
    #     + str(merge_help_write_tuple * comp_fac)
    #     + " upload_network_load_merge_help: "
    #     + str(upload_network_load_merge_help)
    # )


def calc_write_time_per_spill():
    global write_time_per_spill
    if bandwith != -1:
        spill_size = scan_hashmapSize * comp_fac
        if spill_mode == 1:
            write_time_per_spill = max(0, spill_size / bandwith - scan_time_per_spill)
        elif spill_mode == 2:
            write_time_per_spill = spill_size / bandwith
            # print("write_time_per_spill: " + str(write_time_per_spill))


def calc_merge_selectivity():
    global merge_selectivity, post_scan_selectivity, merge_help_selectivity, selectivity
    merge_selectivity = input_lines * selectivity / postScan_tuple_number


def calc_merge_hashmapSize():
    global merge_hashmapSize, merge_threads
    thread_mem = (
        ((mainMemory - 1) * 2**30)
        - (
            (scan_hashmapSize * comp_fac * merge_threads * buffer_fac)
            / partition_number
        )
    ) / merge_threads
    # print("spill file size: " + str(scan_hashmapSize * comp_fac / partition_number))
    merge_hashmapSize = int(thread_mem / hashmap_avg)


def calc_merge_time_per_partition():
    global merge_time_per_partition, file_get_number
    tuples_per_part = postScan_tuple_number / partition_number
    hashmap_size_selec = merge_hashmapSize / merge_selectivity
    # print("merge_selectivity:" + str(merge_selectivity))
    # print("merge_hashmapSize:" + str(merge_hashmapSize))
    rescans = math.ceil(tuples_per_part / hashmap_size_selec)
    rescan_tuples = 0
    rest = max(0, tuples_per_part - hashmap_size_selec - biggest_file)
    output = 0
    file_get_number = 0

    # print("hashmap_size_selec:" + str(hashmap_size_selec))
    # print("tuples_per_part:" + str(tuples_per_part))
    for i in range(rescans):
        rescan_tuples += rest
        file_get_number += rest / (scan_hashmapSize / partition_number)
        file_get_number += 1
        if output > biggest_file:
            rest = max(0, rest - hashmap_size_selec)
        output += hashmap_size_selec

        # rest -= merge_hashmapSize * merge_selectivity

    partition_file_number = math.ceil(postScan_tuple_number / scan_hashmapSize)
    all_tupl_scans = tuples_per_part + rescan_tuples
    print("rescan_tuples:" + str(rescan_tuples))
    print("all_tupl_scans:" + str(all_tupl_scans))
    # file_get_number = all_tupl_scans / (tuples_per_part / partition_file_number)
    print("file_get_number per partition:" + str(file_get_number))
    print("get file dur per partition:" + str(file_get_number * file_get))
    merge_time_per_partition = file_get_number * file_get + all_tupl_scans / (
        merge_speed * 10**6
    )
    # print("merge_time_per_partition:" + str(merge_time_per_partition))


def calc_merge_dur():
    global merge_dur
    merge_dur = merge_time_per_partition * math.ceil(partition_number / merge_threads)


def calc_postScan_tuple_number():
    global postScan_tuple_number
    merge_help_dur = max(0, scan_dur - scan_time_per_spill * 2 - late_start)
    # print("scan_dur: " + str(scan_dur))
    # print("late_start: " + str(-scan_time_per_spill * 2 - late_start))
    merge_help_tuples = merge_help_dur * (merge_help_get_tuple - merge_help_write_tuple)
    print("merge_help_get_tuple: " + str(merge_help_get_tuple))
    print("merge_help_write_tuple: " + str(merge_help_write_tuple))
    postScan_tuple_number = max(
        post_scan_selectivity * input_lines - merge_help_tuples,
        input_lines * selectivity,
    )
    print("postScan_tuple_number1: " + str(post_scan_selectivity * input_lines - merge_help_tuples) + " vs " + str(input_lines * selectivity))
    # print("postScan_tuple_number1: " + str(postScan_tuple_number))
    calc_merge_selectivity()
    calc_merge_hashmapSize()
    calc_biggest_file()
    calc_merge_time_per_partition()
    calc_merge_dur()

    merge_help_dur = max(0, scan_dur - scan_time_per_spill * 2 - late_start + merge_dur)
    merge_help_tuples = merge_help_dur * (merge_help_get_tuple - merge_help_write_tuple)
    # print("merge_help_tuples: " + str(merge_help_tuples))
    postScan_tuple_number = max(
        post_scan_selectivity * input_lines - merge_help_tuples,
        input_lines * selectivity,
    )
    print("postScan_tuple_number1: " + str(post_scan_selectivity * input_lines - merge_help_tuples) + " vs " + str(input_lines * selectivity))
    # print("postScan_tuple_number2: " + str(postScan_tuple_number))


def calc_download_network_load_merge():
    global download_network_load_merge
    download_network_load_merge = (
        (postScan_tuple_number / (partition_number * scan_hashmapSize))
        * file_get_number
        * comp_fac
        / merge_dur
    )


def simul(q, wn, sT, mm, pn, mt, sm, b, divider, ls, isf: bool = False):
    global worker_number, scanThreads, mainMemory, partition_number, merge_threads, spill_mode, bandwith, input_lines, selectivity, tuple_size, file_get, comp_fac, late_start, inf_spill_freq
    reset()
    tpc = q
    worker_number = wn
    scanThreads = sT
    mainMemory = mm
    partition_number = pn
    merge_threads = mt
    spill_mode = sm
    late_start = ls
    inf_spill_freq = isf
    if spill_mode == 0:
        file_get = 0.0053  # speed of get file
    else:
        # file_get = 0.11
        file_get = 0.0053
    bandwith = b
    if bandwith != -1:
        bandwith *= 2**20
    if tpc == 13:
        input_lines = 1533839148 * divider  # input lines
        selectivity = 0.097
        tuple_size = 2  # tuple length
    elif tpc == 17:
        input_lines = 5999989708 * divider  # input lines
        selectivity = 0.033
        tuple_size = 3  # tuple length
    elif tpc == 4:
        input_lines = 3793363898 * divider  # input lines
        selectivity = 0.362
        tuple_size = 1  # tuple length
    comp_fac = 5 * tuple_size
    calc_scan_hashmapSize()
    calc_postScan_selectivity()
    calc_scan_time_per_spill()

    calc_write_time_per_spill()
    calc_scan_dur()
    calc_write_spill_dur()
    calc_network_load_spill()

    calc_merge_help_selectivity()
    calc_merge_help_write_get_tuple()
    

    calc_postScan_tuple_number()

    calc_merge_selectivity()
    calc_merge_hashmapSize()
    calc_merge_time_per_partition()
    calc_merge_dur()
    calc_download_network_load_merge()
    calc_network_load_merge_help()

    # print("post_scan_selectivity: " + str(post_scan_selectivity))
    # print("scan_dur: " + str(scan_dur))
    # print("write spill: " + str(write_spill_dur))
    # print("merge_dur: " + str(merge_dur))
    return (
        write_spill_dur,
        scan_dur,
        merge_dur,
        upload_network_load_scan,
        upload_network_load_merge_help,
        download_network_load_merge_help,
        download_network_load_merge,
    )


def main(q, wn, sT, mm, pn, mt, sm, b, division, isf: bool = False):
    ls = 0
    if division < 1 and wn > 0:
        div = (1 - division) / wn
        stats = simul(q, 0, sT, mm, pn, mt, sm, b, div, ls, isf)
        ls = stats[0] + stats[1] + stats[2]
    stats = simul(q, wn, sT, mm, pn, mt, sm, b, division, ls, True)
    print("scan dur: " + str(stats[1]))
    print("spill dur: " + str(stats[0]))
    print("merge dur: " + str(stats[2]))
    return stats
