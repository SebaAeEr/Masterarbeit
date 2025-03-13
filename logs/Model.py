import math
from scipy.special import comb


# tpc = 4
# tuple_size = 2

worker_number = 0
scanThreads = 6
mainMemory = 4
merge_helper_mainMemory = 4
partition_number = 30
merge_threads = 6
merge_subThreads = 4
merge_help_threads = 4
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
merge_help_speed = 6
# merge_help_speed = 14
scan_hashmapSize = 0
scan_dur = 0
write_spill_dur = 0

post_scan_selectivity = -1
scan_time_per_spill = 0
spill_freq = 0

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
merge_help_max_file_number = 0
mem_pressure = 0
all_tupl_scans = 0
mem_pressure_post = 0
merged_tuples_per_s_w = 0
merged_tuples_per_s = 0


def reset():
    global merged_tuples_per_s_w, merged_tuples_per_s ,mem_pressure_post, all_tupl_scans, mem_pressure, spill_freq, merge_help_max_file_number, biggest_file, scan_hashmapSize, scan_dur, write_spill_dur, post_scan_selectivity, scan_time_per_spill, upload_network_load_scan, upload_network_load_merge_help, download_network_load_merge_help, download_network_load_merge, merge_hashmapSize, merge_time_per_partition, merge_selectivity, merge_help_selectivity, help_merged_tuples, spill_file_merge_speed, postScan_tuple_number, merge_dur, write_time_per_spill, congestion_factor, merge_help_write_tuple, merge_help_get_tuple, file_get_number

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
    merge_help_max_file_number = 0
    spill_freq = 0
    mem_pressure = 0
    all_tupl_scans = 0
    mem_pressure_post = 0
    merged_tuples_per_s = 0
    merged_tuples_per_s_w = 0


def calc_selectivity(values_number, tuple_number):
    """
    Calculates the expected number of distinct colors when drawing z marbles
    from a box with x marbles and y colors, assuming equal distribution of marbles.

    Parameters:
    - x: Total number of marbles in the box
    - y: Total number of distinct colors
    - z: Number of marbles drawn from the box

    Returns:
    - Expected number of distinct colors
    """
    # The number of marbles per color

    # Calculate the expected number of distinct colors
    probability_not_selected = (1 - 1 / values_number) ** tuple_number
    expected_value = values_number * (1 - probability_not_selected)

    return expected_value / tuple_number


def calc_scan_hashmapSize():
    global scan_hashmapSize
    thread_mem = (mainMemory) * 2**30 / scanThreads
    scan_hashmapSize = int(thread_mem / hashmap_avg)
    #print("scan hashmap size: " + str(scan_hashmapSize))


def calc_postScan_selectivity():
    global post_scan_selectivity, selectivity, scan_hashmapSize
    num_same = 1 / selectivity
    post_scan_selectivity = calc_selectivity(
        input_lines * selectivity, scan_hashmapSize
    )
    # num_of_matches = (
    #     num_same * (scan_hashmapSize * (scan_hashmapSize + 1) / 2) / (input_lines)
    # )
    # post_scan_selectivity = (scan_hashmapSize - num_of_matches) / scan_hashmapSize
    # #print("num_of_matches: " + str(scan_hashmapSize - num_of_matches) + " vs " + str(selec))
    # #print(
    #     "post_scan_selectivity: "
    #     + str(post_scan_selectivity)
    #     + " vs "
    #     + str(post_scan_selectivity2)
    # )


def calc_scan_time_per_spill():
    global scan_time_per_spill
    scan_time_per_spill = scan_hashmapSize / (
        post_scan_selectivity * read_speed * 10**6
    )
    scan_time_per_spill += write_time_per_spill
    #print("scan_time_per_spill: " + str(scan_time_per_spill))


def calc_spill_freq():
    global spill_freq
    spill_num = input_lines * post_scan_selectivity / scan_hashmapSize
    spill_freq = spill_num / (scan_dur + write_spill_dur)
    #print("spill_freq: " + str(spill_freq))
    #print("spill_num: " + str(spill_num))
    #print("time: " + str(scan_dur + write_spill_dur))


def calc_write_spill_dur():
    global write_spill_dur
    write_spill_dur = write_time_per_spill * math.ceil(
        input_lines * post_scan_selectivity / scan_hashmapSize
    )
    #print("write_spill_dur: " + str(write_spill_dur))


def calc_scan_dur():
    global scan_dur
    scan_dur = input_lines / (read_speed * 10**6)


def calc_network_load_spill():
    global scan_time_per_spill, upload_network_load_scan
    spill_size = scan_hashmapSize * scanThreads * comp_fac
    # upload_network_load_scan += spill_size / scan_time_per_spill
    upload_network_load_scan = input_lines * post_scan_selectivity / scan_dur
    # #print(
    #     "spill_size: "
    #     + str(spill_size)
    #     + " scan_time_per_second: "
    #     + str(scan_time_per_spill)
    #     + " upload_network_load_scan / s: "
    #     + str(upload_network_load_scan / 2**20)
    # )
    # #print(
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
    # #print("num_same: " + str(num_same))
    spill_tuples = scan_hashmapSize * 8 / partition_number
    # #print("spill_tuples: " + str(spill_tuples))
    hmap_size_base = min(
        merge_helper_mainMemory * 2**30 / (merge_help_threads * hashmap_avg), input_lines / partition_number
    )
    hmap_size_base *= partition_number
    merge_help_selectivity = calc_selectivity(
        selectivity * post_scan_selectivity * input_lines, hmap_size_base
    )


def calc_merge_help_max_file_number():
    global merge_help_max_file_number
    merge_help_max_file_number = (
        mainMemory
        * 2**30
        / (
            merge_help_threads
            * hashmap_avg
            * scan_hashmapSize
            * merge_help_selectivity
            / partition_number
        )
    )
    merge_help_max_file_number = max(8, merge_help_max_file_number)


def calc_merge_help_write_get_tuple():
    global merge_help_write_tuple, merge_help_get_tuple
    if worker_number > 0:
        helper_size = int(merge_helper_mainMemory * 2**30 / hashmap_avg)
        avg_file_number_partition = (
            input_lines * post_scan_selectivity / scan_hashmapSize
        )
        avg_file_number_partition /= 2
        io_dur = (
            (
                helper_size * comp_fac * merge_help_selectivity
            )  # download + upload size
        ) / bandwith
        merge_dur = helper_size  / (partition_number * merge_help_speed * 10**6)
        #print("merge_dur + io_dur: " + str(merge_dur + io_dur))
        print("io_dur: " + str(io_dur))
        print("merge_dur: " + str(merge_dur))
        #print("merge_help_max_file_number: " + str(merge_help_max_file_number))
        if inf_spill_freq:
            min_spill_freq = worker_number * 5 * merge_help_threads / (merge_dur + io_dur)
           # #print("inf, spill_freq: " + str(spill_freq))
        else:
            min_spill_freq = min(
            # scanThreads * partition_number / scan_time_per_spill,
            spill_freq * partition_number,
            worker_number * 5 * merge_help_threads / (merge_dur + io_dur),
            )
        print(
            "min_spill_freq: "
            + str(min_spill_freq)  +
            " spiller: "
            + str(spill_freq * partition_number) +
            " getter: "
            + str(worker_number * 5 * merge_help_threads / (merge_dur + io_dur))
        )
        # #print("min spill_freq: " + str(spill_freq))
        merge_help_write_tuple = (
            scan_hashmapSize
            * min_spill_freq
            * merge_help_selectivity
            / partition_number
        )
        print("merge_help_write_tuple: " + str(merge_help_write_tuple))
        # #print("scan_hashmapSize: " + str(scan_hashmapSize))
        #print("merge_help_selectivity: " + str(merge_help_selectivity))
        merge_help_get_tuple = helper_size * min_spill_freq / partition_number


def calc_biggest_file():
    global biggest_file
    if worker_number > 0:
        tuple_diff = input_lines * post_scan_selectivity - postScan_tuple_number
        merge_help_dur = tuple_diff / (merge_help_get_tuple - merge_help_write_tuple)
        biggest_file = merge_help_dur * merge_help_write_tuple / partition_number
        #print("bigges file: " + str(biggest_file))


def calc_network_load_merge_help():
    global upload_network_load_merge_help, download_network_load_merge_help
    merge_help_write_tuple_temp = merge_help_write_tuple
    merge_help_get_tuple_temp = merge_help_get_tuple
    # if postScan_tuple_number == input_lines * selectivity:
    #     merge_help_dur = max(0, scan_dur - late_start + merge_dur + write_spill_dur)
    #     real_merge_help_dur = postScan_tuple_number / merge_help_write_tuple
    #     #print("real_merge_help_dur: " + str(real_merge_help_dur))
    #     #print("scan_dur: " + str(scan_dur))
    #     #print("merge_help_dur: " + str(merge_help_dur))
    #     merge_help_write_tuple_temp = postScan_tuple_number / (merge_help_dur)
    #     merge_help_get_tuple_temp = input_lines * post_scan_selectivity / merge_help_dur

    upload_network_load_merge_help = merge_help_write_tuple_temp * comp_fac
    download_network_load_merge_help = merge_help_get_tuple_temp * comp_fac
    # #print(
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
            calc_scan_time_per_spill()
            write_time_per_spill = max(0, spill_size / bandwith - scan_time_per_spill)
        elif spill_mode == 2:
            write_time_per_spill = spill_size / bandwith
            # #print("write_time_per_spill: " + str(write_time_per_spill))


def calc_merge_selectivity():
    global merge_selectivity, post_scan_selectivity, merge_help_selectivity, selectivity
    merge_selectivity = input_lines * selectivity / postScan_tuple_number


def calc_merge_hashmapSize():
    global merge_hashmapSize, merge_threads
    thread_mem = (
        ((mainMemory - 1) * 2**30)
        - (
            (scan_hashmapSize * merge_threads * buffer_fac)
            / partition_number
        )
    ) / merge_threads
    # #print("spill file size: " + str(scan_hashmapSize * comp_fac / partition_number))
    merge_hashmapSize = int(thread_mem / hashmap_avg)


def calc_merge_time_per_partition():
    global merge_time_per_partition, file_get_number, all_tupl_scans
    tuples_per_part = postScan_tuple_number / partition_number
    hashmap_size_selec = merge_hashmapSize / merge_selectivity
    # print("tuples_per_part:" + str(tuples_per_part))
    # print("hashmap_size_selec:" + str(hashmap_size_selec))
    # print("biggest_file:" + str(biggest_file))
    rescans = math.ceil(tuples_per_part / hashmap_size_selec)
    # print("rescans: " + str(rescans))
    rescan_tuples = 0
    rest = max(0, tuples_per_part - hashmap_size_selec - biggest_file)
    output = 0
    file_get_number = 0
    # print("rest: " + str(rest))

    # #print("hashmap_size_selec:" + str(hashmap_size_selec))
    # #print("tuples_per_part:" + str(tuples_per_part))
    while rest != 0:
        rescan_tuples += rest
        file_get_number += rest / (scan_hashmapSize / partition_number)
        file_get_number += 1
        if output > biggest_file:
            rest = max(0, rest - hashmap_size_selec)
        output += hashmap_size_selec

        # rest -= merge_hashmapSize * merge_selectivity

    partition_file_number = math.ceil(postScan_tuple_number / scan_hashmapSize)
    all_tupl_scans = rescan_tuples + tuples_per_part
    #print("rescan_tuples:" + str(rescan_tuples))
    #print("all_tupl_scans:" + str(all_tupl_scans))
    # file_get_number = all_tupl_scans / (tuples_per_part / partition_file_number)
    #print("file_get_number per partition:" + str(file_get_number))
    #print("get file dur per partition:" + str(file_get_number * file_get))
    merge_time_per_partition = file_get_number * file_get + all_tupl_scans / (
        merge_speed * 10**6
    )
    # #print("merge_time_per_partition:" + str(merge_time_per_partition))


def calc_merge_dur():
    global merge_dur
    merge_dur = merge_time_per_partition * math.ceil(partition_number / merge_threads)


def calc_postScan_tuple_number():
    global postScan_tuple_number, merge_dur
    if worker_number > 0:
        # #print("postScan_tuple_number1: " + str(postScan_tuple_number))
        postScan_tuple_number = input_lines * post_scan_selectivity
        calc_merge_selectivity()
        calc_merge_hashmapSize()
        calc_biggest_file()
        calc_merge_time_per_partition()
        calc_merge_dur()
        merge_help_dur = 0
        merge_dur_old = 0
        for i in range(100):
            merge_help_dur = max(0, scan_dur - late_start + merge_dur + write_spill_dur)
            merge_help_tuples = merge_help_dur * (merge_help_get_tuple - merge_help_write_tuple)
            #print("merge_help_tuples: " + str(merge_help_tuples))
            postScan_tuple_number = max(
                post_scan_selectivity * input_lines - merge_help_tuples,
                input_lines * selectivity,
            )
            merge_dur_old = merge_dur
            calc_merge_selectivity()
            calc_merge_hashmapSize()
            calc_biggest_file()
            calc_merge_time_per_partition()
            calc_merge_dur()
            merge_dur = (merge_dur_old * 5 + merge_dur) / 6
    else:
        postScan_tuple_number = input_lines * post_scan_selectivity
        
    #print(
    #     "postScan_tuple_number1: "
    #     + str(post_scan_selectivity * input_lines - merge_help_tuples)
    #     + " vs "
    #     + str(input_lines * selectivity)
    # )
    # #print("postScan_tuple_number2: " + str(postScan_tuple_number))


def calc_download_network_load_merge():
    global download_network_load_merge
    download_network_load_merge = (
        (postScan_tuple_number / (partition_number * scan_hashmapSize))
        * file_get_number
        * comp_fac
        / merge_dur
    )

def calc_mem_pressure():
    global mem_pressure, mem_pressure_post, postScan_tuple_number, biggest_file
    mem_pressure_post = all_tupl_scans * partition_number
    all_tuple_post = all_tupl_scans
    postScan_tuple_number = post_scan_selectivity * input_lines
    biggest_file = scan_hashmapSize / partition_number
    calc_merge_selectivity()
    calc_merge_time_per_partition()
    mem_pressure = all_tupl_scans * partition_number
    #print("all_tuple_post: " + str(all_tuple_post) + " all_tuple: " +  str(all_tupl_scans))

def calc_merge_help_tuples():
    global merged_tuples_per_s, merged_tuples_per_s_w
    if worker_number > 0:
        merged_tuples = post_scan_selectivity * input_lines - postScan_tuple_number
        merged_tuples_per_s = merge_help_get_tuple - merge_help_write_tuple
        #merged_tuples_per_s = merged_tuples / (scan_dur + merge_dur + write_spill_dur)
        merged_tuples_per_s_w = merged_tuples_per_s / worker_number

def simul(q, wn, sT, mm, hmm, pn, mt,mht, sm, b, divider, ls,mhs, ss: float = 8, isf: bool = False):
    global merge_help_threads, merge_help_speed, merge_helper_mainMemory, worker_number, scanThreads, mainMemory, partition_number, merge_threads, spill_mode, bandwith, input_lines, selectivity, tuple_size, file_get, comp_fac, late_start, inf_spill_freq, read_speed
    reset()
    tpc = q
    worker_number = wn
    scanThreads = sT
    mainMemory = mm
    partition_number = pn
    merge_threads = mt
    merge_help_threads = mht
    spill_mode = sm
    late_start = ls
    inf_spill_freq = isf
    read_speed = ss
    merge_help_speed = mhs
    merge_helper_mainMemory = hmm
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
    calc_write_time_per_spill()
    calc_scan_time_per_spill()

    calc_scan_dur()
    calc_write_spill_dur()
    calc_network_load_spill()
    calc_spill_freq()

    calc_merge_help_selectivity()
    calc_merge_help_max_file_number()
    calc_merge_help_write_get_tuple()

    calc_postScan_tuple_number()

    calc_biggest_file()
    calc_merge_selectivity()
    calc_merge_hashmapSize()
    calc_merge_time_per_partition()
    calc_merge_dur()
    calc_download_network_load_merge()
    calc_network_load_merge_help()
 
    calc_merge_help_tuples()
    calc_mem_pressure()
    # #print("post_scan_selectivity: " + str(post_scan_selectivity))
    # #print("scan_dur: " + str(scan_dur))
    # #print("write spill: " + str(write_spill_dur))
    # #print("merge_dur: " + str(merge_dur))
    return (
        write_spill_dur,
        scan_dur,
        merge_dur,
        upload_network_load_scan,
        upload_network_load_merge_help,
        download_network_load_merge_help,
        download_network_load_merge,
        merge_help_selectivity,
        mem_pressure,
        mem_pressure_post,
        merged_tuples_per_s,
        merged_tuples_per_s_w,
        merge_help_write_tuple,
        merge_help_selectivity
    )


def main(q, wn, sT, mm, hmm, pn, mt, mht, sm, b, division,mhs, ss: float = 8, isf: bool = False):
    ls = 0
    if division < 1 and wn > 0:
        div = (1 - division) / wn
        stats = simul(q, 0, sT, mm, hmm, pn, mt,mht, sm, b, div, ls,mhs, ss, isf)
        ls = stats[0] + stats[1] + stats[2]
    stats = simul(q, wn, sT, mm, hmm,  pn, mt, mht, sm, b, division, ls,mhs,ss, isf)
    #print("scan dur: " + str(stats[1]))
    #print("spill dur: " + str(stats[0]))
    #print("merge dur: " + str(stats[2]))
    return stats