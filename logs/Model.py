import math

tpc = 4
if tpc == 13:
    input_lines = 1533839148  # input lines
    selectivity = 0.097
    tuple_size = 2  # tuple length
elif tpc == 17:
    input_lines = 5999989708  # input lines
    selectivity = 0.033
    tuple_size = 3  # tuple length
elif tpc == 4:
    input_lines = 3793363898  # input lines
    selectivity = 0.362
    tuple_size = 1  # tuple length

worker_number = 0
scanThreads = 6
mainMemory = 15
partition_number = 30
merge_threads = 6
merge_subThreads = 4
spill_mode = 0
bandwith = 15
if bandwith != -1:
    bandwith *= 2**20

hashmap_avg = 56  # avg size of hashmap entry
read_speed = 7  # mio lines / s
merge_speed = 1.1  # 3.5  # merge mio tuples /s
if spill_mode == 0:
    file_get = 0.0053  # speed of get file
else:
    file_get = 0.11
buffer_fac = 8.53
comp_fac = 5 * tuple_size  # size of compressed tuple
merge_help_speed = 7.7

scan_hashmapSize = 0
scan_dur = 0

post_scan_selectivity = -1
scan_time_per_spill = 0

upload_network_load = 0
download_network_load = 0

merge_hashmapSize = 0
merge_time_per_partition = 0
merge_selectivity = 0
merge_help_selectivity = 0
help_merged_tuples = 0
spill_file_number = 0
spill_file_merge_speed = 0
postScan_tuple_number = 0
merge_dur = 0
write_time_per_spill = 0
congestion_factor = 1
merge_help_write_tuple = 0
merge_help_get_tuple = 0


def calc_scan_hashmapSize():
    global scan_hashmapSize
    thread_mem = (mainMemory - 0.5) * 2**30 / scanThreads
    scan_hashmapSize = int(thread_mem / hashmap_avg)
    print("scan hashmap size: " + str(scan_hashmapSize))


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


def calc_spill_file_number():
    global spill_file_number
    spill_file_number = (
        math.ceil(postScan_tuple_number / scan_hashmapSize) * partition_number
    )
    print("spill_file_number:" + str(spill_file_number))


def calc_scan_dur():
    global scan_dur
    scan_dur = (
        input_lines / (read_speed * 10**6) + write_time_per_spill * spill_file_number
    )


def calc_network_load_spill():
    global scan_time_per_spill, upload_network_load
    spill_size = scan_hashmapSize * scanThreads * comp_fac
    upload_network_load += spill_size / scan_time_per_spill
    print("upload_network_load / s: " + str(upload_network_load / 2**20))


# def calc_congestion_factor():
#     global congestion_factor
#     congestion_factor =


def calc_merge_help_selectivity():
    global merge_help_selectivity
    num_same = post_scan_selectivity / selectivity
    print("num_same: " + str(num_same))
    spill_tuples = scan_hashmapSize / partition_number
    print("spill_tuples: " + str(spill_tuples))
    hmap_size_base = mainMemory * 2**30 / (4 * hashmap_avg * 3)
    print("hmap_size_base: " + str(hmap_size_base))
    num_of_matches = 0
    for i in range(1, 8):
        hmsap_size = spill_tuples * i - num_of_matches + hmap_size_base
        num_of_matches += (
            num_same * partition_number * (hmsap_size * spill_tuples) / input_lines
        )
    print("num_of_matches: " + str(num_of_matches))
    merge_help_selectivity = (spill_tuples * 8 - num_of_matches) / (spill_tuples * 8)
    print("merge_help_selectivity: " + str(merge_help_selectivity))


def calc_merge_help_write_get_tuple():
    global merge_help_write_tuple, merge_help_get_tuple
    if worker_number > 0:
        merge_dur = scan_hashmapSize * 8 / (partition_number * merge_help_speed * 10**6)
        print("merge_dur: " + str(merge_dur))
        spill_freq = min(
            scanThreads / (worker_number * scan_time_per_spill), 8 / merge_dur
        )
        merge_help_out_tuples = (
            scan_hashmapSize * spill_freq * merge_help_selectivity / partition_number
        )
        print("merge_help_out_tuples: " + str(merge_help_out_tuples))
        merge_help_in_tuples = scan_hashmapSize * spill_freq / partition_number
        io_dur = (
            (merge_help_in_tuples + merge_help_out_tuples)  # download + upload size
        ) / bandwith
        io_dur /= 4
        print("io_dur: " + str(io_dur))
        merge_help_write_tuple = merge_help_out_tuples / (io_dur + merge_dur)
        merge_help_get_tuple = merge_help_in_tuples / (io_dur + merge_dur)
        print("merge_help_write_tuple: " + str(merge_help_write_tuple))
        print("merge_help_get_tuple: " + str(merge_help_get_tuple))


def calc_network_load_merge_help():
    global upload_network_load, download_network_load
    upload_network_load += merge_help_write_tuple * comp_fac * worker_number
    download_network_load += merge_help_get_tuple * comp_fac * worker_number


def calc_write_time_per_spill():
    global write_time_per_spill
    if bandwith != -1:
        spill_size = scan_hashmapSize * scanThreads * comp_fac
        if spill_mode == 1:
            write_time_per_spill = max(0, spill_size / bandwith - scan_time_per_spill)
        elif spill_mode == 2:
            write_time_per_spill = spill_size / bandwith


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
    print("spill file size: " + str(scan_hashmapSize * comp_fac / partition_number))
    merge_hashmapSize = int(thread_mem / hashmap_avg)


def calc_postScan_tuple_number():
    global postScan_tuple_number
    merge_help_tuples = (
        scan_dur * merge_help_write_tuple
    ) / merge_help_selectivity - scan_dur * merge_help_write_tuple
    merge_help_tuples *= worker_number
    print("merge_help_tuples: " + str(merge_help_tuples))
    postScan_tuple_number = max(
        post_scan_selectivity * input_lines - merge_help_tuples,
        input_lines * selectivity,
    )


def calc_merge_time_per_partition():
    global merge_time_per_partition
    tuples_per_part = postScan_tuple_number / partition_number
    hashmap_size_selec = merge_hashmapSize / merge_selectivity
    print("merge_selectivity:" + str(merge_selectivity))
    print("merge_hashmapSize:" + str(merge_hashmapSize))
    rescans = math.ceil(tuples_per_part / hashmap_size_selec)
    rescan_tuples = 0
    rest = max(0, tuples_per_part - hashmap_size_selec)
    print("hashmap_size_selec:" + str(hashmap_size_selec))
    print("tuples_per_part:" + str(tuples_per_part))
    for i in range(rescans):
        rescan_tuples += rest
        rest = max(0, rest - hashmap_size_selec)

    partition_file_number = spill_file_number / partition_number
    all_tupl_scans = tuples_per_part + rescan_tuples
    print("all_tupl_scans per partition:" + str(all_tupl_scans))
    file_get_number = all_tupl_scans * partition_file_number / tuples_per_part
    print("file_get_number per partition:" + str(file_get_number))
    print("get file dur per partition:" + str(file_get_number * file_get))
    merge_time_per_partition = file_get_number * file_get + all_tupl_scans / (
        merge_speed * 10**6
    )
    print("merge_time_per_partition:" + str(merge_time_per_partition))


def calc_merge_dur():
    global merge_dur
    merge_dur = merge_time_per_partition * math.ceil(partition_number / merge_threads)


calc_scan_hashmapSize()
calc_postScan_selectivity()
calc_scan_time_per_spill()
calc_network_load_spill()
calc_spill_file_number()
calc_write_time_per_spill()
calc_scan_dur()

calc_merge_help_selectivity()
calc_merge_help_write_get_tuple()
calc_network_load_merge_help()

calc_postScan_tuple_number()

calc_merge_selectivity()
calc_merge_hashmapSize()
calc_merge_time_per_partition()
calc_merge_dur()

print("post_scan_selectivity: " + str(post_scan_selectivity))
print("scan_dur: " + str(scan_dur))
print("merge_dur: " + str(merge_dur))
