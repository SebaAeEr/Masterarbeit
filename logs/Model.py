import math

worker_number = 1
input_lines = 3793363899  # input lines
selectivity = 0.323
scanThreads = 10
mainMemory = 6
hashmap_avg = 56  # avg size of hashmap entry
read_speed = 4  # mio lines / s
merge_speed = 6 # merge mio tuples /s
tuple_size = 1  # tuple length
comp_fac = 5 * tuple_size  # size of compressed tuple
partition_number = 30
merge_threads = 4
merge_subThreads = 4
buffer_fac = 8.53  # filesize * buf_fac = buf_size


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


def calc_scan_hashmapSize():
    global scan_hashmapSize
    thread_mem = (mainMemory - 0.5) * 2**30 / scanThreads
    scan_hashmapSize = int(thread_mem / hashmap_avg)


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


def calc_scan_dur():
    global scan_dur
    scan_dur = input_lines / read_speed * 10**6


def calc_network_load_spill():
    global scan_time_per_spill, upload_network_load
    spill_size = scan_hashmapSize * scanThreads * comp_fac
    print(scan_hashmapSize * comp_fac)
    print(spill_size)
    upload_network_load += spill_size / scan_time_per_spill


def calc_merge_selectivity():
    global merge_selectivity, post_scan_selectivity, merge_help_selectivity, selectivity
    merge_selectivity = selectivity / post_scan_selectivity


def calc_merge_hashmapSize():
    global merge_hashmapSize, merge_threads
    thread_mem = (
        ((mainMemory - 1) * 2**30)
        - scan_hashmapSize * comp_fac * merge_threads * buffer_fac
    ) / merge_threads
    merge_hashmapSize = int(thread_mem / hashmap_avg)


def calc_merge_time_per_partition():
    global merge_time_per_partition
    tuples_per_part = post_scan_selectivity * input_lines / partition_number
    hashmap_size_selec = merge_hashmapSize / merge_selectivity
    rescans = math.ceil(tuples_per_part / hashmap_size_selec)
    rescan_tuples = 0
    rest = tuples_per_part - hashmap_size_selec
    for i in range(rescans):
        rescan_tuples += rest
        rest = max(0, rest - hashmap_size_selec)

    all_tupl_scans = tuples_per_part + rescan_tuples


calc_scan_hashmapSize()
print(scan_hashmapSize)
calc_postScan_selectivity()
print(post_scan_selectivity)
calc_scan_time_per_spill()
print(scan_time_per_spill)
calc_network_load_spill()
print(upload_network_load)
calc_merge_hashmapSize()
print(merge_hashmapSize)
