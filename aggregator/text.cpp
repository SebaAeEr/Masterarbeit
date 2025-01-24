#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <iostream>
#include <random>
// #include <json/json.h>
#include <unordered_map>
#include <stdio.h>
#include <chrono>
// #include <ankerl/unordered_dense.h>
#include <hash_table8.hpp>
#include <thread>
#include <mutex>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectLegalHoldRequest.h>
#include <aws/s3/model/PutObjectLegalHoldRequest.h>
#include <bitset>
#include <cmath>
#include <time.h>
#include <atomic>
#include <future>
#include <mutex>
#include <shared_mutex>
/* #include "cloud/provider.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp" */

// Aggregation functions
enum Operation
{
    count,
    sum,
    exists,
    average
};
// file representation in Mana file
struct file
{
    // name of file
    std::string name;
    // size of file in B
    size_t size;
    // all subfiles with size in B and number of tuples
    std::vector<std::pair<size_t, size_t>> subfiles;
    // status of file 0: free, 255: merged, 1-254: worked on by worker with status as id
    unsigned char status;
};
// partition representation in Mana file
struct partition
{
    // partition number
    char id;
    // lock of partition
    bool lock;
    // files in partition
    std::vector<file> files;
};
// Worker representation in Mana file
struct manaFileWorker
{
    // Worker id
    char id;
    // Length in chars of Worker entry in Mana file
    int length;
    // lock of worker
    bool locked;
    // partitions of worker
    std::vector<partition> partitions;
};
// Mana file struct
struct manaFile
{
    // worker id that locks file
    char worker_lock;
    // thread id that locks file
    char thread_lock;
    // all workers in manaFile
    std::vector<manaFileWorker> workers;
};
// Log struct for a thread
struct threadLog
{
    std::unordered_map<std::string, size_t> sizes;
    std::vector<size_t> spillTimes;
};
// Log struct for a test
struct testLog
{
    bool success;
    int different_key_num;
    bool different_tuple_num;
    int tuple_outputFile;
    int tuple_testFile;
};
// Log struct
struct logFile
{
    std::unordered_map<std::string, size_t> sizes;
    std::vector<threadLog> threads;
    std::vector<size_t> get_lock_durs;
    std::vector<size_t> get_mana_durs;
    std::vector<size_t> write_mana_durs;
    std::vector<size_t> write_spill_durs;
    std::vector<std::pair<size_t, size_t>> writeCall_s3_file_durs;
    std::vector<std::pair<size_t, size_t>> getCall_s3_file_durs;
    std::vector<std::pair<size_t, size_t>> mergeHelp_merge_tuple_num;
    testLog test;
    bool failed;
    std::string err_msg;
};

// length of key- and value-array in Hashmap
static const int max_size = 2;
// coloumn names of the keys of the aggregation
std::string key_names[max_size];
// Aggregation function
enum Operation op;
// Name of coloumn of aggregation function
std::string opKeyName;
// Number of keys
int key_number;
// Number of values for aggregation function
int value_number;
// Id of worker
char worker_id;
// Mana file name in S3
std::string manag_file_name = "manag_file";
// length of pagesize in op system
long pagesize;
// bucketname of S3
std::string bucketName = "trinobucket2";
// Whether memory size of programm should be tracked
bool log_size;
// Whether times, configuration, etc. of programm should be tracked
bool log_time;
// unique log file name
std::string date_now;
// start time of programm iteration
std::chrono::_V2::system_clock::time_point start_time;
// Memory usage of mappings, variables and input streams from awssdk
unsigned long base_size = 1;
// Max number of threads
int threadNumber;
// S3 file name of the lock file
std::string lock_file_name = "lock";
// Max file size spilled to s3 in B
size_t max_s3_spill_size = 10000000;
// Memory used by bitmaps and input streams from awssdk
unsigned long extra_mem = 0;
// Size of spills in Background memory
unsigned long backMem_usage = 0;
// Whether spills should be decoded
bool deencode = true;
// Whether aggregation should also execute a mergePhase (legacy)
bool mergePhase = false;
// Whether more than 1 partitions should be set
bool set_partitions = true;
// Whether multiple get requests for the Mana file should be send after a timelimit
bool straggler_removal = false;
// Whether multiple partitions should be merged in parallel
bool multiThread_merge = true;
// Whether given files should be merged in parallel
bool multiThread_subMerge = true;
// Whether a file_queue should be used to minimize write requests to the Mana file
bool use_file_queue = true;
// Whether the mana file is split to distribute requests
bool split_mana = false;
// number of tuples in one spill file in a partition
float partition_size = 3000000.0;
// max GiB reserved for file mappings in scan
size_t mapping_max = 0.2 * (1ul << 30);
// Key values that are being tracked (debug)
std::vector<unsigned long> test_values = {};
// Number of partitions
int partitions = -1;
// log file
logFile log_file;
// Number of threads writing the Mana file
std::atomic<int> mana_writeThread_num(0);
// Local Worker lock for the Mana file
std::mutex local_mana_lock;
// Whether file format is Json or CSV
bool isJson = false;
// Write Lock for output file
std::mutex writing_ouput;
// Write Lock for file_queue
std::mutex file_queue_mutex;
// Whether another threads already tries to write the file_queue
std::atomic<bool> file_queue_status(true);
// File queue collecting files of all threads that should be added to the Mana file. It takes the file struct and the partition the should be written to in the Mana file.
std::unordered_map<char, std::vector<file>> file_queue;
// Min number of files in a partition for a Helper worker to merge the files
int minFileNumMergeHelper = 2;
// Number of Threads trying to get the Mana file
int getManaThreads_num = 0;
// Whether a ProgressBar should be shown
bool showProgressBar;

int mergeThreads_number = 1;

std::mutex partitions_set_lock;

bool dynamic_extension = false;

int static_partition_number = -1;

float thread_efficiency = 1;
std::mutex write_log_file_lock;

std::atomic<size_t> comb_spill_size = 0;
std::atomic<size_t> spillTuple_number = 0;

// hash function for an long array
auto hash = [](const std::array<unsigned long, max_size> a)
{
    std::size_t h = 0;
    for (int i = 0; i < max_size; i++)
    {
        h ^= std::hash<int>{}(a[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
};
// Comparison function of two long arrays
auto comp = [](const std::array<unsigned long, max_size> v1, const std::array<unsigned long, max_size> v2)
{
    for (int i = 0; i < max_size; i++)
    {
        if (v1[i] != v2[i])
        {
            return false;
        }
    }
    return true;
};
// Comparison function of two tuples representing a S3 file
struct CompareBySecond
{
    bool operator()(const std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>> &a, const std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>> &b) const
    {
        // First compare by second value (descending order)
        if (get<1>(a) != get<1>(b))
        {
            return get<1>(a) > get<1>(b);
        }
        // If second values are equal, compare by first value (lexicographically ascending)
        return get<1>(a) < get<1>(b);
    }
};

// parseLine of /proc/self/status file
size_t parseLine(char *line)
{
    // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}
/**
 *  @brief Get the program memory size
 * @return memory size of program
 * */
size_t getPhyValue()
{ // Note: this value is in KB!
    FILE *file = fopen("/proc/self/status", "r");
    int result = 0;
    char line[128];

    while (fgets(line, 128, file) != NULL)
    {
        if (strncmp(line, "VmRSS:", 6) == 0)
        {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}
/**
 * @brief Write a string into the mapping and return the number of chars written
 *
 * @param mapping pointer to char mapping
 * @param string string to written to mapping
 *
 * @return number of chars written to mapping
 */
int writeString(char *mapping, const std::string &string, size_t output_size = -1)
{
    int counter = 0;
    for (auto &it : string)
    {
        if (output_size != -1 && output_size <= counter)
        {
            std::cout << "wrong output size! " << counter << ">" << output_size << std::endl;
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
        else
        {
            mapping[counter] = it;
            counter++;
        }
    }
    return counter;
}

void writeSubLogFile(std::vector<size_t> vec, std::ofstream &output)
{
    int t_counter = 0;
    for (auto &it : vec)
    {
        t_counter++;
        output << it;
        if (t_counter < vec.size())
            output << ",";
    }
}

/**
 * @brief Write logFile struct to log file in JSON format
 *
 * @param log_t logFile to be written to file
 */
void writeLogFile(logFile log_t)
{
    std::ofstream output;
    output.open(("logfile_" + date_now + ".json").c_str());
    output << "{";
    output << "\"failed\":";
    output << log_t.failed;
    output << ",\"err_msg\":\"";
    output << log_t.err_msg;
    output << "\",";
    for (auto &it : log_t.sizes)
    {
        output << '"';
        output << it.first;
        output << "\":";
        output << it.second;
        output << ",\n";
    }
    output << "\"Threads\":[\n";
    int t_counter = 0;
    for (auto &it : log_t.threads)
    {
        t_counter++;
        output << "{";
        for (auto &itt : it.sizes)
        {
            output << '"';
            output << itt.first;
            output << "\":";
            output << itt.second;
            output << ",\n";
        }
        output << "\"spillTimes\":[\n";
        int counter = 0;
        for (auto &itt : it.spillTimes)
        {
            output << itt;
            counter++;
            if (counter < it.spillTimes.size())
                output << ",";
        }
        output << "]}";
        if (t_counter < log_t.threads.size())
            output << ",";
    }

    output << "],\n\"get_lock_dur\":[";
    writeSubLogFile(log_t.get_lock_durs, output);

    output << "],\n\"write_mana_dur\":[";
    writeSubLogFile(log_t.write_mana_durs, output);

    output << "],\n\"get_mana_dur\":[";
    writeSubLogFile(log_t.get_mana_durs, output);

    output << "],\n\"write_spill_durs\":[";
    writeSubLogFile(log_t.write_spill_durs, output);

    output << "],\n\"writeCall_s3_file_dur\":[";
    t_counter = 0;
    for (auto &it : log_t.writeCall_s3_file_durs)
    {
        t_counter++;
        output << it.first;
        if (t_counter < log_t.writeCall_s3_file_durs.size())
            output << ",";
    }
    output << "],\n\"writeCall_s3_file_size\":[";
    t_counter = 0;
    for (auto &it : log_t.writeCall_s3_file_durs)
    {
        t_counter++;
        output << it.second;
        if (t_counter < log_t.writeCall_s3_file_durs.size())
            output << ",";
    }
    output << "],\n\"getCall_s3_file_dur\":[";
    t_counter = 0;
    for (auto &it : log_t.getCall_s3_file_durs)
    {
        t_counter++;
        output << it.first;
        if (t_counter < log_t.getCall_s3_file_durs.size())
            output << ",";
    }

    output << "],\n\"getCall_s3_file_size\":[";
    t_counter = 0;
    for (auto &it : log_t.getCall_s3_file_durs)
    {
        t_counter++;
        output << it.second;
        if (t_counter < log_t.getCall_s3_file_durs.size())
            output << ",";
    }

    output << "],\n\"mergeHelp_merge_tuple_num_first\":[";
    t_counter = 0;
    for (auto &it : log_t.mergeHelp_merge_tuple_num)
    {
        t_counter++;
        output << it.first;
        if (t_counter < log_t.mergeHelp_merge_tuple_num.size())
            output << ",";
    }

    output << "],\n\"mergeHelp_merge_tuple_num_rest\":[";
    t_counter = 0;
    for (auto &it : log_t.mergeHelp_merge_tuple_num)
    {
        t_counter++;
        output << it.second;
        if (t_counter < log_t.mergeHelp_merge_tuple_num.size())
            output << ",";
    }

    output << "],\n\"test\":{\n\"success\":";
    output << log_t.test.success;
    output << ",\"different_keys_num\":";
    output << log_t.test.different_key_num;
    output << ",\"different_tuple_num\":";
    output << log_t.test.different_tuple_num;
    output << ",\"tuple_outputFile\":";
    output << log_t.test.tuple_outputFile;
    output << ",\"tuple_testFile\":";
    output << log_t.test.tuple_testFile;
    output << "}}";

    output.close();
}

void getPartitionCall(Aws::S3::S3Client *minio_client, std::shared_ptr<std::atomic<bool>> done, manaFile *return_value, bool *donedone, char worker_id, char partition_id)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    std::string key = manag_file_name;
    key += "_";
    key += worker_id;
    key += "_";
    key += std::to_string((int)(partition_id));
    request.SetKey(key);
    Aws::S3::Model::GetObjectOutcome outcome;

    while (true)
    {
        // request.SetVersionId(manag_version);
        outcome = minio_client->GetObject(request);

        // outcome.GetResult().SetObjectLockMode();
        if (!outcome.IsSuccess())
        {
            std::cout << "Error opening partition manag_file: " << outcome.GetError().GetMessage() << std::endl;
        }
        else
        {
            break;
        }
    }
    bool asdf = false;
    if (done->compare_exchange_strong(asdf, true))
    {
        auto &out_stream = outcome.GetResult().GetBody();
        partition partition;
        partition.id = out_stream.get();
        partition.lock = out_stream.get();
        while (out_stream.peek() != EOF)
        {
            file file;
            char temp = out_stream.get();
            std::string filename = "";
            while (temp != ',')
            {
                filename += temp;
                temp = out_stream.get();
            }
            file.name = filename;

            int number;
            char int_length_buf[sizeof(int)];
            out_stream.read(int_length_buf, sizeof(int));
            std::memcpy(&number, &int_length_buf, sizeof(int));

            size_t file_length;
            char length_buf[sizeof(size_t)];
            out_stream.read(length_buf, sizeof(size_t));
            std::memcpy(&file_length, &length_buf, sizeof(size_t));
            file.size = file_length;

            for (char i = 0; i < number; i++)
            {
                size_t sub_file_length;
                out_stream.read(length_buf, sizeof(size_t));
                std::memcpy(&sub_file_length, &length_buf, sizeof(size_t));
                size_t sub_file_tuples;
                out_stream.read(length_buf, sizeof(size_t));
                std::memcpy(&sub_file_tuples, &length_buf, sizeof(size_t));
                file.subfiles.push_back({sub_file_length, sub_file_tuples});
            }
            file.status = out_stream.get();
            partition.files.push_back(file);
        }
        manaFileWorker worker;
        worker.partitions.push_back(partition);
        return_value->workers.push_back(worker);
        *donedone = true;
    }

    // std::cout << "use count: " << done.use_count() << std::endl;
    done.reset();
    getManaThreads_num--;
    return;
}

void getWorkerCall(Aws::S3::S3Client *minio_client, std::shared_ptr<std::atomic<bool>> done, manaFile *return_value, bool *donedone, char worker_id)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    std::string key = manag_file_name;
    key += "_";
    key += worker_id;
    request.SetKey(key);
    Aws::S3::Model::GetObjectOutcome outcome;

    while (true)
    {
        // request.SetVersionId(manag_version);
        outcome = minio_client->GetObject(request);

        // outcome.GetResult().SetObjectLockMode();
        if (!outcome.IsSuccess())
        {
            std::cout << "Error opening worker manag_file: " << outcome.GetError().GetMessage() << std::endl;
        }
        else
        {
            break;
        }
    }
    std::shared_ptr<std::atomic<bool>> p_done = std::make_shared<std::atomic<bool>>(false);
    bool p_donedone = false;
    bool asdf = false;
    if (done->compare_exchange_strong(asdf, true))
    {
        // manaFile mana;
        auto &out_stream = outcome.GetResult().GetBody();
        manaFileWorker worker;

        while (out_stream.peek() != EOF)
        {
            partition part;
            part.id = out_stream.get();
            part.lock = out_stream.get() == 1;
            worker.partitions.push_back(part);
        }
        return_value->workers.push_back(worker);
        *donedone = true;
    }

    // std::cout << "use count: " << done.use_count() << std::endl;
    done.reset();
    getManaThreads_num--;
}

void getDistManaCall(Aws::S3::S3Client *minio_client, std::shared_ptr<std::atomic<bool>> done, manaFile *return_value, bool *donedone)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(manag_file_name);
    Aws::S3::Model::GetObjectOutcome outcome;

    while (true)
    {
        // request.SetVersionId(manag_version);
        outcome = minio_client->GetObject(request);

        // outcome.GetResult().SetObjectLockMode();
        if (!outcome.IsSuccess())
        {
            std::cout << "Error opening dist manag_file: " << outcome.GetError().GetMessage() << std::endl;
        }
        else
        {
            break;
        }
    }
    std::shared_ptr<std::atomic<bool>> w_done = std::make_shared<std::atomic<bool>>(false);
    bool w_donedone = false;
    bool asdf = false;
    if (done->compare_exchange_strong(asdf, true))
    {
        // manaFile mana;
        auto &out_stream = outcome.GetResult().GetBody();
        return_value->worker_lock = out_stream.get();
        return_value->thread_lock = out_stream.get();

        while (out_stream.peek() != EOF)
        {
            manaFileWorker worker;
            char workerid = out_stream.get();
            worker.id = workerid;
            worker.locked = out_stream.get() == 1;
            return_value->workers.push_back(worker);
        }
        *donedone = true;
    }

    // std::cout << "use count: " << done.use_count() << std::endl;
    done.reset();
    getManaThreads_num--;
}

/**
 * @brief Request call to get the Mana file
 *
 * @param minio_client pointer to the minio_client
 * @param done shared pointer to atomic bool which is set to true when function received the Mana file and writes it to return_value
 * @param return_value pointer to manaFile where received ManaFile is written to
 * @param  donedone pointer to bool which is set to true if function is finished
 */
void getManaCall(Aws::S3::S3Client *minio_client, std::shared_ptr<std::atomic<bool>> done, manaFile *return_value, bool *donedone)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(manag_file_name);
    Aws::S3::Model::GetObjectOutcome outcome;

    while (true)
    {
        // request.SetVersionId(manag_version);
        outcome = minio_client->GetObject(request);
        // outcome.GetResult().SetObjectLockMode();
        if (!outcome.IsSuccess())
        {
            std::cout << "Error opening manag_file: " << outcome.GetError().GetMessage() << std::endl;
        }
        else
        {
            break;
        }
    }
    bool asdf = false;
    if (done->compare_exchange_strong(asdf, true))
    {
        // manaFile mana;

        auto &out_stream = outcome.GetResult().GetBody();

        return_value->worker_lock = out_stream.get();
        return_value->thread_lock = out_stream.get();
        return_value->workers = {};
        while (out_stream.peek() != EOF)
        {
            manaFileWorker worker;

            char workerid = out_stream.get();
            worker.id = workerid;
            worker.locked = out_stream.get() == 1;
            std::vector<partition> partitions = {};
            int length;
            char length_buf[sizeof(int)];
            out_stream.read(length_buf, sizeof(int));
            std::memcpy(&length, &length_buf, sizeof(int));
            worker.length = length;
            int head = 0;
            while (head < length)
            {
                partition part;
                part.id = out_stream.get();
                part.lock = out_stream.get() == 1;
                int part_length;
                char part_length_buf[sizeof(int)];
                out_stream.read(part_length_buf, sizeof(int));
                std::memcpy(&part_length, &part_length_buf, sizeof(int));

                std::vector<file> files = {};
                for (int k = 0; k < part_length; k++)
                {
                    file file;
                    char temp = out_stream.get();
                    std::string filename = "";
                    while (temp != ',')
                    {
                        filename += temp;
                        temp = out_stream.get();
                    }
                    file.name = filename;

                    int number;
                    out_stream.read(part_length_buf, sizeof(int));
                    std::memcpy(&number, &part_length_buf, sizeof(int));

                    size_t file_length;
                    char length_buf[sizeof(size_t)];
                    out_stream.read(length_buf, sizeof(size_t));
                    std::memcpy(&file_length, &length_buf, sizeof(size_t));
                    file.size = file_length;

                    for (char i = 0; i < number; i++)
                    {
                        size_t sub_file_length;
                        out_stream.read(length_buf, sizeof(size_t));
                        std::memcpy(&sub_file_length, &length_buf, sizeof(size_t));
                        size_t sub_file_tuples;
                        out_stream.read(length_buf, sizeof(size_t));
                        std::memcpy(&sub_file_tuples, &length_buf, sizeof(size_t));
                        file.subfiles.push_back({sub_file_length, sub_file_tuples});
                    }
                    file.status = out_stream.get();
                    files.push_back(file);
                    head += sizeof(size_t) * number * 2 + sizeof(size_t) + filename.size() + 2 + sizeof(int);
                }
                part.files = files;
                partitions.push_back(part);
                head += sizeof(int) + 2;
            }
            worker.partitions = partitions;
            return_value->workers.push_back(worker);
        }
        *donedone = true;
    }

    // std::cout << "use count: " << done.use_count() << std::endl;
    done.reset();
    getManaThreads_num--;
    return;
}

/**
 * @brief Get manaFile struct from S3
 *
 * @param minio_client pointer to aws client
 *
 * @return manaFile struct
 */
manaFile getMana(Aws::S3::S3Client *minio_client, char worker_id = -1, char partition_id = -1)
{
    auto get_start_time = std::chrono::high_resolution_clock::now();
    std::shared_ptr<std::atomic<bool>> done = std::make_shared<std::atomic<bool>>(false);
    // done->exchange(1);
    manaFile mana;
    mana.worker_lock = -1;
    bool donedone = false;
    // std::cout << "get mana" << std::endl;
    if (straggler_removal)
    {
        std::vector<std::thread> threads;
        while (!done->load())
        {
            getManaThreads_num++;
            auto thread_get_start_time = std::chrono::high_resolution_clock::now();
            if (split_mana)
            {
                if (partition_id != -1)
                {
                    threads.push_back(std::thread(getPartitionCall, minio_client, done, &mana, &donedone, worker_id, partition_id));
                }
                else if (worker_id != -1)
                {
                    threads.push_back(std::thread(getWorkerCall, minio_client, done, &mana, &donedone, worker_id));
                }
                else
                {
                    threads.push_back(std::thread(getDistManaCall, minio_client, done, &mana, &donedone));
                }
            }
            else
            {
                threads.push_back(std::thread(getManaCall, minio_client, done, &mana, &donedone));
            }
            size_t duration = 0;
            while (duration < 65000)
            {
                if (done->load())
                {
                    // std::cout << "size " << threads.size() << std::endl;
                    for (auto &thread : threads)
                    {
                        thread.detach();
                    }
                    break;
                }
                duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - thread_get_start_time).count());
            }
        }
        while (!donedone)
        {
            usleep(10);
        }
    }
    else
    {
        if (split_mana)
        {
            if (partition_id != -1)
            {
                getPartitionCall(minio_client, done, &mana, &donedone, worker_id, partition_id);
            }
            else if (worker_id != -1)
            {
                getWorkerCall(minio_client, done, &mana, &donedone, worker_id);
            }
            else
            {
                getDistManaCall(minio_client, done, &mana, &donedone);
            }
        }
        else
        {
            getManaCall(minio_client, done, &mana, &donedone);
        }
    }
    done.reset();
    write_log_file_lock.lock();
    log_file.get_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - get_start_time).count());
    write_log_file_lock.unlock();
    // std::cout << "got mana" << std::endl;
    if (mana.workers.size() == 0)
    {
        std::cout << "mana worker size == 0!" << std::endl;
    }
    return mana;
}

bool writeManaPartition(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock, char worker_id, char partition_id)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();

    while (true)
    {
        Aws::S3::Model::PutObjectRequest in_request;
        in_request.SetBucket(bucketName);
        std::string key = manag_file_name;
        key += "_";
        key += worker_id;
        key += "_";
        key += std::to_string((int)(partition_id));
        std::cout << "key: " << key << std::endl;
        in_request.SetKey(key);
        const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
        size_t in_mem_size = 2;
        partition partition;

        for (auto &w : mana.workers)
        {
            if (w.id == worker_id)
            {
                for (auto &p : w.partitions)
                {
                    if (p.id == partition_id)
                    {
                        partition = p;
                        break;
                    }
                }
                break;
            }
        }

        *in_stream << partition.id;
        char locked = partition.lock ? 1 : 0;
        *in_stream << locked;

        for (auto &file : partition.files)
        {
            in_mem_size += file.name.length() + 2 + sizeof(int) + sizeof(size_t) + sizeof(size_t) * file.subfiles.size() * 2;
            char int_buf[sizeof(int)];
            *in_stream << file.name;
            *in_stream << ',';
            int l_temp = file.subfiles.size();
            std::memcpy(int_buf, &l_temp, sizeof(int));
            //*in_stream << int_buf;
            for (int i = 0; i < sizeof(int); i++)
            {
                *in_stream << int_buf[i];
            }
            char file_length_buf[sizeof(size_t)];
            std::memcpy(file_length_buf, &file.size, sizeof(size_t));
            //*in_stream << file_length_buf;
            for (int i = 0; i < sizeof(size_t); i++)
            {
                *in_stream << file_length_buf[i];
            }
            for (auto &sub_file : file.subfiles)
            {
                std::memcpy(file_length_buf, &sub_file.first, sizeof(size_t));
                // *in_stream << file_length_buf;
                for (int i = 0; i < sizeof(size_t); i++)
                {
                    *in_stream << file_length_buf[i];
                }
                std::memcpy(file_length_buf, &sub_file.second, sizeof(size_t));
                //*in_stream << file_length_buf;
                for (int i = 0; i < sizeof(size_t); i++)
                {
                    *in_stream << file_length_buf[i];
                }
            }
            // std::cout << "write Mana: " << std::bitset<8>(file.status) << " name: " << file.name << std::endl;
            *in_stream << file.status;
        }
        in_request.SetBody(in_stream);
        in_request.SetContentLength(in_mem_size);
        // in_request.SetWriteOffsetBytes(1000);
        // std::cout << "writing mana" << std::endl;
        auto in_outcome = minio_client->PutObject(in_request);
        if (!in_outcome.IsSuccess())
        {
            std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
        }
        else
        {
            // std::cout << "mana written" << std::endl;
            if (freeLock)
            {
                Aws::S3::Model::DeleteObjectRequest delete_request;
                delete_request.WithKey(lock_file_name + "_" + worker_id + "_" + partition_id).WithBucket(bucketName);
                auto outcome = minio_client->DeleteObject(delete_request);
                if (!outcome.IsSuccess())
                {
                    std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                    return false;
                }
            }
            write_log_file_lock.lock();
            log_file.write_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count());
            write_log_file_lock.unlock();
            return 1;
        }
    }
}

bool writeManaWorker(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock, char worker_id)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    while (true)
    {
        Aws::S3::Model::PutObjectRequest in_request;
        in_request.SetBucket(bucketName);
        std::string key = manag_file_name;
        key += "_";
        key += worker_id;
        in_request.SetKey(key);
        const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
        manaFileWorker worker;
        size_t in_mem_size = 0;
        for (auto &w : mana.workers)
        {
            if (w.id == worker_id)
            {
                worker = w;
                break;
            }
        }
        for (auto &partition : worker.partitions)
        {
            in_mem_size += 2;
            *in_stream << partition.id;
            char locked = partition.lock ? 1 : 0;
            *in_stream << locked;
        }

        in_request.SetBody(in_stream);
        in_request.SetContentLength(in_mem_size);
        // in_request.SetWriteOffsetBytes(1000);
        // std::cout << "writing mana" << std::endl;
        auto in_outcome = minio_client->PutObject(in_request);
        if (!in_outcome.IsSuccess())
        {
            std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
        }
        else
        {
            // std::cout << "mana written" << std::endl;
            if (freeLock)
            {
                Aws::S3::Model::DeleteObjectRequest delete_request;
                delete_request.WithKey(lock_file_name + "_" + worker_id).WithBucket(bucketName);
                auto outcome = minio_client->DeleteObject(delete_request);
                if (!outcome.IsSuccess())
                {
                    std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                    return false;
                }
            }
            write_log_file_lock.lock();
            log_file.write_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count());
            write_log_file_lock.unlock();
            return 1;
        }
    }
}

bool writeDistMana(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    while (true)
    {
        Aws::S3::Model::PutObjectRequest in_request;
        in_request.SetBucket(bucketName);
        in_request.SetKey(manag_file_name);
        const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
        size_t in_mem_size = 2;
        if (freeLock)
        {
            char free = 0;
            *in_stream << free;
            *in_stream << free;
        }
        else
        {
            *in_stream << mana.worker_lock;
            *in_stream << mana.thread_lock;
        }
        for (auto &worker : mana.workers)
        {
            in_mem_size += 2;
            *in_stream << worker.id;
            char locked = worker.locked ? 1 : 0;
            *in_stream << locked;
        }
        in_request.SetBody(in_stream);
        in_request.SetContentLength(in_mem_size);
        // in_request.SetWriteOffsetBytes(1000);
        auto in_outcome = minio_client->PutObject(in_request);
        if (!in_outcome.IsSuccess())
        {
            std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
        }
        else
        {
            // std::cout << "mana written" << std::endl;
            if (freeLock)
            {
                Aws::S3::Model::DeleteObjectRequest delete_request;
                delete_request.WithKey(lock_file_name).WithBucket(bucketName);
                auto outcome = minio_client->DeleteObject(delete_request);
                if (!outcome.IsSuccess())
                {
                    std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                    return false;
                }
            }
            write_log_file_lock.lock();
            log_file.write_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count());
            write_log_file_lock.unlock();
            return 1;
        }
    }
}

/**
 * @brief Write manaFile struct to the Mana file in S3
 *
 * @param minio_client pointer to aws client
 * @param mana manaFile struct written to Mana file
 * @param freeLock whether locks for Mana file should be unlocked
 *
 * @return success
 */
bool writeMana(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock, char worker_id = -1, char partition_id = -1)
{
    if (split_mana)
    {
        if (partition_id != -1)
        {
            std::cout << "Writing mana partition" << std::endl;
            return writeManaPartition(minio_client, mana, freeLock, worker_id, partition_id);
        }
        else if (worker_id != -1)
        {
            return writeManaWorker(minio_client, mana, freeLock, worker_id);
        }
        else
        {
            return writeDistMana(minio_client, mana, freeLock);
        }
    }
    auto write_start_time = std::chrono::high_resolution_clock::now();
    // std::cout << "writing mana" << std::endl;
    while (true)
    {
        Aws::S3::Model::PutObjectRequest in_request;
        in_request.SetBucket(bucketName);
        in_request.SetKey(manag_file_name);
        const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
        size_t in_mem_size = 2;
        if (freeLock)
        {
            char free = 0;
            *in_stream << free;
            *in_stream << free;
        }
        else
        {
            *in_stream << mana.worker_lock;
            *in_stream << mana.thread_lock;
        }
        for (auto &worker : mana.workers)
        {
            in_mem_size += worker.length + sizeof(int) + 2;
            *in_stream << worker.id;
            char locked = worker.locked ? 1 : 0;
            *in_stream << locked;
            char length_buf[sizeof(int)];
            std::memcpy(length_buf, &worker.length, sizeof(int));
            for (int i = 0; i < sizeof(int); i++)
            {
                *in_stream << length_buf[i];
            }
            for (auto &partition : worker.partitions)
            {
                *in_stream << partition.id;
                locked = partition.lock ? 1 : 0;
                *in_stream << locked;
                char int_buf[sizeof(int)];
                int l_temp = partition.files.size();
                std::memcpy(int_buf, &l_temp, sizeof(int));
                //*in_stream << int_buf;
                for (int i = 0; i < sizeof(int); i++)
                {
                    *in_stream << int_buf[i];
                }

                for (auto &file : partition.files)
                {
                    *in_stream << file.name;
                    *in_stream << ',';
                    l_temp = file.subfiles.size();
                    std::memcpy(int_buf, &l_temp, sizeof(int));
                    //*in_stream << int_buf;
                    for (int i = 0; i < sizeof(int); i++)
                    {
                        *in_stream << int_buf[i];
                    }
                    char file_length_buf[sizeof(size_t)];
                    std::memcpy(file_length_buf, &file.size, sizeof(size_t));
                    //*in_stream << file_length_buf;
                    for (int i = 0; i < sizeof(size_t); i++)
                    {
                        *in_stream << file_length_buf[i];
                    }
                    for (auto &sub_file : file.subfiles)
                    {
                        std::memcpy(file_length_buf, &sub_file.first, sizeof(size_t));
                        // *in_stream << file_length_buf;
                        for (int i = 0; i < sizeof(size_t); i++)
                        {
                            *in_stream << file_length_buf[i];
                        }
                        std::memcpy(file_length_buf, &sub_file.second, sizeof(size_t));
                        //*in_stream << file_length_buf;
                        for (int i = 0; i < sizeof(size_t); i++)
                        {
                            *in_stream << file_length_buf[i];
                        }
                    }
                    // std::cout << "write Mana: " << std::bitset<8>(file.status) << " name: " << file.name << std::endl;
                    *in_stream << file.status;
                }
            }
        }
        in_request.SetBody(in_stream);
        in_request.SetContentLength(in_mem_size);
        // in_request.SetWriteOffsetBytes(1000);
        // std::cout << "writing mana" << std::endl;
        auto in_outcome = minio_client->PutObject(in_request);

        if (!in_outcome.IsSuccess())
        {
            std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
        }
        else
        {
            // std::cout << "mana written" << std::endl;
            if (freeLock)
            {
                //  std::cout << "freeing lock" << std::endl;
                Aws::S3::Model::DeleteObjectRequest delete_request;
                delete_request.WithKey(lock_file_name).WithBucket(bucketName);
                auto outcome = minio_client->DeleteObject(delete_request);
                if (!outcome.IsSuccess())
                {
                    std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                    return false;
                }
                // local_mana_lock.exchange(false);
                local_mana_lock.unlock();
                // std::cout << "unlocking" << std::endl;
            }
            write_log_file_lock.lock();
            log_file.write_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count());
            write_log_file_lock.unlock();
            //  std::cout << "mana written" << std::endl;
            return 1;
        }
    }
}

/**
 * @brief Write the lock file if possible
 *
 * @param minio_client pointer to aws client
 *
 * @return success (fail if lock file already exists)
 */
bool writeLock(Aws::S3::S3Client *minio_client, char worker_id = -1, char partition_id = -1)
{
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    if (split_mana)
    {
        if (partition_id != -1)
        {
            request.SetKey(lock_file_name + "_" + worker_id + "_" + partition_id);
        }
        else if (worker_id != -1)
        {
            request.SetKey(lock_file_name + "_" + worker_id);
        }
        else
        {
            request.SetKey(lock_file_name);
        }
    }
    else
    {
        request.SetKey(lock_file_name);
    }
    // Calc spill size
    const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
    *in_stream << "locked";
    request.SetContentLength(6);
    request.SetBody(in_stream);
    request.SetIfNoneMatch("*");

    auto outcome = minio_client->PutObject(request);
    if (!outcome.IsSuccess())
    {
        // std::cout << "Error: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    else
    {
        return true;
    }
}

/**
 * @brief get write lock for Mana file and receive manaFile
 *
 * @param minio_client pointer to aws client
 * @param thread_id thread id of function caller
 *
 * @return manaFile struct
 */
manaFile getLockedMana(Aws::S3::S3Client *minio_client, char thread_id, char worker_id = -1, char partition_id = -1)
{
    auto lock_start_time = std::chrono::high_resolution_clock::now();
    // std::lock_guard<std::mutex> lock(local_mana_lock);
    if (!split_mana)
    {
        local_mana_lock.lock();
    }
    // std::cout << "Trying to get lock thread: " << thread_id << std::endl;
    while (true)
    {
        if (writeLock(minio_client, worker_id, partition_id))
        {
            manaFile mana = getMana(minio_client, worker_id, partition_id);
            // std::cout << (int)(thread_id) << ": got lock" << std::endl;
            /* if (mana.worker_lock == 0)
            {
                // std::cout << "Trying to get lock: " << std::to_string((int)(thread_id)) << std::endl;
                // manaFile mana = getMana(minio_client);
                mana.worker_lock = worker_id;
                mana.thread_lock = thread_id;
                writeMana(minio_client, mana, false);
                mana = getMana(minio_client, worker_id, partition_id);
                // std::cout << "Lock received by: " << std::to_string((int)(thread_id)) << " old thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
                if (mana.worker_lock == worker_id && mana.thread_lock == thread_id)
                { */
            // mana = getMana(minio_client);
            //  std::cout << " new thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
            // std::cout << "locking" << std::endl;
            write_log_file_lock.lock();
            log_file.get_lock_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - lock_start_time).count());
            write_log_file_lock.unlock();
            return mana;
        }
        usleep(500000);
    }
}

/**
 * @brief initialize awssdk
 */
Aws::S3::S3Client init()
{
    Aws::Client::ClientConfiguration c_config;
    c_config.verifySSL = false;
    c_config.region = "us-west-1";
    c_config.scheme = Aws::Http::Scheme::HTTP;
    c_config.endpointOverride = "131.159.16.208:9000";
    c_config.requestTimeoutMs = 500000;
    c_config.connectTimeoutMs = 1000;
    Aws::Auth::AWSCredentials cred("erasmus", "tumThesis123");
    Aws::S3::S3Client minio_client = Aws::S3::S3Client(cred, c_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    return minio_client;
}

/**
 * @brief print Mana file from S3
 *
 * @param minio_client pointer to aws client
 */
void printMana(Aws::S3::S3Client *minio_client)
{
    std::cout << "getting mana" << std::endl;
    manaFile mana = getMana(minio_client);
    std::cout << "got mana: " << mana.workers.size() << std::endl;
    if (split_mana)
    {
        std::cout << "split mana" << std::endl;
        for (auto &w : mana.workers)
        {
            std::cout << "getting worker mana" << std::endl;
            manaFile mana_worker = getMana(minio_client, w.id);
            std::cout << "got worker mana: " << mana_worker.workers[0].partitions.size() << std::endl;
            w.partitions = mana_worker.workers[0].partitions;
            for (auto &p : w.partitions)
            {
                std::cout << "getting partition mana" << std::endl;
                manaFile mana_partition = getMana(minio_client, w.id, p.id);
                std::cout << "got partition mana: " << mana_partition.workers[0].partitions[0].files.size() << std::endl;
                p.files = mana_partition.workers[0].partitions[0].files;
            }
        }
    }
    std::string status = mana.worker_lock == 0 ? "free" : std::to_string(mana.worker_lock);
    std::cout << "worker lock: " << status << ", thread lock: " << std::bitset<8>(mana.thread_lock) << std::endl;
    for (auto &worker : mana.workers)
    {
        std::cout << "Worker id: " << worker.id << " locked: " << worker.locked << std::endl;
        for (auto &partition : worker.partitions)
        {
            std::cout << "  Partition: " << (int)(partition.id) << ", locked: " << partition.lock << std::endl;
            for (auto &file : partition.files)
            {
                std::cout << "    " << file.name << ": size: " << file.size << " worked on by: " << std::bitset<8>(file.status) << " subfiles:" << std::endl;
                for (auto &sub_files : file.subfiles)
                {
                    std::cout << "      size: " << sub_files.first << " #tuples: " << sub_files.second << std::endl;
                }
            }
        }
    }
}

/**
 * @brief Reset Mana file if worker id = 1 (Main worker) or add worker id if id != 1 (Helper)
 *
 * @param minio_client pointer to aws client
 */
void initManagFile(Aws::S3::S3Client *minio_client)
{
    manaFile mana;
    if (worker_id != '1')
    {
        mana = getLockedMana(minio_client, 0);
    }
    manaFileWorker worker;
    worker.id = worker_id;
    worker.length = 0;
    worker.partitions = {};
    worker.locked = false;
    mana.workers.push_back(worker);
    mana.thread_lock = 0;
    mana.worker_lock = 0;
    manaFile mana_worker;
    mana_worker.workers.push_back(worker);
    writeMana(minio_client, mana_worker, true, worker_id);
    writeMana(minio_client, mana, true);

    // printMana(minio_client);
}
/**
 * @brief print a progress bar
 *
 * @param progress fill ration of progress bar
 */
void printProgressBar(float progress)
{
    if (showProgressBar)
    {
        int barWidth = 70;
        std::cout << "[";
        int pos = barWidth * progress;
        for (int i = 0; i < barWidth; ++i)
        {
            if (i < pos)
                std::cout << "=";
            else if (i == pos)
                std::cout << ">";
            else
                std::cout << " ";
        }
        std::cout << "] " << int(progress * 100.0) << " %\r";
        std::cout.flush();
    }
}

/**
 * @brief delete all files in S3 that have an entry in the Mana file
 *
 *  @param minio_client pointer to aws client
 */
void cleanup(Aws::S3::S3Client *minio_client)
{
    if (split_mana)
    {
        for (char p = 0; p < partitions; p++)
        {
            manaFile mana = getMana(minio_client, worker_id, p);
            for (auto &file : mana.workers[0].partitions[0].files)
            {
                size_t counter = 0;
                for (auto &sub_file : file.subfiles)
                {
                    Aws::S3::Model::DeleteObjectRequest request;
                    request.WithKey(file.name + "_" + std::to_string(counter)).WithBucket(bucketName);
                    while (true)
                    {
                        auto outcome = minio_client->DeleteObject(request);
                        if (!outcome.IsSuccess())
                        {
                            std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
    else
    {
        manaFile mana = getMana(minio_client);
        for (auto &w : mana.workers)
        {
            if (w.id == worker_id)
            {
                for (auto &p : w.partitions)
                {
                    for (auto &file : p.files)
                    {
                        size_t counter = 0;
                        for (auto &sub_file : file.subfiles)
                        {
                            Aws::S3::Model::DeleteObjectRequest request;
                            request.WithKey(file.name + "_" + std::to_string(counter)).WithBucket(bucketName);
                            while (true)
                            {
                                auto outcome = minio_client->DeleteObject(request);
                                if (!outcome.IsSuccess())
                                {
                                    std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/**
 * @brief set the number of partitions
 *
 * @param comb_hash_size size of all hashmaps in memory
 */
void setPartitionNumber(size_t comb_hash_size)
{
    if (set_partitions)
    {
        if (static_partition_number == -1)
        {
            partitions = ceil(comb_hash_size / partition_size);
        }
        else
        {
            partitions = static_partition_number;
        }
        // partitions = 2;
        std::cout << "Set partition number to: " << partitions << std::endl;
    }
    else
    {
        partitions = 1;
    }
    write_log_file_lock.lock();
    log_file.sizes["partitionNumber"] = partitions;
    write_log_file_lock.unlock();
}
/**
 * @brief Add a file entry in the Mana file
 *
 * @param minio_client pointer to aws client
 * @param files vector of files to be added to Mana file. pair of file struct and partition id of file
 * @param write_to_id worker id the files should be written to
 * @param thread_id thread id of function caller
 */
void addFileToManag(Aws::S3::S3Client *minio_client, std::vector<std::pair<file, char>> files, char write_to_id, char thread_id)
{
    std::unordered_map<char, std::vector<file>> *files_temp;
    std::unordered_map<char, std::vector<file>> local_files_temp;
    if (use_file_queue)
    {
        file_queue_mutex.lock();
        for (auto &f : files)
        {
            file_queue[f.second].push_back(f.first);
        }
        file_queue_mutex.unlock();
        files_temp = &file_queue;
    }
    else
    {
        for (auto &f : files)
        {
            local_files_temp[f.second].push_back(f.first);
        }
        files_temp = &local_files_temp;
    }
    bool open = true;
    if (!use_file_queue || file_queue_status.compare_exchange_strong(open, false))
    {
        if (!split_mana)
        {
            manaFile mana = getLockedMana(minio_client, thread_id);
            if (use_file_queue)
            {
                file_queue_mutex.lock();
            }
            bool partition_locked;
            for (auto &file : *files_temp)
            {
                bool parition_found = false;
                for (auto &worker : mana.workers)
                {
                    if (worker.id == write_to_id)
                    {
                        if (worker.locked)
                        {
                            writeMana(minio_client, mana, true);
                            mana_writeThread_num.fetch_sub(1);
                            if (use_file_queue)
                            {
                                file_queue.clear();
                                file_queue_status.exchange(true);
                                file_queue_mutex.unlock();
                            }
                            return;
                        }
                        for (auto &partition : worker.partitions)
                        {
                            if (partition.id == file.first)
                            {
                                parition_found = true;
                                if (!partition.lock)
                                {
                                    for (auto &f : file.second)
                                    {
                                        partition.files.push_back(f);
                                    }
                                    partition_locked = false;
                                }
                                else
                                {
                                    partition_locked = true;
                                }
                                break;
                            }
                        }
                        if (!parition_found)
                        {
                            partition partition;
                            partition.id = file.first;
                            partition.lock = false;
                            for (auto &f : file.second)
                            {
                                partition.files.push_back(f);
                            }
                            worker.partitions.push_back(partition);
                            worker.length += 2 + sizeof(int);
                            partition_locked = false;
                        }
                        if (!partition_locked)
                        {
                            for (auto &f : file.second)
                            {
                                worker.length += f.name.size() + 2 + sizeof(int) + sizeof(size_t) + sizeof(size_t) * f.subfiles.size() * 2;
                            }
                        }
                        break;
                    }
                }
            }
            if (use_file_queue)
            {
                file_queue.clear();
                file_queue_status.exchange(true);
                file_queue_mutex.unlock();
            }
            writeMana(minio_client, mana, true);
        }
        else
        {
            for (auto &file : *files_temp)
            {
                manaFile mana_partition = getLockedMana(minio_client, thread_id, write_to_id, file.first);
                for (auto &f : file.second)
                {
                    mana_partition.workers[0].partitions[0].files.push_back(f);
                }
                writeMana(minio_client, mana_partition, true, write_to_id, file.first);
            }
        }
        mana_writeThread_num.fetch_sub(1);
    }
    else
    {
        mana_writeThread_num.fetch_sub(1);
    }
    //  std::cout << "finished Adding file" << std::endl;
    return;
}
/**
 * @brief Get all filenames of a partition that are free or being worked on
 *
 * @param minio_client pointer to aws client
 * @param partition_id id of the partition
 * @param files pointer to set of files the result will be written to
 */
void getAllMergeFileNames(Aws::S3::S3Client *minio_client, char partition_id, std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *files)
{
    if (split_mana)
    {
        if (partition_id == -1)
        {
            for (int i = 0; i < partitions; i++)
            {
                manaFile mana_partition = getMana(minio_client, worker_id, i);
                for (auto &file : mana_partition.workers[0].partitions[0].files)
                {
                    if (file.status != 255)
                    {
                        files->insert({file.name, file.size, file.subfiles});
                    }
                }
            }
        }
        manaFile mana_partition = getMana(minio_client, worker_id, partition_id);
        for (auto &file : mana_partition.workers[0].partitions[0].files)
        {
            if (file.status != 255)
            {
                files->insert({file.name, file.size, file.subfiles});
            }
        }
    }
    else
    {
        manaFile mana = getMana(minio_client);
        for (auto &worker : mana.workers)
        {
            if (worker.id == worker_id)
            {
                for (auto &partition : worker.partitions)
                {
                    if (partition.id == partition_id || partition_id == -1)
                    {
                        for (auto &file : partition.files)
                        {
                            if (file.status != 255)
                            {
                                files->insert({file.name, file.size, file.subfiles});
                            }
                        }
                    }
                }
            }
        }
    }
}
/**
 * @brief Get file structs (if not given worker_id and partition_id) of files that worker can merge (only called from mergeHelper)
 *
 * @param minio_client pointer to aws client
 * @param beggarWorker worker id that should be helped. If 0 beggarWorker is set by function
 * @param partition_id partition that should be helped. If beggarWorker is 0, partition is set by function
 * @param blacklist pointer to vector of filenames that should be ignored
 * @param res pointer to tuple with vector of file structs, the beggarWorker and partition_id where results of function are written to
 * @param thread_id Thread id the function is called by
 * @param min_file_num Min number of files the function should return. If not possible 0 will be returned
 */
void getMergeFileName(Aws::S3::S3Client *minio_client, char beggarWorker, char partition_id, std::vector<std::string> *blacklist, std::tuple<std::vector<file>, char, char> *res, char thread_id, char min_file_num)
{
    // std::cout << "getting file name" << std::endl;
    char given_beggarWorker = beggarWorker;
    file m_file;
    get<0>(*res).clear();
    manaFile mana = getLockedMana(minio_client, thread_id);
    std::cout << "Getting beggar Worker: " << beggarWorker << std::endl;
    // printMana(minio_client);
    //  If no beggarWorker is yet selected choose the worker with the largest spill
    if (beggarWorker == 0)
    {
        // std::cout << "finding partition" << std::endl;
        size_t partition_max = 0;
        size_t biggest_file = 0;
        for (auto &worker : mana.workers)
        {
            for (auto &partition : worker.partitions)
            {
                if (!partition.lock)
                {
                    size_t partition_size_temp = 0;
                    int file_number = 0;
                    size_t b_file = 0;
                    for (auto &file : partition.files)
                    {
                        if (file.status == 0)
                        {
                            file_number++;
                            if (!std::count(blacklist->begin(), blacklist->end(), file.name))
                            {
                                partition_size_temp += file.size;
                                if (b_file < file.size)
                                {
                                    b_file = file.size;
                                }
                            }
                        }
                    }
                    // if (partition_max < partition_size_temp && file_number > 3)
                    if (b_file > biggest_file && file_number >= minFileNumMergeHelper)
                    {
                        partition_max = partition_size_temp;
                        partition_id = partition.id;
                        beggarWorker = worker.id;
                        biggest_file = b_file;
                    }
                }
            }
        }
    }
    if (beggarWorker == 0)
    {
        writeMana(minio_client, mana, true);
        get<1>(*res) = 0;
        return;
    }
    std::cout << "Got beggar Worker: " << beggarWorker << " Getting files" << std::endl;

    // std::cout << "finding files" << std::endl;
    char file_num = threadNumber * 2;
    std::vector<file> res_files(0);
    for (auto &worker : mana.workers)
    {
        if (worker.id == beggarWorker)
        {
            if (worker.locked)
            {
                beggarWorker = 0;
                break;
            }
            for (auto &partition : worker.partitions)
            {
                if (partition.id == partition_id)
                {
                    if (partition.lock)
                    {
                        writeMana(minio_client, mana, true);
                        get<1>(*res) = 0;
                        return;
                    }
                    while (res_files.size() < file_num)
                    {
                        size_t max = 0;
                        file biggest_file;
                        for (auto &file : partition.files)
                        {
                            // std::cout << "File status: " << (int)(file.status) << " size: " << file.size << " name: " << file.name << std::endl;
                            if (file.status == 0 && !std::count(blacklist->begin(), blacklist->end(), file.name))
                            {
                                bool found = false;
                                for (auto &f : res_files)
                                {
                                    if (f.name == file.name)
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found && file.size > max)
                                {
                                    max = file.size;
                                    biggest_file = file;
                                }
                            }
                        }
                        if (max > 0)
                        {
                            // std::cout << "found file name: " << biggest_file.name << std::endl;
                            res_files.push_back(biggest_file);
                        }
                        else
                        {
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }
    if (res_files.size() < min_file_num)
    {
        writeMana(minio_client, mana, true);
        get<1>(*res) = 0;
        // std::cout << "setting beggar to 0" << std::endl;
        return;
    }
    std::cout << "setting file stati" << std::endl;
    /* std::cout << "res_files: ";
    for (auto temp : res_files)
    {
        std::cout << temp.name << ", ";
    }
    std::cout << std::endl; */
    // std::cout << "writing status" << std::endl;

    for (auto &worker : mana.workers)
    {
        if (worker.id == beggarWorker)
        {
            for (auto &partition : worker.partitions)
            {
                if (partition.id == partition_id)
                {
                    for (auto &file : partition.files)
                    {
                        for (auto &b_file : res_files)
                        {
                            if (file.name == b_file.name)
                            {
                                file.status = worker_id;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    std::cout << "creating result" << std::endl;
    get<1>(*res) = beggarWorker;
    get<2>(*res) = partition_id;
    // get<0>(*res) = res_files;
    for (auto &f : res_files)
    {
        get<0>(*res).push_back(f);
    }

    // std::cout << "unlocking Mana" << std::endl;
    writeMana(minio_client, mana, true);
    return;
}

size_t calc_outputSize(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap)
{
    size_t output_size = 0;
    // Calc the output size for hmap.
    for (auto &it : *hmap)
    {
        for (int i = 0; i < key_number; i++)
        {
            output_size += std::to_string(it.first[i]).length();
        }
        if (op != exists)
        {
            if (op != average)
            {
                output_size += std::to_string(it.second[0]).length();
            }
            else
            {
                output_size += std::to_string(it.second[0] / (float)(it.second[1])).length();
            }
        }
    }
    if (isJson)
    {
        for (int i = 0; i < key_number; i++)
        {
            output_size += ("\"" + key_names[i] + "\":").length() * hmap->size();
        }

        // unsigned long output_size_test = strlen(("\"custkey\":,\"_col1\":}").c_str())
        output_size += (strlen("\"_col1\":") + 5 + key_number) * hmap->size();
        if (op != exists)
        {
            output_size += (strlen("\"_col1\":") + 5 + key_number) * hmap->size();
        }
    }
    else
    {
        // ""\n
        output_size += hmap->size() * key_number * 3;
        // ,
        output_size += hmap->size() * (key_number - 1);
        if (op != exists)
        {
            output_size += hmap->size() * 3;
        }
    }
    return output_size;
}

/**
 * @brief Write hashmap hmap into output file.
 *
 * @param hmap pointer to hashmap
 * @param start start point in outputfile
 * @param free_mem available memory for function
 * @param outputfilename output file name
 *
 * @return new size of output file
 */
void writeHashmap(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, unsigned long *output_size, unsigned long free_mem, std::string &outputfilename)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    size_t added_size = calc_outputSize(hmap);
    writing_ouput.lock();
    unsigned long start = *output_size;
    *output_size += added_size;

    // std::cout << "calc output size: " << output_size << " added_size: " << added_size << std::endl;
    int file = open(outputfilename.c_str(), O_RDWR | O_CREAT, 0777);

    // Extend file file.
    lseek(file, (*output_size) - 1, SEEK_SET);
    if (write(file, "", 1) == -1)
    {
        close(file);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }
    writing_ouput.unlock();

    size_t start_diff = start % pagesize;
    size_t start_page = start - start_diff;

    // Map file with given size.
    char *mappedoutputFile = static_cast<char *>(mmap(nullptr, added_size + start_diff, PROT_WRITE | PROT_READ, MAP_SHARED, file, start_page));
    madvise(mappedoutputFile, added_size + start_diff, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (mappedoutputFile == MAP_FAILED)
    {

        std::cout << "Start: " << added_size << " + " << start_diff << " start_page: " << start_page << " is fd valid? " << fcntl(file, F_GETFD) << std::endl;
        perror("Error mmapping the file in write Hashmap");
        close(file);
        exit(EXIT_FAILURE);
    }
    unsigned long freed_mem = 0;
    unsigned long counter = start;
    // Write into file through mapping. Starting at the given start point.
    unsigned long mapped_count = start_diff;
    unsigned long head = 0;
    for (auto &it : *hmap)
    {
        /*  if (counter < 20)
         {
             std::cout << it.first[0] << ":" << it.second[0] << std::endl;
             counter++;
         } */
        if (isJson)
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], "{");
            for (int k = 0; k < key_number; k++)
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
                mapped_count += writeString(&mappedoutputFile[mapped_count], key_names[k]);
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\":");
                mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.first[k]));
                if (k + 1 < key_number)
                {
                    // temp_line += ",";
                    mapped_count += writeString(&mappedoutputFile[mapped_count], ",");
                }
            }
            mapped_count += writeString(&mappedoutputFile[mapped_count], ",\"_col1\":");
            if (op != average)
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0]));
            }
            else
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0] / (float)(it.second[1])));
            }
            mapped_count += writeString(&mappedoutputFile[mapped_count], "},");
            mapped_count += writeString(&mappedoutputFile[mapped_count], "\n");
        }
        else
        {
            for (int k = 0; k < key_number; k++)
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
                mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.first[k]));
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
                if (k < key_number - 1)
                {
                    mapped_count += writeString(&mappedoutputFile[mapped_count], ",");
                }
            }
            if (op != exists)
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], ",");
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
                if (op != average)
                {
                    mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0]));
                }
                else
                {
                    mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0] / (float)(it.second[1])));
                }
                mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
            }
            mapped_count += writeString(&mappedoutputFile[mapped_count], "\n");
        }
        unsigned long used_space = (mapped_count - head);
        if (used_space >= free_mem && used_space > pagesize)
        {
            unsigned long freed_space = used_space - (used_space % pagesize);
            munmap(&mappedoutputFile[head], freed_space);
            head += freed_space;
            freed_mem += freed_space;
        }
    }
    // free mapping and return the size of output of hmap.
    if (munmap(&mappedoutputFile[head], mapped_count - head) == -1)
    {
        perror("Could not free memory in writeHashmap 2!");
    }
    /* if (ftruncate(file, mapped_count + start_page) == -1)
    {
        close(file);
        perror("Error truncation file in write Hashmap");
        exit(EXIT_FAILURE);
    } */
    freed_mem += mapped_count - head;
    // std::cout << "Real output file size: " << mapped_count - start_diff << std::endl;
    write_log_file_lock.lock();
    log_file.sizes["write_output_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count();
    write_log_file_lock.unlock();

    close(file);
}

/**
 * @brief return hash of key array
 *
 * @param key key array
 *
 * @return hash value
 */
int getPartition(std::array<unsigned long, max_size> key)
{
    auto h = hash(key);
    return h % partitions;
}
/**
 * @brief read a csv line
 *
 * @param mapping pointer to mapping the line should read of
 * @param start index of line start in mapping
 * @param keys array of coloumn names
 * @param lineObject pointer to hashmap where the values are written to
 * @param limit size of file that is read
 *
 * @return new index for next line
 */
unsigned long parseCSV(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    unsigned long i = start;
    while (true)
    {
        if (i + 1 > limit)
        {
            return ULONG_MAX;
        }
        // start reading a line when {
        if (mapping[i] == '\n' && mapping[i + 1] == '\"')
        {
            i += 2;
            int readingMode = 0;
            while (true)
            {
                std::string key = keys[readingMode];
                char char_temp = mapping[i];
                (*lineObjects)[key] = "";
                while (char_temp != '"' && char_temp != '\n')
                {
                    if (char_temp == '.' || isdigit(char_temp))
                    {
                        (*lineObjects)[key] += char_temp;
                    }
                    i++;
                    char_temp = mapping[i];
                }
                if (i + 1 < limit && mapping[i + 1] != '\n')
                {
                    readingMode++;
                    i += 3;
                }
                else
                {
                    if (std::find(std::begin(test_values), std::end(test_values), std::stol((*lineObjects)[keys[0]])) != std::end(test_values))
                    {
                        unsigned long temp = start;
                        std::cout << "CSV line: ";
                        while (temp < i)
                        {
                            std::cout << mapping[temp];
                            temp++;
                        }
                        std::cout << std::endl;
                    }
                    return i;
                }
            }
        }
        else
        {
            i++;
        }
    }
}
/**
 * @brief read a json line
 *
 * @param mapping pointer to mapping the line should read of
 * @param start index of line start in mapping
 * @param keys array of coloumn names
 * @param lineObject pointer to hashmap where the values are written to
 * @param limit size of file that is read
 *
 * @return new index for next line
 */
unsigned long parseJson(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    unsigned long i = start;
    while (true)
    {
        if (i > limit)
        {
            return ULONG_MAX;
        }
        // start reading a line when {
        if (mapping[i] == '{')
        {
            int readingMode = 0;
            while (true)
            {
                std::string key = keys[readingMode];
                i += key.length() + 4;
                char char_temp = mapping[i];
                (*lineObjects)[key] = "";
                while (char_temp != ',' && char_temp != '}')
                {
                    if (char_temp == '.' || isdigit(char_temp))
                    {
                        (*lineObjects)[key] += char_temp;
                    }
                    i++;
                    char_temp = mapping[i];
                }
                if (char_temp != '}')
                {
                    readingMode++;
                }
                else
                {
                    if (std::find(std::begin(test_values), std::end(test_values), std::stol((*lineObjects)[keys[0]])) != std::end(test_values))
                    {
                        unsigned long temp = start;
                        std::cout << "Json line: ";
                        while (temp < i)
                        {
                            std::cout << mapping[temp];
                            temp++;
                        }
                        std::cout << std::endl;
                    }
                    return i;
                }
            }
        }
        else
        {
            i++;
        }
    }
}
/**
 * @brief read a line of the input file
 *
 * @param mapping pointer to mapping the line should read of
 * @param start index of line start in mapping
 * @param keys array of coloumn names
 * @param lineObject pointer to hashmap where the values are written to
 * @param limit size of file that is read
 *
 * @return new index for next line
 */
unsigned long readTuple(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    if (isJson)
    {
        return parseJson(mapping, start, keys, lineObjects, limit);
    }
    else
    {
        return parseCSV(mapping, start, keys, lineObjects, limit);
    }
}
/**
 * @brief Spill the hashmap to a local file encoded
 *
 * @param hmap pointer to hashmap
 * @param spill_file vector of files the hashmap is written to. Pair of filename and size of file (filename expected)
 * @param id thread id of caller
 * @param free_mem available memory for function
 */
void spillToFileEncoded(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap,
                        std::vector<std::pair<std::string, size_t>> *spill_file, char id, size_t free_mem)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(long) * (key_number + value_number);
    std::vector<int> file_handlers(partitions);
    for (int i = 0; i < partitions; i++)
    {
        file_handlers[i] = open((*spill_file)[i].first.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
        if (file_handlers[i] == -1)
        {
            // std::cout << "Tying to open file " << (fileName + "_" + std::to_string(i)) << std::endl;
            perror("Error opening file for writing");
            exit(EXIT_FAILURE);
        }
    }

    // std::cout << "extending files" << std::endl;
    // extend file
    for (int i = 0; i < partitions; i++)
    {
        // std::cout << "extending file " << (fileName + "_" + std::to_string(i)) << " by " << (*spill_file)[i].second + spill_mem_size - 1 << std::endl;
        lseek(file_handlers[i], (*spill_file)[i].second + spill_mem_size - 1, SEEK_SET);
        if (write(file_handlers[i], "", 1) == -1)
        {
            close(file_handlers[i]);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
    }
    std::vector<char *> spills;

    for (int i = 0; i < partitions; i++)
    {
        spills.push_back((char *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handlers[i], 0)));
        madvise(spills[i], spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);
        if (spills[i] == MAP_FAILED)
        {
            std::cout << "size: " << spill_mem_size << " is fd valid? " << fcntl(file_handlers[i], F_GETFD) << std::endl;
            close(file_handlers[i]);
            perror("Error mmapping the file while writing encdoded");
            exit(EXIT_FAILURE);
        }
    }
    // Write int to Mapping
    std::vector<unsigned long> counters = std::vector<unsigned long>(partitions, 0);
    std::vector<unsigned long> writeheads = std::vector<unsigned long>(partitions, 0);
    // std::cout << "writing " << fileName << std::endl;

    for (auto &it : *hmap)
    {
        int partition = getPartition(it.first);
        char *spill = spills[partition];
        unsigned long &counter = counters[partition];
        unsigned long &writehead = writeheads[partition];
        for (int i = 0; i < key_number; i++)
        {
            char l_bytes = it.first[i] == 0 ? 0 : (static_cast<int>(log2(it.first[i])) + 8) / 8;
            spill[counter] = l_bytes;
            counter++;

            char byteArray[sizeof(long)];
            std::memcpy(byteArray, &it.first[i], sizeof(long));
            for (int k = 0; k < l_bytes; k++)
            {
                spill[counter] = byteArray[k];
                counter++;
            }
            /* std::memcpy(&spill[counter], &it.first[i], l_bytes);
            counter += l_bytes; */
        }
        for (int i = 0; i < value_number; i++)
        {
            char l_bytes = it.second[i] == 0 ? 0 : (static_cast<int>(log2(it.second[i])) + 8) / 8;
            spill[counter] = l_bytes;
            counter++;

            char byteArray[sizeof(long)];
            std::memcpy(byteArray, &it.second[i], sizeof(long));
            for (int k = 0; k < l_bytes; k++)
            {
                spill[counter] = byteArray[k];
                counter++;
            }
            /* std::memcpy(&spill[counter], &it.second[i], l_bytes);
            counter += l_bytes; */
        }

        if ((counter - writehead) >= free_mem && (counter - writehead) > pagesize)
        {
            unsigned long used_space = (counter - writehead);
            unsigned long freed_space = used_space - (used_space % pagesize);
            munmap(&spill[writehead], freed_space);
            writehead += freed_space;
        }
    }
    // std::cout << "freeing up mapping " << fileName << std::endl;

    for (int i = 0; i < partitions; i++)
    {
        munmap(&spills[i][writeheads[i]], spill_mem_size - writeheads[i]);
        (*spill_file)[i].second += counters[i];
        if (ftruncate(file_handlers[i], counters[i]) == -1)
        {
            close(file_handlers[i]);
            perror("Error truncation file");
            exit(EXIT_FAILURE);
        }
        comb_spill_size.fetch_add(counters[i]);
        close(file_handlers[i]);
    }

    // Cleanup: clear hashmap and free rest of mapping space
    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}
/**
 * @brief Spill the hashmap to a local file
 *
 * @param hmap pointer to hashmap
 * @param spill_file vector of files the hashmap is written to. Pair of filename and size of file (filename expected)
 * @param id thread id of caller
 * @param free_mem available memory for function
 */
void spillToFile(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<std::string, size_t>> *spill_file,
                 char id, size_t free_mem)
{
    if (deencode)
    {
        spillToFileEncoded(hmap, spill_file, id, free_mem);
        return;
    }
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(long) * (key_number + value_number);
    std::vector<int> file_handlers(partitions);

    for (int i = 0; i < partitions; i++)
    {
        file_handlers[i] = open((*spill_file)[i].first.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    }

    // extend file
    for (int i = 0; i < partitions; i++)
    {
        lseek(file_handlers[i], (*spill_file)[i].second + spill_mem_size - 1, SEEK_SET);
        if (write(file_handlers[i], "", 1) == -1)
        {
            close(file_handlers[i]);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
    }
    std::vector<unsigned long *> spills;

    for (int i = 0; i < partitions; i++)
    {
        spills.push_back((unsigned long *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handlers[i], 0)));
        madvise(spills[i], spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);
        if (spills[i] == MAP_FAILED)
        {
            close(file_handlers[i]);
            std::cout << "size: " << spill_mem_size << " is fd valid? " << fcntl(file_handlers[i], F_GETFD) << std::endl;
            perror("Error mmapping the file while spilling to file");
            exit(EXIT_FAILURE);
        }
    }
    // Create mapping to file

    /* size_t start_diff = spill_file->second % pagesize;
    size_t start = spill_file->second - start_diff;

    // Create mapping to file
    unsigned long *spill = (unsigned long *)(mmap(nullptr, spill_mem_size + start_diff, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file->first, start));
    madvise(spill, spill_mem_size + start_diff, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (spill == MAP_FAILED)
    {
        close(spill_file->first);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    } */

    // Write int to Mapping
    std::vector<unsigned long> counters = std::vector<unsigned long>(partitions, 0);
    std::vector<unsigned long> writeheads = std::vector<unsigned long>(partitions, 0);
    // unsigned long counter = start_diff / sizeof(long);
    // unsigned long writehead = 0;
    for (auto &it : *hmap)
    {
        int partition = getPartition(it.first);
        unsigned long *spill = spills[partition];
        unsigned long &counter = counters[partition];
        unsigned long &writehead = writeheads[partition];
        for (int i = 0; i < key_number; i++)
        {
            spill[counter] = it.first[i];
            counter++;
        }
        for (int i = 0; i < value_number; i++)
        {
            spill[counter] = it.second[i];
            counter++;
        }

        if ((counter - writehead) * sizeof(long) >= free_mem && (counter - writehead) * sizeof(long) > pagesize)
        {
            unsigned long used_space = (counter - writehead) * sizeof(long);
            unsigned long freed_space = used_space - (used_space % pagesize);
            munmap(&spill[writehead], freed_space);
            writehead += freed_space / sizeof(long);
        }
    }

    // Cleanup: clear hashmap and free rest of mapping space
    for (int i = 0; i < partitions; i++)
    {
        munmap(&spills[i][writeheads[i]], spill_mem_size - writeheads[i] * sizeof(long));
        (*spill_file)[i].second += (counters[i] - 1) * sizeof(long);
        comb_spill_size.fetch_add((counters[i] - 1) * sizeof(long));
        if (ftruncate(file_handlers[i], (counters[i] - 1) * sizeof(long)) == -1)
        {
            close(file_handlers[i]);
            perror("Error truncation file");
            exit(EXIT_FAILURE);
        }
        close(file_handlers[i]);
    }

    // std::cout << "spilled to file" << std::endl;

    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}
/**
 * @brief Execute the PutRequest
 *
 * @param minio_client pointer to aws client
 * @param request pointer to the PutRequest
 * @param name name of file (error message)
 * @param size size of file (error message)
 * @param done pointer to bool that is set to true if function is finished
 */
void writeS3FileCall(Aws::S3::S3Client *minio_client, Aws::S3::Model::PutObjectRequest *request, std::string &name, size_t size, bool *done)
{
    while (true)
    {
        auto outcome = minio_client->PutObject(*request);

        if (!outcome.IsSuccess())
        {
            std::cout << "Error writing " << name << ": " << outcome.GetError().GetMessage() << " Spill size: " << size << std::endl;
        }
        else
        {
            break;
        }
    }
    *done = true;
}
/**
 * @brief Write a io stream to S3
 *
 * @param minio_client pointer to aws client
 * @param body io stream
 * @param size size of stream
 * @param name name of file
 */
void writeS3File(Aws::S3::S3Client *minio_client, const std::shared_ptr<Aws::IOStream> body, size_t size, std::string &name)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(name);
    request.SetBody(body);
    request.SetContentLength(size);
    bool done;
    writeS3FileCall(minio_client, &request, name, size, &done);
    write_log_file_lock.lock();
    log_file.writeCall_s3_file_durs.push_back({std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count(), size});
    write_log_file_lock.unlock();
}
/**
 * @brief Write a hashmap to S3
 *
 * @param hmap pointer to hashmap
 * @param minio_client pointer to aws client
 * @param sizes pointer to vector of vectors(partitions) of subfiles (pair of size in B and tuple number) of each subfile
 * @param uniqueName name of S3 file (without partition number)
 * @param start_counter pointer to vector containing the number of subfiles per file
 * @param partition_id optional partition id if all hashmap entries are in one partition
 */
void spillS3Hmap(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, Aws::S3::S3Client *minio_client,
                 std::vector<std::vector<std::pair<size_t, size_t>>> *sizes, std::string uniqueName, std::vector<int> *start_counter, char partition_id = -1)
{
    std::vector<int> counter = std::vector<int>(partitions, 0);
    std::vector<std::string> n;
    for (int i = 0; i < partitions; i++)
    {
        n.push_back(uniqueName + "_" + std::to_string(i));
    }

    std::vector<std::shared_ptr<Aws::IOStream>> in_streams;
    for (int i = 0; i < partitions; i++)
    {
        in_streams.push_back(Aws::MakeShared<Aws::StringStream>(""));
    }

    std::vector<unsigned long> temp_counter = std::vector<unsigned long>(partitions, 0);
    std::vector<size_t> spill_mem_size_temp = std::vector<size_t>(partitions, 0);
    char byteArray[sizeof(long)];
    for (auto &it : *hmap)
    {
        int partition;
        if (partition_id != -1)
        {
            partition = partition_id;
        }
        else
        {
            partition = getPartition(it.first);
        }

        if (temp_counter[partition] == max_s3_spill_size)
        {
            if (!deencode)
            {
                spill_mem_size_temp[partition] = temp_counter[partition] * sizeof(long) * (key_number + value_number);
            }
            std::string temp_n = n[partition] + "_" + std::to_string((*start_counter)[partition]);
            // std::cout << "Spill to file: " << temp_n << " size: " << spill_mem_size_temp[partition] << " #tuple: " << temp_counter[partition] << std::endl;
            writeS3File(minio_client, in_streams[partition], spill_mem_size_temp[partition], temp_n);
            counter[partition]++;
            (*start_counter)[partition]++;
            in_streams[partition] = Aws::MakeShared<Aws::StringStream>("");
            (*sizes)[partition].push_back({spill_mem_size_temp[partition], temp_counter[partition]});
            spill_mem_size_temp[partition] = 0;
            temp_counter[partition] = 0;
        }
        temp_counter[partition]++;
        if (deencode)
        {

            for (int i = 0; i < key_number; i++)
            {
                char l_bytes = it.first[i] == 0 ? 0 : (static_cast<int>(log2(it.first[i])) + 8) / 8;
                *in_streams[partition] << l_bytes;

                std::memcpy(byteArray, &it.first[i], sizeof(long));
                //*in_streams[partition] << byteArray;
                for (int k = 0; k < l_bytes; k++)
                {
                    *in_streams[partition] << byteArray[k];
                }
                spill_mem_size_temp[partition] += l_bytes + 1;
            }
            for (int i = 0; i < value_number; i++)
            {
                char l_bytes = it.second[i] == 0 ? 0 : (static_cast<int>(log2(it.second[i])) + 8) / 8;
                *in_streams[partition] << l_bytes;

                std::memcpy(byteArray, &it.second[i], sizeof(long));
                //*in_streams[partition] << byteArray;
                for (int k = 0; k < l_bytes; k++)
                {
                    *in_streams[partition] << byteArray[k];
                }
                spill_mem_size_temp[partition] += l_bytes + 1;
            }
        }
        else
        {
            for (int i = 0; i < key_number; i++)
            {
                // std::cout << it.first[i];
                std::memcpy(byteArray, &it.first[i], sizeof(long));
                //*in_streams[partition] << byteArray;
                for (int k = 0; k < sizeof(long); k++)
                {
                    *in_streams[partition] << byteArray[k];
                }
            }
            for (int i = 0; i < value_number; i++)
            {
                std::memcpy(byteArray, &it.second[i], sizeof(long));
                //*in_streams[partition] << byteArray;
                for (int k = 0; k < sizeof(long); k++)
                    *in_streams[partition] << byteArray[k];
            }
        }
    }
    if (partition_id == -1)
    {
        for (int i = 0; i < partitions; i++)
        {
            if (temp_counter[i] > 0)
            {
                if (!deencode)
                {
                    spill_mem_size_temp[i] = temp_counter[i] * sizeof(long) * (key_number + value_number);
                }

                std::string n_temp = n[i] + "_" + std::to_string((*start_counter)[i]);
                // std::cout << "Spill last to file: " << n_temp << " size: " << spill_mem_size_temp[i] << " #tuple: " << temp_counter[i] << std::endl;
                writeS3File(minio_client, in_streams[i], spill_mem_size_temp[i], n_temp);
                (*sizes)[i].push_back({spill_mem_size_temp[i], temp_counter[i]});
                (*start_counter)[i]++;
            }
        }
    }
    else if (temp_counter[partition_id] > 0)
    {

        if (!deencode)
        {
            spill_mem_size_temp[partition_id] = temp_counter[partition_id] * sizeof(long) * (key_number + value_number);
        }
        std::string n_temp = n[partition_id] + "_" + std::to_string((*start_counter)[partition_id]);
        // std::cout << "Spill last to file: " << n_temp << " size: " << spill_mem_size_temp[i] << " #tuple: " << temp_counter[i] << std::endl;
        writeS3File(minio_client, in_streams[partition_id], spill_mem_size_temp[partition_id], n_temp);
        (*sizes)[partition_id].push_back({spill_mem_size_temp[partition_id], temp_counter[partition_id]});
        (*start_counter)[partition_id]++;
    }
    return;
}
/**
 * @brief Write a local file to S3 encoded
 *
 * @param spill_file local file (pair of filename, size of file)
 * @param minio_client pointer to aws client
 * @param sizes pointer to vector of subfiles (pair of size in B and tuple number)
 * @param uniqueName name of S3 file (without subfilenumber)
 * @param start_counter pointer to vector containing the number of subfiles per file
 */
void spillS3FileEncoded(std::pair<std::string, size_t> spill_file, Aws::S3::S3Client *minio_client, std::vector<std::pair<size_t, size_t>> *sizes, std::string uniqueName, int *start_counter)
{
    size_t spill_mem_size = spill_file.second;
    int file_handler = open(spill_file.first.c_str(), O_RDWR, 0777);
    char *spill_map = static_cast<char *>(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handler, 0));
    if (spill_map == MAP_FAILED)
    {
        close(file_handler);
        std::cout << "size: " << spill_mem_size << " is fd valid? " << fcntl(file_handler, F_GETFD) << std::endl;
        perror("Error mmapping the file while spilling from file to s3 encoded");
        exit(EXIT_FAILURE);
    }
    madvise(spill_map, spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);

    int counter = 0;
    std::string n;

    std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
    unsigned long temp_counter = 0;
    unsigned long i_head = 0;
    size_t spill_mem_size_temp = 0;
    std::vector<char> char_longs;
    unsigned long i = 0;

    // Write int to Mapping
    while (i < spill_mem_size)
    {
        if (temp_counter == max_s3_spill_size)
        {
            n = uniqueName + "_" + std::to_string(*start_counter);
            (*start_counter)++;
            // std::cout << "writing: " << n << " i: " << i << " spill_mem_size: " << spill_mem_size << " spill_mem_size_temp: " << spill_mem_size_temp << std::endl;
            writeS3File(minio_client, in_stream, spill_mem_size_temp, n);
            sizes->push_back({spill_mem_size_temp, temp_counter});
            counter++;
            in_stream = Aws::MakeShared<Aws::StringStream>("");
            temp_counter = 0;
            spill_mem_size_temp = 0;
        }
        temp_counter++;
        char l_bytes = spill_map[i];
        *in_stream << l_bytes;
        spill_mem_size_temp += l_bytes + 1;

        while (l_bytes > 0)
        {
            i++;
            l_bytes--;
            *in_stream << spill_map[i];
        }

        unsigned long i_diff = i - i_head;
        if (i_diff > pagesize * 10)
        {
            unsigned long freed_space_temp = i_diff - (i_diff % pagesize);
            if (munmap(&spill_map[i_head], freed_space_temp) == -1)
            {
                std::cout << "head: " << i_head << " freed_space_temp: " << freed_space_temp << std::endl;
                perror("Could not free memory!");
            }
            i_head += freed_space_temp;
        }
        i++;
    }
    if (munmap(&spill_map[i_head], spill_mem_size - i_head) == -1)
    {
        std::cout << "head: " << i_head << " freed_space_temp: " << spill_mem_size - i_head << std::endl;
        perror("Could not free memory!");
    }
    n = uniqueName + "_" + std::to_string(*start_counter);
    // std::cout << "writing: " << n << " size: " << spill_mem_size_temp << std::endl;
    (*start_counter)++;
    writeS3File(minio_client, in_stream, spill_mem_size_temp, n);
    sizes->push_back({spill_mem_size_temp, temp_counter});
    close(file_handler);
}
/**
 * @brief Write a local file to S3
 *
 * @param spill_file local file (pair of filename, size of file)
 * @param minio_client pointer to aws client
 * @param sizes pointer to vector of subfiles (pair of size in B and tuple number)
 * @param uniqueName name of S3 file (without subfilenumber)
 * @param start_counter pointer to vector containing the number of subfiles per file
 */
void spillS3File(std::pair<std::string, size_t> spill_file, Aws::S3::S3Client *minio_client, std::vector<std::pair<size_t, size_t>> *sizes, std::string uniqueName, int *start_counter)
{
    size_t spill_mem_size = spill_file.second;
    int file_handler = open(spill_file.first.c_str(), O_RDWR, 0777);
    unsigned long *spill_map = static_cast<unsigned long *>(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handler, 0));
    if (spill_map == MAP_FAILED)
    {
        close(file_handler);
        std::cout << "size: " << spill_mem_size << " is fd valid? " << fcntl(file_handler, F_GETFD) << std::endl;
        perror("Error mmapping the file while spilling from file to s3 not encoded");
        exit(EXIT_FAILURE);
    }
    madvise(spill_map, spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);

    int counter = 0;
    std::string n;

    std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
    unsigned long temp_counter = 0;
    unsigned long i_head = 0;
    size_t spill_mem_size_temp = 0;
    std::vector<char> char_longs;

    // Write int to Mapping
    for (unsigned long i = 0; i < spill_mem_size / sizeof(long); i++)
    {
        if (temp_counter == max_s3_spill_size)
        {
            spill_mem_size_temp = temp_counter * sizeof(long) * (key_number + value_number);
            n = uniqueName + "_" + std::to_string(*start_counter);
            (*start_counter)++;
            // std::cout << "writing: " << n << " i: " << i << " spill_mem_size: " << spill_mem_size << " spill_mem_size_temp: " << spill_mem_size_temp << std::endl;
            writeS3File(minio_client, in_stream, spill_mem_size_temp, n);
            sizes->push_back({spill_mem_size_temp, temp_counter});
            counter++;
            in_stream = Aws::MakeShared<Aws::StringStream>("");
            temp_counter = 0;
        }
        if (i % (key_number + value_number) == 0)
        {
            temp_counter++;
        }

        char byteArray[sizeof(long)];
        std::memcpy(byteArray, &spill_map[i], sizeof(long));

        for (int k = 0; k < sizeof(long); k++)
        {
            *in_stream << byteArray[k];
        }

        unsigned long i_diff = (i - i_head) * sizeof(long);
        if (i_diff > pagesize * 10)
        {

            unsigned long freed_space_temp = i_diff - (i_diff % pagesize);
            if (munmap(&spill_map[i_head], freed_space_temp) == -1)
            {
                std::cout << "head: " << i_head << " freed_space_temp: " << freed_space_temp << std::endl;
                perror("Could not free memory!");
            }
            i_head += freed_space_temp / sizeof(long);
        }
    }
    if (munmap(&spill_map[i_head], spill_mem_size - i_head * sizeof(long)) == -1)
    {
        std::cout << "head: " << i_head << " freed_space_temp: " << spill_mem_size - i_head * sizeof(long) << std::endl;
        perror("Could not free memory!");
    }
    n = uniqueName + "_" + std::to_string(*start_counter);
    // std::cout << "writing: " << n << std::endl;
    (*start_counter)++;
    spill_mem_size_temp = temp_counter * sizeof(long) * (key_number + value_number);
    writeS3File(minio_client, in_stream, spill_mem_size_temp, n);
    sizes->push_back({spill_mem_size_temp, temp_counter});
    close(file_handler);
}
/**
 * @brief Spill hashmap or local file to S3
 *
 * @param hmap pointer to hashmap
 * @param spill_file vector of local files (pair of filename, size of file) (if empty hashmap is spilled)
 * @param uniqueName name of S3 file (without subfilenumber)
 * @param minio_client pointer to aws client
 * @param write_to_id worker id the files belong to
 * @param fileStatus status of file in Mana file
 * @param thread_id Thread id of caller
 */
void spillToMinio(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<std::string, size_t>> spill_file, std::string uniqueName,
                  Aws::S3::S3Client *minio_client, char write_to_id, unsigned char fileStatus, char thread_id)
{
    int counter = 0;
    std::vector<std::vector<std::pair<size_t, size_t>>> sizes(partitions);

    std::string n;
    std::vector<int> start_vector = std::vector<int>(partitions, 0);

    // Calc spill size

    if (spill_file[0].first == "")
    {
        spillS3Hmap(hmap, minio_client, &sizes, uniqueName, &start_vector);
    }
    else
    {
        // std::cout << "spilling to s3 from file" << std::endl;
        for (int i = 0; i < partitions; i++)
        {
            counter = 0;
            if (deencode)
            {
                spillS3FileEncoded(spill_file[i], minio_client, &sizes[i], (uniqueName + "_" + std::to_string(i)).c_str(), &counter);
            }
            else
            {
                spillS3File(spill_file[i], minio_client, &sizes[i], (uniqueName + "_" + std::to_string(i)).c_str(), &counter);
            }
        }
        // std::cout << "spilled to s3 from file" << std::endl;
    }
    std::vector<std::pair<file, char>> files;
    for (char i = 0; i < partitions; i++)
    {
        size_t spill_mem_size = 0;
        for (auto &it : sizes[i])
        {
            spill_mem_size += it.first;
        }
        if (spill_mem_size > 0)
        {
            file file;
            file.name = uniqueName + "_" + std::to_string(i);
            file.size = spill_mem_size;
            file.status = fileStatus;
            file.subfiles = sizes[i];
            files.push_back({file, i});
        }
    }
    for (auto &f : files)
    {
        comb_spill_size.fetch_add(f.first.size);
    }
    std::thread thread(addFileToManag, minio_client, files, write_to_id, fileStatus);
    // addFileToManag(minio_client, files, write_to_id, fileStatus);
    mana_writeThread_num.fetch_add(1);
    thread.detach();
}
/**
 * @brief execute the aggregation function
 *
 * @param hashValue pointer to array of values
 * @param value new value
 */
void execOperation(std::array<unsigned long, max_size> *hashValue, int value)
{
    switch (op)
    {
    case count:
        (*hashValue)[0]++;
        break;
    case sum:
        (*hashValue)[0] += value;
        break;
    case average:
        (*hashValue)[0] += value;
        (*hashValue)[1]++;
        break;
    }
}
/**
 * @brief add new hmap entry
 *
 * @param hmap pointer to hashmap
 * @param keys key of hashmap entry
 * @param opValue value of hashmap entry
 */
void addPair(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::array<unsigned long, max_size> keys, unsigned long opValue)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    switch (op)
    {
    case count:
    {
        unsigned long count = 0;
        if (opValue != ULONG_MAX)
        {
            count = 1;
        }
        std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>> pair(keys, {count, 0});
        hmap->insert(pair);
        break;
    }
    case sum:
    {
        unsigned long sum = 0;
        if (opValue != ULONG_MAX)
        {
            sum = opValue;
        }
        std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>> pair(keys, {sum, 0});
        hmap->insert(pair);
        break;
    }
    case average:
    {
        unsigned long count = 0;
        unsigned long sum = 0;
        if (opValue != ULONG_MAX)
        {
            count = 1;
            sum = opValue;
        }
        std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>> pair(keys, {sum, count});
        hmap->insert(pair);
        break;
    }
    default:
    {
        std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>> pair(keys, {});
        hmap->insert(pair);
    }
    }
}
/**
 * @brief Scan input file and insert values into hashmap. If memoryLimit is reached spill hashmap to local memory if possible otherwise to S3.
 *
 * @param hmap pointer to hashmap that is being filled
 * @param file input file handle
 * @param start index where to start in input file
 * @param size length of chars to be read
 * @param addOffset whether to finish last line eventhough size is overstepped
 * @param memLimit available memory
 * @param spill_files pointer to vector of vectors(partitions) to local files the hashmap is spilled to. Will be filled by the function if it has local spills
 * @param numLines number of lines the functions reads
 * @param comb_hash_size combined size of all the hashmaps
 * @param shared_diff combined size of all mappings
 * @param minio_client pointer to aws client
 * @param readBytes combined number of chars read
 * @param memLimitBack background memory limit
 */
void fillHashmap(char id, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, size_t start, size_t size, bool addOffset, size_t memLimit,
                 float &avg, std::vector<std::vector<std::pair<std::string, size_t>>> *spill_files, std::atomic<unsigned long> &numLines, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *shared_diff, Aws::S3::S3Client *minio_client,
                 std::atomic<unsigned long> &readBytes, unsigned long memLimitBack)
{
    // Aws::S3::S3Client minio_client = init();
    //  hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    auto thread_start_time = std::chrono::high_resolution_clock::now();
    int offset = 0;
    unsigned long spill_size = 0;
    if (addOffset)
        offset = 200;
    // map inputfile
    char *mappedFile = static_cast<char *>(mmap(nullptr, size + offset, PROT_READ, MAP_SHARED, file, start));
    if (mappedFile == MAP_FAILED)
    {
        close(file);
        perror("Error mmapping the file in fill hashmap");
        exit(EXIT_FAILURE);
    }
    madvise(mappedFile, size + offset, MADV_SEQUENTIAL | MADV_WILLNEED);
    std::unordered_map<std::string, std::string> lineObjects;
    std::array<unsigned long, max_size> keys = {0, 0};
    std::string coloumns[key_number + 1];
    for (int i = 0; i < key_number; i++)
    {
        coloumns[i] = key_names[i];
    }
    coloumns[key_number] = opKeyName;
    unsigned long head = 0;
    unsigned long numLinesLocal = 0;
    unsigned long maxHmapSize = 0;
    unsigned long opValue = 0;
    int spill_number = 0;
    std::string okey, lineObjectValue;

    std::pair<int, size_t> temp_spill_file(-1, 0);
    std::string temp_spill_file_name;
    temp_spill_file_name += worker_id;
    temp_spill_file_name += "_";
    temp_spill_file_name += std::to_string((int)(id));
    temp_spill_file_name += "_temp_spill";
    std::thread minioSpiller;
    unsigned long old_i = 0;
    bool spillS3Thread = false;
    bool thread_finishFlag = false;
    std::string uName;
    std::string spill_file_name;
    size_t comb_local_spill_size = 0;
    size_t temp_local_spill_size = 0;
    threadLog threadLog;
    threadLog.sizes["id"] = id;
    threadLog.sizes["inputSize"] = size;

    // loop through entire mapping
    for (unsigned long i = 0; i < size + offset; i++)
    {
        i = readTuple(mappedFile, i, coloumns, &lineObjects, size);
        if (i == ULONG_MAX)
        {
            break;
        }
        shared_diff->fetch_add(i - old_i);
        readBytes.fetch_add(i - old_i);
        old_i = i;

        try
        {
            for (int k = 0; k < key_number; k++)
            {
                keys[k] = std::stol(lineObjects[key_names[k]]);
            }
            if (lineObjects[opKeyName] == "null" || lineObjects[opKeyName] == "")
            {
                opValue = ULONG_MAX;
            }
            else
            {
                opValue = std::stol(lineObjects[opKeyName]);
            }
        }
        catch (std::exception &err)
        {
            for (auto &it : lineObjects)
            {
                std::cout << it.first << ", " << it.second << std::endl;
            }
            std::cout << "conversion error on: " << err.what() << std::endl;
        }

        // add 1 to count when customerkey is already in hashmap
        if (hmap->contains(keys))
        {
            if (opValue != ULONG_MAX)
            {
                execOperation(&(*hmap)[keys], opValue);
            }
            if (std::find(std::begin(test_values), std::end(test_values), keys[0]) != std::end(test_values))
            {
                std::cout << "hmap contains key: " << keys[0] << " value: " << opValue << ", " << (*hmap)[keys][1] << std::endl;
            }
        }
        else
        {

            // add customerkey, count pair to hashmap. When orderkey is not null count starts at 1.
            addPair(hmap, keys, opValue);
            if (hmap->size() > maxHmapSize)
            {
                comb_hash_size.fetch_add(1);
                maxHmapSize = hmap->size();
            }
            if (std::find(std::begin(test_values), std::end(test_values), keys[0]) != std::end(test_values))
            {
                std::cout << "Add key to hmap: " << keys[0] << " value: " << opValue << ", " << (*hmap)[keys][1] << std::endl;
            }
        }

        // Check if Estimations exceed memlimit
        // if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
        // std::cout << "maxHmapSize: " << maxHmapSize << std::endl;
        unsigned long freed_space_temp = (i - head) - ((i - head) % pagesize);
        // if ((maxHmapSize * avg + base_size / threadNumber >= memLimit) || freed_space_temp > mapping_max)
        if (maxHmapSize * avg + base_size / threadNumber >= memLimit * 0.95 || freed_space_temp > mapping_max)
        {
            // std::cout << "memLimit broken. Estimated mem used: " << hmap->size() * avg + base_size / threadNumber << " size: " << hmap->size() << " avg: " << avg << " base_size / threadNumber: " << base_size / threadNumber << std::endl;

            // Free up space from mapping that is no longer needed.
            if (i - head > pagesize)
            {
                // calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.

                if (munmap(&mappedFile[head], freed_space_temp) == -1)
                {
                    std::cout << "head: " << head << " freed_space_temp: " << freed_space_temp << std::endl;
                    perror("Could not free memory!");
                }

                // Update Head to point at the new unfreed mapping space.

                // Update numHashRows so that the estimations are still correct.
                // std::cout << "diff: " << i - head + 1 << " freed space: " << freed_space_temp << " Estimated Rows: " << numHashRows << std::endl;
                //  phyMemBase -= freed_space >> 10;
                head += freed_space_temp;
                shared_diff->fetch_sub(freed_space_temp);
            }
        }

        // compare estimation again to memLimit
        // if (freed_space_temp <= pagesize * 10 && hmap->size() * (key_number + value_number) * sizeof(long) > pagesize && hmap->size() * avg + base_size / threadNumber >= memLimit * 0.9)
        // if (hmap->size() >= maxHmapSize * 0.98 && freed_space_temp <= mapping_max)
        if (maxHmapSize * avg + base_size / threadNumber >= memLimit - mapping_max && hmap->size() >= maxHmapSize * 0.98)
        {
            auto start_spill_time = std::chrono::high_resolution_clock::now();
            // std::cout << "spilling with size: " << hmap->size() << " i-head: " << (i - head + 1) << " size: " << getPhyValue() << std::endl;
            //    Reset freed_space and update numHashRows so that Estimation stay correct
            if (maxHmapSize < hmap->size())
            {
                maxHmapSize = hmap->size();
                // std::cout << "new MaxSize: " << maxHmapSize << std::endl;
            }
            unsigned long temp_spill_size = hmap->size() * (key_number + value_number) * sizeof(long);
            spill_size += temp_spill_size;
            comb_spill_size.fetch_add(temp_spill_size);
            spillTuple_number.fetch_add(hmap->size());
            if (partitions == -1)
            {
                partitions_set_lock.lock();
                if (partitions == -1)
                {
                    setPartitionNumber(comb_hash_size);
                    if (spill_files->size() == 0)
                    {
                        for (int i = 0; i < partitions; i++)
                        {
                            spill_files->push_back({});
                        }
                    }
                    if (split_mana)
                    {
                        manaFile mana_worker;
                        mana_worker.workers.push_back(manaFileWorker());
                        std::cout << "Adding partition file" << std::endl;
                        for (char p = 0; p < partitions; p++)
                        {
                            partition part;
                            part.id = p;
                            part.lock = false;
                            mana_worker.workers[0].partitions.push_back(part);

                            writeMana(minio_client, mana_worker, false, worker_id, p);
                        }
                        writeMana(minio_client, mana_worker, false, worker_id);
                    }
                }
                partitions_set_lock.unlock();
            }
            threadLog.sizes["SpillSize"] += temp_spill_size;

            threadLog.spillTimes.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count());

            if (memLimitBack > backMem_usage + temp_spill_size - temp_local_spill_size)
            {
                if (memLimitBack < backMem_usage + temp_spill_size * threadNumber)
                {
                    threadLog.sizes["localS3Spill"]++;
                    threadLog.sizes["s3SpillSize"] += temp_spill_size;
                    std::cout << "local + s3: " << (int)(id) << std::endl;
                    backMem_usage += temp_spill_size - temp_local_spill_size;
                    temp_local_spill_size = temp_spill_size;
                    auto start_wait_time = std::chrono::high_resolution_clock::now();
                    if (spillS3Thread)
                    {
                        minioSpiller.join();
                    }
                    threadLog.sizes["wait_time"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_wait_time).count();
                    spill_file_name = "";
                    spill_file_name += worker_id;
                    spill_file_name += "_";
                    spill_file_name += std::to_string((int)(id));
                    spill_file_name += "_";
                    spill_file_name += "temp_spill";
                    std::vector<std::pair<std::string, size_t>> spill_file(partitions);
                    for (int p = 0; p < partitions; p++)
                    {
                        spill_file[p] = {spill_file_name + "_" + std::to_string(p), 0};
                    }
                    spillToFile(hmap, &spill_file, id, pagesize * 20);
                    uName = "";
                    uName += worker_id;
                    uName += "_";
                    uName += std::to_string((int)(id));
                    uName += "_" + std::to_string(spill_number);
                    // spillToMinio(hmap, std::ref(temp_spill_file_name), std::ref(uName), pagesize * 20, &minio_client, worker_id, 0, id);
                    minioSpiller = std::thread(spillToMinio, hmap, spill_file, uName, minio_client, worker_id, 0, id);
                    spillS3Thread = true;
                }
                else
                {
                    threadLog.sizes["localSpill"]++;
                    threadLog.sizes["localSpillSize"] += temp_spill_size;
                    std::cout << "local: " << (int)(id) << std::endl;
                    spill_file_name = "";
                    spill_file_name += worker_id;
                    spill_file_name += "_";
                    spill_file_name += std::to_string((int)(id));
                    spill_file_name += "_";
                    spill_file_name += std::to_string(spill_number);
                    spill_file_name += "_";
                    spill_file_name += "spill";
                    backMem_usage += temp_spill_size;
                    std::vector<std::pair<std::string, size_t>> spill_file(partitions);
                    for (int p = 0; p < partitions; p++)
                    {
                        spill_file[p] = {spill_file_name + "_" + std::to_string(p), 0};
                    }
                    spillToFile(hmap, &spill_file, id, pagesize * 20);
                    for (int i = 0; i < partitions; i++)
                    {
                        (*spill_files)[i].push_back(spill_file[i]);
                    }
                }
            }
            else
            {
                threadLog.sizes["s3Spill"]++;
                threadLog.sizes["s3SpillSize"] += temp_spill_size;
                std::cout << "s3: " << (int)(id) << std::endl;
                uName = "";
                uName += worker_id;
                uName += "_";
                uName += std::to_string((int)(id));
                uName += "_" + std::to_string(spill_number);
                std::string empty = "";
                std::vector<std::pair<std::string, size_t>> spill_file(partitions, {"", 0});
                spillToMinio(hmap, spill_file, uName, minio_client, worker_id, 0, id);

                for (auto &test_value : test_values)
                {
                    if (hmap->contains({test_value, 0}))
                    {
                        std::cout << "Spilling " << test_value << " to " << uName << " with value " << (*hmap)[{test_value, 0}][0] << ", " << (*hmap)[{test_value, 0}][1] << std::endl;
                    }
                }
            }
            spill_number++;
            write_log_file_lock.lock();
            log_file.write_spill_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_spill_time).count());
            write_log_file_lock.unlock();
            threadLog.sizes["write_file_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_spill_time).count();

            // std::cout << "Spilling ended" << std::endl;
            threadLog.sizes["outputLines"] += hmap->size();
            hmap->clear();
        }
        numLines.fetch_add(1);
        numLinesLocal++;
    }
    std::cout << "finishing Thread" << std::endl;
    if (size - head > 0)
    {
        if (munmap(&mappedFile[head], size - head) == -1)
        {
            std::cout << "head: " << head << " freed_space_temp: " << size - head << std::endl;
            perror("Could not free memory in end of thread!");
        }
    }
    if (spillS3Thread)
    {
        minioSpiller.join();
        spill_file_name = "";
        spill_file_name += worker_id;
        spill_file_name += "_";
        spill_file_name += std::to_string((int)(id));
        spill_file_name += "_";
        spill_file_name += "temp_spill";
        struct stat stats;
        stat(spill_file_name.c_str(), &stats);
        backMem_usage -= stats.st_size;
        remove(spill_file_name.c_str());
    }
    try
    {
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - thread_start_time).count()) / 1000000;
        std::cout << "Thread " << int(id) << " finished scanning. With time: " << duration << "s. Scanned Lines: " << numLinesLocal << ". microseconds/line: " << duration * 1000000 / numLinesLocal << ". Spilled with size: " << spill_size << std::endl;
        threadLog.sizes["duration"] = duration;
        threadLog.sizes["endTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
        threadLog.sizes["inputLines"] = numLinesLocal;
        threadLog.sizes["outputLines"] += hmap->size();
        write_log_file_lock.lock();
        log_file.threads.push_back(threadLog);
        write_log_file_lock.unlock();
    }
    catch (std::exception &err)
    {
        std::cout << "Not able to print time: " << err.what() << std::endl;
    }
}
/**
 * @brief iteratively calculates the average size of hashmap entries and logs program size
 *
 * @param finished Whether to stop function
 * @param memLimit Main memory limit
 * @param comb_hash_size combined size of all hashmaps in memory
 * @param diff combined size of all mappings
 * @param avg average size of hashmap entries
 */
void printSize(int &finished, size_t memLimit, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *diff, float *avg)
{
    std::ofstream output;
    if (log_size)
    {
        // int log_file = open(("times_" + date_now).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
        // close(log_file);
        // std::cout << "times_" + date_now + ".csv" << std::endl;
        output.open(("times_" + date_now + ".csv").c_str());
        output << "mes_size,hmap_size,base_size,map_size,bit_size,avg,time\n";
    }
    int phyMemBase = (getPhyValue()) * 1024;
    bool first = true;
    int small_counter = 0;
    // int counter = 0;
    size_t maxSize = 0;
    size_t old_size = 0;
    size_t size = 0;
    float oldduration = 0;
    float duration = 0;
    int old_finish = finished;
    size_t old_comb_hash_size = 0;
    unsigned long reservedMem = 0;
    // memLimit -= 2ull << 10;
    while (finished == 0 || finished == 1)
    {
        size_t newsize = getPhyValue() * 1024;
        while (abs(static_cast<long>(size - newsize)) > 5000000000)
        {
            newsize = getPhyValue() * 1024;
        }

        if (old_finish != finished)
        {
            phyMemBase = newsize;
            old_finish = finished;
        }
        reservedMem = diff->load();
        old_comb_hash_size = comb_hash_size.load();

        if (log_size)
        {
            duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000;
            if (duration - oldduration > 10)
            {
                oldduration = duration;
                // std::string concat_string = std::to_string(newsize) + "," + std::to_string((unsigned long)((*avg) * comb_hash_size.load())) + "," + std::to_string(phyMemBase) + "," + std::to_string(reservedMem) + "," + std::to_string(*extra_mem) + "," + std::to_string(duration);
                output << std::to_string(newsize) << "," << std::to_string((*avg) * comb_hash_size.load()) << "," << std::to_string(phyMemBase) << "," << std::to_string(reservedMem) << "," << std::to_string(extra_mem) << "," << std::to_string(*avg) << "," << std::to_string(duration);
                // output << concat_string;
                output << std::endl;
                // std::cout << concat_string << std::endl;
            }
        }
        size = newsize;
        base_size = phyMemBase + reservedMem + extra_mem;
        // std::cout << "phy: " << size << std::endl;

        if (size > old_size)
        {
            if (size > maxSize)
            {
                maxSize = size;
                // std::cout << "phy: " << size << std::endl;
            }
            if (comb_hash_size.load() > 0 && size > memLimit * 0.7)
            {
                float temp_avg = (size - base_size) / (float)(comb_hash_size.load());
                // std::cout << "avg: " << *avg << " avg diff: " << std::abs(temp_avg - (*avg)) << std::endl;
                // if (std::abs(temp_avg - (*avg)) < 10 || (*avg < 10 && std::abs(temp_avg - (*avg)) < 100))
                //{
                if (temp_avg < 100)
                {
                    //  *avg = std::max((float)(100), temp_avg);
                    *avg = temp_avg;
                }
                //}
                /* else
                {

                    long change = (long)((long)(size) - (comb_hash_size.load() * (*avg) + base_size));
                    if (change + (long)(extra_mem) < 0)
                    {
                        extra_mem = 0;
                    }
                    else
                    {
                        extra_mem += change;
                    }
                } */

                if (first)
                {
                    max_s3_spill_size = std::min(comb_hash_size.load(), max_s3_spill_size);
                    std::cout << "max_s3_spill_size: " << max_s3_spill_size << std::endl;
                    first = false;
                }
                //*avg *= 1.2;
                // std::cout << "phy: " << size << " phymemBase: " << phyMemBase << " avg: " << *avg << " reservedMem: " << reservedMem << " (*extra_mem): " << (*extra_mem) << std::endl;
                usleep(0);
            }
        }
        // if (base_size + (*avg) * comb_hash_size.load() > memLimit * 0.85 && size < memLimit * 0.8)
        /* if (base_size + (*avg) * comb_hash_size.load() > size && comb_hash_size.load() > 0)
        {
            float temp_avg = (size - base_size) / (float)(comb_hash_size.load());
            if (std::abs(temp_avg - (*avg)) < 10)
            {
                *avg = temp_avg;
            }
            else
            {

                long change = (long)((long)(size) - (comb_hash_size.load() * (*avg) + base_size));
                if (change + (long)(extra_mem) < 0)
                {
                    extra_mem = 0;
                }
                else
                {
                    extra_mem += change;
                }
            }
            usleep(0);
        } */
        old_size = size;
        usleep(0);
    }
    std::cout << "Max Size: " << maxSize << "B." << std::endl;
    write_log_file_lock.lock();
    log_file.sizes["maxMainUsage"] = maxSize;
    write_log_file_lock.unlock();
    output.close();
}
/**
 * @brief Add and/or merge spill files to the given hashmap
 *
 * @param hmap hashmap
 * @param s3spillNames2 s3 spills (tuples of filename, size in B, subfiles(pairs of size in B, number of tuples))
 * @param s3spillBitmaps bitmaps for the s3 files (pair of file handle (if bitmap is spilled), char vector (if not))
 * @param spills local spills (pairs of filename, size in B)
 * @param add whether to add entries to hashmap or only merge
 * @param s3spillFile_head index of first s3 spill file
 * @param bit_head index of first bitmap
 * @param subfile_head index of first subfile in S3 first file
 * @param s3spillStart_head index of first tuple in first S3 file (not decoded)
 * @param s3spillStart_head_charsindex of first tuple in first S3 file (decoded)
 * @param input_head_base index of first tuple in first local file
 * @param size_after_init memory usage of program before function call
 * @param read_lines number of lines read
 * @param minio_client aws client
 * @param writeLock read and write lock for hashmap
 * @param avg average size of hashmap entry
 * @param memLImit available memory
 * @param comb_hash_size combined size of all hashmaps
 * @param diff combined size of all mappings
 * @param increase_size whether size of input stream should be added to program memory usage
 * @param max_hash_size biggest size of hashmap
 * @param t_id thread id
 * @param merge_file_num number of files to be merged
 *
 * @return whether memory limit was reached
 */
bool subMerge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap,
              std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *s3spillNames2,
              std::vector<std::pair<int, std::vector<char>>> *s3spillBitmaps, std::vector<std::pair<std::string, size_t>> *spills, bool add, int *s3spillFile_head,
              int *bit_head, int *subfile_head, size_t *s3spillStart_head, size_t *s3spillStart_head_chars, size_t *input_head_base, size_t size_after_init, std::atomic<size_t> *read_lines,
              Aws::S3::S3Client *minio_client, std::shared_mutex *writeLock, float *avg, float memLimit, std::atomic<unsigned long> &comb_hash_size,
              std::atomic<unsigned long> *diff, bool increase_size, size_t *max_hash_size, int t_id, int merge_file_num)
{
    // input_head_base;
    unsigned long num_entries = 0;
    unsigned long input_head = 0;
    unsigned long offset = 0;
    unsigned long sum = 0;
    unsigned long newi = 0;
    size_t mapping_size = 0;
    size_t comb_spill_size_temp = 0;
    size_t increase = 0;
    int file_counter = 0;
    int conc_threads = multiThread_merge ? mergeThreads_number : 1;

    for (auto &it : *spills)
    {
        comb_spill_size_temp += it.second;
        // std::cout << it.second << ", ";
    }

    // std::cout << "write: " << emHashmap[{221877}][0] << std::endl;
    bool firsts3File = false;
    bool firsts3subFile = false;
    unsigned long *spill_map = nullptr;
    char *spill_map_char = nullptr;
    // std::cout << "s3spillFile: " << s3spillFile_head << std::endl;
    // std::cout << "s3spillStart: " << s3spillStart_head << std::endl;

    int number_of_longs = key_number + value_number;
    int it_counter = *s3spillFile_head;
    bool locked = false;
    std::array<unsigned long, max_size> keys = {0, 0};
    std::array<unsigned long, max_size> values = {0, 0};

    int bit_i = *bit_head;
    if (it_counter < s3spillNames2->size())
    {
        for (auto set_it = std::next(s3spillNames2->begin(), it_counter); set_it != s3spillNames2->end(); set_it++)
        {
            file_counter++;
            if (!add && multiThread_subMerge && file_counter > merge_file_num)
            {
                // std::cout << "file limit reached: " << file_counter << std::endl;
                // extra_mem -= increase;
                return false;
            }
            if (locked && add)
            {
                // extra_mem -= increase;
                return false;
            }

            firsts3File = file_counter == 1 && add;
            // std::cout << "firsts3File: " << firsts3File << std::endl;
            int sub_file_counter = 0;

            if (firsts3File)
            {
                sub_file_counter = *subfile_head;
            }
            for (int sub_file_k = sub_file_counter; sub_file_k < get<2>(*set_it).size(); sub_file_k++)
            {

                auto read_file_start = std::chrono::high_resolution_clock::now();
                auto sub_file = get<2>(*set_it)[sub_file_k].second;
                firsts3subFile = firsts3File && sub_file_k == sub_file_counter;
                // std::cout << "firsts3subFile: " << firsts3subFile << std::endl;
                /* std::cout << "Thread " << t_id;
                if (add)
                {
                    std::cout << " adding ";
                }
                else
                {
                    std::cout << " merging ";
                }
                std::cout << get<0>(*set_it) + "_" + std::to_string(sub_file_counter) << " bitmap: " << bit_i << " Read lines: " << *read_lines << std::endl; */
                Aws::S3::Model::GetObjectRequest request;
                request.SetBucket(bucketName);
                request.SetKey(get<0>(*set_it) + "_" + std::to_string(sub_file_counter));
                sub_file_counter++;
                Aws::S3::Model::GetObjectOutcome outcome;
                while (true)
                {
                    outcome = minio_client->GetObject(request);
                    if (!outcome.IsSuccess())
                    {
                        std::cout << "GetObject error " << get<0>(*set_it) + "_" + std::to_string(sub_file_counter - 1) << " " << outcome.GetError().GetMessage() << std::endl;
                    }
                    else
                    {
                        break;
                    }
                }
                // std::cout << "Reading spill: " << (*set_it).first << std::endl;
                auto &spill = outcome.GetResult().GetBody();
                write_log_file_lock.lock();
                log_file.getCall_s3_file_durs.push_back({std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_file_start).count(), get<2>(*set_it)[sub_file_k].first});
                write_log_file_lock.unlock();
                // spill.rdbuf()->pubsetbuf(buffer, 1ull << 10);
                //   spill.rdbuf()->
                char *bitmap_mapping;
                std::vector<char> *bitmap_vector;
                // std::cout << "first: " << s3spillBitmaps[bit_i].first << std::endl;
                bool spilled_bitmap = false; //(*s3spillBitmaps)[bit_i].first != -1;
                if (!spilled_bitmap)
                {
                    bitmap_vector = &(*s3spillBitmaps)[bit_i].second;
                }
                else
                {
                    bitmap_mapping = static_cast<char *>(mmap(nullptr, std::ceil((float)(sub_file) / 8), PROT_WRITE | PROT_READ, MAP_SHARED, (*s3spillBitmaps)[bit_i].first, 0));
                    if (bitmap_mapping == MAP_FAILED)
                    {
                        perror("Error mmapping the file in opening bitmap");
                        exit(EXIT_FAILURE);
                    }
                    madvise(bitmap_mapping, std::ceil((float)(sub_file) / 8), MADV_SEQUENTIAL | MADV_WILLNEED);
                }
                // std::cout << "Reading spill: " << (*s3spillNames)[i] << " with bitmap of size: " << bitmap_vector->size() << std::endl;
                unsigned long head = 0;
                unsigned long s3spillStart_head_chars_counter = 0;
                if (firsts3File && firsts3subFile)
                {
                    head = *s3spillStart_head;
                    // std::cout << "First File" << std::endl;
                    if (deencode)
                    {
                        spill.ignore(*s3spillStart_head_chars);
                        s3spillStart_head_chars_counter = *s3spillStart_head_chars;
                    }
                    else
                    {
                        spill.ignore((*s3spillStart_head) * sizeof(long) * number_of_longs);
                    }
                    // std::cout << "Load bitmap: " << i << " at index: " << head << std::endl;
                }

                unsigned long lower_index = 0;
                if (increase_size)
                {
                    increase = size_after_init * 1024 * 100 + 1;
                    if (getPhyValue() < size_after_init)
                    {
                        increase = 0;
                    }
                    else
                    {
                        increase = (getPhyValue() - size_after_init) * 1024;
                    }
                    // std::cout << "Stream buffer: " << increase << std::endl;

                    extra_mem += increase;
                    // std::cout << "extra_mem " << extra_mem << std::endl;
                    increase_size = false;
                }
                while (spill.peek() != EOF)
                {
                    char *bit;
                    size_t index = std::floor(head / 8);
                    if (!spilled_bitmap)
                    {
                        bit = &(*bitmap_vector)[index];
                    }
                    else
                    {
                        bit = &bitmap_mapping[index];
                    }

                    // std::cout << "accessing index: " << std::floor(head / 8) << ": " << std::bitset<8>(*bit) << " AND " << std::bitset<8>(1 << (head % 8)) << "= " << ((*bit) & (1 << (head % 8))) << std::endl;
                    if ((*bit) & (1 << (head % 8)))
                    {
                        // auto read_tuple_start = std::chrono::high_resolution_clock::now();
                        unsigned long buf[number_of_longs];
                        read_lines->fetch_add(1);
                        if (deencode)
                        {
                            if (add && !locked)
                            {
                                *s3spillStart_head_chars = s3spillStart_head_chars_counter;
                            }
                            for (int i = 0; i < number_of_longs; i++)
                            {
                                char l_bytes = spill.get();
                                char char_buf[sizeof(long)];
                                int counter = 0;
                                while (counter < l_bytes)
                                {
                                    char_buf[counter] = spill.get();
                                    counter++;
                                }
                                s3spillStart_head_chars_counter += l_bytes + 1;
                                /* for (auto &it : char_buf)
                                {
                                    std::cout << std::bitset<8>(it) << ", ";
                                }
                                std::cout << std::endl; */
                                while (counter < sizeof(long))
                                {
                                    char_buf[counter] = 0;
                                    counter++;
                                }
                                /* for (auto &it : char_buf)
                                {
                                    std::cout << std::bitset<8>(it) << ", ";
                                }
                                std::cout << std::endl; */
                                std::memcpy(&buf[i], &char_buf, sizeof(long));
                            }
                        }
                        else
                        {
                            char char_buf[sizeof(long) * number_of_longs];
                            spill.read(char_buf, sizeof(long) * number_of_longs);
                            std::memcpy(buf, &char_buf, sizeof(long) * number_of_longs);
                        }
                        if (!spill)
                        {

                            break;
                        }
                        // log_file.sizes["get_tuple_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_tuple_start).count();

                        // static_cast<unsigned long *>(static_cast<void *>(buf));
                        // std::cout << buf[0] << ", " << buf[1] << std::endl;
                        for (int k = 0; k < key_number; k++)
                        {
                            keys[k] = buf[k];
                        }

                        for (int k = 0; k < value_number; k++)
                        {
                            values[k] = buf[k + key_number];
                        }
                        writeLock->lock_shared();
                        bool contained = hmap->contains(keys);
                        writeLock->unlock_shared();
                        if (contained)
                        {

                            std::array<unsigned long, max_size> temp = (*hmap)[keys];

                            for (int k = 0; k < value_number; k++)
                            {
                                temp[k] += values[k];
                            }
                            bool asdf = false;
                            writeLock->lock();
                            (*hmap)[keys] = temp;
                            writeLock->unlock();

                            *bit &= ~(0x01 << (head % 8));
                            if (std::find(std::begin(test_values), std::end(test_values), keys[0]) != std::end(test_values))
                            {
                                std::cout << "found key in Spill contained in hashmap: " << keys[0] << " value: " << (*hmap)[keys][0] << ", " << (*hmap)[keys][1] << " In spill: " << (get<0>(*set_it) + "_" + std::to_string(sub_file_counter)) << std::endl;
                            }
                        }
                        else if (add && !locked)
                        {
                            // std::cout << "Setting " << std::bitset<8>(bitmap[std::floor(head / 8)]) << " xth: " << head % 8 << std::endl;
                            hmap->insert(std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>>(keys, values));
                            // std::cout << "max_hash_size: " << *max_hash_size << " hmap siuze: " << hmap->size() << " comb_hash_size: " << comb_hash_size.load();
                            if (hmap->size() > *max_hash_size)
                            {
                                comb_hash_size.fetch_add(1);
                                *max_hash_size += 1;
                                /* if (comb_hash_size.load() % 100 == 0)
                                {
                                    *avg = (getPhyValue() - base_size) / comb_hash_size.load();
                                } */
                            }
                            *bit &= ~(0x01 << (head % 8));
                            if (std::find(std::begin(test_values), std::end(test_values), keys[0]) != std::end(test_values))
                            {
                                std::cout << "found key in Spill added to hashmap: " << keys[0] << " value: " << (*hmap)[keys][0] << ", " << (*hmap)[keys][1] << " In spill: " << (get<0>(*set_it) + "_" + std::to_string(sub_file_counter)) << std::endl;
                            }
                            // std::cout << "After setting " << std::bitset<8>(bitmap[std::floor(head / 8)]) << std::endl;
                        }

                        if (spilled_bitmap)
                        {
                            // diff->exchange(index);
                            if ((*max_hash_size) * (*avg) + base_size / conc_threads >= memLimit / conc_threads)
                            {
                                // std::cout << "spilling: " << head - lower_index << std::endl;
                                unsigned long freed_space_temp = (index - lower_index) - ((index - lower_index) % pagesize);
                                if (index - lower_index >= pagesize)
                                {
                                    if (munmap(&bitmap_mapping[lower_index], freed_space_temp) == -1)
                                    {
                                        std::cout << freed_space_temp << std::endl;
                                        perror("Could not free memory of bitmap 1!");
                                    }
                                    // std::cout << "Free: " << input_head << " - " << freed_space_temp / sizeof(long) + input_head << std::endl;
                                    // Update Head to point at the new unfreed mapping space.
                                    lower_index += freed_space_temp;
                                }
                                if (hmap->size() >= *max_hash_size * 0.99 && freed_space_temp <= pagesize * 20)
                                {
                                    if (add && !locked)
                                    {
                                        locked = true;
                                        *s3spillFile_head = it_counter;
                                        *s3spillStart_head = head;
                                        *bit_head = bit_i;
                                        *subfile_head = sub_file_counter - 1;
                                    }
                                    if (firsts3File)
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            if ((*max_hash_size) * (*avg) + base_size / conc_threads >= (memLimit * 0.9) / conc_threads && hmap->size() >= *max_hash_size * 0.99)
                            {
                                if (add && !locked)
                                {
                                    // std::cout << "Calc size: " << hmap->size() * (*avg) + base_size << " base_size: " << base_size << " hmap length " << hmap->size() << " memlimit: " << memLimit << std::endl;
                                    locked = true;
                                    *s3spillFile_head = it_counter;
                                    *s3spillStart_head = head;
                                    *bit_head = bit_i;
                                    *subfile_head = sub_file_counter - 1;
                                }
                                if (firsts3File)
                                {

                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (deencode)
                        {
                            for (int k = 0; k < number_of_longs; k++)
                            {
                                char skip_bytes = spill.get();
                                spill.ignore(skip_bytes);
                                s3spillStart_head_chars_counter += skip_bytes + 1;
                            }
                            if (!spill)
                            {

                                break;
                            }
                        }
                        else
                        {
                            spill.ignore(sizeof(long) * number_of_longs);
                            if (!spill)
                            {

                                break;
                            }
                        }
                    }
                    head++;
                }

                // std::cout << "head: " << head * sizeof(long) * number_of_longs << ", spillsize: " << sub_file << std::endl;
                if (spilled_bitmap)
                {
                    if (munmap(&bitmap_mapping[lower_index], std::ceil((float)(sub_file) / 8) - lower_index) == -1)
                    {
                        std::cout << std::ceil((float)(get<1>(*set_it)) / 8) - lower_index << " lower_index: " << lower_index << std::endl;
                        perror("Could not free memory of bitmap 2!");
                    }
                }
                bit_i++;
                if (firsts3File && locked)
                {
                    bit_i += get<2>(*set_it).size() - sub_file_counter;
                    // std::cout << "Breaking because first file s3spillFile_head: " << s3spillFile_head << ",s3spillStart_head " << s3spillStart_head << ",bit_head " << bit_head << ", subfilehead: " << subfile_head << std::endl;
                    break;
                }
            }
            it_counter++;
        }
    }
    if (add)
    {
        *s3spillFile_head = s3spillNames2->size();
        std::cout << "adding local input_head_base: " << *input_head_base << ", comb_spill_size: " << comb_spill_size_temp << std::endl;
    }
    else
    {
        std::cout << "Merging local input_head_base: " << *input_head_base << ", comb_spill_size: " << comb_spill_size_temp << std::endl;
    }

    // std::cout << "New round" << std::endl;
    // Go through entire mapping
    int file_handler = -1;
    size_t diff_add = 0;
    size_t diff_sub_f = 0;
    size_t diff_sub_s = 0;
    size_t diff_sub_t = 0;
    size_t diff_diff = 0;

    for (unsigned long i = *input_head_base; (!deencode && i < comb_spill_size_temp / sizeof(long)) || (deencode && i < comb_spill_size_temp); i++)
    {
        if ((!deencode && i >= sum / sizeof(long)) || (deencode && i >= sum))
        {
            // std::cout << t_id << ": new mapping" << std::endl;
            if (((!deencode && i != sum / sizeof(long)) || (deencode && i != sum)) && i != *input_head_base)
            {
                std::cout << "i!=sum! i: " << i << " sum: " << sum << " Input head base: " << *input_head_base << std::endl;
            }
            sum = 0;
            int c = 0;
            for (auto &it : *spills)
            {
                //  std::cout << "opening new mapping" << std::endl;
                sum += it.second;
                if ((!deencode && i < sum / sizeof(long)) || (deencode && i < sum))
                {
                    if (spill_map != nullptr && mapping_size - input_head * sizeof(long) > 0)
                    {
                        // save empty flag and release the mapping
                        if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(long)) == -1)
                        {
                            std::cout << "invalid size: " << mapping_size - input_head * sizeof(long) << std::endl;
                            perror("Could not free memory in merge 2_1!");
                        }

                        if (diff->load() > diff_diff)
                        {
                            diff->fetch_sub(diff_diff);
                        }
                        diff_sub_f += mapping_size - input_head * sizeof(long);
                        /*  else
                         {
                             std::cout << "Diff too small 1! diff: " << diff->load() << " - " << mapping_size - input_head * sizeof(long) << std::endl;
                         } */
                        // std::cout << "Free: " << input_head << " - " << mapping_size / sizeof(long) << std::endl;
                    }
                    if (spill_map_char != nullptr && mapping_size - input_head > 0)
                    {
                        // save empty flag and release the mapping
                        if (munmap(&spill_map_char[input_head], mapping_size - input_head) == -1)
                        {
                            std::cout << "invalid size: " << mapping_size - input_head << std::endl;
                            perror("Could not free memory in merge 2_1!");
                        }
                        if (diff_diff != mapping_size - input_head)
                        {
                            std::cout << "Diff diff != mapping_size - input_head! " << diff_diff << " != " << mapping_size << " - " << input_head << std::endl;
                        }
                        if (diff->load() > diff_diff)
                        {
                            diff->fetch_sub(diff_diff);
                            diff_sub_f += diff_diff;
                        }

                        /* else
                        {
                            std::cout << "Diff too small 1! diff: " << diff->load() << " - " << mapping_size - input_head << std::endl;
                        } */
                        // std::cout << "Free: " << input_head << " - " << mapping_size / sizeof(long) << std::endl;
                    }
                    file_counter++;
                    if (file_handler != -1)
                    {
                        close(file_handler);
                    }
                    if (!add && multiThread_subMerge && file_counter > merge_file_num)
                    {
                        // std::cout << "diff add: " << diff_add << " first diff sub: " << diff_sub_f << " second diff sub: " << diff_sub_s << " third diff sub: " << diff_sub_t << std::endl;
                        //  extra_mem -= increase;
                        return false;
                    }
                    if (locked && add)
                    {
                        // std::cout << "diff add: " << diff_add << " first diff sub: " << diff_sub_f << " second diff sub: " << diff_sub_s << " third diff sub: " << diff_sub_t << std::endl;
                        //  extra_mem -= increase;
                        return false;
                    }
                    unsigned long map_start;

                    file_handler = open(it.first.c_str(), O_RDWR, 0777);
                    if (deencode)
                    {
                        map_start = i - (sum - it.second) - ((i - (sum - it.second)) % pagesize);
                        mapping_size = it.second - map_start;
                        spill_map_char = static_cast<char *>(mmap(nullptr, mapping_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handler, map_start));
                        /* i = (sum - it.second);
                        map_start = 0; */
                        if (spill_map_char == MAP_FAILED)
                        {
                            close(file_handler);
                            perror("Error mmapping the file  in submerge 1");
                            exit(EXIT_FAILURE);
                        }
                        madvise(spill_map_char, mapping_size, MADV_SEQUENTIAL | MADV_WILLNEED);
                        input_head = 0;
                        offset = ((sum - it.second) + map_start);
                        // std::cout << t_id << ": opening new mapping mapsstart: " << map_start << " mapping size: " << mapping_size << " offset: " << offset << " i: " << i << " spillnum: " << c << std::endl;
                    }
                    else
                    {
                        map_start = i * sizeof(long) - (sum - it.second) - ((i * sizeof(long) - (sum - it.second)) % pagesize);

                        mapping_size = it.second - map_start;
                        // std::cout << " map_start: " << map_start << std::endl;
                        spill_map = static_cast<unsigned long *>(mmap(nullptr, mapping_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_handler, map_start));
                        if (spill_map == MAP_FAILED)
                        {
                            close(file_handler);
                            perror("Error mmapping the file in submerge 2");
                            exit(EXIT_FAILURE);
                        }
                        madvise(spill_map, mapping_size, MADV_SEQUENTIAL | MADV_WILLNEED);
                        input_head = 0;
                        offset = ((sum - it.second) + map_start) / sizeof(long);
                    }
                    diff_diff = i - offset;
                    diff->fetch_add(diff_diff);
                    // std::cout << "sum: " << sum / sizeof(long) << " offset: " << offset << " head: " << input_head_base << " map_start: " << map_start / sizeof(long) << " i: " << i << std::endl;
                    break;
                }
                c++;
            }
            if ((!deencode && i >= sum / sizeof(long)) || (deencode && i >= sum))
            {
                std::cout << t_id << ": i too big" << std::endl;
                break;
            }
        }
        newi = i - offset;
        read_lines->fetch_add(1);

        if (newi > mapping_size)
        {
            std::cout << "newi too big!! " << newi << std::endl;
        }
        unsigned long ognewi = newi;

        bool empty = false;
        // auto read_tuple_start = std::chrono::high_resolution_clock::now();

        if (deencode)
        {
            // std::cout << t_id << ": decoding: " << newi << std::endl;
            char char_buf[sizeof(long)];
            for (int k = 0; k < key_number; k++)
            {
                char l_bytes = spill_map_char[newi];
                if (l_bytes > 8)
                {
                    std::cout << "l_bytes too big! l_bytes: " << (int)(l_bytes) << std::endl;
                }
                if (l_bytes < 0 && k == 0)
                {
                    newi += l_bytes * -1 + 1;
                    for (int s = 0; s < key_number + value_number - 1; s++)
                    {
                        newi += spill_map_char[newi] + 1;
                    }
                    empty = true;
                    break;
                }
                newi++;
                int counter = 0;
                char_buf[0] = 0;
                while (counter < l_bytes)
                {
                    char_buf[counter] = spill_map_char[newi];
                    counter++;
                    newi++;
                }
                while (counter < sizeof(long))
                {
                    char_buf[counter] = 0;
                    counter++;
                }
                std::memcpy(&keys[k], &char_buf, sizeof(long));
            }
            if (!empty)
            {
                for (int k = 0; k < value_number; k++)
                {

                    char l_bytes = spill_map_char[newi];
                    newi++;
                    int counter = 0;
                    while (counter < l_bytes)
                    {
                        char_buf[counter] = spill_map_char[newi];
                        counter++;
                        newi++;
                    }
                    while (counter < sizeof(long))
                    {
                        char_buf[counter] = 0;
                        counter++;
                    }
                    std::memcpy(&values[k], &char_buf, sizeof(long));
                }
            }
            // std::cout << t_id << ": decoded: " << newi << std::endl;
        }
        else
        {
            if (spill_map[newi] == ULONG_MAX)
            {
                newi += key_number;
                newi += value_number;
                empty = true;
            }
            else
            {
                for (int k = 0; k < key_number; k++)
                {
                    keys[k] = spill_map[newi];
                    newi++;
                }

                for (int k = 0; k < value_number; k++)
                {
                    values[k] = spill_map[newi];
                    newi++;
                }
            }
        }
        newi--;
        i = newi + offset;
        if (deencode)
        {
            diff->fetch_add(newi - ognewi + 1);
            diff_add += newi - ognewi + 1;
            diff_diff += newi - ognewi + 1;
        }
        else
        {
            diff->fetch_add((newi - ognewi + 1) * sizeof(long));
            diff_diff += (newi - ognewi + 1) * sizeof(long);
        }

        // log_file.sizes["get_tuple_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_tuple_start).count();
        //   std::cout << keys[0] << ", " << values[0] << std::endl;
        if (!empty)
        {

            // std::cout << "i: " << i << ", newi: " << newi << std::endl;

            writeLock->lock_shared();
            bool contained = hmap->contains(keys);
            writeLock->unlock_shared();
            //    Update count if customerkey is in hashmap and delete pair in spill
            if (contained)
            {
                std::array<unsigned long, max_size> temp = (*hmap)[keys];

                for (int k = 0; k < value_number; k++)
                {
                    temp[k] += values[k];
                }
                writeLock->lock();
                (*hmap)[keys] = temp;
                writeLock->unlock();
                if (deencode)
                {
                    // std::cout << "ognewi first: " << (int) (spill_map_char[ognewi]);
                    spill_map_char[ognewi] *= -1;
                    /// std::cout << " later: " <<(int) (spill_map_char[ognewi]) << std::endl;
                }
                else
                {
                    spill_map[ognewi] = ULONG_MAX;
                }
            }
            else if (add && !locked)
            {
                hmap->insert(std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>>(keys, values));
                if (hmap->size() > *max_hash_size)
                {
                    comb_hash_size.fetch_add(1);
                    *max_hash_size += 1;
                }
                // delete pair in spill
                if (deencode)
                {
                    spill_map_char[ognewi] *= -1;
                }
                else
                {
                    spill_map[ognewi] = ULONG_MAX;
                }
            }
        }

        // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
        if (comb_hash_size.load() * (*avg) + base_size >= memLimit * 0.9)
        {

            unsigned long used_space = newi - input_head;
            if (!deencode)
            {
                used_space *= sizeof(long);
            }

            if (used_space > pagesize * 10)
            {
                // std::cout << threadNumber << ": freeing " << used_space << std::endl;
                //  std::cout << "Freeing up mapping" << std::endl;
                //    calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                unsigned long freed_space_temp = used_space - (used_space % pagesize);
                // std::cout << t_id << ": freeing: " << freed_space_temp << std::endl;
                if (deencode)
                {
                    if (munmap(&spill_map_char[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                    input_head += freed_space_temp;
                }
                else
                {
                    if (munmap(&spill_map[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                    input_head += freed_space_temp / sizeof(long);
                }
                if (diff_diff > (used_space % pagesize))
                {
                    diff->fetch_sub(freed_space_temp);
                    diff_diff -= freed_space_temp;
                }
                else
                {
                    std::cout << "diff_diff too small! " << diff_diff << " - " << used_space % pagesize << std::endl;
                }

                diff_sub_s += freed_space_temp;
                /* else
                {
                    std::cout << "Diff too small 2! diff: " << diff->load() << " - " << freed_space_temp << std::endl;
                } */

                // std::cout << "hashmap size: " << emHashmap.size() * avg << " freed space: " << freed_space_temp << std::endl;
            }
            // if (!locked && used_space <= pagesize * 40 && hmap->size() * (*avg) + base_size >= memLimit * 0.9)
            if ((*max_hash_size) * (*avg) + base_size / conc_threads >= memLimit / conc_threads && hmap->size() >= *max_hash_size * 0.99 && !locked && add && used_space <= pagesize * 40)
            // if (hmap->size() >= *max_hash_size * 0.95 && !locked && add && used_space <= pagesize * 40)
            // if (!locked && add && used_space <= pagesize * 40)
            {
                //  std::cout << "head base: " << i + 1 << std::endl;
                locked = true;
                *input_head_base = i + 1;
            }
            // std::cout << t_id << ": freed" << std::endl;
            // std::cout << "freed " << threadNumber << std::endl;
        }
    }
    // std::cout << "Writing hashmap size: " << emHashmap.size() << std::endl;
    //   std::cout << t_id << ": finishing" << std::endl;
    //  save empty flag and release the mapping
    if (deencode)
    {
        if (mapping_size - input_head > 0)
        {
            if (munmap(&spill_map_char[input_head], mapping_size - input_head) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
            if (diff->load() > diff_diff)
            {
                diff->fetch_sub(diff_diff);
            }
            diff_sub_t += diff_diff;
            /* else
            {
                std::cout << "Diff too small 3! diff: " << diff->load() << " - " << mapping_size - input_head << std::endl;
            } */
        }
    }
    else
    {
        if (mapping_size - input_head * sizeof(long) > 0)
        {
            if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(long)) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
            if (diff->load() > diff_diff)
            {
                diff->fetch_sub(diff_diff);
            }
            /* else
            {
                std::cout << "Diff too small 3! diff: " << diff->load() << " - " << mapping_size - input_head * sizeof(long) << std::endl;
            } */
        }
    }

    // extra_mem -= increase;
    /* if (add)
    {
        *input_head_base = comb_spill_size_temp;
    } */
    if (file_handler != -1)
    {
        close(file_handler);
    }
    // std::cout << "diff add: " << diff_add << " first diff sub: " << diff_sub_f << " second diff sub: " << diff_sub_s << " third diff sub: " << diff_sub_t << std::endl;
    //  std::cout << t_id << ": finished" << std::endl;
    return !locked;
}
/**
 * @brief Add the size of the next x files to the spill head
 *
 * @param spills local spills(pair of filename, size in B)
 * @param spill_head current spill_head
 * @param x number of files to add
 */
void addXtoLocalSpillHead(std::vector<std::pair<std::string, size_t>> *spills, unsigned long *spill_head, int x)
{
    size_t sum = 0;
    for (auto &s : (*spills))
    {
        sum += s.second;
        if (deencode && *spill_head < sum)
        {
            *spill_head = sum;
            x--;
            if (x == 0)
            {
                break;
            }
        }
        else if (!deencode && *spill_head < sum / sizeof(long))
        {
            *spill_head = sum / sizeof(long);
            x--;
            if (x == 0)
            {
                break;
            }
        }
    }
}

/**
 * @brief merge all spills of a partition and write the result into the output file
 *
 * @param hmap hashmap
 * @param spills local spills (pair of filename, size in B)
 * @param comb_hash_size combined size of all hashmaps
 * @param avg memory average of hashmap entry
 * @param memLimit available memory
 * @param diff combined size of all mappings
 * @param outputfilename filename of the output file
 * @param s3spillNames2 s3 spills files (tuple of filename, size in B, subfiles (pair of size in B, number of tuples))
 * @param minio_client aws client
 * @param writeRes whether results should be written to output file or written to S3
 * @param uName Filename of file in S3 if results are written to S3
 * @param backMemLimit avaible space in background memory
 * @param output_file_head start index of output file
 * @param done whether this function has finished
 * @param max_hash_size biggest size of hashmap
 * @param partition partition of files being merged (optional only needed if results are written to S3 or getS3files is true)
 * @param beggarWorker worker owning the files
 * @param increase whether the first input stream buffer should be added to the memory consumption
 * @param gets3Files whether the function should get all available files of the given partition to merge
 *  */
int merge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<std::string, size_t>> *spills,
          std::atomic<unsigned long> &comb_hash_size, float *avg, float memLimit, std::atomic<unsigned long> *diff, std::string &outputfilename,
          std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *s3spillNames2, Aws::S3::S3Client *minio_client,
          bool writeRes, std::string &uName, size_t backMemLimit, size_t *output_file_head, std::list<char>::iterator done, std::list<size_t>::iterator max_hash_size_list, char partition = -1, char beggarWorker = 0, bool increase = false, bool gets3Files = false)
{
    /* emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap;
    // Open the outputfile to write results
    if (writeRes && multiThread_merge)
    {
        hmap = new emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
    }
    else
    {
        hmap = hmap_in;
    } */

    size_t max_hash_size = *max_hash_size_list;
    std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> s3spillNames;
    if (gets3Files)
    {
        getAllMergeFileNames(minio_client, partition, &s3spillNames);
        s3spillNames2 = &s3spillNames;
    }
    auto merge_start_time = std::chrono::high_resolution_clock::now();
    // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
    unsigned long input_head_base = 0;
    bool locked = true;
    unsigned long *spill_map = nullptr;
    char *spill_map_char = nullptr;
    unsigned long comb_spill_size_temp = 0;
    unsigned long overall_size = 0;
    std::atomic<unsigned long> read_lines = 0;
    unsigned long written_lines = 0;
    // unsigned long maxHashsize = hmap->size();
    // comb_hash_size.exchange(comb_hash_size.load() > hmap->size() ? comb_hash_size.load() : hmap->size());
    std::array<unsigned long, max_size> keys = {0, 0};
    std::array<unsigned long, max_size> values = {0, 0};
    size_t mapping_size = 0;
    unsigned long bitmap_size_sum = 0;
    std::vector<size_t> bitmap_sizes;
    std::vector<std::pair<int, std::vector<char>>> s3spillBitmaps;
    int s3spillFile_head = 0;
    int subfile_head = 0;
    int bit_head = 0;
    unsigned long s3spillStart_head = 0;
    unsigned long s3spillStart_head_chars = 0;
    unsigned long overall_s3spillsize = 0;
    std::vector<std::tuple<std::thread, size_t, char>> spillThreads;
    char id_counter = 0;

    for (auto &it : *spills)
    {
        comb_spill_size_temp += it.second;
        // std::cout << it.second << ", ";
    }
    // std::cout << std::endl;
    int counter = 0;
    for (auto &name : *s3spillNames2)
    {
        overall_s3spillsize += get<1>(name);
        for (auto &tuple_num : get<2>(name))
        {
            bitmap_size_sum += std::ceil((float)(tuple_num.second) / 8);
        }
    }
    /*  if (bitmap_size_sum > memLimit * 0.7)
     {
         // std::cout << "Spilling bitmaps with size: " << bitmap_size_sum << std::endl;
         for (auto &name : *s3spillNames2)
         {
             int counter = 0;
             for (auto &s : get<2>(name))
             {
                 size_t size = std::ceil((float)(s.second) / 8);
                 // std::cout << "writing bitmap" << (*s3spillNames)[counter] << "_bitmap" << " with size: " << size << std::endl;
                 //  Spilling bitmaps
                 int fd = open((get<0>(name) + std::to_string(counter) + "_bitmap").c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
                 lseek(fd, size - 1, SEEK_SET);
                 if (write(fd, "", 1) == -1)
                 {
                     close(fd);
                     perror("Error writing last byte of the file");
                     exit(EXIT_FAILURE);
                 }
                 char *spill = (char *)(mmap(nullptr, size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
                 madvise(spill, size, MADV_SEQUENTIAL | MADV_WILLNEED);
                 if (spill == MAP_FAILED)
                 {
                     close(fd);
                     perror("Error mmapping the file in creating a bitmap");
                     exit(EXIT_FAILURE);
                 }
                 for (unsigned long i = 0; i < size; i++)
                 {
                     spill[i] = -1;
                 }
                 munmap(spill, size);
                 s3spillBitmaps.push_back({fd, {}});
                 counter++;
             }
         }
         bitmap_size_sum = 0;
     }
     else
     { */
    // not spilling bitmaps
    for (auto &name : *s3spillNames2)
    {
        for (auto &s : get<2>(name))
        {
            std::vector<char> bitmap = std::vector<char>(std::ceil((float)(s.second) / 8), -1);
            s3spillBitmaps.push_back({-1, bitmap});
        }
    }
    // std::cout << "Keeping bitmaps in mem with size: " << bitmap_size_sum << " Number of bitmaps: " << s3spillBitmaps.size() << std::endl;
    // }
    extra_mem += bitmap_size_sum;
    size_t size_after_init = getPhyValue();
    std::vector<int> write_counter(partitions, 0);
    std::vector<std::vector<std::pair<size_t, size_t>>> write_sizes(partitions);
    std::shared_mutex writeLock;

    bool finished = false;
    bool started_writing = false;
    // bool increase = true;

    while (!finished)
    {
        auto s3spillFile_head_old = s3spillFile_head;
        auto input_head_base_old = input_head_base;
        finished = subMerge(hmap, s3spillNames2, &s3spillBitmaps, spills, true, &s3spillFile_head, &bit_head, &subfile_head, &s3spillStart_head, &s3spillStart_head_chars, &input_head_base,
                            size_after_init, &read_lines, minio_client, &writeLock, avg, memLimit, comb_hash_size, diff, increase, &max_hash_size, 0, 0);

        // std::cout << "Start adding from: " << s3spillFile_head_old << " to " << s3spillFile_head << " subfile_head: " << subfile_head << std::endl;
        //   std::cout << "comb_hash_size: " << comb_hash_size.load() << " max_hash_size: " << *max_hash_size << std::endl;
        //   bit_head_end++;
        increase = false;
        if (!finished)
        {
            size_t n = 0;
            int int_n = 0;
            auto temp_it = s3spillNames2->begin();
            int start_bit_merge = 0;
            if (s3spillFile_head < s3spillNames2->size())
            {
                s3spillFile_head = s3spillFile_head + 1;

                for (int i = 0; i < s3spillFile_head; i++)
                {
                    start_bit_merge += get<2>(*temp_it).size();
                    temp_it++;
                }
            }
            // std::cout << "Start merging from: " << start_bit_merge << std::endl;

            size_t old_input_head_base = input_head_base;
            if (input_head_base > 0)
            {
                addXtoLocalSpillHead(spills, &input_head_base, 1);
            }

            std::cout << "round local spill: " << old_input_head_base << " up to: " << input_head_base << std::endl;
            if (multiThread_subMerge)
            {
                int mergefile_num_temp = std::max(0, (int)(s3spillNames2->size() - s3spillFile_head));
                size_t sum = 0;
                int counter = 0;
                for (auto &s : (*spills))
                {
                    sum += s.second;
                    if (deencode && input_head_base < sum)
                    {
                        mergefile_num_temp += spills->size() - counter;
                        break;
                    }
                    else if (!deencode && input_head_base < sum / sizeof(long))
                    {
                        mergefile_num_temp += spills->size() - counter;
                        break;
                    }
                    counter++;
                }
                int merge_file_num = std::max(2, (int)(std::ceil((float)(mergefile_num_temp) / threadNumber)));
                // std::cout << "mergefile_num_temp: " << mergefile_num_temp << " merge_file_num: " << merge_file_num << " spills->size(): " << spills->size() << std::endl;

                std::vector<std::thread> threads;
                std::vector<int> start_heads(s3spillNames2->size());
                std::vector<int> start_bits(s3spillNames2->size());
                std::vector<unsigned long> start_heads_local(spills->size());
                int s3_start_head = s3spillFile_head;
                int start_bit_head = start_bit_merge;
                counter = 0;
                int t_c = 0;
                while (s3_start_head < s3spillNames2->size())
                {

                    start_heads[counter] = s3_start_head;
                    start_bits[counter] = start_bit_head;
                    // std::cout << "merging s3 from start_head: " << s3_start_head << " bit_start_head: " << start_bit_head;
                    threads.push_back(std::thread(subMerge, hmap, s3spillNames2, &s3spillBitmaps, spills, false, &start_heads[counter], &start_bits[counter], &int_n, &n, &n, &input_head_base,
                                                  size_after_init, &read_lines, minio_client, &writeLock, avg, memLimit, std::ref(comb_hash_size), diff, false, &max_hash_size, t_c, merge_file_num));
                    counter++;
                    t_c++;
                    if (s3_start_head + merge_file_num < s3spillNames2->size())
                    {
                        temp_it = s3spillNames2->begin();
                        std::advance(temp_it, s3_start_head);
                        for (int i = 0; i < merge_file_num; i++)
                        {
                            start_bit_head += get<2>(*temp_it).size();
                            temp_it++;
                        }
                    }
                    s3_start_head += merge_file_num;
                    //  std::cout << " to start_head: " << s3_start_head << " bit_start_head: " << start_bit_head << std::endl;
                }
                if ((s3spillNames2->size() - s3spillFile_head) % merge_file_num > 0 && counter > 0)
                {
                    //   std::cout << "add local spill: " << input_head_base;
                    addXtoLocalSpillHead(spills, &input_head_base, (s3spillNames2->size() - s3spillFile_head) % merge_file_num);
                    //  std::cout << " to " << input_head_base << " by: " << (s3spillNames2->size() - s3spillFile_head) % merge_file_num << std::endl;
                }
                counter = 0;
                while ((deencode && input_head_base < comb_spill_size_temp) || (!deencode && input_head_base * sizeof(long) < comb_spill_size_temp))
                {
                    start_heads_local[counter] = input_head_base;
                    std::cout << "counter: " << counter << " merging local input_head_base: " << input_head_base;
                    threads.push_back(std::thread(subMerge, hmap, s3spillNames2, &s3spillBitmaps, spills, false, &s3_start_head, &start_bit_head, &int_n, &n, &n, &start_heads_local[counter],
                                                  size_after_init, &read_lines, minio_client, &writeLock, avg, memLimit, std::ref(comb_hash_size), diff, false, &max_hash_size, t_c, merge_file_num));
                    counter++;
                    t_c++;
                    addXtoLocalSpillHead(spills, &input_head_base, merge_file_num);
                    std::cout << " to: " << input_head_base << std::endl;
                    // std::cout << "add local spill: " << merge_file_num << " to: " << input_head_base << std::endl;
                }
                // std::cout << "Waiting for threads" << std::endl;
                for (auto &thread : threads)
                {
                    thread.join();
                }
            }
            else
            {
                subMerge(hmap, s3spillNames2, &s3spillBitmaps, spills, false, &s3spillFile_head, &start_bit_merge, &int_n, &n, &n, &input_head_base,
                         size_after_init, &read_lines, minio_client, &writeLock, avg, memLimit, comb_hash_size, diff, false, &max_hash_size, 0, 0);
            }

            s3spillFile_head--;
            input_head_base = old_input_head_base;
        }

        if (writeRes)
        {
            written_lines += hmap->size();
            if (deencode)
            {
                std::cout << "Writing hmap with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head_chars: " << s3spillStart_head_chars << " avg " << *avg << " base_size: " << base_size << " locked: " << locked << std::endl;
                // std::cout << "output_file_head: " << *output_file_head << std::endl;
            }
            else
            {
                std::cout << "Writing hmap with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head: " << s3spillStart_head << " avg " << *avg << " base_size: " << base_size << std::endl;
            }
            bool asdf = false;
            /* writing_ouput.lock();
            *output_file_head += calc_outputSize(hmap);
            std::cout << "calc output: " << test << std::endl;
            *output_file_head = writeHashmap(hmap, *output_file_head, pagesize * 30, outputfilename);
            writing_ouput.unlock(); */
            writeHashmap(hmap, output_file_head, pagesize * 30, outputfilename);
            hmap->clear();
            // std::cout << "locked: " << locked << std::endl;
            // comb_hash_size = maxHashsize;
        }
        else
        {
            if (!finished || started_writing)
            {
                auto spill_start_time = std::chrono::high_resolution_clock::now();
                started_writing = true;
                size_t spill_size = hmap->size() * sizeof(long) * (key_number + value_number);
                size_t comb_spill_temp = 0;
                std::string local_spill_name = "local_mergeSpill_";
                for (auto &ls : spillThreads)
                {
                    comb_spill_temp += get<1>(ls);
                }
                written_lines += hmap->size();
                spillS3Hmap(&(*hmap), minio_client, &write_sizes, uName, &write_counter, partition);

                std::cout << "Writing hashmap with size " << hmap->size() << " subfiles of file " << (uName + "_" + std::to_string(partition)) << ":\n";
                for (auto write_size : write_sizes[partition])
                {
                    std::cout << write_size.first << ":" << write_size.second << "\n";
                }
                std::cout << std::endl;
                /*if (backMemLimit < backMem_usage + spill_size + comb_spill_temp)
                {
                    if (backMemLimit <= backMem_usage + spill_size)
                    {
                        // std::cout << "Writing file: " << uName << std::endl;
                        // std::cout << "Writing hmap to " << uName << " with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head_chars: " << s3spillStart_head_chars << " avg " << *avg << " base_size: " << base_size << std::endl;
                        spillS3Hmap(hmap, minio_client, &write_sizes, uName, &write_counter, partition);
                    }
                     else
                    {
                        size_t removed_mem = 0;
                        while (spill_size > removed_mem)
                        {
                            get<0>(spillThreads[0]).join();
                            char id = get<2>(spillThreads[0]);

                            removed_mem += get<1>(spillThreads[0]);
                            remove((local_spill_name + std::to_string((int)(id))).c_str());

                            spillThreads.erase(spillThreads.begin());
                        }
                        std::pair<int, size_t> spill_file(-1, 0);
                        std::string temp_local_name = local_spill_name + std::to_string((int)(id_counter));
                        spillToFile(hmap, &spill_file, 0, pagesize * 10, temp_local_name);
                        spillThreads.push_back({std::thread(spillS3File, spill_file, minio_client, &write_sizes, uName, &write_counter), spill_size, id_counter});
                        id_counter++;
                    }
                }
                else
                {
                    std::pair<int, size_t> spill_file(-1, 0);
                    std::string temp_local_name = local_spill_name + std::to_string((int)(id_counter));
                    spillToFile(hmap, &spill_file, 0, pagesize * 10, temp_local_name);
                    spillThreads.push_back({std::thread(spillS3File, spill_file, minio_client, &write_sizes, uName, &write_counter), spill_size, id_counter});
                    id_counter++;
                } */
                hmap->clear();
                if (finished)
                {
                    // for (auto &thread : spillThreads)
                    //{
                    //    get<0>(thread).join();
                    //    remove((local_spill_name + std::to_string((int)(get<2>(thread)))).c_str());
                    //}
                    size_t write_size = 0;
                    for (auto &w_size : write_sizes[partition])
                    {
                        write_size += w_size.first;
                    }
                    if (write_size > 0)
                    {
                        std::string n_temp = uName + "_" + std::to_string(partition);

                        file temp_file;
                        temp_file.name = uName + "_" + std::to_string(partition);
                        temp_file.size = write_size;
                        temp_file.status = 0;
                        temp_file.subfiles = write_sizes[partition];
                        std::cout << "Adding file " << (uName + "_" + std::to_string(partition)) << " to mana. subdfiles:\n";
                        for (auto write_size : write_sizes[partition])
                        {
                            std::cout << write_size.first << ":" << write_size.second << "\n";
                        }
                        std::cout << std::endl;
                        std::vector<std::pair<file, char>> files = std::vector<std::pair<file, char>>(1, {temp_file, partition});
                        // std::cout << "Adding merge file: " << n_temp << " partition: " << partition << " write size: " << write_size << std::endl;
                        addFileToManag(minio_client, files, beggarWorker, 255);
                        // std::cout << "Finished adding file" << std::endl;
                    }
                }
                write_log_file_lock.lock();
                log_file.sizes["mergeHelp_spilling_Duration"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - spill_start_time).count();
                write_log_file_lock.unlock();
            }
            else
            {
                write_log_file_lock.lock();
                log_file.sizes["linesRead"] += read_lines;
                write_log_file_lock.unlock();
                return true;
            }
        }
    }
    std::cout << "spills size: " << spills->size();
    for (auto &it : *spills)
    {
        remove(it.first.c_str());
    }
    /*for (auto &it : s3spillBitmaps)
    {
        if (it.second.empty())
        {
            close(it.first);
        }
    }
     for (auto &it : *s3spillNames2)
    {
        for (int k = 0; k < get<2>(it).size(); k++)
        {
            remove((get<0>(it) + "_" + std::to_string(k)).c_str());
        }
    } */

    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - merge_start_time).count()) / 1000000;
    // std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << " Read lines: " << read_lines << ". macroseconds/line: " << duration * 1000000 / read_lines << std::endl;
    write_log_file_lock.lock();
    log_file.sizes["linesRead"] += read_lines;
    log_file.sizes["linesWritten"] += written_lines;
    write_log_file_lock.unlock();
    *done = 1;
    extra_mem -= bitmap_size_sum;
    *max_hash_size_list = max_hash_size;
    /* if (writeRes && multiThread_merge)
    {
        delete hmap;
    } */
    return 1;
}

int merge2(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<std::string, size_t>> *spills,
           std::atomic<unsigned long> &comb_hash_size, float *avg, float memLimit, std::atomic<unsigned long> *diff, std::string &outputfilename,
           std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *s3spillNames2, Aws::S3::S3Client *minio_client,
           bool writeRes, std::string &uName, size_t backMemLimit, size_t *output_file_head, char *done, size_t *max_hash_size, char partition = -1, char beggarWorker = 0, bool increase = false, bool gets3Files = false)
{
    std::list<size_t> max_HashSizes;
    max_HashSizes.push_back(*max_hash_size);
    std::list<char> mergeThreads_done;
    mergeThreads_done.push_back(*done);
    return merge(hmap, spills, comb_hash_size, avg, memLimit, diff, outputfilename, s3spillNames2, minio_client, writeRes,
                 uName, backMemLimit, output_file_head, mergeThreads_done.begin(), max_HashSizes.begin(), partition, beggarWorker, increase, gets3Files);
}

/**
 * @brief set the status of file in the Mana file
 *
 * @param minio_client aws client
 * @param file_stati map of filename and status
 * @param worker_id worker owning the files
 * @param partition_id partition the files are in
 * @param thread_id thread id calling this function
 */
void setFileStatus(Aws::S3::S3Client *minio_client, std::unordered_map<std::string, char> *file_stati, char worker_id, char partition_id, char thread_id)
{
    manaFile mana = getLockedMana(minio_client, thread_id);
    for (auto &worker : mana.workers)
    {
        if (worker.id == worker_id && !worker.locked)
        {
            for (auto &partition : worker.partitions)
            {
                if (partition.id == partition_id)
                {
                    for (auto &w_file : partition.files)
                    {
                        for (auto &f_name : *file_stati)
                        {
                            if (w_file.name == f_name.first)
                            {
                                w_file.status = f_name.second;
                            }
                        }
                    }
                    writeMana(minio_client, mana, true);
                    return;
                }
            }
        }
    }
    writeMana(minio_client, mana, true);
}
/**
 * @brief execute the helpMerge where available files are taken from S3, merged and written back to S3
 *
 * @param memLimit available memory
 * @param backMemLimit available background memory
 * @param minio_client aws client
 * @param thread_id id of thread
 * @param hmap hashmap
 * @param comb_hash_size combined size of all hashmaps
 * @param diff combined size of all mappings
 * @param avg average size of entries in the hashmap
 * @param startbeggarWorker first worker id this worker should help
 */
void helpMergePhase(size_t memLimit, size_t backMemLimit, Aws::S3::S3Client minio_client, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, char thread_id,
                    std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> &diff, float *avg)
{

    std::vector<std::string> blacklist;

    char beggarWorker = 0;
    char partition_id = partition_id;
    std::string uName = std::to_string((int)(thread_id));
    uName += "_merge";
    std::string empty_string = "";
    int counter = 0;
    std::tuple<std::vector<file>, char, char> files;
    std::get<0>(files) = std::vector<file>(0);
    bool second_loaded = false;
    std::vector<std::string> file_names;
    std::thread minioSpiller;
    std::string first_fileName;
    std::string local_spillName = std::to_string((int)(thread_id));
    local_spillName += "_helpMergeSpill";
    bool b_minioSpiller = false;
    std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> spills = {};
    size_t zero = 0;
    size_t max_hashSize = 0;
    auto mergeHelp_start_time = std::chrono::high_resolution_clock::now();

    while (true)
    {
        std::cout << std::to_string((int)(thread_id)) << ": getting merge file names" << std::endl;
        spills.clear();
        char file_num = hmap->size() == 0 ? 2 : 1;
        getMergeFileName(&minio_client, beggarWorker, partition_id, &blacklist, &files, thread_id, file_num);
        if (get<1>(files) == 0)
        {
            if (hmap->size() > 0)
            {
                std::cout << std::to_string((int)(thread_id)) << ": spilling hmap" << std::endl;
                uName = worker_id;
                uName += "_";
                uName += std::to_string((int)(thread_id));
                uName += "_merge_" + std::to_string(counter);
                std::vector<std::pair<std::string, size_t>> local_files(partitions, {"", 0});
                // std::cout << "spilling to " << uName << " hmap size: " << hmap->size() << std::endl;
                auto spill_start_time = std::chrono::high_resolution_clock::now();
                spillToMinio(hmap, local_files, uName, &minio_client, beggarWorker, 0, 1);
                counter++;
                write_log_file_lock.lock();
                log_file.sizes["mergeHelp_spilling_Duration"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - spill_start_time).count();
                // Try to change beggar worker or load in new files
                log_file.sizes["linesWritten"] += hmap->size();
                write_log_file_lock.unlock();
                hmap->clear();
                std::unordered_map<std::string, char> file_stati;
                for (auto &f_name : file_names)
                {
                    file_stati.insert({f_name, 255});
                }
                setFileStatus(&minio_client, &file_stati, beggarWorker, partition_id, thread_id);
                file_names.clear();
                beggarWorker = 0;
                partition_id = 0;
            }
            bool finish = false;
            while (!finish)
            {
                // std::cout << "getting Mana" << std::endl;
                manaFile m = getMana(&minio_client);
                // std::cout << "got Mana" << std::endl;
                bool found_files = false;
                for (auto &w : m.workers)
                {
                    if (!found_files && !w.locked)
                    {
                        for (auto &p : w.partitions)
                        {
                            int file_num_temp = 0;
                            for (auto &f : p.files)
                            {
                                if (f.status == 0)
                                {
                                    file_num_temp++;
                                }
                            }
                            if (file_num_temp >= minFileNumMergeHelper && !p.lock)
                            {
                                found_files = true;
                                break;
                            }
                        }
                    }
                    if (w.id == '1' && w.locked)
                    {
                        std::cout << "finish" << std::endl;
                        finish = true;
                        found_files = false;
                        break;
                    }
                }
                // std::cout << "found file: " << found_files << std::endl;
                if (found_files)
                {
                    getMergeFileName(&minio_client, beggarWorker, partition_id, &blacklist, &files, thread_id, file_num);

                    if (get<1>(files) != 0)
                    {
                        // std::cout << "breaking" << std::endl;
                        break;
                    }
                }
                usleep(1000000);
            }
            if (finish)
            {
                break;
            }
        }
        beggarWorker = get<1>(files);
        partition_id = get<2>(files);
        manaFile mana = getMana(&minio_client);
        for (auto &w : mana.workers)
        {
            if (w.id == beggarWorker)
            {
                partitions = w.partitions.size();
                break;
            }
        }

        std::vector<std::pair<std::string, size_t>> empty(0);
        std::cout << "Worker: " << beggarWorker << "; Partition: " << (int)(partition_id) << "; merging files: ";
        size_t col_tuple_num = 0;
        for (auto &merge_file : get<0>(files))
        {
            for (auto sf : merge_file.subfiles)
            {
                col_tuple_num += sf.second;
            }
            file_names.push_back(merge_file.name);
            spills.insert({merge_file.name, merge_file.size, merge_file.subfiles});
            std::cout << merge_file.name << ", ";
            write_log_file_lock.lock();
            if (merge_file.name.substr(2, 5) == "merge")
            {
                log_file.sizes["mergedFiles"]++;
            }
            else
            {
                log_file.sizes["remergedFiles"]++;
            }
            write_log_file_lock.unlock();
        }
        std::cout << std::endl;
        size_t first_tuple_num = 0;
        for (auto sf : get<0>(files)[0].subfiles)
        {
            first_tuple_num += sf.second;
        }
        write_log_file_lock.lock();
        log_file.mergeHelp_merge_tuple_num.push_back({first_tuple_num, col_tuple_num - first_tuple_num});
        write_log_file_lock.unlock();
        uName = worker_id;
        uName += "_";
        uName += std::to_string((int)(thread_id));
        uName += "_merge_" + std::to_string(counter);
        char temp = true;

        size_t spill_time_old = log_file.sizes["mergeHelp_spilling_Duration"];
        auto spill_start_time = std::chrono::high_resolution_clock::now();
        std::cout << std::to_string((int)(thread_id)) << ": merging" << std::endl;
        merge2(hmap, &empty, comb_hash_size, avg, memLimit, &diff, empty_string, &spills, &minio_client, false, uName, backMemLimit, &zero, &temp, &max_hashSize, partition_id, beggarWorker);
        std::cout << std::to_string((int)(thread_id)) << ": merged" << std::endl;
        write_log_file_lock.lock();
        log_file.sizes["mergeHelp_merging_Duration"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - spill_start_time).count() - (log_file.sizes["mergeHelp_spilling_Duration"] - spill_time_old);
        // std::cout << "Merge finished" << std::endl;
        write_log_file_lock.unlock();
        if (hmap->size() == 0)
        {
            std::cout << std::to_string((int)(thread_id)) << ": setting stati" << std::endl;
            std::unordered_map<std::string, char> file_stati;
            for (auto &f_name : file_names)
            {
                file_stati.insert({f_name, 255});
            }
            file_stati.insert({uName, 0});
            setFileStatus(&minio_client, &file_stati, beggarWorker, partition_id, thread_id);
            file_names.clear();
            beggarWorker = 0;
            partition_id = 0;
        }
        counter++;
    }
    if (b_minioSpiller)
    {
        minioSpiller.join();
    }
    write_log_file_lock.lock();
    if (log_file.sizes["linesRead"] > 0)
    {
        log_file.sizes["selectivity"] = (log_file.sizes["linesWritten"] * 1000) / log_file.sizes["linesRead"];
    }
    while (mana_writeThread_num.load() != 0)
    {
    }
    log_file.sizes["mergeHelpDuration"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - mergeHelp_start_time).count();
    write_log_file_lock.unlock();
    std::cout << "End of helping Phase" << std::endl;
}

void helpMerge(size_t memLimit, size_t backMemLimit, Aws::S3::S3Client minio_client)
{
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmaps[threadNumber];
    std::atomic<unsigned long> comb_hash_size = 0;
    std::atomic<unsigned long> diff = 0;
    std::vector<std::thread> threads(threadNumber);
    float avg = 1;
    int finished = 0;
    std::thread sizePrinter;
    sizePrinter = std::thread(printSize, std::ref(finished), memLimit, std::ref(comb_hash_size), &diff, &avg);
    for (char i = 0; i < threadNumber; i++)
    {
        threads[i] = std::thread(helpMergePhase, memLimit / threadNumber, backMemLimit / threadNumber, minio_client, &emHashmaps[i], i, std::ref(comb_hash_size), std::ref(diff), &avg);
    }
    for (char i = 0; i < 1; i++)
    {
        threads[i].join();
    }
    finished = 2;
    sizePrinter.join();
}

char getSplitMergePartition(Aws::S3::S3Client *minio_client)
{
    manaFile mana_worker = getMana(minio_client, worker_id);
    int min_fileNumber = 100000;
    int b_min_fileNumber = 100000;
    char partition = -1;
    char partition_b = -1;
    for (auto &p : mana_worker.workers[0].partitions)
    {
        int temp_fileNumber = 0;
        bool b_part = false;
        if (!p.lock)
        {
            manaFile mana_partition = getMana(minio_client, worker_id, p.id);
            for (auto &p_file : mana_partition.workers[0].partitions[0].files)
            {
                if (p_file.status == 0)
                {
                    temp_fileNumber++;
                }
                else if (p_file.status != 255)
                {
                    b_part = true;
                    temp_fileNumber++;
                }
            }
            if (!b_part && min_fileNumber > temp_fileNumber)
            {
                partition = p.id;
                min_fileNumber = temp_fileNumber;
            }
            else if (b_part && b_min_fileNumber > temp_fileNumber)
            {
                partition_b = p.id;
                b_min_fileNumber = temp_fileNumber;
            }
        }
    }
    if (partition == -1)
    {
        if (partition_b == -1)
        {
            return -1;
        }
        else
        {
            partition = partition_b;
        }
    }
    manaFile mana = getLockedMana(minio_client, 0, worker_id);
    for (auto &p : mana.workers[0].partitions)
    {
        if (p.id == partition)
        {
            p.lock = true;
            writeMana(minio_client, mana, true, worker_id);
            return partition;
        }
    }
    return -1;
}

/**
 * @brief Get the partition the main worker should first merge and write to the output
 *
 * @param minio_client aws client
 *
 * @result partition the worker should merge
 */
char getMergePartition(Aws::S3::S3Client *minio_client)
{
    if (split_mana)
    {
        return getSplitMergePartition(minio_client);
    }
    manaFile mana = getLockedMana(minio_client, 0);
    int min_fileNumber = 100000;
    int b_min_fileNumber = 100000;
    char partition = -1;
    char partition_b = -1;
    for (auto &w : mana.workers)
    {
        if (worker_id == w.id)
        {
            for (auto &p : w.partitions)
            {
                int temp_fileNumber = 0;
                bool b_part = false;
                if (!p.lock)
                {
                    for (auto &p_file : p.files)
                    {
                        if (p_file.status == 0)
                        {
                            temp_fileNumber++;
                        }
                        else if (p_file.status != 255)
                        {
                            b_part = true;
                            temp_fileNumber++;
                        }
                    }
                    if (!b_part && min_fileNumber > temp_fileNumber)
                    {
                        partition = p.id;
                        min_fileNumber = temp_fileNumber;
                    }
                    else if (b_part && b_min_fileNumber > temp_fileNumber)
                    {
                        partition_b = p.id;
                        b_min_fileNumber = temp_fileNumber;
                    }
                }
            }
            break;
        }
    }
    if (partition == -1)
    {
        if (partition_b == -1)
        {
            writeMana(minio_client, mana, true);
            return partition;
        }
        else
        {
            partition = partition_b;
        }
    }
    for (auto &w : mana.workers)
    {
        if (worker_id == w.id)
        {
            for (auto &p : w.partitions)
            {
                if (p.id == partition)
                {
                    p.lock = true;
                    writeMana(minio_client, mana, true);
                    return partition;
                }
            }
        }
    }
    return partition;
}

/**
 * @brief aggregate inputfilename and write results into outpufilename
 *
 * @param inputfilename file name of the input file
 * @param outputfilename file name of the output file
 * @param memLimit available memory
 * @param minio_client aws client
 * @param memLimitBack available background memory
 */
int aggregate(std::string inputfilename, std::string outputfilename, size_t memLimit, Aws::S3::S3Client minio_client, size_t memLimitBack)
{
    // Inits and decls

    // open inputfile and get size from stats
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "Input file size: " << stats.st_size << std::endl;
    std::pair<int, int> spill_file = std::pair<int, int>(-1, 0);
    std::unordered_map<std::string, std::string> lineObjects;

    // https://martin.ankerl.com/2022/08/27/hashmap-bench-01/#emhash8__HashMap

    // ankerl::unordered_dense::segmented_map<int, int> ankerlHashmap;

    // https://github.com/ktprime/emhash/tree/master

    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmaps[threadNumber];
    std::vector<std::vector<std::pair<std::string, size_t>>> spills(0);
    std::atomic<unsigned long> numLines = 0;
    std::atomic<unsigned long> readBytes = 0;
    std::atomic<unsigned long> comb_hash_size = 0;
    std::atomic<unsigned long> diff = 0;
    size_t t1_size = size / threadNumber - (size / threadNumber % pagesize);
    size_t t2_size = size - t1_size * (threadNumber - 1);
    std::cout << "t1 size: " << t1_size << " t2 size: " << t2_size << std::endl;
    std::vector<std::thread> threads;
    std::vector<std::string> s3Spill_names;

    float avg = 1;
    int readingMode = -1;
    int finished = 0;
    std::thread sizePrinter;
    sizePrinter = std::thread(printSize, std::ref(finished), memLimit, std::ref(comb_hash_size), &diff, &avg);

    // auto scan_start_time = std::chrono::high_resolution_clock::now();
    char id = 0;

    for (int i = 0; i < threadNumber - 1; i++)
    {
        emHashmaps[i] = {};
        threads.push_back(std::thread(fillHashmap, id, &emHashmaps[i], fd, t1_size * i, t1_size, true, memLimit / threadNumber,
                                      std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitBack));
        id++;
    }
    emHashmaps[threadNumber - 1] = {};
    threads.push_back(std::thread(fillHashmap, id, &emHashmaps[threadNumber - 1], fd, t1_size * (threadNumber - 1), t2_size, false, memLimit / threadNumber,
                                  std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitBack));

    while ((float)(readBytes.load()) / size < 0.99)
    {
        // std::cout << readBytes.load() << std::endl;
        printProgressBar((float)(readBytes.load()) / size);
        usleep(100);
    }
    printProgressBar(1);
    std::cout << std::endl;

    for (auto &thread : threads)
    {
        thread.join();
    }
    close(fd);

    unsigned long temp_loc_spills = 0;

    for (auto &it : spills)
    {
        for (auto &itt : it)
        {
            temp_loc_spills += itt.second;
        }
    }
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Scanning finished with time: " << duration << "s. Scanned Lines: " << numLines << ". macroseconds/line: " << duration * 1000000 / numLines << " Overall spill: " << comb_spill_size << "B. Spill to Background Memory: " << temp_loc_spills << "B. Spill to S3: " << comb_spill_size - temp_loc_spills << std::endl;
    log_file.sizes["scanTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    log_file.sizes["scanDuration"] = duration;
    log_file.sizes["colS3Spill"] = comb_spill_size - temp_loc_spills;
    log_file.sizes["colBackSpill"] = temp_loc_spills;
    log_file.sizes["inputLines"] = numLines;
    size_t w_lines = 0;
    for (auto &thread : log_file.threads)
    {
        w_lines += thread.sizes["outputLines"];
    }
    log_file.sizes["selectivityPostScan"] = (w_lines * 1000) / numLines;

    auto mergeH_start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> spill_threads;
    std::string empty = "";
    std::vector<std::string> uNames;
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmap;
    std::vector<std::vector<std::pair<std::string, size_t>>> local_spill_files_temp(threadNumber, std::vector<std::pair<std::string, size_t>>(partitions, {"", 0}));

    bool keep_hashmaps = partitions == 1 || (float)(comb_spill_size.load()) / size < 0.1;
    std::cout << "keep hashmaps: " << keep_hashmaps << ": " << partitions << " == 1 || " << (float)(comb_spill_size.load()) / size << " < 0.1" << std::endl;
    if (keep_hashmaps)
    {
        for (int i = 0; i < threadNumber; i++)
        {
            for (auto &tuple : emHashmaps[i])
            {
                if (emHashmap.contains(tuple.first))
                {
                    for (int k = 0; k < value_number; k++)
                    {
                        emHashmap[tuple.first][k] += tuple.second[k];
                    }
                }
                else
                {
                    emHashmap.insert_unique(tuple);
                }
            }
            comb_hash_size.fetch_sub(emHashmaps[i].size());
            emHashmaps[i] = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
        }
        comb_hash_size.exchange(emHashmap.size());
        // emHashmaps[0] = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
    }
    else
    {
        for (int i = 0; i < threadNumber; i++)
        {

            size_t s_size = emHashmaps[i].size() * sizeof(long) * (key_number + value_number);
            std::string uName = "spill_" + std::to_string(i);
            if (backMem_usage + size < memLimitBack)
            {
                for (int p = 0; p < partitions; p++)
                {
                    local_spill_files_temp[i][p] = {uName + "_" + std::to_string(p), 0};
                }
                spill_threads.push_back(std::thread(spillToFile, &emHashmaps[i], &local_spill_files_temp[i], 0, pagesize * 20));
            }
            else
            {
                std::vector<std::pair<std::string, size_t>> local_files(partitions, {"", 0});
                spill_threads.push_back(std::thread(spillToMinio, &emHashmaps[i], local_files, uName, &minio_client, worker_id, 0, i));
            }
            //}
            // delete &emHashmaps[i];
            // emHashmaps[i].clear();
        }
        for (auto &thread : spill_threads)
        {
            thread.join();
        }
        for (int i = 0; i < threadNumber; i++)
        {
            if (local_spill_files_temp[i][0].first != "")
            {
                for (int p = 0; p < partitions; p++)
                {
                    spills[p].push_back(local_spill_files_temp[i][p]);
                }
            }
            emHashmaps[i] = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
        }
        while (mana_writeThread_num.load() != 0)
        {
        }
        comb_hash_size.exchange(0);
    }
    diff.exchange(0);
    // delete[] emHashmaps;
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - mergeH_start_time).count()) / 1000000;
    std::cout << "Merging of hastables finished with time: " << duration << "s." << std::endl;
    log_file.sizes["mergeHashTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    log_file.sizes["mergeHashDuration"] = duration;
    finished++;

    /* if (mergePhase)
    {
        auto colmerge_start_time = std::chrono::high_resolution_clock::now();
        helpMergePhase(memLimit, memLimitBack, minio_client, false, &emHashmap, comb_hash_size, diff, &avg, worker_id);
        std::cout << "Checking if spills are being worked on." << std::endl;
        bool isWorkedOn = true;
        while (isWorkedOn)
        {
            isWorkedOn = false;
            manaFile m = getMana(&minio_client);
            for (auto &worker : m.workers)
            {
                if (worker.id == worker_id)
                {
                    for (auto &partition : worker.partitions)
                    {
                        for (auto &f : partition.files)
                        {
                            if (f.status != 0 && f.status != 255)
                            {
                                isWorkedOn = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - colmerge_start_time).count()) / 1000000;
        std::cout << "Collective Merge ended with time: " << duration << "s." << std::endl;
        log_file.sizes["mergeColTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
        log_file.sizes["mergeColDuration"] = duration;
    }*/
    auto merge_start_time = std::chrono::high_resolution_clock::now();

    // Free up rest of mapping of input file and close the file
    close(fd);

    // Open the outputfile to write results
    int temp = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    close(temp);
    unsigned long written_lines = 0;
    unsigned long read_lines = 0;
    comb_hash_size = emHashmap.size();
    unsigned long freed_mem = 0;
    unsigned long overall_size = 0;
    manaFile mana = getMana(&minio_client, worker_id);
    bool s3spilled;
    for (auto &worker : mana.workers)
    {
        if (worker.id == worker_id)
        {
            s3spilled = !worker.partitions.empty();
            break;
        }
    }

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (!spills.empty() || s3spilled)
    {
        if (!dynamic_extension && !keep_hashmaps)
        {
            size_t p_size = (spillTuple_number.load() / partitions) * avg + max_s3_spill_size;
            size_t available_mem = memLimit - base_size;

            mergeThreads_number = std::floor(available_mem / (p_size * thread_efficiency));
            std::cout << "calc thread number: " << mergeThreads_number << ": ceil(" << available_mem << " / (" << p_size << " * " << thread_efficiency << "))" << std::endl;
        }
        size_t output_file_head = 0;
        bool add_new_thread = false;
        std::vector<char> restarted_threads(mergeThreads_number, 0);
        std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> files;
        std::list<size_t> max_HashSizes(mergeThreads_number, 0);
        std::list<char> thread_bitmap(mergeThreads_number, 0);
        std::list<std::vector<std::pair<std::string, size_t>> *> multi_spills(mergeThreads_number);
        std::list<emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *> mHmaps(mergeThreads_number);
        for (auto &h : mHmaps)
        {
            h = new emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
        }
        char m_partition = 0;
        int counter = 0;
        char done = 0;
        size_t max_hmap_size = 0;

        std::list<std::thread> merge_threads(mergeThreads_number);
        std::list<char> mergeThreads_done(mergeThreads_number, 1);
        if (spills.size() == 0)
        {
            spills.push_back(std::vector<std::pair<std::string, size_t>>(0));
        }
        if (keep_hashmaps)
        {
            std::vector<std::pair<std::string, size_t>> m_spill;
            for (int s_ind = 0; s_ind < spills.size(); s_ind++)
            {
                for (auto s : spills)
                {
                    for (auto ss : s)
                    {
                        m_spill.push_back(ss);
                    }
                }
            }
            getAllMergeFileNames(&minio_client, -1, &files);
            merge2(&emHashmap, &m_spill, comb_hash_size, &avg, memLimit, &diff, outputfilename, &files, &minio_client, true, empty, memLimitBack, &output_file_head, &done, &max_hmap_size, -1, -1, 0);
        }
        else
        {

            /* for (auto &s : spills)
            {
                std::cout << "partition:\n   ";
                for (auto f : s)
                {
                    std::cout << f.first << ":" << f.second << " , ";
                }
                std::cout << std::endl;
            } */
            while (m_partition != -1)
            {
                bool increase = false;
                if (multiThread_merge)
                {
                    bool thread_done = false;
                    for (auto &d : mergeThreads_done)
                    {
                        if (d)
                        {
                            thread_done = true;
                            break;
                        }
                    }
                    std::cout << std::endl;
                    if (dynamic_extension && !thread_done && add_new_thread && (comb_hash_size * avg + base_size) / thread_bitmap.size() < memLimit - (comb_hash_size * avg + base_size))
                    {
                        std::cout << "Adding new Thread" << std::endl;
                        max_HashSizes.push_back(0);
                        thread_bitmap.push_back(0);
                        std::vector<std::pair<std::string, size_t>> temp;
                        multi_spills.push_back(&temp);
                        mHmaps.push_back(new emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>());
                        char m_partition = 0;
                        int counter = 0;
                        add_new_thread = false;
                        restarted_threads.push_back(0);

                        merge_threads.push_back(std::thread());
                        mergeThreads_done.push_back(1);
                        mergeThreads_number++;
                    }
                    int newThread_ind = -1;

                    while (newThread_ind == -1)
                    {
                        int thread_ind_counter = 0;
                        for (auto &d : mergeThreads_done)
                        {
                            if (d)
                            {
                                newThread_ind = thread_ind_counter;
                                auto bitmap_it = std::next(thread_bitmap.begin(), newThread_ind);
                                if (*bitmap_it == 1)
                                {
                                    auto thread_it = std::next(merge_threads.begin(), newThread_ind);
                                    thread_it->join();
                                    *bitmap_it = 0;
                                    restarted_threads[newThread_ind] = 1;
                                    bool temp_bool = true;
                                    for (auto &b : restarted_threads)
                                    {
                                        if (!b)
                                        {
                                            temp_bool = false;
                                            break;
                                        }
                                    }
                                    add_new_thread = temp_bool;
                                }
                                else
                                {
                                    increase = true;
                                }
                                d = 0;

                                break;
                            }
                            thread_ind_counter++;
                        }
                    }
                    // multi_files[newThread_ind].clear();
                    if (s3spilled)
                    {
                        m_partition = getMergePartition(&minio_client);
                    }
                    else if (!spills.empty())
                    {
                        if (counter < partitions)
                        {
                            m_partition = counter;
                        }
                        else
                        {
                            m_partition = -1;
                        }
                    }

                    if (m_partition != -1)
                    {
                        std::cout << "merging partition: " << (int)(m_partition) << std::endl;
                        auto multi_it = std::next(multi_spills.begin(), newThread_ind);

                        *multi_it = spills.size() == 1 ? &spills[0] : &spills[m_partition];

                        auto bit_it = std::next(thread_bitmap.begin(), newThread_ind);
                        *bit_it = 1;

                        auto done_it = std::next(mergeThreads_done.begin(), newThread_ind);

                        auto max_it = std::next(max_HashSizes.begin(), newThread_ind);

                        auto thread_it = std::next(merge_threads.begin(), newThread_ind);

                        auto mHmaps_it = std::next(mHmaps.begin(), newThread_ind);

                        printProgressBar((float)(counter) / partitions);

                        // getAllMergeFileNames(&minio_client, m_partition, &multi_files[newThread_ind]);
                        /* merge_threads[newThread_ind] = std::thread(merge, &merge_emHashmaps[newThread_ind], multi_spills[newThread_ind], std::ref(comb_hash_size), &avg, memLimit, &diff, std::ref(outputfilename), &multi_files[newThread_ind],
                                                                   &minio_client, true, std::ref(empty), memLimitBack, &output_file_head, &mergeThreads_done[newThread_ind], &max_HashSizes[newThread_ind], m_partition, 0, increase, true); */
                        *thread_it = std::thread(merge, *mHmaps_it, *multi_it, std::ref(comb_hash_size), &avg, memLimit, &diff, std::ref(outputfilename), &files,
                                                 &minio_client, true, std::ref(empty), memLimitBack, &output_file_head, done_it, max_it, m_partition, 0, increase, true);
                    }
                }
                else
                {
                    if (s3spilled)
                    {
                        m_partition = getMergePartition(&minio_client);
                    }
                    else if (!spills.empty())
                    {
                        if (counter < partitions)
                        {
                            m_partition = counter;
                        }
                        else
                        {
                            m_partition = -1;
                        }
                    }
                    if (m_partition != -1)
                    {
                        std::cout << "merging partition: " << (int)(m_partition) << std::endl;
                        printProgressBar((float)(counter) / partitions);
                        files.clear();
                        getAllMergeFileNames(&minio_client, m_partition, &files);
                        auto *m_spill = spills.size() == 1 ? &spills[0] : &spills[m_partition];

                        /* for (auto &name : files)
                        {
                            std::cout << std::get<0>(name) << ", ";
                        }
                        std::cout << std::endl;
                        std::string empty = "";
                        std::cout << "output file head: " << output_file_head << std::endl; */
                        merge2(&emHashmap, m_spill, comb_hash_size, &avg, memLimit, &diff, outputfilename, &files, &minio_client, true, empty, memLimitBack, &output_file_head, &done, &max_hmap_size, m_partition, -1, 0);
                    }
                }
                // std::cout << " max_HashSizes[0]: " << max_HashSizes[0] << std::endl;

                counter++;
            }
        }
        mana = getLockedMana(&minio_client, 0);
        for (auto &worker : mana.workers)
        {
            if (worker.id == worker_id)
            {
                worker.locked = true;
                break;
            }
        }
        writeMana(&minio_client, mana, true);

        if (multiThread_merge && !keep_hashmaps)
        {
            for (int i = 0; i < thread_bitmap.size(); i++)
            {
                auto bitmap_it = thread_bitmap.begin();
                std::advance(bitmap_it, i);
                if (*bitmap_it == 1)
                {
                    std::cout << "waiting for thread: " << i << std::endl;
                    auto thread_it = merge_threads.begin();
                    std::advance(thread_it, i);
                    thread_it->join();
                    // merge_threads[i].join();
                }
            }
            for (auto &h : mHmaps)
            {
                delete h;
            }
        }
    }
    else
    {
        // std::cout << "writing to output file" << std::endl;
        written_lines += emHashmap.size();

        // write hashmap to output file
        writeHashmap(&emHashmap, 0, pagesize * 10, outputfilename);
    }
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - merge_start_time).count()) / 1000000;
    std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << std::endl;
    log_file.sizes["mergeDuration"] = duration;
    log_file.sizes["mergeTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    log_file.sizes["selectivity"] = (log_file.sizes["linesWritten"] * 1000) / log_file.sizes["inputLines"];
    log_file.sizes["mergeThread_number"] = mergeThreads_number;

    finished++;
    sizePrinter.join();
    return 0;
}
/**
 * @brief test if both files have the same data
 *
 * @param file1name file name of file 1
 * @param file2name file name of file 2
 *
 * @result success
 */
int test(std::string file1name, std::string file2name)
{
    int fd = open(file1name.c_str(), O_RDONLY);
    if (fd == -1)
    {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }
    struct stat stats;
    stat(file1name.c_str(), &stats);
    size_t size = stats.st_size;
    char *mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));
    if (mappedFile == MAP_FAILED)
    {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
    std::string coloumns[key_number + 1];
    for (int i = 0; i < key_number; i++)
    {
        coloumns[i] = key_names[i];
    }
    coloumns[key_number] = "_col1";
    std::unordered_map<std::string, std::string> lineObjects;
    std::string ckey;
    int count;
    emhash8::HashMap<std::array<unsigned long, max_size>, float, decltype(hash), decltype(comp)> hashmap;
    bool reading = false;
    float opValue;
    std::array<unsigned long, max_size> keys;
    for (unsigned long i = 0; i < size; ++i)
    {
        i = readTuple(mappedFile, i, coloumns, &lineObjects, size);
        if (i == -1)
        {
            break;
        }
        try
        {
            for (int k = 0; k < key_number; k++)
            {
                keys[k] = std::stol(lineObjects[key_names[k]]);
            }
            opValue = std::stof(lineObjects["_col1"]);
        }
        catch (std::exception &err)
        {
            for (auto &it : lineObjects)
            {
                std::cout << it.first << ", " << it.second << std::endl;
            }
            std::cout << "conversion error test on file 1: " << err.what() << std::endl;
            return -1;
        }
        hashmap.insert(std::pair<std::array<unsigned long, max_size>, float>{keys, opValue});
    }
    munmap(mappedFile, size);
    close(fd);

    std::cout << "first file scanned" << std::endl;

    int fd2 = open(file2name.c_str(), O_RDONLY);
    if (fd2 == -1)
    {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }
    stat(file2name.c_str(), &stats);
    size = stats.st_size;
    mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd2, 0));
    if (mappedFile == MAP_FAILED)
    {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
    emhash8::HashMap<std::array<unsigned long, max_size>, float, decltype(hash), decltype(comp)> hashmap2;
    for (unsigned long i = 0; i < size; ++i)
    {
        i = readTuple(mappedFile, i, coloumns, &lineObjects, size);
        if (i == -1)
        {
            break;
        }
        try
        {
            for (int k = 0; k < key_number; k++)
            {
                keys[k] = std::stol(lineObjects[key_names[k]]);
            }
            opValue = std::stof(lineObjects["_col1"]);
        }
        catch (std::exception &err)
        {
            for (auto &it : lineObjects)
            {
                std::cout << it.first << ", " << it.second << std::endl;
            }
            std::cout << "conversion error test on file 2: " << err.what() << std::endl;
            return -1;
        }
        hashmap2.insert(std::pair<std::array<unsigned long, max_size>, float>{keys, opValue});
    }
    munmap(mappedFile, size);
    close(fd2);
    std::cout << "Scanning finished" << std::endl;
    testLog testlog;
    testlog.tuple_outputFile = hashmap.size();
    testlog.tuple_testFile = hashmap2.size();
    testlog.different_tuple_num = hashmap.size() != hashmap2.size();

    if (hashmap2.size() != hashmap.size())
    {
        std::cout << "Files have different number of keys." << " File1: " << hashmap.size() << " File2: " << hashmap2.size() << std::endl;
        // return 0;
    }

    bool same = true;
    unsigned long not_contained_keys = 0;
    unsigned long different_values = 0;
    for (auto &it : hashmap)
    {
        if (!hashmap2.contains(it.first))
        {
            not_contained_keys++;
            if (not_contained_keys < 50)
            {
                std::cout << "File 2 does not contain: " << it.first[0] << std::endl;
            }
            same = false;
            if (std::find(std::begin(test_values), std::end(test_values), it.first[0]) != std::end(test_values))
            {
                std::cout << "File 2 does not contain: " << it.first[0] << std::endl;
            }
        }
        if (std::abs(hashmap2[it.first] - it.second) > 0.001)
        {
            different_values++;
            if (different_values < 50)
            {
                std::cout << "File 2 has different value for key: " << it.first[0] << "; File 1: " << it.second << "; File 2: " << hashmap2[it.first] << std::endl;
            }
            same = false;
        }
    }
    for (auto &it : hashmap2)
    {
        if (!hashmap.contains(it.first))
        {
            not_contained_keys++;
            if (not_contained_keys < 50)
            {
                std::cout << "File 1 does not contain: " << it.first[0] << std::endl;
            }
            same = false;
            if (std::find(std::begin(test_values), std::end(test_values), it.first[0]) != std::end(test_values))
            {
                std::cout << "File 1 does not contain: " << it.first[0] << std::endl;
            }
        }
    }
    testlog.success = same;
    testlog.different_key_num = different_values;
    log_file.test = testlog;
    if (same)
    {
        std::cout << "Files are the Same!" << std::endl;
    }
    else
    {
        std::cout << "Files different! Not contained keys: " << not_contained_keys << ", different values: " << different_values << std::endl;
    }
    return 1;
}
/**
 * @brief turn string to int
 *
 * @param str string
 * @param h ?
 */
constexpr unsigned int str2int(const char *str, int h = 0)
{
    return !str[h] ? 5381 : (str2int(str, h + 1) * 33) ^ str[h];
}

int main(int argc, char **argv)
{
    /* // The file to be downloaded
    auto url = "s3a://131.159.16.208:9000";
    auto fileName = "trinobucket2/manag_file";

    // Create a new task group
    anyblob::network::TaskedSendReceiverGroup group;

    // Create an AnyBlob scheduler object for the group
    auto sendReceiverHandle = group.getHandle();
    // Create the provider for the corresponding filename
    bool https = false;
    auto provider = anyblob::cloud::Provider::makeProvider(url, https, "", "", &sendReceiverHandle);
    // Optionally init the specialized aws cache
    // provider->initCache(sendReceiverHandle);

    // Update the concurrency according to instance settings
    auto config = provider->getConfig(sendReceiverHandle);
    group.setConfig(config);

    // Create the get request
    anyblob::network::Transaction getTxn(provider.get());
    getTxn.getObjectRequest(fileName);

    // Retrieve the request synchronously with the scheduler object on this thread
    getTxn.processSync(sendReceiverHandle);

    for (const auto &it : getTxn)
    {
        // Check if the request was successful
        if (!it.success())
        {
            std::cout << "Request was not successful!" << std::endl;
            continue;
        }
        // Simple string_view interface
        std::cout << it.getResult() << std::endl;

        // Advanced raw interface
        // Note that the data lies in the data buffer but after the offset to skip the HTTP header
        // Note that the size is already without the header, so the full request has size + offset length
        std::string_view rawDataString(reinterpret_cast<const char *>(it.getData()) + it.getOffset(), it.getSize());
        std::cout << rawDataString << std::endl;
    }
    return 0; */

    // Init awssdk; optionally logging
    Aws::SDKOptions options;
    // options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    Aws::InitAPI(options);

    // Status request of Mana file
    if (argc == 3 || argc == 2)
    {
        std::string f = argv[1];
        if (f.compare("status") == 0)
        {
            std::string f2 = argc == 3 ? argv[2] : "nope";
            std::cout << f2 << std::endl;
            split_mana = f2.compare("dist") == 0;
            Aws::S3::S3Client minio_client_2 = init();
            printMana(&minio_client_2);
            Aws::ShutdownAPI(options);
            return 1;
        }
    }
    /* else
    {
        Aws::S3::S3Client minio_client_3 = init();
        worker_id = '1';
        split_mana = true;
        initManagFile(&minio_client_3);
        manaFile mana_worker;

        mana_worker.workers.push_back(manaFileWorker());
        mana_worker.workers[0].id = worker_id;
        for (char p = 0; p < 5; p++)
        {
            std::cout << "Adding partition file: " << (int)(p) << std::endl;
            partition part;
            part.id = p;
            part.lock = false;
            file file;
            file.name = "asdf";
            file.size = 123;
            file.status = 0;
            file.subfiles.push_back({132, 123});
            part.files.push_back(file);
            mana_worker.workers[0].partitions.push_back(part);

            writeMana(&minio_client_3, mana_worker, false, worker_id, p);
        }
        std::cout << "Writing worker file" << std::endl;
        writeMana(&minio_client_3, mana_worker, false, worker_id);
        return 1;
    } */

    // set pagesize with system pagesize
    pagesize = sysconf(_SC_PAGE_SIZE);
    // Name of file results are compared to
    std::string test_file;
    // Name of file with input data
    std::string input_file;
    // Number of TPC query
    int tpc_query;
    // Main memory Limit in B
    size_t memLimit;
    // Background memory limit in B
    size_t memLimitBack;
    // Number of iterations with different setup
    int iteration = 0;
    // Bool if mergeHelp should be executed
    bool do_mergeHelp = false;

    // Vectors of different setups for every iteration

    std::vector<size_t> memLimit_vec(1);
    std::vector<size_t> memLimitBack_vec(1);
    std::vector<int> threadNumber_vec(1);
    std::vector<bool> deencode_vec(1, deencode);
    std::vector<bool> set_partitions_vec(1, set_partitions);
    std::vector<bool> mergePhase_vec(1, mergePhase);
    std::vector<bool> multiThread_merge_vec(1, multiThread_merge);
    std::vector<bool> multiThread_subMerge_vec(1, multiThread_subMerge);
    std::vector<bool> straggler_removal_vec(1, straggler_removal);
    std::vector<bool> use_file_queue_vec(1, use_file_queue);
    std::vector<bool> split_mana_vec(1, split_mana);
    std::vector<float> partition_size_vec(1, partition_size);
    std::vector<size_t> mapping_max_vec(1, mapping_max);
    std::vector<size_t> max_s3_spill_size_vec(1, max_s3_spill_size);
    std::vector<std::string> memLimit_string_vec(1);
    std::vector<std::string> memLimitBack_string_vec(1);
    std::vector<bool> dynamic_extension_vec(1, dynamic_extension);
    std::vector<int> static_partition_number_vec(1, static_partition_number);
    std::vector<int> mergeThreads_number_vec(1, mergeThreads_number);
    std::vector<float> thread_efficiency_vec(1, thread_efficiency);

    // If no conf file is used configuration can be obtained directly from command (legacy)
    if (argc == 10)
    {
        std::string threadNumber_string;
        std::string tpc_query_string;
        std::string log_size_string;
        std::string log_time_string;
        std::string memLimit_string;
        std::string memLimitBack_string;

        input_file = argv[1];
        test_file = argv[2];
        memLimit_string = argv[3];
        memLimitBack_string = argv[4];
        threadNumber_string = argv[5];
        tpc_query_string = argv[6];
        worker_id = *argv[7];
        log_size_string = argv[8];
        log_time_string = argv[9];

        threadNumber_vec[0] = std::stoi(threadNumber_string);
        tpc_query = std::stoi(tpc_query_string);
        memLimit_vec[0] = (std::stof(memLimit_string) - 0.01) * (1ul << 30);
        memLimitBack_vec[0] = std::stof(memLimitBack_string) * (1ul << 30);
        log_size = log_size_string.compare("true") == 0;
        log_time = log_time_string.compare("true") == 0;
        memLimit_string_vec[0] = memLimit_string;
        memLimitBack_string_vec[0] = memLimitBack_string;
    }
    else
    {
        // read conf file and fill configuration vectors

        // standard conf file name if no name is given
        std::string conf_name = "conf";
        if (argc == 2)
        {
            conf_name = argv[1];
        }
        std::ifstream input(conf_name);
        std::string line;
        char del = ':';
        while (getline(input, line))
        {
            std::stringstream ss(line);
            std::string name;
            getline(ss, name, del);
            std::string value;
            getline(ss, value, del);
            // set conf variables
            switch (str2int(name.c_str()))
            {
            case str2int("tpc_query"):
            {
                tpc_query = std::stoi(value);
                break;
            }
            case str2int("input_file"):
            {
                input_file = value;
                break;
            }
            case str2int("test_file"):
            {
                test_file = value;
                break;
            }
            case str2int("mainLimit"):
            {
                memLimit_vec[iteration] = (std::stof(value) - 0.01) * (1ul << 30);
                memLimit_string_vec[iteration] = value;
                break;
            }
            case str2int("backLimit"):
            {
                memLimitBack_vec[iteration] = std::stof(value) * (1ul << 30);
                memLimitBack_string_vec[iteration] = value;
                break;
            }
            case str2int("threadNumber"):
            {
                threadNumber_vec[iteration] = std::stoi(value);
                break;
            }
            case str2int("log_size"):
            {
                log_size = value.compare("true") == 0;
                break;
            }
            case str2int("log_time"):
            {
                log_time = value.compare("true") == 0;
                break;
            }
            case str2int("worker_id"):
            {
                worker_id = value[0];
                break;
            }
            case str2int("deencode"):
            {
                deencode_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("set_partitions"):
            {
                set_partitions_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("straggler_removal"):
            {
                straggler_removal_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("mergePhase"):
            {
                mergePhase_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("multiThread_subMerge"):
            {
                multiThread_subMerge_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("multiThread_merge"):
            {
                multiThread_merge_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("do_mergeHelp"):
            {
                do_mergeHelp = value.compare("true") == 0;
                break;
            }
            case str2int("use_Filequeue"):
            {
                use_file_queue_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("partition_size"):
            {
                partition_size_vec[iteration] = std::stof(value);
                break;
            }
            case str2int("mapping_max"):
            {
                mapping_max_vec[iteration] = std::stof(value) * (1ul << 30);
                break;
            }
            case str2int("max_s3_spill_size"):
            {
                max_s3_spill_size_vec[iteration] = std::stof(value);
                break;
            }
            case str2int("dynamic_extension"):
            {
                dynamic_extension_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("static_partition_number"):
            {
                static_partition_number_vec[iteration] = std::stoi(value);
                break;
            }
            case str2int("merge_thread_number"):
            {
                mergeThreads_number_vec[iteration] = std::stoi(value);
                break;
            }
            case str2int("split_mana"):
            {
                split_mana_vec[iteration] = value.compare("true") == 0;
                break;
            }
            case str2int("thread_efficiency"):
            {
                thread_efficiency_vec[iteration] = std::stof(value);
                break;
            }
            case str2int("iteration"):
            {
                memLimit_vec.push_back(memLimit_vec[0]);
                memLimitBack_vec.push_back(memLimitBack_vec[0]);
                threadNumber_vec.push_back(threadNumber_vec[0]);
                deencode_vec.push_back(deencode_vec[0]);
                set_partitions_vec.push_back(set_partitions_vec[0]);
                mergePhase_vec.push_back(mergePhase_vec[0]);
                multiThread_merge_vec.push_back(multiThread_merge_vec[0]);
                multiThread_subMerge_vec.push_back(multiThread_subMerge_vec[0]);
                straggler_removal_vec.push_back(straggler_removal_vec[0]);
                memLimitBack_string_vec.push_back(memLimitBack_string_vec[0]);
                memLimit_string_vec.push_back(memLimit_string_vec[0]);
                use_file_queue_vec.push_back(use_file_queue_vec[0]);
                partition_size_vec.push_back(partition_size_vec[0]);
                mapping_max_vec.push_back(mapping_max_vec[0]);
                max_s3_spill_size_vec.push_back(max_s3_spill_size_vec[0]);
                dynamic_extension_vec.push_back(dynamic_extension_vec[0]);
                static_partition_number_vec.push_back(static_partition_number_vec[0]);
                mergeThreads_number_vec.push_back(mergeThreads_number_vec[0]);
                split_mana_vec.push_back(split_mana_vec[0]);
                thread_efficiency_vec.push_back(thread_efficiency_vec[0]);
                iteration++;
                break;
            }
            }
        }
    }

    // set configuration of TPC Query
    switch (tpc_query)
    {
    case (13):
    {
        std::string tmp = "custkey";
        key_names[0] = tmp;
        op = count;
        opKeyName = "orderkey";
        key_number = 1;
        value_number = 1;
        break;
    }
    case (4):
    {
        std::string tmp = "orderkey";
        key_names[0] = tmp;
        op = exists;
        opKeyName = "";
        key_number = 1;
        value_number = 0;
        break;
    }
    case (17):
    {
        std::string tmp = "partkey";
        key_names[0] = tmp;
        op = average;
        opKeyName = "quantity";
        key_number = 1;
        value_number = 2;
        break;
    }
    case (20):
    {
        std::string tmp = "partkey";
        key_names[0] = tmp;
        tmp = "suppkey";
        key_names[1] = tmp;
        op = sum;
        opKeyName = "quantity";
        key_number = 2;
        value_number = 1;
        break;
    }
    default:
    {
        *key_names = "custkey";
        op = count;
        opKeyName = "orderkey";
        key_number = 1;
    }
    }

    // output file name where results are written to
    std::string agg_output = "output_" + test_file;
    // set minio_client
    Aws::S3::S3Client minio_client = init();
    std::cout << "Iterations: " << memLimit_vec.size() << std::endl;
    // show Progress Bar only when we have 1 iteration; with more iterations it is expected user reads output in nohup.out file
    showProgressBar = iteration == 0;

    // Execute aggregation, test and mergeHelp for each iteration
    for (int i = 0; i < iteration + 1; i++)
    {
        // reset number of partitions and log file

        partitions = -1;
        log_file = logFile();
        backMem_usage = 0;

        // set configuration for specific iteration

        memLimit = memLimit_vec[i];
        memLimitBack = memLimitBack_vec[i];
        threadNumber = threadNumber_vec[i];
        mergeThreads_number = threadNumber;
        deencode = deencode_vec[i];
        set_partitions = set_partitions_vec[i];
        mergePhase = mergePhase_vec[i];
        multiThread_merge = multiThread_merge_vec[i];
        multiThread_subMerge = multiThread_subMerge_vec[i];
        straggler_removal = straggler_removal_vec[i];
        use_file_queue = use_file_queue_vec[i];
        partition_size = partition_size_vec[i];
        mapping_max = mapping_max_vec[i];
        max_s3_spill_size = max_s3_spill_size_vec[i];
        dynamic_extension = dynamic_extension_vec[i];
        static_partition_number = static_partition_number_vec[i];
        mergeThreads_number = mergeThreads_number_vec[i];
        split_mana = split_mana_vec[i];
        thread_efficiency = thread_efficiency_vec[i];

        // setup mana file
        initManagFile(&minio_client);

        start_time = std::chrono::high_resolution_clock::now();

        // setup log file name

        time_t now = time(0);
        struct tm tstruct;
        char buf[80];
        tstruct = *localtime(&now);
        strftime(buf, sizeof(buf), "%H-%M", &tstruct);
        date_now = std::to_string(tpc_query) + "_" + memLimit_string_vec[i] + "_" + memLimitBack_string_vec[i] + "_" + std::to_string(threadNumber) + "_" + buf;
        std::cout << date_now << std::endl;

        // log configuration

        log_file.sizes.insert(std::make_pair("threadNumber", threadNumber));
        log_file.sizes["mainLimit"] = memLimit;
        log_file.sizes["backLimit"] = memLimitBack;
        log_file.sizes["tpc_query"] = tpc_query;
        log_file.sizes["threadNumber"] = threadNumber;
        log_file.sizes["deencode"] = deencode;
        log_file.sizes["set_partitions"] = set_partitions;
        log_file.sizes["mergePhase"] = mergePhase;
        log_file.sizes["straggler_removal"] = straggler_removal;
        log_file.sizes["multiThread_subMerge"] = multiThread_subMerge;
        bool failed = false;

        // check if we have json or csv file format

        std::string suffix = "json";
        if (test_file != "-")
        {
            isJson = test_file.substr(test_file.length() - suffix.length()) == suffix;
        }
        else if (input_file != "-")
        {
            isJson = input_file.substr(input_file.length() - suffix.length()) == suffix;
        }

        // aggregate if input_file is given
        if (input_file != "-")
        {
            try
            {
                aggregate(input_file, agg_output, memLimit, minio_client, memLimitBack);
            }
            catch (std::exception &err)
            {
                std::cout << "Error while aggregating: " << err.what() << std::endl;
                log_file.err_msg = err.what();
                failed = true;
            }

            // log time

            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start_time).count()) / 1000000;
            std::cout << "Aggregation finished. With time: " << duration << "s. Checking results." << std::endl;
            log_file.sizes["queryDuration"] = duration;
            log_file.failed = failed;
        }

        // remember log_size configuration
        bool temp_log_size = log_size;
        // keep log_size only if we have a helper worker (test and input file not given) otherwise set it to false
        log_size = test_file == "-" && input_file == "-" ? log_size : false;
        // run test if test file is given and aggregation didn't fail
        if (test_file != "-" && !failed)
        {
            std::cout << "Testing" << std::endl;
            try
            {
                test(agg_output, test_file);
            }
            catch (std::exception &err)
            {
                std::cout << "Error while testing: " << err.what() << std::endl;
            }
        }

        // mergeHelp
        if (do_mergeHelp)
        {
            // setup variables for mergeHelp
            // Size of all Hashmap in memory
            std::atomic_ulong comb_hash_size = 0;
            // Size of all open mappings
            std::atomic_ulong diff = 0;
            // Average size of Hasmap entry in memory
            float avg = 1;
            // Hashmap
            emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> hmap;
            // set use_file_queue false as it is only needed in aggregate
            use_file_queue = false;
            try
            {
                helpMerge(memLimit, memLimitBack, minio_client);
            }
            catch (std::exception &err)
            {
                std::cout << "Error during mergePhase: " << err.what() << std::endl;
            }
        }

        cleanup(&minio_client);
        if (log_time)
        {
            writeLogFile(log_file);
        }
        // reset log_size
        log_size = temp_log_size;
    }
    // wait for all Threads started to get Mana to finish
    while (getManaThreads_num > 0)
    {
    }
    // shutdown awssdk
    Aws::ShutdownAPI(options);
    return 1;
}