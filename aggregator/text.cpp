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
/* #include "cloud/provider.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp" */

enum Operation
{
    count,
    sum,
    exists,
    average
};

struct file
{
    std::string name;
    size_t size;
    std::vector<std::pair<size_t, size_t>> subfiles;
    unsigned char status;
};

struct partition
{
    char id;
    bool lock;
    std::vector<file> files;
};

struct manaFileWorker
{
    char id;
    int length;
    bool locked;
    std::vector<partition> partitions;
};

struct manaFile
{
    char worker_lock;
    char thread_lock;
    std::vector<manaFileWorker> workers;
};

struct threadLog
{
    std::unordered_map<std::string, size_t> sizes;
    std::vector<size_t> spillTimes;
};

struct testLog
{
    bool success;
    int different_key_num;
    bool different_tuple_num;
    int tuple_outputFile;
    int tuple_testFile;
};

struct logFile
{
    std::unordered_map<std::string, size_t> sizes;
    std::vector<threadLog> threads;
    std::vector<size_t> get_lock_durs;
    std::vector<size_t> get_mana_durs;
    std::vector<size_t> write_mana_durs;
    std::vector<std::pair<size_t, size_t>> writeCall_s3_file_durs;
    std::vector<std::pair<size_t, size_t>> getCall_s3_file_durs;
    testLog test;
    bool failed;
    std::string err_msg;
};

static const int max_size = 2;
std::string key_names[max_size];
enum Operation op;
std::string opKeyName;
int key_number;
int value_number;
char worker_id;
std::string manag_file_name = "manag_file";
long pagesize;
std::string bucketName = "trinobucket2";
bool log_size;
bool log_time;
std::string date_now;
std::chrono::_V2::system_clock::time_point start_time;
unsigned long base_size = 1;
int threadNumber;
std::string lock_file_name = "lock";
size_t max_s3_spill_size = 10000000;
unsigned long extra_mem = 0;
unsigned long mainMem_usage = 0;
bool deencode = true;
bool mergePhase = false;
bool set_partitions = true;
bool straggler_removal = true;
bool multiThread_merge = true;
bool multiThread_subMerge = true;
std::vector<unsigned long> test_values = {};
int partitions = -1;
logFile log_file;
std::atomic<int> mana_writeThread_num(0);
std::atomic<bool> local_mana_lock(false);
int merge_file_num = 5;
float average_write_speed = 1.5;
bool isJson = false;
std::atomic<bool> writing_ouput(false);

auto hash = [](const std::array<unsigned long, max_size> a)
{
    std::size_t h = 0;
    for (int i = 0; i < max_size; i++)
    {
        h ^= std::hash<int>{}(a[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
};
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

// Aggregator(std::string *key_names, enum Operation op, std::string opKeyName, int key_number, int value_number) : key_names(key_names), op(op), opKeyName(opKeyName), key_number(key_number), value_number(value_number) {}

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

int writeString(char *mapping, const std::string &string)
{
    int counter = 0;
    for (auto &it : string)
    {
        mapping[counter] = it;
        counter++;
    }
    return counter;
}

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
    t_counter = 0;
    for (auto &it : log_t.get_lock_durs)
    {
        t_counter++;
        output << it;
        if (t_counter < log_t.get_lock_durs.size())
            output << ",";
    }

    output << "],\n\"write_mana_dur\":[";
    t_counter = 0;
    for (auto &it : log_t.write_mana_durs)
    {
        t_counter++;
        output << it;
        if (t_counter < log_t.write_mana_durs.size())
            output << ",";
    }

    output << "],\n\"get_mana_dur\":[";
    t_counter = 0;
    for (auto &it : log_t.get_mana_durs)
    {
        t_counter++;
        output << it;
        if (t_counter < log_t.get_mana_durs.size())
            output << ",";
    }

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

std::string getManaVersion(Aws::S3::S3Client *minio_client)
{
    manaFile mana;
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(manag_file_name);
    Aws::S3::Model::GetObjectOutcome outcome;

    while (true)
    {
        outcome = minio_client->GetObject(request);
        if (!outcome.IsSuccess())
        {
            std::cout << "Error opening manag_file: " << outcome.GetError().GetMessage() << std::endl;
        }
        else
        {
            return outcome.GetResult().GetVersionId();
        }
    }
}

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

    // done->exchange(true);
    //*return_value = mana;
    /* std::cout << "overwrite mana. Mana worker size: " << mana.workers.size() << " mana in pointer: " << return_value->workers.size() << std::endl;

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
    } */

    /* std::string status = return_value->worker_lock == 0 ? "free" : std::to_string(return_value->worker_lock);
    std::cout << "worker lock: " << status << ", thread lock: " << std::bitset<8>(return_value->thread_lock) << std::endl;
    for (auto &worker : return_value->workers)
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
    } */

    // std::cout << "use count: " << done.use_count() << std::endl;
    done.reset();
    return;
}

manaFile getMana(Aws::S3::S3Client *minio_client)
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
            auto thread_get_start_time = std::chrono::high_resolution_clock::now();
            threads.push_back(std::thread(getManaCall, minio_client, done, &mana, &donedone));
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
        getManaCall(minio_client, done, &mana, &donedone);
    }
    done.reset();
    log_file.get_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - get_start_time).count());
    // std::cout << "got mana" << std::endl;
    if (mana.workers.size() == 0)
    {
        std::cout << "mana worker size == 0!" << std::endl;
    }
    return mana;
}

void writeManaCall(Aws::S3::S3Client *minio_client, std::shared_ptr<Aws::IOStream> in_stream, size_t size, bool freeLock, std::atomic<bool> &done, std::atomic<bool> &return_value)
{
    Aws::S3::Model::PutObjectRequest in_request;
    in_request.SetBucket(bucketName);
    in_request.SetKey(manag_file_name);
    in_request.SetBody(in_stream);
    in_request.SetContentLength(size);
    // in_request.SetWriteOffsetBytes(1000);

    auto in_outcome = minio_client->PutObject(in_request);
    if (!in_outcome.IsSuccess())
    {
        std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << size << std::endl;
    }
    else
    {
        if (freeLock)
        {
            Aws::S3::Model::DeleteObjectRequest delete_request;
            delete_request.WithKey(lock_file_name).WithBucket(bucketName);
            auto outcome = minio_client->DeleteObject(delete_request);
            if (!outcome.IsSuccess())
            {
                // std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                return_value.exchange(false);
                done.exchange(true);
                return;
            }
        }
        return_value.exchange(true);
        done.exchange(true);
        return;
    }
}

bool writeMana(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock)
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
        auto in_outcome = minio_client->PutObject(in_request);
        if (!in_outcome.IsSuccess())
        {
            std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
        }
        else
        {

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
                local_mana_lock.exchange(false);
            }
            log_file.write_mana_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count());
            return 1;
        }
    }
}

bool writeLock(Aws::S3::S3Client *minio_client)
{
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(lock_file_name.c_str());
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

manaFile getLockedMana(Aws::S3::S3Client *minio_client, char thread_id)
{
    auto lock_start_time = std::chrono::high_resolution_clock::now();
    while (true)
    {
        bool asdf = false;
        if (local_mana_lock.compare_exchange_strong(asdf, true))
        {
            manaFile mana = getMana(minio_client);
            if (mana.worker_lock == 0)
            {
                // std::cout << "Trying to get lock: " << std::to_string((int)(thread_id)) << std::endl;
                // manaFile mana = getMana(minio_client);
                mana.worker_lock = worker_id;
                mana.thread_lock = thread_id;
                if (!writeLock(minio_client))
                {
                    local_mana_lock.exchange(false);
                    continue;
                }
                writeMana(minio_client, mana, false);
                mana = getMana(minio_client);
                // std::cout << "Lock received by: " << std::to_string((int)(thread_id)) << " old thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
                if (mana.worker_lock == worker_id && mana.thread_lock == thread_id)
                {
                    // mana = getMana(minio_client);
                    //  std::cout << " new thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
                    log_file.get_lock_durs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - lock_start_time).count());
                    return mana;
                }
            }
            local_mana_lock.exchange(false);
        }
        // usleep(10);
    }
}

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

void printMana(Aws::S3::S3Client *minio_client, manaFile given_mana, bool usegetMana = true)
{
    manaFile mana;
    if (usegetMana)
    {
        mana = getMana(minio_client);
    }
    else
    {
        mana = given_mana;
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
    writeMana(minio_client, mana, true);
    // printMana(minio_client);
}

void printProgressBar(float progress)
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

void cleanup(Aws::S3::S3Client *minio_client)
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

void encode(unsigned long l, std::vector<char> *res)
{
    char l_bytes = l == 0 ? 0 : (static_cast<int>(log2(l)) + 8) / 8;
    // std::cout << "long: " << l << ", " << std::bitset<64>(l) << " l_bytes: " << (int)(l_bytes) << std::endl;
    res->push_back(l_bytes);

    char byteArray[sizeof(long)];
    std::memcpy(byteArray, &l, sizeof(long));
    /* for (auto &it : byteArray)
    {
        std::cout << std::bitset<8>(it) << ", ";
    }
    std::cout << std::endl;*/
    for (int i = 0; i < l_bytes; i++)
    {
        res->push_back(byteArray[i]);
    }
    /* for (auto &it : *res)
    {
        std::cout << std::bitset<8>(it) << ", ";
    }
    std::cout << std::endl;*/
}

unsigned long decode(std::vector<char> *in_stream)
{
    int l_bytes = (*in_stream)[0];
    char char_buf[sizeof(long)];
    int counter = 0;
    for (auto it = in_stream->begin(); it != in_stream->end(); ++it)
    {
        if (counter == 0)
        {
            counter++;
            continue;
        }
        char_buf[counter - 1] = *it;
        counter++;
        if (counter == in_stream->size())
        {
            break;
        }
    }
    for (auto &it : char_buf)
    {
        std::cout << std::bitset<8>(it) << ", ";
    }
    std::cout << std::endl;
    for (int i = 0; i < sizeof(long) - l_bytes; i++)
    {
        char_buf[counter - 1] = 0;
        counter++;
    }
    for (auto &it : char_buf)
    {
        std::cout << std::bitset<8>(it) << ", ";
    }
    std::cout << std::endl;
    unsigned long buf;
    // spill.read(char_buf, sizeof(long));
    std::memcpy(&buf, &char_buf, sizeof(long));
    return buf;
}

void setPartitionNumber(size_t comb_hash_size)
{
    if (set_partitions)
    {
        partitions = ceil(comb_hash_size / 3000000.0);
        // partitions = 20;
        std::cout << "Set partition number to: " << partitions << std::endl;
    }
    else
    {
        partitions = 1;
    }
    log_file.sizes["partitionNumber"] = partitions;
}

void addFileToManag(Aws::S3::S3Client *minio_client, std::vector<std::pair<file, char>> files, char write_to_id, char thread_id)
{
    manaFile mana = getLockedMana(minio_client, thread_id);
    for (auto &file : files)
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
                    return;
                }
                for (auto &partition : worker.partitions)
                {
                    if (partition.id == file.second)
                    {
                        if (partition.lock)
                        {
                            writeMana(minio_client, mana, true);
                            mana_writeThread_num.fetch_sub(1);
                            return;
                        }
                        partition.files.push_back(file.first);
                        parition_found = true;
                        break;
                    }
                }
                if (!parition_found)
                {
                    partition partition;
                    partition.id = file.second;
                    partition.lock = false;
                    partition.files.push_back(file.first);
                    worker.partitions.push_back(partition);
                    worker.length += 2 + sizeof(int);
                }

                worker.length += file.first.name.size() + 2 + sizeof(int) + sizeof(size_t) + sizeof(size_t) * file.first.subfiles.size() * 2;
                break;
            }
        }
    }
    writeMana(minio_client, mana, true);
    // std::cout << "Printing mana:" << std::endl;
    // manaFile asdf;
    // printMana(minio_client, asdf);
    mana_writeThread_num.fetch_sub(1);
    return;
}

void getAllMergeFileNames(Aws::S3::S3Client *minio_client, char partition_id, std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *files)
{
    manaFile mana = getMana(minio_client);
    for (auto &worker : mana.workers)
    {
        if (worker.id == worker_id)
        {
            for (auto &partition : worker.partitions)
            {
                if (partition.id == partition_id)
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

void getMergeFileName(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, Aws::S3::S3Client *minio_client,
                      char beggarWorker, char partition_id, std::vector<std::string> *blacklist, std::tuple<std::vector<file>, char, char> *res, char thread_id, char file_num)
{
    // std::cout << "getting file name" << std::endl;
    char given_beggarWorker = beggarWorker;
    file m_file;
    manaFile mana = getLockedMana(minio_client, thread_id);
    // printMana(minio_client);
    //  If no beggarWorker is yet selected choose the worker with the largest spill
    if (beggarWorker == 0)
    {
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
                    if (b_file > biggest_file && file_number > 3)
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
    if (res_files.size() < file_num)
    {
        writeMana(minio_client, mana, true);
        get<1>(*res) = 0;
        std::cout << "setting beggar to 0" << std::endl;
        return;
    }
    /* std::cout << "res_files: ";
    for (auto temp : res_files)
    {
        std::cout << temp.name << ", ";
    }
    std::cout << std::endl; */
    get<1>(*res) = beggarWorker;
    get<2>(*res) = partition_id;
    get<0>(*res) = res_files;

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
    writeMana(minio_client, mana, true);
    return;
}

// Write hashmap hmap into file with head on start.
unsigned long writeHashmap(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, unsigned long start, unsigned long free_mem, std::string &outputfilename)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    unsigned long output_size = 0;
    if (isJson)
    {
        // Calc the output size for hmap.

        for (auto &it : *hmap)
        {
            for (int i = 0; i < key_number; i++)
            {
                output_size += std::to_string(it.first[i]).length();
            }

            if (op != average)
            {
                output_size += std::to_string(it.second[0]).length();
            }
            else
            {
                output_size += std::to_string(it.second[0] / (float)(it.second[1])).length();
            }
        }
        for (int i = 0; i < key_number; i++)
        {
            output_size += ("\"" + key_names[i] + "\":").length() * hmap->size();
        }

        // unsigned long output_size_test = strlen(("\"custkey\":,\"_col1\":}").c_str())

        output_size += (strlen("\"_col1\":") + 5 + key_number) * hmap->size();
    }
    else
    {
        for (auto &it : *hmap)
        {
            for (int i = 0; i < key_number; i++)
            {
                output_size += std::to_string(it.first[i]).length();
            }

            if (op != average)
            {
                output_size += std::to_string(it.second[0]).length();
            }
            else
            {
                output_size += std::to_string(it.second[0] / (float)(it.second[1])).length();
            }
        }
        output_size += hmap->size() * (key_number + value_number);
    }
    // std::cout << "Output file size: " << output_size << std::endl;

    if (!fcntl(file, F_GETFD))
    {
        std::cout << "reopening file handle in writeHashmap" << std::endl;
        file = open(outputfilename.c_str(), O_RDWR | O_CREAT, 0777);
    }

    // Extend file file.
    lseek(file, start + output_size - 1, SEEK_SET);
    if (write(file, "", 1) == -1)
    {
        close(file);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }

    size_t start_diff = start % pagesize;
    size_t start_page = start - start_diff;

    // Map file with given size.
    char *mappedoutputFile = static_cast<char *>(mmap(nullptr, output_size + start_diff, PROT_WRITE | PROT_READ, MAP_SHARED, file, start_page));
    madvise(mappedoutputFile, output_size + start_diff, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (mappedoutputFile == MAP_FAILED)
    {

        std::cout << "Start: " << output_size << " + " << start_diff << " start_page: " << start_page << " is fd valid? " << fcntl(file, F_GETFD) << std::endl;
        perror("Error mmapping the file in write Hashmap");
        close(file);
        exit(EXIT_FAILURE);
    }
    unsigned long freed_mem = 0;

    // Write into file through mapping. Starting at the given start point.
    unsigned long mapped_count = start_diff;
    unsigned long head = 0;
    for (auto &it : *hmap)
    {
        if (isJson)
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], "{");
        }
        // std::string temp_line = "{";
        for (int k = 0; k < key_number; k++)
        {
            if (isJson)
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
            else
            {
                mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.first[k]));
                mapped_count += writeString(&mappedoutputFile[mapped_count], ",");
            }
        }
        if (isJson)
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], ",\"_col1\":");
        }
        if (op != average)
        {
            // temp_line += ",\"_col1\":" + std::to_string(it.second[0]) + "}";
            mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0]));
        }
        else
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.second[0] / (float)(it.second[1])));
            // temp_line += ",\"_col1\":" + std::to_string(it.second[0] / (float)(it.second[1])) + "}";
        }
        if (isJson)
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], "},");
        }
        mapped_count += writeString(&mappedoutputFile[mapped_count], "\n");
        // std::cout << temp_line << std::endl;
        // for (auto &itt : temp_line)
        //{
        unsigned long used_space = (mapped_count - head);
        if (used_space >= free_mem && used_space > pagesize)
        {
            unsigned long freed_space = used_space - (used_space % pagesize);
            munmap(&mappedoutputFile[head], freed_space);
            head += freed_space;
            freed_mem += freed_space;
        }
        //}
    }

    // free mapping and return the size of output of hmap.
    if (munmap(&mappedoutputFile[head], (output_size + start_diff) - head) == -1)
    {
        perror("Could not free memory in writeHashmap 2!");
    }
    freed_mem += (output_size + start_diff) - head;
    // std::cout << "freed mem: " << freed_mem << " size: " << output_size + start_diff << std::endl;
    // std::cout << "Output file size: " << output_size << std::endl;
    log_file.sizes["write_output_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count();

    return output_size;
}

int getPartition(std::array<unsigned long, max_size> key)
{
    auto h = hash(key);
    return h % partitions;
}

unsigned long parseCVS(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    unsigned long i = start;
    while (true)
    {
        if (i > limit)
        {
            return ULONG_MAX;
        }
        // start reading a line when {
        if (isdigit(mapping[i]))
        {
            int readingMode = 0;
            while (true)
            {
                std::string key = keys[readingMode];
                char char_temp = mapping[i];
                (*lineObjects)[key] = "";
                while (char_temp != ',' && char_temp != '\n')
                {
                    if (char_temp == '.' || isdigit(char_temp))
                    {
                        (*lineObjects)[key] += char_temp;
                    }
                    i++;
                    char_temp = mapping[i];
                }
                if (char_temp != '\n')
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

unsigned long readTuple(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    if (isJson)
    {
        return parseJson(mapping, start, keys, lineObjects, limit);
    }
    else
    {
        return parseCVS(mapping, start, keys, lineObjects, limit);
    }
}

void spillToFileEncoded(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> *spill_file, char id, size_t free_mem, std::string &fileName)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(long) * (key_number + value_number);

    if ((*spill_file)[0].first == -1)
    {
        for (int i = 0; i < partitions; i++)
        {
            // std::cout << "opening file " << (fileName + "_" + std::to_string(i)) << std::endl;
            (*spill_file)[i].first = open((fileName + "_" + std::to_string(i)).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
            if ((*spill_file)[i].first == -1)
            {
                // std::cout << "Tying to open file " << (fileName + "_" + std::to_string(i)) << std::endl;
                perror("Error opening file for writing");
                exit(EXIT_FAILURE);
            }
        }
    }

    // extend file
    for (int i = 0; i < partitions; i++)
    {
        // std::cout << "extending file " << (fileName + "_" + std::to_string(i)) << " by " << (*spill_file)[i].second + spill_mem_size - 1 << std::endl;
        lseek((*spill_file)[i].first, (*spill_file)[i].second + spill_mem_size - 1, SEEK_SET);
        if (write((*spill_file)[i].first, "", 1) == -1)
        {
            close((*spill_file)[i].first);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
    }
    std::vector<char *> spills;
    // std::cout << "opening mappings " << fileName << std::endl;

    for (int i = 0; i < partitions; i++)
    {
        spills.push_back((char *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, (*spill_file)[i].first, 0)));
        madvise(spills[i], spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);
        if (spills[i] == MAP_FAILED)
        {
            close((*spill_file)[i].first);
            perror("Error mmapping the file");
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
        if (ftruncate((*spill_file)[i].first, counters[i]) == -1)
        {
            close((*spill_file)[i].first);
            perror("Error truncation file");
            exit(EXIT_FAILURE);
        }
    }
    // Cleanup: clear hashmap and free rest of mapping space
    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}

void spillToFile(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> *spill_file, char id, size_t free_mem, std::string &fileName)
{
    if (deencode)
    {
        spillToFileEncoded(hmap, spill_file, id, free_mem, fileName);
        return;
    }
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(long) * (key_number + value_number);

    if ((*spill_file)[0].first == -1)
    {
        for (int i = 0; i < partitions; i++)
        {
            (*spill_file)[i].first = open((fileName + "_" + std::to_string(i)).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
        }
    }

    // extend file
    for (int i = 0; i < partitions; i++)
    {
        lseek((*spill_file)[i].first, (*spill_file)[i].second + spill_mem_size - 1, SEEK_SET);
        if (write((*spill_file)[i].first, "", 1) == -1)
        {
            close((*spill_file)[i].first);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
    }
    std::vector<unsigned long *> spills;

    for (int i = 0; i < partitions; i++)
    {
        spills.push_back((unsigned long *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, (*spill_file)[i].first, 0)));
        madvise(spills[i], spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);
        if (spills[i] == MAP_FAILED)
        {
            close((*spill_file)[i].first);
            perror("Error mmapping the file");
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
        if (ftruncate((*spill_file)[i].first, (counters[i] - 1) * sizeof(long)) == -1)
        {
            close((*spill_file)[i].first);
            perror("Error truncation file");
            exit(EXIT_FAILURE);
        }
    }

    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}

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

void writeS3File(Aws::S3::S3Client *minio_client, const std::shared_ptr<Aws::IOStream> body, size_t size, std::string &name)
{
    auto write_start_time = std::chrono::high_resolution_clock::now();
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(name);
    request.SetBody(body);
    request.SetContentLength(size);
    size_t expected_time = size / average_write_speed;
    bool done;

    /* if (straggler_removal)
    {
        std::vector<std::thread> threads;
        while (!done)
        {
            auto thread_get_start_time = std::chrono::high_resolution_clock::now();
            threads.push_back(std::thread(writeS3FileCall, minio_client, &request, std::ref(name), size, &done));
            size_t duration = 0;
            while (duration < expected_time)
            {
                if (done)
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
    }
    else
    { */
    writeS3FileCall(minio_client, &request, name, size, &done);
    //}

    log_file.writeCall_s3_file_durs.push_back({std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - write_start_time).count(), size});
}

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

                char byteArray[sizeof(long)];
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

                char byteArray[sizeof(long)];
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
            char byteArray[sizeof(long)];
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
    else
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

void spillS3FileEncoded(std::pair<int, size_t> spill_file, Aws::S3::S3Client *minio_client, std::vector<std::pair<size_t, size_t>> *sizes, std::string uniqueName, int *start_counter)
{
    size_t spill_mem_size = spill_file.second;
    char *spill_map = static_cast<char *>(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));
    if (spill_map == MAP_FAILED)
    {
        close(spill_file.first);
        perror("Error mmapping the file");
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
}

void spillS3File(std::pair<int, size_t> spill_file, Aws::S3::S3Client *minio_client, std::vector<std::pair<size_t, size_t>> *sizes, std::string uniqueName, int *start_counter)
{
    size_t spill_mem_size = spill_file.second;
    unsigned long *spill_map = static_cast<unsigned long *>(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));
    if (spill_map == MAP_FAILED)
    {
        close(spill_file.first);
        perror("Error mmapping the file");
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
}

void spillToMinio(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> spill_file, std::string uniqueName,
                  Aws::S3::S3Client *minio_client, char write_to_id, unsigned char fileStatus, char thread_id)
{
    int counter = 0;
    std::vector<std::vector<std::pair<size_t, size_t>>> sizes(partitions);

    std::string n;
    std::vector<int> start_vector = std::vector<int>(partitions, 0);

    // Calc spill size

    if (spill_file[0].first == -1)
    {
        spillS3Hmap(hmap, minio_client, &sizes, uniqueName, &start_vector);
    }
    else
    {
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
    std::thread thread(addFileToManag, minio_client, files, write_to_id, fileStatus);
    // addFileToManag(minio_client, files, write_to_id, fileStatus);
    mana_writeThread_num.fetch_add(1);
    thread.detach();
}

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

void fillHashmap(char id, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, size_t start, size_t size, bool addOffset, size_t memLimit,
                 float &avg, std::vector<std::vector<std::pair<int, size_t>>> *spill_files, std::atomic<unsigned long> &numLines, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *shared_diff, Aws::S3::S3Client *minio_client,
                 std::atomic<unsigned long> &readBytes, unsigned long memLimitBack, std::atomic<unsigned long> &comb_spill_size)
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
        perror("Error mmapping the file");
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
        if (maxHmapSize * avg + base_size / threadNumber >= memLimit * 0.9)
        {
            // std::cout << "memLimit broken. Estimated mem used: " << hmap->size() * avg + base_size / threadNumber << " size: " << hmap->size() << " avg: " << avg << " base_size / threadNumber: " << base_size / threadNumber << std::endl;
            unsigned long freed_space_temp = (i - head) - ((i - head) % pagesize);
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

            // compare estimation again to memLimit
            if (freed_space_temp <= pagesize * 10 && hmap->size() * (key_number + value_number) * sizeof(long) > pagesize && hmap->size() * avg + base_size / threadNumber >= memLimit * 0.9)
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
                }
                threadLog.sizes["SpillSize"] += temp_spill_size;

                threadLog.spillTimes.push_back(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count());

                if (memLimitBack > mainMem_usage + temp_spill_size - temp_local_spill_size)
                {
                    if (memLimitBack < mainMem_usage + temp_spill_size * threadNumber)
                    {
                        threadLog.sizes["localS3Spill"]++;
                        threadLog.sizes["s3SpillSize"] += temp_spill_size;
                        std::cout << "local + s3: " << (int)(id) << std::endl;
                        mainMem_usage += temp_spill_size - temp_local_spill_size;
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
                        std::vector<std::pair<int, size_t>> spill_file(partitions, {-1, 0});
                        spillToFile(hmap, &spill_file, id, pagesize * 20, spill_file_name);
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
                        mainMem_usage += temp_spill_size;
                        std::vector<std::pair<int, size_t>> spill_file(partitions, {-1, 0});
                        spillToFile(hmap, &spill_file, id, pagesize * 20, spill_file_name);
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
                    std::vector<std::pair<int, size_t>> spill_file = std::vector<std::pair<int, size_t>>(partitions, {-1, 0});
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
                threadLog.sizes["write_file_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_spill_time).count();

                // std::cout << "Spilling ended" << std::endl;
                hmap->clear();
            }
        }
        // After line is read clear it and set reading to false till the next {
        numLines.fetch_add(1);
        numLinesLocal++;
    }
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
        mainMem_usage -= stats.st_size;
        remove(spill_file_name.c_str());
    }
    try
    {
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - thread_start_time).count()) / 1000000;
        std::cout << "Thread " << int(id) << " finished scanning. With time: " << duration << "s. Scanned Lines: " << numLinesLocal << ". microseconds/line: " << duration * 1000000 / numLinesLocal << ". Spilled with size: " << spill_size << std::endl;
        threadLog.sizes["duration"] = duration;
        threadLog.sizes["endTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
        log_file.threads.push_back(threadLog);
    }
    catch (std::exception &err)
    {
        std::cout << "Not able to print time: " << err.what() << std::endl;
    }
}

void printSize(int &finished, size_t memLimit, int threadNumber, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *diff, float *avg)
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
    // memLimit -= 2ull << 10;
    while (finished == 0 || finished == 1)
    {
        if (old_finish != finished)
        {
            phyMemBase = (getPhyValue()) * 1024 - comb_hash_size.load() * (*avg);
            old_finish = finished;
        }
        size_t newsize = getPhyValue() * 1024;
        while (abs(static_cast<long>(size - newsize)) > 5000000000)
        {
            newsize = getPhyValue() * 1024;
        }
        unsigned long reservedMem = diff->load();

        if (log_size)
        {
            duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000;
            if (duration - oldduration > 100)
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
                *avg = (size - base_size) / (float)(comb_hash_size.load());
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
        if (base_size + (*avg) * comb_hash_size.load() > size && comb_hash_size.load() > 0)
        {
            float temp_avg = (size - base_size) / (float)(comb_hash_size.load());
            if (temp_avg < *avg)
            {
                *avg = temp_avg;
            }
            // std::cout << "phy: " << size << " phymemBase: " << phyMemBase << " avg: " << *avg << " reservedMem: " << reservedMem << " (*extra_mem): " << (*extra_mem) << std::endl;
            //*avg *= 1.2;
            //  std::cout << "phy: " << size << " phymemBase: " << phyMemBase << " hash_avg: " << *avg << std::endl;
            usleep(0);
        }
        old_size = size;
        usleep(0);
    }
    std::cout << "Max Size: " << maxSize << "B." << std::endl;
    log_file.sizes["maxMainUsage"] = maxSize;
    output.close();
}

bool subMerge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap,
              std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *s3spillNames2,
              std::vector<std::pair<int, std::vector<char>>> *s3spillBitmaps, std::vector<std::pair<int, size_t>> *spills, bool add, int *s3spillFile_head,
              int *bit_head, int *subfile_head, size_t *s3spillStart_head, size_t *s3spillStart_head_chars, size_t *input_head_base, size_t size_after_init, std::atomic<size_t> *read_lines,
              Aws::S3::S3Client *minio_client, std::atomic<bool> *writeLock, std::atomic<int> *readNum, float *avg, float memLimit, std::atomic<unsigned long> &comb_hash_size,
              std::atomic<unsigned long> *diff, bool increase_size, size_t *max_hash_size)
{
    // input_head_base;
    unsigned long num_entries = 0;
    unsigned long input_head = 0;
    unsigned long offset = 0;
    unsigned long sum = 0;
    unsigned long newi = 0;
    size_t mapping_size = 0;
    size_t comb_spill_size = 0;
    size_t increase = 0;
    int file_counter = 0;

    for (auto &it : *spills)
    {
        comb_spill_size += it.second;
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
                extra_mem -= increase;
                return false;
            }
            if (locked && add)
            {
                extra_mem -= increase;
                return false;
            }
            // std::cout << "Reading " << get<0>(*set_it) << std::endl;
            firsts3File = hmap->empty();
            int sub_file_counter = 0;

            if (firsts3File)
            {
                sub_file_counter = *subfile_head;
            }
            for (int sub_file_k = sub_file_counter; sub_file_k < get<2>(*set_it).size(); sub_file_k++)
            {
                auto read_file_start = std::chrono::high_resolution_clock::now();
                auto sub_file = get<2>(*set_it)[sub_file_k].second;
                firsts3subFile = hmap->empty();
                // std::cout << "Reading " << get<0>(*set_it) + "_" + std::to_string(sub_file_counter) << " bitmap: " << bit_i << " Read lines: " << read_lines << std::endl;
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
                log_file.getCall_s3_file_durs.push_back({std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_file_start).count(), get<2>(*set_it)[sub_file_k].first});
                // spill.rdbuf()->pubsetbuf(buffer, 1ull << 10);
                //   spill.rdbuf()->
                char *bitmap_mapping;
                std::vector<char> *bitmap_vector;
                // std::cout << "first: " << s3spillBitmaps[bit_i].first << std::endl;
                bool spilled_bitmap = (*s3spillBitmaps)[bit_i].first != -1;
                if (!spilled_bitmap)
                {
                    bitmap_vector = &(*s3spillBitmaps)[bit_i].second;
                }
                else
                {
                    bitmap_mapping = static_cast<char *>(mmap(nullptr, std::ceil((float)(sub_file) / 8), PROT_WRITE | PROT_READ, MAP_SHARED, (*s3spillBitmaps)[bit_i].first, 0));
                    if (bitmap_mapping == MAP_FAILED)
                    {
                        perror("Error mmapping the file");
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
                        auto read_tuple_start = std::chrono::high_resolution_clock::now();
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
                        log_file.sizes["get_tuple_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_tuple_start).count();

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
                        while ((*writeLock))
                        {
                        }
                        readNum->fetch_add(1);
                        bool contained = hmap->contains(keys);
                        readNum->fetch_sub(1);
                        if (contained)
                        {

                            std::array<unsigned long, max_size> temp = (*hmap)[keys];

                            for (int k = 0; k < value_number; k++)
                            {
                                temp[k] += values[k];
                            }
                            bool asdf = false;
                            while (!writeLock->compare_exchange_strong(asdf, true))
                            {
                            }
                            while ((*readNum) > 0)
                            {
                            }
                            (*hmap)[keys] = temp;
                            writeLock->exchange(false);

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
                            if (hmap->size() > *max_hash_size)
                            {
                                comb_hash_size.fetch_add(1);
                                *max_hash_size++;
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
                            diff->exchange(index);
                            if (hmap->size() * (*avg) + base_size >= memLimit * 0.9)
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
                                if (freed_space_temp < pagesize * 2)
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
                            if (hmap->size() * (*avg) + base_size >= memLimit * 0.9)
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
    }
    // std::cout << "Merging local input_head_base: " << *input_head_base << ", comb_spill_size: " << comb_spill_size << std::endl;

    // std::cout << "New round" << std::endl;
    // Go through entire mapping
    for (unsigned long i = *input_head_base; (!deencode && i < comb_spill_size / sizeof(long)) || (deencode && i < comb_spill_size); i++)
    {
        if ((!deencode && i >= sum / sizeof(long)) || (deencode && i >= sum))
        {
            // std::cout << "New mapping sum: " << sum << std::endl;
            sum = 0;
            for (auto &it : *spills)
            {
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
                        // std::cout << "Free: " << input_head << " - " << mapping_size / sizeof(long) << std::endl;
                    }
                    file_counter++;
                    if (!add && multiThread_subMerge && file_counter > merge_file_num)
                    {
                        extra_mem -= increase;
                        return false;
                    }
                    if (locked && add)
                    {
                        extra_mem -= increase;
                        return false;
                    }
                    unsigned long map_start;
                    if (deencode)
                    {
                        map_start = i - (sum - it.second) - ((i - (sum - it.second)) % pagesize);
                        mapping_size = it.second - map_start;

                        spill_map_char = static_cast<char *>(mmap(nullptr, mapping_size, PROT_WRITE | PROT_READ, MAP_SHARED, it.first, map_start));
                        if (spill_map_char == MAP_FAILED)
                        {
                            close(it.first);
                            perror("Error mmapping the file");
                            exit(EXIT_FAILURE);
                        }
                        madvise(spill_map_char, mapping_size, MADV_SEQUENTIAL | MADV_WILLNEED);
                        input_head = 0;
                        offset = ((sum - it.second) + map_start);
                        // std::cout << "opening new mapping mapsstart: " << map_start << " mapping size: " << mapping_size << " offset: " << offset << " i: " << i << std::endl;
                    }
                    else
                    {
                        map_start = i * sizeof(long) - (sum - it.second) - ((i * sizeof(long) - (sum - it.second)) % pagesize);

                        mapping_size = it.second - map_start;
                        // std::cout << " map_start: " << map_start << std::endl;
                        spill_map = static_cast<unsigned long *>(mmap(nullptr, mapping_size, PROT_WRITE | PROT_READ, MAP_SHARED, it.first, map_start));
                        if (spill_map == MAP_FAILED)
                        {
                            close(it.first);
                            perror("Error mmapping the file");
                            exit(EXIT_FAILURE);
                        }
                        madvise(spill_map, mapping_size, MADV_SEQUENTIAL | MADV_WILLNEED);
                        input_head = 0;
                        offset = ((sum - it.second) + map_start) / sizeof(long);
                    }

                    // std::cout << "sum: " << sum / sizeof(long) << " offset: " << offset << " head: " << input_head_base << " map_start: " << map_start / sizeof(long) << " i: " << i << std::endl;
                    break;
                }
            }
        }
        newi = i - offset;

        if (newi > mapping_size)
        {
            std::cout << "newi too big!! " << newi << std::endl;
        }
        unsigned long ognewi = newi;
        if (deencode)
        {
            diff->exchange((newi - input_head));
        }
        else
        {
            diff->exchange((newi - input_head) * sizeof(long));
        }
        bool empty = false;
        auto read_tuple_start = std::chrono::high_resolution_clock::now();

        if (deencode)
        {
            char char_buf[sizeof(long)];
            for (int k = 0; k < key_number; k++)
            {

                char l_bytes = spill_map_char[newi];
                if (l_bytes < 0 && k == 0)
                {
                    // std::cout << "l_bytes negative: " << (int)(l_bytes) << " add: " << l_bytes * -1 + 1;
                    i += l_bytes * -1 + 1;
                    for (int s = 0; s < key_number + value_number - 1; s++)
                    {
                        // std::cout << ", " << spill_map_char[i] + 1;
                        i += spill_map_char[i] + 1;
                    }
                    // std::cout << std::endl;
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
        }
        else
        {
            if (spill_map[newi] == ULONG_MAX)
            {
                i += key_number;
                i += value_number - 1;
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
        log_file.sizes["get_tuple_dur"] += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - read_tuple_start).count();
        // std::cout << keys[0] << ", " << values[0] << std::endl;
        if (!empty)
        {
            newi--;
            i = newi + offset;
            read_lines->fetch_add(1);
            // std::cout << "i: " << i << ", newi: " << newi << std::endl;
            //  std::cout << "merging/adding" << std::endl;
            readNum->fetch_add(1);
            bool contained = hmap->contains(keys);
            readNum->fetch_sub(1);
            //   Update count if customerkey is in hashmap and delete pair in spill
            if (contained)
            {
                std::array<unsigned long, max_size> temp = (*hmap)[keys];

                for (int k = 0; k < value_number; k++)
                {
                    temp[k] += values[k];
                }
                bool asdf = false;
                while (!writeLock->compare_exchange_strong(asdf, true))
                {
                }
                while ((*readNum) > 0)
                {
                }
                (*hmap)[keys] = temp;
                writeLock->exchange(false);
                // mergeHashEntries(&emHashmap[keys], &values);
                //    delete pair in spill
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
                    *max_hash_size++;
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
            if (used_space > pagesize)
            {
                // std::cout << "Freeing up mapping" << std::endl;
                //   calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                unsigned long freed_space_temp = used_space - (used_space % pagesize);
                if (deencode)
                {
                    if (munmap(&spill_map_char[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                }
                else
                {
                    if (munmap(&spill_map[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                }
                // std::cout << "Free: " << input_head << " - " << freed_space_temp / sizeof(long) + input_head << std::endl;
                // Update Head to point at the new unfreed mapping space.

                if (deencode)
                {
                    input_head += freed_space_temp;
                }
                else
                {
                    input_head += freed_space_temp / sizeof(long);
                }
                // std::cout << "Freed up mapping" << std::endl;
                //  std::cout << input_head << std::endl;
                //   Update numHashRows so that the estimations are still correct.

                // std::cout << "hashmap size: " << emHashmap.size() * avg << " freed space: " << freed_space_temp << std::endl;
            }
            if (!locked && used_space <= pagesize * 40 && hmap->size() * (*avg) + base_size >= memLimit * 0.9)
            {
                // std::cout << "head base: " << input_head_base << std::endl;
                locked = true;
                *input_head_base = i + 1;
            }
        }
    }
    // std::cout << "Writing hashmap size: " << emHashmap.size() << std::endl;

    //  save empty flag and release the mapping
    if (deencode)
    {
        if (mapping_size - input_head > 0)
        {
            if (munmap(&spill_map_char[input_head], mapping_size - input_head) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
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
        }
    }
    extra_mem -= increase;
    if (add)
    {
        *input_head_base = comb_spill_size;
    }
    return !locked;
}

void addXtoLocalSpillHead(std::vector<std::pair<int, size_t>> *spills, unsigned long *spill_head, int x)
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

int merge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> *spills, std::atomic<unsigned long> &comb_hash_size,
          float *avg, float memLimit, std::atomic<unsigned long> *diff, std::string &outputfilename, std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> *s3spillNames2, Aws::S3::S3Client *minio_client,
          bool writeRes, std::string &uName, size_t memMainLimit, size_t *output_file_head, char *done, size_t *max_hash_size, char partition = -1, char beggarWorker = 0, int output_fd = -1)
{
    // Open the outputfile to write results
    /* if (writeRes)
    {
        output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    } */
    auto merge_start_time = std::chrono::high_resolution_clock::now();
    diff->exchange(0);
    // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
    unsigned long input_head_base = 0;
    bool locked = true;
    unsigned long *spill_map = nullptr;
    char *spill_map_char = nullptr;
    unsigned long comb_spill_size = 0;
    unsigned long overall_size = 0;
    std::atomic<unsigned long> read_lines = 0;
    unsigned long written_lines = 0;
    // unsigned long maxHashsize = hmap->size();
    comb_hash_size.exchange(comb_hash_size.load() > hmap->size() ? comb_hash_size.load() : hmap->size());
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
        comb_spill_size += it.second;
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
    if (bitmap_size_sum > memLimit * 0.7)
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
                    perror("Error mmapping the file");
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
    {
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
    }
    extra_mem = bitmap_size_sum;
    size_t size_after_init = getPhyValue();
    bool increase_size = true;
    std::vector<int> write_counter(partitions, 0);
    std::vector<std::vector<std::pair<size_t, size_t>>> write_sizes(partitions);
    std::atomic<bool> writeLock;
    std::atomic<int> readNum;

    bool finished = false;
    bool increase = true;

    while (!finished)
    {
        // std::cout << "Start adding s3spillStart_head: " << s3spillStart_head << " bit_head: " << bit_head << std::endl;
        finished = subMerge(hmap, s3spillNames2, &s3spillBitmaps, spills, true, &s3spillFile_head, &bit_head, &subfile_head, &s3spillStart_head, &s3spillStart_head_chars, &input_head_base,
                            size_after_init, &read_lines, minio_client, &writeLock, &readNum, avg, memLimit, comb_hash_size, diff, increase, max_hash_size);
        increase = false;
        size_t n = 0;
        int int_n = 0;
        auto temp_it = s3spillNames2->begin();
        int bit_increase = 0;
        if (s3spillFile_head < s3spillNames2->size())
        {
            std::advance(temp_it, s3spillFile_head);
            bit_increase = get<2>(*temp_it).size();
            bit_head += bit_increase;
        }
        s3spillFile_head++;
        size_t old_input_head_base = input_head_base;
        addXtoLocalSpillHead(spills, &input_head_base, 1);
        // std::cout << "round local spill: " << old_input_head_base << " up to: " << input_head_base << std::endl;
        if (multiThread_subMerge)
        {
            int mergefile_num = s3spillNames2->size() - s3spillFile_head;
            size_t sum = 0;
            int counter = 0;
            for (auto &s : (*spills))
            {
                sum += s.second;
                if (deencode && input_head_base < sum)
                {
                    mergefile_num += spills->size() - counter;
                    break;
                }
                else if (!deencode && input_head_base < sum / sizeof(long))
                {
                    mergefile_num += spills->size() - counter;
                    break;
                }
                counter++;
            }
            // std::cout << "merge_file_numn: " << mergefile_num << std::endl;
            merge_file_num = std::max(2, mergefile_num / threadNumber);

            std::vector<std::thread> threads;
            std::vector<int> start_heads(s3spillNames2->size());
            std::vector<int> start_bits(s3spillNames2->size());
            std::vector<unsigned long> start_heads_local(spills->size());
            int s3_start_head = s3spillFile_head;
            int start_bit_head = bit_head;
            counter = 0;
            while (s3_start_head < s3spillNames2->size())
            {
                // std::cout << "merging s3 start_head: " << s3_start_head << " bit_start_head: " << start_bit_head << std::endl;
                start_heads[counter] = s3_start_head;
                start_bits[counter] = start_bit_head;
                threads.push_back(std::thread(subMerge, hmap, s3spillNames2, &s3spillBitmaps, spills, false, &start_heads[counter], &start_bits[counter], &int_n, &n, &n, &input_head_base,
                                              size_after_init, &read_lines, minio_client, &writeLock, &readNum, avg, memLimit, std::ref(comb_hash_size), diff, false, max_hash_size));
                counter++;
                if (s3_start_head < s3spillNames2->size())
                {
                    temp_it = s3spillNames2->begin();
                    std::advance(temp_it, s3_start_head);
                    for (int i = 0; i < merge_file_num; i++)
                    {
                        start_bit_head += get<2>(*temp_it).size();
                        temp_it++;
                    }

                    s3_start_head += merge_file_num;
                }
            }
            if (s3_start_head - s3spillNames2->size() > 0 && counter > 0)
            {
                addXtoLocalSpillHead(spills, &input_head_base, s3_start_head - s3spillNames2->size());
                // std::cout << "add local spill: " << s3_start_head - s3spillNames2->size() << " to: " << input_head_base << std::endl;
            }
            counter = 0;
            while (input_head_base < comb_spill_size)
            {
                // std::cout << "merging local input_head_base: " << input_head_base << std::endl;
                start_heads_local[counter] = input_head_base;
                threads.push_back(std::thread(subMerge, hmap, s3spillNames2, &s3spillBitmaps, spills, false, &s3_start_head, &start_bit_head, &int_n, &n, &n, &start_heads_local[counter],
                                              size_after_init, &read_lines, minio_client, &writeLock, &readNum, avg, memLimit, std::ref(comb_hash_size), diff, false, max_hash_size));
                counter++;
                addXtoLocalSpillHead(spills, &input_head_base, merge_file_num);
                // std::cout << "add local spill: " << merge_file_num << " to: " << input_head_base << std::endl;
            }
            for (auto &thread : threads)
            {
                thread.join();
            }
        }
        else
        {
            subMerge(hmap, s3spillNames2, &s3spillBitmaps, spills, false, &s3spillFile_head, &bit_head, &int_n, &n, &n, &input_head_base,
                     size_after_init, &read_lines, minio_client, &writeLock, &readNum, avg, memLimit, comb_hash_size, diff, false, max_hash_size);
        }
        s3spillFile_head--;
        bit_head -= bit_increase;
        input_head_base = old_input_head_base;

        if (writeRes)
        {
            written_lines += hmap->size();
            if (deencode)
            {
                // std::cout << "Writing hmap with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head_chars: " << s3spillStart_head_chars << " avg " << *avg << " base_size: " << base_size << " locked: " << locked << std::endl;
            }
            else
            {
                // std::cout << "Writing hmap with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head: " << s3spillStart_head << " avg " << *avg << " base_size: " << base_size << std::endl;
            }
            if (!fcntl(output_fd, F_GETFD))
            {
                std::cout << "reopening file handle" << std::endl;
                output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT, 0777);
            }
            bool asdf = false;
            std::cout << "Trying to get lock" << std::endl;
            while (!writing_ouput.compare_exchange_strong(asdf, true))
            {
                std::cout << writing_ouput.load() << std::endl;
            }
            std::cout << "writing output, output_file_head: " << *output_file_head  << " writing_ouput: " <<writing_ouput.load() << std::endl;
            *output_file_head += writeHashmap(hmap, output_fd, *output_file_head, pagesize * 30, outputfilename);
            std::cout << "free lock, output_file_head: " << *output_file_head << std::endl;
            writing_ouput.exchange(false);

            hmap->clear();
            // std::cout << "locked: " << locked << std::endl;
            // comb_hash_size = maxHashsize;
        }
        else
        {
            if (!finished || s3spillFile_head + s3spillStart_head > 0)
            {
                size_t spill_size = hmap->size() * sizeof(long) * (key_number + value_number);
                size_t comb_spill_size = 0;
                std::string local_spill_name = "local_mergeSpill_";
                for (auto &ls : spillThreads)
                {
                    comb_spill_size += get<1>(ls);
                }
                spillS3Hmap(hmap, minio_client, &write_sizes, uName, &write_counter, partition);
                /*if (memMainLimit < mainMem_usage + spill_size + comb_spill_size)
                {
                    if (memMainLimit <= mainMem_usage + spill_size)
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
                        std::vector<std::pair<file, char>> files = std::vector<std::pair<file, char>>(1, {temp_file, partition});
                        // std::cout << "Adding merge file: " << n_temp << " partition: " << partition << " write size: " << write_size << std::endl;
                        addFileToManag(minio_client, files, beggarWorker, 0);
                        // std::cout << "Finished adding file" << std::endl;
                    }
                }
            }
            else
            {
                log_file.sizes["linesRead"] += read_lines;
                return true;
            }
        }
    }
    for (auto &it : *spills)
        close(it.first);
    for (int i = 0; i < spills->size(); i++)
    {
        remove(("spill_file_" + std::to_string(i)).c_str());
    }
    for (auto &it : s3spillBitmaps)
    {
        if (it.first != -1)
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
    }

    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - merge_start_time).count()) / 1000000;
    // std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << " Read lines: " << read_lines << ". macroseconds/line: " << duration * 1000000 / read_lines << std::endl;
    log_file.sizes["linesRead"] += read_lines;
    log_file.sizes["linesWritten"] += written_lines;
    *done = 1;
    return 1;
}

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

void helpMergePhase(size_t memLimit, size_t memMainLimit, Aws::S3::S3Client minio_client, bool init, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap,
                    std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> &diff, float *avg, char startBeggarWorker = 0, char p_id = 0)
{
    int finished = 0;
    std::thread sizePrinter;

    std::vector<std::string> blacklist;

    char beggarWorker = startBeggarWorker;
    char partition_id = partition_id;
    std::string uName = "merge";
    std::string empty_string = "";
    int counter = 0;
    std::tuple<std::vector<file>, char, char> files;
    bool second_loaded = false;
    std::vector<std::string> file_names;
    std::thread minioSpiller;
    std::string first_fileName;
    std::string local_spillName = "helpMergeSpill";
    bool b_minioSpiller = false;
    std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> spills = {};
    size_t zero = 0;
    size_t max_hashSize = 0;

    if (init)
    {
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit, 1, std::ref(comb_hash_size), &diff, avg);
    }

    while (true)
    {
        spills.clear();
        char file_num = hmap->size() == 0 ? 2 : 1;
        getMergeFileName(hmap, &minio_client, beggarWorker, partition_id, &blacklist, &files, 0, file_num);
        if (get<1>(files) == 0)
        {
            if (hmap->size() > 0)
            {
                uName = worker_id;
                uName += "_merge_" + std::to_string(counter);
                std::vector<std::pair<int, size_t>> local_files = std::vector<std::pair<int, size_t>>(partitions, {-1, 0});
                std::cout << "spilling to " << uName << " hmap size: " << hmap->size() << std::endl;
                spillToMinio(hmap, local_files, uName, &minio_client, beggarWorker, 0, 1);
                // Try to change beggar worker or load in new files
                hmap->clear();
                std::unordered_map<std::string, char> file_stati;
                for (auto &f_name : file_names)
                {
                    file_stati.insert({f_name, 255});
                }
                setFileStatus(&minio_client, &file_stati, beggarWorker, partition_id, 0);
                file_names.clear();
                beggarWorker = 0;
                partition_id = 0;
                getMergeFileName(hmap, &minio_client, beggarWorker, partition_id, &blacklist, &files, 0, 2);
                if (get<1>(files) == 0)
                {
                    std::cout << "finish" << std::endl;
                    break;
                }
            }
            else
            {
                std::cout << "finish" << std::endl;
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

        std::vector<std::pair<int, size_t>> empty(0);
        std::cout << "Worker: " << beggarWorker << "; Partition: " << (int)(partition_id) << "; merging files: ";
        for (auto &merge_file : get<0>(files))
        {
            file_names.push_back(merge_file.name);
            spills.insert({merge_file.name, merge_file.size, merge_file.subfiles});
            std::cout << merge_file.name << ", ";
        }
        std::cout << std::endl;

        uName = worker_id;
        uName += "_merge_" + std::to_string(counter);
        char temp = true;

        merge(hmap, &empty, comb_hash_size, avg, memLimit, &diff, empty_string, &spills, &minio_client, false, uName, memMainLimit, &zero, &temp, &max_hashSize, partition_id, beggarWorker);
        // std::cout << "Merge finished" << std::endl;
        if (hmap->size() == 0)
        {
            std::unordered_map<std::string, char> file_stati;
            for (auto &f_name : file_names)
            {
                file_stati.insert({f_name, 255});
            }
            setFileStatus(&minio_client, &file_stati, beggarWorker, partition_id, 0);
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
    finished = 2;
    if (init)
    {
        sizePrinter.join();
    }
}

char getMergePartition(Aws::S3::S3Client *minio_client)
{
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

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename, size_t memLimit, bool measure_mem, Aws::S3::S3Client minio_client, size_t memLimitBack)
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
    std::vector<std::vector<std::pair<int, size_t>>> spills(0);
    std::atomic<unsigned long> numLines = 0;
    std::atomic<unsigned long> readBytes = 0;
    std::atomic<unsigned long> comb_hash_size = 0;
    std::atomic<unsigned long> comb_spill_size = 0;
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
    if (measure_mem)
    {
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit, threadNumber, std::ref(comb_hash_size), &diff, &avg);
    }

    // auto scan_start_time = std::chrono::high_resolution_clock::now();
    char id = 0;

    for (int i = 0; i < threadNumber - 1; i++)
    {
        emHashmaps[i] = {};
        threads.push_back(std::thread(fillHashmap, id, &emHashmaps[i], fd, t1_size * i, t1_size, true, memLimit / threadNumber,
                                      std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitBack, std::ref(comb_spill_size)));
        id++;
    }
    emHashmaps[threadNumber - 1] = {};
    threads.push_back(std::thread(fillHashmap, id, &emHashmaps[threadNumber - 1], fd, t1_size * (threadNumber - 1), t2_size, false, memLimit / threadNumber,
                                  std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitBack, std::ref(comb_spill_size)));

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

    auto mergeH_start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> spill_threads;
    std::string empty = "";
    std::vector<std::string> uNames;
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmap;
    std::vector<emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>> merge_emHashmaps(threadNumber);

    for (int i = 0; i < threadNumber; i++)
    {
        /* if ((comb_hash_size + emHashmaps[i].size()) * avg + base_size < memLimit * 0.9 && (emHashmap.size() + emHashmaps[i].size()) * avg + base_size < memLimit * 0.5)
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
                    comb_hash_size.fetch_add(1);
                }
            }
            comb_hash_size.fetch_sub(emHashmaps[i].size());
            emHashmaps[i] = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
            avg = ((getPhyValue() * 1024 - base_size) / (float)(comb_hash_size));
        }
        else
        { */
        std::string uName = "spill_" + std::to_string(i);
        std::vector<std::pair<int, size_t>> local_files = std::vector<std::pair<int, size_t>>(partitions, {-1, 0});
        spill_threads.push_back(std::thread(spillToMinio, &emHashmaps[i], local_files, uName, &minio_client, worker_id, 0, i));
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
        emHashmaps[i] = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
    }
    while (mana_writeThread_num.load() != 0)
    {
    }
    comb_hash_size.exchange(emHashmap.size());
    // delete[] emHashmaps;
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - mergeH_start_time).count()) / 1000000;
    std::cout << "Merging of hastables finished with time: " << duration << "s." << std::endl;
    log_file.sizes["mergeHashTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    log_file.sizes["mergeHashDuration"] = duration;

    finished++;

    if (mergePhase)
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
    }
    auto merge_start_time = std::chrono::high_resolution_clock::now();
    // calc optimistic new avg to better fit spill files as: 8/avgLineLength * (hash_avg - avg)
    // float avglineLengtth = size / numLines.load();
    // avg = hash_avg + (avg - hash_avg) * (sizeof(int) * 2 / avglineLengtth) + 0.02;
    // avg = avg + (float)(sizeof(int) - avglineLengtth) / 1024;
    // avg *= 8 / avglineLengtth;

    // std::cout << "new avg: " << avg << " hashmap size: " << emHashmap.size() << " hash avg: " << hash_avg << std::endl;

    // Free up rest of mapping of input file and close the file
    close(fd);
    avg = 1;

    // std::cout << "Scanning finished." << std::endl;

    // Open the outputfile to write results
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    unsigned long written_lines = 0;
    unsigned long read_lines = 0;
    comb_hash_size = emHashmap.size();
    unsigned long freed_mem = 0;
    unsigned long overall_size = 0;
    manaFile mana = getMana(&minio_client);
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

        size_t output_file_head = 0;
        std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond> files;
        std::vector<std::set<std::tuple<std::string, size_t, std::vector<std::pair<size_t, size_t>>>, CompareBySecond>> multi_files(threadNumber);
        std::vector<size_t> max_HashSizes(threadNumber, 0);
        char m_partition = getMergePartition(&minio_client);
        int counter = 0;

        std::vector<std::thread> merge_threads(threadNumber);
        std::vector<char> mergeThreads_done(threadNumber, 1);
        int thread_number = 0;
        while (m_partition != -1)
        {
            std::cout << "merging partition: " << (int)(m_partition) << std::endl;
            printProgressBar((float)(counter) / partitions);
            if (spills.size() == 0)
            {
                spills.push_back(std::vector<std::pair<int, size_t>>(0));
            }
            auto *m_spill = spills.size() == 1 ? &spills[0] : &spills[m_partition];

            if (multiThread_merge)
            {
                int newThread_ind = -1;

                while (newThread_ind == -1)
                {
                    int thread_ind_counter = 0;
                    for (auto &d : mergeThreads_done)
                    {
                        if (d)
                        {
                            newThread_ind = thread_ind_counter;
                            if (thread_number > threadNumber - 1)
                            {
                                merge_threads[newThread_ind].join();
                                thread_number--;
                            }
                            thread_number++;
                            d = 0;
                            break;
                        }
                        thread_ind_counter++;
                    }
                }

                files.clear();
                getAllMergeFileNames(&minio_client, m_partition, &multi_files[newThread_ind]);

                /* for (auto &name : files)
                {
                    std::cout << std::get<0>(name) << ", ";
                }
                std::cout << std::endl;
                std::string empty = "";
                std::cout << "output file head: " << output_file_head << std::endl; */

                std::cout << "newThread_ind: " << newThread_ind << std::endl;
                merge_threads[newThread_ind] = std::thread(merge, &merge_emHashmaps[newThread_ind], m_spill, std::ref(comb_hash_size), &avg, memLimit, &diff, std::ref(outputfilename), &multi_files[newThread_ind],
                                                           &minio_client, true, std::ref(empty), memLimitBack, &output_file_head, &mergeThreads_done[newThread_ind], &max_HashSizes[newThread_ind], -1, 0, output_fd);
            }
            else
            {
                files.clear();
                getAllMergeFileNames(&minio_client, m_partition, &files);

                /* for (auto &name : files)
                {
                    std::cout << std::get<0>(name) << ", ";
                }
                std::cout << std::endl;
                std::string empty = "";
                std::cout << "output file head: " << output_file_head << std::endl; */
                merge(&emHashmap, m_spill, comb_hash_size, &avg, memLimit, &diff, outputfilename, &files, &minio_client, true, empty, memLimitBack, &output_file_head, &mergeThreads_done[0], &max_HashSizes[0], -1, 0, output_fd);
            }
            m_partition = getMergePartition(&minio_client);
            counter++;
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
        for (int i = 0; i < thread_number; i++)
        {
            std::cout << "waiting for thread: " << i << std::endl;
            merge_threads[i].join();
        }
    }
    else
    {
        // std::cout << "writing to output file" << std::endl;
        written_lines += emHashmap.size();

        // write hashmap to output file
        writeHashmap(&emHashmap, output_fd, 0, pagesize * 10, outputfilename);
    }
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - merge_start_time).count()) / 1000000;
    std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << std::endl;
    log_file.sizes["mergeDuration"] = duration;
    log_file.sizes["mergeTime"] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count();

    close(output_fd);
    if (measure_mem)
    {
        finished++;
        sizePrinter.join();
    }
    return 0;
}

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

    Aws::SDKOptions options;
    // options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    Aws::InitAPI(options);

    if (argc == 2)
    {
        std::string f = argv[1];
        if (f.compare("status") == 0)
        {
            Aws::S3::S3Client minio_client_2 = init();
            manaFile asdf;
            printMana(&minio_client_2, asdf);
            Aws::ShutdownAPI(options);
            return 1;
        }
    }

    /*  Aws::S3::S3Client minio_client_2 = init();
     worker_id = '1';
     initManagFile(&minio_client_2);
     std::cout << "printing mana" << std::endl;
     printMana(&minio_client_2);
     manaFile mana = getMana(&minio_client_2);
     partition p;
     p.id = 1;
     mana.workers[0].partitions.push_back(p);
     writeMana(&minio_client_2, mana, true);
     printMana(&minio_client_2);
     Aws::ShutdownAPI(options);
     return 1; */
    std::string tpc_sup;
    std::string memLimit_string;
    std::string memLimitBack_string;
    std::string threadNumber_string;
    std::string tpc_query_string;
    std::string log_size_string;
    std::string log_time_string;
    std::string co_output;

    if (argc == 10)
    {
        co_output = argv[1];
        tpc_sup = argv[2];
        memLimit_string = argv[3];
        memLimitBack_string = argv[4];
        threadNumber_string = argv[5];
        tpc_query_string = argv[6];
        worker_id = *argv[7];
        log_size_string = argv[8];
        log_time_string = argv[9];
    }
    else
    {
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
            switch (str2int(name.c_str()))
            {
            case str2int("tpc_query"):
            {
                tpc_query_string = value;
                break;
            }
            case str2int("input_file"):
            {
                co_output = value;
                break;
            }
            case str2int("test_file"):
            {
                tpc_sup = value;
                break;
            }
            case str2int("mainLimit"):
            {
                memLimit_string = value;
                break;
            }
            case str2int("backLimit"):
            {
                memLimitBack_string = value;
                break;
            }
            case str2int("threadNumber"):
            {
                threadNumber_string = value;
                break;
            }
            case str2int("log_size"):
            {
                log_size_string = value;
                break;
            }
            case str2int("log_time"):
            {
                log_time_string = value;
                break;
            }
            case str2int("worker_id"):
            {
                worker_id = value[0];
                break;
            }
            case str2int("deencode"):
            {
                deencode = value.compare("true") == 0;
                break;
            }
            case str2int("set_partitions"):
            {
                set_partitions = value.compare("true") == 0;
                break;
            }
            case str2int("straggler_removal"):
            {
                straggler_removal = value.compare("true") == 0;
                break;
            }
            case str2int("mergePhase"):
            {
                mergePhase = value.compare("true") == 0;
                break;
            }
            case str2int("multiThread_subMerge"):
            {
                multiThread_subMerge = value.compare("true") == 0;
                break;
            }
            case str2int("multiThread_merge"):
            {
                multiThread_merge = value.compare("true") == 0;
                break;
            }
            }
        }
    }

    log_size = log_size_string.compare("true") == 0;
    log_time = log_time_string.compare("true") == 0;

    threadNumber = std::stoi(threadNumber_string);
    int tpc_query = std::stoi(tpc_query_string);
    size_t memLimit = (std::stof(memLimit_string) - 0.01) * (1ul << 30);

    // memLimit -= 1ull << 20;
    size_t memLimitBack = std::stof(memLimitBack_string) * (1ul << 30);
    pagesize = sysconf(_SC_PAGE_SIZE);

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
    std::string agg_output = "output_" + tpc_sup;
    Aws::S3::S3Client minio_client = init();

    bool it = argc == 3;
    int it_number = it ? 1 : 5;
    initManagFile(&minio_client);
    start_time = std::chrono::high_resolution_clock::now();
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    // Visit http://en.cppreference.com/w/cpp/chrono/c/strftime
    // for more information about date/time format
    // strftime(buf, sizeof(buf), "%m-%d_%H-%M", &tstruct);
    strftime(buf, sizeof(buf), "%H-%M", &tstruct);
    date_now = tpc_query_string + "_" + memLimit_string + "_" + memLimitBack_string + "_" + threadNumber_string + "_" + buf;
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
    std::string suffix = "json";
    if (co_output != "-")
    {
        isJson = co_output.substr(co_output.length() - suffix.length()) == suffix;
        try
        {
            aggregate(co_output, agg_output, memLimit, true, minio_client, memLimitBack);
        }
        catch (std::exception &err)
        {
            std::cout << "conversion error on: " << err.what() << std::endl;
            log_file.err_msg = err.what();
            failed = true;
        }
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start_time).count()) / 1000000;
        std::cout << "Aggregation finished. With time: " << duration << "s. Checking results." << std::endl;
        log_file.sizes["queryDuration"] = duration;
        log_file.failed = failed;
        log_size = false;
    }
    if (tpc_sup != "-" && !failed)
    {
        try
        {
            test(agg_output, tpc_sup);
        }
        catch (std::exception &err)
        {
            std::cout << "conversion error on: " << err.what() << std::endl;
        }
    }
    std::atomic_ulong comb_hash_size = 0;
    std::atomic_ulong diff = 0;
    float avg = 1;
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> hmap;
    try
    {
        helpMergePhase(memLimit, memLimitBack, minio_client, true, &hmap, comb_hash_size, diff, &avg);
    }
    catch (std::exception &err)
    {
        std::cout << "conversion error on: " << err.what() << std::endl;
    }

    cleanup(&minio_client);
    Aws::ShutdownAPI(options);
    if (log_time)
    {
        writeLogFile(log_file);
    }

    return 1;
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");S */
}