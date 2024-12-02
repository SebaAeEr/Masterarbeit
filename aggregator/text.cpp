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

enum Operation
{
    count,
    sum,
    exists,
    average
};

struct manaFileWorker
{
    char id;
    int length;
    bool locked;
    std::vector<std::tuple<std::string, char, size_t, std::vector<size_t>, unsigned char>> files;
};

struct manaFile
{
    char worker_lock;
    char thread_lock;
    std::vector<manaFileWorker> workers;
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
size_t max_s3_spill_size = 0;

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
    bool operator()(const std::tuple<std::string, size_t, std::vector<size_t>> &a, const std::tuple<std::string, size_t, std::vector<size_t>> &b) const
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

manaFile getMana(Aws::S3::S3Client *minio_client)
{
    manaFile mana;
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
    Aws::S3::Model::PutObjectRequest in_request;
    in_request.SetBucket(bucketName);
    in_request.SetKey(manag_file_name);
    const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
    auto &out_stream = outcome.GetResult().GetBody();
    size_t out_size = outcome.GetResult().GetContentLength();

    mana.worker_lock = out_stream.get();
    mana.thread_lock = out_stream.get();
    mana.workers = {};
    while (out_stream.peek() != EOF)
    {
        manaFileWorker worker;
        char workerid = out_stream.get();
        worker.id = workerid;
        worker.locked = out_stream.get() == 1;
        std::vector<std::tuple<std::string, char, size_t, std::vector<size_t>, unsigned char>> files = {};
        int length;
        char length_buf[sizeof(int)];
        out_stream.read(length_buf, sizeof(int));
        std::memcpy(&length, &length_buf, sizeof(int));
        worker.length = length;
        int head = 0;
        while (head < length)
        {
            char temp = out_stream.get();
            std::string filename = "";
            while (temp != ',')
            {
                filename += temp;
                temp = out_stream.get();
            }

            char number = out_stream.get();

            size_t file_length;
            char length_buf[sizeof(size_t)];
            out_stream.read(length_buf, sizeof(size_t));
            std::memcpy(&file_length, &length_buf, sizeof(size_t));

            std::vector<size_t> sub_files = {};
            for (char i = 0; i < number; i++)
            {
                size_t sub_file_length;
                length_buf[sizeof(size_t)];
                out_stream.read(length_buf, sizeof(size_t));
                std::memcpy(&sub_file_length, &length_buf, sizeof(size_t));
                sub_files.push_back(sub_file_length);
            }
            char m_worker = out_stream.get();
            files.push_back({filename, number, file_length, sub_files, m_worker});

            head += sizeof(size_t) * number + sizeof(size_t) + filename.size() + 3;
        }
        worker.files = files;
        mana.workers.push_back(worker);
    }
    return mana;
}

bool writeMana(Aws::S3::S3Client *minio_client, manaFile mana, bool freeLock)
{
    while (true)
    {
        auto start_time = std::chrono::high_resolution_clock::now();
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
            for (auto &file : worker.files)
            {
                *in_stream << get<0>(file);
                *in_stream << ',';

                *in_stream << get<1>(file);

                char file_length_buf[sizeof(size_t)];
                std::memcpy(file_length_buf, &get<2>(file), sizeof(size_t));
                for (int i = 0; i < sizeof(size_t); i++)
                {
                    *in_stream << file_length_buf[i];
                }

                for (auto &sub_file : get<3>(file))
                {
                    file_length_buf[sizeof(size_t)];
                    std::memcpy(file_length_buf, &sub_file, sizeof(size_t));
                    for (int i = 0; i < sizeof(size_t); i++)
                    {
                        *in_stream << file_length_buf[i];
                    }
                }
                *in_stream << get<4>(file);
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
                    // std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
                    return false;
                }
            }
            return 1;
        }
        //}
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
    while (true)
    {
        manaFile mana = getMana(minio_client);
        if (mana.worker_lock == 0)
        {
            // std::cout << "Trying to get lock: " << std::to_string((int)(thread_id)) << std::endl;
            manaFile mana = getMana(minio_client);
            mana.worker_lock = worker_id;
            mana.thread_lock = thread_id;
            if (!writeLock(minio_client))
            {
                // std::cout << "Failed getting lock: " << std::to_string((int)(thread_id)) << std::endl;
                continue;
            }
            writeMana(minio_client, mana, false);
            mana = getMana(minio_client);
            if (mana.worker_lock == worker_id && mana.thread_lock == thread_id)
            {
                // std::cout << "Lock received by: " << std::to_string((int)(thread_id)) << " old thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
                mana = getMana(minio_client);
                // std::cout << " new thread lock: " << std::to_string((int)(mana.thread_lock)) << std::endl;
                return mana;
            }
        }
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

void printMana(Aws::S3::S3Client *minio_client)
{
    manaFile mana = getMana(minio_client);
    std::string status = mana.worker_lock == 0 ? "free" : std::to_string(mana.worker_lock);
    std::cout << "worker lock: " << status << ", thread lock: " << std::bitset<8>(mana.thread_lock) << std::endl;
    for (auto &worker : mana.workers)
    {
        std::cout << "Worker id: " << worker.id << " locked: " << worker.locked << std::endl;
        for (auto &file : worker.files)
        {
            std::cout << "  " << std::get<0>(file) << "size: " << std::get<2>(file) << " worked on by: " << std::bitset<8>(std::get<4>(file)) << " subfiles:" << std::endl;
            for (auto &sub_files : std::get<3>(file))
            {
                std::cout << "    size: " << sub_files << std::endl;
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
    worker.files = {};
    worker.locked = false;
    mana.workers.push_back(worker);
    mana.thread_lock = 0;
    mana.worker_lock = 0;
    writeMana(minio_client, mana, true);
    printMana(minio_client);
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

int addFileToManag(Aws::S3::S3Client *minio_client, std::string &file_name, std::vector<size_t> file_size, size_t comb_file_size, char write_to_id, unsigned char fileStatus, char thread_id)
{
    manaFile mana = getLockedMana(minio_client, thread_id);
    for (auto &worker : mana.workers)
    {
        if (worker.id == write_to_id)
        {
            if (worker.locked)
            {
                writeMana(minio_client, mana, true);
                return 0;
            }
            worker.files.push_back({file_name, file_size.size(), comb_file_size, file_size, fileStatus});
            worker.length += file_name.size() + 3 + sizeof(size_t) + sizeof(size_t) * file_size.size();
            break;
        }
    }
    writeMana(minio_client, mana, true);
    return 1;
}

std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond> *getAllMergeFileNames(Aws::S3::S3Client *minio_client)
{
    std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond> *files = new std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond>();
    manaFile mana = getMana(minio_client);
    for (auto &worker : mana.workers)
    {
        if (worker.id == worker_id)
        {
            for (auto &file : worker.files)
            {
                if (get<4>(file) != 255)
                {
                    files->insert({get<0>(file), get<2>(file), get<3>(file)});
                }
            }
        }
    }
    return files;
}

void getMergeFileName(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, Aws::S3::S3Client *minio_client,
                      char beggarWorker, size_t memLimit, float *avg, std::vector<std::string> *blacklist, std::pair<std::tuple<std::string, size_t, std::vector<size_t>>, char> *res, char thread_id)
{
    *res = {{"", 0, {}}, 0};
    char given_beggarWorker = beggarWorker;
    std::vector<char> worker_blacklist = {};
    std::tuple<std::string, size_t, std::vector<size_t>> m_file = {"", 0, {}};
    manaFile mana = getLockedMana(minio_client, thread_id);
    while (true)
    {
        // If no beggarWorker is yet selected choose the worker with the largest spill
        if (beggarWorker == 0)
        {
            size_t max = 0;
            for (auto &worker : mana.workers)
            {
                if (!worker.locked && !std::count(worker_blacklist.begin(), worker_blacklist.end(), worker.id))
                {
                    size_t size_temp = 0;
                    for (auto &file : worker.files)
                    {
                        if (get<4>(file) == 0 && !std::count(blacklist->begin(), blacklist->end(), get<0>(file)))
                        {
                            size_temp += get<2>(file);
                        }
                    }
                    if (max < size_temp)
                    {
                        max = size_temp;
                        beggarWorker = worker.id;
                    }
                }
            }
        }
        if (beggarWorker == 0)
        {
            writeMana(minio_client, mana, true);
            return;
        }
        for (auto &worker : mana.workers)
        {
            if (worker.id == beggarWorker)
            {
                if (worker.locked)
                {
                    beggarWorker = 0;
                    break;
                }
                size_t max = 0;
                for (auto &file : worker.files)
                {
                    if (get<4>(file) == 0 && !std::count(blacklist->begin(), blacklist->end(), get<0>(file)))
                    {
                        size_t size_temp = get<2>(file);
                        if (size_temp > max && (size_temp / (sizeof(unsigned long) * (key_number + value_number)) + hmap->size()) * (*avg) + base_size < memLimit * 0.9)
                        {
                            max = size_temp;
                            m_file = {get<0>(file), size_temp, get<3>(file)};
                        }
                    }
                }
                break;
            }
        }
        if (get<1>(m_file) == 0)
        {
            if (given_beggarWorker == 0)
            {
                worker_blacklist.push_back(beggarWorker);
                beggarWorker = 0;
            }
            else
            {
                writeMana(minio_client, mana, true);
                return;
            }
        }
        else
        {
            break;
        }
    }

    for (auto &worker : mana.workers)
    {
        if (worker.id == beggarWorker)
        {
            for (auto &file : worker.files)
            {
                if (get<0>(file) == get<0>(m_file))
                {
                    get<4>(file) = worker_id;
                    break;
                }
            }
        }
    }
    writeMana(minio_client, mana, true);
    *res = {m_file, beggarWorker};
    return;
}

// Write hashmap hmap into file with head on start.
unsigned long writeHashmap(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, unsigned long start, unsigned long free_mem)
{
    // Calc the output size for hmap.
    unsigned long output_size = 0;
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

    output_size += (strlen("\"_col1\":") + 2 + key_number) * hmap->size();
    // std::cout << "Output file size: " << output_size << std::endl;

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
        close(file);
        std::cout << "Start: " << output_size << " + " << start_diff << " start_page: " << start_page << std::endl;
        perror("Error mmapping the file in write Hashmap");
        exit(EXIT_FAILURE);
    }
    unsigned long freed_mem = 0;

    // Write into file through mapping. Starting at the given start point.
    unsigned long mapped_count = start_diff;
    unsigned long head = 0;
    for (auto &it : *hmap)
    {
        mapped_count += writeString(&mappedoutputFile[mapped_count], "{");
        // std::string temp_line = "{";
        for (int k = 0; k < key_number; k++)
        {
            mapped_count += writeString(&mappedoutputFile[mapped_count], "\"");
            mapped_count += writeString(&mappedoutputFile[mapped_count], key_names[k]);
            mapped_count += writeString(&mappedoutputFile[mapped_count], "\":");
            mapped_count += writeString(&mappedoutputFile[mapped_count], std::to_string(it.first[k]));

            // temp_line += "\"" + key_names[k] + "\":" + std::to_string(it.first[k]);
            if (k + 1 < key_number)
            {
                // temp_line += ",";
                mapped_count += writeString(&mappedoutputFile[mapped_count], ",");
            }
        }
        mapped_count += writeString(&mappedoutputFile[mapped_count], ",\"_col1\":");
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
        mapped_count += writeString(&mappedoutputFile[mapped_count], "}");
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
    return output_size;
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
                    (*lineObjects)[key] += char_temp;
                    i++;
                    char_temp = mapping[i];
                }
                if (char_temp != '}')
                {
                    readingMode++;
                }
                else
                {
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

void spillToFile(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::pair<int, size_t> *spill_file, char id, size_t free_mem, std::string &fileName)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(unsigned long) * (key_number + value_number);

    if (spill_file->first == -1)
    {
        spill_file->first = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    }

    // extend file
    lseek(spill_file->first, spill_file->second + spill_mem_size - 1, SEEK_SET);
    if (write(spill_file->first, "", 1) == -1)
    {
        close(spill_file->first);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }

    size_t start_diff = spill_file->second % pagesize;
    size_t start = spill_file->second - start_diff;

    // Create mapping to file
    unsigned long *spill = (unsigned long *)(mmap(nullptr, spill_mem_size + start_diff, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file->first, start));
    madvise(spill, spill_mem_size + start_diff, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (spill == MAP_FAILED)
    {
        close(spill_file->first);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
    size_t freed_mem = 0;

    // Write int to Mapping
    unsigned long counter = start_diff / sizeof(unsigned long);
    unsigned long writehead = 0;
    for (auto &it : *hmap)
    {
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

        if ((counter - writehead) * sizeof(unsigned long) >= free_mem && (counter - writehead) * sizeof(unsigned long) > pagesize)
        {
            unsigned long used_space = (counter - writehead) * sizeof(unsigned long);
            unsigned long freed_space = used_space - (used_space % pagesize);
            munmap(&spill[writehead], freed_space);
            writehead += freed_space / sizeof(unsigned long);
            freed_mem += freed_space;
        }
    }

    // Cleanup: clear hashmap and free rest of mapping space
    munmap(&spill[writehead], (spill_mem_size + start_diff) - writehead * sizeof(unsigned long));
    freed_mem += (spill_mem_size + start_diff) - writehead * sizeof(unsigned long);
    spill_file->second += spill_mem_size;
    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}

int spillToMinio(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::string &file, std::string uniqueName,
                 Aws::S3::S3Client *minio_client, char write_to_id, unsigned char fileStatus, char thread_id)
{
    size_t spill_mem_size = hmap->size() * sizeof(unsigned long) * (key_number + value_number);
    int counter = 0;
    std::vector<size_t> sizes = {};
    while (max_s3_spill_size * counter <= spill_mem_size)
    {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(uniqueName + "_" + std::to_string(counter));

        size_t spill_mem_size_temp = std::min(max_s3_spill_size, spill_mem_size - max_s3_spill_size * counter);
        if (spill_mem_size - max_s3_spill_size * (counter + 1) < 2048)
        {
            spill_mem_size_temp += spill_mem_size - max_s3_spill_size * (counter + 1);
            counter++;
        }
        // std::cout << spill_mem_size_temp << ", " << spill_mem_size << ", " << spill_mem_size - max_s3_spill_size * counter << std::endl;
        //  if(spill_mem_size_temp < )
        sizes.push_back(spill_mem_size_temp);
        counter++;
        // Calc spill size

        if (file == "")
        {
            const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
            unsigned long temp_counter = 0;
            // Write int to Mapping
            for (auto &it : *hmap)
            {
                if (temp_counter * sizeof(unsigned long) * (key_number + value_number) == spill_mem_size_temp)
                {
                    break;
                }
                temp_counter++;
                char byteArray[sizeof(long int)];
                for (int i = 0; i < key_number; i++)
                {
                    // std::cout << it.first[i];
                    std::memcpy(byteArray, &it.first[i], sizeof(long int));
                    for (int k = 0; k < sizeof(unsigned long); k++)
                    {
                        *in_stream << byteArray[k];
                    }
                }
                for (int i = 0; i < value_number; i++)
                {
                    std::memcpy(byteArray, &it.second[i], sizeof(long int));
                    for (int k = 0; k < sizeof(unsigned long); k++)
                        *in_stream << byteArray[k];
                }
            }
            request.SetBody(in_stream);
        }
        else
        {
            struct stat stats;
            stat(file.c_str(), &stats);
            spill_mem_size = stats.st_size;
            const std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>("", file.c_str(), std::ios_base::in | std::ios_base::binary);
            // const std::shared_ptr<Aws::IOStream> inputData = temp;
            request.SetBody(inputData);
        }
        request.SetContentLength(spill_mem_size_temp);

        while (true)
        {
            auto outcome = minio_client->PutObject(request);

            if (!outcome.IsSuccess())
            {
                std::cout << "Error: " << outcome.GetError().GetMessage() << " Spill size: " << spill_mem_size_temp << std::endl;
            }
            else
            {
                break;
            }
        }
    }
    return addFileToManag(minio_client, uniqueName, sizes, spill_mem_size, write_to_id, fileStatus, thread_id);
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
                 float &avg, std::vector<std::pair<int, size_t>> *spill_files, std::atomic<unsigned long> &numLines, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *shared_diff, Aws::S3::S3Client *minio_client,
                 std::atomic<unsigned long> &readBytes, unsigned long memLimitMain, std::atomic<unsigned long> &comb_spill_size)
{
    // Aws::S3::S3Client minio_client = init();
    //  hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    auto start_time = std::chrono::high_resolution_clock::now();
    auto thread_time = std::chrono::high_resolution_clock::now();
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
    bool spilltoS3 = false;
    bool spillS3Thread = false;
    bool thread_finishFlag = false;
    std::string uName;
    std::string spill_file_name;

    // loop through entire mapping
    for (unsigned long i = 0; i < size + offset; i++)
    {
        i = parseJson(mappedFile, i, coloumns, &lineObjects, size);
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
        }
        else
        {

            // add customerkey, count pair to hashmap. When orderkey is not null count starts at 1.
            addPair(hmap, keys, opValue);
            if (hmap->size() > maxHmapSize)
            {
                comb_hash_size.fetch_add(1);
            }
        }

        // Check if Estimations exceed memlimit
        // if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
        if (hmap->size() * avg + base_size / threadNumber >= memLimit * 0.9)
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
            if (freed_space_temp <= pagesize * 10 && hmap->size() * (key_number + value_number) * sizeof(unsigned long) > pagesize)
            {
                // std::cout << "spilling with size: " << hmap->size() << " i-head: " << (i - head + 1) << " size: " << getPhyValue() << std::endl;
                //    Reset freed_space and update numHashRows so that Estimation stay correct
                if (maxHmapSize < hmap->size())
                {
                    maxHmapSize = hmap->size();
                    // std::cout << "new MaxSize: " << maxHmapSize << std::endl;
                }
                unsigned long temp_spill_size = hmap->size() * (key_number + value_number) * sizeof(unsigned long);
                spill_size += temp_spill_size;
                comb_spill_size.fetch_add(temp_spill_size);
                spilltoS3 = memLimitMain < spill_size + temp_spill_size * 2;

                std::pair<int, size_t> spill_file(-1, 0);
                // std::cout << spill_file_name << ", " << hmap->size() << std::endl;

                if (spilltoS3)
                {
                    if (memLimitMain > spill_size + temp_spill_size)
                    {
                        if (spillS3Thread)
                        {
                            minioSpiller.join();
                        }
                        spill_file_name = "";
                        spill_file_name += worker_id;
                        spill_file_name += "_";
                        spill_file_name += std::to_string((int)(id));
                        spill_file_name += "_";
                        spill_file_name += "temp_spill";
                        spillToFile(hmap, &spill_file, id, pagesize * 20, spill_file_name);
                        uName = "";
                        uName += worker_id;
                        uName += "_";
                        uName += std::to_string((int)(id));
                        uName += "_" + std::to_string(spill_number);
                        // spillToMinio(hmap, std::ref(temp_spill_file_name), std::ref(uName), pagesize * 20, &minio_client, worker_id, 0, id);
                        minioSpiller = std::thread(spillToMinio, hmap, std::ref(spill_file_name), uName, minio_client, worker_id, 0, id);
                        spillS3Thread = true;
                    }
                    else
                    {
                        uName = "";
                        uName += worker_id;
                        uName += "_";
                        uName += std::to_string((int)(id));
                        uName += "_" + std::to_string(spill_number);
                        std::string empty = "";
                        if (!spillToMinio(hmap, empty, uName, minio_client, worker_id, 0, id))
                        {
                            std::cout << "Spilling to Minio failed because worker is locked!" << std::endl;
                        }
                    }
                }
                else
                {
                    spill_file_name = "";
                    spill_file_name += worker_id;
                    spill_file_name += "_";
                    spill_file_name += std::to_string((int)(id));
                    spill_file_name += "_";
                    spill_file_name += std::to_string(spill_number);
                    spill_file_name += "_";
                    spill_file_name += "spill";
                    spillToFile(hmap, &spill_file, id, pagesize * 20, spill_file_name);
                    spill_files->push_back(spill_file);
                }
                spill_number++;

                // std::cout << "Spilling ended" << std::endl;
                hmap->clear();
            }
        }
        // After line is read clear it and set reading to false till the next {
        numLines.fetch_add(1);
        numLinesLocal++;
    }

    if (munmap(&mappedFile[head], size - head) == -1)
    {
        std::cout << "head: " << head << " freed_space_temp: " << size - head << std::endl;
        perror("Could not free memory in end of thread!");
    }
    if (spillS3Thread)
    {
        minioSpiller.join();
    }
    try
    {
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        std::cout << "Thread " << int(id) << " finished scanning. With time: " << duration << "s. Scanned Lines: " << numLinesLocal << ". microseconds/line: " << duration * 1000000 / numLinesLocal << ". Spilled with size: " << spill_size << std::endl;
    }
    catch (std::exception &err)
    {
        std::cout << "Not able to print time: " << err.what() << std::endl;
    }
}

void printSize(int &finished, size_t memLimit, int threadNumber, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> *diff, float *avg, unsigned long *extra_mem)
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
                output << std::to_string(newsize) << "," << std::to_string((*avg) * comb_hash_size.load()) << "," << std::to_string(phyMemBase) << "," << std::to_string(reservedMem) << "," << std::to_string(*extra_mem) << "," << std::to_string(*avg) << "," << std::to_string(duration);
                // output << concat_string;
                output << std::endl;
                // std::cout << concat_string << std::endl;
            }
        }
        size = newsize;
        base_size = phyMemBase + reservedMem + (*extra_mem);
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
    output.close();
}

int merge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> *spills, std::atomic<unsigned long> &comb_hash_size,
          float *avg, float memLimit, std::atomic<unsigned long> *diff, std::string &outputfilename, std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond> *s3spillNames2, Aws::S3::S3Client *minio_client, unsigned long *extra_mem, bool writeRes)
{
    // Open the outputfile to write results
    int output_fd;
    if (writeRes)
    {
        output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    }
    auto start_time = std::chrono::high_resolution_clock::now();
    diff->exchange(0);
    // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
    unsigned long input_head_base = 0;
    unsigned long output_head = 0;
    bool locked = true;
    unsigned long *spill_map = nullptr;
    unsigned long comb_spill_size = 0;
    unsigned long freed_mem = 0;
    unsigned long overall_size = 0;
    unsigned long read_lines = 0;
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
    int bit_head = 0;
    unsigned long s3spillStart_head = 0;
    unsigned long overall_s3spillsize = 0;

    for (auto &it : *spills)
        comb_spill_size += it.second;

    int counter = 0;
    for (auto &name : *s3spillNames2)
    {
        overall_s3spillsize += get<1>(name);
        bitmap_size_sum += std::ceil((float)(get<1>(name)) / 8);
    }
    if (bitmap_size_sum > memLimit * 0.7)
    {
        std::cout << "Spilling bitmaps with size: " << bitmap_size_sum << std::endl;
        for (auto &name : *s3spillNames2)
        {
            int counter = 0;
            for (auto &s : get<2>(name))
            {
                size_t size = std::ceil((float)(s) / 8);
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
        std::cout << "Keeping bitmaps in mem with size: " << bitmap_size_sum << std::endl;
        // not spilling bitmaps
        for (auto &name : *s3spillNames2)
        {
            for (auto &s : get<2>(name))
            {
                std::vector<char> bitmap = std::vector<char>(std::ceil((float)(s) / 8), -1);
                s3spillBitmaps.push_back({-1, bitmap});
            }
        }
    }
    *extra_mem = bitmap_size_sum;
    printProgressBar(0);
    size_t size_after_init = getPhyValue();
    bool increase_size = true;
    // char buffer[(int)((memLimit - size_after_init * 1024) * 0.1)];

    // std::cout << "buffer size: " << (memLimit - size_after_init * 1024) * 0.1 << std::endl;

    // create mapping to spill

    // merge and fill hashmap with all spills
    while (locked)
    {
        // input_head_base;
        locked = false;
        unsigned long num_entries = 0;
        unsigned long input_head = 0;
        unsigned long offset = 0;
        unsigned long sum = 0;
        unsigned long newi = 0;
        size_t mapping_size = 0;

        // std::cout << "merger: freed_mem: " << freed_mem << " size: " << overall_size << std::endl;
        overall_size = 0;
        freed_mem = 0;
        // std::cout << "write: " << emHashmap[{221877}][0] << std::endl;
        bool firsts3File = false;
        // std::cout << "s3spillFile: " << s3spillFile_head << std::endl;
        // std::cout << "s3spillStart: " << s3spillStart_head << std::endl;

        int number_of_longs = key_number + value_number;
        int i = s3spillFile_head;
        int bit_i = bit_head;
        for (auto set_it = std::next(s3spillNames2->begin(), i); set_it != s3spillNames2->end(); set_it++)
        {
            std::cout << "Reading " << get<0>(*set_it) << std::endl;
            firsts3File = hmap->empty();
            int sub_file_counter = 0;
            for (auto &sub_file : get<2>(*set_it))
            {
                std::cout << "Reading " << get<0>(*set_it) + "_" + std::to_string(sub_file_counter) << std::endl;
                // std::cout << "Start reading: " << (*set_it).first << std::endl;
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
                        std::cout << "GetObject error " << get<0>(*set_it) << " " << outcome.GetError().GetMessage() << std::endl;
                    }
                    else
                    {
                        break;
                    }
                }
                // std::cout << "Reading spill: " << (*set_it).first << std::endl;
                auto &spill = outcome.GetResult().GetBody();
                // spill.rdbuf()->pubsetbuf(buffer, 1ull << 10);
                //   spill.rdbuf()->
                char *bitmap_mapping;
                std::vector<char> *bitmap_vector;
                bool spilled_bitmap = s3spillBitmaps[bit_i].first != -1;
                if (!spilled_bitmap)
                {
                    bitmap_vector = &s3spillBitmaps[bit_i].second;
                }
                else
                {
                    bitmap_mapping = static_cast<char *>(mmap(nullptr, std::ceil((float)(sub_file) / 8), PROT_WRITE | PROT_READ, MAP_SHARED, s3spillBitmaps[bit_i].first, 0));
                    if (bitmap_mapping == MAP_FAILED)
                    {
                        perror("Error mmapping the file");
                        exit(EXIT_FAILURE);
                    }
                    madvise(bitmap_mapping, std::ceil((float)(sub_file) / 8), MADV_SEQUENTIAL | MADV_WILLNEED);
                }
                // std::cout << "Reading spill: " << (*s3spillNames)[i] << " with bitmap of size: " << bitmap_vector->size() << std::endl;
                unsigned long head = 0;
                if (firsts3File)
                {
                    head = s3spillStart_head;
                    // std::cout << "First File" << std::endl;
                    spill.ignore(s3spillStart_head * sizeof(unsigned long) * number_of_longs);
                    // std::cout << "Load bitmap: " << i << " at index: " << head << std::endl;
                }

                unsigned long lower_index = 0;
                if (increase_size)
                {
                    auto increase = (getPhyValue() - size_after_init) * 1024;
                    std::cout << "Stream buffer: " << increase << std::endl;
                    *extra_mem += increase;
                    increase_size = false;
                }
                while (spill.peek() != EOF)
                {
                    char *bit;
                    size_t index = std::floor(head / 8);
                    if (!spilled_bitmap)
                    {
                        // std::cout << "New round: " << index << std::endl;
                        bit = &(*bitmap_vector)[index];
                    }
                    else
                    {
                        bit = &bitmap_mapping[index];
                    }

                    // std::cout << "accessing index: " << std::floor(head / 8) << ": " << std::bitset<8>(*bit) << " AND " << std::bitset<8>(1 << (head % 8)) << "= " << ((*bit) & (1 << (head % 8))) << std::endl;
                    if ((*bit) & (1 << (head % 8)))
                    {
                        unsigned long buf[number_of_longs];
                        char char_buf[sizeof(unsigned long) * number_of_longs];
                        spill.read(char_buf, sizeof(unsigned long) * number_of_longs);
                        std::memcpy(buf, &char_buf, sizeof(unsigned long) * number_of_longs);
                        if (!spill)
                        {
                            // std::cout << "breaking" << std::endl;
                            break;
                        }

                        // static_cast<unsigned long *>(static_cast<void *>(buf));
                        //  std::cout << buf[0] << ", " << buf[1] << std::endl;
                        for (int k = 0; k < key_number; k++)
                        {
                            keys[k] = buf[k];
                        }

                        for (int k = 0; k < value_number; k++)
                        {
                            values[k] = buf[k + key_number];
                        }
                        if (hmap->contains(keys))
                        {
                            read_lines++;

                            std::array<unsigned long, max_size> temp = (*hmap)[keys];

                            for (int k = 0; k < value_number; k++)
                            {
                                temp[k] += values[k];
                            }
                            (*hmap)[keys] = temp;

                            *bit &= ~(0x01 << (head % 8));
                        }
                        else if (!locked)
                        {
                            read_lines++;
                            // std::cout << "Setting " << std::bitset<8>(bitmap[std::floor(head / 8)]) << " xth: " << head % 8 << std::endl;
                            hmap->insert(std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>>(keys, values));
                            if (hmap->size() > comb_hash_size.load())
                            {
                                comb_hash_size.fetch_add(1);
                                /* if (comb_hash_size.load() % 100 == 0)
                                {
                                    *avg = (getPhyValue() - base_size) / comb_hash_size.load();
                                } */
                            }
                            *bit &= ~(0x01 << (head % 8));
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
                                    // std::cout << "Free: " << input_head << " - " << freed_space_temp / sizeof(unsigned long) + input_head << std::endl;
                                    freed_mem += freed_space_temp;
                                    // Update Head to point at the new unfreed mapping space.
                                    lower_index += freed_space_temp;
                                }
                                if (freed_space_temp < pagesize * 2)
                                {
                                    if (!locked)
                                    {
                                        locked = true;
                                        s3spillFile_head = i;
                                        s3spillStart_head = head;
                                        bit_head = bit_i;
                                    }
                                    if (firsts3File)
                                    {
                                        std::cout << "Breaking because first file: " << s3spillFile_head << ", " << s3spillStart_head << ", " << bit_head << std::endl;
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            if (hmap->size() * (*avg) + base_size >= memLimit * 0.9)
                            {

                                if (!locked)
                                {
                                    std::cout << "Calc size: " << hmap->size() * (*avg) + base_size << " base_size: " << base_size << " hmap length " << hmap->size() << " memlimit: " << memLimit << std::endl;
                                    locked = true;
                                    s3spillFile_head = i;
                                    s3spillStart_head = head;
                                    bit_head = bit_i;
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
                        spill.ignore(sizeof(unsigned long) * number_of_longs);
                        if (!spill)
                        {
                            // std::cout << "breaking" << std::endl;
                            break;
                        }
                    }
                    head++;
                }
                if (spilled_bitmap)
                {
                    if (munmap(&bitmap_mapping[lower_index], std::ceil((float)(get<1>(*set_it)) / 8) - lower_index) == -1)
                    {
                        std::cout << std::ceil((float)(get<1>(*set_it)) / 8) - lower_index << " lower_index: " << lower_index << std::endl;
                        perror("Could not free memory of bitmap 2!");
                    }
                }
                bit_i++;
                if (firsts3File)
                {
                    bit_i += get<2>(*set_it).size() - sub_file_counter;
                    break;
                }
            }
            i++;
        }

        // std::cout << "New round" << std::endl;

        // Go through entire mapping
        for (unsigned long i = input_head_base; i < comb_spill_size / sizeof(unsigned long); i++)
        {
            if (i >= sum / sizeof(unsigned long))
            {
                // std::cout << "New mapping" << std::endl;
                sum = 0;
                for (auto &it : *spills)
                {
                    sum += it.second;
                    if (i < sum / sizeof(unsigned long))
                    {
                        if (spill_map != nullptr && mapping_size - input_head * sizeof(unsigned long) > 0)
                        {
                            // save empty flag and release the mapping
                            if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(unsigned long)) == -1)
                            {
                                std::cout << "invalid size: " << mapping_size - input_head * sizeof(unsigned long) << std::endl;
                                perror("Could not free memory in merge 2_1!");
                            }
                            freed_mem += mapping_size - input_head * sizeof(unsigned long);
                            // std::cout << "Free: " << input_head << " - " << mapping_size / sizeof(unsigned long) << std::endl;
                        }
                        unsigned long map_start = i * sizeof(unsigned long) - (sum - it.second) - ((i * sizeof(unsigned long) - (sum - it.second)) % pagesize);
                        mapping_size = it.second - map_start;
                        // std::cout << " map_start: " << map_start << std::endl;
                        spill_map = static_cast<unsigned long *>(mmap(nullptr, mapping_size, PROT_WRITE | PROT_READ, MAP_SHARED, it.first, map_start));
                        overall_size += mapping_size;
                        if (spill_map == MAP_FAILED)
                        {
                            close(it.first);
                            perror("Error mmapping the file");
                            exit(EXIT_FAILURE);
                        }
                        madvise(spill_map, mapping_size, MADV_SEQUENTIAL | MADV_WILLNEED);
                        input_head = 0;
                        offset = ((sum - it.second) + map_start) / sizeof(unsigned long);

                        // std::cout << "sum: " << sum / sizeof(unsigned long) << " offset: " << offset << " head: " << input_head_base << " map_start: " << map_start / sizeof(unsigned long) << " i: " << i << std::endl;
                        break;
                    }
                }
            }
            newi = i - offset;
            unsigned long ognewi = newi;
            diff->exchange((newi - input_head) * sizeof(unsigned long));

            if (spill_map[newi] == ULONG_MAX)
            {
                i += key_number;
                i += value_number - 1;
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
                newi--;
                i = newi + offset;

                // std::cout << "merging/adding" << std::endl;
                //  Update count if customerkey is in hashmap and delete pair in spill
                if (hmap->contains(keys))
                {
                    read_lines++;

                    std::array<unsigned long, max_size> temp = (*hmap)[keys];

                    for (int k = 0; k < value_number; k++)
                    {
                        temp[k] += values[k];
                    }
                    (*hmap)[keys] = temp;
                    // mergeHashEntries(&emHashmap[keys], &values);
                    //    delete pair in spill
                    spill_map[ognewi] = ULONG_MAX;
                }
                else if (!locked)
                {
                    read_lines++;
                    hmap->insert(std::pair<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>>(keys, values));
                    if (hmap->size() > comb_hash_size.load())
                    {
                        comb_hash_size.fetch_add(1);
                    }
                    // delete pair in spill
                    spill_map[ognewi] = ULONG_MAX;
                }
            }

            // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
            if (hmap->size() * (*avg) + base_size >= memLimit * 0.9)
            {
                unsigned long used_space = (newi - input_head) * sizeof(unsigned long);
                if (used_space > pagesize)
                {
                    //  calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                    unsigned long freed_space_temp = used_space - (used_space % pagesize);
                    if (munmap(&spill_map[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                    // std::cout << "Free: " << input_head << " - " << freed_space_temp / sizeof(unsigned long) + input_head << std::endl;
                    freed_mem += freed_space_temp;
                    // Update Head to point at the new unfreed mapping space.
                    input_head += freed_space_temp / sizeof(unsigned long);
                    // std::cout << input_head << std::endl;
                    //  Update numHashRows so that the estimations are still correct.

                    // std::cout << "hashmap size: " << emHashmap.size() * avg << " freed space: " << freed_space_temp << std::endl;
                }
                if (!locked && used_space <= pagesize * 40)
                {
                    // std::cout << "head base: " << input_head_base << std::endl;
                    locked = true;
                    input_head_base = i + 1;
                }
            }
        }
        // std::cout << "Writing hashmap size: " << emHashmap.size() << std::endl;

        //  save empty flag and release the mapping
        if (mapping_size - input_head * sizeof(unsigned long) > 0)
        {
            if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(unsigned long)) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
        }
        // std::cout << "Free: " << input_head << " - " << mapping_size / sizeof(unsigned long) << std::endl;
        //  std::cout << "Last head: " << input_head << " should be: " << (mapping_size - (mapping_size - input_head * sizeof(unsigned long))) / sizeof(unsigned long) << std::endl;
        freed_mem += mapping_size - input_head * sizeof(unsigned long);

        // std::cout << "Writing hmap with size: " << hmap->size() << std::endl;
        //  write merged hashmap to the result and update head to point at the end of the file
        if (writeRes)
        {
            written_lines += hmap->size();
            unsigned long finished_rows = 0;
            int counter = 0;
            for (auto &name : *s3spillNames2)
            {
                if (counter >= s3spillFile_head)
                {
                    break;
                }
                finished_rows += get<1>(name);
                counter++;
            }
            finished_rows += s3spillStart_head * number_of_longs * sizeof(unsigned long);
            printProgressBar((finished_rows + input_head_base * sizeof(unsigned long)) / (float)(overall_s3spillsize + comb_spill_size));
            // std::cout << "Writing hmap with size: " << hmap->size() << " s3spillFile_head: " << s3spillFile_head << " s3spillStart_head: " << s3spillStart_head << " avg " << *avg << " base_size: " << base_size << std::endl;
            output_head += writeHashmap(hmap, output_fd, output_head, pagesize * 30);
            /*
                        if (hmap->size() > maxHashsize)
                        {
                            maxHashsize = hmap->size();
                        } */
            hmap->clear();
            // comb_hash_size = maxHashsize;
        }
        else if (locked)
        {
            std::cout << "memLimit reached without finishing merge!";
            return 0;
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

    if (writeRes)
    {
        for (auto &it : *s3spillNames2)
        {
            for (int k = 0; k < get<2>(it).size(); k++)
            {
                Aws::S3::Model::DeleteObjectRequest request;
                request.WithKey(get<0>(it) + "_" + std::to_string(k)).WithBucket(bucketName);
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

    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << " Read lines: " << read_lines << ". macroseconds/line: " << duration * 1000000 / read_lines << std::endl;
    return 1;
}

void spillHelpMerge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::string uName, std::string &local_spillName, Aws::S3::S3Client *minio_client, char beggarWorker, std::string filename, std::string old_uName)
{
    // std::cout << "spilling to: " << uName << std::endl;
    std::string empty = "";
    // spillToMinio(hmap, std::ref(temp_spill_file_name), std::ref(uName), pagesize * 20, &minio_client, worker_id, 0, id);

    if (!spillToMinio(hmap, local_spillName, uName, minio_client, beggarWorker, worker_id, 1))
    {
        return;
    }
    std::cout << "spilled" << std::endl;
    manaFile mana = getLockedMana(minio_client, 1);
    for (auto &worker : mana.workers)
    {
        if (worker.id == beggarWorker && !worker.locked)
        {
            for (auto &w_file : worker.files)
            {
                if (std::get<0>(w_file) == filename)
                {
                    std::get<4>(w_file) = 255;
                }
                if (std::get<0>(w_file) == old_uName)
                {
                    std::get<4>(w_file) = 255;
                }
            }
            break;
        }
    }
    writeMana(minio_client, mana, true);
}

void helpMergePhase(size_t memLimit, size_t memMainLimit, Aws::S3::S3Client minio_client, bool init, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap,
                    std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> &diff, float avg = 1, unsigned long extra_mem = 0)
{
    int finished = 0;
    std::thread sizePrinter;

    std::vector<std::pair<int, size_t>> spills = std::vector<std::pair<int, size_t>>();
    std::vector<std::string> blacklist;

    char beggarWorker = 0;
    std::string uName = "merge";
    int counter = 0;
    std::pair<std::tuple<std::string, size_t, std::vector<size_t>>, char> file;
    std::vector<std::pair<std::tuple<std::string, size_t, std::vector<size_t>>, char>> files;
    std::thread minioSpiller;
    std::string first_fileName;
    std::string local_spillName = "helpMergeSpill";

    if (init)
    {
        comb_hash_size.exchange(0);
        diff.exchange(0);
        *hmap = emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)>();
        log_size = false;
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit, 1, std::ref(comb_hash_size), &diff, &avg, &extra_mem);
    }

    while (true)
    {
        while (true)
        {
            getMergeFileName(hmap, &minio_client, beggarWorker, memLimit, &avg, &blacklist, &file, 0);
            if (file.second == 0)
            {
                break;
            }
            files.push_back(file);
            beggarWorker = file.second;
        }
        if (files.empty())
        {
            if (beggarWorker != 0)
            {
                // Try to change beggar worker or load in new files
                hmap->clear();
                manaFile mana = getLockedMana(&minio_client, 0);
                for (auto &worker : mana.workers)
                {
                    if (worker.id == beggarWorker && !worker.locked)
                    {
                        for (auto &w_file : worker.files)
                        {
                            if (std::get<0>(w_file) == uName)
                            {
                                std::get<4>(w_file) = 0;
                            }
                        }
                        break;
                    }
                }
                writeMana(&minio_client, mana, true);
                beggarWorker = 0;
                while (true)
                {
                    getMergeFileName(hmap, &minio_client, beggarWorker, memLimit, &avg, &blacklist, &file, 0);
                    if (file.second == 0)
                    {
                        break;
                    }
                    files.push_back(file);
                    beggarWorker = file.second;
                }
                std::cout << "beggar: " << file.second << std::endl;
                if (files.empty())
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
        std::vector<std::pair<int, size_t>> empty;
        std::string empty_string = "";
        std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond> spills;
        for (auto &it : files)
        {
            spills.insert(it.first);
        }
        // merge(&emHashmap, &spills, comb_hash_size, &avg, memLimit, &diff, outputfilename, files, &minio_client, true);
        std::cout << "found beggar: " << beggarWorker << std::endl;
        if (!merge(hmap, &empty, comb_hash_size, &avg, memLimit, &diff, empty_string, &spills, &minio_client, &extra_mem, false))
        {
            // blacklist.push_back(file.first.first);
            hmap->clear();
            manaFile mana = getLockedMana(&minio_client, 0);
            for (auto &worker : mana.workers)
            {
                if (worker.id == beggarWorker && !worker.locked)
                {
                    for (auto &w_file : worker.files)
                    {
                        if (std::get<0>(w_file) == uName)
                        {
                            std::get<4>(w_file) = 0;
                        }
                    }
                    break;
                }
            }
            writeMana(&minio_client, mana, true);
            beggarWorker = 0;
            continue;
        }
        std::cout << "merged hmap size: " << hmap->size() << std::endl;
        size_t phy = getPhyValue() * 1024;

        if (counter > 0)
        {
            if (memMainLimit > hmap->size() * sizeof(unsigned long) * (key_number + value_number))
            {
                if (uName != "merge")
                {
                    blacklist.push_back(uName);
                    minioSpiller.join();
                }
                std::string old_uName = counter == 1 ? first_fileName : uName;
                uName = worker_id;
                uName += "_merge_" + std::to_string(counter);
                std::string empty_file = "";

                std::pair<int, size_t> temp_spill_file = {-1, 0};
                spillToFile(hmap, &temp_spill_file, 0, pagesize * 20, local_spillName);
                minioSpiller = std::thread(spillHelpMerge, hmap, uName, std::ref(local_spillName), &minio_client, beggarWorker, get<0>(file.first), old_uName);
            }
            else
            {
                std::string old_uName = counter == 1 ? first_fileName : uName;
                uName = worker_id;
                uName += "_merge_" + std::to_string(counter);
                if (!spillToMinio(hmap, local_spillName, uName, &minio_client, beggarWorker, worker_id, 1))
                {
                    return;
                }
                std::cout << "spilled" << std::endl;
                manaFile mana = getLockedMana(&minio_client, 1);
                for (auto &worker : mana.workers)
                {
                    if (worker.id == beggarWorker && !worker.locked)
                    {
                        for (auto &w_file : worker.files)
                        {
                            if (std::get<0>(w_file) == get<0>(file.first))
                            {
                                std::get<4>(w_file) = 255;
                            }
                            if (std::get<0>(w_file) == old_uName)
                            {
                                std::get<4>(w_file) = 255;
                            }
                        }
                        break;
                    }
                }
                writeMana(&minio_client, mana, true);
            }
        }
        else
        {
            first_fileName = get<0>(file.first);
        }
        counter++;
    }
    if (uName != "merge")
    {
        minioSpiller.join();
    }
    finished = 2;
    sizePrinter.join();
}

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename, size_t memLimit, bool measure_mem, Aws::S3::S3Client minio_client, size_t memLimitMain)
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
    value_number = 1;
    if (op == average)
    {
        value_number = 2;
    }

    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmaps[threadNumber];
    std::vector<std::pair<int, size_t>> spills = std::vector<std::pair<int, size_t>>();
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
    unsigned long extra_mem = 0;
    if (measure_mem)
    {
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit, threadNumber, std::ref(comb_hash_size), &diff, &avg, &extra_mem);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    char id = 0;

    for (int i = 0; i < threadNumber - 1; i++)
    {
        emHashmaps[i] = {};
        threads.push_back(std::thread(fillHashmap, id, &emHashmaps[i], fd, t1_size * i, t1_size, true, memLimit / threadNumber,
                                      std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitMain / threadNumber, std::ref(comb_spill_size)));
        id++;
    }
    emHashmaps[threadNumber - 1] = {};
    threads.push_back(std::thread(fillHashmap, id, &emHashmaps[threadNumber - 1], fd, t1_size * (threadNumber - 1), t2_size, false, memLimit / threadNumber,
                                  std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff, &minio_client, std::ref(readBytes), memLimitMain / threadNumber, std::ref(comb_spill_size)));

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
        temp_loc_spills += it.second;
    }
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Scanning finished with time: " << duration << "s. Scanned Lines: " << numLines << ". macroseconds/line: " << duration * 1000000 / numLines << " Overall spill: " << comb_spill_size << "B. Spill to Main Memory: " << temp_loc_spills << "B. Spill to S3: " << comb_spill_size - temp_loc_spills << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> spill_threads;
    std::string empty = "";
    std::vector<std::string> uNames;
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmap;
    for (int i = 0; i < threadNumber; i++)
    {
        if ((comb_hash_size + emHashmaps[i].size()) * avg + base_size < memLimit * 0.9 && (emHashmap.size() + emHashmaps[i].size()) * avg + base_size < memLimit * 0.5)
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
        {
            std::string uName = "spill_" + std::to_string(i);
            spill_threads.push_back(std::thread(spillToMinio, &emHashmaps[i], std::ref(empty), uName, &minio_client, worker_id, 0, i));
        }
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
    comb_hash_size.exchange(emHashmap.size());
    // delete[] emHashmaps;
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Merging of hastables finished with time: " << duration << "s." << std::endl;

    if (false)
    {
        start_time = std::chrono::high_resolution_clock::now();
        helpMergePhase(memLimit, memLimitMain, minio_client, false, &emHashmap, comb_hash_size, diff, avg, extra_mem);
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
                    for (auto &f : worker.files)
                    {
                        if (std::get<4>(f) != 0 && std::get<4>(f) != 255)
                        {
                            isWorkedOn = true;
                            break;
                        }
                    }
                }
            }
        }
        duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        std::cout << "Collective Merge ended with time: " << duration << "s." << std::endl;
    }
    start_time = std::chrono::high_resolution_clock::now();
    // calc optimistic new avg to better fit spill files as: 8/avgLineLength * (hash_avg - avg)
    // float avglineLengtth = size / numLines.load();
    // avg = hash_avg + (avg - hash_avg) * (sizeof(int) * 2 / avglineLengtth) + 0.02;
    // avg = avg + (float)(sizeof(int) - avglineLengtth) / 1024;
    // avg *= 8 / avglineLengtth;

    // std::cout << "new avg: " << avg << " hashmap size: " << emHashmap.size() << " hash avg: " << hash_avg << std::endl;

    // Free up rest of mapping of input file and close the file
    close(fd);
    finished++;
    avg = 1;

    // std::cout << "Scanning finished." << std::endl;

    // Open the outputfile to write results
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    start_time = std::chrono::high_resolution_clock::now();
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
            s3spilled = !worker.files.empty();
            break;
        }
    }

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (!spills.empty() || s3spilled)
    {
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
        // printMana(&minio_client);
        auto files = getAllMergeFileNames(&minio_client);
        /* for (auto &name : *files)
        {
            std::cout << std::get<0>(name) << ", ";
        }
        std::cout << std::endl;
        std::cout << files->size() << std::endl; */
        merge(&emHashmap, &spills, comb_hash_size, &avg, memLimit, &diff, outputfilename, files, &minio_client, &extra_mem, true);
        delete files;
    }
    else
    {
        // std::cout << "writing to output file" << std::endl;
        written_lines += emHashmap.size();

        // write hashmap to output file
        writeHashmap(&emHashmap, output_fd, 0, pagesize * 10);
    }
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << std::endl;

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
        i = parseJson(mappedFile, i, coloumns, &lineObjects, size);
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
        i = parseJson(mappedFile, i, coloumns, &lineObjects, size);
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

    if (hashmap2.size() != hashmap.size())
    {
        std::cout << "Files have different number of keys." << " File1: " << hashmap.size() << " File2: " << hashmap2.size() << std::endl;
        return 0;
    }
    bool same = true;
    unsigned long not_contained_keys = 0;
    unsigned long different_values = 0;
    for (auto &it : hashmap)
    {
        if (!hashmap2.contains(it.first))
        {
            not_contained_keys++;
            std::cout << "File 2 does not contain: " << it.first[0] << std::endl;
            same = false;
        }
        if (std::abs(hashmap2[it.first] - it.second) > 0.001)
        {
            different_values++;
            //            std::cout << "File 2 has different value for key: " << it.first[0] << "; File 1: " << it.second << "; File 2: " << hashmap2[it.first] << std::endl;
            same = false;
        }
    }
    for (auto &it : hashmap2)
    {
        if (!hashmap.contains(it.first))
        {
            not_contained_keys++;
            std::cout << "File 1 does not contain: " << it.first[0] << std::endl;
            same = false;
        }
    }
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

void helpMerge(size_t memLimit, size_t memMainLimit, Aws::S3::S3Client minio_client)
{
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> hmap;
    std::vector<std::pair<int, size_t>> spills = std::vector<std::pair<int, size_t>>();
    std::atomic<unsigned long> comb_hash_size = 0;
    std::atomic<unsigned long> diff = 0;
    std::vector<std::string> blacklist;

    float avg = 1;
    int finished = 0;
    std::thread sizePrinter;
    unsigned long extra_mem = 0;
    log_size = false;

    sizePrinter = std::thread(printSize, std::ref(finished), memLimit, 1, std::ref(comb_hash_size), &diff, &avg, &extra_mem);

    char beggarWorker = 0;
    unsigned long phyMemBase = getPhyValue() * 1024;
    std::string uName = "merge";
    int counter = 0;
    std::pair<std::tuple<std::string, size_t, std::vector<size_t>>, char> file;
    std::thread minioSpiller;
    std::string first_fileName;
    std::string local_spillName = "helpMergeSpill";
    std::string empty_string = "";

    while (true)
    {
        getMergeFileName(&hmap, &minio_client, beggarWorker, memLimit, &avg, &blacklist, &file, 0);
        if (file.second == 0)
        {
            if (beggarWorker != 0)
            {
                // Try to change beggar worker or load in new files
                hmap.clear();
                manaFile mana = getLockedMana(&minio_client, 0);
                for (auto &worker : mana.workers)
                {
                    if (worker.id == beggarWorker && !worker.locked)
                    {
                        for (auto &w_file : worker.files)
                        {
                            if (std::get<0>(w_file) == uName)
                            {
                                std::get<4>(w_file) = 0;
                            }
                        }
                        break;
                    }
                }
                writeMana(&minio_client, mana, true);
                beggarWorker = 0;
                getMergeFileName(&hmap, &minio_client, beggarWorker, memLimit, &avg, &blacklist, &file, 0);
                std::cout << "beggar: " << file.second << std::endl;
                if (file.second == 0)
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
        beggarWorker = file.second;
        std::vector<std::pair<int, size_t>> empty = {};

        std::set<std::tuple<std::string, size_t, std::vector<size_t>>, CompareBySecond> spills;
        spills.insert(file.first);
        // merge(&emHashmap, &spills, comb_hash_size, &avg, memLimit, &diff, outputfilename, files, &minio_client, true);
        std::cout << "found beggar: " << beggarWorker << std::endl;
        if (!merge(&hmap, &empty, comb_hash_size, &avg, memLimit, &diff, empty_string, &spills, &minio_client, &extra_mem, false))
        {
            // blacklist.push_back(file.first.first);
            hmap.clear();
            manaFile mana = getLockedMana(&minio_client, 0);
            for (auto &worker : mana.workers)
            {
                if (worker.id == beggarWorker && !worker.locked)
                {
                    for (auto &w_file : worker.files)
                    {
                        if (std::get<0>(w_file) == uName)
                        {
                            std::get<4>(w_file) = 0;
                        }
                    }
                    break;
                }
            }
            writeMana(&minio_client, mana, true);
            beggarWorker = 0;
            continue;
        }
        std::cout << "merged hmap size: " << hmap.size() << std::endl;
        size_t phy = getPhyValue() * 1024;

        if (counter > 0)
        {
            avg = (getPhyValue() * 1024 - base_size) / hmap.size();
            if (memMainLimit > hmap.size() * sizeof(unsigned long) * (key_number + value_number))
            {
                if (uName != "merge")
                {
                    blacklist.push_back(uName);
                    minioSpiller.join();
                }
                std::string old_uName = counter == 1 ? first_fileName : uName;
                uName = worker_id;
                uName += "_merge_" + std::to_string(counter);
                std::string empty_file = "";

                std::pair<int, size_t> temp_spill_file = {-1, 0};
                spillToFile(&hmap, &temp_spill_file, 0, pagesize * 20, local_spillName);
                minioSpiller = std::thread(spillHelpMerge, &hmap, uName, std::ref(local_spillName), &minio_client, beggarWorker, get<0>(file.first), old_uName);
            }
            else
            {
                std::string old_uName = counter == 1 ? first_fileName : uName;
                uName = worker_id;
                uName += "_merge_" + std::to_string(counter);
                if (!spillToMinio(&hmap, empty_string, uName, &minio_client, beggarWorker, worker_id, 1))
                {
                    return;
                }
                std::cout << "spilled" << std::endl;
                manaFile mana = getLockedMana(&minio_client, 1);
                for (auto &worker : mana.workers)
                {
                    if (worker.id == beggarWorker && !worker.locked)
                    {
                        for (auto &w_file : worker.files)
                        {
                            if (std::get<0>(w_file) == get<0>(file.first))
                            {
                                std::get<4>(w_file) = 255;
                            }
                            if (std::get<0>(w_file) == old_uName)
                            {
                                std::get<4>(w_file) = 255;
                            }
                        }
                        break;
                    }
                }
                writeMana(&minio_client, mana, true);
            }
        }
        else
        {
            first_fileName = get<0>(file.first);
        }
        counter++;
    }
    if (uName != "merge")
    {
        minioSpiller.join();
    }
    finished = 2;
    sizePrinter.join();
}

int main(int argc, char **argv)
{
    Aws::SDKOptions options;
    // options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    Aws::InitAPI(options);

    std::string co_output = argv[1];

    if (co_output.compare("status") == 0)
    {
        Aws::S3::S3Client minio_client_2 = init();
        printMana(&minio_client_2);
        Aws::ShutdownAPI(options);
        return 1;
    }

    std::string tpc_sup = argv[2];
    std::string memLimit_string = argv[3];
    std::string memLimitMain_string = argv[4];
    std::string threadNumber_string = argv[5];
    std::string tpc_query_string = argv[6];
    worker_id = *argv[7];
    std::string log_size_string = argv[8];
    std::string log_time_string = argv[9];

    log_size = log_size_string.compare("true") == 0;
    log_time = log_time_string.compare("true") == 0;

    threadNumber = std::stoi(threadNumber_string);
    int tpc_query = std::stoi(tpc_query_string);
    size_t memLimit = (std::stof(memLimit_string) - 0.01) * (1ul << 30);

    // memLimit -= 1ull << 20;
    size_t memLimitMain = std::stof(memLimitMain_string) * (1ul << 30);
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
    max_s3_spill_size = (1ul << 20) - ((1ul << 20) % (key_number + value_number) * sizeof(unsigned long)); // memLimit / 8;
    std::string agg_output = "output_" + tpc_sup;
    Aws::S3::S3Client minio_client = init();
    initManagFile(&minio_client);
    start_time = std::chrono::high_resolution_clock::now();
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    // Visit http://en.cppreference.com/w/cpp/chrono/c/strftime
    // for more information about date/time format
    strftime(buf, sizeof(buf), "%m-%d_%H-%M", &tstruct);
    date_now = buf;

    if (co_output != "-")
    {
        aggregate(co_output, agg_output, memLimit, true, minio_client, memLimitMain);
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start_time).count()) / 1000000;
        std::cout << "Aggregation finished. With time: " << duration << "s. Checking results." << std::endl;
    }
    if (tpc_sup != "-")
    {
        test(agg_output, tpc_sup);
    }
    helpMerge(memLimit, memLimitMain, minio_client);
    Aws::ShutdownAPI(options);
    std::cout << "Finished!" << std::endl;
    return 1;
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");S */
}