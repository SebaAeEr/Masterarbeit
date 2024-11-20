#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <iostream>
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
#include <bitset>
#include <cmath>

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
    std::vector<std::tuple<std::string, size_t, char>> files;
};

struct manaFile
{
    int version;
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
    bool operator()(const std::pair<std::string, size_t> &a, const std::pair<std::string, size_t> &b) const
    {
        return a.second > b.second;
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

manaFile getMana(Aws::S3::S3Client *minio_client)
{
    manaFile mana;
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket("trinobucket");
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
            break;
        }
    }
    Aws::S3::Model::PutObjectRequest in_request;
    in_request.SetBucket("trinobucket");
    in_request.SetKey(manag_file_name);
    const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
    auto &out_stream = outcome.GetResult().GetBody();
    size_t out_size = outcome.GetResult().GetContentLength();
    size_t in_mem_size = out_size;

    int version;
    char char_buf[sizeof(int)];
    out_stream.read(char_buf, sizeof(int));
    std::memcpy(&version, &char_buf, sizeof(int));
    mana.version = version;
    mana.workers = {};
    while (out_stream.peek() != EOF)
    {
        manaFileWorker worker;
        char workerid = out_stream.get();
        worker.id = workerid;
        worker.locked = out_stream.get() == 1;
        std::vector<std::tuple<std::string, size_t, char>> files = {};
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
            size_t file_length;
            char length_buf[sizeof(size_t)];
            out_stream.read(length_buf, sizeof(size_t));
            std::memcpy(&file_length, &length_buf, sizeof(size_t));
            char m_worker = out_stream.get();
            files.push_back({filename, file_length, m_worker});
            head += filename.size() + 2 + sizeof(size_t);
        }
        worker.files = files;
        mana.workers.push_back(worker);
    }
    return mana;
}

int getManagVersion(Aws::S3::S3Client *minio_client)
{
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket("trinobucket");
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
            Aws::S3::Model::PutObjectRequest in_request;
            in_request.SetBucket("trinobucket");
            in_request.SetKey(manag_file_name);
            const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
            auto &out_stream = outcome.GetResult().GetBody();
            size_t out_size = outcome.GetResult().GetContentLength();
            size_t in_mem_size = out_size;

            int version;
            char char_buf[sizeof(int)];
            out_stream.read(char_buf, sizeof(int));
            std::memcpy(&version, &char_buf, sizeof(int));
            return version;
        }
    }
}

bool writeMana(Aws::S3::S3Client *minio_client, manaFile mana, bool checkVersion)
{
    while (true)
    {
        Aws::S3::Model::PutObjectRequest in_request;
        in_request.SetBucket("trinobucket");
        in_request.SetKey(manag_file_name);
        const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");
        size_t in_mem_size = sizeof(int);

        char version_buf[sizeof(int)];
        std::memcpy(version_buf, &mana.version, sizeof(int));
        for (int i = 0; i < sizeof(int); i++)
        {
            *in_stream << version_buf[i];
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
                char file_length_buf[sizeof(size_t)];
                std::memcpy(file_length_buf, &get<1>(file), sizeof(size_t));
                for (int i = 0; i < sizeof(size_t); i++)
                {
                    *in_stream << file_length_buf[i];
                }
                *in_stream << get<2>(file);
            }
        }

        in_request.SetBody(in_stream);
        in_request.SetContentLength(in_mem_size);
        if (checkVersion)
        {
            int newVersion = getManagVersion(minio_client);

            if (newVersion != mana.version - 1)
            {
                std::cout << "Old Version: " << mana.version << ", new Version: " << newVersion << std::endl;
                return 0;
            }
        }
        while (true)
        {
            auto in_outcome = minio_client->PutObject(in_request);
            if (!in_outcome.IsSuccess())
            {
                std::cout << "Error: " << in_outcome.GetError().GetMessage() << " size: " << in_mem_size << std::endl;
            }
            else
            {
                return 1;
            }
        }
    }
}

void initManagFile(Aws::S3::S3Client *minio_client)
{
    manaFile mana;
    if (worker_id == '1')
    {
        mana.version = 0;
    }
    else
    {
        mana = getMana(minio_client);
    }
    manaFileWorker worker;
    worker.id = worker_id;
    worker.length = 0;
    worker.files = {};
    worker.locked = false;
    mana.workers.push_back(worker);
    writeMana(minio_client, mana, false);
}

Aws::S3::S3Client init()
{
    Aws::Client::ClientConfiguration c_config;
    c_config.verifySSL = false;
    c_config.region = "us-west-1";
    c_config.scheme = Aws::Http::Scheme::HTTP;
    c_config.endpointOverride = "131.159.16.208:9000";
    Aws::Auth::AWSCredentials cred("erasmus", "tumThesis123");
    Aws::S3::S3Client minio_client = Aws::S3::S3Client(cred, c_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    initManagFile(&minio_client);
    return minio_client;
}

void printMana(Aws::S3::S3Client *minio_client)
{
    manaFile mana = getMana(minio_client);
    std::cout << "version: " << mana.version << std::endl;
    for (auto &worker : mana.workers)
    {
        std::cout << "Worker id: " << worker.id << " locked: " << worker.locked << std::endl;
        for (auto &file : worker.files)
        {
            std::cout << "  " << std::get<0>(file) << " size: " << std::get<1>(file) << " worked on by: " << std::get<2>(file) << std::endl;
        }
    }
}

int addFileToManag(Aws::S3::S3Client *minio_client, std::string *file_name, size_t file_size, char write_to_id, char fileStatus)
{
    while (true)
    {
        manaFile mana = getMana(minio_client);
        for (auto &worker : mana.workers)
        {
            if (worker.id == write_to_id)
            {
                if (worker.locked)
                {
                    return 0;
                }
                worker.files.push_back({*file_name, file_size, fileStatus});
                worker.length += file_name->size() + 2 + sizeof(size_t);
                break;
            }
        }
        mana.version++;
        if (writeMana(minio_client, mana, true))
        {
            break;
        }
    }
    return 1;
}

std::set<std::pair<std::string, size_t>, CompareBySecond> *getAllMergeFileNames(Aws::S3::S3Client *minio_client)
{
    std::set<std::pair<std::string, size_t>, CompareBySecond> *files = new std::set<std::pair<std::string, size_t>, CompareBySecond>();
    manaFile mana = getMana(minio_client);
    for (auto &worker : mana.workers)
    {
        if (worker.id == worker_id)
        {
            for (auto &file : worker.files)
            {
                if (get<2>(file) != 255)
                {
                    files->insert({get<0>(file), get<1>(file)});
                }
            }
        }
    }
    return files;
}

std::pair<std::pair<std::string, size_t>, char> *getMergeFileName(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, Aws::S3::S3Client *minio_client,
                                                                  char beggarWorker, size_t memLimit, float *avg)
{
    std::pair<std::pair<std::string, size_t>, char> *res = new std::pair<std::pair<std::string, size_t>, char>();
    *res = {{"", 0}, 0};
    char given_beggarWorker = beggarWorker;
    while (true)
    {
        std::pair<std::string, size_t> m_file = {};
        manaFile mana = getMana(minio_client);
        // If no beggarWorker is yet selected choose the worker with the largest spill
        if (beggarWorker == 0)
        {
            size_t max = 0;
            for (auto &worker : mana.workers)
            {
                if (!worker.locked)
                {
                    size_t size_temp = 0;
                    for (auto &file : worker.files)
                    {
                        size_temp += get<1>(file);
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
            return res;
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
                    size_t size_temp = get<1>(file);
                    if (size_temp > max && (size_temp / (sizeof(unsigned long) * (key_number + value_number)) + hmap->size()) * (*avg) < memLimit)
                    {
                        max = size_temp;
                        m_file = {get<0>(file), size_temp};
                    }
                }
                break;
            }
        }
        for (auto &worker : mana.workers)
        {
            if (worker.id == beggarWorker)
            {
                for (auto &file : worker.files)
                {
                    if (get<0>(file) == m_file.first)
                    {
                        get<0>(file) = worker_id;
                        break;
                    }
                }
            }
        }
        mana.version++;
        if (writeMana(minio_client, mana, true))
        {
            *res = {m_file, beggarWorker};
            return res;
        }
        else
        {
            beggarWorker = given_beggarWorker;
        }
    }
    return res;
}

// Write hashmap hmap into file with head on start.
int writeHashmap(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, unsigned long start, unsigned long free_mem)
{
    // Calc the output size for hmap.
    unsigned long output_size = 0;
    for (auto &it : *hmap)
    {
        for (auto &itt : it.first)
        {
            output_size += std::to_string(itt).length();
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
        perror("Error mmapping the file");
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

void spillToFile(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::pair<int, size_t> *spill_file, int id, size_t free_mem)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(unsigned long) * (key_number + value_number);

    if (spill_file->first == -1)
    {
        std::string name = "spill_file_" + std::to_string(id);
        spill_file->first = open(name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
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
    hmap->clear();
    munmap(&spill[writehead], (spill_mem_size + start_diff) - writehead * sizeof(unsigned long));
    freed_mem += (spill_mem_size + start_diff) - writehead * sizeof(unsigned long);
    spill_file->second += spill_mem_size;
    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}

int spillToMinio(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::string *uniqueName,
                 size_t free_mem, Aws::S3::S3Client *minio_client, char write_to_id, char fileStatus)
{
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket("trinobucket");
    request.SetKey(uniqueName->c_str());
    const std::shared_ptr<Aws::IOStream> in_stream = Aws::MakeShared<Aws::StringStream>("");

    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(unsigned long) * (key_number + value_number);

    // Write int to Mapping
    for (auto &it : *hmap)
    {
        char byteArray[sizeof(long int)];
        for (int i = 0; i < key_number; i++)
        {
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
    request.SetContentLength(spill_mem_size);
    while (true)
    {
        auto outcome = minio_client->PutObject(request);
        if (!outcome.IsSuccess())
        {
            std::cout << "Error: " << outcome.GetError().GetMessage() << " Spill size: " << spill_mem_size << std::endl;
        }
        else
        {
            break;
        }
    }
    hmap->clear();
    return addFileToManag(minio_client, uniqueName, spill_mem_size, write_to_id, fileStatus);
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

void fillHashmap(int id, emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, int file, size_t start, size_t size, bool addOffset, size_t memLimit, int phyMembase,
                 float &avg, std::vector<std::pair<int, size_t>> *spill_files, std::atomic<unsigned long> &numLines, std::atomic<unsigned long> &comb_hash_size, unsigned long *shared_diff, Aws::S3::S3Client *minio_client, std::vector<std::string> *s3Spill_names)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    auto start_time = std::chrono::high_resolution_clock::now();
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
    std::array<unsigned long, max_size> keys;
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
    std::pair<int, size_t> spill_file(-1, 0);

    // loop through entire mapping
    for (unsigned long i = 0; i < size + offset; i++)
    {
        i = parseJson(mappedFile, i, coloumns, &lineObjects, size);

        *shared_diff = i - head;
        if (i == ULONG_MAX)
        {
            break;
        }
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
        if (hmap->size() * avg + (i - head + 1) >= memLimit * 0.8)
        {
            // std::cout << "memLimit broken. Estimated mem used: " << hmap->size() * avg + (i - head + 1) << " size: " << hmap->size() << " avg: " << avg << " diff: " << i - head << std::endl;
            unsigned long freed_space_temp = (i - head + 1) - ((i - head + 1) % pagesize);
            // Free up space from mapping that is no longer needed.
            if (i - head + 1 > pagesize)
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
            }

            // compare estimation again to memLimit
            if (freed_space_temp <= pagesize * 10)
            {
                // std::cout << "spilling with size: " << hmap->size() << " i-head: " << (i - head + 1) << " size: " << getPhyValue() << std::endl;
                //    Reset freed_space and update numHashRows so that Estimation stay correct
                if (maxHmapSize < hmap->size())
                {
                    maxHmapSize = hmap->size();
                    // std::cout << "new MaxSize: " << maxHmapSize << std::endl;
                }
                spill_size += hmap->size() * (key_number + value_number) * sizeof(unsigned long);
                // comb_hash_size -= hmap->size();
                // spillToFile(hmap, &spill_file, id, pagesize, pagesize * 20);
                // std::cout << "Spilling" << std::endl;
                std::string uName = worker_id + "_" + std::to_string(id) + "_" + std::to_string(spill_number);
                if (!spillToMinio(hmap, &uName, pagesize * 20, minio_client, worker_id, 0))
                {
                    std::cout << "Spilling to Minio failed because worker is locked!" << std::endl;
                }
                (*s3Spill_names).push_back(uName);
                spill_number++;

                // std::cout << "Spilling ended" << std::endl;
                //  hmap->clear();
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

    if (spill_file.first != -1)
    {
        spill_files->push_back(spill_file);
        // std::cout << "Thread: " << id << " spilled with size: " << spill_file.second << std::endl;
    }
    try
    {
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        std::cout << "Thread " << id << " finished scanning. With time: " << duration << "s. Scanned Lines: " << numLinesLocal << ". microseconds/line: " << duration * 1000000 / numLinesLocal << ". Spilled with size: " << spill_size << std::endl;
    }
    catch (std::exception &err)
    {
        std::cout << "Not able to print time: " << err.what() << std::endl;
    }
}

void printSize(int &finished, float memLimit, int threadNumber, std::atomic<unsigned long> &comb_hash_size, std::vector<unsigned long> diff, float *avg, unsigned long *extra_mem)
{
    int phyMemBase = (getPhyValue()) * 1024;
    bool first = true;
    int small_counter = 0;
    // int counter = 0;
    size_t maxSize = 0;
    size_t old_size = 0;
    size_t size = 0;
    while (finished == 0 || finished == 1)
    {
        size_t newsize = getPhyValue() * 1024;
        while (abs(static_cast<long>(size - newsize)) > 5000000000)
        {
            newsize = getPhyValue() * 1024;
        }
        size = newsize;
        // std::cout << "phy: " << size << std::endl;

        if (size > old_size)
        {
            if (size > maxSize)
            {
                maxSize = size;
                // std::cout << "phy: " << size << std::endl;
            }
            if (memLimit * 0.95 < size)
            {
                unsigned long reservedMem = 0;
                for (int i = 0; i < diff.size(); i++)
                {
                    reservedMem += diff[i];
                }
                *avg = (size - phyMemBase - reservedMem - (*extra_mem)) / (float)(comb_hash_size.load());
                *avg *= 1.2;
                // std::cout << "phy: " << size << " phymemBase: " << phyMemBase << " hash_avg: " << *avg << std::endl;
                sleep(0.5);
            }
        }
        old_size = size;
        sleep(0.1);
    }
    std::cout << "Max Size: " << maxSize << "B." << std::endl;
}

int merge(emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> *hmap, std::vector<std::pair<int, size_t>> *spills, std::atomic<unsigned long> &comb_hash_size,
          float *avg, float memLimit, std::vector<unsigned long> *diff, std::string &outputfilename, std::set<std::pair<std::string, size_t>, CompareBySecond> *s3spillNames2, Aws::S3::S3Client *minio_client, unsigned long *extra_mem, bool writeRes)
{
    // Open the outputfile to write results
    int output_fd;
    if (writeRes)
    {
        output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    }
    auto start_time = std::chrono::high_resolution_clock::now();
    diff->clear();
    diff->push_back(0);
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
    unsigned long maxHashsize = hmap->size();
    comb_hash_size = hmap->size();
    std::array<unsigned long, max_size> keys = {0, 0};
    std::array<unsigned long, max_size> values = {0, 0};
    size_t mapping_size = 0;
    unsigned long bitmap_size_sum = 0;
    std::vector<size_t> bitmap_sizes;
    std::vector<std::pair<int, std::vector<char>>> s3spillBitmaps;
    int s3spillFile_head = 0;
    unsigned long s3spillStart_head = 0;

    for (auto &it : *spills)
        comb_spill_size += it.second;

    int counter = 0;
    for (auto &name : *s3spillNames2)
    {
        bitmap_size_sum += std::ceil((float)(name.second) / 8);
    }
    if (bitmap_size_sum > memLimit * 0.3)
    {
        std::cout << "Spilling bitmaps with size: " << bitmap_size_sum << std::endl;
        int counter = 0;
        for (auto &name : *s3spillNames2)
        {
            size_t size = std::ceil((float)(name.second) / 8);
            // std::cout << "writing bitmap" << (*s3spillNames)[counter] << "_bitmap" << " with size: " << size << std::endl;
            //  Spilling bitmaps
            int fd = open((name.first + "_bitmap").c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
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
            for (int i = 0; i < size; i++)
            {
                spill[i] = 255;
            }
            munmap(spill, size);
            s3spillBitmaps.push_back({fd, {}});
            counter++;
        }
        bitmap_size_sum = 0;
    }
    else
    {
        std::cout << "Keeping bitmaps in mem with size: " << bitmap_size_sum << std::endl;
        // not spilling bitmaps
        for (auto &name : *s3spillNames2)
        {
            std::vector<char> bitmap = std::vector<char>(std::ceil((float)(name.second) / 8), 255);
            s3spillBitmaps.push_back({-1, bitmap});
        }
    }
    *extra_mem = bitmap_size_sum;

    // std::cout << "comb_spill_size: " << comb_spill_size << std::endl;

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
            (*diff)[0] = (newi - input_head) * sizeof(unsigned long);

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
                    if (hmap->size() > maxHashsize)
                    {
                        comb_hash_size++;
                    }
                    // delete pair in spill
                    spill_map[ognewi] = ULONG_MAX;
                }
            }

            // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
            if (hmap->size() * (*avg) + (newi - input_head) * sizeof(unsigned long) >= memLimit * 0.7)
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
        written_lines += hmap->size();
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
        // std::cout << "merger: freed_mem: " << freed_mem << " size: " << overall_size << std::endl;
        overall_size = 0;
        freed_mem = 0;
        // std::cout << "write: " << emHashmap[{221877}][0] << std::endl;
        bool firsts3File = false;
        // std::cout << "s3spillFile: " << s3spillFile_head << std::endl;
        // std::cout << "s3spillStart: " << s3spillStart_head << std::endl;

        int number_of_longs = key_number + value_number;
        int i = s3spillFile_head;
        for (auto set_it = std::next(s3spillNames2->begin(), i); set_it != s3spillNames2->end(); set_it++)
        {
            firsts3File = !firsts3File && i == s3spillFile_head;
            // std::cout << "Start reading: " << spillname << std::endl;
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket("trinobucket");
            request.SetKey((*set_it).first);
            Aws::S3::Model::GetObjectOutcome outcome;
            while (true)
            {
                outcome = minio_client->GetObject(request);
                if (!outcome.IsSuccess())
                {
                    std::cout << "GetObject error " << (*set_it).first << " " << outcome.GetError().GetMessage() << std::endl;
                }
                else
                {
                    break;
                }
            }
            // std::cout << "Reading spill: " << (*set_it).first << std::endl;
            auto &spill = outcome.GetResult().GetBody();
            char *bitmap_mapping;
            std::vector<char> *bitmap_vector;
            bool spilled_bitmap = s3spillBitmaps[i].first != -1;
            if (!spilled_bitmap)
            {
                bitmap_vector = &s3spillBitmaps[i].second;
            }
            else
            {
                bitmap_mapping = static_cast<char *>(mmap(nullptr, std::ceil((float)((*set_it).second) / 8), PROT_WRITE | PROT_READ, MAP_SHARED, s3spillBitmaps[i].first, 0));
                if (bitmap_mapping == MAP_FAILED)
                {
                    perror("Error mmapping the file");
                    exit(EXIT_FAILURE);
                }
                madvise(bitmap_mapping, std::ceil((float)((*set_it).second) / 8), MADV_SEQUENTIAL | MADV_WILLNEED);
                // std::cout << "Size: " << std::ceil((float)((*set_it).second) / 8) << " Addr: " << bitmap_mapping << std::endl;
            }
            // std::cout << "Reading spill: " << (*s3spillNames)[i] << " with bitmap of size: " << bitmap_vector->size() << std::endl;
            unsigned long head = 0;
            if (firsts3File)
            {
                head = s3spillStart_head;
                // std::cout << "First File" << std::endl;
                spill.ignore(s3spillStart_head * sizeof(unsigned long) * number_of_longs);
            }
            unsigned long lower_head = 0;
            while (spill.peek() != EOF)
            {
                char *bit;
                size_t index = std::floor(head / 8);
                if (index >= bitmap_vector->size())
                {
                    std::cout << "index too big: " << index << " head: " << head << " bitmap size: " << bitmap_vector->size() << std::endl;
                    break;
                }
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
                        if (hmap->size() > maxHashsize)
                        {
                            comb_hash_size++;
                        }
                        *bit &= ~(0x01 << (head % 8));
                        // std::cout << "After setting " << std::bitset<8>(bitmap[std::floor(head / 8)]) << std::endl;
                    }
                    if (spilled_bitmap)
                    {
                        if (hmap->size() * (*avg) + (head - lower_head) + bitmap_size_sum >= memLimit * 0.7)
                        {
                            // std::cout << "spilling: " << head - lower_head << std::endl;
                            unsigned long freed_space_temp = (head - lower_head) - ((head - lower_head) % pagesize);
                            if (head - lower_head >= pagesize)
                            {
                                if (munmap(&bitmap_mapping[lower_head], freed_space_temp) == -1)
                                {
                                    std::cout << freed_space_temp << std::endl;
                                    perror("Could not free memory of bitmap 1!");
                                }
                                // std::cout << "Free: " << input_head << " - " << freed_space_temp / sizeof(unsigned long) + input_head << std::endl;
                                freed_mem += freed_space_temp;
                                // Update Head to point at the new unfreed mapping space.
                                lower_head += freed_space_temp;
                            }
                            if (freed_space_temp < pagesize * 2)
                            {
                                if (!locked)
                                {
                                    locked = true;
                                    s3spillFile_head = i;
                                    s3spillStart_head = head;
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
                        if (hmap->size() * (*avg) + bitmap_size_sum >= memLimit * 0.7)
                        {
                            if (!locked)
                            {
                                locked = true;
                                s3spillFile_head = i;
                                s3spillStart_head = head;
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
                if (munmap(&bitmap_mapping[lower_head], std::ceil((float)((*set_it).second) / 8) - lower_head) == -1)
                {
                    std::cout << std::ceil((float)((*set_it).second) / 8) - lower_head << " lower_head: " << lower_head << std::endl;
                    perror("Could not free memory of bitmap 2!");
                }
            }
            i++;
        }

        // std::cout << "Writing hmap with size: " << hmap->size() << std::endl;
        //  write merged hashmap to the result and update head to point at the end of the file
        if (writeRes)
        {
            output_head += writeHashmap(hmap, output_fd, output_head, pagesize * 30);

            if (hmap->size() > maxHashsize)
            {
                maxHashsize = hmap->size();
            }
            hmap->clear();
            comb_hash_size = maxHashsize;
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
        remove((it.first + "_bitmap").c_str());
    }

    if (writeRes)
    {
        // for (auto &it : *s3spillNames2)
        // {
        //     Aws::S3::Model::DeleteObjectRequest request;
        //     request.WithKey(it.first).WithBucket("trinobucket");
        //     while (true)
        //     {
        //         auto outcome = minio_client->DeleteObject(request);
        //         if (!outcome.IsSuccess())
        //         {
        //             std::cerr << "Error: deleteObject: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
        //         }
        //         else
        //         {
        //             break;
        //         }
        //     }
        // }
    }
    delete s3spillNames2;

    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << " Read lines: " << read_lines << ". macroseconds/line: " << duration * 1000000 / read_lines << std::endl;
    return 1;
}

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename, size_t memLimit, int threadNumber, bool measure_mem)
{

    // Inits and decls

    Aws::S3::S3Client minio_client = init();

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
    std::atomic<unsigned long> comb_hash_size = 0;
    std::vector<unsigned long> diff;
    for (int i = 0; i < threadNumber; i++)
    {
        diff.push_back(0);
    }

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
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit, threadNumber, std::ref(comb_hash_size), diff, &avg, &extra_mem);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    int phyMemBase = getPhyValue();

    for (int i = 0; i < threadNumber - 1; i++)
    {
        emHashmaps[i] = {};
        threads.push_back(std::thread(fillHashmap, i, &emHashmaps[i], fd, t1_size * i, t1_size, true, (memLimit - phyMemBase) / threadNumber, phyMemBase / threadNumber,
                                      std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff[i], &minio_client, &s3Spill_names));
    }
    emHashmaps[threadNumber - 1] = {};
    threads.push_back(std::thread(fillHashmap, threadNumber - 1, &emHashmaps[threadNumber - 1], fd, t1_size * (threadNumber - 1), t2_size, false, (memLimit - phyMemBase) / threadNumber, phyMemBase / threadNumber,
                                  std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), &diff[threadNumber - 1], &minio_client, &s3Spill_names));

    // calc avg as Phy mem used by hashtable + mapping / hashtable size
    /* avg = (getPhyValue() - phyMemBase) / (float)(comb_hash_size.load());
    avg *= 1.3;
    std::cout << "phy: " << getPhyValue() << " phymemBase: " << phyMemBase << " #Hash entries: " << comb_hash_size.load() << " avg: " << avg << std::endl;
 */
    for (auto &thread : threads)
    {
        thread.join();
    }
    close(fd);
    unsigned long o_spill = 0;
    for (auto &spill : spills)
    {
        o_spill += spill.second;
    }
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Scanning finished with time: " << duration << "s. Scanned Lines: " << numLines << ". macroseconds/line: " << duration * 1000000 / numLines << " Overall spill: " << o_spill << "B." << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> emHashmap = emHashmaps[0];
    for (int i = 1; i < threadNumber; i++)
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
        // delete &emHashmaps[i];
        emHashmaps[i].clear();
    }
    // delete[] emHashmaps;
    duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Merging of hastables finished with time: " << duration << "s." << std::endl;
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
    diff.clear();
    diff.push_back(0);
    comb_hash_size = emHashmap.size();
    unsigned long maxHashsize = comb_hash_size;
    unsigned long freed_mem = 0;
    unsigned long overall_size = 0;

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (!spills.empty() || !s3Spill_names.empty())
    {
        while (true)
        {
            manaFile mana = getMana(&minio_client);
            for (auto &worker : mana.workers)
            {
                if (worker.id == worker_id)
                {
                    worker.locked = true;
                    break;
                }
            }
            mana.version++;
            if (writeMana(&minio_client, mana, true))
            {
                break;
            }
        }
        auto files = getAllMergeFileNames(&minio_client);
        merge(&emHashmap, &spills, comb_hash_size, &avg, memLimit, &diff, outputfilename, files, &minio_client, &extra_mem, true);
    }
    else
    {
        // std::cout << "writing to output file" << std::endl;
        written_lines += emHashmap.size();

        // write hashmap to output file
        writeHashmap(&emHashmap, output_fd, 0, pagesize * 10);
        duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << " Written lines: " << written_lines << ". macroseconds/line: " << duration * 1000000 / written_lines << std::endl;
    }
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
    for (auto &it : hashmap)
    {
        if (!hashmap2.contains(it.first))
        {
            std::cout << "File 2 does not contain: " << it.first[0] << std::endl;
            same = false;
        }
        if (std::abs(hashmap2[it.first] - it.second) > 0.001)
        {
            std::cout << "File 2 has different value for key: " << it.first[0] << "; File 1: " << it.second << "; File 2: " << hashmap2[it.first] << std::endl;
            same = false;
        }
    }
    if (same)
    {
        std::cout << "Files are the Same!" << std::endl;
    }
    return 1;
}

void helpMerge(size_t memLimit)
{
    Aws::S3::S3Client minio_client = init();
    emhash8::HashMap<std::array<unsigned long, max_size>, std::array<unsigned long, max_size>, decltype(hash), decltype(comp)> hmap;
    std::vector<std::pair<int, size_t>> spills = std::vector<std::pair<int, size_t>>();
    std::atomic<unsigned long> comb_hash_size = 0;
    std::vector<unsigned long> diff;

    float avg = 1;
    int finished = 0;
    std::thread sizePrinter;
    unsigned long extra_mem = 0;
    sizePrinter = std::thread(printSize, std::ref(finished), memLimit, 1, std::ref(comb_hash_size), diff, &avg, &extra_mem);

    char beggarWorker = 0;
    unsigned long phyMemBase = getPhyValue() * 1024;
    std::string uName = "merge";
    std::pair<std::pair<std::string, size_t>, char> *file;
    while (true)
    {
        printMana(&minio_client);
        file = getMergeFileName(&hmap, &minio_client, beggarWorker, memLimit, &avg);
        if (file->second == 0)
        {
            if (beggarWorker != 0)
            {
                beggarWorker = 0;
                file = getMergeFileName(&hmap, &minio_client, beggarWorker, memLimit, &avg);
                if (file->second == 0)
                {
                    delete file;
                    break;
                }
            }
            else
            {
                delete file;
                break;
            }
        }
        beggarWorker = file->second;
        std::vector<std::pair<int, size_t>> empty = {};
        std::string empty_string = "";
        std::set<std::pair<std::string, size_t>, CompareBySecond> spills;
        spills.insert(file->first);
        // merge(&emHashmap, &spills, comb_hash_size, &avg, memLimit, &diff, outputfilename, files, &minio_client, true);
        merge(&hmap, &empty, comb_hash_size, &avg, memLimit, &diff, empty_string, &spills, &minio_client, &extra_mem, false);
        size_t phy = getPhyValue() * 1024;
        avg = (phy - phyMemBase) / (float)(hmap.size());
        avg *= 1.2;
        std::string old_uName = uName;
        uName += "_" + file->first.first;
        delete file;
        if (!spillToMinio(&hmap, &uName, memLimit - phy, &minio_client, beggarWorker, 255))
        {
            continue;
        }
        while (true)
        {
            manaFile mana = getMana(&minio_client);
            for (auto &worker : mana.workers)
            {
                if (worker.id == beggarWorker && !worker.locked)
                {
                    for (auto &w_file : worker.files)
                    {
                        if (std::get<0>(w_file) == file->first.first)
                        {
                            std::get<2>(w_file) = 255;
                        }
                        if (std::get<0>(w_file) == uName)
                        {
                            std::get<2>(w_file) = 0;
                        }
                        if (std::get<0>(w_file) == old_uName)
                        {
                            std::get<2>(w_file) = 255;
                        }
                    }
                    break;
                }
            }
            mana.version++;
            if (writeMana(&minio_client, mana, true))
            {
                break;
            }
        }
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
    std::string tpc_sup = argv[2];
    std::string memLimit_string = argv[3];
    std::string threadNumber_string = argv[4];
    std::string tpc_query_string = argv[5];
    worker_id = *argv[6];

    int threadNumber = std::stoi(threadNumber_string);
    int tpc_query = std::stoi(tpc_query_string);
    size_t memLimit = std::stof(memLimit_string) * (1ul << 30);
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

    auto start = std::chrono::high_resolution_clock::now();

    if (co_output != "-")
    {
        aggregate(co_output, agg_output, memLimit, threadNumber, true);
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count()) / 1000000;
        std::cout << "Aggregation finished. With time: " << duration << "s. Checking results." << std::endl;
        if (tpc_sup != "-")
        {
            test(agg_output, tpc_sup);
        }
    }
    helpMerge(memLimit);
    Aws::ShutdownAPI(options);
    return 1;
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");S */
}