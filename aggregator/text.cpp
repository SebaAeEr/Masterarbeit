#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
// #include <json/json.h>
#include <unordered_map>
#include <stdio.h>
#include <chrono>
// #include <ankerl/unordered_dense.h>
#include <hash_table8.hpp>
#include <thread>
#include <mutex>

enum Operation
{
    count,
    sum,
    exists,
    average
};
static const int max_size = 2;
std::string key_names[max_size];
enum Operation op;
std::string opKeyName;
int key_number;
int value_number;

auto hash = [](const std::array<int, max_size> a)
{
    std::size_t h = 0;
    for (int i = 0; i < max_size; i++)
    {
        h ^= std::hash<int>{}(a[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
};
auto comp = [](const std::array<int, max_size> v1, const std::array<int, max_size> v2)
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

// Aggregator(std::string *key_names, enum Operation op, std::string opKeyName, int key_number, int value_number) : key_names(key_names), op(op), opKeyName(opKeyName), key_number(key_number), value_number(value_number) {}

int parseLine(char *line)
{
    // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
    //  This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

int getVirtValue()
{ // Note: this value is in KB!
    FILE *file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL)
    {
        if (strncmp(line, "VmSize:", 7) == 0)
        {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

int getPhyValue()
{ // Note: this value is in KB!
    FILE *file = fopen("/proc/self/status", "r");
    int result = -1;
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

int estimateNumEntries(float avg, float memLimit, int phyMemBase, int realeased, int maxSize)
{
    int estSize = ((memLimit * (1ull << 20) - phyMemBase) + (realeased >> 10)) / avg;
    if (maxSize == -1)
    {
        return estSize;
    }
    else
    {
        return std::min(estSize, maxSize);
    }
}

// Write hashmap hmap into file with head on start.
int writeHashmap(emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> *hmap, int file, int start)
{
    // Calc the output size for hmap.
    long pagesize = sysconf(_SC_PAGE_SIZE);
    unsigned long output_size = 0;
    auto test = hmap->begin();
    for (auto &it : *hmap)
    {
        for (auto &itt : it.first)
        {
            output_size += strlen(std::to_string(itt).c_str());
        }

        if (op != average)
        {
            output_size += strlen(std::to_string(it.second[0]).c_str());
        }
        else
        {
            output_size += strlen(std::to_string(it.second[0] / (float)(it.second[1])).c_str());
        }
    }
    for (int i = 0; i < key_number; i++)
    {
        output_size += (strlen(("\"" + key_names[i] + "\":").c_str())) * hmap->size();
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

    // Map file with given size.
    char *mappedoutputFile = static_cast<char *>(mmap(nullptr, start + output_size, PROT_WRITE | PROT_READ, MAP_SHARED, file, 0));
    madvise(mappedoutputFile, start + output_size, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (mappedoutputFile == MAP_FAILED)
    {
        close(file);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    // Write into file through mapping. Starting at the given start point.
    int mapped_count = start;
    int head = 0;
    for (auto &it : *hmap)
    {
        std::string temp_line = "{";
        for (int k = 0; k < key_number; k++)
        {
            temp_line += "\"" + key_names[k] + "\":" + std::to_string(it.first[k]);
            if (k + 1 < key_number)
            {
                temp_line += ",";
            }
        }
        if (op != average)
        {
            temp_line += ",\"_col1\":" + std::to_string(it.second[0]) + "}";
        }
        else
        {
            temp_line += ",\"_col1\":" + std::to_string(it.second[0] / (float)(it.second[1])) + "}";
        }

        for (auto &itt : temp_line)
        {
            mappedoutputFile[mapped_count] = itt;
            mapped_count += 1;
            if (mapped_count - head > pagesize * 100)
            {
                // calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                int freed_space_temp = (mapped_count - head) - ((mapped_count - head) % pagesize);
                if (munmap(&mappedoutputFile[head], freed_space_temp) == -1)
                {
                    perror("Could not free memory in writeHashmap!");
                }
                // Update Head to point at the new unfreed mapping space.

                head += freed_space_temp;
            }
        }
    }

    // free mapping and return the size of output of hmap.
    if (munmap(&mappedoutputFile[head], start + output_size - head) == -1)
    {
        perror("Could not free memory in writeHashmap 2!");
    }

    return output_size;
}

unsigned long parseJson(char *mapping, unsigned long start, std::string keys[], std::unordered_map<std::string, std::string> *lineObjects, size_t limit)
{
    unsigned long i = start;
    while (true)
    {
        if (i > limit)
        {
            return -1;
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

void spillToFile(emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> *hmap, std::pair<int, size_t> *spill_file, int id, long pagesize)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    auto firstEntry = hmap->begin();
    // Calc spill size
    size_t spill_mem_size = hmap->size() * sizeof(int) * (key_number + value_number);

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

    // Create mapping to file
    int *spill = (int *)(mmap(nullptr, spill_file->second + spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file->first, 0));
    madvise(spill, spill_file->second + spill_mem_size, MADV_SEQUENTIAL | MADV_WILLNEED);
    if (spill == MAP_FAILED)
    {
        close(spill_file->first);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    // Write int to Mapping
    unsigned long counter = spill_file->second / sizeof(int);
    unsigned long writehead = counter;
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

        // Free up space every 100th page
        if (int used_space = (counter - writehead) * sizeof(int) > pagesize * 100)
        {
            int freed_space = used_space - (used_space % pagesize);
            munmap(&spill[writehead], freed_space);
            writehead += freed_space / sizeof(int);
        }
    }

    // Cleanup: clear hashmap and free rest of mapping space
    hmap->clear();
    munmap(&spill[writehead], spill_file->second + spill_mem_size - writehead * sizeof(int));
    spill_file->second += spill_mem_size;
    // std::cout << "Spilled with size: " << spill_mem_size << std::endl;
}

void execOperation(std::array<int, max_size> *hashValue, enum Operation op, int value)
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

void addPair(emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> *hmap, std::array<int, max_size> keys, int opValue)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    switch (op)
    {
    case count:
    {
        int count = 0;
        if (opValue != -1)
        {
            count = 1;
        }
        std::pair<std::array<int, max_size>, std::array<int, max_size>> pair(keys, {count, -1});
        hmap->insert(pair);
        break;
    }
    case sum:
    {
        int sum = 0;
        if (opValue != -1)
        {
            sum = opValue;
        }
        std::pair<std::array<int, max_size>, std::array<int, max_size>> pair(keys, {sum, -1});
        hmap->insert(pair);
        break;
    }
    case average:
    {
        int count = 0;
        int sum = 0;
        if (opValue != -1)
        {
            count = 1;
            sum = opValue;
        }
        std::pair<std::array<int, max_size>, std::array<int, max_size>> pair(keys, {sum, count});
        hmap->insert(pair);
        break;
    }
    default:
    {
        std::pair<std::array<int, max_size>, std::array<int, max_size>> pair(keys, {});
        hmap->insert(pair);
    }
    }
}

void fillHashmap(int id, emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> *hmap, int file, size_t start, size_t size, bool addOffset, float memLimit, int phyMembase,
                 float &avg, std::vector<std::pair<int, size_t>> *spill_files, std::atomic<unsigned long> &numLines, std::atomic<unsigned long> &comb_hash_size, std::atomic<unsigned long> &shared_freed_space, unsigned long *shared_diff)
{
    // hmap = (emhash8::HashMap<std::array<int, key_number>, std::array<int, value_number>, decltype(hash), decltype(comp)> *)(hmap);
    auto start_time = std::chrono::high_resolution_clock::now();
    long pagesize = sysconf(_SC_PAGE_SIZE);
    int offset = 0;
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
    std::array<int, max_size> keys;
    std::string coloumns[key_number + 1];
    for (int i = 0; i < key_number; i++)
    {
        coloumns[i] = key_names[i];
    }
    coloumns[key_number] = opKeyName;
    unsigned long numHashRows = estimateNumEntries(avg, memLimit, phyMembase, 0, -1);
    unsigned long head = 0;
    unsigned long freed_space = 0;
    unsigned long numLinesLocal = 0;
    unsigned long maxHmapSize = 0;
    int opValue = -1;
    std::string okey, lineObjectValue;
    std::pair<int, size_t> spill_file(-1, 0);

    // loop through entire mapping
    for (unsigned long i = 0; i < size + offset; i++)
    {
        i = parseJson(mappedFile, i, coloumns, &lineObjects, size);
        *shared_diff = i - head;
        if (i == -1)
        {
            break;
        }
        try
        {
            for (int k = 0; k < key_number; k++)
            {
                keys[k] = std::stoi(lineObjects[key_names[k]]);
            }
            if (lineObjects[opKeyName] == "null")
            {
                opValue = -1;
            }
            else
            {
                opValue = std::stoi(lineObjects[opKeyName]);
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
            if (opValue != -1)
            {
                execOperation(&(*hmap)[keys], op, opValue);
            }
        }
        else
        {

            // add customerkey, count pair to hashmap. When orderkey is not null count starts at 1.
            addPair(hmap, keys, opValue);
            comb_hash_size.fetch_add(1);

            // Check if Estimations exceed memlimit
            // if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
            if (hmap->size() >= numHashRows * 0.9)
            {
                // std::cout << "memLimit broken. Estimated mem used: " << hashmap.size() * avg + phyMemBase << " Real memory usage: " << getPhyValue() << std::endl;

                // Free up space from mapping that is no longer needed.
                if (i - head + 1 > pagesize)
                {
                    // calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                    int freed_space_temp = (i - head + 1) - ((i - head + 1) % pagesize);
                    if (munmap(&mappedFile[head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory!");
                    }

                    // std::cout << "Releasing memory: " << freed_space << std::endl;

                    // Update Head to point at the new unfreed mapping space.

                    freed_space += freed_space_temp;
                    // Update numHashRows so that the estimations are still correct.
                    numHashRows = estimateNumEntries(avg, memLimit, phyMembase, freed_space, maxHmapSize);
                    // std::cout << "diff: " << i - head + 1 << " freed space: " << freed_space_temp << " Estimated Rows: " << numHashRows << std::endl;
                    //  phyMemBase -= freed_space >> 10;
                    head += freed_space_temp;
                }

                // compare estimation again to memLimit
                if (hmap->size() > numHashRows)
                {

                    // Reset freed_space and update numHashRows so that Estimation stay correct
                    freed_space = 0;
                    if (maxHmapSize == 0)
                    {
                        maxHmapSize = hmap->size();
                        // std::cout << "new MaxSize: " << maxHmapSize << std::endl;
                    }
                    numHashRows = estimateNumEntries(avg, memLimit, phyMembase, freed_space, maxHmapSize);

                    spillToFile(hmap, &spill_file, id, pagesize);
                }
            }
        }
        // After line is read clear it and set reading to false till the next {
        numLines.fetch_add(1);
        numLinesLocal++;
    }

    munmap(&mappedFile[head], size - head);

    if (spill_file.first != -1)
    {
        spill_files->push_back(spill_file);
        // std::cout << "Thread: " << id << " spilled with size: " << spill_file.second << std::endl;
    }
    shared_freed_space.fetch_add(freed_space);
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Thread " << id << " finished scanning. With time: " << duration << "s. Scanned Lines: " << numLinesLocal << ". microseconds/line: " << duration * 1000000 / numLinesLocal << ". Spilled with size: " << spill_file.second << std::endl;
}

void printSize(bool &finished, float memLimit)
{
    // int counter = 0;
    float maxSize = 0;
    while (!finished)
    {
        float size = (float)(getPhyValue()) / 1049000;
        if (size > maxSize)
        {
            maxSize = size;
        }
        if (memLimit < size)
        {
            std::cout << "TOO BIG! " << size << std::endl;
            sleep(1);
        }
        /* else if (counter > 100)
        {
            std::cout << "Size: " << size << std::endl;
            counter = 0;
        } */
        sleep(0.1);
        // counter++;
    }
    std::cout << "Max Size: " << maxSize << std::endl;
}

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename, float memLimit, int threadNumber, bool measure_mem)
{
    // Inits and decls
    long pagesize = sysconf(_SC_PAGE_SIZE);
    std::string err;

    // open inputfile and get size from stats
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "Input file size: " << stats.st_size << std::endl;
    std::pair<int, int> spill_file = std::pair<int, int>(-1, 0);
    std::unordered_map<std::string, std::string> lineObjects;
    int readingMode = -1;
    bool finished = false;
    std::thread sizePrinter;
    if (measure_mem)
    {
        sizePrinter = std::thread(printSize, std::ref(finished), memLimit);
    }
    memLimit -= 0.005;

    // https://martin.ankerl.com/2022/08/27/hashmap-bench-01/#emhash8__HashMap

    // ankerl::unordered_dense::segmented_map<int, int> ankerlHashmap;

    // https://github.com/ktprime/emhash/tree/master
    value_number = 1;
    if (op == average)
    {
        value_number = 2;
    }

    emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> emHashmaps[threadNumber];
    std::vector<std::pair<int, size_t>> spills = std::vector<std::pair<int, size_t>>();
    std::atomic<unsigned long> numLines = 0;
    std::atomic<unsigned long> freed_space = 0;
    std::atomic<unsigned long> comb_hash_size = 0;
    std::vector<unsigned long> diff;
    for (int i = 0; i < threadNumber; i++)
    {
        diff.push_back(0);
    }
    float avg = 0.5;

    size_t t1_size = size / threadNumber - (size / threadNumber % pagesize);
    size_t t2_size = size - t1_size * (threadNumber - 1);
    std::cout << "t1 size: " << t1_size << " t2 size: " << t2_size << std::endl;
    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();

    int phyMemBase = getPhyValue();

    for (int i = 0; i < threadNumber - 1; i++)
    {
        emHashmaps[i] = {};
        threads.push_back(std::thread(fillHashmap, i, &emHashmaps[i], fd, t1_size * i, t1_size, true, memLimit / threadNumber, phyMemBase / threadNumber,
                                      std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), std::ref(freed_space), &diff[i]));
    }
    emHashmaps[threadNumber - 1] = {};
    threads.push_back(std::thread(fillHashmap, threadNumber, &emHashmaps[threadNumber - 1], fd, t1_size * (threadNumber - 1), t2_size, false, memLimit / threadNumber, phyMemBase / threadNumber,
                                  std::ref(avg), &spills, std::ref(numLines), std::ref(comb_hash_size), std::ref(freed_space), &diff[threadNumber - 1]));

    while (numLines.load() < 100000)
    {
    }
    unsigned long reservedMem = 0;
    for (int i = 0; i < threadNumber; i++)
    {
        reservedMem += diff[i];
    }
    float hash_avg = (getPhyValue() - phyMemBase - (reservedMem >> 10)) / (float)(comb_hash_size.load());
    std::cout << "phy: " << getPhyValue() << " phymemBase: " << phyMemBase << " hash_avg: " << hash_avg << std::endl;

    // calc avg as Phy mem used by hashtable + mapping / hashtable size
    avg = (getPhyValue() - phyMemBase) / (float)(comb_hash_size.load());
    avg *= 1.3;
    std::cout << "phy: " << getPhyValue() << " phymemBase: " << phyMemBase << " #Hash entries: " << comb_hash_size.load() << " avg: " << avg << std::endl;

    for (auto &thread : threads)
    {
        thread.join();
    }
    close(fd);
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
    std::cout << "Scanning finished with time: " << duration << "s. Scanned Lines: " << numLines << ". seconds/line: " << duration * 1000000 / numLines << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> emHashmap = emHashmaps[0];
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
    float avglineLengtth = size / numLines.load();
    avg = hash_avg + (avg - hash_avg) * (sizeof(int) * 2 / avglineLengtth) + 0.02;
    // avg = avg + (float)(sizeof(int) - avglineLengtth) / 1024;
    // avg *= 8 / avglineLengtth;

    // std::cout << "new avg: " << avg << " hashmap size: " << emHashmap.size() << " hash avg: " << hash_avg << std::endl;

    // Free up rest of mapping of input file and close the file
    close(fd);

    // std::cout << "Scanning finished." << std::endl;

    // Open the outputfile to write results
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (!spills.empty())
    {

        // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
        unsigned long input_head_base = 0;
        unsigned long output_head = 0;
        bool locked = true;
        int *spill_map = nullptr;
        unsigned long comb_spill_size = 0;
        for (auto &it : spills)
            comb_spill_size += it.second;

        // std::cout << "comb_spill_size: " << comb_spill_size << std::endl;

        // create mapping to spill

        // merge and fill hashmap with all spills
        while (locked)
        {
            unsigned long num_entries = 0;
            unsigned long numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space, -1);
            unsigned long input_head = 0; // input_head_base;
            locked = false;
            unsigned long offset = 0;
            unsigned long sum = 0;
            unsigned long newi = 0;
            size_t mapping_size = 0;
            // Go through entire mapping
            for (unsigned long i = input_head_base; i < comb_spill_size / sizeof(int); i++)
            {
                if (i >= sum / sizeof(int))
                {
                    sum = 0;
                    for (auto &it : spills)
                    {
                        sum += it.second;
                        if (i < sum / sizeof(int))
                        {
                            if (spill_map != nullptr && mapping_size - input_head * sizeof(int) > 0)
                            {
                                // save empty flag and release the mapping
                                if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(int)) == -1)
                                {
                                    std::cout << "invalid size: " << mapping_size - input_head * sizeof(int) << std::endl;
                                    perror("Could not free memory in merge 2_1!");
                                }
                                freed_space += mapping_size - input_head * sizeof(int);
                                numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space, -1);
                            }
                            spill_map = static_cast<int *>(mmap(nullptr, it.second, PROT_WRITE | PROT_READ, MAP_SHARED, it.first, 0));
                            if (spill_map == MAP_FAILED)
                            {
                                close(it.first);
                                perror("Error mmapping the file");
                                exit(EXIT_FAILURE);
                            }
                            madvise(spill_map, it.second, MADV_SEQUENTIAL | MADV_WILLNEED);
                            input_head = 0;
                            offset = (sum - it.second) / sizeof(int);
                            mapping_size = it.second;
                            std::cout << "sum: " << sum << " offset: " << offset << " head: " << input_head_base << " i: " << i << std::endl;
                            break;
                        }
                    }
                }
                // std::cout << "start" << std::endl;
                newi = i - offset;
                int ognewi = newi;

                if (spill_map[newi] == -1)
                {
                    i += key_number;
                    if (op == average)
                    {
                        i++;
                    }
                    continue;
                }
                std::array<int, max_size> keys = {};
                std::array<int, max_size> values = {};

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
                // std::cout << "free" << std::endl;

                // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
                if (emHashmap.size() * hash_avg + (newi - input_head + 2) * sizeof(int) * (1ull >> 10) >= memLimit * (1ull << 20) && (newi - input_head + 2) * sizeof(int) > pagesize)
                {
                    //  calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                    int used_space = (newi - input_head + 2) * sizeof(int);
                    int freed_space_temp = used_space - (used_space % pagesize);
                    if (munmap(&spill_map[input_head], freed_space_temp) == -1)
                    {
                        perror("Could not free memory in merge 1!");
                    }
                    // Update Head to point at the new unfreed mapping space.
                    input_head += freed_space_temp / sizeof(int);
                    freed_space += freed_space_temp;
                    // Update numHashRows so that the estimations are still correct.
                    numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space, -1);
                    // std::cout << "hashmap size: " << emHashmap.size() * hash_avg << " freed space: " << (newi - input_head + 2) * sizeof(int) * (1ull >> 10) << "memLimit" << memLimit * (1ull << 20) << std::endl;
                }
                // std::cout << "merging/adding" << std::endl;
                //  Update count if customerkey is in hashmap and delete pair in spill
                if (emHashmap.contains(keys))
                {
                    for (int i = 0; i < value_number; i++)
                    {
                        emHashmap[keys][i] += values[i];
                    }
                    // mergeHashEntries(&emHashmap[keys], &values);
                    //  std::cout << "temp merging" << std::endl;
                    //    delete pair in spill
                    spill_map[ognewi] = -1;
                }
                else
                {
                    if (!locked && emHashmap.size() * hash_avg + (newi - input_head + 2) * sizeof(int) * (1ull >> 10) < memLimit * (1ull << 20))
                    {
                        // add pair to hashmap
                        std::pair<std::array<int, max_size>, std::array<int, max_size>> pair{keys, values};
                        emHashmap.insert(pair);
                        // delete pair in spill
                        spill_map[ognewi] = -1;
                    }
                    else
                    {
                        // If pair in spill is not deleted flag the spill as not empty
                        if (!locked)
                        {
                            input_head_base = newi - 1;
                        }
                        num_entries++;
                        locked = true;
                    }
                }
            }
            // save empty flag and release the mapping
            if (munmap(&spill_map[input_head], mapping_size - input_head * sizeof(int)) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
            std::cout << "hashmap size: " << emHashmap.size() << std::endl;
            //   write merged hashmap to the result and update head to point at the end of the file
            output_head += writeHashmap(&emHashmap, output_fd, output_head);
            emHashmap.clear();
            freed_space = 0;
        }
        for (auto &it : spills)
            close(it.first);
        duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        // std::cout << "Merging Spills and writing output finished with time: " << duration << "s." << std::endl;
    }
    else
    {
        std::cout << "writing to output file" << std::endl;

        // write hashmap to output file
        writeHashmap(&emHashmap, output_fd, 0);
        duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time).count()) / 1000000;
        std::cout << "Writing output finished with time: " << duration << "s." << std::endl;
    }
    close(output_fd);
    if (measure_mem)
    {
        finished = true;
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
    emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> hashmap;
    bool reading = false;
    int opValue;
    std::array<int, max_size> keys;
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
                keys[k] = std::stoi(lineObjects[key_names[k]]);
            }
            if (lineObjects[opKeyName] == "null")
            {
                opValue = -1;
            }
            else
            {
                opValue = std::stoi(lineObjects["_col1"]);
            }
        }
        catch (std::exception &err)
        {
            for (auto &it : lineObjects)
            {
                std::cout << it.first << ", " << it.second << std::endl;
            }
            std::cout << "conversion error test on: " << err.what() << std::endl;
        }
        addPair(&hashmap, keys, opValue);
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
    emhash8::HashMap<std::array<int, max_size>, std::array<int, max_size>, decltype(hash), decltype(comp)> hashmap2;
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
                keys[k] = std::stoi(lineObjects[key_names[k]]);
            }
            if (lineObjects[opKeyName] == "null")
            {
                opValue = -1;
            }
            else
            {
                opValue = std::stoi(lineObjects["_col1"]);
            }
        }
        catch (std::exception &err)
        {
            for (auto &it : lineObjects)
            {
                std::cout << it.first << ", " << it.second << std::endl;
            }
            std::cout << "conversion error test on: " << err.what() << std::endl;
        }
        addPair(&hashmap2, keys, opValue);
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
        if (hashmap2[it.first][0] != it.second[0])
        {
            std::cout << "File 2 has different value for key: " << it.first[0] << "; File 1: " << it.second[0] << "; File 2: " << hashmap2[it.first][0] << std::endl;
            same = false;
        }
    }
    if (same)
    {
        std::cout << "Files are the Same!" << std::endl;
    }

    return 0;
}

int main(int argc, char **argv)
{
    std::string co_output = argv[1];
    std::string tpc_sup = argv[2];
    std::string memLimit_string = argv[3];
    std::string threadNumber_string = argv[4];
    std::string tpc_query_string = argv[5];
    bool measure_mem = false;
    if (argc > 6)
    {
        std::string measure_mem_string = argv[6];
        measure_mem = measure_mem_string == "true";
    }

    int threadNumber = std::stoi(threadNumber_string);
    int tpc_query = std::stoi(tpc_query_string);
    float memLimit = std::stof(memLimit_string);

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

    aggregate(co_output, agg_output, memLimit, threadNumber, measure_mem);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count()) / 1000000;
    std::cout << "Aggregation finished. With time: " << duration << "s. Checking results." << std::endl;
    return test(agg_output, tpc_sup);
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");S */
}