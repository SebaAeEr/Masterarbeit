#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <json/json.h>
#include <unordered_map>
#include <stdio.h>

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

int writeHashmap(std::unordered_map<int, int> hmap, int file, int start)
{
    long unsigned int output_size = 0;
    for (auto &it : hmap)
    {
        std::string temp_line = "{\"custkey\":" + std::to_string(it.first) + ",\"_col1\":" + std::to_string(it.second) + "}\n";
        output_size += strlen(temp_line.c_str());
    }
    lseek(file, start + output_size - 1, SEEK_SET);
    if (write(file, "", 1) == -1)
    {
        close(fd);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }
    std::cout << "Output file size: " << output_size << std::endl;
    char *mappedoutputFile = static_cast<char *>(mmap(nullptr, output_size, PROT_WRITE | PROT_READ, MAP_SHARED, file, 0));
    if (mappedoutputFile == MAP_FAILED)
    {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    int mapped_count = 0;
    for (auto &it : hmap)
    {
        std::string temp_line = "{\"custkey\":" + std::to_string(it.first) + ",\"_col1\":" + std::to_string(it.second) + "}\n";
        for (auto &itt : temp_line)
        {
            mappedoutputFile[mapped_count] = itt;
            mapped_count += 1;
        }
    }

    if (msync(mappedoutputFile, output_size, MS_SYNC) == -1)
    {
        perror("Could not sync the file to disk");
    }
    // memcpy(mappedoutputFile, jsonString.c_str(), output_size);

    munmap(mappedoutputFile, output_size);
    return output_size;
}

int aggregate(std::string inputfilename, std::string outputfilename)
{
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "Input file size: " << stats.st_size << std::endl;
    char *mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));
    Json::CharReaderBuilder readerBuilder;
    Json::Value inputDataAsJson;

    std::string err;
    const std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
    std::string line = "";
    int ckey;
    std::string okey;
    std::unordered_map<int, int> hashmap;
    std::vector<std::string> vectortemp;
    bool reading = false;
    int virtMemBase = getVirtValue();
    std::cout << "virtMemBase: " << virtMemBase << std::endl;
    int phyMemBase = getPhyValue();
    std::cout << "phyMemBase: " << phyMemBase << std::endl;
    float avg = 0.44;
    float memLimit = 0.5;
    int *spill;
    bool spill_occ = false;
    int spill_size;
    std::vector<std::pair<int, int>> spills;
    for (int i = 0; i < size; ++i)
    {
        if (mappedFile[i] == '{')
        {
            reading = true;
        }
        if (reading)
        {
            line += mappedFile[i];
            if (mappedFile[i] == '}')
            {
                if (reader->parse(line.c_str(), line.c_str() + line.length(), &inputDataAsJson, &err))
                {
                    ckey = inputDataAsJson["custkey"].asInt();
                    okey = inputDataAsJson["orderkey"].asString();
                    if (hashmap.contains(ckey))
                    {
                        if (!okey.empty())
                        {
                            hashmap[ckey] += 1;
                        }
                    }
                    else
                    {
                        std::pair<int, int> pair(ckey, 0);
                        if (!okey.empty())
                        {
                            pair.second = 1;
                        }

                        hashmap.insert(pair);
                        if (hashmap.size() % 100000 == 0)
                        {
                            int curphyValue = getPhyValue();
                            avg = (curphyValue - phyMemBase) / (float)(hashmap.size());
                            std::cout << "total: " << curphyValue << " hashmap size: " << hashmap.size() << " div: " << avg << std::endl;
                        }
                        if (hashmap.size() * avg > memLimit * (1ull << 20))
                        {
                            avg = (getPhyValue() - phyMemBase) / (float)(hashmap.size());
                            if (hashmap.size() * avg > memLimit * (1ull << 20))
                            {
                                phyMemBase += getPhyValue();
                                size_t spill_mem_size = hashmap.size() * sizeof(int) * 2;
                                spill_size = hashmap.size() * 2;
                                std::string file_name = "spill_" + std::to_string(spills.size()) + outputfilename;
                                int spill_fd = open(file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
                                lseek(spill_fd, spill_mem_size - 1, SEEK_SET);
                                if (write(spill_fd, "", 1) == -1)
                                {
                                    close(fd);
                                    perror("Error writing last byte of the file");
                                    exit(EXIT_FAILURE);
                                }
                                spills.push_back(std::pair<int, int>(spill_fd, spill_size));
                                spill = (int *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_fd, 0));
                                if (spill == MAP_FAILED)
                                {
                                    close(fd);
                                    perror("Error mmapping the file");
                                    exit(EXIT_FAILURE);
                                }
                                int counter = 0;
                                for (auto &it : hashmap)
                                {
                                    spill[counter] = it.first;
                                    counter += 1;
                                    spill[counter] = it.second;
                                    counter += 1;
                                }
                                hashmap.clear();
                                spill_occ = true;
                                std::cout << "Spilled with size: " << spill_mem_size << std::endl;
                                std::cout << "PhyMem: " << getPhyValue() << std::endl;
                                munmap(spill, spill_size * sizeof(int));
                            }
                        }
                    }
                }
                else
                {
                    std::cout << err << "; " << line << std::endl;
                }
                vectortemp.clear();
                line.clear();
                reading = false;
            }
        }
    }
    munmap(mappedFile, size);
    close(fd);
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    int head = 0;
    if (spill_occ)
    {
        for (auto &spill_file : spills)
        {
            int *spill_map = static_cast<int *>(mmap(nullptr, spill_file.second * sizeof(int), PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));
            for (int i = 0; i < spill_file.second; i += 2)
            {
                int custkey = spill_map[i];
                int count = spill_map[i + 1];
                if (hashmap.contains(custkey))
                {
                    hashmap[custkey] = hashmap[custkey] + count;
                    spill_map[i] = -1;
                    spill_map[i + 1] = -1;
                }
            }
        }
        head += writeHashmap(hashmap, output_fd, head);
    }
    else
    {
        writeHashmap(hashmap, output_fd, 0);
    }
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
    Json::CharReaderBuilder readerBuilder;
    Json::Value inputDataAsJson;
    std::string err;
    const std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
    std::string line = "";
    std::string ckey;
    int count;
    std::unordered_map<std::string, int> hashmap;
    bool reading = false;
    for (int i = 0; i < size; ++i)
    {
        if (mappedFile[i] == '{')
        {
            reading = true;
        }
        if (reading)
        {
            line += mappedFile[i];
            if (mappedFile[i] == '}')
            {
                if (reader->parse(line.c_str(), line.c_str() + line.length(), &inputDataAsJson, &err))
                {
                    ckey = inputDataAsJson["custkey"].asString();
                    count = inputDataAsJson["_col1"].asInt();
                    std::pair<std::string, int> pair(ckey, count);
                    hashmap.insert(pair);
                }
                line.clear();
                reading = false;
            }
        }
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
    std::unordered_map<std::string, int> hashmap2;
    for (int i = 0; i < size; ++i)
    {
        if (mappedFile[i] == '{')
        {
            reading = true;
        }
        if (reading)
        {
            line += mappedFile[i];
            if (mappedFile[i] == '}')
            {
                if (reader->parse(line.c_str(), line.c_str() + line.length(), &inputDataAsJson, &err))
                {
                    ckey = inputDataAsJson["custkey"].asString();
                    count = inputDataAsJson["_col1"].asInt();
                    std::pair<std::string, int> pair(ckey, count);
                    hashmap2.insert(pair);
                }
                line.clear();
            }
        }
    }
    munmap(mappedFile, size);
    close(fd2);

    if (hashmap2.size() != hashmap.size())
    {
        std::cout << "Files have different number of keys." << " File1: " << hashmap.size() << " File2: " << hashmap2.size() << std::endl;
        return 0;
    }
    for (auto &it : hashmap)
    {
        if (!hashmap2.contains(it.first))
        {
            std::cout << "File 2 does not contain: " << it.first << std::endl;
            // return 0;
        }
        if (hashmap2[it.first] != it.second)
        {
            std::cout << "File 2 has different value for key: " << it.first << "; File 1: " << it.second << "; File 2: " << hashmap2[it.first] << std::endl;
            // return 0;
        }
    }
    std::cout << "Files are the Same!" << std::endl;
    return 0;
}

int main(int argc, char **argv)
{
    std::string co_output = argv[1];
    std::string tpc_sup = argv[2];
    std::string agg_output = "output_" + tpc_sup;
    aggregate(co_output, agg_output);
    std::cout << "Aggregation finished. Checking results." << std::endl;
    return test(agg_output, tpc_sup);
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json"); */
}