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
#include <chrono>

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

int estimateNumEntries(float avg, float memLimit, int phyMemBase, int realeased)
{
    return ((memLimit * (1ull << 20) - phyMemBase) + (realeased >> 10)) / avg;
}

// Write hashmap hmap into file with head on start.
int writeHashmap(std::unordered_map<int, int> *hmap, int file, int start)
{
    // Calc the output size for hmap.
    long pagesize = sysconf(_SC_PAGE_SIZE);
    long unsigned int output_size = 0;
    for (auto &it : *hmap)
    {
        std::string temp_line = "{\"custkey\":" + std::to_string(it.first) + ",\"_col1\":" + std::to_string(it.second) + "}\n";
        output_size += strlen(temp_line.c_str());
    }
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
        std::string temp_line = "{\"custkey\":" + std::to_string(it.first) + ",\"_col1\":" + std::to_string(it.second) + "}\n";
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

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename, float memLimit)
{
    // Inits and decls
    long pagesize = sysconf(_SC_PAGE_SIZE);
    std::cout << "pagesize: " << pagesize << std::endl;

    Json::CharReaderBuilder readerBuilder;
    Json::Value inputDataAsJson;
    std::string err;
    const std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
    std::string line = "";
    int ckey;
    std::string okey;
    std::unordered_map<int, int> hashmap;
    bool reading = false;
    int virtMemBase = getVirtValue();
    std::cout << "virtMemBase: " << virtMemBase << std::endl;
    int phyMemBase = getPhyValue();
    int resetphyMemBase = phyMemBase;
    std::cout << "phyMemBase: " << phyMemBase << std::endl;
    float avg = 0.4;
    float hash_avg = -1;
    int *spill;
    bool spill_occ = false;
    long spill_size;
    std::vector<std::pair<int, int>> spills;
    long head = 0;
    int numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, 0);
    long freed_space = 0;

    // open inputfile and get size from stats
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "Input file size: " << stats.st_size << std::endl;
    unsigned long numLines = 0;
    std::pair<int, int> spill_file = std::pair<int, int>(-1, 0);

    // map inputfile
    char *mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));

    // loop through entire mapping
    for (int i = 0; i < size; i++)
    {
        // start reading a line when {
        if (mappedFile[i] == '{')
        {
            reading = true;
        }
        if (reading)
        {
            line += mappedFile[i];
            // finish reading a line when }
            if (mappedFile[i] == '}')
            {
                // Parse string into JsonObject
                if (reader->parse(line.c_str(), line.c_str() + line.length(), &inputDataAsJson, &err))
                {
                    // extract custkey and orderkey from JsonObject
                    ckey = inputDataAsJson["custkey"].asInt();
                    okey = inputDataAsJson["orderkey"].asString();

                    // add 1 to count when customerkey is already in hashmap
                    if (hashmap.contains(ckey))
                    {
                        if (!okey.empty())
                        {
                            hashmap[ckey] += 1;
                        }
                    }
                    else
                    {

                        // add customerkey, count pair to hashmap. When orderkey is not null count starts at 1.
                        std::pair<int, int> pair(ckey, 0);
                        if (!okey.empty())
                        {
                            pair.second = 1;
                        }
                        hashmap.insert(pair);

                        // Calc average hashmap entry size after the first 100000 entries
                        if (!spill_occ && hashmap.size() == 100000)
                        {
                            // calc avg as Phy mem used by hashtable + mapping / hashtable size
                            avg = (getPhyValue() + ((freed_space >> 10) - phyMemBase)) / (float)(hashmap.size());

                            // update numHashRows
                            numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, 0);

                            std::cout << numHashRows << std::endl;
                            std::cout << " Setting average to: " << avg << " setting numHashRows to: " << numHashRows << std::endl;
                        }

                        // Check if Estimations exceed memlimit
                        // if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
                        if (hashmap.size() >= numHashRows)
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
                                numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space);
                                // std::cout << "diff: " << i - head + 1 << " freed space: " << freed_space_temp << " Estimated Rows: " << numHashRows << std::endl;
                                //  phyMemBase -= freed_space >> 10;
                                head += freed_space_temp;
                                if (hash_avg == -1 && hashmap.size() > 100000)
                                {
                                    hash_avg = (getPhyValue() - phyMemBase) / (float)(hashmap.size());
                                    std::cout << "Set hash_avg: " << hash_avg << std::endl;
                                }
                            }

                            // compare estimation again to memLimit
                            if (hashmap.size() > numHashRows)
                            {

                                // Reset freed_space and update numHashRows so that Estimation stay correct
                                // std::cout << "MemBase: " << phyMemBase << std::endl;
                                // phyMemBase = resetphyMemBase;
                                // std::cout << " size: " << hashmap.size() << std::endl;
                                freed_space = 0;
                                numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space);

                                // Calc spill size
                                size_t spill_mem_size = hashmap.size() * sizeof(int) * 2;
                                spill_size = hashmap.size() * 2;

                                if (spill_file.first == -1)
                                {
                                    spill_file.first = open("spill_file", O_RDWR | O_CREAT | O_TRUNC, 0777);
                                }

                                // extend file
                                lseek(spill_file.first, spill_file.second + spill_mem_size - 1, SEEK_SET);
                                if (write(spill_file.first, "", 1) == -1)
                                {
                                    close(fd);
                                    perror("Error writing last byte of the file");
                                    exit(EXIT_FAILURE);
                                }

                                // Create mapping to file
                                spill = (int *)(mmap(nullptr, spill_file.second + spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));
                                if (spill == MAP_FAILED)
                                {
                                    close(fd);
                                    perror("Error mmapping the file");
                                    exit(EXIT_FAILURE);
                                }

                                // Write int to Mapping
                                long counter = spill_file.second / sizeof(int);
                                long writehead = counter;
                                for (auto &it : hashmap)
                                {
                                    spill[counter] = it.first;
                                    counter += 1;
                                    spill[counter] = it.second;
                                    counter += 1;

                                    // Free up space every 100th page
                                    if (int used_space = (counter - writehead) * sizeof(int) > pagesize * 100)
                                    {
                                        int freed_space_2 = used_space - (used_space % pagesize);
                                        munmap(&spill[writehead], freed_space_2);
                                        writehead += freed_space_2 / sizeof(int);
                                    }
                                }

                                // Cleanup: clear hashmap (and destroy it) and free rest of mapping space
                                hashmap.clear();
                                spill_occ = true;
                                munmap(&spill[writehead], spill_file.second + spill_mem_size - writehead * sizeof(int));
                                spill_file.second += spill_mem_size;
                                std::cout << "Spilled with size: " << spill_mem_size << std::endl;
                                // std::cout << "last PhyMem: " << getPhyValue() << std::endl;
                            }
                        }
                    }
                }
                else
                {
                    // Error if Json parsing went wrong
                    std::cout << err << "; " << line << std::endl;
                }
                // After line is read clear it and set reading to false till the next {
                line.clear();
                numLines++;
                reading = false;
            }
        }
    }

    // calc optimistic new avg to better fit spill files as: 8/avgLineLength * (hash_avg - avg)
    float avglineLengtth = size / numLines;
    avg = hash_avg + (avg - hash_avg) * (sizeof(int) * 2 / avglineLengtth) + 0.02;
    // avg = avg + (float)(sizeof(int) - avglineLengtth) / 1024;
    // avg *= 8 / avglineLengtth;

    std::cout << "avglineLength: " << avglineLengtth << " new avg: " << avg << std::endl;

    // Free up rest of mapping of input file and close the file
    munmap(&mappedFile[head], size - head);
    freed_space += size - head;
    close(fd);

    std::cout << "Scanning finished." << std::endl;

    // Open the outputfile to write results
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);

    size_t orig_hash_size = hashmap.size();

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (spill_occ)
    {
        // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
        long input_head_base = 0;
        long output_head = 0;
        bool locked = true;

        // create mapping to spill

        // merge and fill hashmap with all spills
        while (locked)
        {
            long num_entries = 0;
            numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space);
            long input_head = 0; // input_head_base;
            locked = false;
            int *spill_map = static_cast<int *>(mmap(nullptr, spill_file.second, PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));

            // Go through entire mapping
            for (int i = input_head_base; i < spill_file.second / sizeof(int); i += 2)
            {
                int custkey = spill_map[i];
                int count = spill_map[i + 1];
                // Update count if customerkey is in hashmap and delete pair in spill
                if (hashmap.contains(custkey))
                {
                    // update count
                    hashmap[custkey] += count;
                    // delete pair in spill
                    spill_map[i] = -1;
                    spill_map[i + 1] = -1;
                }
                // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
                else if (custkey != -1)
                {
                    if (hashmap.size() >= numHashRows && (i - input_head + 2) * sizeof(int) > pagesize)
                    {
                        //  calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                        int used_space = (i - input_head + 2) * sizeof(int);
                        int freed_space_temp = used_space - (used_space % pagesize);
                        if (munmap(&spill_map[input_head], freed_space_temp) == -1)
                        {
                            perror("Could not free memory in merge 1!");
                        }
                        // Update Head to point at the new unfreed mapping space.
                        input_head += freed_space_temp / sizeof(int);
                        freed_space += freed_space_temp;
                        // Update numHashRows so that the estimations are still correct.
                        numHashRows = estimateNumEntries(avg, memLimit, phyMemBase, freed_space);
                        // std::cout << "diff: " << used_space << " freed space: " << freed_space_temp << "Estimated Rows: " << numHashRows << std::endl;
                    }
                    if (!locked && hashmap.size() < numHashRows)
                    {
                        // add pair to hashmap
                        std::pair<int, int> pair(custkey, count);
                        hashmap.insert(pair);
                        // delete pair in spill
                        spill_map[i] = -1;
                        spill_map[i + 1] = -1;
                    }
                    else
                    {
                        // If pair in spill is not deleted flag the spill as not empty
                        if (!locked)
                        {
                            input_head_base = i;
                        }
                        num_entries++;
                        locked = true;
                    }
                }
            }
            // save empty flag and release the mapping
            if (munmap(&spill_map[input_head], spill_file.second - input_head * sizeof(int)) == -1)
            {
                perror("Could not free memory in merge 2!");
            }
            // write merged hashmap to the result and update head to point at the end of the file
            output_head += writeHashmap(&hashmap, output_fd, output_head);
            hashmap.clear();
            freed_space = 0;
        }
        close(spill_file.first);
    }
    else
    {
        // write hashmap to output file
        writeHashmap(&hashmap, output_fd, 0);
    }
    close(output_fd);
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
    std::string memLimit_string = argv[3];
    float memLimit = std::stof(memLimit_string);
    std::string agg_output = "output_" + tpc_sup;
    auto start = std::chrono::high_resolution_clock::now();
    aggregate(co_output, agg_output, memLimit);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = (float)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count()) / 1000000;
    std::cout << "Aggregation finished. With time: " << duration << "seconds. Checking results." << std::endl;
    return test(agg_output, tpc_sup);
    // return aggregate("test.txt", "output_test.json");
    /* aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");S */
}