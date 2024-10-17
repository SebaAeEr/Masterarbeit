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

// Write hashmap hmap into file with head on start.
int writeHashmap(std::unordered_map<int, int> hmap, int file, int start)
{
    // Calc the output size for hmap.
    long unsigned int output_size = 0;
    for (auto &it : hmap)
    {
        std::string temp_line = "{\"custkey\":" + std::to_string(it.first) + ",\"_col1\":" + std::to_string(it.second) + "}\n";
        output_size += strlen(temp_line.c_str());
    }
    std::cout << "Output file size: " << output_size << std::endl;

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

    /* if (msync(mappedoutputFile, start + output_size, MS_SYNC) == -1)
    {
        perror("Could not sync the file to disk");
    } */
    // memcpy(mappedoutputFile, jsonString.c_str(), output_size);

    // free mapping and return the size of output of hmap.
    munmap(mappedoutputFile, start + output_size);
    return output_size;
}

// aggregate inputfilename and write results into outpufilename
int aggregate(std::string inputfilename, std::string outputfilename)
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
    float avg = 0.44;
    float memLimit = 0.02;
    int *spill;
    bool spill_occ = false;
    int spill_size;
    std::vector<std::pair<int, int>> spills;
    int head = 0;

    // open inputfile and get size from stats
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "Input file size: " << stats.st_size << std::endl;

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
                            int curphyValue = getPhyValue();
                            avg = (curphyValue - phyMemBase) / (float)(hashmap.size());
                            std::cout << "total: " << curphyValue << " hashmap size: " << hashmap.size() << " Setting average to: " << avg << std::endl;
                        }

                        // Check if Estimations exceed memlimit
                        if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
                        {
                            // std::cout << "memLimit broken. Estimated mem used: " << hashmap.size() * avg + phyMemBase << " Real memory usage: " << getPhyValue() << std::endl;

                            // Free up space from mapping that is no longer needed.
                            if (i - head  + 1> pagesize)
                            {
                                // calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                                int freed_space = (i - head + 1) - ((i - head + 1) % pagesize);
                                if (munmap(&mappedFile[head], freed_space) == -1)
                                {
                                    perror("Could not free memory!");
                                }
                                // std::cout << "Releasing memory: " << freed_space << std::endl;

                                // Update Head to point at the new unfreed mapping space.
                                head += freed_space;
                                // Update phyMemBase so that the estimations are still correct.
                                phyMemBase -= freed_space >> 10;
                            }

                            // compare estimation again to memLimit
                            if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20))
                            {

                                // Reset phyMemBase so that Estimation stay correct
                                phyMemBase = resetphyMemBase;
                                std::cout << " size: " << hashmap.size() << std::endl;

                                // Calc spill size
                                size_t spill_mem_size = hashmap.size() * sizeof(int) * 2;
                                spill_size = hashmap.size() * 2;

                                // Set Filename to "spill_#spills"
                                std::string file_name = "spill_" + std::to_string(spills.size());
                                // open (new) File to write to
                                int spill_fd = open(file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);

                                // extend file
                                lseek(spill_fd, spill_mem_size - 1, SEEK_SET);
                                if (write(spill_fd, "", 1) == -1)
                                {
                                    close(fd);
                                    perror("Error writing last byte of the file");
                                    exit(EXIT_FAILURE);
                                }

                                // Save opened filehandler and spill size for later reading
                                spills.push_back(std::pair<int, int>(spill_fd, spill_size));

                                // Create mapping to file
                                spill = (int *)(mmap(nullptr, spill_mem_size, PROT_WRITE | PROT_READ, MAP_SHARED, spill_fd, 0));
                                if (spill == MAP_FAILED)
                                {
                                    close(fd);
                                    perror("Error mmapping the file");
                                    exit(EXIT_FAILURE);
                                }

                                // Write int to Mapping
                                int counter = 0;
                                int writehead = 0;
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
                                munmap(&spill[writehead], spill_mem_size - writehead * sizeof(int));
                                std::cout << "Spilled with size: " << spill_mem_size << std::endl;
                                std::cout << "last PhyMem: " << getPhyValue() << std::endl;
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
                reading = false;
            }
        }
    }
    // Free up rest of mapping of input file and close the file
    munmap(&mappedFile[head], size - head);
    close(fd);

    std::cout << "Scanning finished." << std::endl;

    // Open the outputfile to write results
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    int file_head = 0;

    // In case a spill occured, merge spills, otherwise just write hashmap
    if (spill_occ)
    {
        // Until all spills are written: merge hashmap with all spill files and fill it up until memLimit is reached, than write hashmap and clear it, repeat
        while (!spills.empty())
        {
            std::vector<bool> empties;
            bool locked = false;
            // merge and fill hashmap with all spills
            for (auto &spill_file : spills)
            {
                bool is_empty = true;
                head = 0;
                int num_entries = 0;
                phyMemBase = resetphyMemBase;

                // create mapping to spill
                int *spill_map = static_cast<int *>(mmap(nullptr, spill_file.second * sizeof(int), PROT_WRITE | PROT_READ, MAP_SHARED, spill_file.first, 0));

                // Go through entire mapping
                for (int i = 0; i < spill_file.second; i += 2)
                {
                    int custkey = spill_map[i];
                    int count = spill_map[i + 1];
                    // Update count if customerkey is in hashmap and delete pair in spill
                    if (hashmap.contains(custkey))
                    {
                        // update count
                        hashmap[custkey] = hashmap[custkey] + count;
                        // delete pair in spill
                        spill_map[i] = -1;
                        spill_map[i + 1] = -1;
                    }
                    // If pair in spill is not deleted and memLimit is not exceeded, add pair in spill to hashmap and delete pair in spill
                    else if (custkey != -1)
                    {
                        if (hashmap.size() * avg + phyMemBase > memLimit * (1ull << 20) && (i - head + 2) * sizeof(int) > pagesize)
                        {
                            //std::cout << "Start releasing" << std::endl;
                            // calc freed_space (needs to be a multiple of pagesize). And free space according to freedspace and head.
                            int used_space = (i - head + 2) * sizeof(int);
                            int freed_space = used_space - (used_space % pagesize);
                            if (munmap(&spill_map[head], freed_space) == -1)
                            {
                                perror("Could not free memory!");
                            }
                            // Update Head to point at the new unfreed mapping space.
                            head += freed_space/ sizeof(int);
                            // Update phyMemBase so that the estimations are still correct.
                            phyMemBase -= freed_space >> 10;
                            // std::cout << "Memlimit exceeded: " << hashmap.size() * avg + phyMemBase << " released: " << freed_space << std::endl;
                        }
                        if (!locked && hashmap.size() * avg + phyMemBase < memLimit * (1ull << 20))
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
                            is_empty = false;
                            num_entries++;
                            locked = true;
                        }
                    }
                }
                // save empty flag and release the mapping
                empties.push_back(is_empty);
                munmap(&spill_map[head], (spill_file.second  - head) * sizeof(int));
                std::cout << "Spill: " << empties.size() - 1 << " num of entries: " << num_entries << std::endl;
            }
            // write merged hashmap to the result and update head to point at the end of the file
            file_head += writeHashmap(hashmap, output_fd, file_head);
            std::cout << " size: " << hashmap.size() << std::endl;
            hashmap.clear();

            // go through all spills and remove every spill flagged as empty
            for (int i = 0; i < empties.size(); i++)
            {
                if (empties[i])
                {
                    close(spills[i].first);
                    spills.erase(spills.begin() + i);
                }
            }
        }
    }
    else
    {
        // write hashmap to output file
        writeHashmap(hashmap, output_fd, 0);
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