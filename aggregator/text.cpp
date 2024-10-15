#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <json/json.h>
#include <unordered_map>

int aggregate(std::string inputfilename, std::string outputfilename)
{
    int fd = open(inputfilename.c_str(), O_RDONLY);
    struct stat stats;
    stat(inputfilename.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << "size: " << stats.st_size << std::endl;
    void *mappedFile = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    char *fileChars = static_cast<char *>(mappedFile);
    Json::CharReaderBuilder readerBuilder;
    Json::Value inputDataAsJson;

    std::string err;
    const std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
    std::string line = "";
    std::string ckey, okey;
    std::unordered_map<std::string, int> hashmap;
    std::vector<std::string> vectortemp;
    bool reading = false;
    for (int i = 0; i < size; ++i)
    {
        if (fileChars[i] == '{')
        {
            reading = true;
        }
        if (reading)
        {
            line += fileChars[i];
            if (fileChars[i] == '}')
            {
                if (reader->parse(line.c_str(), line.c_str() + line.length(), &inputDataAsJson, &err))
                {
                    ckey = inputDataAsJson["custkey"].asString();
                    okey = inputDataAsJson["orderkey"].asString();
                    if (hashmap.contains(ckey))
                    {
                        if (!okey.empty())
                        {
                            /*  // vectortemp = hashmap[ckey];
                             hashmap[ckey].push_back(okey);
                             if (std::find(vectortemp.begin(), vectortemp.end(), okey) == vectortemp.end())
                             {
                                 hashmap[ckey].push_back(okey);
                             } */
                            hashmap[ckey] += 1;
                        }
                    }
                    else
                    {
                        std::pair<std::string, int> pair(ckey, 0);
                        if (!okey.empty())
                        {
                            pair.second = 1;
                        }

                        hashmap.insert(pair);
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

    Json::Value outputDataJson;
    Json::Value outputLineJson;

    for (auto &it : hashmap)
    {
        outputLineJson["custkey"] = it.first;
        outputLineJson["_col1"] = it.second;
        outputDataJson.append(outputLineJson);
        outputLineJson.clear();
    }
    std::string jsonString = outputDataJson.toStyledString();
    // std::cout << jsonString << std::endl;
    int output_fd = open(outputfilename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    size_t output_size = (strlen(jsonString.c_str()));
    lseek(output_fd, output_size - 1, SEEK_SET);
    if (write(output_fd, "", 1) == -1)
    {
        close(fd);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }
    std::cout << output_size << std::endl;
    char *mappedoutputFile = static_cast<char *>(mmap(nullptr, output_size, PROT_WRITE | PROT_READ, MAP_SHARED, output_fd, 0));
    if (mappedoutputFile == MAP_FAILED)
    {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    memcpy(mappedoutputFile, jsonString.c_str(), output_size);

    munmap(mappedoutputFile, output_size);
    close(output_fd);
    return 0;
}

int test(std::string file1name, std::string file2name)
{
    int fd = open(file1name.c_str(), O_RDONLY);
    struct stat stats;
    stat(file1name.c_str(), &stats);
    size_t size = stats.st_size;
    char *mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));
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
    stat(file2name.c_str(), &stats);
    size = stats.st_size;
    mappedFile = static_cast<char *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd2, 0));
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

int main()
{
    aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
    return test("tpc_13_output_sup_tiny_c.json", "tpc_13_sup_tiny.json");
    // return aggregate("test.txt", "output_test.json");
    //   return aggregate("co_output_tiny.json", "tpc_13_output_sup_tiny_c.json");
}