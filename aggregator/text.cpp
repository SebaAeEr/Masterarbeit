#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include </home/sebthelegend/Masterarbeit/aggregator/jsoncpp/dist/json/json.h>

int main()
{
    std::string file_name = "co_output_small.json";
    int fd = open(file_name.c_str(), O_RDONLY);
    struct stat stats;
    stat(file_name.c_str(), &stats);
    size_t size = stats.st_size;
    std::cout << stats.st_size << std::endl;
    void *mappedFile = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    char *fileChars = static_cast<char *>(mappedFile);
    Json::CharReaderBuilder readerBuilder;
    Json::Value outputDataAsJson;
    std::string err;
    const std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
    std::string line = "";
    std::string ckey, okey;
    for (int i = 0; i < size; ++i)
    {
        // if (fileInts[i] != ' ')
        //{
        line += fileChars[i];
        if (fileChars[i] == '}')
        {
            if (reader->parse(line.c_str(), line.c_str() + line.length(), &outputDataAsJson, &err))
            {
                ckey = outputDataAsJson["custkey"].asString();
                okey = outputDataAsJson["orderkey"].asString();
                std::cout << ckey << ", " << okey << std::endl;
            }
            line.clear();
        }
        //}
    }
    munmap(mappedFile, size);
    close(fd);
}
