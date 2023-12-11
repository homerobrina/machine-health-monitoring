#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>

#include <fstream>
#include <string>
#include <sstream>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

std::string clientId = "sensor-monitor";
mqtt::client client(BROKER_ADDRESS, clientId);
std::mutex mutex;

// Estimativa de quanto da memória está disponível para iniciar novos processos
long getAvailableMemory() {
    std::ifstream meminfoFile("/proc/meminfo");
    std::string line;

    if (!meminfoFile.is_open()) {
        std::cerr << "Não foi possível abrir o arquivo /proc/meminfo" << std::endl;
        return -1; // Indica erro
    }

    long availableMemory = -1;

    // Procura pela linha que contém a informação sobre a memória livre (MemAvailable)
    while (getline(meminfoFile, line)) {
        std::istringstream iss(line);
        std::string key;
        long value;

        if (iss >> key >> value) {
            if (key == "MemAvailable:") {
                availableMemory = value;
                break;
            }
        }
    }

    meminfoFile.close();

    return availableMemory;
}

long getActiveMemory() {
    std::ifstream meminfoFile("/proc/meminfo");
    std::string line;

    if (!meminfoFile.is_open()) {
        std::cerr << "Não foi possível abrir o arquivo /proc/meminfo" << std::endl;
        return -1; // Indica erro
    }

    long activeMemory = -1;

    // Procura pela linha que contém a informação sobre a memória ativa
    while (getline(meminfoFile, line)) {
        std::istringstream iss(line);
        std::string key;
        long value;

        if (iss >> key >> value) {
            if (key == "Active:") {
                activeMemory = value;
                break;
            }
        }
    }

    meminfoFile.close();

    return activeMemory;
}

double calculateCpuUsage() {
    std::ifstream statFile("/proc/stat");
    std::string line;

    if (!statFile.is_open()) {
        std::cerr << "Não foi possível abrir o arquivo /proc/stat" << std::endl;
        return -1.0; // Indica erro
    }

    long user, nice, system, idle, iowait, irq, softirq;

    // Lê a primeira linha do arquivo que contém as informações de CPU
    getline(statFile, line);
    std::istringstream iss(line);

    // Descarta a palavra "cpu" e lê os valores correspondentes
    iss >> line >> user >> nice >> system >> idle >> iowait >> irq >> softirq;

    statFile.close();

    // Calcula a porcentagem de uso da CPU
    double totalCpuTime = user + nice + system + idle + iowait + irq + softirq;
    double cpuUsagePercentage = 100.0 * (1.0 - static_cast<double>(idle) / totalCpuTime);

    return cpuUsagePercentage;
}

void read_and_publish_ava_mem(std::string machineId, int freq_sensor_ava_memory, std::string id_sensor_ava_mem){

    while(true) {// Get the current time in ISO 8601 format.
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Generate a random value.       
        int value = getAvailableMemory();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        std::string topic = "/sensors/" + machineId + "/" + id_sensor_ava_mem;
        mqtt::message msg(topic, j.dump(), QOS, false);
        std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;

        mutex.lock();
        client.publish(msg);
        mutex.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(freq_sensor_ava_memory));
    }
}

void read_and_publish_act_mem(std::string machineId, int freq_sensor_act_memory, std::string id_sensor_act_mem){

    while(true) {// Get the current time in ISO 8601 format.
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Generate a random value.       
        int value = getActiveMemory();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        std::string topic = "/sensors/" + machineId + "/" + id_sensor_act_mem;
        mqtt::message msg(topic, j.dump(), QOS, false);
        std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;

        mutex.lock();
        client.publish(msg);
        mutex.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(freq_sensor_act_memory));
    }
}

void read_and_publish_cpu_use(std::string machineId, int freq_sensor_cpu_use, std::string id_sensor_cpu_use){

    while(true) {// Get the current time in ISO 8601 format.
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Generate a random value.       
        int value = calculateCpuUsage();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        std::string topic = "/sensors/" + machineId + "/" + id_sensor_cpu_use;
        mqtt::message msg(topic, j.dump(), QOS, false);
        std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;

        mutex.lock();
        client.publish(msg);
        mutex.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(freq_sensor_cpu_use));
    }
}

int main(int argc, char* argv[]) {

    if (argc < 4) {
        std::cerr << "Digite frequências válidas: " << argv[0] << " <freq_available_mem> <freq_active_mem> <freq_cpu_usage>";
        return EXIT_FAILURE;
    }

    int freq_sensor_ava_mem = std::atoi(argv[1]);
    int freq_sensor_act_mem = std::atoi(argv[2]);
    int freq_sensor_cpu_use = std::atoi(argv[3]);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "connected to the broker" << std::endl;

    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    std::string machineId(hostname);

    int frequency = 1000;

    std::string id_sensor_ava_mem("available_memory");
    std::string id_sensor_act_mem("active_memory");
    std::string id_sensor_cpu_use("cpu_usage");

    nlohmann::json j_inicial, j_sensor_ava_mem, j_sensor_act_mem, j_sensor_cpu_use;

    j_sensor_ava_mem["sensor_id"] = id_sensor_ava_mem;
    j_sensor_ava_mem["data_type"] = "float";
    j_sensor_ava_mem["data_interval"] = freq_sensor_ava_mem;
    
    j_sensor_act_mem["sensor_id"] = id_sensor_act_mem;
    j_sensor_act_mem["data_type"] = "float";
    j_sensor_act_mem["data_interval"] = freq_sensor_act_mem;

    j_sensor_cpu_use["sensor_id"] = id_sensor_cpu_use;
    j_sensor_cpu_use["data_type"] = "float";
    j_sensor_cpu_use["data_interval"] = freq_sensor_cpu_use;

    j_inicial["machine_id"] = machineId;
    j_inicial["sensors"] = {j_sensor_ava_mem,j_sensor_act_mem,j_sensor_cpu_use};

    std::string topic_i = "/sensor_monitors";
    mqtt::message msg1(topic_i, j_inicial.dump(), QOS, false);
    std::clog << "message published - topic: " << topic_i << " - message: " << j_inicial.dump() << std::endl;
    client.publish(msg1);

    std::thread thread_sensor_ava_mem(read_and_publish_ava_mem, machineId, freq_sensor_ava_mem, id_sensor_ava_mem);
    std::thread thread_sensor_act_mem(read_and_publish_act_mem, machineId, freq_sensor_act_mem, id_sensor_act_mem);
    std::thread thread_sensor_cpu_use(read_and_publish_cpu_use, machineId, freq_sensor_cpu_use, id_sensor_cpu_use);

    thread_sensor_ava_mem.join();
    thread_sensor_act_mem.join();
    thread_sensor_cpu_use.join();

    return EXIT_SUCCESS;
}
