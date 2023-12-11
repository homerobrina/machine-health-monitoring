#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "json.hpp" 
#include "mqtt/client.h" 
#include <boost/asio.hpp>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "graphite"
#define GRAPHITE_PORT "2003"

std::string clientId = "clientId";
mqtt::async_client client(BROKER_ADDRESS, clientId);

// ID de cada sensor
std::string sensor_id1;
std::string sensor_id2;
std::string sensor_id3;

// Frequencia de cada sensor
int freq_sensor_id1;
int freq_sensor_id2;
int freq_sensor_id3;

// Timestamp da última mensagem recebida
std::chrono::_V2::system_clock::time_point last_timestamp_sensor1;
std::chrono::_V2::system_clock::time_point last_timestamp_sensor2;
std::chrono::_V2::system_clock::time_point last_timestamp_sensor3;

// Flag inatividade
bool sensor1_inactive;
bool sensor2_inactive;
bool sensor3_inactive;

void post_metric(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp_str, const int value) {
    // Passo 1: Criar io_service
    boost::asio::io_service io_service;

    try {
        // Passo 2: Criar um socket baseado no io_service
        boost::asio::ip::tcp::socket socket(io_service);

        // Passo 3: Resolver o nome do host para obter o endereço IP
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(GRAPHITE_HOST, GRAPHITE_PORT);
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        // Passo 4: Conectar ao servidor
        boost::asio::connect(socket, endpoint_iterator);

        // Passo 5: Enviar dados para o Graphite
        // std::string data = "cpu.usage 42.0 " + std::to_string(std::time(0)) + "\n";
        // boost::asio::write(socket, boost::asio::buffer(data));

        // Passo 6: Fechar a conexão
        socket.close();
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

auto parseTimestamp = [](const std::string& timestamp_str) {
        std::tm tm = {};
        std::istringstream ss(timestamp_str);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
        auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        return tp;
};

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

void check_inactivity() {
    while (true) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        auto diff1 = parseTimestamp(timestamp) - last_timestamp_sensor1;
        auto diff1_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(diff1);
        auto diff2 = parseTimestamp(timestamp) - last_timestamp_sensor2;
        auto diff2_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(diff2);
        auto diff3 = parseTimestamp(timestamp) - last_timestamp_sensor3;
        auto diff3_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(diff3);
        
        if (diff1_milliseconds.count() >= 5*freq_sensor_id1 && !sensor1_inactive){
            std::cout << "SENSOR INATIVO - ID: " << sensor_id1 << std::endl;
            sensor1_inactive = true;
        } else if (diff1_milliseconds.count() < 5*freq_sensor_id1 && sensor1_inactive){
            sensor1_inactive = false;
            std::cout << "SENSOR REATIVADO - ID: " << sensor_id1 << std::endl;
        }
        if (diff2_milliseconds.count() >= 5*freq_sensor_id2 && !sensor2_inactive){
            std::cout << "SENSOR INATIVO - ID: " << sensor_id2 << std::endl;
            sensor2_inactive = true;
        } else if (diff2_milliseconds.count() < 5*freq_sensor_id2 && sensor2_inactive){
            sensor2_inactive = false;
            std::cout << "SENSOR REATIVADO - ID: " << sensor_id2 << std::endl;
        }
        if (diff3_milliseconds.count() >= 5*freq_sensor_id3 && !sensor3_inactive){
            std::cout << "SENSOR INATIVO - ID: " << sensor_id3 << std::endl;
            sensor3_inactive = true;
        } else if (diff3_milliseconds.count() < 5*freq_sensor_id3 && sensor3_inactive){
            sensor3_inactive = false;
            std::cout << "SENSOR REATIVADO - ID: " << sensor_id3 << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

int main(int argc, char* argv[]) {

    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
    public:

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            if(msg->get_topic() == "/sensor_monitors"){
                // ID da máquina comum a todos
                std::string machine_id = j["machine_id"];

                // ID de cada sensor
                sensor_id1 = j["sensors"][0]["sensor_id"];
                sensor_id2 = j["sensors"][1]["sensor_id"];
                sensor_id3 = j["sensors"][2]["sensor_id"];

                std::cout << "sensor_id1: " << sensor_id1 << std::endl;
                std::cout << "sensor_id2: " << sensor_id2 << std::endl;
                std::cout << "sensor_id3: " << sensor_id3 << std::endl;

                // Frequencia de cada sensor
                freq_sensor_id1 = j["sensors"][0]["data_interval"];
                freq_sensor_id2 = j["sensors"][1]["data_interval"];
                freq_sensor_id3 = j["sensors"][2]["data_interval"];

                // Timestamp da primeira mensagem recebida

                auto now = std::chrono::system_clock::now();
                std::time_t now_c = std::chrono::system_clock::to_time_t(now);
                std::tm* now_tm = std::localtime(&now_c);
                std::stringstream ss;
                ss << std::put_time(now_tm, "%FT%TZ");
                std::string timestamp = ss.str();

                last_timestamp_sensor1 = parseTimestamp(timestamp);
                last_timestamp_sensor2 = parseTimestamp(timestamp);
                last_timestamp_sensor3 = parseTimestamp(timestamp);

                // Criando e assinando aos tópicos de cada sensor
                std::string topic_sensor1 = "/sensors/" + machine_id + "/" + sensor_id1;
                std::string topic_sensor2 = "/sensors/" + machine_id + "/" + sensor_id2;
                std::string topic_sensor3 = "/sensors/" + machine_id + "/" + sensor_id3;

                // std::cout << "primeira recebida" << std::endl;

                client.subscribe(topic_sensor1, QOS);
                client.subscribe(topic_sensor2, QOS);
                client.subscribe(topic_sensor3, QOS);

            } else {
                std::string topic = msg->get_topic();
                auto topic_parts = split(topic, '/');
                std::string machine_id = topic_parts[2];
                std::string sensor_id = topic_parts[3];

                // std::cout << "sensor_id: " << sensor_id << std::endl;
                // std::cout << "sensor_id1: " << sensor_id1 << std::endl;

                std::string timestamp = j["timestamp"];
                int value = j["value"];

                std::thread t_check_inactivity(check_inactivity);
                t_check_inactivity.detach();

                if (sensor_id == sensor_id1) {
                    last_timestamp_sensor1 = parseTimestamp(timestamp);
                }

                // std::cout << "inscrição ok" << std::endl;
                post_metric(machine_id, sensor_id, timestamp, value);
            }
        }
    };

    callback cb;
    client.set_callback(cb);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts)->wait();
        client.subscribe("/sensor_monitors", QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}
