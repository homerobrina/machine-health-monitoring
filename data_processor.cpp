#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "json.hpp" 
#include "mqtt/client.h" 

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "graphite"
#define GRAPHITE_PORT 2003

std::string clientId = "clientId";
mqtt::async_client client(BROKER_ADDRESS, clientId);

void post_metric(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp_str, const int value) {

}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
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
                std::string sensor_id1 = j["sensors"][0]["sensor_id"];
                std::string sensor_id2 = j["sensors"][1]["sensor_id"];
                std::string sensor_id3 = j["sensors"][2]["sensor_id"];

                // Frequencia de cada sensor
                int freq_sensor_id1 = j["sensors"][0]["data_interval"];
                int freq_sensor_id2 = j["sensors"][1]["data_interval"];
                int freq_sensor_id3 = j["sensors"][2]["data_interval"];

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

                // std::cout << "inscrição ok" << std::endl;

                std::string timestamp = j["timestamp"];
                int value = j["value"];
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
