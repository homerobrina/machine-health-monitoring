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

std::mutex mutex_send, mutex_vec;
std::vector<int> last_measures_ava_mem(20,0);
int index_circ_buffer;
int window_size_for_av;

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

std::time_t Iso8601_2_UNIX(std::string timestamp_Iso8601){

    // Converter o timestamp ISO 8601 para um objeto de tempo
    std::tm tm = {};
    std::istringstream ss(timestamp_Iso8601);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail()) {
        std::cerr << "Erro ao converter o timestamp ISO 8601 para o objeto de tempo." << std::endl;
        return 1;
    }

    // Converter o objeto de tempo para o timestamp Unix
    std::time_t unixTimestamp = std::mktime(&tm);

    return unixTimestamp;
}

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
        mutex_send.lock();
        boost::asio::connect(socket, endpoint_iterator);

        // Passo 5: Enviar dados para o Graphite
        std::string data = machine_id + "." + sensor_id + " " + std::to_string(value) + " " + std::to_string(Iso8601_2_UNIX(timestamp_str)) + "\n";
        boost::asio::write(socket, boost::asio::buffer(data));
        // std::cout << "Mensagem enviada :" << data << std::endl;

        // Passo 6: Fechar a conexão
        socket.close();
        mutex_send.unlock();
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

void post_inactivity_alarms(const std::string& machine_id, const std::string& timestamp_str) {
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
        mutex_send.lock();
        boost::asio::connect(socket, endpoint_iterator);

        // Passo 5: Enviar dados para o Graphite
        std::string data = machine_id + ".alarms.inactivity." + sensor_id1  + " " + std::to_string(sensor1_inactive) + " " + std::to_string(Iso8601_2_UNIX(timestamp_str)) + "\n";
        boost::asio::write(socket, boost::asio::buffer(data));
        // std::cout << "Mensagem enviada :" << data << std::endl;
        data = machine_id + ".alarms.inactivity." + sensor_id2  + " " + std::to_string(sensor2_inactive) + " " + std::to_string(Iso8601_2_UNIX(timestamp_str)) + "\n";
        boost::asio::write(socket, boost::asio::buffer(data));
        // std::cout << "Mensagem enviada :" << data << std::endl;
        data = machine_id + ".alarms.inactivity." + sensor_id3  + " " + std::to_string(sensor3_inactive) + " " + std::to_string(Iso8601_2_UNIX(timestamp_str)) + "\n";
        boost::asio::write(socket, boost::asio::buffer(data));
        // std::cout << "Mensagem enviada :" << data << std::endl;

        // Passo 6: Fechar a conexão
        socket.close();
        mutex_send.unlock();
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
    }
}

void post_moving_av(const std::string& machine_id, const std::string& timestamp_str, int moving_av){
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
        mutex_send.lock();
        boost::asio::connect(socket, endpoint_iterator);

        // Passo 5: Enviar dados para o Graphite
        std::string data = machine_id + "." + sensor_id1 + "_mov_av"  + " " + std::to_string(moving_av) + " " + std::to_string(Iso8601_2_UNIX(timestamp_str)) + "\n";
        boost::asio::write(socket, boost::asio::buffer(data));
        // std::cout << "Mensagem enviada :" << data << std::endl;

        // Passo 6: Fechar a conexão
        socket.close();
        mutex_send.unlock();
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

void check_inactivity(const std::string& machine_id) {
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
            // std::cout << "SENSOR INATIVO - ID: " << sensor_id1 << std::endl;
            sensor1_inactive = true;
        } else if (diff1_milliseconds.count() < 5*freq_sensor_id1 && sensor1_inactive){
            sensor1_inactive = false;
            // std::cout << "SENSOR REATIVADO - ID: " << sensor_id1 << std::endl;
        }
        if (diff2_milliseconds.count() >= 5*freq_sensor_id2 && !sensor2_inactive){
            // std::cout << "SENSOR INATIVO - ID: " << sensor_id2 << std::endl;
            sensor2_inactive = true;
        } else if (diff2_milliseconds.count() < 5*freq_sensor_id2 && sensor2_inactive){
            sensor2_inactive = false;
            // std::cout << "SENSOR REATIVADO - ID: " << sensor_id2 << std::endl;
        }
        if (diff3_milliseconds.count() >= 5*freq_sensor_id3 && !sensor3_inactive){
            // std::cout << "SENSOR INATIVO - ID: " << sensor_id3 << std::endl;
            sensor3_inactive = true;
        } else if (diff3_milliseconds.count() < 5*freq_sensor_id3 && sensor3_inactive){
            sensor3_inactive = false;
            // std::cout << "SENSOR REATIVADO - ID: " << sensor_id3 << std::endl;
        }

        post_inactivity_alarms(machine_id, timestamp);

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void calculate_moving_av(int window_size, const std::string& machine_id){
    while(true) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        int sum = 0;
        mutex_vec.lock();
        for (int i = 0; i < window_size; i++)
        {
            sum = sum + last_measures_ava_mem[i];
        }
        mutex_vec.unlock();

        int moving_av = sum/window_size;

        post_moving_av(machine_id, timestamp, moving_av);

        std::this_thread::sleep_for(std::chrono::milliseconds(freq_sensor_id1));
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

                // std::cout << "sensor_id1: " << sensor_id1 << std::endl;
                // std::cout << "sensor_id2: " << sensor_id2 << std::endl;
                // std::cout << "sensor_id3: " << sensor_id3 << std::endl;

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

                std::thread t_check_inactivity(check_inactivity, machine_id);
                t_check_inactivity.detach();

                index_circ_buffer = 0;
                window_size_for_av = 30;

                std::thread t_calculate_moving_av(calculate_moving_av, window_size_for_av, machine_id);
                t_calculate_moving_av.detach();

            } else {
                std::string topic = msg->get_topic();
                auto topic_parts = split(topic, '/');
                std::string machine_id = topic_parts[2];
                std::string sensor_id = topic_parts[3];

                std::string timestamp = j["timestamp"];
                int value = j["value"];

                if (sensor_id == sensor_id1) {
                    last_timestamp_sensor1 = parseTimestamp(timestamp);
                    mutex_vec.lock();
                    last_measures_ava_mem[index_circ_buffer] = value;
                    index_circ_buffer = (index_circ_buffer + 1) % window_size_for_av;
                    mutex_vec.unlock();
                } else if (sensor_id == sensor_id2) {
                    last_timestamp_sensor2 = parseTimestamp(timestamp);
                } else if (sensor_id == sensor_id3) {
                    last_timestamp_sensor3 = parseTimestamp(timestamp);
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
