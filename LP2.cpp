//TO COMPILE: mpiCC LP2.cpp -o program
//TO RUN: mpirun --mca btl_vader_single_copy_mechanism none -n 6 program

#include <mpi.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <iomanip>
#include "json.hpp"

#define NUMBER_OF_CLIENTS 25
#define DATA_ARRAY_SIZE 10

using namespace std;
using json = nlohmann::json;

//KLIENTU KLASE
class Client
{
public:
	Client();
	Client(string name, int age, double cash);
	Client(string name, int age, double cash, int lastPrime);
    string toJSON();
    static Client fromJSON(string jsonString);

	string name;
	int age;
	double cash;
	int lastPrime;
};

Client::Client() {
	this->name = "";
	this->age = 0;
	this->cash = 0;
	this->lastPrime = 0;
}

Client::Client(string name, int age, double cash) {
	this->name = name;
	this->age = age;
	this->cash = cash;
	this->lastPrime = 0;
}

Client::Client(string name, int age, double cash, int lastPrime) {
	this->name = name;
	this->age = age;
	this->cash = cash;
	this->lastPrime = lastPrime;
}

string Client::toJSON(){
    json js;

    js["name"] = this->name;
    js["age"] = this->age;
    js["cash"] = this->cash;
    js["lastPrime"] = this->lastPrime;

    return js.dump();
}

Client Client::fromJSON(string jsonString){
    auto clientData = json::parse(jsonString);
    return Client(clientData["name"], clientData["age"], clientData["cash"], clientData["lastPrime"]);
}

//METODAI

//Nuskaityti duomenis
vector<Client> ReadData() {
	//Nuskaitome duomenis is failo i json struktura(objekta)
	std::ifstream i("IFF77_AlgirdasV_L1_dat_1.json");
	json j;
	i >> j;
	//Skaitysime duomenis is klientu saraso
	json js = j["clients"];

	//Skaitysime kiekvieno kliento duomenis atskirai ir juos irasysime i sarasa
	vector<Client> Clients;

	int ah = 0;
	for (auto& el: js.items())
	{
		json jso = js[ah++];
		Client *c = new Client(jso["name"], jso["age"], jso["cash"]);
		Clients.push_back(*c);
        delete(c);
	}

	return Clients;
}


//Patikrinti ar eilute baigiasi tam tikrais simboliais
bool endsWith(const string& str, const string& suffix)
{
	return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
}

//Ar atitinka kriteriju
bool MatchesCriteria(int num) {
	string s = to_string(num);
	if (endsWith(s, "3"))
	{
		return true;
	}
	return false;
}

//Suranda paskutini pirmini skaiciu pries duota numeri
int FindLastPrimeBeforeNumber(int num) {
	int lastPrime = 0;
	for (int i = 0; i < num; i++)
	{
		bool prime = true;
		for (int j = 2; j < i; j++)
		{
			if (i % j == 0)
			{
				prime = false;
			}
		}
		if (prime)
		{
			lastPrime = i;
		}
	}
	return lastPrime;
}

//MAIN METODAS
int main(){
    MPI::Init();
    auto processNumber = MPI::COMM_WORLD.Get_rank();    //Proceso skaicius
    auto numberOfProcesses = MPI::COMM_WORLD.Get_size();//Visu procesu skaicius

    //TAGS
    //1 - data to be put into data array/thread
    //2 - data requested from worker threads(sent from data thread towards worker threads) and communications
    //3 - data going from workers to result thread and communications between them
    //4 - data going from result thread to the main thread for writing to file and comms
    
    //Pagrindine gija
    if(processNumber == 0){
        //Nuskaitome duomenis ir sudedame i klientu objektu sarasa
	    vector<Client> Clients = ReadData();
        //Klientu rezultatu masyvas
        Client* ClientResults = new Client[NUMBER_OF_CLIENTS];

        string serialized;              //Kliento duomenys issaugoti json formatu string eiluteje
        int serializedLength;           //Eilutes ilgis
        const char* serializedCharArray;//Eilute issaugota char masyve

        //Siunciame klientu duomenis i duomenu gija
        for(Client c : Clients){
            serialized = c.toJSON();
            serializedLength = static_cast<int>(serialized.size());
            serializedCharArray = serialized.c_str();
            MPI::COMM_WORLD.Send(&serializedLength, 1, MPI::INT, 1, 1);
            MPI::COMM_WORLD.Send(serializedCharArray, serializedLength, MPI::CHAR, 1, 1);
        }
        serializedLength = 0;
        //Nusiunciama zinute kad baigta idedineti duomenis i duomenu masyva/gija
        MPI::COMM_WORLD.Send(&serializedLength, 1, MPI::INT, 1, 1);

        int messageSize = 0;
        //Is rezultatu gijos gaunamas rezultatu masyvo dydis
        MPI::COMM_WORLD.Recv(&messageSize, 1, MPI::INT, 2, 4);
        for(int i = 0; i < messageSize; i++){
            MPI::COMM_WORLD.Recv(&serializedLength, 1, MPI::INT, 2, 4);
            char clientData[serializedLength];
            MPI::COMM_WORLD.Recv(clientData, serializedLength, MPI::CHAR, 2, 4);
            ClientResults[i] = Client::fromJSON(string(clientData, 0, static_cast<unsigned long>(serializedLength)));
        }

        //PRINT DATA
        ofstream ofs;
        ofs.open("IFF77_VasiliauskasA_L1_rez.txt", ofstream::out | ofstream::trunc);
        ofs << "-------------------------------------------------" << endl;
        ofs << "| " << setw(20) << "Name" << " | " << setw(3) << "Age" << " | " << setw(7) << "Cash" << " | " << setw(6) << "Prime" << " |" << endl;
        ofs << "-------------------------------------------------" << endl;
        for (int i = 0; i < messageSize; i++)
        {
            ofs << "| " << setw(20) << ClientResults[i].name << " | " << setw(3) << ClientResults[i].age << " | " << setw(7) << ClientResults[i].cash << " | " << setw(6) << ClientResults[i].lastPrime << " |" << endl;
        }
        ofs << "-------------------------------------------------" << endl;
        ofs.close();
    }
    //Duomenu gija
    else if(processNumber == 1){
        Client Clients[DATA_ARRAY_SIZE];
        int numberOfClients = 0;
        bool ended = false;             //Ar pasibaige skaiciavimai
        MPI_Status status;              //Probe funkcijos metu grazinti zinutes meta duomenys
        int senderProcessNumber = 0;    //Siuntejo kuris atsiuncia zinute procesoriaus numeris
        int messageSize = 0;            //Gautos zinutes dydis
        string serialized;              //Kliento duomenys issaugoti json formatu string eiluteje
        int serializedLength;           //Eilutes ilgis
        const char* serializedCharArray;//Eilute issaugota char masyve

        while(true){
            if((numberOfClients == 0) && (ended == true)){
                //If all working threads did their jobs(no more data in data array and no more data to be added from main thread)
                messageSize = 0;
                for(int i = 3; i < numberOfProcesses; i++){
                    MPI::COMM_WORLD.Send(&messageSize, 1, MPI::INT, i, 2);
                }
                break;
            }
            else if(numberOfClients == 0){
                //Waiting for data to be inserted
                MPI_Probe(0, 1, MPI::COMM_WORLD, &status);
            }else if(numberOfClients == DATA_ARRAY_SIZE){
                //Waiting for data to be removed(array is full)
                MPI_Probe(MPI::ANY_SOURCE, 2, MPI::COMM_WORLD, &status);
            }else{
                //Waiting for any message(default, after specific cases)
                MPI_Probe(MPI::ANY_SOURCE, MPI::ANY_TAG, MPI::COMM_WORLD, &status);
            }

            senderProcessNumber = status.MPI_SOURCE;    //Nustatomas siuntejo proceso numeris
            //Jeigu atsiusta zinute is pagrindines gijos
            if(senderProcessNumber == 0){
                MPI::COMM_WORLD.Recv(&messageSize, 1, MPI::INT, 0, 1);
                //Jeigu atsiusta tuscia zinute
                if(messageSize == 0){
                    ended = true;
                }else{
                    char clientData[messageSize];
                    MPI::COMM_WORLD.Recv(clientData, messageSize, MPI::CHAR, 0, 1);
                    Client client = Client::fromJSON(string(clientData, 0, static_cast<unsigned long>(messageSize)));
                    Clients[numberOfClients++] = client;
                }
            }
            //Jeigu is kitos gijos(ne pagrindines)
            else{
                MPI::COMM_WORLD.Recv(&messageSize, 1, MPI::INT, MPI::ANY_SOURCE, 2);
                serialized = Clients[numberOfClients - 1].toJSON();
                serializedLength = static_cast<int>(serialized.size());
                serializedCharArray = serialized.c_str();
                MPI::COMM_WORLD.Send(&serializedLength, 1, MPI::INT, senderProcessNumber, 2);
                MPI::COMM_WORLD.Send(serializedCharArray, serializedLength, MPI::CHAR, senderProcessNumber, 2);
                numberOfClients--;
            }

        }
    }
    //Rezultatu gija
    else if(processNumber == 2){
        Client* Clients = new Client[NUMBER_OF_CLIENTS];
        Client* cl;
        int numberOfClients = 0;
        int workingThreads = numberOfProcesses - 3; //Kiek darbiniu giju dar dirba
        MPI_Status status;              //Probe funkcijos metu grazinti zinutes meta duomenys
        int messageSize = 0;            //Gautos zinutes dydis
        string serialized;              //Kliento duomenys issaugoti json formatu string eiluteje
        int serializedLength;           //Eilutes ilgis
        const char* serializedCharArray;//Eilute issaugota char masyve

        while(true){
            //If no more threads are computing results - end the program
            if(workingThreads == 0){
                break;
            }
            MPI_Probe(MPI::ANY_SOURCE, 3, MPI::COMM_WORLD, &status);
            MPI::COMM_WORLD.Recv(&messageSize, 1, MPI::INT, status.MPI_SOURCE, 3);
            //If working thread returns message with size 0 that means it has ended its job
            if(messageSize == 0){
                workingThreads--;
            }else{
                char clientData[messageSize];
                MPI::COMM_WORLD.Recv(clientData, messageSize, MPI::CHAR, status.MPI_SOURCE, 3);
                Client client = Client::fromJSON(string(clientData, 0, static_cast<unsigned long>(messageSize)));
                int i = 0;
                if (numberOfClients != 0)
                {
                    for (i = numberOfClients - 1; i >= 0; i--)
                    {
                        if (Clients[i].lastPrime < client.lastPrime)
                        {
                            Clients[i + 1] = *(new Client(Clients[i].name, Clients[i].age, Clients[i].cash, Clients[i].lastPrime));
                        }
                        else
                        {
                            break;
                        }
                    }
                    Clients[i + 1] = client;
                    numberOfClients++;
                }
                else
                {
                    Clients[i] = client;
                    numberOfClients = 1;
                }
            }
        }
        MPI::COMM_WORLD.Send(&numberOfClients, 1, MPI::INT, 0, 4);
        for(int i = 0; i < numberOfClients; i++){
            cl = &(Clients[i]);
            serialized = cl->toJSON();
            serializedLength = static_cast<int>(serialized.size());
            serializedCharArray = serialized.c_str();
            MPI::COMM_WORLD.Send(&serializedLength, 1, MPI::INT, 0, 4);
            MPI::COMM_WORLD.Send(serializedCharArray, serializedLength, MPI::CHAR, 0, 4);
        }
    }
    //Darbine gija
    else{
        int messageSize = 0;            //Gautos zinutes dydis
        string serialized;              //Kliento duomenys issaugoti json formatu string eiluteje
        int serializedLength;           //Eilutes ilgis
        const char* serializedCharArray;//Eilute issaugota char masyve

        while(true){
            //Message without any meaningful content - only the tag is meaningful to triger a request for data from data thread
            MPI::COMM_WORLD.Send(&messageSize, 1, MPI::INT, 1, 2);
            MPI::COMM_WORLD.Recv(&messageSize, 1, MPI::INT, 1, 2);

            //If there is no more data to be sent then stop the thread
            if(messageSize == 0){
                break;
            }
            char clientData[messageSize];
            MPI::COMM_WORLD.Recv(clientData, messageSize, MPI::CHAR, 1, 2);
            Client client = Client::fromJSON(string(clientData, 0, static_cast<unsigned long>(messageSize)));
            
            //Compute result
            int num = 0;
            for (char& c : client.name)
            {
                num += (int)c;
            }
            num = ((num % 150) * client.age * (int)floor(client.cash)) % 30000;

            int prime = FindLastPrimeBeforeNumber(num);
            client.lastPrime = prime;
            if (MatchesCriteria(prime))
            {
                serialized = client.toJSON();
                serializedLength = static_cast<int>(serialized.size());
                serializedCharArray = serialized.c_str();
                MPI::COMM_WORLD.Send(&serializedLength, 1, MPI::INT, 2, 3);
                MPI::COMM_WORLD.Send(serializedCharArray, serializedLength, MPI::CHAR, 2, 3);
            }

        }
        //Gija baige darba
        messageSize = 0;
        MPI::COMM_WORLD.Send(&messageSize, 1, MPI::INT, 2, 3);
    }
    MPI::Finalize();
}