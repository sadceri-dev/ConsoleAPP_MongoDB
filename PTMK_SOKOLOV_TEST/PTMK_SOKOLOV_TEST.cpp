#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <string>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <ctime>
#include <chrono>



using namespace std;
using bsoncxx::builder::stream::document;

class Rabotnik {
public:
	string FIO;
	string birthDate;
	string gender;


	Rabotnik(const string& name, const string& date, const string& gen)
		:FIO(name), birthDate(date), gender(gen) {}


	int ageFromBirthDate() const {
		auto now = chrono::system_clock::now();
		time_t now_t = chrono::system_clock::to_time_t(now);
		tm nowTimeStruct;
		localtime_s(&nowTimeStruct, &now_t);

		int tekYear = nowTimeStruct.tm_year + 1970;
		int drYear = stoi(birthDate.substr(0, 4));

		return tekYear - drYear;
	}

	bsoncxx::document::value toDocument() const {
		bsoncxx::builder::stream::document document{};
		document	<< "FIO" << FIO
					<< "birthDate" << birthDate
					<< "gender" << gender
					<< "age" << ageFromBirthDate();
		return document << bsoncxx::builder::stream::finalize;
	}
};


class RabotnikiDatabase {
private:
	mongocxx::client client;
	mongocxx::collection rabotnik_db;

public:
	RabotnikiDatabase(const string& uri) //connection
		:client(mongocxx::uri{ uri }), rabotnik_db(client["rabotniki_db"]["rabotniki"]) {}

	void optimizedDB() {
		bsoncxx::builder::stream::document index_optimized;
		index_optimized << "gender" << 1 << "FIO" << 1;
		rabotnik_db.create_index(index_optimized.view());
		cout << "index created for optimized" << endl;
	}

	void newCollection() {
		mongocxx::database db = client["rabotniki_db"];
		db.create_collection("rabotniki");
		cout << "Collection 'rabotniki' sozdana" << endl;

	}

	void newRabotnik(const Rabotnik& emp) {
		rabotnik_db.insert_one(emp.toDocument().view());
		cout << "Rabotnik " << emp.FIO << " added successfully!" << endl;
	}

	void NewManyRabotnik(const vector<Rabotnik>& rabotniki) {
		vector<bsoncxx::document::value> documents;
		for (const auto& emp : rabotniki) {
			documents.push_back(emp.toDocument());
		}
		rabotnik_db.insert_many(documents);
		cout << rabotniki.size() << "rabotnikov dobavleno" << endl;
	}

	void allRabotnik() {
		auto cursor = rabotnik_db.find({});
		for (auto&& doc : cursor) {
			cout << bsoncxx::to_json(doc) << endl;
		}
	}

	void filteredByFandGender(const string& gender, const string& firstLetter) {	
		bsoncxx::builder::stream::document filter_builder{};
		filter_builder	<< "gender" << gender
						<< "FIO" << bsoncxx::builder::stream::open_document
						<< "$regex" << ("^" + firstLetter) //first letter
						<< bsoncxx::builder::stream::close_document;
		
		auto cursor = rabotnik_db.find(filter_builder.view());
		for (auto&& doc : cursor) {
			cout << bsoncxx::to_json(doc) << endl;
		}
	}

	void timeToFind(const string& gender, const string& firstLetter) {
		cout << "time before optimized" << endl;
		auto start = chrono::high_resolution_clock::now();
		filteredByFandGender(gender, firstLetter);
		auto end = chrono::high_resolution_clock::now();
		chrono::duration<double> timetoFind = end - start;
		chrono::duration<double> timetoFindBefore = end - start;
		cout << "time: " << timetoFindBefore.count() << endl;

		optimizedDB();

		cout << "time after optimized" << endl;
		start = chrono::high_resolution_clock::now();
		filteredByFandGender(gender, firstLetter);
		end = chrono::high_resolution_clock::now();
		timetoFind = end - start;
		cout << "time before: " << timetoFindBefore.count() << endl;
		cout << "time after: " << timetoFind.count() << endl;
	}

};

static void millionRabotnikovAndSpecial100(RabotnikiDatabase& db, int millionRabotnikov = 1000000, int special100 = 100) {
	string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	srand(time(0));
	
	vector<Rabotnik> rabotniki;
	
	for (int i = 0; i < millionRabotnikov; ++i) {
		string Familia(1, alphabet[rand() % 26]);
		string Name(5, alphabet[rand() % 26]);
		string Otchestvo(3, alphabet[rand() % 26]);
		string FIO = Familia + " " + Name + " " + Otchestvo;
		string birthDate = to_string(1970+rand() %50) + "-" + to_string(1 + rand() % 12) + "-" + to_string(1 + rand() % 28);
		string gender = (i % 2 == 0) ? "Male" : "Female";
		rabotniki.emplace_back(FIO, birthDate, gender);
	}

	db.NewManyRabotnik(rabotniki);

	vector<Rabotnik> specialRabotniki;
	for (int i = 0; i < 100; ++i) {
		string FIO = "F" + to_string(rand() % 100) + " Oleg";
		string birthDate = to_string(1970 + rand() % 50) + "-" + to_string(1 + rand() % 12) + "-" + to_string(1 + rand() % 28);
		string gender = "Male";
		specialRabotniki.emplace_back(FIO, birthDate, gender);
	}

	db.NewManyRabotnik(specialRabotniki);
}


int main(int argc, char* argv[]) {
	
	mongocxx::instance instance{};
	string uri = "mongodb://127.0.0.1:27017";
	RabotnikiDatabase db(uri);

	int mode = stoi(argv[1]);

	switch (mode) {
		case 1: {
			db.newCollection();
			break;
		}
		case 2: {
			if (argc != 5) {
				cout << "Usage minimum 5 arguments: FIO, birthDate, gender and 2 in start programm_name + code of mode" << endl;
				return 1;
			}
			string FIO = argv[2];
			string birthDate = argv[3];
			string gender = argv[4];

			Rabotnik emp(FIO, birthDate, gender);
			db.newRabotnik(emp);
			break;
		}
		case 3: {
			db.allRabotnik();
			break;
		}
		case 4: {
			millionRabotnikovAndSpecial100(db);
			break;
		}
		case 5: {
			db.timeToFind("Male", "F");
			break;
		}
		default: {
			cout << "Use argument i know" << endl;
			return 1;
		}

	}
	
	return 0;
}