#include <fstream>


#include "DBFile.h"
#include "HeapFile.h"
#include "GenericDBFileBaseClass.h"
//#include "SortedFile.h"

using std::string;
using std::ifstream;
using std::ofstream;

int DBFile::Create(const char* fpath, fType ftype, void* startup) {
	
	Genricdb = new HeapFile;
	return Genricdb->Create(fpath, startup);
}

int DBFile::Open(const char* fpath) {
	Genricdb = new HeapFile;
	return Genricdb->Open(fpath);
}


int DBFile::Close() {
	return Genricdb->Close();
}

void DBFile::Add(Record& addme) {
	return Genricdb->Add(addme);
}

void DBFile::Load(Schema& myschema, char* loadpath) {
	return Genricdb->Load(myschema, loadpath);
}

void DBFile::MoveFirst() {
	return Genricdb->MoveFirst();
}

int DBFile::GetNext(Record& fetchme) {
	return Genricdb->GetNext(fetchme);
}

int DBFile::GetNext(Record& fetchme, CNF& cnf, Record& literal) {
	return Genricdb->GetNext(fetchme, cnf, literal);
}

DBFile::DBFile() {}
DBFile::~DBFile() { }//delete Genricdb; }



