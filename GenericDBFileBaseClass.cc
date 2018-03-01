#include "GenericDBFileBaseClass.h"


#include "DBFile.h"
#include "HeapFile.h"




int GenericDBFileBaseClass::Create(const char* fpath, void* startup) {
	
	return 1;
}

int GenericDBFileBaseClass::Open(const char* fpath) {
	
	return 1;
}

void GenericDBFileBaseClass::Load(Schema& myschema, char* loadpath) {
	
}

int GenericDBFileBaseClass::GetNext(Record& fetchme) {

	return 1;
}

int GenericDBFileBaseClass::Close()
{
	return 1;
}

void GenericDBFileBaseClass::Add(Record& addme)
{

}

void GenericDBFileBaseClass::MoveFirst()
{

}

int GenericDBFileBaseClass::GetNext(Record& fetchme, CNF& cnf, Record& literal)
{
	return 1;
}
