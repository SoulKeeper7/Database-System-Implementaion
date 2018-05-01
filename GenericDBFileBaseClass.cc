#include "GenericDBFileBaseClass.h"


#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"




int GenericDBFileBaseClass::Create( char* fpath,fType ftype, void* startup) {
	
	return 1;
}

int GenericDBFileBaseClass::Open( char* fpath) {
	
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
