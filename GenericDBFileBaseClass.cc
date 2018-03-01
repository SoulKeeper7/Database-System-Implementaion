#include "GenericDBFileBaseClass.h"


#include "DBFile.h"
#include "HeapFile.h"




int GenericDBFileBaseClass::Create(const char* fpath, void* startup) {
	//theFile.Open(0, fpath);
	return 1;
}

int GenericDBFileBaseClass::Open(const char* fpath) {
	//theFile->Open(1, fpath);
	return 1;
}

void GenericDBFileBaseClass::Load(Schema& myschema, char* loadpath) {
	//startWrite();
	
	//cout << count;
}

int GenericDBFileBaseClass::GetNext(Record& fetchme) {
	
	return 1;
}
//virtual int Create(const char* fpath, void* startup);
//virtual int Open(const char* fpath);
int GenericDBFileBaseClass::Close()
{
	return 1;
 }

void GenericDBFileBaseClass::Add(Record& addme)
{

 }
//virtual void Load(Schema& myschema, char* loadpath);

// this function does not deal with spanned records
void GenericDBFileBaseClass::MoveFirst()
{

 }
//virtual int GetNext(Record& fetchme);
int GenericDBFileBaseClass::GetNext(Record& fetchme, CNF& cnf, Record& literal)
{
	return 1;
 }
