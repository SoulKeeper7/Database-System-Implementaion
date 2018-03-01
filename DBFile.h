#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFileBaseClass.h"

//typedef enum { heap, sorted, tree } fType;

//class GenericDBFileBaseClass;

/* This is the public (handle) interface
* should not be inherited.
*/
class DBFile {
private:
	GenericDBFileBaseClass * Genricdb;

public:
	DBFile();
	~DBFile();

	int Create(const char* fpath, fType ftype, void* startup);
	int Open(const char* fpath);
	int Close();

	void Add(Record& addme);
	void Load(Schema& myschema, char* loadpath);

	void MoveFirst();
	int GetNext(Record& fetchme);
	int GetNext(Record& fetchme, CNF& cnf, Record& literal);

//private:
	//GenericDBFileBaseClass * db;


	//void createFile(fType ftype);

	//DBFile(const DBFile&);
	//DBFile& operator=(const DBFile&);
};


/* for internal use only */
// TODO: empty the current page before starting add.
// be careful when switching from read mode to write mode, or vice versa (in heap files)
#endif