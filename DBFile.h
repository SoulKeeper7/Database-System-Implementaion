#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFileBaseClass.h"

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

	};
#endif