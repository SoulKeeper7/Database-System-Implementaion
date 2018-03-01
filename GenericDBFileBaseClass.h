#ifndef GENERICDBFILEBASECLASS_H
#define GENERICDBFILEBASECLASS_H
#include <string>

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
typedef enum { heap, sorted, tree } fType;
class GenericDBFileBaseClass
{
public:
	GenericDBFileBaseClass() {};
	~GenericDBFileBaseClass() {};



	virtual int Create(const char* fpath, void* startup);
	virtual int Open(const char* fpath);
	virtual int Close() = 0;

	virtual void Add(Record& addme) = 0;
	virtual void Load(Schema& myschema, char* loadpath);

	// this function does not deal with spanned records
	virtual void MoveFirst() = 0;
	virtual int GetNext(Record& fetchme);
	virtual int GetNext(Record& fetchme, CNF& cnf, Record& literal) = 0;

	
};
#endif

