#pragma once
#include "ComparisonEngine.h"
#include "GenericDBFileBaseClass.h"
#include "Pipe.h"
#include "HeapFile.h"
#include "DBFile.h"
#include "BigQ.h"

typedef enum { Read, Write } ReadWriteMode;

typedef struct Sortorderinfo 
{
		OrderMaker *givenorder;
		int runLength;
}Sortinfoobj;


class  SortedFile : virtual public GenericDBFileBaseClass
{

private:

	Page * outpipe_write_page;
	char* pathoffile;
	File *outpipefile; //sortedfile
	File *genericFile;
	Page *write_page; //Record writes go into this page
	Page *read_page;  //This page is only for reading
	int cur_page;     //Current Page being read. 0 means no pages to read
	bool dirty;       //If true, current page being read is dirty(Not yet written to disk). 
	fType type;
	ReadWriteMode mode;
	
	Pipe *inputpipe;
	Pipe *outputpipe;
	OrderMaker *myOrder;
	int runLength;
	HeapFile *heapDB;
	Threadparameters* bigqinstance;
	pthread_t thread1;
	int addcount ;
	int searchStatus;


public:
	
	int Close();
	void Add(Record& me);

	void Read_Write_Switch();

	//int BinarySearch(Record &literal, Record &fetchMe);
	void MoveFirst();
	int GetNext(Record& fetchme, CNF& cnf, Record& literal);
	int checkAndDoBinarySearch(CNF & applyMe, OrderMaker &searchquery, OrderMaker &literalquery, Record & fetchme, ComparisonEngine &engine, Record & literal);
	
	int BinarySearch(Record & literal, Record & fetchMe, OrderMaker searchquey, OrderMaker literalquery);
	int GetNext(Record& fetchme);

	//void GetFirstRecord(Record & fetchme);

	int Open( char * fpath);

	int Create( char * fpath, fType ftype,void * startup);


	void Load(Schema & myschema, char * loadpath);
	//void NewFunction()
	SortedFile();
	~SortedFile();
	void merge();
};

