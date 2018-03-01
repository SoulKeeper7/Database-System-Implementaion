#ifndef HEAP_FILE_H
#define HEAP_FILE_H

#include "DBFile.h"
#include "GenericDBFileBaseClass.h"

class HeapFile : virtual public GenericDBFileBaseClass {
	//friend class DBFile;
	//friend class SortedFile;   // sort file used a temp heap file for merging
	//using GenericDBFileBaseClass::GetNext;

private:
	File * theFile;
	Page *curPage; //Record writes go into this page
	//Page *read_page;  //This page is only for reading
	int curPageIdx;     //Current Page being read. 0 means no pages to read
	//bool dirty;       //If true, current page being read is dirty(Not yet written to disk). 
	fType type;
public:
	HeapFile();
	~HeapFile() ;

	int Close();
	void Add(Record& me);

	void MoveFirst();
	int GetNext(Record& fetchme, CNF& cnf, Record& literal);
	int GetNext(Record& fetchme);

	int Open(const char * f_path);

	int Create( const char * fpath, void * startup);


	void Load(Schema & myschema, char * loadpath);

//protected:
	//void startWrite(); //{ mode = WRITE; }
	//void startRead();

/*private:
	void addtoNewPage(Record& rec) {
		curPage->EmptyItOut();
		curPage->Append(&rec);
	}*/

	//HeapFile(const HeapFile&);
	//HeapFile& operator=(const HeapFile&);
};

/*inline void HeapFile::startRead() {
	//if (mode == READ) return;
	//mode = READ;
	 theFile.AddPage(&curPage,0);
}*/

#endif