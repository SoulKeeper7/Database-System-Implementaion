#include <iostream> 
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"

#include "HeapFile.h"

//HeapFile::HeapFile() {}
HeapFile::~HeapFile() {
	//cout << "DBFile DESTRUCTOR" << endl;
	//delete theFile;	
	//delete 	curPage;
	
}
HeapFile::HeapFile() {
	off_t curPageIdx=0;
	 curPage= new Page();
	 theFile = new File();
}

int HeapFile::Close() {	
	return theFile->Close();
}

void HeapFile::Add(Record& addme) {
	//startRead();
	//theFile.AddPage(&curPage, 0);
	int ret;
	ret = curPage->Append(&addme);// if posssible appnend the next page
	if (ret == 0) {
		//Could not fit in page; Add it to File
		int currlen = theFile->GetLength(); // get the file length
		int whichpage = currlen == 0 ? 0 : currlen - 1; // the page number to be added
		theFile->AddPage(curPage, whichpage);// add the page
		curPage->EmptyItOut();// clear records
		curPage->Append(&addme);// add records to the page
	}
	
}

void HeapFile::MoveFirst() {
	//startRead();// set the read mode

	theFile->GetPage(curPage, curPageIdx = 0); // get the page 
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	while (GetNext(fetchme))
		if (comp.Compare(&fetchme, &literal, &cnf)) return 1;   // Record matched return true
	return 0;  // no matching reco
}

void HeapFile::Load(Schema& myschema, char* loadpath)
{
	FILE* ifp = fopen(loadpath, "r");

	Record next;
	curPage->EmptyItOut();  // creates the first page
	int count;
	while (next.SuckNextRecord(&myschema, ifp))
	{
		Add(next);
		
	}
	int currlen = theFile->GetLength();
	int whichpage = currlen == 0 ? 0 : currlen - 1;
	theFile->AddPage(curPage, whichpage);
}
int HeapFile::GetNext(Record& fetchme) {
	while (!curPage->GetFirst(&fetchme)) {
		if (++curPageIdx > theFile->lastIndex()) return 0;  // no more records
		theFile->GetPage(curPage, curPageIdx);
	}
}
int HeapFile::Open(const char *f_path) {
	//todo:check again
	theFile->Open(1, f_path);
	return 1;
}

 int HeapFile::Create( const char* fpath, void* startup) {
	 theFile->Open(0, fpath);
	 return 1;
}