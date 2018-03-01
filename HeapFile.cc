#include <iostream> 
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"

#include "HeapFile.h"


HeapFile::~HeapFile() {
	
}
HeapFile::HeapFile() {
	off_t curPageIdx = 0;
	curPage = new Page();
	theFile = new File();
	tempPage= new Page();
	//Record newrec;
}

int HeapFile::Close() {
	return theFile->Close();
}

void HeapFile::Add(Record& addme) {
	
	int ret;
	//Schema x("catalog", "customer");
	//addme.Print(&x);
	/*ret = curPage->Append(&addme);// if posssible appnend the next page
	
	if (ret == 0) {
		//Could not fit in page; Add it to File
		int currlen = theFile->GetLength(); // get the file length
		int whichpage = currlen == 0 ? 0 : currlen - 1; // the page number to be added
		theFile->AddPage(curPage, whichpage);// add the page
		curPage->EmptyItOut();// clear records
		curPage->Append(&addme);// add records to the page
	}*/
			if (theFile->GetLength() == 0 ) 
			{
				theFile->AddPage(curPage, 0);
				theFile->GetPage(curPage, 0);
			}

		int result = curPage->Append(&addme);


		if (result == 0) {

			
			int currlen = theFile->GetLength(); // get the file length
			int whichpage = currlen == 0 ? 0 : currlen - 1; // the page number to be added
			curPage = new Page();
			theFile->AddPage(curPage, whichpage);
			theFile->GetPage(curPage, whichpage);
			curPage->Append(&addme);

		}
		int currlen = theFile->GetLength(); // get the file length
		int whichpage = currlen == 0 ? 0 : currlen - 1; // the page number to be added

		theFile->AddPage(curPage, whichpage-1);

}

void HeapFile::MoveFirst() 
{
	theFile->GetPage(curPage, curPageIdx = 0); // get the page 
	curPage->settherecord();
	//curPage
	//Record temp;
	//GetNext(temp);
	//GetFirstRecord(temp);

}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	//cout<< "getnext called\n";
	ComparisonEngine comp;
	while(GetNext(fetchme))
    if(comp.Compare(&fetchme, &literal, &cnf)) return 1;   // matched
  return 0;
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
	//int currlen = theFile->GetLength();
	//int whichpage = currlen == 0 ? 0 : currlen - 1;
	//theFile->AddPage(curPage, whichpage);
}
int HeapFile::GetNext(Record& fetchme) 
{
	//cout<<"enetered next\n";
	if (curPage->checkLength())
	{
		Record *x = curPage->gettherecord();
		fetchme.Copy(x);
		return 1;
	}
	else if(++curPageIdx <= theFile->lastIndex())
	{
		//theFile->GetPage(curPage, curPageIdx);
		theFile->GetPage(curPage, curPageIdx); // get the page 
		curPage->settherecord();
		Record *x = curPage->gettherecord();
		fetchme.Copy(x);
		return 1;
	}
	return 0;
	
}

void HeapFile::GetFirstRecord(Record& fetchme) {
	curPage->MoveToTheFirstRecord(&fetchme);
	
}
int HeapFile::Open(const char *f_path) {
	
	theFile->Open(1, f_path);
	return 1;
}

int HeapFile::Create(const char* fpath, void* startup) {
	theFile->Open(0, fpath);
	return 1;
}