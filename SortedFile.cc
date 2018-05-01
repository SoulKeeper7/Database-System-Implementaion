#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include <sstream>
#include "DBFile.h"
#include <fstream>
#include<stdlib.h>
//#inluce<stdio.h>

void *worker(void *arg) 
{

	Threadparameters *t = (Threadparameters *)arg;
	BigQ b_queue(*(t->in), *(t->out), (t->order), t->runLength);
}

int SortedFile::Close()
{
	if (mode == Write) {
		cout << "Switch Mode from write to read" << endl;
		mode = Read;
		inputpipe->ShutDown();
		
		merge();
		delete bigqinstance;
		delete inputpipe;
		delete outputpipe;
		inputpipe = NULL;
		outputpipe = NULL;
	}
	//else if (mode == Read) 
	{
		return genericFile->Close() ;
		
	}
}

void SortedFile::Add(Record & me)
{
	
	if (mode == Read)
	{
		Read_Write_Switch();
	}
	inputpipe->Insert(&me);
	//addcount++;
}

void SortedFile::Read_Write_Switch()
{
	if (mode == Read)
	{
		mode = Write;
		inputpipe = new Pipe(100);
		outputpipe = new Pipe(100);
		bigqinstance = new Threadparameters;
		bigqinstance->in = inputpipe;
		bigqinstance->out = outputpipe;
		bigqinstance->order = *myOrder;
		bigqinstance->runLength = runLength;
		pthread_create(&thread1, NULL, worker, (void*)bigqinstance);


	}
	else if (mode == Write) {		
		mode = Read;
		inputpipe->ShutDown();		
		merge();
		delete bigqinstance;
		delete inputpipe;
		delete outputpipe;
		inputpipe = NULL;
		outputpipe = NULL;
	}
}

void SortedFile::MoveFirst()
{
	if (mode == Write)
	{
		Read_Write_Switch();
	}
	genericFile->GetPage(read_page, cur_page = 0); // get the page 
	read_page->settherecord();
}



int SortedFile::GetNext(Record & fetchme)
{
	if (mode == Write)
	{
		Read_Write_Switch();
	}
	if (read_page->checkLength())
	{
		Record *x = read_page->gettherecord();
		fetchme.Copy(x);
		return 1;
	}
	else if (++cur_page <= genericFile->lastIndex())
	{
		//theFile->GetPage(curPage, curPageIdx);
		genericFile->GetPage(read_page, cur_page); // get the page 
		read_page->settherecord();
		Record *x = read_page->gettherecord();
		fetchme.Copy(x);
		return 1;
	}
	return 0;
}


int SortedFile::Open( char * fpath)
{

	char tbl_path[100];
	sprintf(tbl_path, "%s.meta", fpath);

	ifstream in(tbl_path);
	
	pathoffile = fpath;
	mode = Read;

	//Write myOrder ordermaker and runlength from meta data file
	string line;
	if (in.is_open()) {
		getline(in, line); //"sorted"
		getline(in, line); // runLength
		std::stringstream s_str(line);
		s_str >> runLength;
		myOrder->makeordermaker(in);
	}	
	//cout << " open in fileMode: " << mode << endl;

	genericFile->Open(1, fpath);
	
}

int SortedFile::Create( char * fpath, fType ftype, void * startup)
{
	
	pathoffile = fpath;
	mode = Read; //while Adding record, we are taking care of initial Write!

	Sortorderinfo *sortinfo;
	//cout << "Sorted DBFile Create called: " << fpath<<endl;
	char file_tbl_path[100];
	sprintf (file_tbl_path, "%s.meta", fpath);
	ofstream out(file_tbl_path);	
	out << "sorted" <<endl;
	sortinfo  = (Sortinfoobj *)startup;
	 runLength = sortinfo->runLength;	
	*myOrder = *(sortinfo->givenorder);
	out << runLength << endl;
	(sortinfo->givenorder);
	
	myOrder->FilePrint(out);
	if (FILE *file = fopen(fpath, "r"))
	{
		fclose(file);
		genericFile->Open(1, fpath);
		
	}
	else
	{
		genericFile->Open(0, fpath);
		
	}
	//delete 
		//open the file 
	//heapDB->Create(fpath, heap, NULL);// 
	return 1;
}

		
void SortedFile::Load(Schema & myschema, char * loadpath)
{
	if (mode == Write)
	{
		Read_Write_Switch();
	}
	Record temp_rec;
	FILE *filetable = fopen(loadpath, "r");

	if (filetable) {
		while (temp_rec.SuckNextRecord(&myschema, filetable) == 1) {
			Add(temp_rec);
		}
	}
}

void SortedFile::merge() {
	cout << "no oftimes insert called" << addcount << endl;
	inputpipe->ShutDown();
	Record fromFile, fromPipe;
	bool fileNotEmpty = true;
	if(genericFile->GetLength()==0 )
	{
		fileNotEmpty = false;
	}
	//bool fileNotEmpty = (genericFile->GetLength() == 0 && ;
	bool pipeNotEmpty = outputpipe->Remove(&fromPipe);
	//if(pipeNotEmpty)

	HeapFile *tmp =new HeapFile() ;
	char *a = "rand";
	tmp->Create(a, heap, NULL);  // temporary file will be renamed
	ComparisonEngine ce;

	// initializ->
	if (fileNotEmpty)
	{
		genericFile->GetPage(read_page, cur_page = 0); 		// move first
		read_page->settherecord();
		fileNotEmpty = GetNext(fromFile);
	}
	
	int count = 0;
	int newcount = 0;
	if((fileNotEmpty) && pipeNotEmpty)
	{

		// two-way merge
		while ((fileNotEmpty) && pipeNotEmpty)
		{
			if ((fileNotEmpty && pipeNotEmpty) && ce.Compare(&fromFile, &fromPipe, myOrder) > 0) {
				newcount++;
				tmp->Add(fromPipe);
				pipeNotEmpty = outputpipe->Remove(&fromPipe);
			}
			else if ((fileNotEmpty && pipeNotEmpty) && ce.Compare(&fromFile, &fromPipe, myOrder) <= 0)
			{
				count++;
				tmp->Add(fromFile);
				fileNotEmpty = GetNext(fromFile);
			}
		}
	}
	if (pipeNotEmpty)
	{
		while (pipeNotEmpty)
		{
			newcount++;
			tmp->Add(fromPipe);
			pipeNotEmpty = outputpipe->Remove(&fromPipe);
		}
		//tmp.Add(fromFile);

	}
	else if(fileNotEmpty)
	{
		while (fileNotEmpty)
		{
			count++;
			tmp->Add(fromFile);
			fileNotEmpty = GetNext(fromFile);		
		}
	}
	cout << count<<endl;
	cout << newcount<<endl;
	//memory issue happend beacuse of new page in add no issues here remember.
	//remove()
	//genericFile->Close();
	//remove(pathoffile);
	rename(a, pathoffile);
	outputpipe->ShutDown();

	//tmp->Close();
	//free(a);
	//free(tmp);
	//remove(tmp);
	//delete tmp;

	tmp->Close();
	//tmp.Close();;
	//delete  &tmp;
	//remove(tmp);
}

int SortedFile::GetNext(Record & fetchme, CNF & applyMe, Record & literal)
{
	if (mode == Write)
	{
		Read_Write_Switch();
	}

	ComparisonEngine engine;	
	OrderMaker searchquery;
	OrderMaker literalquery;
	switch (searchStatus)
	{
	case 0:
		return checkAndDoBinarySearch(applyMe, searchquery, literalquery, fetchme, engine, literal); 
		break;
	case 1:
		while (GetNext(fetchme) != 0)
		{
			//Always test CNF before returning 1
			if (engine.Compare(&fetchme, &literal, &applyMe) != 0)
				return 1;
			if (engine.Compare(&fetchme, &searchquery, &literal, &literalquery) > 0)
			{				
				searchStatus = 2;
				return 0;
			}
		}		
		return 0;
		break; 
	case 2:
		return 0; //as it is sorted need to look again
		break; 
	case 3:

		while (GetNext(fetchme) != 0)
		{
			//Always test CNF before returning 1
			if (engine.Compare(&fetchme, &literal, &applyMe) != 0)
				return 1;
		}		
		return 0;
		break;
	}
	return 0; 
}
int SortedFile::checkAndDoBinarySearch(CNF & applyMe, OrderMaker &searchquery, OrderMaker &literalquery, Record & fetchme, ComparisonEngine &engine, Record & literal)
{
	if (applyMe.queryOrderMaker(*myOrder, searchquery, literalquery) == 0)
	{
		//if(mateches) do sequwntial scan 
		searchStatus = 3;
		while (GetNext(fetchme) != 0)
		{
			
			if (engine.Compare(&fetchme, &literal, &applyMe) == 0)
				return 1;
			else
				return 0;
		}

		return 0;
	}
	else
	{
		//(check if binary seacrh possible)
		if (BinarySearch(literal, fetchme, searchquery, literalquery) == 0)
		{
			searchStatus = 2;
			return 0;
		}
		else
		{
			//iterate till ncomparison holds
			searchStatus = 1;

			do
			{
				//Always test CNF before returning 1
				if (engine.Compare(&fetchme, &literal, &applyMe) != 0)
				{
					//cout << "Suprising comapre returned 1";
					return 1;
				}
				if (engine.Compare(&fetchme, &searchquery, &literal, &literalquery) > 0)
				{
					//Our search has terminated, hence no need to go till the end
					searchStatus = 2;
					return 0;
				}
			} while (GetNext(fetchme) != 0);
			//No more records left
			return 0;
		}
	}

}

int SortedFile::BinarySearch(Record &literal, Record &fetchMe,OrderMaker searchquey, OrderMaker literalquery)
{
	ComparisonEngine engine;	
	int low = 0;
	int high = 0; ;

	if (genericFile->GetLength() == 0)
		high = -1;
	else
		 high =genericFile->GetLength() - 1;

	// find the starting page  to search from 

	while (high - low>1)
	{
		int mid = (low + high) / 2;
		if (read_page != NULL)
		{
			read_page->EmptyItOut();
			genericFile->GetPage(read_page, mid);
			read_page->settherecord();
		}
		else
		{
			read_page = new Page;
			genericFile->GetPage(read_page, mid);
			read_page->settherecord();

		}
		GetNext(fetchMe); // fetch the first record of the page
		//Record *x = read_page->gettherecord();
		//fetchme.Copy(x);

		//read_pagc,la,e->GemkcakmatFirstfafffetchMe);

		int res = engine.Compare(&fetchMe, &searchquey, &literal, &literalquery);

		if (res >= 0)	// if returned <=0 mean starting point is blow or equal to middle 
		{
			high = mid;
		}
		else		// if returned greater than means the starting point fromthe search must be abpove the middle
		{
			low = mid;
		}
	}

	/*if (low > 0)
	{
		read_page->EmptyItOut();
		genericFile->GetPage(read_page, low);// fetch the page to start searching from 
		read_page->settherecord();
	
		if (read_page->checkLength())
		{
			Record *x = read_page->gettherecord();
			fetchMe.Copy(x);
			//return 1;
			int res ;
			while (( res = engine.Compare(&fetchMe, &searchquey, &literal, &literalquery)) != 0)
			{
				if (res > 0)
				{
					continue;
					//cerr << " No record found\n"; // always look for query errors if nothing is found

					//return 0;
				}
				if (GetNext(fetchMe) == 0)
				{
					return 0;
				}
			}

		}

	}*/

	///// neeed to check the previous page?????? I guess so need some clearity 

	int res;
	int QuesrystartPage = low;
	
	read_page->EmptyItOut();// empty out the last filled pages to restart reading the data.

	if (genericFile->GetLength() > QuesrystartPage)
	{
		genericFile->GetPage(read_page, QuesrystartPage);// fetch the page to start searching from 
		read_page->settherecord();
	}
	else
	{
		//cout << "f lefth" << genericFile->GetLength() << endl;// probably something cab be fishy deb
	}

	if (GetNext(fetchMe) == 0) // fetch the first record
	{	
		return 0;
	}

	// compare and get the 
	while ((res = engine.Compare(&fetchMe, &searchquey, &literal, &literalquery)) != 0)
	{
		if (res > 0)
		{
			cerr << " No record found\n"; // always look for query errors if nothing is found
			
			return 0;
		}
		if (GetNext(fetchMe) == 0)
		{
			return 0;
		}
	}

	return 1;
}


SortedFile::SortedFile()
{
	cur_page = 0;
	outpipefile = new File();
	read_page = new Page();
	genericFile = new File();
	write_page = new Page();
	myOrder = new OrderMaker();
	heapDB = new HeapFile();
	addcount = 0;
	 searchStatus = 0;
}


SortedFile::~SortedFile()
{
	delete genericFile;
	delete read_page;
	delete write_page;
	//delete outpipefile;
	
}


