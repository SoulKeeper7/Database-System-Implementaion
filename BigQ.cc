#include "BigQ.h"
#include <vector>
#include<algorithm>
#include <pthread.h>
#include"Schema.h"
#include <queue>
#include <cmath>
#include <map>
#include<stdlib.h>

void writeToDisk(std::vector<Record *> &recordsVector, Page &sortPage, File &myFile, int &runlength);
void* worker_Thread(void* arg)
{

	Threadparameters *info = new Threadparameters();
	info = (Threadparameters*)arg;
	Pipe *in = info->in;
	Pipe *out = info->out;
	OrderMaker sortorder = info->order;
	int runlen = info->runLength;
	File myFile = info->myFile;
	char temp_file[100];
	sprintf(temp_file, "temp%d.bin", rand());
	myFile.Open(0, temp_file);

	map<int, Page*> overflow;
	vector<int> Recordsperrun;
	Record myRecord;
	Page myPage;
	Page sortPage;
	int pageCounter = 0;	
	std::vector<Record *> recordsVector;
	std::vector<int> pageeachinRun;

	int runlength = 0;
	int runscounter = 0;
	int counc = 0;
	while (in->Remove(&myRecord))
	{
		counc++;
		///removed the record
		Record *recordcopy = new Record;
		recordcopy->Copy(&myRecord);

		if (myPage.Append(&myRecord) && pageCounter != runlen) // if it is appendable to the page add the record
		{
			recordsVector.push_back(recordcopy);

		}
		else
		{
			if (++pageCounter == runlen)
			{
				stable_sort(recordsVector.begin(), recordsVector.end(), Sort_Run(&sortorder));
				Recordsperrun.push_back(recordsVector.size());				
				writeToDisk(recordsVector, sortPage, myFile, runlength);
				pageCounter = 0;
				recordsVector.clear();
				myPage.EmptyItOut(); ///empty out the page
				myPage.Append(&myRecord);// append the record
				recordsVector.push_back(recordcopy);// push it to vector		
				pageeachinRun.push_back(runlength);
				runlength = 0;
			}
			else //if ( pageCounter< runlen)	// check if the page is appendable		
			{
				myPage.EmptyItOut(); ///empty out the page
				myPage.Append(&myRecord);// append the record
				recordsVector.push_back(recordcopy);// push it to vector

			}

		}
		recordcopy = NULL;
		delete recordcopy;
		//myRecord = NULL;

	}

	if (recordsVector.size()>0)
	{
		Recordsperrun.push_back(recordsVector.size());
		stable_sort(recordsVector.begin(), recordsVector.end(), Sort_Run(&sortorder));
		writeToDisk(recordsVector, sortPage, myFile, runlength);
		pageeachinRun.push_back(runlength);
		runlength = 0;
		pageCounter = 0;
		recordsVector.clear();

		myFile.Close();
	}


	myFile.Open(1, temp_file);
	int Noofpages = myFile.GetLength();
	int  runscount;

	if (Noofpages != 0)
	{
		runscount = Recordsperrun.size();
	}
	else
	{
		runscount = 0;
	}

	priority_queue<Record*, vector<Record*>, Sort_Merge> p_queue(&sortorder);

	map<Record*, int> m_record;

	int* page_index = new int[runscount];


	Page** page_array = new (std::nothrow) Page*[runscount];


	int page_num = 0;

	


	for (int i = 0; i<runscount; i++) {
		page_array[i] = new Page();
		myFile.GetPage(page_array[i], page_num);

		Record* r = new  Record();
		page_array[i]->GetFirst(r);
		p_queue.push(r);
		m_record[r] = i;
		r = NULL;
		page_index[i] = page_num;
		page_num += pageeachinRun[i];


	}
	int x = 0;
	while (!p_queue.empty())
	{

		Record * newrecord = p_queue.top();
		p_queue.pop();

		int Run_no_of_poped_record = m_record[newrecord];
		Recordsperrun[Run_no_of_poped_record] = --Recordsperrun[Run_no_of_poped_record];
		m_record.erase(newrecord);
		Record* next = new Record;

		if (Recordsperrun[Run_no_of_poped_record] >0)
		{
			if (!(page_array[Run_no_of_poped_record]->GetFirst(next)))
			{
				
				page_array[Run_no_of_poped_record] = new Page();
				page_index[Run_no_of_poped_record] = page_index[Run_no_of_poped_record] + 1;
				myFile.GetPage(page_array[Run_no_of_poped_record], page_index[Run_no_of_poped_record]);
				page_array[Run_no_of_poped_record]->GetFirst(next);
				
			}
			p_queue.push(next);
			m_record[next] = Run_no_of_poped_record;


		}
		x++;
		out->Insert(newrecord);
		newrecord = NULL;
		delete newrecord;

	}

	//int j = 0;
	/*for (int i = 0; i < runscount; i++)
	{
		page_array[i]->EmptyItOut();
	}*/
	/*while (page_array[j])
	{
		page_array[j]->EmptyItOut();
		j++;
	}*/
	
	//cout << "inputputqueue" << counc << endl;
	//cout << "outputqueue" << x << endl;;
	myFile.Close();
	remove(temp_file); //Deleting the temp file
	out->ShutDown();
	//priority_queue<Record*, vector<Record*>, Sort_Merge> p_queue(&sortorder);
	 //delete myFile;
	//page_array.clear();

	//int* page_index = new int[runscount];






}

void writeToDisk(std::vector<Record *> &recordsVector, Page &sortPage, File &myFile, int &runlength)
{
	for (int i = 0; i<recordsVector.size(); i++)
	{
		if (!sortPage.Append(recordsVector[i]))
		{

			int currlen = myFile.GetLength();
			int whichpage = currlen == 0 ? 0 : currlen - 1;
			myFile.AddPage(&sortPage, whichpage);
			sortPage.EmptyItOut();	
			//delete &sortPage;
			//Record x;
			//x.Copy(recordsVector[i]);
			sortPage.Append(recordsVector[i]);
			runlength++;
			

		}

	}

	if (sortPage.GetNumRecs()>0)
	{
		int currlen = myFile.GetLength();
		int whichpage = currlen == 0 ? 0 : currlen - 1;
		myFile.AddPage(&sortPage, whichpage);
		sortPage.EmptyItOut();
		runlength++;
	}

	for (int i = 0; i < recordsVector.size(); i++)
	{
		delete recordsVector[i];
	}
	
}




BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	Threadparameters *info = new Threadparameters();
	//cout << "-------->" << &in << "---------->";
	info->in = &in;
	info->out = &out;
	info->order = sortorder;
	info->runLength = runlen;
	
	if (info->runLength <= 0)
	{
		out.ShutDown();
	}
	
	pthread_t Worker_Thread;

	//cout << "\ninitialting worker thread\n";
	int ret = pthread_create(&Worker_Thread, NULL, worker_Thread, (void*)info);
	//pthread_join(Worker_Thread,NULL);

}






BigQ::~BigQ()
{
	//myFile.clear();
}


