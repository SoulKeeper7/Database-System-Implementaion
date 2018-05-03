#include "RelOp.h"
#include "BigQ.h"
#include <vector>

using namespace std;
void* run_Thread(void* arg)
{

	Threadpara *info= new Threadpara();
	info = (Threadpara*)arg;
		
	Record fetchme;
	ComparisonEngine comp;
	int vount = 0;
	info->mydbfile->MoveFirst();
	while (info->mydbfile->GetNext(fetchme))
	{
		
		if (comp.Compare(&fetchme, info->fethme, info->mycnf))
		{
			vount++;
			info->out->Insert(&fetchme);
		}
	}	
	info->out->ShutDown();
	//std::cout << "no of records scanned" << vount;;
}






void * project_run_thread(void *arg)
{
	Threadpara *info= new Threadpara();
	info = (Threadpara*)arg;
	
	Record inpiRecord;

	while (info->in->Remove(&inpiRecord))
	{
		inpiRecord.Project(info->keepme, info->numAttsOutput, info->numAttsInput);
		info->out->Insert(&inpiRecord);
	}

	info->out->ShutDown();
}
void *pipe_run_thread(void*arg)
{
	Threadpara *info = new Threadpara();
	info = (Threadpara*)arg;
	
	Record inPiRecord;
	
	ComparisonEngine comp;
	while (info->in->Remove(&inPiRecord))
	{
		if (comp.Compare(&inPiRecord, info->fethme,info->mycnf))
		{
			info->out->Insert(&inPiRecord);
		}
	}
	info->out->ShutDown();
}
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
	Threadpara *param=new Threadpara();
	param->mydbfile = &inFile;
	param->out = &outPipe;
	param->mycnf = &selOp;
	param->fethme = &literal;
	
	//cout << "\ninitialting Selectfile Run thread\n";
	pthread_create(&Run_Thread, NULL, run_Thread, (void*)param);	
}

void SelectFile::WaitUntilDone () 
{
	//cout << "\closing Selectfile Run thread\n";
	pthread_join(Run_Thread, NULL);
	//cout << "\closing Selectfile Run thread\n";
}

void SelectFile::Use_n_Pages (int runlen) 
{
	
}

void SelectPipe::Run(Pipe & inPipe, Pipe & outPipe, CNF & selOp, Record & literal)
{
	
	Threadpara *param = new Threadpara();
	param->in = &inPipe;
	param->out = &outPipe;
	param->mycnf = &selOp;
	param->fethme = &literal;
	//pthread_t Run_Thread;

	//cout << "\ninitialting Run thread\n";

	pthread_create(&Run_Thread, NULL, pipe_run_thread, (void*)param);

		

}

void SelectPipe::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}

void SelectPipe::Use_n_Pages(int n)
{
}

void Use_n_Pages(int n)
{
}

void Project::Run(Pipe & inPipe, Pipe & outPipe, int * keepMe, int numAttsInput, int numAttsOutput)
{
	Threadpara *param=new Threadpara();
	param->in = &inPipe;
	param->out = &outPipe;
	param->keepme = keepMe;
	param->numAttsInput = numAttsInput;
	param->numAttsOutput = numAttsOutput;
	pthread_create(&Run_Thread, NULL, project_run_thread, (void*)param);
}

void Project::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}

void Project::Use_n_Pages(int n)
{
}

void * join_run_thread(void *arg)
{
	Threadpara *info = new Threadpara();
	info = (Threadpara*)arg;
	Pipe *outL = new Pipe(100);
	Pipe * outR = new Pipe(100);
	Pipe *inL = info->inL;
	Pipe *inR = info->inR;
	Pipe *out = info->out;
	Record *fetchme = info->fethme;
	CNF *mycnf = info->mycnf;

	OrderMaker forLeft, forRight;


	int canUsemMrgeSort = mycnf->GetSortOrders(forLeft, forRight);

	//sort the first file
	BigQ Workffff(*inL, *outL, forLeft, info->runLength > 0 ? info->runLength : 1);

	//sort the next 
	BigQ Worker2(*inR, *outR, forRight, info->runLength > 0 ? info->runLength : 1);

	Record fromleft;
	Record fromright;

	int count = 0;
	bool pipeNotEmptyR = outR->Remove(&fromright);
	bool pipeNotEmptyL = outL->Remove(&fromleft);

	int numAttsLeft = ((((int*)(fromleft.bits))[1]) / sizeof(int)) - 1;
	int numAttsRight = ((((int*)(fromright.bits))[1]) / sizeof(int)) - 1;
	int numtokeep = numAttsLeft + numAttsRight;

	int *attsToKeep = new int[numtokeep];
	int k;
	for (k = 0; k < numAttsLeft; k++)
	{
		attsToKeep[k] = k;
	}

	int startOfRight = k;

	for (int l = 0; l < numAttsRight; l++, k++)
	{
		attsToKeep[k] = l;
	}
	if (canUsemMrgeSort)
	{
		ComparisonEngine ce;
		if ((pipeNotEmptyL) && pipeNotEmptyR)
		{

			while ((pipeNotEmptyL) && pipeNotEmptyR)
			{
				if ((pipeNotEmptyL && pipeNotEmptyR) && ce.Compare(&fromleft, &forLeft, &fromright, &forRight) < 0)//(&fromFile, &fromPipe, myOrder) > 0) {
				{
					
					pipeNotEmptyL = outL->Remove(&fromleft);
				}
				else if ((pipeNotEmptyL && pipeNotEmptyR) && ce.Compare(&fromleft, &forLeft, &fromright, &forRight) > 0)
				{
					pipeNotEmptyR = outR->Remove(&fromright);
				}
				else if ((pipeNotEmptyL && pipeNotEmptyR) && ce.Compare(&fromleft, &forLeft, &fromright, &forRight) == 0)
				{
					vector <Record*> rec_vector;

					while (pipeNotEmptyL == true && (ce.Compare(&fromleft, &forLeft, &fromright, &forRight) == 0))
					{

						rec_vector.push_back(new Record());
						rec_vector.back()->Copy(&fromleft);
						pipeNotEmptyL = outL->Remove(&fromleft);
					}

					while (ce.Compare(rec_vector[0], &forLeft, &fromright, &forRight) == 0)
					{
						for (int i = 0; i < rec_vector.size(); i++)
						{

							if (ce.Compare(rec_vector[i], &forLeft, &fromright, &forRight) == 0)
							{
								Record MergeRecord;
								Record y;
								y.Copy(new Record(fromright));
								MergeRecord.MergeRecords(rec_vector[i], &y, numAttsLeft, numAttsRight, attsToKeep, numtokeep, startOfRight);
								out->Insert(&MergeRecord);
								

								count = count + 1;
								if (count == 800000)
								{
									cout << count;
								}

							}
						}
						pipeNotEmptyR = outR->Remove(&fromright);
						if (!pipeNotEmptyR)
							break;

					}
					for (int i = 0; i < rec_vector.size(); i++)
					{
						delete rec_vector[i];
						rec_vector[i] = NULL;
					}
					rec_vector.clear();


					//fromleft.Copy(&orignalleft);
					//pipeNotEmptyL = outL->Remove(&fromleft);

				}



			}

			while (pipeNotEmptyL)
			{
				pipeNotEmptyL = outL->Remove(&fromleft);
			}
			while (pipeNotEmptyR)
			{
				pipeNotEmptyR = outR->Remove(&fromright);
			}
		}

	}
	else
	{
		cout << "amit" << endl;
		std::vector <Record*> rec_vector1;
		std::vector <Record*> rec_vector2;
		while (pipeNotEmptyL)
		{
			rec_vector1.push_back(&fromleft);
			pipeNotEmptyL = outL->Remove(&fromleft);
		}

		while (pipeNotEmptyR)
		{
			rec_vector2.push_back(&fromright);
			pipeNotEmptyR = outR->Remove(&fromright);
		}

		ComparisonEngine ce;
		for (int i = 0; i < rec_vector1.size(); i++)
		{
			for (int j = 0; j < rec_vector2.size(); j++)
			{
				if (ce.Compare(rec_vector1[i], rec_vector2[j], fetchme, mycnf))
				{
					Record MergeRecord;
					MergeRecord.MergeRecords(rec_vector1[i], rec_vector2[j], numAttsLeft, numAttsRight, attsToKeep, numtokeep, startOfRight);
					out->Insert(&MergeRecord);
				}
			}
		}
	}
	

	outL->ShutDown();
	outR->ShutDown();
	
	out->ShutDown();
}
void Join::Run(Pipe & inPipeL, Pipe & inPipeR, Pipe & outPipe, CNF & selOp, Record & literal)
{
	Threadpara *param= new Threadpara();
	param->inL = &inPipeL;
	param->inR = &inPipeR;
	param->out = &outPipe;
	param->mycnf = &selOp;
	param->fethme = &literal;	
	param->runLength = RunPagecount;
	pthread_create(&Run_Thread, NULL, join_run_thread, (void*)param);

}

void Join::WaitUntilDone()
{
	//cout << "\closing join Run thread\n";
	pthread_join(Run_Thread, NULL);
	//cout << "\closing join Run thread\n";
}


void  *DupRemove_run_thread(void *arg)
{
	Threadpara *info= new Threadpara();
	info = (Threadpara*)arg;
	Pipe *out = info->out;
	Pipe  *in = info->in;
	Schema * myschems = info->mysema;

	OrderMaker myOrder(myschems);
	Pipe *out1 = new Pipe(100);

	//call BigQclass;
	BigQ DupRemoval_biQ(*in, *out1, myOrder, 1);

	Record current;
	Record prev;

	out1->Remove(&current);
	prev.Copy(&current);
	out->Insert(&current);
	ComparisonEngine comp;
	int count = 1;
	while (out1->Remove(&current))
	{
		if (comp.Compare(&current, &prev,&myOrder))
		{
			count++;
			prev.Copy(&current);
			out->Insert(&current);
		}
	}
	
	in->ShutDown();
	out1->ShutDown();	
	out->ShutDown();
}


void DuplicateRemoval::Run(Pipe & inPipe, Pipe & outPipe, Schema & mySchema)
{
	Threadpara *param = new Threadpara();
	param->in = &inPipe;	
	param->out = &outPipe;
	param->mysema = &mySchema;	
	pthread_create(&Run_Thread, NULL, DupRemove_run_thread, (void*)param);

}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}

void  *Sum_run_thread(void *arg)
{
	Threadpara *info = new Threadpara();
	info = (Threadpara*)arg;
	Pipe * in = info->in;
	Pipe * out = info->out;
	Function * computeme = info->computeMe;
	int intsum = 0;
	int intres;
	double  doublesum = 0;
	double doubleres;
	Record temp;
	Attribute myAtt;
	myAtt.name = "SUM";
	int count = 0;
	while (in->Remove(&temp))
	{
		count++;
		if (computeme->Apply(temp, intres, doubleres) == Int)
		{		
			myAtt.myType = Int;			
			intsum += intres;
		}
		else
		{			
			myAtt.myType = Double;
			doublesum += doubleres;
		}
	}

	cout << count;
	Schema out_sch("sum_schema", 1, &myAtt);
	char buffer[100];
	myAtt.myType==Int? sprintf(buffer, "%d|", intsum): sprintf(buffer, "%f|", doublesum);	
	
	Record newRec;
	newRec.ComposeRecord(&out_sch, (const char *)&buffer[0]);
	//newRec.Print(&out_sch);
	out->Insert(&newRec);
	out->ShutDown();
	
	
}
void Sum::Run(Pipe & inPipe, Pipe & outPipe, Function & computeMe)
{
	Threadpara *param = new Threadpara();
	param->in = &inPipe;
	param->out = &outPipe;
	param->computeMe = &computeMe;
	pthread_create(&Run_Thread, NULL, Sum_run_thread, (void*)param);
	//cout << "\ninitialting sum Run thread\n";
}

void Sum::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}

void  *WriteOut_run_thread(void *arg)
{
	Threadpara *info = new Threadpara();
	info = (Threadpara*)arg;
	
	Record inpiperec;
	int count = 0;
	while (info->in->Remove(&inpiperec))
	{
		inpiperec.FilePrint(info->IOFILE,info->mysema);
	}
	info->in->ShutDown();
	//std::cout << count;
	
}


void WriteOut::Run(Pipe & inPipe, FILE * outFile, Schema & mySchema)
{
	Threadpara *param = new Threadpara();
	param->in = &inPipe;
	param->IOFILE = outFile;
	param->mysema = &mySchema;
	pthread_create(&Run_Thread, NULL, WriteOut_run_thread, (void*)param);
}

void WriteOut::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}


void  *Groupby_run_thread(void *arg)
{
	Threadpara *info = new Threadpara();
	info = (Threadpara*)arg;
	Pipe * in = info->in;
	Pipe * out = info->out;
	Function * computeme = info->computeMe;
	OrderMaker groupAtts = info->order;

	///creating a bigQ to sort on the basis of groupby attributes
	Pipe *outBigQ = new Pipe(100);
	BigQ b_myqueue(*in, *outBigQ, groupAtts, 1);

	int AttsList[MAX_ANDS];
	groupAtts.GetAttsList(AttsList);
	int numAttsToKeep = groupAtts.GetNumAtts();

	int intsum = 0;
	int intres;
	double  doublesum = 0;
	double doubleres;
	Record temp;
	Attribute myAtt;

	int *NewAttsList = new int[100];
	NewAttsList[0] = 0;
	//Fill the Atts list needed for the Merged Records
	for (int i = 1; i< numAttsToKeep + 1; i++) 
	{
			NewAttsList[i] = AttsList[i - 1];		
	}
	Record current;
	Record previous;

	Attribute sumAtt;
	int numAttsNow;
	sumAtt.name = "SUM";
	ComparisonEngine comp;
	char buffer[100];
	outBigQ->Remove(&current);
	if (computeme->Apply(current, intres, doubleres)==Int)
	{
		sumAtt.myType = Int;
		intsum = intres;
	}
	else
	{
		sumAtt.myType = Double;
		doublesum = doubleres;
	}
	numAttsNow = current.GetNumAtts();
	previous.Consume(&current);
	Schema out_sch("sum_schema", 1, &sumAtt);
	
	

	
	while (outBigQ->Remove(&current))	
	{		
		computeme->Apply(current, intres, doubleres);
		//current.Print();
		if (comp.Compare(&previous, &current, &groupAtts) == 0)
		{
			if (sumAtt.myType == Int)
			{
				intsum += intres;
			}
			else if (sumAtt.myType == Double) 
			{
				doublesum += doubleres;
			}
		}
		else
		{	
			
			if (sumAtt.myType == Int)
			{
				sprintf(buffer, "%d|", intsum);
				intsum = intres;
			}
			else if (sumAtt.myType == Double)
			{
				sprintf(buffer, "%f|", doublesum);
				doublesum = doubleres;
			}
			Record newrec;
			Record MergeRec;

			newrec.ComposeRecord(&out_sch, (const char *)&buffer[0]);			
			MergeRec.MergeRecords(new Record(newrec),new Record(previous), 1, numAttsToKeep, NewAttsList, (numAttsToKeep + 1), 1);
			out->Insert(&MergeRec);
			
				
		}
		previous.Consume(&current);


	}



	///
	Record MergeRec;
	Record newrec;

	if (sumAtt.myType == Int)
	{
		sprintf(buffer, "%d|", intsum);
		intsum = intres;
	}
	else if (sumAtt.myType == Double)
	{
		sprintf(buffer, "%f|", doublesum);
		doublesum = doubleres;
	}
	newrec.ComposeRecord(&out_sch, (const char *)&buffer[0]);		
	MergeRec.MergeRecords(new Record(newrec), new Record(previous), 1, numAttsToKeep, NewAttsList, numAttsToKeep + 1, 1);
	out->Insert(&MergeRec);
	
	out->ShutDown();


}
void GroupBy::Run(Pipe & inPipe, Pipe & outPipe, OrderMaker & groupAtts, Function & computeMe)
{
	Threadpara *param = new Threadpara();
	param->in = &inPipe;
	param->out = &outPipe;
	param->order = groupAtts;
	param->computeMe = &computeMe;

	
	pthread_create(&Run_Thread, NULL, Groupby_run_thread, (void*)param);
	//para
}

void GroupBy::WaitUntilDone()
{
	pthread_join(Run_Thread, NULL);
}
