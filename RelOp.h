#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"


class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class Threadpara
{
public:
	Pipe * in;
	Pipe *out;
	Pipe *inL;
	Pipe *inR;
	OrderMaker order;
	int runLength;
	File *myFile;
	FILE * IOFILE;
	Schema *mysema;
	CNF *mycnf;
	DBFile *mydbfile;
	Record *fethme;
	Function *computeMe;
	
	int *keepme;
	int numAttsInput;
	int numAttsOutput;
	//
};

class BigQParam {
public:
	Pipe * in;
	Pipe *out;
	OrderMaker *order;
	int runLength;
	File *myFile;
};


class SelectFile : public RelationalOp { 

	private:
	pthread_t Run_Thread;
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
private:
	pthread_t Run_Thread;;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	
	void WaitUntilDone(); //{ }
	void Use_n_Pages(int n);// { }
};

class Project : public RelationalOp { 
private:
	pthread_t Run_Thread;
	public:
		void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
		void WaitUntilDone();	
		void Use_n_Pages(int n);
};
class Join : public RelationalOp { 
private:
	pthread_t Run_Thread;
	int RunPagecount ;
	public:
		void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		//{ }
		void WaitUntilDone();
		//{
			//pthread_join(Run_Thread, NULL);
		//}
		void Use_n_Pages(int n) 
		{ 
			RunPagecount = n;
		}
};
class DuplicateRemoval : public RelationalOp {
private: 
	pthread_t Run_Thread;
;
	public:
		void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema); //{ }
		void WaitUntilDone();//; {
		
	
		void Use_n_Pages (int n) { }
};
class Sum : public RelationalOp {
	private:
		pthread_t Run_Thread;

	public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);// { }
	void WaitUntilDone();
	//{
		
	//}
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {

private:
	pthread_t Run_Thread;
	public:
		void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);// { }
		void WaitUntilDone();//{ }
	void Use_n_Pages (int n) { }
};
class WriteOut : public RelationalOp {
private :
	pthread_t Run_Thread;
	public:
		void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);// { }
		void WaitUntilDone();;
	//{
		
	//}
	void Use_n_Pages (int n) { }
};
#endif
