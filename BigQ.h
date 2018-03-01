#ifndef BIGQ_H
#define BIGQ_H
#define HAVE_STRUCT_TIMESPEC
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include <pthread.h>

#include "Schema.h"

using namespace std;

class BigQ {

public:
 //OrderMaker
	File myFile;
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};
class Threadparameters {
public:
	Pipe * in;
	Pipe *out;
	OrderMaker order;
	int runLength;
	File myFile;
};




 class Sort_Merge{

	OrderMaker *sort_order;
public:

	Sort_Merge(OrderMaker *abc)
	{
		sort_order = abc;
	}

bool operator()(Record* r1,Record* r2) 
{
	ComparisonEngine comp;
	if(comp.Compare(r1,r2,sort_order)<0) 
		return false;
	else return true;
}
};


class Sort_Run 
{
	OrderMaker *sort_order;
public:
	Sort_Run(OrderMaker *order)
	{		
		sort_order = order;		
	}
	ComparisonEngine comp;
	bool operator()(Record * r1, Record * r2)
	{

		if (comp.Compare(r1, r2, sort_order)>0)
		{			
			return false;
		}
		else return true;
	}
};

#endif

