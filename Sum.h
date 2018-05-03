#ifndef SUM_N
#define SUM_N


#include "RelOpNode.h"
class SumNode : virtual public RelOpNode 
{
private:
	Function Func;	
	FuncOperator *funcOperator; // used for printing
	Schema* sumNOdeSchemna;
	Sum sum;
	

public:
	SumNode(FuncOperator *fOperator, RelOpNode* &root, int& pID) 
	{
		
		left = root;
		root = this;
		Attribute DA = { "Sum", Double };			
		this->funcOperator = fOperator;	
		sumNOdeSchemna = left->schema();		
		Func.GrowFromParseTree(funcOperator, *sumNOdeSchemna);
		relschema = new Schema("outSchema", 1, &DA);
		pipeID = pID;
		pID++; 
	};

	Schema* schema()
	{
		return relschema;
	}

	~SumNode()
	{
	};

	void Print()
	{
		cout << endl;
		cout << ":::::::::::::::::::::::::SUM OPERATION :::::::::::::::::::::::::::::" << endl;
		cout << "Input pipe ID:: " << left->pipeID << "  " << "Output pipe ID:: " << pipeID << endl;
		PrintOutputSchema(relschema);
		cout << endl<<"Corresponding Function::" << endl;
		Func.Print(funcOperator, *relschema);		
	};
	void Run() {
		// cout << "sum started" << endl; // debug
		sum.Use_n_Pages(100);
		sum.Run(*(left->outpipe), *outpipe, Func); // Sum takes its input from its left child's
												 // outPipe. Its right child is NULL.
	};

	void WaitUntilDone() {
		// cout << "sum ended" << endl; // debug
		cout << "output " << pipeID;
		sum.WaitUntilDone();
		cout << "closing " << pipeID;

	}

};
#endif // !SUM_N