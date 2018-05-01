#include "RelOpNode.h"
class DupRemNode : virtual public RelOpNode 
{
private:
	DuplicateRemoval D;
public:
	DupRemNode(int distinctAtts, int distinctFunc, RelOpNode* &root, int& pID) {
		
		if (!(distinctAtts || distinctFunc))
		{
			return;
		}
		
		left = root;
		root = this;		
		relschema = left->schema();
		pipeID = pID;
		pID++; 
	};

	Schema* schema()
	{
		return relschema;
	}

	~DupRemNode()
	{
	};

	void Print()
	{
		cout << endl;		
		cout << ":::::::::::::::::::::::::DUPLICATE OPERATION :::::::::::::::::::::::::::::" << endl;		
		cout << "Input pipe ID: " << left->pipeID << "  " << "Output pipe ID: " << pipeID << endl;
		PrintOutputSchema(relschema);		
	};
	void Run() {
		// cout << "dupremoval started" << endl; // debug
		D.Use_n_Pages(100);
		D.Run(*(left->outpipe), *outpipe, *relschema); // DuplicateRemoval takes its input from its left child's
													 // outPipe. Its right child is NULL.
	};

	void WaitUntilDone() {
		// cout << "dupremoval ended" << endl; // debug
		D.WaitUntilDone();
	}

};