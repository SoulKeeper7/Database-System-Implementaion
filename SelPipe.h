#ifndef SEL_PIPE
#define SEL_PIPE
#include "RelOpNode.h"
class SelcPipNode : virtual public RelOpNode 
{
private:
	string selectpipeRelName;
	Record literal;
	CNF cnf;	
	SelectPipe mySelPipe;
public:
	SelcPipNode(struct AndList &andList, string &RelName, map<string, RelOpNode*> &GenTreeHash, int pID, map<string, string> &joinedNodesAlias) 
	{		

				
		RelOpNode* leftSubTree = NULL;
		RelName = joinedNodesAlias.count(RelName) > 0 ? joinedNodesAlias[RelName] : RelName;		
		this->selectpipeRelName = RelName;
		leftSubTree = GenTreeHash.count(RelName) > 0 ? GenTreeHash[RelName] : NULL;		
		left = leftSubTree;
		GenTreeHash[RelName] = this;		
		relschema = left->schema();				
		cnf.GrowFromParseTree(&andList, left->schema(), literal);
		pipeID = pID;
	};

	Schema* schema() 
	{
		return relschema;
	}

	~SelcPipNode() {
	};

	void Print() {
		cout << endl;
		cout << ":::::::::::::::::::::::::SELECT PIPE OPERATION :::::::::::::::::::::::::::::" << endl; 
		cout << "Input pipe ID:: " << left->pipeID << "    " << "Output pipe ID: " << pipeID << endl;
		PrintOutputSchema(left->schema());
		cout << endl;
		cout << endl << "Select PIPE CNF::";
		cnf.Print();
		cout << endl;	
	};

	void Run()
	{
		//dbfile.Open(rel->path());
		//dbfile.MoveFirst();
		mySelPipe.Use_n_Pages(100);
		mySelPipe.Run(*(left->outpipe), *outpipe, cnf, literal);
	};

	void WaitUntilDone()
	{
		mySelPipe.WaitUntilDone();
	};
	
};
#endif

