#ifndef JOIN_N
#define JOIN_N

#include "RelOpNode.h"
#include "SelcFile.h"
class JoinNode : virtual public RelOpNode {
private:
	
	Record literal;
	CNF cnf;
	Join J;
	string Relleft,Relright;
	Join j;
public:
	JoinNode(struct AndList &dummy, string &RelName0, string &RelName1, map<string, RelOpNode*> &GenTreeHash, int& pID, map<string, string> &joinedNodesAlias) {
		

		RelOpNode* leftsubtree = NULL, *rightsubtree = NULL;

		RelName0 = joinedNodesAlias.count(RelName0) > 0 ? RelName0 = joinedNodesAlias[RelName0] : RelName0;		
		RelName1 = joinedNodesAlias.count(RelName1) > 0 ? RelName1 = joinedNodesAlias[RelName1] : RelName1;
		
		this->Relleft = RelName0;
		this->Relright = RelName1;		

		
		createLeftandRightTree(GenTreeHash, RelName0, pID, leftsubtree, RelName1, rightsubtree);

		
		left = leftsubtree;
		right = rightsubtree;

		
		GenTreeHash[RelName0] = this;
		GenTreeHash[RelName1] = this;
		
		if (GenTreeHash.count(RelName1))
		{
			GenTreeHash.erase(RelName1); //delete if exists , now we have only one node in hasmap
		}
		
		joinedNodesAlias[RelName1] = RelName0; //add to alias
		
		map <string, string>::iterator it;
		for ( it = joinedNodesAlias.begin(); it != joinedNodesAlias.end(); it++)
		{
			if ((it->second).compare(RelName1) == 0)
			{
				it->second = RelName0;
			}
		}
		
		relschema = left->schema();
		relschema = relschema->mergeSchema(right->schema());
		pipeID = pID;		
		cnf.GrowFromParseTree(&dummy, left->schema(), right->schema(), literal);
	}









	// creates left or right select node if present  just assign to it
	void createLeftandRightTree(std::map<std::string, RelOpNode *> & GenTreeHash, std::string & RelName0, int & pID, RelOpNode * &leftsubtree, std::string & RelName1, RelOpNode * &rightsubtree)
	{
		if (!GenTreeHash.count(RelName0))
		{

			RelOpNode* NewQNode;
			AndList tempAndList;
			tempAndList.left = NULL;
			tempAndList.rightAnd = NULL;
			NewQNode = new SeleFileNode(tempAndList, RelName0, GenTreeHash, pID); //create a select node with empty CNF should select all
			pID++;
			GenTreeHash[RelName0] = NewQNode;
		}

		leftsubtree = GenTreeHash[RelName0]; // already there or created


		if (!GenTreeHash.count(RelName1))
		{
			RelOpNode* NewQNode;
			AndList tempAndList;
			tempAndList.left = NULL;
			tempAndList.rightAnd = NULL;
			NewQNode = new SeleFileNode(tempAndList, RelName1, GenTreeHash, pID);
			pID++;
			GenTreeHash[RelName1] = NewQNode;
		}

		rightsubtree = GenTreeHash[RelName1];
	}
	;

	Schema* schema() 
	{
		return relschema;
	}

	~JoinNode()
	{
	};

	void Print() {
		cout << endl;
		cout << ":::::::::::::::::::::::::JOIN OPERATION :::::::::::::::::::::::::::::" << endl;
		cout << "Input pipe 1 ID:: " << left->pipeID << "    "  << "Input pipe 2 ID::" <<right->pipeID << "  " << "Output pipe ID: " << pipeID << endl;
		PrintOutputSchema(relschema);
		cout <<endl<< "JOIN CNF: "; cnf.Print();
		
	};

	void Run() 
	{
		J.Use_n_Pages(100);
		J.Run(*(left->outpipe), *(right->outpipe), *outpipe, cnf, literal); 
	};

	void WaitUntilDone() {
		
		cout << "output " << pipeID;
		J.WaitUntilDone();
		cout << "closing " << pipeID;
		//delete outpipe;
	}


};
#endif // !JOIN_N