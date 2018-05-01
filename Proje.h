#include"RelOpNode.h"
#include<vector>

 class myattstruct
{
public:
	int temp;
	Type type;
	char* tempName;
};
class ProjectNode : virtual public RelOpNode 
{
private:
	int* keepMe;
	NameList nameLis; 
	int numAttsIn, numAttsOut;
	Project P;
	public:
	ProjectNode(NameList *atts, RelOpNode* &root, int& pID) 
	{
		
		left = root;
		root = this; 
		nameLis = *atts; 
		NameList tempNameList = *atts; 
		NameList* tempL = &tempNameList;

		Schema* inschema = left->schema();
		numAttsIn = inschema->GetNumAtts();
		
		vector<myattstruct> obj;
		int i = 0;
		while (tempL)
		{			
				myattstruct x;
				tempL->name = strpbrk(tempL->name, ".") + 1;
				x.temp = inschema->Find(tempL->name);
				x.type = inschema->FindType(tempL->name);
				x.tempName = tempL->name;			
				obj.push_back(x);		
				tempL = tempL->next;
		}
		
		keepMe = new int[obj.size()]();
		Attribute* tempAttArray = new Attribute[obj.size()](); 
		for (int i = 0; i<obj.size(); i++)
		{
			keepMe[i] = obj[i].temp;
			tempAttArray[i].name = obj[i].tempName;
			tempAttArray[i].myType = obj[i].type;
		}
		numAttsOut = obj.size();
		relschema = new Schema("projectOutputSchema", obj.size(), tempAttArray);
		pipeID = pID;
		pID++;

	};

	Schema* schema()
	{
		return relschema;
	}

	~ProjectNode() 
	{
	};

	void Print() 
	{
		cout << endl;
		
		cout << ":::::::::::::::::::::::::PROJECT OPERATION :::::::::::::::::::::::::::::" << endl;
		
		cout << "Input pipe ID:: " << left->pipeID <<"   " <<"Output pipe ID:: " << pipeID << endl;
		PrintOutputSchema(relschema);
		cout << endl;
		cout << "Project Attributes::" ;
		PrintNameList(&nameLis);	
	
	};
	void Run()
	{
		//dbfile.Open(rel->path());
		//dbfile.MoveFirst();
		P.Use_n_Pages(100);
		P.Run(*(left->outpipe), *outpipe, keepMe, numAttsIn, numAttsOut);
	};

	void WaitUntilDone()
	{
		P.WaitUntilDone();
		//dbfile.Close();
	}

	
};