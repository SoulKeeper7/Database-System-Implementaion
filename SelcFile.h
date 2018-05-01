#ifndef SEL_FILE
#define SEL_FILE


#include "RelOpNode.h"
class SeleFileNode : virtual public RelOpNode 
{
private:
	string RelName;
	relation* myrel;
	Record literal;
	CNF cnf;
	DBFile dbfile;	
	SelectFile mysel;
	
public:
	SeleFileNode(struct AndList &andList, string &RelName, map<string, RelOpNode*> &GenTreeHash, int pipeIDcounter) 
	{
	
		this->RelName = RelName;		
		//string x = aliastotable[RelName];
		//cout << x << endl;//debug
		myrel = new relation((char*)aliastotable[RelName].c_str(), new Schema(catalog_path, (char*)aliastotable[RelName].c_str()), dbfile_dir);		
		pipeID = pipeIDcounter;		
		relschema = myrel->schema();
		cnf.GrowFromParseTree(&andList, relschema, literal);
		GenTreeHash[RelName] = this;
		
	};

	Schema* schema() 
	{
		return relschema;
	}

	~SeleFileNode()
	{
	};

	void Print()
	{
		cout << endl;		
		cout << ":::::::::::::::::::::::::SELECT FILE OPERATION :::::::::::::::::::::::::::::" << endl;		
		cout << "Output pipe ID:: " << pipeID << endl;
		PrintOutputSchema(myrel->schema());
		cout << endl<<"SELECT CNF:: ";
		cnf.Print();
		cout  << endl;
	};

	void Run()
	{
		dbfile.Open(myrel->path());
		//dbfile.MoveFirst();
		mysel.Use_n_Pages(100);
		mysel.Run(dbfile, *outpipe,cnf, literal);
	};

	void WaitUntilDone()
	{
		mysel.WaitUntilDone();
		dbfile.Close();
	}
	
};
#endif // !1
