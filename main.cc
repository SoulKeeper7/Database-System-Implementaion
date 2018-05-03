#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
#include <string.h>
#include "SelcFile.h"
#include "SelPipe.h"
#include "Dup.h"
#include "Proje.h"
#include "JoinNode.h"
#include "GroupBy.h"
#include "Sum.h"
#include<string.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
	extern struct TableList *tables; // the list of tables and aliases in the query
	extern struct AndList *boolean; // the previousdicate in the WHERE clause
	extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
	extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
	extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
	extern struct SchemaList *schemas; // the list of tables and aliases in the query
	extern char* LoadfileName; // bulk loading file name string
	extern char* OutFileName; // output file name or STDOUT string
	//extern int commandFlag; // 1 if the command is a create table command.
	extern int NumAtt;
	extern struct CreateTableType* createTableType;

}
map<string, RelOpNode*> GenTreeHash;
RelOpNode* QueryRoot; // after executing queryPlanning the main root node						
map<string, string> relationAlias;
map<string, string> joinedNodesAlias;
RelOpNode* deleteQueryRoot = NULL;;


void ConvertOperand(struct Operand *Op) 
{
	if (Op == NULL)
		return;
	if (Op->code != NAME)
		return;
	char key[] = ".";			
	Op->value = strpbrk(Op->value, key) + 1;
		
}

void ConvertComparisonOp(struct ComparisonOp *compOp) 
{
	if (compOp == NULL)
		return;
	ConvertOperand(compOp->left);
	ConvertOperand(compOp->right);
	
}
void PrintOperand(struct Operand *Op)
{
	if (Op == NULL)
	{
		return;
	}
	
	cout << Op->value << " ";
	
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
	if (pCom != NULL)
	{
		PrintOperand(pCom->left);
		switch (pCom->code)
		{
		case LESS_THAN:
			cout << " < "; break;
		case GREATER_THAN:
			cout << " > "; break;
		case EQUALS:
			cout << " = ";

		}
		PrintOperand(pCom->right);

	}
	else
	{
		return;
	}
}
void PrintOrList(struct OrList *pOr)
{
	if (pOr != NULL)
	{
		struct ComparisonOp *pCom = pOr->left;
		PrintComparisonOp(pCom);

		if (pOr->rightOr)
		{
			cout << " OR ";
			PrintOrList(pOr->rightOr);
		}
	}
	else
	{
		return;
	}
}
void PrintAndList(struct AndList *pAnd)
{
	if (pAnd != NULL)
	{
		cout << "(";
		struct OrList *pOr = pAnd->left;
		PrintOrList(pOr);
		cout << ")";
		if (pAnd->rightAnd)
		{

			cout << " AND ";

			PrintAndList(pAnd->rightAnd);
		}
	}
	else
	{
		return;
	}
}
void PrintRelationNamesList(struct NameList *RelationNames)
{
	if (RelationNames != NULL)
	{
		cout << ("%s ", RelationNames->name);
		if (RelationNames->next) {
			cout << ", ";
			PrintRelationNamesList(RelationNames->next);
		}
	}
	else
	{
		cout << endl;
		return;
	}
}

void PrintTableList(struct TableList *tables)
{
	if (tables != NULL)
	{
		cout << tables->tableName << tables->aliasAs;
		if (tables->next) {
			cout << ", ";
			PrintTableList(tables->next);
		}
	}
	else
	{
		cout << endl;
		return;
	}
}




void SplitAttribute(struct OrList *orL) {
	if (orL == NULL)
	{
		return;
	}
	struct ComparisonOp *pCom = orL->left;
	ConvertComparisonOp(pCom);
	if (orL->rightOr)
	{
			SplitAttribute(orL->rightOr);
	}
	else
	{
		return;
	}
	
}

void createTheGenNode(struct AndList &dummy, char* RelName[], int numToJoin, int& pipeID)
{


	RelOpNode* NewQNode;
	string leftRelName = RelName[0];
	string rightRelName;

	SplitAttribute(dummy.left); 	

	if (numToJoin == 1) 
	{ 		
		if (GenTreeHash.count(leftRelName) || joinedNodesAlias.count(leftRelName))
		{
			NewQNode = new SelcPipNode(dummy, leftRelName, GenTreeHash, pipeID, joinedNodesAlias); // used lareay created datas
		}		
		else
		{			
			NewQNode = new SeleFileNode(dummy, leftRelName, GenTreeHash, pipeID); //create a new select file
		}
	}
	else if (numToJoin == 2) 
	{ 
		rightRelName.assign(RelName[1]);		
		NewQNode = new JoinNode(dummy, leftRelName, rightRelName, GenTreeHash, pipeID, joinedNodesAlias);// create a join node
	}
	else
	{
		cerr << "ERROR: Join issue" << endl;
	}
	
	GenTreeHash[leftRelName] = NewQNode; // add the new node to the hash
}

void InOrderPrintQTree(RelOpNode* currentNode)
{
	if (!currentNode)
		return;
	InOrderPrintQTree(currentNode->left);
	currentNode->Print();
	InOrderPrintQTree(currentNode->right);
}


int clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	ofstream ou(OutFileName);
	while (in_pipe.Remove(&rec)) {
		if (print && (strcmp(OutFileName,"STDOUT")==0))
		{
			rec.Print(schema);
		}
		else if(strcmp(OutFileName, "NONE")!=0)
		{
			
			//out(OutFileName);
			
			rec.Print(schema, ou);

		}
		cnt++;
	}
	out.flush();
	return cnt;
}

void GreedilyEvalAndList(struct AndList *&startNode, struct AndList *constructelist, struct AndList *&andList, Statistics &s, int &pipeID)
{
	// terminating base case. andList exhausted.
	if (!andList)
		return;

	char leftOperandValue[60];
	char rightOperandValue[60];
	struct AndList *temp = andList;	
	vector<int> estimatedValues; // holds the intemediate estimated values
	char * RelationNames[2]; //holds name of the realtions in the previousdicate
	struct AndList andNode;// make a andNode andlist that only has one operation inside.
	
	while (temp != NULL) 
	{
		andNode.left = temp->left; //create a singleton AndNListNode
		andNode.rightAnd = NULL;

		struct ComparisonOp * pCom = andNode.left->left; //comparison operator
		int numToJoin = 1; //intially assume as an singleton 

		// fill the operand values
		strcpy(leftOperandValue, pCom->left->value);
		strcpy(rightOperandValue, pCom->right->value);
		
		// check for join condition
		if (pCom->left->code == NAME && pCom->right->code == NAME && pCom->code == EQUALS)
		{
			//split and get rel names
			RelationNames[0] = strtok(leftOperandValue, ".");
			RelationNames[1] = strtok(rightOperandValue, ".");
			numToJoin = 2;
			
			// getting the original relation name from hasmap
			if (relationAlias.count(RelationNames[1]))
			{
				strcpy(RelationNames[1], relationAlias[RelationNames[1]].c_str());
			}
			if (relationAlias.count(RelationNames[0]))
			{
				strcpy(RelationNames[0], relationAlias[RelationNames[0]].c_str());
			}
		}
		else 
		{
			RelationNames[0] = (pCom->left->code == NAME) ? strtok(leftOperandValue, ".") : strtok(rightOperandValue, ".");

			if (relationAlias.count(RelationNames[0]))
			{
				strcpy(RelationNames[0], relationAlias[RelationNames[0]].c_str());
			}
		}
		
		int val = s.Estimate(&andNode, RelationNames, numToJoin); // estimate the value of the min relation
		//cout << val << endl;
		estimatedValues.push_back(val); //add the value for later use
		temp = temp->rightAnd;
	}
	// find the index of the minimum cost.
	int min = estimatedValues[0];
	int estimatedIndex = 0;
	for (int i = 0; i<estimatedValues.size(); i++) 
	{
		if (estimatedValues[i] < min)
		{
			estimatedIndex = i;
			min = estimatedValues[i];
		}
	}

	
	struct AndList *current = andList;
	struct AndList *previous = andList;

	
	//Manupluate the Anlist 
	while (estimatedIndex > 0) 
	{
		estimatedIndex--;
		previous = current;
		current = current->rightAnd;
	}

	if (current == andList)
	{ // the node to be removed is the first node.
		andList = current->rightAnd;
		current->rightAnd = NULL;
	}
	else 
	{ 
		previous->rightAnd = current->rightAnd;
		current->rightAnd = NULL;
	}

	//PrintAndList(current);

	// need apply for the selected node
	andNode.left = current->left;
	struct ComparisonOp * pCom = andNode.left->left;

	int numToJoin = 1;
	
	strcpy(leftOperandValue, pCom->left->value);
	strcpy(rightOperandValue, pCom->right->value);

	// two attribute equal join
	if (pCom->left->code == NAME && pCom->right->code == NAME && pCom->code == EQUALS)
	{

		RelationNames[0] = strtok(leftOperandValue, ".");
		RelationNames[1] = strtok(rightOperandValue, ".");
		numToJoin = 2;
		string leftRelName(RelationNames[0]);
		string rightRelName(RelationNames[1]);

		if (relationAlias.count(rightRelName))
		{
			strcpy(RelationNames[1], relationAlias[rightRelName].c_str());
		}
		if (relationAlias.count(leftRelName))
		{
			strcpy(RelationNames[0], relationAlias[leftRelName].c_str());
		}

		if (relationAlias.count(rightRelName) == 0 && relationAlias.count(leftRelName) == 0)
		{
			relationAlias[rightRelName] = leftRelName;
		}		
		else if (relationAlias.count(leftRelName) != 0 && relationAlias.count(rightRelName) == 0)
		{
			relationAlias[rightRelName] = relationAlias[leftRelName];
		}		
		else if (relationAlias.count(leftRelName) == 0 && relationAlias.count(rightRelName) != 0) 
		{
			string temp = relationAlias[rightRelName];
			map<string, string>::iterator it;
			for ( it = relationAlias.begin(); it != relationAlias.end(); it++) 
			{
				if ((it->second).compare(temp) == 0)it->second = leftRelName;
			}
		}		
		else
		{
			string temp = relationAlias[rightRelName];
			if (relationAlias[leftRelName] != relationAlias[rightRelName])
			{
				map<string, string>::iterator it;
				for ( it = relationAlias.begin(); it != relationAlias.end(); it++) 
				{
					if ((it->second).compare(temp) == 0)it->second = relationAlias[leftRelName];
				}
			}
		}

	}
	else
	{
		RelationNames[0] = (pCom->left->code == NAME) ? strtok(leftOperandValue, ".") : strtok(rightOperandValue, ".");		
		
	}

	s.Apply(&andNode, RelationNames, numToJoin); // apply the stat object
	
	createTheGenNode(andNode, RelationNames, numToJoin, pipeID); //create the corresponding treenode for the where clause

	if (!constructelist) 
	{  
		startNode = current; // if nothing added yet add the starting node
		constructelist = current;
	}
	else 
	{ 
		constructelist->rightAnd = current;
		constructelist = current;
	}

	pipeID++;
	GreedilyEvalAndList(startNode, constructelist, andList, s, pipeID);
}


void QueryOptimizer(struct AndList *&andList, TableList *tableList, Statistics &stat) 
{
	int relationToJoin = 0;  //check if singleton realtion or join
	while (tableList)
	{
		relationToJoin++;
		if (tableList->aliasAs)
		{
			stat.CopyRel(tableList->tableName, tableList->aliasAs); // add tostats the alais node and attributes
		}
		tableList = tableList->next;
	}
	//GenTreeHash.clear();
	struct AndList *StartNode; // starting node of the new constructed query/andlist
	int pipeID = 0;
	GreedilyEvalAndList(StartNode, NULL, andList, stat, pipeID);
	andList = StartNode;
	PrintAndList(StartNode); //for debugpostorder
	
	// add other nodes-->grouping/sum/project/distinct
	RelOpNode* root = GenTreeHash.begin()->second;
	if (groupingAtts)
	{
		new Group_byNode(groupingAtts, finalFunction, root, pipeID);
	}
	else if (finalFunction)
	{
		new SumNode(finalFunction, root, pipeID);
	}
	else
	{
		new ProjectNode(attsToSelect, root, pipeID);
	}
	new DupRemNode(distinctAtts, distinctFunc, root, pipeID);
	QueryRoot = root;
	//deleteQueryRoot = root;
}

void PostOrderRun(RelOpNode* currentNode) {
	if (!currentNode) {
		return;
	}
	PostOrderRun(currentNode->left);
	PostOrderRun(currentNode->right);
	currentNode->Run();
}

void PostOrderWait(RelOpNode* currentNode) {
	if (!currentNode)
	{
		return;
	}
	PostOrderWait(currentNode->left);
	PostOrderWait(currentNode->right);
	currentNode->WaitUntilDone();
}
void deleteTree(RelOpNode *currentNode)
{
	if (currentNode==NULL) {
		return;
	}
	deleteTree(currentNode->left);
	deleteTree(currentNode->right);
	//if (currentNode->left !=NULL)
	//{
	//	delete currentNode->outpipe;
	//	
	//	
	//	//delete currentNode->left->outpipe;
	//	//currentNode->left = NULL;
	//}
	//if (currentNode->right !=NULL)
	//{
	//	delete currentNode->outpipe;
	//	//delete (currentNode->right)->outpipe;
	//	//currentNode->right = NULL;
	//}
	//currentNode = NULL;
	delete currentNode;
	
}

void queryExecution() 
{
	cout << endl << "--------------------------------------------" << endl;
	cout << "          Starting query execution";
	cout << endl << "--------------------------------------------" << endl;

	
	PostOrderRun(QueryRoot);
	
	int cnt = clear_pipe(*(QueryRoot->outpipe), QueryRoot->schema(), true);	
	PostOrderWait(QueryRoot);

	
	cout << "\nQuery returned " << cnt << " records \n";
	cout << endl << "--------------------------------------------" << endl;
	cout << "           Query execution done";
	cout << endl << "--------------------------------------------" << endl;

	GenTreeHash.clear();
	
	deleteTree(QueryRoot);
	 QueryRoot =NULL;
	
}
 void RunQuery()
{
	struct TableList * iter = tables;
	DBinfo.clear();
	aliastotable.clear();
	while (iter != NULL)
	{
		DBinfo[iter->aliasAs] = DBinfo[iter->tableName];
		aliastotable.insert({ iter->aliasAs,iter->tableName }); // for creating relational schema
		iter = iter->next;
	}

	char *fileName = "Statistics.txt";
	relationAlias.clear();
	joinedNodesAlias.clear();
	Statistics stat;
	stat.Read(fileName);

	QueryOptimizer(boolean, tables, stat); // calls the QueryPlanner
	cout << endl << ":::::::::::::::Query Plan::::::::::::::::::: " << endl; // InOrder print out the tree.
	InOrderPrintQTree(QueryRoot);
	cout << endl << "											" << endl;
	cout << ":::::::::::::::End Of Query optimization :::::::::::::::";
	cout << endl << "										" << endl;

	

}
 void updateCatalog(string relname)
 {
	// FILE *foo = fopen(catalog_path, "r");
	 ifstream infile;
	 infile.open(catalog_path);

	 // this is enough space to hold any tokens
	 //char space[200];
	 string space;

	 getline(infile, space);
	 int totscans = 1;

	 // see if the file starts with the correct keyword
	 
	 if (strcmp((char*)space.c_str(), "BEGIN")) 
	 {
		 //cout << "Unfortunately, this does not seem to be a schema file.\n";
		 
		 //exit(1);
	 }
	 char tempPath[100] = "tempfile.txt";
	 ofstream outfile;
	 outfile.open(tempPath, ios::out);
	 outfile << endl;
	 int count = 0;
	 
	 while (1)
	 {

		 if (!getline(infile, space))
		 {
			 //outfile << space << endl;
			 break;
		 }
		 string tmp = space;
		 /*else
		 {
			 outfile << space << endl;
		 }*/

		 
		 getline(infile, space);
		 if (strcmp((char*)space.c_str(), (char*)relname.c_str()))
		 {
			 outfile << tmp << endl;
			 outfile << space << endl;
			 			 
			 while (1)
			 {
				 
				 // suck up another token
				 if (!getline(infile, space))
				 {
					 break;
				 }

				 if (!strcmp((char*)space.c_str(), "\0"))
				 {
					 outfile << space << endl;
					 break;
					 //cerr << "Could not find the schema for the specified relation.\n";
					 //exit(1);
				 }
				 else
				 {
					 outfile << space << endl;
				 }

			 }

			 // otherwise, got the correct file!!
		 }
		 else
		 {

			 while (getline(infile, space))
			 {
				 //fscanf(foo, "%s", space);
				 // suck up another token
					
					 if (!strcmp(space.c_str(), "\0"))
					 {
						 break;
						 
					 }
				 
			 }
		 }
	 }	

	//outfile << endl;
	
	outfile.close();
	infile.close();

	 remove(catalog_path);

	 rename(tempPath, catalog_path);

 
	 
	
 }

 void UpdateSavedstate()
 {
	 remove(Savedstate);
	 
	 ofstream outpu;
	 outpu.open(Savedstate);
	 map<string, relation*> ::iterator it;
	 for( it = createdrelationlist.begin(); it != createdrelationlist.end(); it++)
	 {
		 outpu << it->first << endl;;
	 }
	 outpu.close();

 }
 void CreateTable()
{	 
	 std::ofstream out;

	 // std::ios::app is the open mode "append" meaning
	 // new data will be written to the end of the file.
	 if (createdrelationlist.find(tables->tableName) != createdrelationlist.end())
	 {

		 cout << "table with the same name already exists!" << endl;
		 return;
	 }
	 out.open(catalog_path, std::ios::app);
	 std::string str = "I am here.";
	 out << "BEGIN"<<endl;
	 out << tables->tableName<<endl;
	 string x  = (string)tables->tableName + "." + "tbl";
	 out << x <<endl;

	 
	 while (schemas)
	 {
		 string c;
		 if (!strcmp(schemas->type, "INTEGER"))
		 {
			 c = "Int";
		 }
		 else if (!(strcmp(schemas->type, "DOUBLE")))
		 {
			 c = "Double";
		 }
		 else if (!(strcmp(schemas->type, "STRING")))
		 {
			 c = "String";
		 }
		 out << schemas->attName << " " << c << endl;
		 schemas = schemas->next;
	 }

	 out << "END"<<endl;
	 out << endl;

	 out.close();
	 Schema *s = new Schema(catalog_path, tables->tableName);
	 relation *myrel = new relation((char *)tables->tableName,s , dbfile_dir);
	 createdrelationlist.insert({ tables->tableName,myrel });


	 char *relationName = tables->tableName;
	 
	 

	 Attribute creatatbleatt [NumAtt];
	 int i = 0;
	 while (schemas != NULL)
	 {
		 creatatbleatt[i].name = strdup(schemas->attName);		 

		 if (strcmp(schemas->type, "INTEGER") == 0)
		 {
			 creatatbleatt[i].myType = Int;
		 }
		 else if (strcmp(schemas->type, "DOUBLE") == 0)
		 {
			 creatatbleatt[i].myType = Double;
		 }
		 else if (strcmp(schemas->type, "STRING") == 0)
		 {
			 creatatbleatt[i].myType = String;
		 }

		 schemas = schemas->next;
		 i++;
	 } 
	 
	 DBFile dbfile;
	 //relation * myrel = new relation(relationName, new Schema(relationName, NumAtt, creatatbleatt), dbfile_dir);	 
	 char db_path[100]; 
	 if (strcmp(createTableType->heapOrSorted, "HEAP") == 0)
	 {
		 cout << "HEAP DBFile will be placed at " << myrel->path() << "..." << endl;
		 dbfile.Create(myrel->path(), heap, NULL);
		 createdrelationlist[relationName] = myrel;
		 dbfile.Close();		 
	 }
	 else if (strcmp(createTableType->heapOrSorted, "SORTED") == 0)
	 {
		 if (createTableType->sortedAttList == NULL) 
		 {
			 cout << "Please enter sorting attributes." << endl << endl;
			 return;
		 }
		 else
		 {			 
			 OrderMaker *orm = new OrderMaker();			 		 
			 orm->ADDSortingAtt(createTableType->sortedAttList, s);			
			 int runlen =1;
			 cout<<endl<< "please enter the runlength"<<endl;
			 cin >> runlen;
			 struct { OrderMaker *o; int l; } startup = { orm, runlen };			 
			 dbfile.Create(myrel->path(), sorted, &startup);
			 createdrelationlist[relationName] = myrel;
			 dbfile.Close();
			 
		 }
	 }
	
	 UpdateSavedstate();
}

 void InsertIntoTable()
 {
	 char *rel = tables->tableName;
	 if (!createdrelationlist.count(rel)) 
	 {
		 cout << "relationName doesn't exist";
		 return;
	 }
	 else
	 {

		 relation * myrel = createdrelationlist[rel];
		 char tbl_path[100]; 
		 sprintf(tbl_path, "%s%s", tpch_dir, LoadfileName);
		 cout << "Inserting data from " << tbl_path << "..." << endl;
		 DBFile dbfile;
		 dbfile.Open(myrel->path());
		 dbfile.Load(*(myrel->schema()), tbl_path);
		 dbfile.Close();
		
	 }
	 
 }
 void DropATable()
 {
	 char *relName = tables->tableName;// outputFileName->name
	if (!createdrelationlist.count(relName))
	{
		cout << "The table doesn't exist. Cannot drop." << endl;
		return;
	}
	 relation *myrel = createdrelationlist[relName];
	 char db_path[100]; // construct path of the tpch flat textfile
	 char db_bin[100];
	 sprintf(db_path, "%s%s.bin.meta", dbfile_dir, relName);
	 sprintf(db_bin, "%s%s.bin", dbfile_dir, relName);
	 if (remove(db_bin) != 0)
	 {
		 perror("Cannot delete .bin");
	 }
	 if (remove(db_path) != 0)
	 {
		 perror("Cannot delete .bin.meta");
	 }	
	 delete myrel;
	 cout << "Table Dropp Sucessfull" << endl;
	 
	 createdrelationlist.erase(relName);
	 UpdateSavedstate();	
	 updateCatalog(relName);
 }
 


int main () 
{	

	setup();
	bool exi = true;
	ifstream infile;
	char db_sting[100];
	infile.open(Savedstate);
	 string val;
	relation *myrel;
	while (getline(infile, val))
	{			
		myrel = new relation((char *)val.c_str(), new Schema(catalog_path, val.c_str()), dbfile_dir);		
		createdrelationlist.insert({ val,myrel});
		//cout << "realtions created";
	}
	while (exi)
	{
	
		
		
		cout <<endl<< "			1::Create Table" << endl;
		cout << "			2::Insert Into" << endl;
		cout << "			3::Drop A Table" << endl;
		cout << "			4::SetOutPut" << endl;
		cout << "			5::Run Sum Query" << endl;
		cout << "			6::Close" << endl<<endl<<endl;

		cout << "Enter the option number" << endl;
		int option;
		cin >> option;

		switch (option)
		{
			case 1: cout << endl << "Enter the query to create table" << endl;
				yyparse();
				CreateTable();
				break;
			case 2:
				cout << endl << "Enter the insert query" << endl;
				yyparse();
				InsertIntoTable();
				break;
			case 3:
				cout << endl << "Enter the Drop Query" << endl;
				yyparse();
				DropATable();
				break;
			case 4:	
				cout << endl << "SET OUTPUT" << endl;
				yyparse();

				break;
			case 5:
				cout << endl << "Enter the SQL query" << endl;
				yyparse();

				if (strcmp(OutFileName,"NONE")==0)
				{
					cout << OutFileName;
					RunQuery();
					break;
				}
				RunQuery();
				queryExecution();
				
				break;
			case 6:
				UpdateSavedstate();
				exit(0);
				break;
		}
	}	
}


 


