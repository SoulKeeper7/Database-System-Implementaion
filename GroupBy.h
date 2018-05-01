#ifndef GROUP_BY
#define GROUP_BY


#include"RelOpNode.h"
class Group_byNode : virtual public RelOpNode
{
private:
	Function Func;
	NameList groupingatt; 
	attNoAndType* orderMakerAtts; 
	OrderMaker grp_order; // used to create the OrderMaker passed to GroupBy in Run
	FuncOperator *funcOperator; // used for printing
	GroupBy g;

public:
	Group_byNode(NameList *groupatt, FuncOperator *funcOperator, RelOpNode* &root, int& pID) 
	{
		
		left = root;
		root = this;
		groupingatt = *groupatt; 
		NameList tempNameList = *groupatt; 
		relschema = left->schema();		
		Func.GrowFromParseTree(funcOperator, *relschema); 
		int groupattcount = 0;

		
		while (groupatt)
		{
			groupattcount++;
			groupatt = groupatt->next;
		} 

		orderMakerAtts = new attNoAndType[groupattcount](); 
		groupattcount++; 

		Attribute* outputAtts = new Attribute[groupattcount]; 
		outputAtts[0] = { "Sum", Double }; 

		
		int i = 0;
		NameList* temp = &tempNameList;
		while (temp) 
		{
			temp->name = strpbrk(temp->name, ".") + 1;
			orderMakerAtts[i].attNo = relschema->Find(temp->name);
			orderMakerAtts[i].attType = relschema->FindType(temp->name);
			i++;
			outputAtts[i] = { temp->name,relschema->FindType(temp->name) };
			temp = temp->next;
		} 	

		
		grp_order.initOrderMaker(groupattcount - 1, orderMakerAtts);
		relschema = new Schema("out_sch", groupattcount, outputAtts);

		this->funcOperator = funcOperator;

		pipeID = pID;
		pID++; 
	};

	Schema* schema() 
	{
		return relschema;
	}

	~Group_byNode() {
	};

	void Print() 
	{
		cout << endl;		
		cout << ":::::::::::::::::::::::::GroupBY OPERATION :::::::::::::::::::::::::::::" << endl;
		cout << "Input pipe ID:: " << left->pipeID << "  " << "Output pipe ID:: " << pipeID << endl;
		PrintOutputSchema(relschema);
		cout << endl << "Grouping Attributes::";
		PrintNameList(&groupingatt);
		cout << endl<<"Aggregate Function::" ;
		Func.Print(funcOperator, *relschema);
		
	};
	void Run() {
		// cout << "groupby started" << endl; // debug
		g.Use_n_Pages(100);
		g.Run(*(left->outpipe), *outpipe, grp_order, Func); // GroupBy takes its input from its left child's
															// outPipe. Its right child is NULL.
	};

	void WaitUntilDone() {
		// cout << "groupby ended" << endl; // debug
		g.WaitUntilDone();
	}


	

};
#endif // !GROUP_BY