#include "Statistics.h"
#include <fstream>
#include<string>
#include<utility>
#include <cstdlib>
#include<iostream>
#include <sstream>
#include<vector>
#include<string.h>
#include<math.h>
#include<set>

using namespace std;

Statistics::Statistics()
{
	isCalledFrmApply = false;
	isApply = false;
	lastHandledRel = 0;
	
}
Statistics::Statistics(Statistics &copyMe)
{
	
	this->Addatt = copyMe.Addatt;
	this->Addrel = copyMe.Addrel;
	this->attAlias = copyMe.attAlias;
	this->andEqualCache = copyMe.andEqualCache;
	this->isApply = copyMe.isApply;
	this->isCalledFrmApply = copyMe.isCalledFrmApply;
	this->orEqualCache = copyMe.orEqualCache;
	this->lastHandledRel = copyMe.lastHandledRel;

}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
	map<string, int> ::iterator present;
	present = Addrel.find(relName);
	if(present == Addrel.end())
	{
		Addrel.insert(std::pair<string, int>(relName, numTuples));
	}
	else
	{
		Addrel[relName] = numTuples;
	}

	
	
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	
	map<string, int> ::iterator present;
	 present = Addrel.find(relName);
	if (present != Addrel.end())
	{
		//find the object in Addatt;
		map < string, map<string, int> > ::iterator presentinaddatt;
		presentinaddatt = Addatt.find(relName);
		///if object is present in addatttrbute
		if (presentinaddatt != Addatt.end())
		{
			map<string, int> ::iterator findattribute;
			 findattribute = presentinaddatt->second.find(attName);

			if (findattribute != presentinaddatt->second.end())
			{
				if (numDistincts != -1)
					presentinaddatt->second[attName] = numDistincts;
				else
					presentinaddatt->second[attName] = present->second;

			}
			else
			{
				presentinaddatt->second.insert(std::pair<string, int>(attName, numDistincts));
			}

		}
		else
		{
			map<string, int> x;
			x.insert(pair<string,int>(attName, numDistincts));
			//No object is added yet add new one
			Addatt.insert(pair<string, map<string, int> >(relName, x));

		}
	}
	else
	{
		//relation not added;
	}
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	map<string, int>::iterator findrel;
	findrel = Addrel.find(oldName);
	if (findrel != Addrel.end())
	{
		Addrel.insert(std::pair<string,int>(newName, findrel->second));

		map < string, map<string, int> > ::iterator findatt;
		 findatt = Addatt.find(oldName);

		if (findatt != Addatt.end())
		{
			map<string, int> listofatt;
			map<string, int>::iterator i;
			for ( i = (findatt->second).begin(); i != (findatt->second).end();i++)
			{
				//string x = newName;
				//x+= "." + i->first;				
				listofatt.insert(pair<string, int>(i->first, i->second));
			}

			Addatt.insert({ newName,listofatt });
		}
	}
}
	
void Statistics::Read(char *fromWhere)
{
	string line;
	std::ifstream infile(fromWhere);

	std::getline(infile, line);
	int reltaionCount = atoi(line.c_str());

	for (int i = 0; i < reltaionCount; i++)
	{
		std::getline(infile, line);
		std::istringstream iss(line);
		std::string token;
		
		char str[1024] ;
		strcpy(str, line.c_str());
		//string x;
		char * pch;
		pch = strtok(str, "|");
		vector<char*> x;
		while (pch != NULL)
		{
			x.push_back(pch);
			pch = strtok(NULL, "|");
		}		
		AddRel(x[0], atoi(x[1]));
	}
	
	
	while (std::getline(infile, line))
	{
		char str[1024];
		strcpy(str, line.c_str());
		char * pch;
		pch = strtok(str, "|");
		vector<char*> x;
		while (pch != NULL)
		{
			x.push_back(pch);
			pch = strtok(NULL, "|");
		}		
		AddAtt(x[0], x[1], atoi(x[2]));

	}
}
void Statistics::Write(char *fromWhere)
{
	std::ofstream outfile(fromWhere);
	
	outfile << Addrel.size() << endl;
	map<string, int>::iterator ip;
	if(ip != Addrel.end()) 
	{
		for (ip = Addrel.begin(); ip != Addrel.end(); ip++)
		{
			outfile << ip->first << "|" << ip->second << endl;
			//outfile << ;
		}
	}
	
	map < string, map<string, int> >::iterator i;
	if(i!=Addatt.end())
	{
		for (i = Addatt.begin(); i != Addatt.end(); i++)
		{
			map<string, int>::iterator j;
			for (j = i->second.begin(); j != i->second.end(); j++)
			{
				outfile << i->first << "|" << j->first << "|" << j->second << endl;
			}
		}
	}
}





void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{	
	double ANDnumOfTuples;
	bool flag = true;
	lastHandledRel = 0;
	evalAndList(parseTree, relNames, numToJoin, ANDnumOfTuples, flag);

}

int Statistics::evalComparisonOp(struct ComparisonOp *compOp, char **relNames, int numToJoin, double &numOfTuples, bool writeFlag, bool &cnfDuplicateFoundFlag, bool &sameAndAttDiffValue, bool&orSameAttDiffValue) {
	if (compOp != NULL) 
	{	
		if (compOp->left->code != NAME && compOp->right->code != NAME)
			return -1;

		
		if (numToJoin == 1 || ((compOp->left->code == NAME) != (compOp->right->code == NAME)))
		{ 
																							
			SingletonPredicate(compOp, numOfTuples, relNames, writeFlag, cnfDuplicateFoundFlag, sameAndAttDiffValue); 
			return lastHandledRel;
		}		
		else 
		{
			bool retflag;
			int retval = CheckForJoin(compOp, numToJoin, relNames, numOfTuples, writeFlag, retflag);
			if (retflag) 
			return retval;
		}
	}
	return -1;
}

void Statistics::SingletonPredicate(ComparisonOp * compOp, double & numOfTuples, char ** relNames, bool writeFlag, bool & cnfDuplicateFoundFlag, bool & sameAndAttDiffValue)
{
	string att = (compOp->left->code == NAME) ? string(compOp->left->value) : string(compOp->right->value);

	string value = (compOp->left->code == NAME) ? string(compOp->right->value) : string(compOp->left->value);

	att = att.substr(att.find(".") + 1); // split the leftname
	if (attAlias.count(att) != 0)
	{
		att = attAlias[att]; //after join the attllias can contain data(wiil be utlised if any join happened)
	}
	if (compOp->code == EQUALS)
	{ 							// the original number of tuples/the number of distinct attribute values.
		numOfTuples = Addrel[relNames[lastHandledRel]] / Addatt[relNames[lastHandledRel]][att];


		Addrel[string(relNames[lastHandledRel])] = (int)numOfTuples;
		Addatt[relNames[lastHandledRel]][att] = 1;


		// cache the attribute value pair, modify the cnfDuplicateFoundFlag, sameAndAttDiffValue and orSameAttDiffValue to direct decision in evalOr and evalAnd
		if (writeFlag)
		{
			if (andEqualCache.count(att))
			{
				if (andEqualCache[att] == value)
					cnfDuplicateFoundFlag = true; // find a duplicate, the AND should have no effect.
				else
				{
					sameAndAttDiffValue = true; // no duplicates, the AND result should be zero.
					Addrel.erase(string(relNames[lastHandledRel]));
					Addatt.erase(string(relNames[lastHandledRel]));
				}
			}
			else andEqualCache[att] = value;    
		}
		else
		{											
			if (!orEqualCache.count(att)) // this attribute is a new attribute to evalOR
				orEqualCache.insert({ att,value });
			
		}
	}
	else if (compOp->code == LESS_THAN || compOp->code == GREATER_THAN)
	{ 	// reduce the number of tuple and distinct attribute number to a third.
		numOfTuples = Addrel[string(relNames[lastHandledRel])] / 3.0;
		if (writeFlag)
		{
			Addatt[string(relNames[lastHandledRel])][att] = (int)(Addatt[string(relNames[lastHandledRel])][att] / 3.0);
			Addrel[string(relNames[lastHandledRel])] = (int)numOfTuples;
		}
	}
	else
	{
		// the original number of tuples - (the original number of tuples/the number of distinct attribute values).
		numOfTuples = Addrel[string(relNames[lastHandledRel])] / Addatt[string(relNames[lastHandledRel])][att];
		numOfTuples = Addrel[string(relNames[lastHandledRel])] - numOfTuples;
		if (writeFlag) 
		{
			Addrel[string(relNames[lastHandledRel])] = (int)numOfTuples;
			// the number of distinct values left is now reduced by one (all the originals except the one we excluded)
			Addatt[string(relNames[lastHandledRel])][att]--;
		}
	}

}

int Statistics::CheckForJoin(ComparisonOp * compOp, int numToJoin, char ** relNames, double & numOfTuples, bool writeFlag, bool &retflag)
{
	retflag = true;
	if (compOp->left->code == NAME && compOp->right->code == NAME && compOp->code == EQUALS)
	{
		bool lresult = false;
		bool rresult = false;
		int lIndex = 100, rIndex = 100;

		string lOperand(compOp->left->value);
		int dotOffset = (lOperand.find("."));
		char *leftValue = compOp->left->value + (dotOffset + 1);
		string lAttName(leftValue);
		string rOperand(compOp->right->value);
		dotOffset = (rOperand.find("."));
		char *rightValue = compOp->right->value + (dotOffset + 1);
		string rAttName(rightValue);

		// check if left attribute has an alias. If it does, use it
		if (attAlias.find(lAttName) != attAlias.end())
		{
			lAttName = attAlias[lAttName];
		}

		// check if right attribute has an alias. If it does, use it
		if (attAlias.find(rAttName) != attAlias.end())
		{
			rAttName = attAlias[rAttName];
		}

		for (int i = 0; i < numToJoin; i++)
		{
			if (Addatt[string(relNames[i])][lAttName])
			{
				lresult = true;
				lIndex = i;
			}
			if (Addatt[string(relNames[i])][rAttName])
			{
				rresult = true;
				rIndex = i;
			}
		}

		//case to handle self join
		if (rAttName.compare(lAttName) == 0) 
		{
			int count = 0;
			for (int i = 0; i < numToJoin; i++) 
			{
				if (Addatt[string(relNames[i])][lAttName]) 
				{
					if (count == 0)
					{
						lresult = true;
						lIndex = i;
						count = 1;
					}
					else
					{
						rresult = true;
						rIndex = i;
						break;
					}
				}
			}
		}
		
		if (lresult && rresult && (lIndex != rIndex))
		{
			return Join_estimate_update(relNames, lIndex, rIndex, lAttName, rAttName, numOfTuples, writeFlag, rightValue, leftValue);// return the relation index of the result relation so that evalOrList knows what to do.
		}
		else
		{ // attributes not found in the relations
			cerr << "ERROR: Join attributes not found  in relations" << endl;

			cout << "debug information: lIndex, rIndex," << lIndex << " " << rIndex << lresult << rresult << " num to Join " << numToJoin << leftValue << endl;
			return -1;
		}
	}
	retflag = false;
	return {};
}

int Statistics::Join_estimate_update(char ** relNames, int lIndex, int rIndex, std::string &lAttName, std::string &rAttName, double & numOfTuples, bool writeFlag, char * rightValue, char * leftValue)
{
	int leftTuples = Addrel[string(relNames[lIndex])];
	int rightTuples = Addrel[string(relNames[rIndex])];
	int distJoinAttLeft = Addatt[string(relNames[lIndex])][lAttName];
	int distJoinAttRight = Addatt[string(relNames[rIndex])][rAttName];
	numOfTuples = (double)leftTuples * rightTuples / ((distJoinAttLeft > distJoinAttRight) ? distJoinAttLeft : distJoinAttRight);
	if (writeFlag)
	{
		Addrel[string(relNames[lIndex])] = (int)numOfTuples;

		map<string, int> ::iterator x;
		for (x = Addatt[string(relNames[rIndex])].begin(); x != Addatt[string(relNames[rIndex])].end(); ++x)
		{
			if (strcmp(x->first.c_str(), rightValue) != 0)
				Addatt[string(relNames[lIndex])].insert({ x->first, x->second });
		}
	}
	// map the right-hand attribute name to the left-hand attribute name
	attAlias[rightValue] = leftValue;
	if (writeFlag) 
	{
		// delete the right relation from the datastructure
		Addrel.erase(string(relNames[rIndex]));
		Addatt.erase(string(relNames[rIndex]));
	}
	lastHandledRel = lIndex;
	return lIndex;
}


int Statistics::evalOrList(struct OrList *pOr, char **relNames, int numToJoin, double &ORnumOfTuples, bool &ListOrSingleton, bool &cnfDuplicateFoundFlag, bool &sameAndAttDiffValue) {
	if (pOr != NULL) 
	{
		struct ComparisonOp *compOp = pOr->left;
		double numOfTuples = 0;

		
		int *temp = new int[numToJoin]; // records the number of tuples in each relation
		for (int i = 0; i < numToJoin; i++)
			temp[i] = Addrel[relNames[i]];

		// if pOr->rightOr is NULL, then perform write operation
		bool orSameAttDiffValue = false;
		bool writeflag = (pOr->rightOr == NULL && ListOrSingleton);
		int rIndex = evalComparisonOp(compOp, relNames, numToJoin, numOfTuples, writeflag, cnfDuplicateFoundFlag, sameAndAttDiffValue, orSameAttDiffValue);
		int denominator = temp[rIndex];
		delete[]temp;
		double nominator = numOfTuples;

		// aggregate the result.
		if (orSameAttDiffValue && !cnfDuplicateFoundFlag)
			ORnumOfTuples = ORnumOfTuples + nominator;  // not seen equal selection on a already seen attribute
		else if (!orSameAttDiffValue && !cnfDuplicateFoundFlag)                              
			// normal case equi-join or an equal selection on a not seen attribute
			ORnumOfTuples = ORnumOfTuples + nominator - ORnumOfTuples / denominator * nominator;

		if (pOr->rightOr) 
		{
			ListOrSingleton = false;
			bool cnfDuplicateFoundFlag = false;
			rIndex = evalOrList(pOr->rightOr, relNames, numToJoin, ORnumOfTuples, ListOrSingleton, cnfDuplicateFoundFlag, sameAndAttDiffValue);
		}
		return rIndex;
	}
	else {
		return -1;
	}
}


int Statistics::evalAndList(struct AndList *pAnd, char **relNames, int numToJoin, double &ANDnumOfTuples, bool &flag) {
	if (pAnd != NULL) 
	{

		double ORnumOfTuples = 0;
		struct OrList *pOr = pAnd->left; // get the left 

		bool ListOrSingleton = true; // ListOrSingleton true; when OR clause begins. false when the OR clause ends
		bool cnfDuplicateFoundFlag = false;
		bool sameAndAttDiffValue = false;
		int ind = evalOrList(pOr, relNames, numToJoin, ORnumOfTuples, ListOrSingleton, cnfDuplicateFoundFlag, sameAndAttDiffValue);

		
		if (ind != -1)
		{
			Addrel[relNames[ind]] = (int)ORnumOfTuples;
		}		
		if (sameAndAttDiffValue) 
		{ 
			ANDnumOfTuples = 0;
			return ind;
		}
		else
		{
			ANDnumOfTuples = ORnumOfTuples;  
		}

		if (pAnd->rightAnd) 
		{
			ind = evalAndList(pAnd->rightAnd, relNames, numToJoin, ANDnumOfTuples, flag);
		}

		return ind;
	}
	else 
	{
		return -1;
	}
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) 
{
	Statistics copy(*this) ; // create a copy so we don't change the original
	double ANDnumOfTuples;
	bool flag = true;
	lastHandledRel = 0;
	copy.evalAndList(parseTree, relNames, numToJoin, ANDnumOfTuples, flag);
	return ANDnumOfTuples;
}

