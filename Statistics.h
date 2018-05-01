#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <vector>
#include<set>
#include<string>

using namespace std;
class Statistics
{
private:
	map <string, int> Addrel;
	map <string, map<string, int> > Addatt;
	map<string, string> attAlias;
	map<string, string> andEqualCache;
	multimap<string, string> orEqualCache;
	
	bool isCalledFrmApply;
	bool isApply;
public:
	int lastHandledRel;
	Statistics();
	
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	int evalComparisonOp(ComparisonOp * pCom, char ** relNames, int numToJoin, double & numOfTuples, bool writeFlag, bool & cnfDuplicateFoundFlag, bool & sameAndAttDiffValue, bool & orSameAttDiffValue);
	void SingletonPredicate(ComparisonOp * compOp, double & numOfTuples, char ** relNames, bool writeFlag, bool & cnfDuplicateFoundFlag, bool & sameAndAttDiffValue);
	int CheckForJoin(ComparisonOp * compOp, int numToJoin, char ** relNames, double & numOfTuples, bool writeFlag, bool &retflag);
	int Join_estimate_update(char ** relNames, int lIndex, int rIndex, std::string &lAttName, std::string &rAttName, double & numOfTuples, bool writeFlag, char * rightValue, char * leftValue);
	int evalOrList(OrList * pOr, char ** relNames, int numToJoin, double & ORnumOfTuples, bool & ListOrSingleton, bool & cnfDuplicateFoundFlag, bool & sameAndAttDiffValue);
	int evalAndList(AndList * pAnd, char ** relNames, int numToJoin, double & ANDnumOfTuples, bool & flag);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	void cardinalityfactor(bool isJoin, string &leftRelation, ComparisonOp * currentCompOp, string &rightRelation, double &selCarOr, string &joinLeftRelation, string &joinRightRelation, bool isdep, bool &rightOrPresent, map<string, int> &relOpMap);

	void CallApply(bool isJoinPerformed, map<string, int> &relOpMap, char ** relNames, string &joinLeftRelation,string &joinRightRelation, double resultEstimate);

	void insertnewrealtionJoin(set<string> &addedJoinAttrSet, string & joinLeftRelation, string &relkey,string & joinRightRelation);

	void deletesingletonrelation(string &relkey, double resultEstimate, string & joinLeftRelation, string & joinRightRelation);

};

#endif
