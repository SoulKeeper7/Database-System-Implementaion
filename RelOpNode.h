#ifndef GEN_TREE_NODE
#define GEN_TREE_NODE
#include "ParseTree.h"
#include "Comparison.h"
#include "myUtils.h"
#include "Defs.h"
#include "RelOp.h"
#include <map>

class RelOpNode 
{
protected:
	Schema * relschema;

public:
	RelOpNode * left = NULL;
	RelOpNode* right = NULL;	
	Pipe* outpipe = new Pipe(100);
	int pipeID;	

	virtual Schema* schema() {};
	virtual ~RelOpNode() {};
	virtual void Print() {};
	virtual void Run() {};
	virtual void WaitUntilDone() {};
	
};
#endif
