#ifndef GENERICTREENODE_
#define GENERICTREENODE_
#include <string>
#include <iostream>
#include <unordered_map>
#include <sstream>
#include "ParseTree.h"
#include "Comparison.h"
#include "a3.h"

using namespace std;
class GenericTreeNode
{

protected:
	Schema * rschema;
public:
	GenericTreeNode * left;
	GenericTreeNode* right;
	// the outpipe that output according to the output schema.
	Pipe* outpipe;
	int pipeID;

	GenericTreeNode() {
		left = NULL;
		right = NULL;

		// create the output pipe so that we can call Run on the nodes
		// without worrying about the order of traversal of the tree
		// (we actually use pre-order traversal, but this way, any
		// traversal method would work)
		outpipe = new Pipe(100);
	};

	virtual Schema* schema() {};
	virtual ~GenericTreeNode() {};
	virtual void Print() {};
	virtual void Run() {};
	virtual void WaitUntilDone() {};
};
#endif

