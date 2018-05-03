#ifndef TEST_H
#define TEST_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>
#include "Function.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include <map>

using namespace std;
const char *settings = "test.cat";
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;


char *Savedstate= "SavedDBState.txt";
streambuf * buf = std::cout.rdbuf();
ofstream of;
ostream out(buf);

class relation 
{

private:
	char *rname;
	char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (char *_name, Schema *_schema, char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) 
	{
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	char* name () { return rname; }
	char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info ()
	{
		cout << " relation info\n";
		cout << "\t name: " << name () << endl;
		cout << "\t path: " << path () << endl;
	}
	
};



void PrintOutputSchema(Schema* s)
{
	cout << ":::::Output Schema:::::" <<endl;
	for (int i = 0; i<s->GetNumAtts(); i++)
	{
		Attribute *att = s->GetAtts();
		cout << att[i].name << "::";
		switch (att[i].myType) {
		case 0:
			cout << "Int" << "   ";
			break;
		case 1:
			cout << "Double" << "   ";
			break;
		case 2:
			cout << "String" << "   ";
			break;
		}
	}
}
void PrintNameList(NameList *names)
{
	while (names != NULL)
	{
		cout << "    " << names->name << "  ";
		names = names->next;
	}
}


map<string, relation*>DBinfo;
map<string, string>aliastotable;
relation *rel;
map<string, relation*> Tableandchar;
vector<string> createdTable;
map<string, relation*> createdrelationlist;


char *supplier = "supplier"; 
char *partsupp = "partsupp"; 
char *part = "part"; 
char *nation = "nation"; 
char *customer = "customer"; 
char *orders = "orders"; 
char *region = "region"; 
char *lineitem = "lineitem"; 

relation *s, *p, *ps, *n, *l, *r, *o, *c;

void setup () 
{
	FILE *fp = fopen (settings, "r");
	if (fp) 
	{
		char *mem = (char *) malloc (80 * 3);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	else {
		cerr << " Test settings files 'test.cat' missing \n";
		exit (1);
	}
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";
	
	/*s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
	l = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);*/
	//cout << "yaha ho raha hai";
}




void cleanup () 
{
	delete s, p, ps, n, l, r, o, c;
	free (catalog_path);
}

#endif
