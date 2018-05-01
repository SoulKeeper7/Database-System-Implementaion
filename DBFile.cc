#include <fstream>

#include<iostream>
#include "DBFile.h"
#include "HeapFile.h"
#include "GenericDBFileBaseClass.h"
#include "SortedFile.h"
//#include "SortedFile.h"

using std::string;
using std::ifstream;
using std::ofstream;

int DBFile::Create( char* fpath, fType ftype, void* startup) 
{
	
	//Genricdb = new HeapFile;
	//return Genricdb->Create(fpath, startup);
	std::cout<<"metafile creted";
	char file_tbl_path[100];
    sprintf (file_tbl_path, "%s.meta", fpath);
	ofstream out(file_tbl_path);
		std::cout<<"metafile creted---------->";
	
	switch(ftype)
	{
		case 0:Genricdb = new HeapFile();
			out << "heap" << endl;
			break;
		case 1:Genricdb = new SortedFile();
		break;
	}
		return Genricdb->Create(fpath, ftype, startup);;
	}
    
	

int DBFile::Open( char* fpath)
 {
	//Genricdb = new HeapFile;
	//return Genricdb->Open(fpath);
	
		char file_tbl_path[100];
        string line;
        sprintf (file_tbl_path, "%s.meta", fpath);
		ifstream myfile (file_tbl_path);
		//myfile << "sorted" << endl;
	if (myfile.is_open()) 
	{
		if (getline (myfile,line)) 
		{
		    if (line.compare("heap") == 0) 
			{			
	        	Genricdb = new HeapFile();
		    }
			else if (line.compare("sorted") == 0) 
			{
			
	       		Genricdb = new SortedFile();
		   // Genricdb
			}
		}
	}
	else
	{
		Genricdb = new HeapFile();
	}
	return Genricdb->Open(fpath);
}


int DBFile::Close() {
	return Genricdb->Close();
}

void DBFile::Add(Record& addme) {
	return Genricdb->Add(addme);
}

void DBFile::Load(Schema& myschema, char* loadpath) {
	return Genricdb->Load(myschema, loadpath);
}

void DBFile::MoveFirst() {
	return Genricdb->MoveFirst();
}

int DBFile::GetNext(Record& fetchme) {
	return Genricdb->GetNext(fetchme);
}

int DBFile::GetNext(Record& fetchme, CNF& cnf, Record& literal) {
	return Genricdb->GetNext(fetchme, cnf, literal);
}

DBFile::DBFile() {}
DBFile::~DBFile() { delete Genricdb; }



