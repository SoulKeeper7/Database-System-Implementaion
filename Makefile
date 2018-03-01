CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapFile.o GenericDBFileBaseClass.o  y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o  DBFile.o HeapFile.o  GenericDBFileBaseClass.o  y.tab.o lex.yy.o test.o -lfl -lpthread
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o DBFile.o HeapFile.o  GenericDBFileBaseClass.o   y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o  DBFile.o HeapFile.o  GenericDBFileBaseClass.o  y.tab.o lex.yy.o main.o thread1 -lfl -lpthread
	
test.o: test.cc
	$(CC) -g -c test.cc 

main.o: main.cc
	$(CC) -g -c main.cc

test.o: test.cc
	$(CC) -g -c test.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc	

	
GenericDBFileBaseClass.o: GenericDBFileBaseClass.cc
	$(CC) -g -c GenericDBFileBaseClass.cc
	
SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
