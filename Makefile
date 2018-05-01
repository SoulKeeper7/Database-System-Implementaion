CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif



main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o DBFile.o HeapFile.o SortedFile.o GenericDBFileBaseClass.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o DBFile.o HeapFile.o SortedFile.o GenericDBFileBaseClass.o BigQ.o RelOp.o Function.o  Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o -lfl -lpthread

main.o: main.cc
	$(CC) -g -c main.cc

a3.o: a3.cc
	$(CC) -g -c a3.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc	

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc	

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc	
	
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

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc
	
Function.o: Function.cc
	$(CC) -g -c Function.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Statistics.o:Statistics.cc
	$(CC) -g -c Statistics.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
	
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c
	
lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
