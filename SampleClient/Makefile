CC=g++
CXX=g++
LD=g++

EXESRC=FileWordCounter.cpp SampleClient.cpp
EXEOBJ=$(EXESRC:.cpp=.o)

INCS=-I. -I..
CFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
CXXFLAGS =  -Wall -std=c++11 -pthread -g $(INCS)  
LDFLAGS = -L.. -L. -lMapReduceFramework

EXEFILE = FileWordCounter 
EXESIMPLE = SampleClient
TARGETS = $(EXEFILE) $(EXESIMPLE)

TAR=tar
TARFLAGS=-cf
TARNAME=sampleclient.tar
TARSRCS=$(EXESRC) Makefile $(TEST_TAR)

all: $(TARGETS)

$(TARGETS): %: %.o
	$(LD) $(CXXFLAGS) $< $(LDFLAGS) -o $@

	
clean:
	$(RM) $(TARGETS) $(EXE) $(OBJ) $(EXEOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)
