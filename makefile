############################
# Vertica Analytic Database
#
# Makefile to build export user defined functions
#
# To run under valgrind:
#   make RUN_VALGRIND=1 run
#
# Copyright 2012 Vertica, 2012
############################

SDK_HOME?=/opt/vertica/sdk
VSQL?=/opt/vertica/bin/vsql

# Define the .so name here (and update the references in install.sql and uninstall.sql)
PACKAGE_LIBNAME=lib/exportdata.so

CXX=g++
# CXXFLAGS=-I ${SDK_HOME}/include -g -Wall -Wno-unused-value -shared -fPIC 

# CXXFLAGS := $(CXXFLAGS) -I $(SDK_HOME)/include -I $(SDK_HOME)/examples/HelperLibraries -O0 -g -fPIC -shared 
CXXFLAGS := $(CXXFLAGS) -I $(SDK_HOME)/include -I $(SDK_HOME)/examples/HelperLibraries -O0 -g -Wall -Wno-unused-value -fPIC -shared -DNO_SUDO


ifdef RUN_VALGRIND
VALGRIND=valgrind --leak-check=full
endif

.PHONEY: simulator run

all: ${PACKAGE_LIBNAME}

${PACKAGE_LIBNAME}: src/*.cpp src/*.c ${SDK_HOME}/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h $(SDK_HOME)/examples/HelperLibraries/LoadArgParsers.h
	mkdir -p lib
	$(CXX) $(CXXFLAGS) -o $@ src/*.cpp src/*.c ${SDK_HOME}/include/Vertica.cpp 

# Targets to install and uninstall the library and functions
install: $(PACKAGE_LIBNAME) uninstall ddl/install.sql
	$(VSQL) -f ddl/install.sql
uninstall: ddl/uninstall.sql
	$(VSQL) -f ddl/uninstall.sql

# run examples
run: $(PACKAGE_LIBNAME) install test/test.sql
	$(VSQL) -f test/test.sql | tee testresult.txt

clean:
	[ -d lib ] && rm -rf lib || true
	[ -f testresult.txt ] && rm -f testresult.txt || true
	(ls test/export* 2>&1) > /dev/null && rm -f test/export* || true
