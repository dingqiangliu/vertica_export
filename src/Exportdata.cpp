/* Copyright (c) 2005 - 2014 Vertica, an HP company -*- C++ -*- */
/*
 * Description: User Defined Transform Function: concurrently exporting data to files.
 *
 * Create Date: Dec 15, 2011
 */

#include "Vertica.h"
#include <sstream>
#include <fstream>
#include <iconv.h>

#include "ProcessLaunchingPlugin.h"

using namespace Vertica;
using namespace std;
using namespace Basics;

#define DEFAULT_path ""
#define DEFAULT_cmd ""
#define DEFAULT_buffersize 1024
#define DEFAULT_separator "|"

#define I Int8OID
#define F Float8OID
#define V VarcharOID
#define O 0

#define MACRO_NODENAME "${nodeName}"
#define MACRO_HOSTNAME "${hostName}"


class CodeConverter {
private:
	iconv_t cd;
public:
	DataBuffer ccbuf;

    CodeConverter(const char *from_charset, const char *to_charset) {
        ccbuf.offset = 0;
        ccbuf.size = 65536;
        ccbuf.buf = (char*)malloc(ccbuf.size);
        cd = iconv_open(to_charset, from_charset);
    }

	~CodeConverter() {
		if(cd != NULL) {
			iconv_close(cd);
			cd = NULL;
		}
		if(ccbuf.buf != NULL) {
			free(ccbuf.buf);
			ccbuf.offset = 0;
			ccbuf.size = 65536;
			ccbuf.buf = NULL;
		}
	}

	char* convert(char *str, size_t inlen) {
		if( inlen * 3/2 + 1 > ccbuf.size) {
			free(ccbuf.buf);
			ccbuf.size = inlen * 3/2 + 1;
			ccbuf.buf = (char*)malloc(ccbuf.size);
		}

		size_t outlen = ccbuf.size;
		char* outptr = ccbuf.buf;

		memset(ccbuf.buf, 0, ccbuf.size);
		iconv(cd, &str, &inlen , &outptr, &outlen);
		ccbuf.offset = ccbuf.size-outlen;

		return ccbuf.buf;
	}
};

class Output{
public:
	Output(){}
	virtual ~Output(){}
	
	virtual void open(std::string cmd) = 0;
	virtual void flush() = 0;
	virtual void close() = 0;
	virtual Output& operator<< (const char* s ) = 0;
};


class FileOutput: public Output {
    private:
        int buffersize;
        ofstream ofs;

    public:
        FileOutput(int buffersize): buffersize(buffersize){}

        void open(std::string path){
            ofs.open (path.c_str(), ofstream::out | ofstream::app);
        }
        
        void flush(){
            ofs.flush();
        }

        void close(){
            ofs.close();
        }
        
        Output& operator<< (const char* s ){
            ofs << s;
            return *this;
        }
};

class CmdOutput: public Output {
private:
    int buffersize;

	ProcessLaunchingPlugin* plp;
	DataBuffer bfIn;
	DataBuffer bfOut;

public:
	CmdOutput(int buffersize): buffersize(buffersize){
		plp = NULL;
	}
	
	~CmdOutput(){
		close();
	}
	
	void open(std::string cmd){
        std::vector<std::string> env;

		//open subprocess for saving data
		if(plp != NULL){
			plp->destroyProcess();
			plp = NULL;
		}
		plp = new ProcessLaunchingPlugin(cmd, env);
		plp->setupProcess();

		bfIn.offset = 0;
		bfIn.size = 0;
		bfIn.buf = (char*)malloc(buffersize);
		//memset(bfIn.buf, 0, buffersize);

		bfOut.offset = 0;
		bfOut.size = buffersize;
		bfOut.buf = (char*)malloc(buffersize);
		//memset(bfOut.buf, 0, buffersize);
	}
	
	void flush(){
		flush(false);
	}

	void flush(bool bClose){
		StreamState ss = OUTPUT_NEEDED;
		bfOut.offset = 0;
		bfOut.size = buffersize;
		while( ( (bfIn.offset != bfIn.size) && (ss==INPUT_NEEDED) ) || (ss==OUTPUT_NEEDED) ){
			InputState input_state = bClose? END_OF_FILE: OK;
			ss = plp->pump(bfIn, input_state, bfOut);
			// ignore output
			bfOut.offset = 0;
			bfOut.size = buffersize;
		}
	}
  	
	void close(){
		//close sub process for data saving cmd

        // mod this to tear down all of the command pipe
		if(plp != NULL){
			flush(true);
			plp->destroyProcess();
			plp = NULL;
		}

		if(bfIn.buf != NULL) {
			free(bfIn.buf);
			bfIn.offset = 0;
			bfIn.size = 0;
			bfIn.buf = NULL;
		}

		if(bfOut.buf != NULL) {
			free(bfOut.buf);
			bfOut.offset = 0;
			bfOut.size = buffersize;
			bfOut.buf = NULL;
		}
	}
	
	Output& operator<< (const char* s ){
		// output to sub process data saving cmd
		char* p=(char*)s;
		while((p != NULL) && (*p != 0x0)) {
			 size_t len = strlen(p);
			 
			 // Note: in ProcessLaunchingPlugin,  
			 //   input: [offset, size) is used buffer, [0, offset) and  [size, buffersize) are unused.
			 //   output: [0, offset) is used buffer, [offset, size) are unused
			 if(len > buffersize - bfIn.size){
			 	flush();
			 	if(bfIn.offset == bfIn.size){
			 		bfIn.offset = 0;
			 		bfIn.size = 0;
			 	}
			 	else{
			 		// move buffer
			 		memmove(bfIn.buf, bfIn.buf+bfIn.offset, bfIn.size-bfIn.offset);
			 		bfIn.offset = 0;
			 		bfIn.size = bfIn.size-bfIn.offset;
			 	}
			 }
			 if(len > buffersize - bfIn.size){
			 	len = buffersize - bfIn.size;
			 }

			 memcpy(bfIn.buf + bfIn.size, p, len);
			 bfIn.size += len;
			 p += len;
		}
		
		return *this;
	}
};



/*
 * 
 */

class Exportdata : public TransformFunction
{
private:
	std::string path;
	std::string cmd;
    int buffersize;
	std::string separator;
    std::string fromcharset;
    std::string tocharset;
    
	Output *os;
    CodeConverter* pCodeConverter;

public:
	Exportdata() {
		buffersize = DEFAULT_buffersize;
		os = NULL;
		pCodeConverter = NULL;
	}

	virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &argTypes){
        path = DEFAULT_path;
        separator = DEFAULT_separator;

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        if (paramReader.containsParameter("path")){
            path = paramReader.getStringRef("path").str();
        }
    	size_t found = path.find(MACRO_NODENAME);
    	if(found!=std::string::npos){
    		path.replace(found, strlen(MACRO_NODENAME), srvInterface.getCurrentNodeName());
    	}
    	found = path.find(MACRO_HOSTNAME);
    	if(found!=std::string::npos){
    		path.replace(found, strlen(MACRO_HOSTNAME), getenv("HOSTNAME"));
    	}

        cmd =  DEFAULT_cmd;
        if (paramReader.containsParameter("cmd"))
            cmd = paramReader.getStringRef("cmd").str();
        found = cmd.find(MACRO_NODENAME);
    	if(found!=std::string::npos){
    		cmd.replace(found, strlen(MACRO_NODENAME), srvInterface.getCurrentNodeName());
    	}
        found = cmd.find(MACRO_HOSTNAME);
    	if(found!=std::string::npos){
            cmd.replace(found, strlen(MACRO_HOSTNAME), getenv("HOSTNAME"));
    	}

        if (paramReader.containsParameter("buffersize"))
            buffersize = paramReader.getIntRef("buffersize");

        if (paramReader.containsParameter("separator")){
            separator = paramReader.getStringRef("separator").str();
        }

        if (paramReader.containsParameter("fromcharset"))
            fromcharset = paramReader.getStringRef("fromcharset").str();
        if (paramReader.containsParameter("tocharset"))
            tocharset = paramReader.getStringRef("tocharset").str();

        if(path.compare(DEFAULT_path) != 0) {
            // use system utilities to determine if that path is writable
            if(system(NULL)){
                if(system(("mkdir -p $(dirname " + path + ")").c_str()) || system(("touch " + path).c_str())){
                    vt_report_error(1234, ("Cannot write to " + path).c_str());
                }
            }
	    	// output to file
        	os = new FileOutput(buffersize);
        	(*os).open(path);
		}
        else if(cmd.compare(DEFAULT_cmd) != 0) {
	    	// output to saving cmd
        	os = new CmdOutput (buffersize);
        	(*os).open(cmd);
	    }

		if( !fromcharset.empty() && !tocharset.empty() && (fromcharset.compare(tocharset) != 0) ) {
	    	pCodeConverter = new CodeConverter(fromcharset.c_str(), tocharset.c_str());
	    }
	}
	
	virtual void destroy (ServerInterface &srvInterface, const SizedColumnTypes &argTypes){
        if( os != NULL ) {
        	(*os).close();
		    delete os;
		    os = NULL;
	    }

		if(pCodeConverter != NULL) {
			delete pCodeConverter;
			pCodeConverter = NULL;
		}
	}

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer)
    {
        if( os == NULL ) {
        	vt_report_error(0, "Parameter path and cmd must not be empty all");
        }

        if (input_reader.getNumCols() < 1)
            vt_report_error(0, "Function need 1 argument at least, but %zu provided", input_reader.getNumCols());
		
		int lines = 0;
		
        int columncount = input_reader.getNumCols();
        SizedColumnTypes types = input_reader.getTypeMetaData();
        std::string row = ""; // use row as a buffer for each row, this is an attempt to insure atomic writes
        size_t max_row_size = DEFAULT_buffersize;
        row.reserve(max_row_size);
        const int COLUMNS_SIZE = 8192-1;
        char value[COLUMNS_SIZE+1];
        do {
            for(int i=0; i<columncount; i++) {
                if (i > 0)  row.append(separator.c_str());

                const VerticaType &vt = types.getColumnType(i);
                if(vt.isBool()) {
                    bool elem =  input_reader.getBoolRef(i);
                    if(elem != vbool_null){
                        elem == vbool_true ? row.append("t"): row.append("f");
                    }
                }
                else if(vt.isInt()) {
                    vint elem =  input_reader.getIntRef(i);
                    if (elem != vint_null ) {
                        sprintf(value, "%lli", (long long)elem);
                        row.append(value);
                    }
                }
                else if(vt.isFloat()) {
                    vfloat elem =  input_reader.getFloatRef(i);
                    if (!vfloatIsNull(elem)) {
                        sprintf(value, "%.15g", (double)elem);
                        row.append(value);
                    }
                }
                else if(vt.isNumeric()){
                    VNumeric elem = input_reader.getNumericRef(i);
                    if(!elem.isNull()){
                        elem.toString(value, vt.getNumericPrecision() + 3);
                        row.append(value);
                    }
                }
                else if(vt.isDate()) {
                    DateADT elem =  input_reader.getDateRef(i);
                    if((unsigned)elem != DT_NULL){
                        dateToChar(elem, value, COLUMNS_SIZE, USE_ISO_DATES, true);
                        row.append(value);
                    }
                }
                else if(vt.isTimestamp()) {
                    Timestamp elem = input_reader.getTimestampRef(i);
                    if ((unsigned)elem != DT_NULL) {
                        timestampToChar(elem, value, COLUMNS_SIZE, USE_ISO_DATES, true);
                        row.append(value);
                    }
                }
                else if(vt.isTimestampTz()){
                    TimestampTz elem = input_reader.getTimestampTzRef(i);
                    if((unsigned)elem != DT_NULL){
                        timestamptzToChar(elem, value, COLUMNS_SIZE, USE_ISO_DATES, true);
                        row.append(value);
                    }
                }
                else if(vt.isTime()){
                    TimeADT elem = input_reader.getTimeRef(i);
                    if((unsigned)elem != DT_NULL){
                        timeToChar(elem, value, COLUMNS_SIZE, true);
                        row.append(value);
                    }
                }
                else if(vt.isTimeTz()){
                    TimeTzADT elem = input_reader.getTimeTzRef(i);
                    if((unsigned)elem != DT_NULL){
                        timetzToChar(elem, value, COLUMNS_SIZE, true);
                        row.append(value);
                    }
                }
                else if(vt.isInterval()) {
                    Interval elem = input_reader.getIntervalRef(i);
                    if((unsigned)elem != DT_NULL){
                        intervalToChar(elem, INTERVAL_TYPMOD(6, INTERVAL_DAY2SECOND), value, COLUMNS_SIZE, USE_SQL_DATES, true);
                        row.append(value);
                    }
                }
                else if(vt.isIntervalYM()) {
                    IntervalYM elem = input_reader.getIntervalYMRef(i);
                    if((unsigned)elem != DT_NULL){
                        intervalToChar(elem, INTERVAL_TYPMOD(0, INTERVAL_YEAR2MONTH), value, COLUMNS_SIZE, USE_SQL_DATES, true);
                        row.append(value);
                    }
                }
                else if(vt.isBinary() || vt.isLongVarbinary() || vt.isVarbinary()) {
                    const VString& elem = input_reader.getStringRef(i);
                    if (! elem.isNull()) {
                        const char * ba = elem.data();
                        vsize  size = elem.length();
                        for(vsize i = 0; i < size; i++){
                            sprintf(value, "%02x", ba[i]);
                            row.append(value);
                        }
                    }
                }
                else if (vt.isUuid()){ 
                    const VUuid& elem = input_reader.getUuidRef(i);
                    if (! elem.isNull()) {
                        row.append(elem.toString());
                    }
                }
                else if (vt.isStringType()){ 
                    const VString& elem = input_reader.getStringRef(i);
                    if (! elem.isNull()) {
                        if(pCodeConverter != NULL) {
                            row.append(pCodeConverter->convert((char *)elem.str().c_str(), (size_t)elem.str().length()));
                        } else{
                            row.append(elem.str());
                        }
                    }
                }
                else {
                    vt_report_error(1, "Unsupported type [%s] of column [%zu]", vt.getTypeStr(), i+1);
                }
            }       
            row.append("\n");
            *os << row.c_str();
            if(row.length() > max_row_size){ // check if our string is too small
                do {
                    max_row_size = max_row_size * 2; // resize it until its large enough
                } while(row.length() > max_row_size);
                row.reserve(max_row_size); // set the capacity once we have a good size
            }
            row.clear();

            lines ++;
        } while (input_reader.next());
		
		os->flush();

	    // User defined transform must return at least one column
	    if((path.compare(DEFAULT_path) != 0) || (cmd.compare(DEFAULT_cmd) != 0)) {
			char value[255];
			sprintf(value, "exported [%d] lines on node [%s(%s)]", lines, srvInterface.getCurrentNodeName().c_str(), getenv("HOSTNAME"));
		    
		    output_writer.getStringRef(0).copy(value);
	    	output_writer.next();
		}
    }
};

class ExportdataFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();

        argTypes.addAny();
        
        // Note: need not add any type to returnType. empty returnType means any columns and types!
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &input_types,
                               SizedColumnTypes &output_types)
    {
        if (input_types.getColumnCount() < 1)
            vt_report_error(0, "Function need 1 argument at least, but %zu provided", input_types.getColumnCount());

	    output_types.addVarchar(65000, "result");
	}

    // Defines the parameters for this UDSF. Works similarly to defining
    // arguments and return types.
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        //parameter: data file path
        parameterTypes.addVarchar(65000, "path");
        //parameter: data saving cmd
        parameterTypes.addVarchar(65000, "cmd");
        //parameter: buffersize for saving
        parameterTypes.addInt("buffersize");
        //parameter: separator string for concatenating, default value is '|'.
        parameterTypes.addVarchar(200, "separator");

        parameterTypes.addVarchar(200, "fromcharset");
        parameterTypes.addVarchar(200, "tocharset");
    }


    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
    	return vt_createFuncObj(srvInterface.allocator, Exportdata); 
    }

};

RegisterFactory(ExportdataFactory);

