/* Copyright (c) 2005 - 2011 Vertica, an HP company -*- C++ -*- */
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
			// igore output
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
			 //   input: [offset, size) is used buffer, [0, offset) and  [size, buffersize) are unsued.
			 //   output: [0, offset) is used buffer, [offset, size) are unsed
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
	    	// output to file
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
        char value[8192];
        do {
	        for(int i=0; i<columncount; i++) {
                if (i > 0)  row.append(separator.c_str());

			    const VerticaType &vt = types.getColumnType(i);
                    // if(vt.isBinary() || vt.isLongVarbinary() || vt.isVarbinary()) {
                        // const unsigned char * elem = (const unsigned char *)input_reader.getStringRef(i).data();
                        // for(int b = 0; b < sizeof(elem); b++){
                            // sprintf(value,"\%3o", elem[b]);
                            // row.append(value);
                        // }
                    // }
				if(vt.isBinary ()) {
					// TODO: ignore
				}
                else if(vt.isBool()) {
                    bool elem =  input_reader.getBoolRef(i);
                    if(elem != vbool_null){
                        elem == vbool_true ? row.append("t"): row.append("f");
				    }
				}
				else if(vt.isDate()) {
                    struct tm * dt;
					DateADT elem =  input_reader.getDateRef(i);
                    if((unsigned)elem != DT_NULL){
                        time_t rawtime = getUnixTimeFromDate(elem);
                        dt = gmtime(&rawtime);
                        strftime(value, sizeof(value), "%4Y-%2m-%2d", dt);
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
				else if(vt.isInt()) {
					vint elem =  input_reader.getIntRef(i);
			        if (elem != vint_null ) {
                            sprintf(value, "%lli", (long long)elem);
                            row.append(value);
                    }
                }
                else if(vt.isInterval()) {
                    Interval elem = input_reader.getIntervalRef(i);
                    if((unsigned)elem != DT_NULL){
                        int64 days, hour, min;
                        float sec;
                        VInterval::breakUp(elem, days, hour, min, sec);
                        
                        if (days > 0 || hour > 0 || min > 0 || sec > 0) {
                            if(days > 0){
                                sprintf(value, "%d", (int)days);
                                row.append(value);
                            }
                            if (hour > 0 || min > 0 || sec > 0) {
                                std::string format;
                                if(days > 0){
                                     format = " %02d";
                                } else {
                                     format = "%02d";
                                }
                                if(min > 0 || sec > 0){
                                    format.append(":%02d");
                                }
                                if(sec > 0){
                                    format.append(":%02d");
                                }
                                sprintf(value, format.c_str(), (int)hour, (int)min, (int)sec);
                                row.append(value);

                                int subseconds = (int)(elem % usPerSecond);
                                if(subseconds > 0){
                                    row.append(".");
                                    if(subseconds < 100000){
                                        row.append("0");
                                    }
                                    if(subseconds < 10000){
                                        row.append("0");
                                    }
                                    if(subseconds < 1000){
                                        row.append("0");
                                    }
                                    while(subseconds % 10 == 0){
                                        subseconds = subseconds / 10;
                                    }
                                    sprintf(value, "%d", subseconds);
                                    row.append(value);
                                }
                            }
                        } else {
                            sprintf(value, "%d", 0);
                            row.append(value);
                        }
                    }
                }
                else if(vt.isIntervalYM()) {
                    IntervalYM elem = input_reader.getIntervalYMRef(i);
                    if((unsigned)elem != DT_NULL){
                        int64 years, months;
                        VIntervalYM::breakUp(elem, years, months);

                        sprintf(value, "%d", (int)years);
                        row.append(value);
                        row.append("-");
                        sprintf(value, "%d", (int)months);
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
                else if (vt.isStringType()){ // catches VARCHAR/CHAR/VARBINARY/BINARY/LONG VARCHAR/LONG VARBINARY
                    const VString& elem = input_reader.getStringRef(i);
                    if (! elem.isNull()) {
                        if(pCodeConverter != NULL) {
                            row.append(pCodeConverter->convert((char *)elem.str().c_str(), (size_t)elem.str().length()));
                        } else{
                            row.append(elem.str());
                        }
                    }
                }
                else if(vt.isTimestamp()) {
                    Timestamp elem = input_reader.getTimestampRef(i);
                    if ((unsigned)elem != DT_NULL) {
                        struct tm * dt;
                        time_t rawtime = getUnixTimeFromTimestamp(elem);

                        dt = gmtime(&rawtime);
                        // if(dt->tm_isdst != 0){
                            // sprintf(value, "%+d", dt->tm_isdst);
                            // row.append(value);
                        // }
                        
                        int subseconds = (int)(elem % usPerSecond);
                        if(subseconds < 0){
                            dt->tm_sec -= 1;
                            mktime(dt);
                        }
                        strftime(value, sizeof(value), "%4Y-%2m-%2d %2H:%2M:%2S", dt);
                        row.append(value);
                        if(subseconds != 0){
                            if(subseconds < 0){
                                subseconds = subseconds + 1000000;
                            }
                            row.append(".");
                            if(subseconds < 100000){
                                row.append("0");
                            }
                            if(subseconds < 10000){
                                row.append("0");
                            }
                            if(subseconds < 1000){
                                row.append("0");
                            }
                            while(subseconds % 10 == 0){
                                subseconds = subseconds / 10;
                            }
                            sprintf(value, "%d", subseconds);
                            row.append(value);
                        }
                    }
                }
                else if(vt.isTimestampTz()){
                    TimestampTz elem = input_reader.getTimestampTzRef(i);
                    if((unsigned)elem != DT_NULL){
                        struct tm * dt;
                        time_t rawtime = getUnixTimeFromTimestampTz(elem);

                        dt = localtime(&rawtime);

                        int subseconds = (int)(elem % usPerSecond);
                        if(subseconds < 0){
                            dt->tm_sec -= 1;
                            mktime(dt);
                        }

                        strftime(value, sizeof(value), "%4Y-%2m-%2d %2H:%2M:%2S", dt);
                        row.append(value);

                        if(subseconds != 0){
                            if(subseconds < 0){
                                subseconds = subseconds + 1000000;
                            }
                            row.append(".");
                            if(subseconds < 100000){
                                row.append("0");
                            }
                            if(subseconds < 10000){
                                row.append("0");
                            }
                            if(subseconds < 1000){
                                row.append("0");
                            }
                            while(subseconds % 10 == 0){
                                subseconds = subseconds / 10;
                            }
                            sprintf(value, "%d", subseconds);
                            row.append(value);
                        }
                        int32 zone;
                        strftime(value, sizeof(value), " %z", dt); // pull this separately to allow sub second precision to be maintained
                        sscanf(value, "%d", &zone);
                        int32 zoneMinutes = zone % 100;
                        zone = zone / 100;
                        sprintf(value, "%+03d",zone); // pull this separately to allow sub second precision to be maintained
                        row.append(value);
                        if(zoneMinutes != 0) {
                            sprintf(value, ":%02d", abs(zoneMinutes));
                            row.append(value);
                        }

                        // add check for partial hour offset (such as Central Australia, Newfoundland, Nepal, and India)
                    }
                }
                else if(vt.isTime()){
                    TimeADT elem = input_reader.getTimeRef(i);
                    if((unsigned)elem != DT_NULL){
                        struct tm * dt;
                        time_t rawtime = getUnixTimeFromTime(elem);
                        dt = gmtime(&rawtime);
                        strftime(value, sizeof(value), "%2H:%2M:%2S", dt);
                        row.append(value);
                        int subseconds = (int)(elem % usPerSecond);
                        if(subseconds > 0){
                            row.append(".");
                            if(subseconds < 100000){
                                row.append("0");
                            }
                            if(subseconds < 10000){
                                row.append("0");
				            }
                            if(subseconds < 1000){
                                row.append("0");
                            }
                            while(subseconds % 10 == 0){
                                subseconds = subseconds / 10;
                            }
                            sprintf(value, "%d", subseconds);
                            row.append(value);
                        }
                    }
                }
                else if(vt.isTimeTz()){
                    TimeTzADT elem = input_reader.getTimeTzRef(i);
                    if((unsigned)elem != DT_NULL){
                        struct tm * dt;
                        int32 zone=getZoneTz(elem);
                        int64 elemTime=getTimeTz(elem);
                        time_t rawtime = getUnixTimeFromTime(elemTime);
                        dt = gmtime(&rawtime);
                        strftime(value, sizeof(value), "%2H:%2M:%2S", dt);
                        row.append(value);
                        int subseconds = (int)(elemTime % usPerSecond);
                        if(subseconds > 0){
                            row.append(".");
                            if(subseconds < 100000){
                                row.append("0");
                            }
                            if(subseconds < 10000){
                                row.append("0");
                            }
                            if(subseconds < 1000){
                                row.append("0");
                            }
                            while(subseconds % 10 == 0){
                                subseconds = subseconds / 10;
                            }
                            while(subseconds % 10 == 0){
                                subseconds = subseconds / 10;
                            }
                            sprintf(value, "%d", subseconds);
                            row.append(value);
                        }
                        sprintf(value, "%+03d",-1*zone/60/60); // pull this separately to allow sub second precision to be maintained
                        // strftime(value, sizeof(value), " %z", dt); // pull this separately to allow sub second precision to be maintained
                        row.append(value);
                        int32 zoneMinutes = (zone/60) % 60;
                        if(zoneMinutes != 0) {
                            sprintf(value, ":%02d", abs(zoneMinutes));
                            row.append(value);
				        }
				    }
				}
				else {
                    // catches char/varchar/long varchar
			        const VString& elem = input_reader.getStringRef(i);
			        if (! elem.isNull()) {
					    if(pCodeConverter != NULL) {
                            row.append(pCodeConverter->convert((char *)elem.str().c_str(), (size_t)elem.str().length()));
					    }
					    else{
                            row.append(elem.str());
					    }
			        }
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
