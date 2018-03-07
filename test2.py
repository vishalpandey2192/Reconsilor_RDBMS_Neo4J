import linecache

def getRequestIfFailed(requestId, start_line):
    with open("log2.txt") as fp:
        for end_line, line in enumerate(fp):
            if line is not None:
                if "Transaction failed" in line:
                    id = line.split("-")[-2].split(":")[-1].strip()
                    if(id==requestId):
                        json = getFailedJSON(start_line,end_line+1)
                        return json
    return ;


def getFailedJSON(start_line,end_line):
    line=''
    json_end=0
    json=''
    for i in range(start_line+1,end_line+1):
        line = linecache.getline('log2.txt', i)[8:]
        if line is not None:
            if "Examining source dictionary" in line:
                break
            else:
                json = json + line
    return "{"+json

def generateDicts(log_fh):
    start_line = 0
    requests_dict = {}
    for line in log_fh:
        if line is not None:
            start_line = start_line + 1
            if "Start RequestId:" in line:
                requestId=line.split("-")[-1].split(":")[-2].strip()
                request = getRequestIfFailed(requestId,start_line)
                if(request != None):
                    requests_dict[requestId]=request
    return requests_dict;


with open("log2.txt") as f:
    if f is not None:
        failed_requests_dict = generateDicts(f)
        print(failed_requests_dict)
