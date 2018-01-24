#!/Users/vjain/anaconda/bin/python
import sys, hl7, json
from datetime import datetime
#import time
import re

#ifile = '/Users/vjain/Documents/Healthcare/datasets/SAMPLEHL7/messages/sample1.hl7'
#ifile = '/tmp/hl7-messages/soto_jennie_.txt'
#infile = open(ifile)

infile = sys.stdin

def to_date(dt):
    try:
        date = datetime.strptime(dt , '%Y%m%d')
        return date
    except:
        return None

def get_pname(seg):
    try:    
        name = str(seg[5])
        name = name.replace("^", " ")
    
    except:
        name = None
    
    return name

def get_paddress(seg):
    try:
        address = str(seg[11]).replace("^", ",")
        address = address.split(',')
        address = str(address[:4]).replace("[", "").replace("]", "")
            
    except:
        address  = None
    
    return address
    
    

def hl72JSON(h):
    msh_10 = h.segment('MSH')[10];
    #event = h.segment('MSH')[9];
    msgdt = str(h.segment('MSH')[7]);  
    msgdt = to_date(msgdt[:8])
    _segments = [];
    segIndex = 1;
    segDic = {};
    segARR = []
    try:
        pname = get_pname(h.segment('PID'))
    except:
        pname = None
    
    try:
        paddress = get_paddress(h.segment('PID'))
    except:
        paddress = None
    
    for seg in h:
        segName = unicode(seg[0])
        segVal = unicode(seg)
        fieldIndex = 1
        fieldCount = 1
        _fields = []
        seg.pop(0)
        if(segName == 'MSH'):
            fieldDoc = {'_id':'MSH_1','Val': seg.separator}
            _fields.append(fieldDoc)
            fieldCount += 1
            fieldIndex += 1

        for field in seg:
            fieldName = segName+'_'+unicode(fieldIndex)
            fieldVal = unicode(field)
            hasRepetitions = False;
            if fieldVal:
                fieldDoc = {'_id': fieldName,'Val': fieldVal}
                
                if ('~' in fieldVal and fieldName != 'MSH_2'):
                    hasRepetitions = True;
                    _repfields = []
                    repFields = fieldVal.split('~');
                    repIndex = 1;
                    for repField in repFields:
                        if repField:
                            repFieldVal = unicode(repField);
                            fieldName = segName+'_'+unicode(fieldIndex)
                            fieldDoc = {'_id': fieldName,'Val': repFieldVal, 'Rep': repIndex}
                            _repfields.append(fieldDoc)
                        
                            if('^' in repFieldVal):
                                repFieldComps = repFieldVal.split('^');
                                comIndex = 1;
                                for repFieldComp in repFieldComps:
                                    repFieldCompVal = unicode(repFieldComp);
                                    comName = segName+'_'+unicode(fieldIndex)+'_'+unicode(comIndex)
                                    if repFieldCompVal:
                                        fieldDoc = {'_id': comName,'Val': repFieldCompVal, 'Rep': repIndex}
                                        _repfields.append(fieldDoc)
                                    comIndex += 1
                        repIndex += 1;	
							
                    fieldDoc = {'_id': fieldName,'Val': fieldVal, 'Repetitions': _repfields}
					
                _fields.append(fieldDoc)
                fieldCount += 1
				
                if (hasRepetitions == False and len(field) > 1 and fieldName != 'MSH_2'):
                    comIndex = 1
                    for component in field:
                        comName = segName+'_'+unicode(fieldIndex)+'_'+unicode(comIndex)
                        comVal = unicode(component)
                        if comVal:
                            fieldDoc = {'_id': comName,'Val': comVal}
                            _fields.append(fieldDoc)
                        comIndex += 1
            fieldIndex += 1

        if segName in segDic:
            segDic[segName] = segDic[segName] + 1;
        else:
            segDic[segName] = 1;
		
        segDoc ={'_id': segName, 'Rep': segDic[segName], 'Seq': segIndex, 'Val': segVal, 'FC': fieldIndex-1, 'VF': fieldCount-1, 'Fields': _fields}
        _segments.append(segDoc)
        segIndex += 1
        #segARR.append(segName)
        #segJ = [json.dumps(x) for x in segARR]
        
    json_segments = [json.dumps(x) for x in _segments]
    
    
    #json_segments = cleanup_json(json_segments)
    #ts = time.time()
    hl7doc = ('{ "id": "%s", "date": "%s", "name": "%s", "address": "%s", "segments": %s }') % (msh_10, msgdt, pname, paddress, json_segments)

    hl7doc = cleanup_json(hl7doc)
    hl7js = json.dumps(hl7doc)
    
    return hl7js
 

def cleanup_json(thisjson):
    clean_json = re.sub(r':\s*{', ': {', thisjson)
    clean_json = re.sub(r'}\n,', '},', clean_json)
    clean_json = re.sub(r',(\s*)}', r'\1}', clean_json)
    clean_json = thisjson.replace("'", "\\").replace("\\","")
    return clean_json



def readHL7(infile):
    firstLine = True
    Messages = []
    strMessage = ''
    for line in infile:
        line = line.strip()
        if(len(line) > 0):
             if(firstLine == False and line.startswith('MSH|^~\&')):
                 Messages.append(strMessage)
                 strMessage = line + "\r"
             else:
                 strMessage += line + "\r"
             firstLine = False
    if(len(strMessage) >= 4):
         Messages.append(strMessage)

    for message in Messages:
        h = hl7.parse(message)
        doc = hl72JSON(h)        
        newjson = json.loads(doc)
        return newjson


if __name__ == "__main__":

    Messages = readHL7(infile);
    print Messages
    
    #message = json.loads(Messages)
    #print message
    #print message['segments'][1]['_id']

#==============================================================================
#     
#     message = json.loads(Messages)
#     for i in xrange(1, 6):    
#         print message['segments'][i]['_id']
#==============================================================================

