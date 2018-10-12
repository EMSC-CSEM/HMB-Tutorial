#!/usr/bin/env python27
"""Module for sending messages to an httpmsgbus server. """
"""
TO DO?
    recv_all() - remove timeout code? rely on requests timeouts? 
               - change retries semantics to apply to the top level loop?
"""
import requests
import time
import json
import logging
import bson
import numbers

######## Default Logger #####
logger = logging.getLogger("hmb_messaging")
logger.setLevel(logging.INFO)
if not logger.handlers:
    # create console handler and set level to warning
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s.%(process)d - %(message)s')
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
#############################


class Hmbsession(object):
    def __init__(self, url, param={}, logger=logger, retry_wait=1, use_bson=False, autocreate_queues=False, **kwargs):
        """opens a session with an hmb server at provided url.
        
       param = {
                "cid": <string>,
                "heartbeat": <int>,
                "recv_limit": <int>,
                "queue": {
                    <queue_name>: {
                        "topics": <list of string>,
                        "seq": <int>,
                        "endseq": <int>,
                        "starttime": <string>,
                        "endtime": <string>,
                        "filter": <doc>,
                        "qlen": <int>,
                        "oowait": <int>,
                        "keep": <bool>
                        },
                    ...
                    },
                }
       """
        self.url = url
        self.param = param
        self.connection_kwargs = kwargs
        self._logger = logger
        self.retry_wait = retry_wait
        self._sid = None
        self._oid = ''
        self._use_json = not use_bson #flag selecting format of messages: either json or bson
        self._autocreate_queues = autocreate_queues #This might sometimes prevent issues over missing queues
        """
        if 'queue' in self.param:
            # initially set 'keep' to false to get pending data
            for q in self.param['queue'].values():
                q['keep'] = False
        """
    
    def _open(self):
        """opens the HMB session"""
        try:
            headers = {"Content-type": "application/json" if self._use_json else "application/bson"}
            r = requests.post(self.url + '/open', 
                        data=json.dumps(self.param) if self._use_json else bson.BSON.encode(self.param),
                        headers=headers, **self.connection_kwargs)

            if r.status_code == 400:
                raise requests.exceptions.RequestException("bad request: " + r.text.strip())

            elif r.status_code == 503:
                raise requests.exceptions.RequestException("service unavailable: " + r.text.strip())

            r.raise_for_status()

            ack = r.json() if self._use_json else bson.BSON(r.content).decode()
            self._sid = ack['sid']
            self._oid = ''
            self.param['cid'] = ack['cid']
            
            qinfo = ack.get('queue',{})
            for qname,queue in qinfo.items():
                seqnext = queue['seq']
                if isinstance(seqnext,numbers.Integral) and qname in self.param['queue']:
                    if seqnext > self.param['queue'][qname].get('seq',None):
                        self.param['queue'][qname]['seq'] = seqnext #next message number
            
            #errors or missing queues
            for qname,queue in qinfo.items():
                error = queue.get('error',None)
                if error:
                    self._logger.warning("hmb server gives an error for queue '%s': %s",qname,error)
                    
                    #if queue not found then create queue?
                    if error == u'queue not found' and self._autocreate_queues:
                        msg = {'type':'TOUCH','queue':qname}
                        self.send({'0':msg} if self._use_json else msg)
                        self.param['queue'][qname]['seq'] = 1
                        self._logger.warning("created (hmb) queue '%s' by sending test message",qname)
                    
            
            self._logger.info("hmbsession opened, sid=%s, cid=%s",ack['sid'], ack['cid'])
            self._logger.info("hmbsession parameters are: %r",self.param)

        except requests.exceptions.RequestException as e:
            errmsg = str(e)
            if "service unavailable" in errmsg: 
                errmsg = "service unavailable: The server is down due to maintenance downtime or capacity problems"
            self._logger.warning("hmb error: %s",errmsg)
            self._logger.warning("connection to hmb message bus failed")
            
    def info(self):
        """gets info from the hmb server on defined queues, topics and available
        data."""
        try:
            return self._info_request('info')
        except requests.exceptions.RequestException as e:
            self._logger.error("error getting info: %s",str(e))
            return None

    def features(self):
        """gets functions and capabilities supported by the server and optionally
        the name and version of the server software."""
        try:
            return self._info_request('features')
        except requests.exceptions.RequestException as e:
            self._logger.error("error getting features: %s",str(e))
            return None
    
    def status(self):
        """gets status of connected clients (sessions)."""
        try:
            return self._info_request('status')
        except requests.exceptions.RequestException as e:
            self._logger.error("error getting status: %s",str(e))
            return None
    
    def _info_request(self,cmd):
        """gets info, functions and capabilities supported by the server."""
        r = requests.get(self.url + '/'+cmd, **self.connection_kwargs)

        if r.status_code == 400:
            raise requests.exceptions.RequestException("bad request: " + r.text.strip())

        elif r.status_code == 503:
            raise requests.exceptions.RequestException("service unavailable: " + r.text.strip())

        r.raise_for_status()     
        
        return r.json()
                        
    def set_format(self,use_bson):
        """defines whether connection object should use bson or json 
        note that this may force the connection to the server to be reestablished.
        """
        if not isinstance(use_bson,bool): 
            raise TypeError("use_bson must be a boolean value")
        if self._use_json != use_bson:
            #no change to format so do nothing
            pass
        else:
            self._use_json = not use_bson
            self._sid = None #mark session as closed
    
    def send_msg(self,mtype,queue,data,topic=None,retries=1,**kwargs):
        """send single message to HMB session.
            mtype - message type (string)
            queue - destination queue of the message
            data - json compatible payload
            topic - optional tag for the message
            retries - number of times to retry sending message
            kwargs - any extra keyvalues to put in the message ie. seq, starttime, endtime
        """
        msg = {"type":mtype,
               "queue":queue,
               "data":data}
        if topic: msg["topic"] = topic
        for k,v in kwargs.items(): msg["k"] = v
        
        if self._use_json: msg = {0:msg} #json messages always require multi-message format
        else: pass #bson messages use a different type of concatenation
        
        self.send(msg,retries)        
    
    def send(self,msg,retries=1):
        """send message to HMB session. Handles disconnections and retries
        sending the message. The message should have the correct hmb format.
        """                    
        for i in range(retries+1):
            try:
                if not self._sid:
                    self._open()
                self._send(msg)
            except Exception as e:
                self._sid = None #mark session as closed
                if i == retries:
                    self._logger.error("hmb error: %s",str(e))
                    self._logger.error("problem hmb msg: %s", msg) #the first time it probably isn't the message's fault
                elif i == 0:
                    self._logger.info("hmb error: %s",str(e))
                    self._logger.info("closing connection to hmb message bus lost, retrying")
                else:
                    self._logger.warning("hmb error: %s",str(e))
                    self._logger.warning("problem hmb msg: %s", msg) #the first time it probably isn't the message's fault
                    self._logger.warning("closing connection to hmb message bus lost, retrying in %d seconds", self.retry_wait)
                    time.sleep(self.retry_wait) #don't wait on the first retry
                    
            else: #if no exceptions -
                break
        
    def _send(self,msg):
        """actually sends message to HMB session"""
        r = requests.post(self.url + '/send/' + self._sid,
                    headers={"Content-type": "application/json" if self._use_json else "application/bson"},
                    data=json.dumps(msg,allow_nan=False) if self._use_json else bson.BSON.encode(msg),
                    **self.connection_kwargs)

        if r.status_code == 400:
            raise requests.exceptions.RequestException("bad request: " + r.text.strip())

        elif r.status_code == 503:
            raise requests.exceptions.RequestException("service unavailable: " + r.text.strip())

        r.raise_for_status()        
    
    def recv_all(self,retries=1,timeout=None):
        """receives all messages from an HMB query. This should not be 
        used for realtime operation."""
        starttime = time.time()
        starttime -= 0.2 #correction factor so that timeout works as expected.
        messages = [] 
        while True:
            subset = self.recv(retries=retries)
            if not subset: #no messages received
                pass #(continue)
            elif subset[-1]['type'] != 'EOF':
                messages += subset
            else:
                messages += subset[:-1]
                break
            
            if timeout and time.time() > starttime + timeout:
                self._sid = None
                break
        return messages
        
    def recv(self,retries=1):
        """receives messages from HMB session. Request is blocking until the 
        next heartbeat message if "keep=True" is specified in the connection
        parameters for any of the queues.. HEARTBEAT messages are 
        elimated but EOF messages are kept so that we know when the end of 
        the stream is reached."""
        messages = []
        
        for i in range(retries+1):
            try:
                if not self._sid:
                    self._open()
                messages = self._recv()
            except Exception as e:
                self._sid = None #mark session as closed
                if i == retries:
                    self._logger.error("hmb error: %s",str(e))
                elif i == 0:
                    self._logger.info("hmb error: %s",str(e))
                    self._logger.info("connection to hmb message bus lost, retrying")
                else:
                    self._logger.warning("hmb error: %s",str(e))
                    self._logger.warning("connection to hmb message bus lost, retrying in %d seconds", self.retry_wait)
                    time.sleep(self.retry_wait) #don't wait on the first retry
                
            else: #if no exceptions.
                break
        #else:
        #    raise
                
        #eliminate HEARTBEAT messages
        messages = [m for m in messages if m['type'] not in ('HEARTBEAT',)]
        return messages
                
    def _recv(self):
        """actually receive messages from HMB. Request is blocking if "keep=True"
        is specified in the connection parameters for any of the queues."""
        r = requests.get(self.url + '/recv/' + self._sid + self._oid, **self.connection_kwargs)
        
        if r.status_code == 400:
            raise requests.exceptions.RequestException("bad request: " + r.text.strip())

        elif r.status_code == 503:
            raise requests.exceptions.RequestException("service unavailable: " + r.text.strip())

        r.raise_for_status()
        
        if self._use_json:
            msgdict = r.json() # can be multiple messages
            messages = [msgdict[str(i)] for i in range(len(msgdict))] #convert to list
        else: #bson
            messages = bson.decode_all(r.content)
            
        #print r.content
        #print 
        #print msgdict
        #print
        
        ###
        seqnum = None
        for obj in messages:
            #extracts sequence number from messages to ensure future continuity of messages received.
            if 'seq' in obj and 'queue' in obj:
                seqnum = obj['seq']
                if isinstance(seqnum,numbers.Integral):
                    if seqnum >= self.param['queue'][obj['queue']]['seq']:
                        self.param['queue'][obj['queue']]['seq'] = seqnum + 1 #next message number
                    self._oid = '/%s/%d' % (obj['queue'], seqnum)
        
        #closing session if EOF message is last message received
        if obj['type'] == 'EOF': #will always be the last message?
            self._sid = None #close current session when we reach latest message.
        
        #changing queues to realtime mode
        #is this useful?
        """ 
        try:
            if obj['type'] == 'EOF':
                self._sid = None #close current session when we reach latest message.

                for q in self.param['queue'].values():
                    q['keep'] = True #change session connection parameters to keep session alive 
        except KeyError:
            pass
        """
        ###
        
        return messages



#############################

def example0():
    """Very basic example of receiving HMB messages"""
    # Choose here the bus to subsribe:
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule0' # CLOSEST
    hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule1' # FASTER
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule2' # STRONGEST
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule3' # HIGHEST
    
    param = {
            'queue': {
                'SYSTEM_ALERT': { #choose queue to listen on
                        'seq': -5,  #choose message to start at.
                        'keep': True  #make request blocking
                        }
                }
            }
    #each message in the queue has a sequence number. Setting:
    #seq=-1 => start with next new message
    #seq=-2 => start with most recent message
    #seq=-3 => start with message before the last message.
    #...
    #seq=10 => start with the 10th message in the queue
    
    hmbconn = Hmbsession(url=hmbbus, param=param, use_bson=False)
    
    msgs = hmbconn.recv()
    for msg in msgs:
        logger.info('hmb msg: %r',msg)
    

def example1():
    """Basic example of receiving HMB messages with some robustness"""
    # Choose here the bus to subsribe:
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule0' # CLOSEST
    hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule1' # FASTER
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule2' # STRONGEST
    #hmbbus='http://cerf.emsc-csem.org:80/MTtest_rule3' # HIGHEST
    
    param = {
            'heartbeat': 10, #sec
            'queue': {
                'SYSTEM_ALERT': { #choose queue to listen on
                        'seq': -1,  #choose message to start at.
                        'keep': True,  #make request blocking
                        #'topics': [],
                        }
                }
            }
    
    hmbconn = Hmbsession(url=hmbbus, param=param, use_bson=True, retry_wait=2, timeout=(6.05,11), autocreate_queues=True)
    #setup timeouts just longer than the heartbeat rate
    #set a delay between retries
        
    while True:
        msgs = hmbconn.recv(retries=10)
        #the method will retry in the event of an error or timeout by reopening a
        #new connection to the server and trying again.
        
        #nb. msgs might be empty due to eliminated heartbeat messages
        for msg in msgs:
            logger.info('hmb msg: %r',msg)
    

if __name__ == "__main__":
    #example0()
    
    example1()
