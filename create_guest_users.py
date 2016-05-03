#!/usr/bin/env python
from __future__ import print_function
import optparse as op
from subprocess import Popen,PIPE
import os.path
import shutil

numUsers=3
keyFileName="./guest-key"

def addParserOptions(parser):
  """Adds command line options
  """
  
  pass
def parseOptions(actions):
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog [options] ACTION"
    ,version="%prog 1.0",description="Creates or deletes a set of guest user accounts with hdfs directories.")
  
  #add options
  addParserOptions(parser)
  
  #parse command line options
  return parser.parse_args()
def main():
  
  #these actions should match methods in the Cluster class
  actions=["create","delete"]

  #parse command line options
  (options,args)=parseOptions(actions)
  
  #check we got the expected number of arguments
  if (len(args)!=1):
    raise Exception("Expected one argument only, the action to perform.")
  
  #check we got an action we recognize
  if args[0] not in actions:
    raise Exception(args[0]+" not in known actions "+str(actions))
  
  if(args[0]=="create"):
    
    #make a keypair
    #ssh-keygen -q -t dsa -C ${name} -f $keyname -N '' 2>/dev/null
    if os.path.isfile(keyFileName):
      os.remove(keyFileName)
    if os.path.isfile(keyFileName+".pub"):
      os.remove(keyFileName+".pub")
    cmd=["ssh-keygen","-q","-t","rsa","-C","guest","-f",keyFileName,"-N",'']
    print("cmd=",cmd)
    process=Popen(cmd,stdout=PIPE,stderr=PIPE)
    stdout,stderr=process.communicate()
    print("stdout=",stdout)
    print("stderr=",stderr)
    
    #create the users
    for i in range(numUsers):
      
      #create a user
      username="guest"+str(i)
      cmd=["sudo","useradd",username,"-s","/bin/bash","-m"]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #ensure there is a .ssh directory
      cmd=["sudo","-H","-u",username,"mkdir","-p","/home/"+username+"/.ssh"]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #set correct permissions
      cmd=["sudo","-H","-u",username,"chmod","0700","/home/"+username+"/.ssh"]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #copy over the public key
      cmd=["sudo","-H","-u",username,"cp",keyFileName+".pub","/home/"+username+"/.ssh/authorized_keys"]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #set correct permissions
      cmd=["sudo","-H","-u",username,"chmod","0600","/home/"+username+"/.ssh/authorized_keys"]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #create folders in hdfs for the users
      cmd=["sudo","-i","-u","hdfs","hdfs","dfs","-mkdir","-p","/user/"+username]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #set ownership
      cmd=["sudo","-i","-u","hdfs","hdfs","dfs","-chown",username+":"+username,"/user/"+username]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #set mode
      cmd=["sudo","-i","-u","hdfs","hdfs","dfs","-chmod","0755","/user/"+username]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
  elif(args[0]=="delete"):
    
    for i in range(numUsers):
      
      #create a user
      username="guest"+str(i)
    
      #delete user
      cmd=["sudo","deluser","--remove-home",username]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
      
      #remove hdfs directory
      cmd=["sudo","-i","-u","hdfs","hdfs","dfs","-rm","-r","-f","/user/"+username]
      print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      print("stdout=",stdout)
      print("stderr=",stderr)
    
    #remove key
    if os.path.isfile(keyFileName):
      os.remove(keyFileName)
    if os.path.isfile(keyFileName+".pub"):
      os.remove(keyFileName+".pub")
if __name__ == "__main__":
  main()