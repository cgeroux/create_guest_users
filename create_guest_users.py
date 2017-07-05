#!/usr/bin/env python3
from __future__ import print_function
import optparse as op
from subprocess import Popen,PIPE
import os.path
import shutil
import string
import random

def addParserOptions(parser):
  """Adds command line options
  """
  
  parser.add_option("-n",dest="numUsers",type="int"
    ,help="Specify the number of users to create/delete [default: %default]."
    ,default=1)
  parser.add_option("--with-hadoop",dest="withHadoop",action="store_true"
    ,help="If specified it will also create directories for the users in HDFS [default: %default]."
    ,default=False)
def parseOptions(actions):
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog [options] ACTION"
    ,version="%prog 1.0",description="Creates or deletes a set of guest user accounts with hdfs directories.")
  
  #add options
  addParserOptions(parser)
  
  #parse command line options
  return parser.parse_args()
def genPassword(length=8
  ,chars=string.ascii_uppercase+string.ascii_lowercase+string.digits):
  
  passwd=''
  for i in range(length):
    passwd+=random.SystemRandom().choice(chars)
  
  return passwd
def main():
  
  #these actions should match methods in the Cluster class
  actions=["create","delete"]

  #parse command line options
  (options,args)=parseOptions(actions)
  numUsers=options.numUsers
  
  #check we got the expected number of arguments
  if (len(args)!=1):
    raise Exception("Expected one argument only, the action to perform.")
  
  #check we got an action we recognize
  if args[0] not in actions:
    raise Exception(args[0]+" not in known actions "+str(actions))
  
  if(args[0]=="create"):
    
    #create the users
    for i in range(numUsers):
      
      #get encrypted password
      
      
      #create a user
      username="guest"+str(i)
      passwd=genPassword()
      cmd=["sudo","useradd",username,"-s","/bin/bash","-m"]
      #print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      #print("stdout=",stdout)
      #print("stderr=",stderr)
      
      #set password
      passwd=genPassword()
      cmd="sudo chpasswd <<EOF\n"+username+":"+passwd+"\nEOF"
      #print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE,shell=True)
      stdout,stderr=process.communicate()
      #print("stdout=",stdout)
      #print("stderr=",stderr)
      
      print(username,":",passwd)
      if options.withHadoop:
      
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
      print("removing user "+username)
      cmd=["sudo","deluser","--remove-home",username]
      #print("cmd=",cmd)
      process=Popen(cmd,stdout=PIPE,stderr=PIPE)
      stdout,stderr=process.communicate()
      #print("stdout=",stdout)
      #print("stderr=",stderr)
      
      if options.withHadoop:
        
        #remove hdfs directory
        cmd=["sudo","-i","-u","hdfs","hdfs","dfs","-rm","-r","-f","/user/"+username]
        print("cmd=",cmd)
        process=Popen(cmd,stdout=PIPE,stderr=PIPE)
        stdout,stderr=process.communicate()
        print("stdout=",stdout)
        print("stderr=",stderr)
if __name__ == "__main__":
  main()