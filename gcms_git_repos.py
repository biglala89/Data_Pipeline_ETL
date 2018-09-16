# -*- coding: utf-8 -*-
"""
This program is to clone and pull git repositories into /data/gcms_repos

@author: Xianqiao Li
"""

import os, subprocess, time
from time import strftime, localtime

os.chdir('/data/gcms_repos')

pwd = os.getcwd()

drt = os.listdir(pwd)

with open('/data/gcms_conf/git_repo_list.conf', 'r') as f:
    repo_list = f.readlines()

with open('/data/gcms_conf/git_credentials.conf', 'r') as c:
    credentials = c.readlines()
    username = credentials[0].split(':')[1].strip()
    password = credentials[1].split(':')[1].strip()

t0 = time.time()

print "The time is: {}".format(strftime("%a, %d %b %Y %H:%M:%S", localtime()))
    
for r in repo_list:
    r = str(r.strip())
    
    if r in drt:
        print 'Repository: {} is already there, performing GIT PULL...'.format(r)
        pwd1 = os.path.join(pwd, r).replace('\\', '/')
        try:
            p = subprocess.Popen(['git', 'pull', '--progress'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=pwd1)
            for line in p.stdout:
                print line
        except Exception as e:
            print e
            continue
    else:
        service_user = '{}'.format(username)
        repo = '{}'.format(r)
        aws = 'https://{}:{}@stash.aws.dnb.com/scm/edtrl/{}.git'.format(service_user, password, repo)
        try:
            p = subprocess.Popen(['git', 'clone', '--progress', aws], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=pwd)
            for line in p.stdout:
                print line
        except Exception as e:
            print e
            continue
            
    t1 = time.time()
print int(t1-t0), 'seconds'