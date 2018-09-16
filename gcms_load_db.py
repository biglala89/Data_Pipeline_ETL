# -*- coding: utf-8 -*-
"""
This program is created to load the Flare files to gcms_u_co_text table

Created on Tue Feb 14 13:24:20 2017

@author: Xianqiao Li & GiteG

Last modified on Fri Apr 19 14:58:03 2017
"""

import re, os, time, subprocess
import cx_Oracle
import datetime
from time import localtime, strftime
from collections import OrderedDict

os.environ["NLS_LANG"] = ".AL32UTF8"

project_path = '/data/gcms_repos'
load_db_path = '/data/gcms_logs/gcms_load_db.log'
database_connection_path = '/data/gcms_conf/database_connect.conf'
error_log_path = '/data/gcms_logs/gcms_load_error.log'
commit_hash_path = '/data/gcms_commit_hash'
archive_path = '/data/gcms_logs/archive'

os.chdir(project_path)
    
all_repos=[]
for d in os.listdir(project_path):
    if os.path.isdir(os.path.join(project_path, d)):
        all_repos.append(os.path.join(project_path, d))

start_time = strftime("%a, %d %b %Y %H:%M:%S", localtime())


#   returns gcms_text_type/segment number from the snippet
def xtag(path):
    with open(path) as s:
        return re.findall('<text_type>(\S*?)</text_type>', s.read())[0]

#   connect to oracle db
def openconn(user, pwd, dsn1, dsn2):
    try:    
        db = cx_Oracle.connect(user, pwd, dsn1)
    except Exception as e:
        print e
        try:    
            db = cx_Oracle.connect(user, pwd, dsn2)
        except Exception as e:
            print e
    return db

#   check previous error log
def check_len():
    global p_len
    global p_lst
    if os.path.isfile(error_log_path):
        with open(error_log_path, 'r') as p:
            prev = p.read()
        p_lst = prev.split('\n\n')
        del p_lst[-1]
        p_len = len(p_lst)
    else:
        p_lst = []
        p_len = len(p_lst)
    return p_len, p_lst

#   remove error log duplicates
def err_dup_remove(p_len, p_lst):
    if os.path.isfile(error_log_path):
        with open(error_log_path, 'r') as err:
            x = err.read()
        c_lst = x.split('\n\n')
        del c_lst[-1]
        if len(c_lst) > p_len:
            addition = [c for c in c_lst if c not in p_lst]
            addition = list(OrderedDict.fromkeys(addition))
            with open(error_log_path, 'w') as dm:
                for i in p_lst:
                    dm.write(i + '\n\n')
                dm.write('---Start time: %s---\n\n' % start_time)
                for l in addition:
                    dm.write('{}\n\n'.format(l))
                dm.write('-----END-----\n\n')
        else:
            pass
    else:
        pass

#   Manage archive files    
def manage_archive():
    try:
        os.chdir(archive_path)
        archive = os.getcwd()
        logs = os.listdir(archive)    
        year_checker = strftime("%Y", localtime())
    
        print '\nManaging logs...'
        for log in logs:
            if not re.search(year_checker, log):
                try:
                    os.remove(os.path.join(archive, log))
                    print '%s deleted' % log
                except Exception as e:
                    print e
                    pass
    except Exception as e:
        print 'Directory does not exist, pass'
        pass

#   load records to GCMS    
def load(db_e, cursor, mod_files, listing, log, repo):
    t = time.time()
    
    load_counter=0
    with open(load_db_path, 'a') as q:
        et0 = strftime("%a, %d %b %Y %H:%M:%S", localtime())
        q.write('---Start time: %s---\n\n' % et0)

    n=0 # counter for total number of inserts
    filelist = mod_files
    k=0
    print 'Committing to GCMS db...'
    for infile in filelist:
        try:
            print infile 
            with open(infile) as f:
                text = re.sub("'", "''", f.read()) # double escape single quotes for insert sql statements
            duns = re.findall(r'</h1>.*?(\d*)\s*</td>.*?(\d*)\s*</td>.*?(\d*)\s*</td>', text, flags=re.I | re.S)[0][0]    
            hoovers_id = re.findall(r'</h1>.*?(\d*)\s*</td>.*?(\d*)\s*</td>', text, flags=re.I | re.S)[0][1]
            tier = re.findall(r'</h1>.*?(\d*)\s*</td>.*?(\d*)\s*</td>.*?(\d*)\s*</td>', text, flags=re.I | re.S)[0][2]
            snames = re.findall(r'<MadCap:snippetBlock src="../Resources/Snippets/Master/Editorial/(H2.*?flsnp)', text, flags=re.I)
            family = infile.split('/')[-4]
            if len(snames) != len(set(snames)):
                with open(error_log_path, 'a') as q:
                    load_file = infile.split('/')[-1]
                    tt = strftime("%a, %d %b %Y %H:%M:%S", localtime())            
                    q.write(load_file + '\n' + family + '\n' + duns + '\n' + tt + '\n' + 'Duplicate segments found' + '\n\n')
                continue
            k+=1
            print "This is the %sth file of %s files" % (k, len(filelist))
    
            for row in log:
                if infile in row:
                    author = row.split('\n')[0].split('|^|')[1]
                    mod_date = row.split('\n')[0].split('|^|')[2]
                    comment = row.split('\n')[0].split('|^|')[3]
                    break
                else:
                    continue
    
            comment = re.sub("'", "''", comment)
            infile = re.sub("'", "''", infile)
            i=0 #   counter for different text segments in a particular file
            for sname in snames: # is there a master path for all snippets?
                spath = os.path.join(repo,'Content','Resources','Snippets','Master','Editorial', sname)
                snp = re.findall(r'<madcap.*?%s.*?/>' % sname, text, flags=re.I)
                if (sname!='H2 Synopsis.flsnp' and sname!='H2 History.flsnp'):
                    text = re.sub(snp[0], snp[0]+'<p><strong>'+sname[3:-6]+'</strong></p>', text, flags=re.I)
                
                start_madcap = re.findall(r'<MadCap.*?H2.*?\.flsnp".*?/>', text, flags=re.I)
                end_madcap = re.findall(r'<MadCap.*?XML text end\.flsnp".*?/>', text, flags=re.I)
                if (len(start_madcap) == len(end_madcap) and len(start_madcap) != 0):
                    segment_text = re.findall(r'<MadCap.*?H2.*?\.flsnp".*?/>(.*?)<MadCap.*?XML text end\.flsnp".*?/>', text, flags=re.S | re.I)
                    if re.search(r'.*?delete\s*!+.*?(CDATA)?.*?', segment_text[i], flags=re.I | re.S) or \
                        re.search(r'Master/DnB-Project\.Status-Descoped', start_madcap[i], flags=re.I) or \
                        re.search(r'Master/DnB-Project\.Status-Draft', start_madcap[i], flags=re.I):
                        segment_text[i] = ''
                elif (len(start_madcap) == len(end_madcap) == 0):
                    f = open(error_log_path, 'a')
                    fname = infile.split('/')[-1]
                    et = strftime("%a, %d %b %Y %H:%M:%S", localtime())
                    f.write(fname + '\n' + family + '\n' + duns + '\n' + et + '\n' + 'Bad segment tags' + '\n\n')
                    f.close()
                    continue
                else:
                    print 'Unequal MadCap tags, start_madcap = %d, end_madcap = %d' % (len(start_madcap), len(end_madcap))
                    f = open(error_log_path, 'a')
                    fname = infile.split('/')[-1]
                    et = strftime("%a, %d %b %Y %H:%M:%S", localtime())
                    f.write(fname + '\n' + family + '\n' + duns + '\n' + et + '\n' + 'Unequal MadCap tags' + '\n\n')
                    f.close()
                    continue
            
                isql = """
                DECLARE
                    long_text CLOB;
                BEGIN
                    long_text := '%s';
                    
                    insert into globalcms.gcms_u_co_text (gcms_duns, gcms_company_id, gcms_file_name, mod_by, mod_comment, mod_date, gcms_company_tier, gcms_text_type_id, gcms_text_load_date, gcms_text_data) 
                    values (%s, %s, '%s', '%s', '%s', to_date('%s', 'dy mon dd hh24:mi:ss yyyy'), %s, %s, sysdate, long_text);  
                    
                END;""" %(segment_text[i], duns, hoovers_id, infile.encode('utf-8'), author.encode('utf-8'), comment.encode('utf-8'), mod_date.encode('utf-8'), tier, xtag(spath))
    
                try:
                    cursor.execute(isql)
                    n+=1
                except Exception as e:
                    print e, ' - Performing a Rollback!!'
                    db_e.rollback()
                i+=1
            
    
            try:
                db_e.commit() # Commiting only once for migration, move inside 'for infile:' loop to commit after every file
                with open(load_db_path, 'a') as q:
                    load_file = infile.split('/')[-1]
                    if os.path.isfile(error_log_path):
                        with open(error_log_path, 'r') as e:
                            err = e.read()
                            if re.search(load_file, err.split('-----END-----')[-1], flags=re.I | re.S):
                                pass
                            else:
                                et1 = strftime("%a, %d %b %Y %H:%M:%S", localtime())            
                                q.write(load_file + '\n' + family + '\n' + duns + '\n' + et1 + '\n\n')
                                load_counter+=1
                    else:
                        et2 = strftime("%a, %d %b %Y %H:%M:%S", localtime())            
                        q.write(load_file + '\n' + family + '\n' + duns + '\n' + et2 + '\n\n')
                        load_counter+=1
            except Exception as e:
                print e, ' -- Rolling back!'
                db_e.rollback()
                with open(error_log_path, 'a') as q:
                    load_file = infile.split('/')[-1]
                    tt = strftime("%a, %d %b %Y %H:%M:%S", localtime())            
                    q.write(load_file + '\n' + family + '\n' + duns + '\n' + tt + '\n' + 'DB rollback' + '\n\n')
                    
        except Exception as e:
            with open(error_log_path, 'a') as el:
                tt = strftime("%a, %d %b %Y %H:%M:%S", localtime())
                el.write(infile.split('/')[-1] + '\n' + family + '\n' + duns + '\n' + tt + '\n' + str(e) + '\n\n')
            continue
    
    print "Uploaded time: ", datetime.datetime.now()
    cursor.close()
    db_e.close()
    
    with open(load_db_path, 'a') as g:
        g.write('Total number of files loaded: %d\n-----END-----\n\n' % load_counter)
    
    if n==0:
        print 'Connection closed. No updates detected'
    else:
        print 'Connection closed. %s rows loaded in'%n, int(time.time()-t), 'secs'
   
#   main function
def run(project):
    print "Start time: ", datetime.datetime.now()
    
    check_len()
    
    with open(database_connection_path, 'r') as q:
        logins = q.readlines()
    user, pwd, dsn1, dsn2 = [l.strip() for l in logins]
    
    for i in range(5):
        try:
            db_e = openconn(user, pwd, dsn1, dsn2)
            print 'Connected to', db_e.version, db_e.dsn
            break
        except Exception as e:
            print e, 'Failed attempt', db_e.dsn, i+1
            time.sleep(2)
    
    try:
        cursor=db_e.cursor()
        print 'Cursor established'
    except Exception as e:
        print e
            
    log=''
    allcommits=set()
    listing=set()
    
    global all_prev_hashes
    all_prev_hashes = []
    
    for repo in all_repos:
        commit_file = '{}'.format(repo.split('/')[-1])
        file_path = os.path.join(commit_hash_path, commit_file).replace('\\', '/')
        
        if os.path.isfile(file_path):
            with open(file_path, 'r') as a:
                previous_hash = a.read()
                all_prev_hashes.append(previous_hash)
                
            p = subprocess.Popen('git log {}..HEAD --date=local --pretty=format:"~*~%h|^|%cn, %ce|^|%cd|^|%s" --name-only'.format(str(previous_hash)), stdout=subprocess.PIPE, cwd=repo, shell=True)
            stdout = p.communicate()[0]
            out = re.sub(r'(Content/en-US/.*?htm)', '%s/\\1'%repo.replace('\\', '/'), stdout)
            log += out.decode(encoding='utf-8')            
            last_commit = out.split('|^|')[0][3:]
            allcommits |= set(re.findall(r'~\*~(\w*)', out))

            if len(stdout) == 0:
                with open(file_path, 'w') as u:
                    u.write(str(previous_hash))
            else:
                with open(file_path, 'w') as s:
                    s.close()
    
                with open(file_path, 'w') as d:
                    d.write(str(last_commit))
    
        else:
            with open(file_path, 'w') as h:
                h.close()

            p = subprocess.Popen('git log --date=local --pretty=format:"~*~%h|^|%cn, %ce|^|%cd|^|%s" --name-only', stdout=subprocess.PIPE, cwd=repo, shell=True)
            stdout = p.communicate()[0]
            out = re.sub(r'(Content/en-US/.*?htm)', '%s/\\1'%repo.replace('\\', '/'), stdout)
            log += out.decode(encoding='utf-8')
            last_commit = out.split('|^|')[0][3:]
            allcommits |= set(re.findall(r'~\*~(\w*)', out))

            with open(file_path, 'w') as g:
                g.write(str(last_commit))

        filepath = os.path.join(repo,'Content','en-US')
        listing |= set([os.path.join(filepath, f).replace('\\', '/') for f in os.listdir(unicode(filepath, 'utf-8'))]) # read all filenames in folder 'filepath' in a list

    listing = [l for l in listing if re.search(r'htm$', l)] # keep, only the files ending with htm
    log = log.split('~*~')  
    del log[0]
    mod_files = set(re.findall(r'.*?/Content/en-US/.*htm', ''.join(log), flags=re.I))&set(listing)
    
    print "Performing loading..."
    load(db_e, cursor, mod_files, listing, log, repo)
 
    err_dup_remove(p_len, p_lst)
    
    manage_archive()
    

# start the project
if __name__ == '__main__':
    try:
        run(project_path)
        close_time = strftime("%d %b %Y %H:%M:%S", localtime())
        print "\nProcess completed at %s" % close_time
    except Exception as e:
        print 'Error message:', e
        print 'Restoring commit hash...'
        cc = 0
        for repo in all_repos:
            commit_file = '{}'.format(repo.split('/')[-1]) #use this line on server
            file_path = os.path.join(commit_hash_path, commit_file).replace('\\', '/')
            if os.path.isfile(file_path):
                with open(file_path, 'w') as a:
                    a.write(str(all_prev_hashes[cc]))
                    cc+=1
                    print "%s's previous commit hash has been restored" % commit_file
            else:
                continue