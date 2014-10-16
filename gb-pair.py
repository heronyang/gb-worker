#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
from settings import Database

class GB_STATUS():
    ENABLED     = 0
    DISABLED    = 1
    SUCCEED     = 2

DEFAULT_GBS_SUCCESS_STATUS = 0

gbs = []
gbs_success = []

db = MySQLdb.connect(host=Database.HOST, user=Database.USER, passwd=Database.PASSWD, db=Database.DB, charset="utf8", use_unicode=False)
cursor = db.cursor()

def db_fetch():
    try:
        cursor.execute("SELECT `gid`, `user1`, `user2`, `content` FROM `gb` WHERE `status`=" + str(GB_STATUS.ENABLED) + " ORDER BY `ctime` ASC")
        db.commit()
    except:
        print "Error: unable to fecth data"
        return

    numrows = int(cursor.rowcount)
    for i in range(numrows):
        row = cursor.fetchone()
        gbs.append({'gid':      row[0],
                    'user1':    row[1],
                    'user2':    row[2],
                    'content':  row[3]})

def gb_pair():
    n = len(gbs)
    for i1 in range(n):
        for i2 in range(i1+1, n):
            if gbs[i1]['user2'] == gbs[i2]['user1'] and gbs[i1]['user1'] == gbs[i2]['user2']:
                gbs_success.append( (i1, i2) )

def db_write():
    n = len(gbs_success)
    for (i1, i2) in gbs_success:
        gb1 = gbs[i1]
        gb2 = gbs[i2]

        user1 = str(gb1['user1'])
        user2 = str(gb2['user1'])
        gid1 =  str(gb1['gid'])
        gid2 =  str(gb2['gid'])
        status = str(DEFAULT_GBS_SUCCESS_STATUS)

        # INSERT to gb_success
        sql = 'INSERT INTO `gb_success` (`user1`, `user2`, `gid1`, `gid2`, `status`, `ctime`) VALUES ('+user1+', '+user2+', '+gid1+', '+gid2+', '+status+', NOW())'
        try:
            cursor.execute(sql)
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()

        # UPDATE gb status
        sql = 'UPDATE `gb` SET `status`='+str(GB_STATUS.SUCCEED)+' WHERE gid='+gid1
        try:
            cursor.execute(sql)
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()

        sql = 'UPDATE `gb` SET `status`='+str(GB_STATUS.SUCCEED)+' WHERE gid='+gid2
        try:
            cursor.execute(sql)
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()

def disconnect():
    db.close()

def print_debug():
    import pprint
    print '**gbs: '
    for gb in gbs:
        print '\tgid: ' + str(gb['gid'])
        print '\tuser1: ' + str(gb['user1']) + '\tuser2: ' + str(gb['user2'])
        print '\tcontent: ' + gb['content']
        print '-'
    print '**gbs_success: '
    pprint.pprint(gbs_success)
    print '---'


# main
def main(argv):
    print 'TIME: '+datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    db_fetch()
    gb_pair()
    db_write()
    disconnect()

    print_debug()

if __name__ == "__main__":
    main(sys.argv)
