#!/usr/bin/env python
# lag.py [cuttoffDate] [startDate]

import os
import sys
import datetime
import pymssql
from dotenv import load_dotenv
from envelopes import Envelope
from smtplib import SMTPException # allow for silent fail in try exceptio


def resource_path(relative_path: str) -> str:
    """Get absolute path to resource, works for dev and for PyInstaller

    Arguments:
        relative_path: str -- releative path from current directory + file name

    Returns:
        str -- absoulte path to file
    """
    try:
        # pylint: disable=protected-access
        base_path: str = sys._MEIPASS  # type: ignore
    except AttributeError:
        base_path: str = os.path.abspath(".")  #type: ignore

    return os.path.join(base_path, relative_path)


def split_address(email_address):
    """Return a tuple of (address, name), name may be an empty string
       Can convert the following forms
         exaple@example.com
         <example@exmaple.con>
         Example <example@example.com>
         Example<example@example.com>
    """
    address = email_address.split('<')
    if len(address) == 1:
        return (address[0], '')
    if address[0]:
        return (address[1][:-1], address[0].strip())
    return (address[1][:-1], '')

def mail_results(subject, body, attachment=''):
    """ Send emial with html formatted body and parameters from env"""
    envelope = Envelope(
        from_addr=split_address(os.environ.get('MAIL_FROM')),
        subject=subject,
        html_body=body
    )

    # add standard recepients
    tos = os.environ.get('MAIL_TO','').split(',')
    if tos[0]:
        for to in tos:
            envelope.add_to_addr(to)

    # add carbon coppies
    ccs = os.environ.get('MAIL_CC','').split(',')
    if ccs[0]:
        for cc in ccs:
            envelope.add_cc_addr(cc)

    # add blind carbon copies recepients
    bccs = os.environ.get('MAIL_BCC','').split(',')
    if bccs[0]:
        for bcc in bccs:
            envelope.add_bcc_addr(bcc)

    if attachment:
        envelope.add_attachment(attachment)

    # send the envelope using an ad-hoc connection...
    try:
        _ = envelope.send(
            os.environ.get('MAIL_SERVER'),
            port=os.environ.get('MAIL_PORT'),
            login=os.environ.get('MAIL_LOGIN'),
            password=os.environ.get('MAIL_PASSWORD'),
            tls=True
        )
    except SMTPException:
        print("SMTP EMail error")

def send_email(output):
    """xxxx"""
    htmlText = """<p>Here is the Lag Time Report for %s.</p><br /><br/><pre>"""%(datetime.date.today())
    htmlText += outupt  + "</pre>"

    plainText = """Here is the Lag Time Report for %s."""%(datetime.date.today())
    plainText += "\n\n" + outupt

    mail_results(
    'Labor By Boat for %s'%(datetime.date.today()),
    htmlText,
    text=plainText)



def get_boats(cursor):
    """Get the list of jobs 

    Aarguments:
        cursor: -- sql cursor

    Retunrs:
        list of job ids, boat names

    """
    SQL = """
    SELECT  DISTINCT(tp.job_id), job.jobname
      FROM  timeWorkingPunch tp
 LEFT JOIN  job on tp.job_id = job.job_id
     WHERE  tp.inpunch_dt > DATEADD(year,-1,GETDATE())
       AND  tp.active_yn = 1
       AND  tp.job_id > 7000
    """
    cursor.execute(SQL)
    return cursor.fetchall()

def get_boat_hours(cursor, job):
    """ get hours for boat"""
    SQL = """
        SELECT  substring(departmentname, 1,3 ) as departmentname, 
               SUM(CASE 
                 WHEN DATEPART(MINUTE, workingpunch_ts) = 45 THEN DATEPART(HOUR, workingpunch_ts) + .7
                 WHEN DATEPART(MINUTE, workingpunch_ts) = 30 THEN DATEPART(HOUR, workingpunch_ts) + .5
                 WHEN DATEPART(MINUTE, workingpunch_ts) = 15 THEN DATEPART(HOUR, workingpunch_ts) + .25
                 WHEN DATEPART(MINUTE, workingpunch_ts) =  0 THEN DATEPART(HOUR, workingpunch_ts)
               END) as WorkTime
         FROM  timeWorkingPunch tp
    LEFT JOIN  job on tp.job_id = job.job_id
    LEFT JOIN  task on tp.task_id = task.task_id
         JOIN  empMain em ON tp.employee_id = em.employee_id
         JOIN  tblDepartment dp ON tp.department_id  = dp.department_id
        WHERE  tp.job_id = %s
     GROUP BY  substring(departmentname, 1,3 )
    """
    cursor.execute(SQL, job)
    return dict(cursor.fetchall())

def get_latest_punch(cursor, job):
    SQL = """
         SELECT  max(workingpunch_ts)
           FROM  timeWorkingPunch tp
          WHERE  tp.job_id = %s
            AND  tp.active_yn = 1
            AND  tp.department_id = 221
    """
    cursor.execute(SQL, job)
    return cursor.fetchone()[0]

def valid_jobs(cursor, all_jobs):
    """ 
    latest = None
             No outfitting hours, if fab hours then ok
    latest > cutoff
             currently if fab
    """
    cutoff = datetime.datetime.today() - datetime.timedelta(days=15)
    jobs = []
    for job in all_jobs:
        latest = get_latest_punch(cursor, job[0])
        if not (latest is None or latest > cutoff):
            continue
        result = get_boat_hours(cursor, job[0])
        jobs.append([job[1], result])
    return sorted(jobs)


def nice_number(hours, dept):
    if hours.get(dept, None) is None:
        return "       "
    return f"{hours.get(dept):7.2f}"

def main():
    load_dotenv(dotenv_path=resource_path(".env"))
    with pymssql.connect(
            os.getenv('DB_HOST'),
            os.getenv('DB_USER'),
            os.getenv('DB_PASSWORD'),
            os.getenv('DB_DATABASE'),
            tds_version=r'7.0') as conn:
        with conn.cursor() as cursor:
            jobs = sorted(get_boats(cursor))
            results = valid_jobs(cursor, jobs)
    output = "<p>Here is the Department Hours by Boat Report for %s.</p>\n<br />\n"%(datetime.date.today())
    output += "<pre>\n"
    output += "Boat        Fabrication      Paint     Canvas Outfitting\n"
    output += "----------  -----------  ---------  --------- ----------\n"
    for result in results:
        buffer = f"{result[0]:14.12}  "
        buffer += nice_number(result[1], 'Fab')
        buffer += "    " + nice_number(result[1], 'Pai')
        buffer += "    " + nice_number(result[1], 'Can')
        buffer += "    " + nice_number(result[1], 'Out')
        output += buffer + "\n"
    output += "</pre>\n"
    subject = "Department Hours by Boat Report for %s.\n\n"%(datetime.date.today())
    mail_results(subject, output)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

