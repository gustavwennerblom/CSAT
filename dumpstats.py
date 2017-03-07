import mysql.connector
from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.sql import select, and_, or_
from sqlalchemy.orm import sessionmaker
from openpyxl import Workbook
from openpyxl.styles import Font
import sys

sqluser="root"
sqlpassword="***"

if sqlpassword=="***":
    print("Update to actual password")
    sys.exit(0)

conn = mysql.connector.connect(user=sqluser, password=sqlpassword, host='localhost', database='CSS')
cur = conn.cursor()

db = create_engine("mysql+mysqlconnector://%s:%s@localhost/CSS" % (sqluser, sqlpassword))
db.echo = False
meta = MetaData()
meta.reflect(bind=db)
projects = Table('projects', meta, autoload=True)
con = db.connect()

# attribute_list_OLD = ["Office", "Project Name", "PM First Name", "PM Last Name", "SubProjNo", "Customer Name", "External Contact", "Contact"]
attribute_list = ["Office", "Client", "Project number", "Sent to PM", "Sent by PM","PM First Name", "PM Last Name"]
title_font = Font(bold=True)

# Constructs a set of all offices in the database
def build_office_set():
    stmt = select([projects.c.office])
    result = con.execute(stmt)
    office_set=set()
    for row in result:
        office_set.add(row[0])
    return office_set

def build_region_set():
    stmt = select([projects.c.region])
    result = con.execute(stmt)
    region_set=set()
    for row in result:
        region_set.add(row[0])
    return region_set

def get_pending(office):
    stmt = select([
        projects.c.office,
        projects.c.projectName,
        projects.c.pmName,
        projects.c.pmLastName,
        projects.c.subProjectNo,
        projects.c.customerName,
        projects.c.externalContact,
        projects.c.contact
    ]).where(and_(
        projects.c.office == office,
        projects.c.pmSendStatus== "no",
        projects.c.adminSendStatus=="yes"
    ))
    result = con.execute(stmt)

    out = []
    for row in result:
        out.append(row)
    return out

# Returns list of pending surveys for a given region
def get_pending_region(region):
    stmt = select([
        projects.c.office,
        projects.c.customerName,
        projects.c.subProjectNo,
        projects.c.adminSendStatus,
        projects.c.pmSendStatus,
        projects.c.pmName,
        projects.c.pmLastName,
    ]).where(and_(
        projects.c.region == region,
    )).order_by(
        projects.c.office,
        projects.c.subProjectNo,
        projects.c.adminSendStatus,
        projects.c.pmSendStatus)

    result = con.execute(stmt)

    out = []
    for row in result:
        out.append(row)
    return out

def print_all_pending_by_office(offices):
    counter = 0
    for office in offices[:2]:
        out = get_pending(office)
        for s in out:
            print(s)
            counter += 1
        print("---")
    print("%i surveys missing" % counter)

def print_all_pending_by_region(regions):
    wb = Workbook()



    for reg in regions:
        Totals = {}
        ws = wb.create_sheet(title=reg)
        ws.cell(row=1, column=1).value = "CSS with 'adminSendStatus=[Yes]' and 'pmSendStatus=[No]'for Region %s" % reg
        col=1
        row=3
        # Loop to write header row
        for header in attribute_list:
            ws.cell(row=row, column=col).value=header
            ws.cell(row=row, column=col).font=title_font
            col += 1
        row += 1
        col = 1

        #Collect list of pending surveys for the region
        missing_surveys = get_pending_region(reg)
        i = 0
        for survey in missing_surveys:
            Totals[survey[0]]=[]
            for val in survey:
                try:
                    Totals[survey[0]][i] += 1
                except IndexError:
                    print("Creating index for %s" % survey[0])
                    Totals[survey[0]].append(0)
                i += 1
                ws.cell(row=row, column=col).value = val
                col += 1
                # print("¤ %s" % val)
            row += 1
            col = 1
            i = 0
            # print("---")

        row+=1
        ws.cell(row=row, column=col).value="Totals by office"
        row+=1
        col = 1
        ws.cell(row=row, column=col).value="Total sent by PM"
        col+=1
        ws.cell(row=row, column=col).value = "Total sent to client"
        col += 1
        ws.cell(row=row, column=col).value = "Total rejected"
        row += 1
        col = 1
        for key, values in Totals.items():
            ws.cell(row = row, column=col).value = key
            col += 1
            ws.cell(row=row, column=col).value = values[0]
            col += 1
            ws.cell(row=row, column=col).value = values[1]
            col = 1
            row += 1


    #print("%i surveys missing" % counter)


    # Clean up autocreated blank sheets in workbook
    wb.remove_sheet(wb.get_sheet_by_name("Sheet"))
    # wb.remove_sheet(wb.get_sheet_by_name("Sheet1"))

    # Add a time/date stamp to filename and save
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    wb.save("Missing CSS (regions) %s.xlsx" % timestamp)



def get_pmSendStatus(office):
    pass

def inspect_table(tablename):
    inspector = inspect(db)
    for column in inspector.get_columns(tablename):
        print (column['name'])
    # stmt = select([projects])
    # out = con.execute(stmt)
    # print(out.fetchall())

def get_fields():
    inspect_table("projects")


def main():
    #inspect_table("projects")
    offices = list(build_office_set())
    regions = list(build_region_set())

    offices.sort()
    regions.sort()

    #get_all_pending_by_office(offices)
    #print(regions)
    print_all_pending_by_region(regions)

    #inspect_table('projects')

if __name__ == '__main__':
    main()