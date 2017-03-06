import mysql.connector
from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.sql import select, and_
from sqlalchemy.orm import sessionmaker
from openpyxl import Workbook

conn = mysql.connector.connect(user='root', password='*****', host='localhost', database='CSS')
cur = conn.cursor()

db = create_engine("mysql+mysqlconnector://root:Clupea_8@localhost/CSS")
db.echo = False
meta = MetaData()
meta.reflect(bind=db)
projects = Table('projects', meta, autoload=True)
con = db.connect()

attribute_list = ["Office", "Project Name", "PM First Name", "PM Last Name", "SubProjNo", "Customer Name", "External Contact", "Contact"]

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

def get_pending_region(region):
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
        projects.c.region == region,
        projects.c.pmSendStatus== "no",
        projects.c.adminSendStatus=="yes"
    ))
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
        ws = wb.create_sheet(title=reg)
        ws.cell(row=1, column=1).value = "CSS with 'adminSendStatus=[Yes]' and 'pmSendStatus=[No]'for Region %s" % reg
        col=1
        row=3
        for header in attribute_list:
            ws.cell(row=row, column=col).value=header
            col += 1
        row += 1
        col = 1
        missing_surveys = get_pending_region(reg)
        for survey in missing_surveys:
            for val in survey:
                ws.cell(row=row, column=col).value = val
                col += 1
                print("¤ %s" % val)
            row += 1
            col = 1
            print("---")
    #print("%i surveys missing" % counter)
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    wb.save("Missing CSS (regions) %s.xlsx" % timestamp)



def get_pmSendStatus(office):
    pass

def inspect_table(tablename):
    inspector = inspect(db)
    for column in inspector.get_columns(tablename):
        print (column['name'])

def get_fields():
    inspect_table("projects")

def main():
    #inspect_table("projects")
    offices = list(build_office_set())
    regions = list(build_region_set())

    offices.sort()
    regions.sort()

    #get_all_pending_by_office(offices)
    print(regions)
    print_all_pending_by_region(regions)

if __name__ == '__main__':
    main()