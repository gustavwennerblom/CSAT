import json
import logging
import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.sql import select, and_
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
import analytics


class CSATanalyzer:

    def __init__(self):
        # Create logger
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename="CSATstats.log", format=FORMAT, level=logging.INFO)

        # Read MySQL config and credentials and store in a dict
        with open("creds_mysql.json") as f:
            logging.info("Accessing database credentials")
            config = json.loads(f.readline())

        sqluser = config["sqluser"]
        sqlpassword = config["sqlpassword"]
        sqlhost = config["sqlhost"]
        database_name = config["database"]

        self.conn = mysql.connector.connect(user=sqluser, password=sqlpassword, host=sqlhost, database=database_name)
        self.cur = self.conn.cursor()

        self.db = create_engine("mysql+mysqlconnector://%s:%s@%s/%s" % (sqluser, sqlpassword, sqlhost, database_name))
        self.db.echo = False

        meta = MetaData()
        meta.reflect(bind=self.db)
        self.projects = Table('projects', meta, autoload=True)
        self.questions = Table('questions', meta, autoload=True)
        self.answers = Table('answers', meta, autoload=True)
        self.ratings = Table('ratings', meta, autoload=True)
        self.con = self.db.connect()
        logging.info("Connected to database")

        self.attribute_list = ["Office", "Client", "Project number", "Sent to PM",
                               "Sent by PM", "PM First Name", "PM Last Name", "Date of upload"]
        self.title_font = Font(bold=True)

    # Constructs a set of all offices in the database
    def build_office_set(self):
        stmt = select([self.projects.c.office])
        result = self.con.execute(stmt)
        office_set = set()
        for row in result:
            office_set.add(row[0])
        logging.info("Set of offices constructed from database")
        return office_set

    def build_region_set(self):
        stmt = select([self.projects.c.region])
        result = self.con.execute(stmt)
        region_set = set()
        for row in result:
            region_set.add(row[0])
        logging.info("Set of regions constructed from database")
        return region_set

    # Currently not in use
    def get_pending(self, office):
        stmt = select([
            self.projects.c.office,
            self.projects.c.projectName,
            self.projects.c.pmName,
            self.projects.c.pmLastName,
            self.projects.c.subProjectNo,
            self.projects.c.customerName,
            self.projects.c.externalContact,
            self.projects.c.contact
        ]).where(and_(
            self.projects.c.office == office,
            self.projects.c.pmSendStatus == "no",
            self.projects.c.adminSendStatus == "yes"
        ))
        result = self.con.execute(stmt)

        out = []
        for row in result:
            out.append(row)
        return out

    # Returns list of two ints, the first one representing total CSS's due to send by the office and the second one
    # the number of actually sent CSS's
    def count_pending(self, office, **kwargs):
        start_date = datetime(2017, 1, 1)
        try:
            assert isinstance(kwargs["start_date"], datetime)
            start_date=kwargs["start_date"]
        except:
            logging.info("No start date given, considering surveys triggered from {0} "
                         "and forward in time".format(start_date))

        stmt_total = select([self.projects.c.subProjectNo]).where(and_(
            self.projects.c.office == office,
            self.projects.c.dateUpload > start_date))

        stmt_pending = select([self.projects.c.subProjectNo]).where(and_(
            self.projects.c.office == office,
            self.projects.c.pmSendStatus == "no",
            self.projects.c.dateUpload > start_date))


        result = [len(self.con.execute(stmt_total).fetchall()), len(self.con.execute(stmt_pending).fetchall())]
        return result

    # TODO: Add the kwarg to set a custom start date
    # Returns list of two ints, the first one representing total CSS's due to send by the country and the second one
    # the number of actually sent CSS's
    def count_pending_by_country(self, country):
        unit_set = analytics.map_country_to_units(country)
        country_result =[0,0]
        for unit in unit_set:
            unit_result=self.count_pending(unit)
            country_result[0]+=unit_result[0]
            country_result[1]+=unit_result[1]

        return country_result


    #test function
    def get_a_date(self):
        stmt = select([self.projects.c.dateUpload]).where(self.projects.c.office == "Shanghai")
        result = self.con.execute(stmt)
        one_date = result.fetchone()
        return 1




    # Returns a list of answered surveys for a given office:
    def get_answers_office(self, office):
        stmt = select([
            self.projects.c.office,
            self.projects.c.customerName,
            self.projects.c.subProjectNo,
            self.projects.c.pmName,
            self.projects.c.pmLastName,
            self.answers.c.dateAnswer,
            self.answers.c.questionId,
            self.questions.c.question,
            self.answers.c.answersNumeric,
            self.answers.c.answersText,
        ]).where(and_(
            self.answers.c.ratingId == self.ratings.c.ratingId,
            self.answers.c.questionId == self.questions.c.questionId,
            self.ratings.c.projectId == self.projects.c.projectId,
            self.projects.c.office == office)
        ).order_by(
            self.projects.c.office,
            self.projects.c.subProjectNo,
            self.answers.c.answerId,
            self.answers.c.questionId
        )

        result = self.con.execute(stmt)
        logging.info("Queried database for surveys answered: office % s" % office)

        out = []
        for row in result:
            out.append(row)
        return out


    # Returns list of pending surveys for a given region
    def get_pending_region(self, region):
        stmt = select([
            self.projects.c.office,
            self.projects.c.customerName,
            self.projects.c.subProjectNo,
            self.projects.c.adminSendStatus,
            self.projects.c.pmSendStatus,
            self.projects.c.pmName,
            self.projects.c.pmLastName,
            self.projects.c.dateUpload
        ]).where(and_(
            self.projects.c.region == region,
        )).order_by(
            self.projects.c.office,
            self.projects.c.subProjectNo,
            self.projects.c.adminSendStatus,
            self.projects.c.pmSendStatus)

        result = self.con.execute(stmt)
        logging.info("Queried database for surveys triggered")
        out = []
        for row in result:
            out.append(row)
        return out


    def print_all_pending_by_office(self, offices):
        counter = 0
        for office in offices[:2]:
            out = self.get_pending(office)
            for s in out:
                print(s)
                counter += 1
            print("---")
        print("%i surveys missing" % counter)


    def print_all_pending_by_region(self, regions):
        wb = Workbook()
        logging.info("Preparing results spreadsheet")
        for reg in regions:
            totals = {}
            ws = wb.create_sheet(title=reg)
            ws.cell(row=1, column=1).value = "Client Satisfaction Surveys for Region %s" % reg
            col = 1
            row = 3
            # Loop to write header row
            for header in self.attribute_list:
                ws.cell(row=row, column=col).value = header
                ws.cell(row=row, column=col).font = self.title_font
                col += 1
            row += 1
            col = 1

            # Collect list of pending surveys for the region
            missing_surveys = self.get_pending_region(reg)
            i = 0
            for survey in missing_surveys:
                totals[survey[0]] = []
                for val in survey:
                    try:
                        totals[survey[0]][i] += 1
                    except IndexError:
                        logging.info("Creating index for %s" % survey[0])
                        totals[survey[0]].append(0)
                    i += 1
                    ws.cell(row=row, column=col).value = val
                    col += 1
                    # print("¤ %s" % val)
                row += 1
                col = 1
                i = 0
                # print("---")

            row += 1
            ws.cell(row=row, column=col).value = "Totals by office"
            row += 1
            col = 1
            ws.cell(row=row, column=col).value = "Total sent by PM"
            col += 1
            ws.cell(row=row, column=col).value = "Total sent to client"
            col += 1
            ws.cell(row=row, column=col).value = "Total rejected"
            row += 1
            col = 1
            for key, values in totals.items():
                ws.cell(row=row, column=col).value = key
                col += 1
                ws.cell(row=row, column=col).value = values[0]
                col += 1
                ws.cell(row=row, column=col).value = values[1]
                col = 1
                row += 1

        # print("%i surveys missing" % counter)

        # Clean up autocreated blank sheets in workbook
        wb.remove_sheet(wb.get_sheet_by_name("Sheet"))
        logging.info("Cleaning up stuff...")
        # wb.remove_sheet(wb.get_sheet_by_name("Sheet1"))

        # Add a time/date stamp to filename and save
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        filename = "Missing CSS (regions) %s.xlsx" % timestamp
        wb.save(filename)
        logging.info("Excel file saved in program root with name %s" % filename)


    @staticmethod
    def alternating_fill(colorcode):
        color1 = PatternFill("solid", fgColor="00CCFF")
        color2 = PatternFill("solid", fgColor="FFFFFF")
        if colorcode == color1:
            return color2
        else:
            return color1


    def print_all_answers_by_office(self, answers, headers):
        wb = Workbook()
        logging.info("Preparing answers workbook")
        cellcolor = self.alternating_fill(PatternFill("solid", fgColor="FFFFFF"))

        for office in answers:
            ws = wb.create_sheet(title=office)
            ws.cell(row=1, column=1).value = "Client Satisfaction Survey Answers for %s" % office
            col = 1
            row = 3
            # Loop to write header row
            for header in headers:
                ws.cell(row=row, column=col).value = header
                ws.cell(row=row, column=col).font = self.title_font
                col += 1
            row += 1
            col = 1

            answers_for_office = self.get_answers_office(office)

            for answer in answers_for_office:
                for data in answer:
                    ws.cell(row=row, column=col).value = data
                    col += 1
                # Change cell fill color between survey answers
                # cellcolor = alternating_fill(cellcolor)
                # ws.cell(row=row, column=col).fill = cellcolor
                # Put cursor on new row and first column
                row += 1
                col = 1

        # Clean up autocreated blank sheets in workbook
        wb.remove_sheet(wb.get_sheet_by_name("Sheet"))
        logging.info("Cleaning up stuff...")

        # Add a time/date stamp to filename and save
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        filename = "CSS answers (by office) %s.xlsx" % timestamp
        wb.save(filename)
        logging.info("Excel file saved in program root with name %s" % filename)


    def inspect_table(self, tablename):
        inspector = inspect(self.db)
        for column in inspector.get_columns(tablename):
            print(column['name'])
        # stmt = select([projects])
        # out = con.execute(stmt)
        # print(out.fetchall())


    def get_fields(self):
        self.inspect_table("projects")


    def get_status_main(self):
        # inspect_table("projects")
        offices = list(self.build_office_set())
        regions = list(self.build_region_set())

        offices.sort()
        regions.sort()

        self.print_all_pending_by_region(regions)
        logging.info("All done, shutting down. Enjoy.")


    def get_answers_main(self):
        offices = list(self.build_office_set())
        offices.sort()
        headers = ["Office", "Client", "Project no", "PM Name", "PM Last Name",
                   "Date answered", "Question number", "Question", "Answer (score)", "Answer (text/comment)"]

        self.print_all_answers_by_office(offices, headers)

        logging.info("All done, shutting down. Enjoy.")


if __name__ == '__main__':
    # Method to trigger printout of survey's pending to be sent
    # get_status_main()

    # Method to trigger collection of answers
    ca = CSATanalyzer()
    ca.get_a_date()
    offices = ca.build_office_set()
    for office in offices:
        print("{1}: {2} pending, of total {0}". format(ca.count_pending(office, start_date=datetime(2017, 1, 1))[0], office, ca.count_pending(office, start_date=datetime(2017,1,1))[1]))

