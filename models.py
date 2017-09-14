from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Project:
    __tablename__ = 'projects'
    office = Column(String)
    projectName =