from dataclasses import make_dataclass
from library import jsonconverter as jc
import importlib
import os
import pandas as pd
import logging
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import declarative_base,Session
from sqlalchemy import create_engine
import argparse

logging.basicConfig(level=logging.INFO)
_log=logging.getLogger("MAIN")

def create_db_engine(password,host,db_name):
    """Creates the data bases engine 

    Args:
        password (str): password of database connection
        host (str): hostname of database connection
        db_name (str): database to connect
    Returns:
        Connection object tp create engine
    """
    CONNECTION_STR=f"mysql+pymysql://root:{password}@{host}/{db_name}"
    db_engine=create_engine(CONNECTION_STR,echo=True)
    return db_engine


class SchemaContainer:
    def __init__(self,schema_path:str,emit:bool,engine:object):
        self.schemaPath=schema_path
        self.Base=declarative_base()
        self.read_schema_object(self.schemaPath)
        if emit:
            self.Base.metadata.create_all(engine)
        else:
            pass
    
    def _create_column(self,column):
        positional_args=[] # to add positional arguments make sure the order is correct. 
                            #To check the order see the Column defination in sqlalchecmy
        keyword_args={}  # key word arguments comes after positional arguments.

        if hasattr(column,"datatype"):
            exec(f"from sqlalchemy import {column.datatype}") #dynamically import sqlalchemy datatypes
            datatype=locals().get(column.datatype) # get the datatype class from local namespace
            if hasattr(column,"size"):
                size=column.size
                positional_args.append(datatype(size))
            else:
                positional_args.append(datatype)

        if hasattr(column,"PrimaryKey"):
            keyword_args["primary_key"]=column.PrimaryKey # keyworded argument

        if hasattr(column,"ForeignTable"):
            foreign_table=column.ForeignTable
            positional_args.append(ForeignKey(f"{foreign_table}.{column.name}"))
        
        
        return Column(*positional_args,**keyword_args)       
        
    def _create_namespace(self,table_name,column_data:list):
        """Creates the namespace required for creating a SQLalchemy mapping table.
            The namespace is created as a dictionary so that the mapping class can be created dynamically.
            This method is called after the schema file is read.
        Args:
            table_name (str): the name of the table to be mapped into database.
            column_data (list): The mapping of columns indicating which type of column to create.
                                This data is found from reading the json file indicating the schema.
                                This json  
        Returns:
            dict: The dictonary representing the namespace
        Example:
            {__tablename__:"tablename",...}
        """
        name_space={"__tablename__":table_name} 
        
        for column in column_data:
            name_space[column.name]=self._create_column(column)
        return name_space

    def read_schema_object(self,file_path):
        """Reads the schema file, to create different mapping class,dynamically
            For example:
                ++++++++++++ schmea +++++++++++++
                {   "sheetname":"Sheet1",
                    "tablename":"Student",
                    "columns":[
                        {"name":"Name","datatype":"String","size":10},
                        {"name":"Age","datatype":"Integer","size":2},
                        {"name":"Marks","datatype":"Integer","size":3}
                    ]
                }

                ++++++++++++ Mapping class ++++++++
                class Student(Base):
                    __tablename__ = "Student"
                    id:int = Column(Integer,primary_key=True)
                    ..

        Args:
            file_path (str): The json file path for schemafile
        """
        schema=jc.convert_json(file_path)
        setattr(self,"schema",schema)
        for sheet in schema.sheets:
            setattr(self,str(sheet.tablename),make_dataclass(sheet.tablename,[],bases=(self.Base,),namespace=self._create_namespace(sheet.tablename,sheet.columns)))

class ExcelReader:
    def __init__(self,schemaObj:SchemaContainer,file_path:str) -> None:
        self.schemaObj=schemaObj
        self.schema=getattr(schemaObj,"schema")
        self.file_path=file_path

    def insertData(self,data,table,engine,columns):
        """Inserts Data into database tables.
        Args:
            data (list): list of all rows to be inserted into database
            table (object): The mapping class 
            engine (object): The database engine 
            columns (list): The list of columns to create a dictionary of parameters 
                            which can be used for initilaizing the class.
                            example:
                                {"name":"abc","class":12}
        """
        with Session(engine) as session:
            for row in data:
                attribute_dict={key:value for key,value in zip(columns,row)}
                session.add(table(**attribute_dict))
            session.commit()

    def readData(self,engine):
        """Reads the excel data and converts into dataframe
        Args:
            engine (Object): SQLalchemy database engine.
        """
        for sheet in self.schema.sheets:
            df=pd.read_excel(io=self.file_path,sheet_name=sheet.sheetname,engine="openpyxl")
            table=getattr(self.schemaObj,sheet.tablename)
            self.insertData(df.values,table,engine,df.columns)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hostname",help="hostname of database",dest="hostname",default="localhost")
    parser.add_argument("--password",help="password for database",dest="password",default="password")
    parser.add_argument("--database",help="database name",dest="dbname")
    parser.add_argument("schemapath",help="path for schema json file")
    parser.add_argument("excelpath",help="path for excel data")
    args=parser.parse_args()
    SCHEMA_PATH=args.schemapath
    EXCEL_PATH=args.excelpath
    PASS=args.password
    HOST=args.hostname
    DB=args.dbname 
    db_engine=create_db_engine(PASS,HOST,DB)
    sc=SchemaContainer(SCHEMA_PATH,True,db_engine)
    er=ExcelReader(sc,EXCEL_PATH)
    er.readData(engine=db_engine)