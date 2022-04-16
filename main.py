from cmath import log
from dataclasses import make_dataclass
from library import jsonconverter as jc
import os
import pandas as pd
import logging
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base,Session
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
_log=logging.getLogger("MAIN")

def create_db_engine(password,host,db_name):
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
    
    def _create_namespace(self,table_name,column_data:list):
        '''
           Create the namespace {__tablename__:"tablename",...} for the Mapping class 
        '''
        name_space={"__tablename__":table_name,
                    "id":Column(Integer,primary_key=True)}
        
        for column in column_data:
            if column.datatype=="Integer":
                name_space[column.name]=Column(Integer)
            if column.datatype=="String":
                name_space[column.name]=Column(String(column.size))
        return name_space

    def read_schema_object(self,file_path):
        '''
            Create the object from json file and create dynamic class and assign them as attributes.
            The structure of the object created is as follows:-
            obj.schema=parsed value of the json file containing meta data about excel data.
            obj.<table1> .... obj.<tableN> =dynamically created class based on the meta data.
        '''
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
        with Session(engine) as session:
            for row in data:
                attribute_dict={key:value for key,value in zip(columns,row)}
                print(attribute_dict)
                session.add(table(**attribute_dict))
            session.commit()

    def readData(self,push_data,engine):
        for sheet in self.schema.sheets:
            print(sheet.sheetname)
            df=pd.read_excel(io=self.file_path,sheet_name=sheet.sheetname,engine="openpyxl")
            columns=df.columns
            table=getattr(self.schemaObj,sheet.tablename)
            if push_data:
                self.insertData(df.values,table,engine,columns)

if __name__ == "__main__":
    ROOT_PATH=os.path.dirname(__file__)
    SCHEMA_PATH=os.path.join(ROOT_PATH,"testdata","schema.json")
    EXCEL_PATH=os.path.join(ROOT_PATH,"testdata","Marksheet.xlsx")
    db_engine=create_db_engine("password","localhost","giraffe")
    sc=SchemaContainer(SCHEMA_PATH,True,db_engine)
    er=ExcelReader(sc,EXCEL_PATH)
    er.readData(push_data=True,engine=db_engine)