# pyexcel

## DESCRIPTION

### The project explores a way to insert data from excel files into any database.

*  The idea of the project is to use to define a **schema.json** file which contains information about,
    different database table, which is idicated by "tablename" and "columns".

*  The data about the data base should be a **excel file**, where in a excel file different sheet eg:"sheet1","sheet2"
    contains data for the database. The reference of "sheetname" in the json file will point to sheetname in excel.

            {
                "sheetname":"Sheet1",
                "tablename":"Teacher",
                "columns":[
                    {"name":"Name","datatype":"String","size":10},
                    {"name":"Subject","datatype":"String","size":10}
                ]
            },
            {   "sheetname":"Sheet2",
                "tablename":"Student",
                "columns":[
                    {"name":"Teach_id","datatype":"Integer","ForeignTable":"Teacher"},
                    {"name":"Name","datatype":"String","size":10},
                    {"name":"Age","datatype":"Integer"},
                    {"name":"Subject","datatype":"String","size":10},
                    {"name":"Marks","datatype":"Integer"}
                ]
            }
    


