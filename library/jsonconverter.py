import json
import logging
_log=logging.getLogger("JSON-CON")
class JsonConverter:

    @classmethod
    def object_converter(cls,json_dict:dict):
        obj=cls()
        obj.__dict__=json_dict
        return obj

    def __repr__(self) -> str:
        return str(self.__dict__)

def convert_json(file_path:str):
    with open(file_path) as fp:
        json_object=json.load(fp,object_hook=JsonConverter.object_converter)
        return json_object
