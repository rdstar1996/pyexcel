from threading import local
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from dataclasses import dataclass, field, make_dataclass

Base=declarative_base()

mapper_registry = registry()
# class User(Base):
#     __tablename__='Users'

#     id=Column(Integer(),primary_key=True)
#     name=Column(String(length=5))
#     fullname=Column(String(length=5))
#     nickname=Column(String(length=5))

#     def __repr__(self):
#         return f"<User(name:{self.name},fullname:{self.fullname},nickname:{self.nickname})"

# @mapper_registry.mapped
# @dataclass
# class User:
#     __tablename__ = "user"

#     __sa_dataclass_metadata_key__ = "sa"
#     id: int = field(
#         init=False, metadata={"sa": Column(Integer, primary_key=True)}
#     )
#     name: str = field(default=None, metadata={"sa": Column(String(50))})
#     fullname: str = field(default=None, metadata={"sa": Column(String(50))})
#     nickname: str = field(default=None, metadata={"sa": Column(String(12))})

# fields=[("id",int,field(init=True,default=Column(Integer, primary_key=True)))]
User=make_dataclass("User",[],bases=(Base,),namespace={"__tablename__":"User","id": Column(Integer, primary_key=True)})
if __name__ == "__main__":
    # CONNECTION_STR="mysql+pymysql://root:password@localhost/giraffe"
    # db_engine=create_engine(CONNECTION_STR,echo=True)
    # Base.metadata.create_all(db_engine)
    # u1=User(name="ritwk",fullname="ritwi",nickname="subho")
    u1=User()
    print(u1.__dict__)
    print(User.__dict__)
    pass



