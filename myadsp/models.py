# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, ARRAY, String, Text
from sqlalchemy.ext.declarative import declarative_base
import json
from adsputils import get_date, UTCDateTime

Base = declarative_base()


class KeyValue(Base):
    """Example model, it stores key/value pairs - a persistent configuration"""
    __tablename__ = 'storage'
    key = Column(String(255), primary_key=True)
    value = Column(Text)

    def toJSON(self):
        return {'key': self.key, 'value': self.value }


class AuthorInfo(Base):
    __tablename__ = 'authors'

    id = Column(Integer, primary_key=True)
    created = Column(UTCDateTime)
    last_sent = Column(UTCDateTime)


class Results(Base):
    __tablename__ = 'results'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    qid = Column(String(32))
    results = Column(ARRAY(String))
    created = Column(UTCDateTime)