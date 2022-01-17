from datetime import datetime
from app import db
from time import time

class Unit(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    unit_no = db.Column(db.String(64), index=True, unique=True)

    def __repr__(self):
        return '{}'.format(self.unit_no)

class BasicBitchPayment(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(64))
    unit1 = db.Column(db.String(64))
    payment = db.Column(db.String(64))
    pay_float = db.Column(db.Numeric)
    date_code = db.Column(db.String(10))

    def __repr__(self):
        return f'{self.unit1} {self.name} {self.payment} {self.pay_float} {self.date_code}'

