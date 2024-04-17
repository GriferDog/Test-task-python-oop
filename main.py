import sys
import psycopg2
from datetime import date, timedelta, datetime
import random
import string

class Console:

    @staticmethod
    def Output(array):
        for i in array:
            print(i.str_With_Age())
        print("Всего строк: "+str(len(array)))

    @staticmethod
    def Output_With_Time(array):
        start_time = datetime.now()
        Console.Output(array)
        delta_time = datetime.now()-start_time
        print("Время выполнения: "+str(delta_time.total_seconds()))


class PersonaGenerator:

    @staticmethod
    def _generate_String(length, first_symbol="") -> string:
        all_symbols = string.ascii_lowercase
        result = "".join(random.choice(all_symbols) for _ in range(length))
        result = first_symbol+result
        result = result[0].upper() + result[1:]
        return result
    
    @staticmethod
    def _generate_Date() -> date:
        start_date = date(year=1970, month=1, day=1)
        end_date = date(year=2006, month=1, day=1)
        delta = end_date - start_date
        return start_date + timedelta(random.randint(0, delta.days))
    
    @staticmethod
    def _generate_Sex() -> string:
        if random.randint(0,1)==1:
            return "Male"
        else:
            return "Female"
    
    @staticmethod
    def Generate_New(first_symbol=""):
        name = PersonaGenerator._generate_String(7,first_symbol)+" "+PersonaGenerator._generate_String(6)+" "+PersonaGenerator._generate_String(7)
        date = PersonaGenerator._generate_Date()
        sex = PersonaGenerator._generate_Sex()
        return name, date, sex

class Employees:
    
    def __init__(self, name = None, birthday = None, sex = None, first_symbol=""):
        if (name!=None)&(birthday!=None)&(sex!=None):
            self.name = name
            self.birthday = birthday
            self.sex = sex
        elif (name==None)&(birthday==None)&(sex!="")&(first_symbol!=""):
            self.name, self.birthday, self.sex = PersonaGenerator.Generate_New(first_symbol)
            self.sex = sex
        else:
            self.name, self.birthday, self.sex = PersonaGenerator.Generate_New()

    def __str__(self) -> string:
        return self.name + "\t" + str(self.birthday) + "\t" + self.sex
            
    def str_With_Age(self) -> string:
        return str(self) + "\t" + str(self.Age())
    
    def Age(self) -> int:
        age = round((date.today() - self.birthday).days // 365.25)
        return age
    
    def add_To_DB(self,db):
        db.create_New(self)


class Database:
    
    def __init__(self):
        self.conn = psycopg2.connect(dbname="Test", user="postgres", password="123", host="127.0.0.1", port="5432")

    def db_close(self):
        self.conn.commit()
        self.conn.close()

    def create_Table(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("CREATE TABLE employees (id SERIAL PRIMARY KEY, name CHARACTER VARYING(30), birthday DATE, sex CHARACTER VARYING(6));")
            cursor.close()
        except psycopg2.errors.DuplicateTable:
            print("This table already exists")
    
    def create_New(self, employee):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO employees (name, birthday, sex) VALUES (%s,%s,%s);", (employee.name, employee.birthday, employee.sex))
        cursor.close()
    
    def create_New_From_Batch(self, employees):
        cursor = self.conn.cursor()
        for i in employees:
            cursor.execute("INSERT INTO employees (name, birthday, sex) VALUES (%s,%s,%s);", (i.name, i.birthday, i.sex))
        cursor.close()

    def create_Object_From_DB(self, line) -> Employees:
        employee = Employees(name=line[1], birthday=line[2], sex=line[3])
        return employee
    
    def create_Many_Objects_From_DB(self, array) -> list:
        employees = []
        for i in array:
            employees.append(self.create_Object_From_DB(i))
        return employees

    def select_All(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM employees ORDER BY name;")
        request = cursor.fetchall()
        cursor.close()
        return self.create_Many_Objects_From_DB(request)
    
    def select_Filter(self, sex, first_symbol):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM employees WHERE sex = %s AND name LIKE %s;", (sex, first_symbol+"%"))
        request = cursor.fetchall()
        cursor.close()
        return self.create_Many_Objects_From_DB(request)


def first_CreateTable(db):
    print("Режим 1")
    db.create_Table()

def second_CreateNew(name, birthday, sex):
    print("Режим 2")
    emp = Employees(name, birthday, sex)
    emp.add_To_DB(db)

def third_ShowAllTable(db):
    print("Режим 3")
    Console.Output(db.select_All())

def fourth_AutoMillionLines(number = 1000000, f_Number = 100):
    print("Режим 4")
    for i in range(0,number):
        emp = Employees()
        db.create_New(emp)
    employees = []
    for i in range(0,f_Number):
        employees.append(Employees(sex="Male",first_symbol="F"))
    db.create_New_From_Batch(employees)

def fifth_SelectTable(db):
    print("Режим 5")
    Console.Output_With_Time(db.select_Filter("Male","F"))
    
def sixth_Optimize():
    print("Режим 6")


if __name__ == "__main__":

    db = Database()

    if sys.argv[1] == "1":
        first_CreateTable(db)
    if sys.argv[1] == "2":
        second_CreateNew(sys.argv[2],sys.argv[3],sys.argv[4])
    if sys.argv[1] == "3":
        third_ShowAllTable(db)
    if sys.argv[1] == "4":
        fourth_AutoMillionLines()
    if sys.argv[1] == "5":
        fifth_SelectTable(db)
    if sys.argv[1] == "6":
        sixth_Optimize()

    db.db_close()