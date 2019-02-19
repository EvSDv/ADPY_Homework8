import psycopg2 as pg

DB_NAME = 'netology_db'
DB_USER = 'postgres'
DB_PASSWORD = '123456q!'


def create_db():
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE Student (
                id INTEGER PRIMARY KEY NOT NULL,
                name VARCHAR(100) NOT NULL,
                gpa NUMERIC(10,2),
                birth TIMESTAMP WITH TIME ZONE);
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE Course (
                id INTEGER PRIMARY KEY NOT NULL,
                name VARCHAR(100) NOT NULL);
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE student_course (
                id serial PRIMARY KEY,
                student_id INTEGER REFERENCES Student(id),
                course_id INTEGER REFERENCES Course(id));
                """)


def add_student(student):
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Student (id,name, gpa, birth) values (%s, %s, %s, %s);
                """, (student.get('id'), student.get('name'), student.get('gpa'), student.get('birth')))


def get_student(student_id):
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT name, gpa, birth
            FROM Student
            WHERE id = %s ;
            """, str(student_id))
            return cur.fetchall()


def get_students(course_id):  # возвращает студентов определенного курса
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""select s.id, s.name, c.name from student_course sc
            join student s on s.id = sc.student_id
            join course c on c.id = sc.course_id WHERE c.id = %s
            """, (str(course_id)))
            return cur.fetchall()


def add_students(course_id, students):  # создает студентов и # записывает их на курс
    conn = pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    for student in students:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO Student (id,name, gpa, birth) values (%s, %s, %s, %s);
            """, (student.get('id'), student.get('name'), student.get('gpa'), student.get('birth')))
        cur = conn.cursor()
        cur.execute("""
            insert into student_course (student_id, course_id) values (%s, %s)
            """, (student.get('id'), course_id))  # добавить связь студент-курс
    conn.commit()
    conn.close()


def add_cours(cours):  # добавляет курс
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Course (id,name) values (%s, %s);
                """, (cours.get('id'), cours.get('name')))


if __name__ == '__main__':
    create_db()
    student = {'id': 1, 'name': 'Иванов Иван Иванович', 'gpa': 8,
               'birth': '1987-09-07'}  # студент для теста add_student()
    add_student(student)
    print(get_student(1))  # тест get_student()
    add_cours({'id': 1, 'name': 'Нетология'})  # Тест add_cours()

    students = [{'id': 2, 'name': 'Batman Bin Superman', 'gpa': 3, 'birth': '1980-05-12'},
                {'id': 3, 'name': 'Андреев Андрей Андреевич', 'gpa': 4, 'birth': '1982-04-16'},
                {'id': 4, 'name': 'Михайлов Михаил Михайлович', 'gpa': 5, 'birth': '1990-08-07'},
                {'id': 5, 'name': 'Антошкин Антон Антонович', 'gpa': 7, 'birth': '1993-01-18'}]

    add_students(1, students)  # Тест add_students()
    print(get_students(1))
