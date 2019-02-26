import psycopg2 as pg

DB_NAME = 'netology_db'
DB_USER = 'postgres'
DB_PASSWORD = '123456q!'


def create_db():
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Student (
                id SERIAL PRIMARY KEY NOT NULL,
                name VARCHAR(100) NOT NULL,
                gpa NUMERIC(10,2),
                birth TIMESTAMP WITH TIME ZONE);
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Course (
                id SERIAL PRIMARY KEY NOT NULL,
                name VARCHAR(100) NOT NULL);
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS student_course (
                id serial PRIMARY KEY,
                student_id INTEGER REFERENCES Student(id) ON DELETE CASCADE,
                course_id INTEGER REFERENCES Course(id) ON DELETE CASCADE);
                """)


def add_student(student):
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Student (name, gpa, birth) values (%s, %s, %s) RETURNING id;
                """, (student['name'], student['gpa'], student['birth']))
            return cur.fetchone()[0]


def get_student(student_id):
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT name, gpa, birth
            FROM Student
            WHERE id = %s ;
            """, str(student_id))
            return cur.fetchone()[0]


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
    cur = conn.cursor()
    cur.execute("""
                SELECT id FROM Course WHERE id = %s;
                """, str(course_id))
    if cur.fetchone() is not None:

        for student in students:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO Student (name, gpa, birth) values (%s, %s, %s) RETURNING id;
                """, (student['name'], student['gpa'], student['birth']))
            id_new_student = cur.fetchone()[0]
            cur = conn.cursor()
            cur.execute("""
                insert into student_course (student_id, course_id) values (%s, %s)
                """, (id_new_student, course_id))  # добавить связь студент-курс
        conn.commit()
    conn.close()


def add_cours(cours):  # добавляет курс
    with pg.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Course (name) values (%s);
                """, (cours['name'],))


if __name__ == '__main__':
    create_db()
    student = {'name': 'Иванов Иван Иванович',
               'gpa': 8,
               'birth': '1987-09-07'}  # студент для теста add_student()
    add_student(student)
    print(get_student('1'))  # тест get_student()
    add_cours({'name': 'Нетология'})  # Тест add_cours()
    #
    students = [{'name': 'Batman Bin Superman', 'gpa': 3, 'birth': '1980-05-12'},
                {'name': 'Андреев Андрей Андреевич', 'gpa': 4, 'birth': '1982-04-16'},
                {'name': 'Михайлов Михаил Михайлович', 'gpa': 5, 'birth': '1990-08-07'},
                {'name': 'Антошкин Антон Антонович', 'gpa': 7, 'birth': '1993-01-18'}]

    add_students(1, students)  # Тест add_students()
    print(get_students(1))
