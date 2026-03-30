-- 01 Create Table
CREATE TABLE students (
    id INT  KEY,
    name TEXT(20),
    age INT INDEX,
    active BOOL
);

CREATE TABLE courses (
    course_id INT KEY,
    course_name TEXT(20),
    credits INT
);

SHOW database;

