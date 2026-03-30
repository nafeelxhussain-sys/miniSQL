-- 06 UPDATE Test
UPDATE students SET age = 24 WHERE name = 'Bob';
UPDATE students SET active = false WHERE age > 23;

SELECT * FROM students;