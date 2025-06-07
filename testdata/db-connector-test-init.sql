BEGIN;

CREATE TABLE orders (
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT,
    price INT,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE student_course_enrollments (
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    enrollment_date DATE,
    grade VARCHAR(2),
    PRIMARY KEY (student_id, course_id)
);

CREATE TABLE book_authors (
    book_isbn VARCHAR(13) NOT NULL,
    author_id INT NOT NULL,
    royalty_share DECIMAL(5,2),
    PRIMARY KEY (book_isbn, author_id)
);

COMMIT;