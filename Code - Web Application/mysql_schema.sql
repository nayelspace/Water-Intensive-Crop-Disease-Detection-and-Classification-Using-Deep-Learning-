CREATE TABLE User
(
    id       INT AUTO_INCREMENT PRIMARY KEY,
    name     VARCHAR(255) NOT NULL,
    email    VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    datetime DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE Field
(
    id       INT AUTO_INCREMENT PRIMARY KEY,
    user_id  INT NOT NULL,
    name     VARCHAR(255) NOT NULL,
    datetime DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    code     VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES User(id)
);

CREATE TABLE Batch
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    user_id    INT NOT NULL,
    field_id   INT NOT NULL,
    img_qty    INT NOT NULL,
    x_grid     INT NOT NULL,
    y_grid     INT NOT NULL,
    datetime   DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    date_taken DATE,
    FOREIGN KEY (user_id) REFERENCES User(id),
    FOREIGN KEY (field_id) REFERENCES Field(id)
);

CREATE TABLE Image
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    batch_id   INT NOT NULL,
    filename   VARCHAR(255) NOT NULL,
    label      VARCHAR(255) DEFAULT 'no' NOT NULL,
    path       VARCHAR(255) NOT NULL,
    datetime   DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    healthy    FLOAT,
    rice_blast FLOAT,
    brown_spot FLOAT,
    `order`    INT DEFAULT 0 NOT NULL,
    date_taken DATE,
    FOREIGN KEY (batch_id) REFERENCES Batch(id)
);
