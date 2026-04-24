
CREATE SCHEMA  trolling;
GO


CREATE TABLE trolling.Transactions (
    TransactionID     INT            NOT NULL PRIMARY KEY,
    AccountID         INT            NOT NULL,
    Amount            DECIMAL(12,2)  NULL,
    TransactionDate   DATETIME       NULL,
    TransactionType   NVARCHAR(20)   NULL,
    amount_flag       NVARCHAR(50)   NULL
);


select * from trolling.Transactions