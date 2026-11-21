USE [Chinook_CDC_Lab]
GO

/****** Object:  Trigger [dbo].[trg_Customer_Outbox]    Script Date: 11/21/2026 7:31:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   TRIGGER [dbo].[trg_Customer_Outbox]
ON [dbo].[Customer]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.ChangeOutbox
    (
        EntityName,
        EntityId,
        OperationType,
        BeforeData,
        AfterData
    )
    SELECT
        'Customer',

        COALESCE(i.CustomerId, d.CustomerId),

        CASE
            WHEN d.CustomerId IS NULL THEN 'INSERT'
            WHEN i.CustomerId IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END,

        CASE
            WHEN d.CustomerId IS NULL THEN NULL
            ELSE (
                SELECT old.*
                FROM deleted AS old
                WHERE old.CustomerId = d.CustomerId
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
            )
        END,

        CASE
            WHEN i.CustomerId IS NULL THEN NULL
            ELSE (
                SELECT new.*
                FROM inserted AS new
                WHERE new.CustomerId = i.CustomerId
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
            )
        END

    FROM inserted AS i
    FULL OUTER JOIN deleted AS d
        ON i.CustomerId = d.CustomerId;
END;

GO

ALTER TABLE [dbo].[Customer] ENABLE TRIGGER [trg_Customer_Outbox]
GO
