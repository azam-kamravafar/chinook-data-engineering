CREATE DATABASE IF NOT EXISTS ChinookDW;

CREATE TABLE ChinookDW.DimCustomer
(
    `CustomerKey` UInt64,
    `CustomerId` UInt32,
    `FirstName` String,
    `LastName` String,
    `Company` Nullable(String),
    `Address` Nullable(String),
    `City` Nullable(String),
    `State` Nullable(String),
    `Country` Nullable(String),
    `PostalCode` Nullable(String),
    `Email` String,
    `SupportRepId` Nullable(UInt32),
    `ValidFrom` DateTime64(3, 'UTC'),
    `ValidTo` Nullable(DateTime64(3, 'UTC')),
    `IsCurrent` UInt8,
    `RowHash` String,
    `SourceEventId` Nullable(UInt64),
    `LoadedAt` DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (CustomerId, ValidFrom, CustomerKey)
SETTINGS index_granularity = 8192;

CREATE TABLE ChinookDW.FactSales
(
    `InvoiceLineId` UInt32,
    `InvoiceId` UInt32,
    `DateKey` UInt32,
    `CustomerKey` UInt64,
    `TrackKey` UInt32,
    `Quantity` UInt32,
    `UnitPrice` Decimal(10, 2),
    `SalesAmount` Decimal(18, 2),
    `LoadedAt` DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (DateKey, InvoiceLineId)
SETTINGS index_granularity = 8192;
