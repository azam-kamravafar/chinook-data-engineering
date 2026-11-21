USE [Chinook_CDC_Lab]
GO

/****** Object:  Table [dbo].[ChangeOutbox]    Script Date: 11/21/2026 7:22:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ChangeOutbox](
	[EventId] [bigint] IDENTITY(1,1) NOT NULL,
	[EntityName] [nvarchar](100) NOT NULL,
	[EntityId] [int] NOT NULL,
	[OperationType] [varchar](6) NOT NULL,
	[BeforeData] [nvarchar](max) NULL,
	[AfterData] [nvarchar](max) NULL,
	[CreatedAtUTC] [datetime2](3) NOT NULL,
	[PublishedAtUTC] [datetime2](3) NULL,
PRIMARY KEY CLUSTERED
(
	[EventId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[ChangeOutbox] ADD  DEFAULT (sysutcdatetime()) FOR [CreatedAtUTC]
GO

ALTER TABLE [dbo].[ChangeOutbox]  WITH CHECK ADD  CONSTRAINT [CK_ChangeOutbox_Operation] CHECK  (([OperationType]='DELETE' OR [OperationType]='UPDATE' OR [OperationType]='INSERT'))
GO

ALTER TABLE [dbo].[ChangeOutbox] CHECK CONSTRAINT [CK_ChangeOutbox_Operation]
GO
