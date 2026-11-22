# راهنمای راه‌اندازی پروژه Chinook

## ۱. معرفی محیط

این پروژه در محیط زیر توسعه و آزمایش شده است:

- Windows و WSL2
- Ubuntu 24.04
- Docker و Docker Compose
- SQL Server و SSMS در Windows
- Python برای Producer و Consumer

## ۲. سرویس‌های اصلی

| سرویس | نسخه | پورت میزبان |
|---|---|---|
| Kafka | 4.1.2 | 29092 |
| PostgreSQL | 16 | 15432 |
| ClickHouse | 26.8.36.3 | 18123 و 19000 |
| Airflow | 3.4.0 | 8080 |

## ۳. شبکه Docker

سرویس‌های اصلی از شبکه مشترک زیر استفاده می‌کنند:

`chinook-net`

## ۴. ذخیره‌سازی داده‌ها

| سرویس | Docker Volume |
|---|---|
| Kafka | kafka_kafka_data |
| PostgreSQL | postgres_pgdata |
| ClickHouse | chinook-clickhouse-data |

Volume مربوط به ClickHouse در Compose
به‌صورت External تعریف شده است.

## ۵. فایل‌های Docker Compose

- kafka/compose.yaml
- postgres/compose.yaml
- clickhouse/compose.yaml
- airflow/compose.yaml
- airflow/compose.docker.yaml

## ۶. تنظیمات محرمانه

رمزهای عبور باید در فایل‌های محیطی محلی
قرار بگیرند و در GitHub منتشر نشوند.

فایل‌های .env توسط Git نادیده گرفته می‌شوند.

## ۷. وضعیت راهنمای نصب

این مستند در حال تکمیل است.

دستورهای راه‌اندازی سرویس‌ها، ایجاد دیتابیس‌ها،
بارگذاری داده اولیه Chinook و اجرای Pipeline
پس از بررسی وابستگی‌ها اضافه خواهند شد.

تا تکمیل این مراحل، راه‌اندازی پروژه روی
یک سیستم جدید به‌صورت کامل مستند نشده است.
