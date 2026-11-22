# Chinook Data Engineering Pipeline

**Tech stack:** SQL Server · Python · Kafka · PostgreSQL · Airflow · Spark · ClickHouse · Grafana · Docker

## English

A hands-on, end-to-end data engineering project using the Chinook dataset. Customer changes are captured in SQL Server and processed for analytics and monitoring.

**Pipeline:** SQL Server (Trigger + Outbox) → Python Producer → Kafka → Python Consumer → PostgreSQL (staging + ledger) → Airflow / Spark (SCD Type 2) → ClickHouse → Grafana.

**Highlights:** Customer change tracking, historical customer versions (SCD Type 2), a sales star schema, and checks for duplicates, broken references, amounts, and SCD history. Initial sales data is loaded separately from the Kafka CDC flow.

**Dashboards:** Sales Intelligence · Customers & Catalog · Pipeline Operations · Warehouse Quality. JSON exports are in `grafana/dashboards/`.

**Local setup:** Requires Docker/Compose, Python, and access to SQL Server. Configure private `.env` files, start the services, run the producer, and trigger the `chinook_cdc_to_warehouse` Airflow DAG. See [`docs/SETUP_FA.md`](docs/SETUP_FA.md). Never commit credentials.

**Status:** Core flow and sample events tested. Full automation, clean-install verification, and final recovery tests are pending. **Portfolio project; not production-ready.**

---

## فارسی

این پروژه یک پایپ‌لاین عملی مهندسی داده با دیتاست Chinook است. تغییرات مشتریان در SQL Server ثبت و پس از انتقال و پردازش، برای تحلیل و مانیتورینگ آماده می‌شوند.

**مسیر داده:** SQL Server (Trigger + Outbox) → Python → Kafka → PostgreSQL → Airflow / Spark (SCD Type 2) → ClickHouse → Grafana.

**قابلیت‌ها:** ثبت تغییرات مشتری، نگهداری تاریخچه با SCD Type 2، مدل ستاره‌ای فروش و کنترل کیفیت داده (تکراری‌ها، کلیدهای نامعتبر، اختلاف مبلغ و تاریخچه). بارگذاری اولیه فروش جدا از مسیر Kafka انجام شده است.

**داشبوردها:** تحلیل فروش، مشتریان و محصولات، مانیتورینگ پایپ‌لاین و کیفیت انبار داده. فایل‌ها در `grafana/dashboards/` هستند.

**راه‌اندازی:** Docker/Compose، Python و دسترسی به SQL Server لازم است. فایل‌های `.env` شخصی را تنظیم کنید، سرویس‌ها و Producer را اجرا کنید و DAG با نام `chinook_cdc_to_warehouse` را در Airflow اجرا کنید. راهنمای فعلی: [`docs/SETUP_FA.md`](docs/SETUP_FA.md). رمزها را وارد Git نکنید.

**وضعیت:** بخش‌های اصلی و رویدادهای آزمایشی تست شده‌اند؛ خودکارسازی کامل، نصب از صفر و تست نهایی بازیابی هنوز باقی مانده است. **پروژه نمونه‌کار است، نه سرویس آماده Production.**
