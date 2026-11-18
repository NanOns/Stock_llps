-- R08/业务10：BaoStock同捕获重试直接比较规范typed字段，不保存逐行内容摘要。
ALTER TABLE stock.source_baostock_daily
    DROP COLUMN normalized_sha256;
