## 用途

将SNS SMS每日使用报告存储至MySQL数据库，供后续报表分析

## 配置说明

1. 创建用于保存使用报告的S3桶，并配置VPC endpoint

2. SNS SMS开启每日使用报告，保存至S3桶

3. 创建RDS数据库、表，留意VPC、安全组配置

3）创建Lambda，添加两个Layer，VPC、安全组配置确保与RDS互通

4）测试数据是否能够成功写入到RDS MySQL



