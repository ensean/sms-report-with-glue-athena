## 说明

使用Glue & Athena 生成SNS SMS使用月报

## 配置方式

1. SNS SMS启用用量日报，保存至S3

2. 使用Glue生成table

3. 使用Glue ETL Job（parse_phone.py）步骤2生成的结果进行调整，保存结果到S3
    * 从手机号码获取国家/地区名称
    * 获取发送状态：Success|Fail

4. 使用Glue爬取步骤3生成的数据，生成table

5. 使用Athena查询步骤4生成的table，输出使用统计报表
