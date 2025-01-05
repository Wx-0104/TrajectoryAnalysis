package org.example;

import org.apache.spark.sql.SparkSession;

public class TrajectoryProvinceSnapshotProcessor {

    public static void main(String[] args) {
        // 设置 Hadoop 用户名，以便 Spark 可以正确连接到 Hive 和 Hadoop 文件系统
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 创建 SparkSession，SparkSession 是与 Spark SQL 交互的入口
        SparkSession sparkSession = createSparkSession();
        // 设置 Hive 的动态分区模式为 nonstrict
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict;");

        // 执行 Hive 表创建和数据插入操作
        createTablesAndInsertData(sparkSession);

        // 执行业务逻辑查询
        processData(sparkSession);

        // 结束 Spark 会话，释放资源
        sparkSession.stop();
    }

    /**
     * 创建 SparkSession，启用 Hive 支持
     */
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .enableHiveSupport()  // 启用 Hive 支持，使 Spark 可以与 Hive 进行交互
                .appName("Spark on Hive")  // 设置应用程序的名称
                .getOrCreate();
    }

    /**
     * 创建 Hive 表并插入示例数据
     */
    private static void createTablesAndInsertData(SparkSession sparkSession) {


        // 创建 `t_lbs_trajectory` 表
        String createTrajectoryTable = "create table if not exists t_lbs_trajectory\n" +
                "(\n" +
                "    c_msisdn          string comment \"手机号\",\n" +
                "    c_imsi            string comment \"国际移动用户识别码\",\n" +
                "    c_imei            string comment \"国际移动设备识别码\",\n" +
                "    c_uli             string comment \"基站编码\",\n" +
                "    c_lng             string comment \"经度\",\n" +
                "    c_lat             string comment \"纬度\",\n" +
                "    c_home_province   string comment \"国内用户归属省\",\n" +
                "    c_curr_province   string comment \"用户当前所在省\",\n" +
                "    c_areacode        string comment \"区域编码\",\n" +
                "    c_homecode        string comment \"归属地编码\",\n" +
                "    c_imsi_mcc        string comment \"IMSI中的MCC\",\n" +
                "    c_msisdn_home     string comment \"信令系统识别国内号码并摘取其中的7位归属地号码\",\n" +
                "    c_imei_brand      string comment \"设备品牌，imei前8位\",\n" +
                "    c_event_timestamp timestamp comment \"数据记录事件时间\",\n" +
                "    c_event_date      string comment \"数据记录事件日期\"\n" +
                ") comment \"Hive历史轨迹表\";";

        // 执行创建 `t_lbs_trajectory` 表的 SQL
        sparkSession.sql(createTrajectoryTable);

        // 插入数据到 `t_lbs_trajectory`
        String insertData = "insert overwrite table t_lbs_trajectory\n" +
                "(c_msisdn, c_imsi, c_imei, c_uli, c_lng, c_lat, c_home_province, c_curr_province,\n" +
                " c_areacode, c_homecode, c_imsi_mcc, c_msisdn_home, c_imei_brand, c_event_timestamp, c_event_date)\n" +
                "VALUES ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735680000000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735680600000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735681200000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '天津', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735681800000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '天津', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735682400000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '天津', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735683000000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '天津', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735683600000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735684200000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735684800000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13812345678', '460023456789012', '869234123456789', '12345', '116.405285', '39.904989', '北京', '北京', '010',\n" +
                "        '01001', '460', '0101234', 'Xiaomi', cast(from_unixtime(1735685400000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735680000000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735680600000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735681200000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '南京', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735681800000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '南京', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735682400000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '南京', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735683000000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '南京', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735683600000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735684200000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735684800000 / 1000) as timestamp), '20250102'),\n" +
                "       ('13923456789', '460024567890123', '868765432109876', '54321', '121.473701', '31.230416', '上海', '上海', '021',\n" +
                "        '02102', '460', '0212345', 'Huawei', cast(from_unixtime(1735685400000 / 1000) as timestamp), '20250102');\n";

        // 执行插入操作
        sparkSession.sql(insertData);
    }

    /**
     * 执行业务逻辑：处理数据并插入到 `t_lbs_person_province_snapshot` 表中
     */
    private static void processData(SparkSession sparkSession) {
        String createSnapshotTable = "create table if not exists t_lbs_person_province_snapshot\n" +
                "(\n" +
                "    c_msisdn      string comment \"手机号\",\n" +
                "    c_imsi        string comment \"国际移动用户识别码\",\n" +
                "    c_imei        string comment \"国际移动设备识别码\",\n" +
                "    c_homecode    string comment \"归属地编码\",\n" +
                "    c_imsi_mcc    string comment \"IMSI中的MCC\",\n" +
                "    c_msisdn_home string comment \"信令系统识别国内号码并摘取其中的7位归属地号码\",\n" +
                "    c_imei_brand  string comment \"设备品牌，imei前8位\",\n" +
                "    c_province    string comment \"用户所在省\",\n" +
                "    c_duration    int comment \"驻留时长\",\n" +
                "    c_cnt         int comment \"轨迹点数量\"\n" +
                ") comment '国家中心快照hive数据表'\n" +
                "    partitioned by (`c_event_date` string);";

        // 创建 `t_lbs_person_province_snapshot` 表
        sparkSession.sql(createSnapshotTable);

        // 执行 SQL 处理业务逻辑，生成数据并插入
        String query = "with t1 as (select c_msisdn,\n" +
                "                   c_curr_province,\n" +
                "                   c_event_timestamp,\n" +
                "                   c_event_date,\n" +
                "                   case\n" +
                "                       when c_event_date =\n" +
                "                            lag(c_event_date) over (partition by c_msisdn,c_event_date order by c_event_timestamp)\n" +
                "                           and c_curr_province =\n" +
                "                               lag(c_curr_province) over (partition by c_msisdn,c_event_date order by c_event_timestamp)\n" +
                "                           then 0\n" +
                "                       else 1\n" +
                "                       end as curr_province_change\n" +
                "            from t_lbs_trajectory),\n" +
                "     t2 as (select c_msisdn,\n" +
                "                   c_curr_province,\n" +
                "                   c_event_timestamp,\n" +
                "                   c_event_date,\n" +
                "                   sum(curr_province_change)\n" +
                "                       over (partition by c_msisdn,c_event_date order by c_event_timestamp) as curr_province_window_id\n" +
                "            from t1),\n" +
                "     t3 as (select c_msisdn,\n" +
                "                   c_event_date,\n" +
                "                   c_curr_province,\n" +
                "                   unix_timestamp(max(c_event_timestamp)) - unix_timestamp(min(c_event_timestamp)) c_duration,\n" +
                "                   curr_province_window_id\n" +
                "            from t2\n" +
                "            group by c_msisdn, c_event_date, c_curr_province, curr_province_window_id),\n" +
                "     t4 as (select c_msisdn,\n" +
                "                   c_event_date,\n" +
                "                   c_curr_province                   c_province,\n" +
                "                   sum(c_duration)                   c_duration,\n" +
                "                   count(curr_province_window_id) as c_cnt\n" +
                "            from t3\n" +
                "            group by c_msisdn, c_event_date, c_curr_province\n" +
                "            order by c_msisdn, c_event_date)\n" +
                "insert overwrite table t_lbs_person_province_snapshot partition ( c_event_date )\n" +
                "select t4.c_msisdn,\n" +
                "       c_imsi,\n" +
                "       c_imei,\n" +
                "       c_homecode,\n" +
                "       c_imsi_mcc,\n" +
                "       c_msisdn_home,\n" +
                "       c_imei_brand,\n" +
                "       c_province,\n" +
                "       c_duration,\n" +
                "       c_cnt,\n" +
                "       t4.c_event_date\n" +
                "from t4\n" +
                "         left join (select distinct c_msisdn,\n" +
                "                                    c_imsi,\n" +
                "                                    c_imei,\n" +
                "                                    c_homecode,\n" +
                "                                    c_imsi_mcc,\n" +
                "                                    c_msisdn_home,\n" +
                "                                    c_imei_brand\n" +
                "                    from t_lbs_trajectory) t on t4.c_msisdn = t.c_msisdn;";

        // 执行查询操作
        sparkSession.sql(query);

        sparkSession.sql("select * from t_lbs_person_province_snapshot where c_event_date=20250102").show();
    }
}
