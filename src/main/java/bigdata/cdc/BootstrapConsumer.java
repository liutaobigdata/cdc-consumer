package bigdata.cdc;

import bigdata.cdc.consumer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tao.liu
 * @data 2020.10
 * @desc kafka consume  from   postgres cdc
 */
public class BootstrapConsumer {
    private static Logger logger = LoggerFactory.getLogger(BootstrapConsumer.class);

    private static String url;
    private static String user;
    private static String password;

    private static String kafkaServer;
    private static String kafkaTopic;
    private static String kafkaConsumerGroup;
    private static int kafkaConsumerNum;
    private static String kafkaSSLLocation;
    private static String checkConnectionTimeRate;

    private volatile static String dbType;

    private static String dingTalkToken;

    private static Properties config(String filePath) {
        Properties prop = new Properties();
        try {
            File file = new File(filePath);

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.contains("#")) {
                    continue;
                }
                String[] kV = line.split("=");
                prop.put(kV[0].trim(), kV[1] != null ? kV[1].trim() : "");

            }
        } catch (Exception e) {
            logger.error("读取配置文件异常" + e.getMessage());
        }
        return prop;
    }

    public static void main(String[] args) {
        try {
            Properties properties = config(args[0]);
            url = properties.getProperty("target_jdbc_url");
            user = properties.getProperty("target_user");
            password = properties.getProperty("target_password");
            kafkaServer = properties.getProperty("kafka_hosts");
            kafkaTopic = properties.getProperty("kafka_topic");
            kafkaConsumerGroup = properties.getProperty("kafka_consumer_group");
            kafkaConsumerNum = Integer.valueOf(properties.getProperty("kafka_consumer_num"));
            kafkaSSLLocation = properties.getProperty("kafka_ssl_location");
            dingTalkToken = properties.getProperty("dingTalk_token");
            checkConnectionTimeRate = properties.getProperty("check_connection_rate");
            dbType = properties.getProperty("target_db_type");
            System.setProperty("environment", properties.getProperty("environment"));


            BootstrapConsumer cdcConsumer = new BootstrapConsumer();

            cdcConsumer.consume(kafkaConsumerNum, kafkaServer, kafkaTopic, kafkaSSLLocation, kafkaConsumerGroup);

        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
    }

    private void consume(int consumerNum, String kafkaServer, String topic, String locationPath, String groupId) throws Exception {

        doConsumer(consumerNum, kafkaServer, locationPath, topic, groupId);
    }

    private void doConsumer(int consumerNum, String kafkaServer, String locationPath, String topic, String groupId) throws Exception {

        Thread[] consumerThreads = new Thread[consumerNum];
        for (int i = 0; i < consumerNum; i++) {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.consumerProperties(kafkaServer, locationPath, groupId));
            KafkaConsumerRunner kafkaConsumerRunner = new KafkaConsumerRunner(consumer, i, topic);
            consumerThreads[i] = new Thread(kafkaConsumerRunner);
        }

        for (int i = 0; i < consumerNum; i++) {
            consumerThreads[i].start();
        }

    }

    class KafkaConsumerRunner implements Runnable {
        private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, new ScheduledThreadPoolExecutor.CallerRunsPolicy());
        private boolean ISVALID = true;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;
        private volatile Connection connection;
        private int partition;
        private String topic;
        private ParseEvent parseEvent;
        private Notify notify;

        KafkaConsumerRunner(KafkaConsumer consumer, int partition, String topic) {
            this.consumer = consumer;
            this.partition = partition;
            this.topic = topic;
            this.parseEvent = new ParseEvent();
            this.notify = new Notify(dingTalkToken);
            PgConfig pgConfig = new PgConfig().url(url).user(user).password(password);
            try {

                scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> {

                    if (!ISVALID) {
                        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        Notify notify = new Notify(dingTalkToken);
                        notify.sendDingTalk("【" + timestamp + "】adb重新尝试连接");

                        switch (dbType) {
                            case "mysql":
                                connection = ResourceConnection.createMysqlConn(url, user, password);

                                break;
                            case "postgres":
                                connection = ResourceConnection.createRplConn(pgConfig);
                                break;
                        }


                    }


                }, 0, Long.valueOf(checkConnectionTimeRate), TimeUnit.MINUTES);

                switch (dbType) {
                    case "mysql":
                        connection = ResourceConnection.createMysqlConn(url, user, password);
                        break;
                    case "postgres":

                        connection = ResourceConnection.createRplConn(pgConfig);
                        break;
                }


            } catch (Exception e) {
                ISVALID = false;
                notify.sendDingTalk("adb获取连接异常" + e.getLocalizedMessage());
            }
        }

        @Override
        public void run() {
            try {
                consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));
                while (!closed.get()) {
                    try {


                        if (this.connection.isValid(3600)) {
                            ISVALID = true;
                            ConsumerRecords<String, String> records = consumer.poll(1000);
                            for (ConsumerRecord<String, String> record : records) {
                                Statement st = this.connection.createStatement();

                                Event event = parseEvent.parseEvent(record.value());
                                String resultSql = null;
                                String sql;

                                if (event.getEventType().name().equalsIgnoreCase("delete")) {

                                    String id = event.getDataList().get(0).getValue();
                                    resultSql = "delete from " + event.getTable() + " where id=" + id;
                                } else if (event.getEventType().name().equalsIgnoreCase("insert")) {
                                    ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
                                    List<String> keys = new ArrayList<>();
                                    List<Object> values = new ArrayList<>();


                                    event.getDataList().forEach(item -> {
                                        String value = item.getValue();
                                        String key = item.getName();
                                        if (key.contains("old-key") || key.contains("new-tuple")) {
                                            map.put("id", value);
                                            return;
                                        }
                                        map.put(key, value);
                                    });

                                    StringBuilder insert = new StringBuilder("insert   into " + event.getTable() + "(");

                                    map.forEach((key, value) -> {
                                        keys.add(key);
                                        values.add(value);
                                    });
                                    int colunmsSize = keys.size();
                                    for (String key : keys) {
                                        insert.append(key);
                                        insert.append(",");
                                    }

                                    sql = insert.substring(0, insert.length() - 1) + ")values(";
                                    StringBuilder sb = new StringBuilder(sql);

                                    for (int j = 0; j < colunmsSize; j++) {
                                        if (values.get(j) == null || ((String) values.get(j)).equalsIgnoreCase("null")) {
                                            sb.append("null");
                                        } else {
                                            sb.append("'" + TimeUtil.timeFormat(values.get(j).toString()) + "'");
                                        }
                                        sb.append(",");
                                    }

                                    resultSql = sb.substring(0, sb.length() - 1) + ")";
                                    map.clear();
                                    keys.clear();
                                    values.clear();
                                } else if (event.getEventType().name().equalsIgnoreCase("update")) {
                                    String id = event.getDataList().get(0).getValue();
                                    ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
                                    List<String> keys = new ArrayList<>();
                                    List<Object> values = new ArrayList<>();


                                    event.getDataList().forEach(item -> {
                                        String value = item.getValue();
                                        String key = item.getName();
                                        if (key.contains("old-key") || key.contains("new-tuple")) {
                                            map.put("id", value);
                                            return;
                                        }
                                        map.put(key, value);
                                    });

                                    StringBuilder insert = new StringBuilder("update " + event.getTable() + " set ");

                                    map.forEach((key, value) -> {
                                        if (value != null && !value.toString().equals("null")) {
                                            insert.append(key + "='" + value + "',");
                                        } else {
                                            insert.append(key + "=" + value + ",");
                                        }


                                    });

                                    sql = insert.substring(0, insert.length() - 1) + " where id=" + id;

                                    resultSql = sql;
                                    map.clear();
                                    keys.clear();
                                    values.clear();
                                }

                                st.executeUpdate(resultSql);
                                st.close();
                            }
                            consumer.commitSync();
                        } else {
                            ISVALID = false;

                        }

                    } catch (Exception e) {
                        logger.error(e.getLocalizedMessage());
                    }
                }

            } catch (WakeupException e) {
                if (!closed.get()) {
                    throw e;
                }
            }
        }

    }

}
