package FlinkKafka;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
/* parser imports */
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
public class KafkaDemo {
    static Integer globalCounter=0;


    public static void main(String[] args) throws Exception
    {

        //这里是个环境 类似spark 的sc.
        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "127.0.0.1:9092");
        p.setProperty("zookeeper.connect", "localhost:2181");
        p.setProperty("group.id", "test");
        // sc. add source. 类似 sc.loaddb...之类.
        // FlinkKafkaConsumer011 ("topic",结构)
        // SimpleStringSchema 简化的字符结构.
        // 最后一个参数是 p一个参数数组.
        // 返回是一个DS. data 流 里面都是字符串.
        // 名字叫kafkaData.
        DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), p));

        //##########Going to perform 3 Tuple operation to see how the DAG looks like. ###########
        // 将执行3 Tuple操作以查看DAG的外观。
        // /*#####################
        // 这是一个map 函数. 也可以理解是个转换,或者说是对单行数据处理的闭包函数. 或者叫lamda function.
        
        // 这样可以输出
        kafkaData.print();
        
        kafkaData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
            {
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                {
                    // splite 从kafka采集到的字符串
                    String[] words = value.split(" ");
                    // 遍历这个字符串 输出当前行分裂出来的词的行和数目.
                    for (String word : words)
                        out.collect(new Tuple2<String, Integer>(word, 1));
                }	
            }
        ).keyBy(0).sum(1).writeAsText("/Users/caolei/Desktop/big-data/workspace_mvn/flink-kafka/flinkkafka/kafka.txt");
        //通过第一列进行sum,写入文件中.来一行写一次统计.
        // 比如过来的是hello world
        // 输出为 
        // hello 1 
        // world 1
        // 如果增加一个时间参数,就能得到 hello 1 2020-1-20 这个条数据保存到hdfs中.
        // 下一秒又来了一个 hello 则 hello为2 2020-1-21 写入文件. 这样得到的是一个时序统计数据.
        // 换成sql 一样的某个字段在增加. 这种流数据是不变的. 汇总结果如果不是增量写入,而是批量的写入.
        // 并且可以更新, 比如A pro的得分总数 变成188分. 一次写入一组 1000个用户信息.或者是5分钟内的.这样就是流运算了.
        // 用户或者运营商那里的显示还是直接读表.可以是mysql.但是显示时要加上时序.就变成动态的数据展示了. 有总数汇总.还有每小时,每分钟的统计.
        // 如果超过某个设定值,就可以触发报警或者其他消息. 关键这整个系统是分布式实现的可以很容扩展.
        // flink也是类似spark那样分布式处理.可以集中内存和cpu.进行运算.
        // 




        //*/

        // 下面是对 这个DS进行了多次 mapreduce .
        // 
        // DataStream<Tuple2<String, String>> kf2 = kafkaData.flatMap(new TweetParser());
        // DataStream<String> kf3 = kafkaData.flatMap(new TweetTextParser());
        // DataStream<String> kf4 = kafkaData.flatMap(new TweetParserAddCount());


        //kf2.keyBy(1).writeAsText("/Users/mohammedabdulrazzak/Documents/ZignalLabs/Technology/ApacheFlick//kafka.txt");
                /*
        kafkaData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
        {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
            {
                String[] words = value.split(" ");
                for (String word : words)
                    out.collect(new Tuple2<String, Integer>(word, 1));
            }	})
                .keyBy(0)
                .sum(1)
*/

        //kf3.writeAsText("/Users/mohammedabdulrazzak/Documents/ZignalLabs/Technology/ApacheFlick//kafka.txt");

        // 写回数据源???kafka的另一个频道里去.
        // 这就完成了一个数据转换过程. 好简单.
        // kf4.addSink(new FlinkKafkaProducer011( "flinkSink", new SimpleStringSchema(), p));

        env.execute("Kafka Flink Realtime to sink");
    }

    // 用json解析器解析json 
    public static class TweetParser	implements FlatMapFunction<String, Tuple2<String, String>>
    {
        public void flatMap(String value, Collector<Tuple2<String,String >> out) throws Exception
        {
            ObjectMapper jsonParser = new ObjectMapper();
            // node 就是解出来的json串.
            JsonNode node = jsonParser.readValue(value, JsonNode.class);
            // 如果 有user 字段,并且有语言字段,并且语言字段等于en. 则这个用户是英语用户.
            // 看来这个数据源是 Tweet的流数据.也就是打点数据的清洗.ETL.
            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");
            // 如果有 body 则表示有内容.
            boolean hasText = node.has("body");
            // 默认字符
            String tweet = "No Text  ???  ";
            // 如果有body用body 否则用text.
            if (hasText)
            {
                tweet = node.get("body").asText();
            } else {
                tweet = node.get ("text").asText();
            }
            // 然后再加上一个计数.这个是第几个.
            tweet = tweet + " ===  Tweete Count: "+(globalCounter++).toString();
            // 将所有结果输出成Tuple2 二维数组
            // 这里将元数据返回给下一个处理程序.也就是抽取出来一个字段的有效数据.
            // ['hahahah === Tweet Count:10','{body:"hahah"}']
            out.collect(new Tuple2<String, String>(tweet, value));
        }
    }

    // 对tweet的text直接做解析.保存到一个DF(DS)中
    public static class TweetTextParser	implements FlatMapFunction<String, String>
    {
        public void flatMap(String value, Collector<String> out) throws Exception
        {
            String  tweet = getText(value);
            out.collect(tweet);
        }
    }

    // 边解析边增加计数量.
    public static class TweetParserAddCount	implements FlatMapFunction<String, String>
    {
        public void flatMap(String value, Collector<String> out) throws Exception
        {
            String  tweet = getText(value) + " ^^^ $$$$ ^^^  Tweet Count: "+(globalCounter++).toString();
            out.collect(tweet);
        }
    }

    // 这应该是上面的解析的分解出来的方法. 目的是一样的.
    public static String getText (String value) {
        String tweet = "No Text  ???  ";
        try {
            ObjectMapper jsonP = new ObjectMapper();
            JsonNode node = jsonP.readValue(value, JsonNode.class);
            boolean hasText = node.has("body");

            if (hasText) {
                tweet = node.get("body").asText();
            } else {
                tweet = node.get("text").asText();
            }

        } catch (Exception e) {
            //  Block of code to handle errors
            System.out.println("Something went wrong.");
        }
        return tweet;
    }



}