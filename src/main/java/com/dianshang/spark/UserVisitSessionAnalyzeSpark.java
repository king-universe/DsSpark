package com.dianshang.spark;

import com.alibaba.fastjson.JSONObject;
import com.dianshang.dao.*;
import com.dianshang.dao.factory.DAOFactory;
import com.dianshang.domain.*;
import com.dianshang.model.AggrStatAccumulatorModel;
import com.dianshang.utils.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import parquet.it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings(value = {"unchecked", "rawtypes"})
public class UserVisitSessionAnalyzeSpark {

    private static final Logger LOGGER = LogManager.getLogger(UserVisitSessionAnalyzeSpark.class);

    /**
     * 用户访问session分析Spark作业
     * <p>
     * 接收用户创建的分析任务，用户可能指定的条件如下：
     * <p>
     * 1、时间范围：起始日期~结束日期
     * 2、性别：男或女
     * 3、年龄范围
     * 4、职业：多选
     * 5、城市：多选
     * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
     * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
     * <p>
     * 我们的spark作业如何接受用户创建的任务？
     * <p>
     * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
     * 字段中
     * <p>
     * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
     * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
     * 参数就封装在main函数的args数组中
     * <p>
     * 这是spark本身提供的特性
     *
     * @author Administrator
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName(Constants.SPARK_APP_NAME_SESSION);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sparkContext.sc());
        mockData(sparkContext, sqlContext);
        // 创建需要使用的DAO组件
        ITaskDAO iTaskDAO = DAOFactory.getTaskDAO();

        //首先得查询出来指定的任务，并获取任务的查询参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
//        mockData(sparkContext, sqlContext);
//        outputMessage(sqlContext);
        Task task = iTaskDAO.findById(taskId);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            LOGGER.error(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据

        /**
         * actionRDD，就是一个公共RDD
         * 第一，要用ationRDD，获取到一个公共的sessionid为key的PairRDD
         * 第二，actionRDD，用在了session聚合环节里面
         *
         * sessionid为key的PairRDD，是确定了，在后面要多次使用的
         * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
         * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
         *
         * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
         *
         */
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);


        JavaPairRDD<String, Row> session2ActionRdd = getSession2ActionRdd(actionRDD);

        /**
         * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，第二选择
         * StorageLevel.MEMORY_AND_DISK()，第三选择
         * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
         * StorageLevel.DISK_ONLY()，第五选择
         *
         * 如果内存充足，要使用双副本高可靠机制
         * 选择后缀带_2的策略
         * StorageLevel.MEMORY_ONLY_2()
         *
         */
//        session2ActionRdd = session2ActionRdd.persist(StorageLevel.MEMORY_ONLY());


        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sparkContext, sqlContext, session2ActionRdd);

//        List<Tuple2<String, String>> collect1 = sessionid2AggrInfoRDD.collect();
       /* for (Tuple2<String, String> tuple :
                collect1) {
            System.out.println(tuple);
        }*/
        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的

        // 重构，同时进行过滤和统计
        SessionAggrStatAccumulator statAccumulator = new SessionAggrStatAccumulator();
        sparkContext.sc().register(statAccumulator);

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sparkContext, sessionid2AggrInfoRDD, taskParam, statAccumulator);
//        filteredSessionid2AggrInfoRDD.collect();
//        System.out.println(statAccumulator.value().toString());

        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

//        System.out.println(statAccumulator.value().toString());

        // 生成公共的RDD：通过筛选条件的session的访问明细数据

        /**
         * 重构：sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
         */
        JavaPairRDD<String, Row> session2DetailRdd = getSession2DetailRdd(filteredSessionid2AggrInfoRDD, session2ActionRdd);
//        System.out.println(statAccumulator.value().toString());

        /**
         * 对时间进行处理，然后进行抽取
         */
        randomExtractSession(sparkContext, taskId, filteredSessionid2AggrInfoRDD, session2DetailRdd, statAccumulator);
//        System.out.println(statAccumulator.value().toString());


        /**
         * 特别说明
         * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
         * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
         * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
         */


        /**
         * 获取top10热门品类
         */
        List<Tuple2<CategorySortKey, String>> top10Category = getTop10Category(taskId, session2DetailRdd);


        /**
         * 获取top10活跃session
         */
        getTop10Session(taskId, sparkContext, top10Category, session2DetailRdd);
        sparkContext.close();
    }

    /**
     * 获取top10活跃的session
     *
     * @param taskId
     * @param sparkContext
     * @param top10Category
     * @param session2DetailRdd
     */
    private static void getTop10Session(Long taskId, JavaSparkContext sparkContext, List<Tuple2<CategorySortKey, String>> top10Category, JavaPairRDD<String, Row> session2DetailRdd) {
        /**
         * 第一步：将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList =
                new ArrayList<Tuple2<Long, Long>>();
        for (Tuple2<CategorySortKey, String> tuple : top10Category) {
            Long top10CategoryId = Long.valueOf(StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(top10CategoryId, top10CategoryId));
        }
        JavaPairRDD<Long, Long> top10CategoryIdPairRDD = sparkContext.parallelizePairs(top10CategoryIdList);


        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = session2DetailRdd.groupByKey();

        JavaPairRDD<Long, String> category2sessionAndCountRdd = sessionid2detailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
                Iterator<Row> iterator = tuple2._2.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (row.isNullAt(6)) {

                    } else {
                        long categoryId = row.getLong(6);
                        Long count = categoryCountMap.get(categoryId);
                        if (count == null) {
                            count = 0L;
                        }
                        count++;
                        categoryCountMap.put(categoryId, count);
                    }
                }
                // 返回结果，<categoryid,sessionid,count>格式
                List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
                System.out.println(categoryCountMap);
                for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                    Long category = entry.getKey();
                    Long count = entry.getValue();
                    String sessionAndCount = tuple2._1 + "," + count;
                    System.out.println("category:    "+category+"      sessionAndCount:   "+sessionAndCount);
                    list.add(new Tuple2<>(category, sessionAndCount));
                }
                return list.iterator();
            }
        });
        // 获取到to10热门品类，被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdPairRDD
                .join(category2sessionAndCountRdd)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple2) throws Exception {

                        return new Tuple2<>(tuple2._1, tuple2._2._2);
                    }
                });
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                Iterator<String> iterable = tuple._2.iterator();
                Long categoryId = tuple._1;
                String[] sessions = new String[10];

                while (iterable.hasNext()) {
                    String sessionCount = iterable.next();

                    Long count = Long.valueOf(sessionCount.split(",")[1]);

                    for (int i = 0; i < sessions.length; i++) {
                        if (sessions[i] == null && count.longValue()!=0) {
                            sessions[i] = sessionCount;
                            break;
                        } else {
                            Long _count = Long.valueOf(sessions[i].split(",")[1]);

                            if (_count < count) {
                                // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                                for (int j = 9; j > i; j--) {
                                    sessions[j] = sessions[j - 1];
                                }
                                sessions[i] = sessionCount;
                                break;
                            }

                        }
                    }
                }
                // 将数据写入MySQL表
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                for (String sessionCount : sessions) {
                    if (sessionCount != null) {
                        String sessionid = sessionCount.split(",")[0];
                        long count = Long.valueOf(sessionCount.split(",")[1]);

                        // 将top10 session插入MySQL表
                        Top10Session top10Session = new Top10Session();
                        top10Session.setTaskid(taskId);
                        top10Session.setCategoryid(categoryId);
                        top10Session.setSessionid(sessionid);
                        top10Session.setClickCount(count);

                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        top10SessionDAO.insert(top10Session);

                        // 放入list
                        list.add(new Tuple2<String, String>(sessionid, sessionid));
                    }
                }
                return list.iterator();
            }
        });
        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(session2DetailRdd);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskId);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.isNullAt(6) ? 0 : row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });

    }


    /**
     * 进行过滤根据task
     *
     * @param sparkContext
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @param statAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaSparkContext sparkContext, JavaPairRDD<String, String> sessionid2AggrInfoRDD, JSONObject taskParam, SessionAggrStatAccumulator statAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                if (!ValidUtils.newBetween(aggrInfo, Constants.FIELD_AGE, "\\|", startAge, endAge)) {
                    return false;
                }
                if (!ValidUtils.newIn(aggrInfo, Constants.FIELD_PROFESSIONAL, "\\|", professionals)) {
                    return false;
                }
                if (!ValidUtils.newIn(aggrInfo, Constants.FIELD_CITY, "\\|", cities)) {
                    return false;
                }
                if (!ValidUtils.newEqual(aggrInfo, Constants.FIELD_SEX, "\\|", sex)) {
                    return false;
                }
                if (!ValidUtils.newIn(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, "\\|", keywords)) {
                    return false;
                }
                if (!ValidUtils.newIn(aggrInfo, Constants.FIELD_CATEGORY_ID, "\\|", categoryIds)) {
                    return false;
                }

                // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                // 进行相应的累加计数

                // 主要走到这一步，那么就是需要计数的session
                statAccumulator.add(Constants.SESSION_COUNT);

                //计算访问时长和访问步长的累加
                Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));

                Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                //访问时长累计
                calculateVisitLength(visitLength, statAccumulator);
                //访问步长累计
                calculatestepLength(stepLength, statAccumulator);


                return true;
            }

            /**
             * 访问时长的
             * @param visitLength
             * @param statAccumulator
             */
            private void calculateVisitLength(Long visitLength, SessionAggrStatAccumulator statAccumulator) {
                if (visitLength >= 1 && visitLength <= 3) {
                    statAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    statAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    statAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength < 30) {
                    statAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength >= 30 && visitLength < 60) {
                    statAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength >= 60 && visitLength < 180) {
                    statAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength >= 180 && visitLength < 600) {
                    statAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength >= 600 && visitLength <= 1800) {
                    statAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    statAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 访问步长的
             * @param stepLength
             * @param statAccumulator
             */
            private void calculatestepLength(Long stepLength, SessionAggrStatAccumulator statAccumulator) {
                if (stepLength >= 1 && stepLength <= 3) {
                    statAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    statAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    statAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength < 30) {
                    statAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength >= 30 && stepLength <= 60) {
                    statAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    statAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return filteredSessionid2AggrInfoRDD;
    }


    private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sparkContext, SQLContext sqlContext, JavaPairRDD<String, Row> session2ActionRdd) {
        JavaPairRDD<String, Iterable<Row>> sessionGroup2ActionRdd = session2ActionRdd.groupByKey();
        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2MessageRdd = sessionGroup2ActionRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                Long userid = null;

                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userid == null) {
                        userid = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);
                    if (!row.isNullAt(6)) {
                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null && !clickCategoryIdsBuffer.toString().contains(clickCategoryId + "")) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }
                    // 实际上这里要对数据说明一下
                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 其实，只有搜索行为，是有searchKeyword字段的
                    // 只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足：不能是null值
                    // 其次，之前的字符串中还没有搜索词或者点击品类id
                    if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywordsBuffer.toString().contains(searchKeyword)) {
                        searchKeywordsBuffer.append(searchKeyword + ",");
                    }

                    //操作时的时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));

                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }

                    // 计算session访问步长
                    stepLength++;
                }

                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

//                System.out.println("startTime: "+startTime+"; endTime: "+endTime);
                // 计算session访问时长（秒）
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;


                // 大家思考一下
                // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                // 然后再直接将返回的Tuple的key设置成sessionid
                // 最后的数据格式，还是<sessionid,fullAggrInfo>

                // 聚合数据，用什么样的格式进行拼接？
                // 我们这里统一定义，使用key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
                return new Tuple2<Long, String>(userid, partAggrInfo);
            }
        });

        String userSql = "select * from user_info";
        Dataset<Row> userDataSet = sqlContext.sql(userSql);
        JavaPairRDD<Long, Row> userId2UserInfoRdd = userDataSet.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });
        /**
         * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
         *
         * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
         * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
         *
         */
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userId2MessageRdd.join(userId2UserInfoRdd);

        JavaPairRDD<String, String> sessionId2FullmessageRdd = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
                String partAggrInfo = longTuple2Tuple2._2._1;
                Row userInfo = longTuple2Tuple2._2._2;

                String sessionid = StringUtils.getFieldFromConcatString(
                        partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);


                int age = userInfo.getInt(3);
                String professional = userInfo.getString(4);
                String city = userInfo.getString(5);
                String sex = userInfo.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<String, String>(sessionid, fullAggrInfo);
            }
        });
        return sessionId2FullmessageRdd;
    }

    /**
     * 将元数据进行session粒度的聚合
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSession2ActionRdd(JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> session2ActionRdd = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
        return session2ActionRdd;
    }

    /**
     * 将所有的数据解析成我们需要的时间内的数据
     *
     * @param sqlContext
     * @param param
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject param) {
        String startDate = ParamUtils.getParam(param, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(param, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where action_time > '" + startDate + "'  and  action_time< '" + endDate + "'";
//        String sql = "select * from user_visit_action where  action_time< '" + endDate+"'";

        Dataset<Row> actionDataset = sqlContext.sql(sql);

        return actionDataset.javaRDD();
    }

    private static void outputMessage(SQLContext sqlContext) {
        String sql = "select  * from user_visit_action";
        Dataset<Row> rowDataset = sqlContext.sql(sql);
        JavaRDD<Row> rdd = rowDataset.javaRDD();
//        System.out.println(rdd);
        JavaPairRDD<String, Row> sessionAndUser = rdd.mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
//                System.out.println(row);
                String sessionId = row.getString(2);
                Long userId = row.getLong(1);
                return new Tuple2<String, Row>(sessionId, row);
            }
        });
        List<Tuple2<String, Row>> collect = sessionAndUser.collect();
        for (Tuple2<String, Row> tuple : collect) {
            System.out.println(tuple._1 + "" + tuple._2.getLong(1));
        }
    }

    private static SQLContext getSQLContext(SparkContext sc) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }

    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 生成删选后的详细数据
     *
     * @param filteredSessionid2AggrInfoRDD
     * @param session2ActionRdd
     * @return
     */
    private static JavaPairRDD<String, Row> getSession2DetailRdd(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String, Row> session2ActionRdd) {
        JavaPairRDD<String, Row> session2DetailRdd = session2ActionRdd.join(filteredSessionid2AggrInfoRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Row, String>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<Row, String>> tuple) throws Exception {
                return new Tuple2<String, Row>(tuple._1, tuple._2._1);
            }
        });
        return session2DetailRdd;
    }

    /**
     * 对数据进行随机抽取按照时间的比例
     *
     * @param sparkContext
     * @param taskId
     * @param sessionid2AggrInfoRDD
     * @param session2ActionRdd
     * @param statAccumulator
     */
    private static void randomExtractSession(JavaSparkContext sparkContext, Long taskId, JavaPairRDD<String, String> sessionid2AggrInfoRDD, JavaPairRDD<String, Row> session2ActionRdd, SessionAggrStatAccumulator statAccumulator) {
        //计算出每小时的RDD数
        //组织成时间为yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String startTime = StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<String, String>(dateHour, tuple._2);
            }
        });

        Map<String, Long> countMap = time2sessionidRDD.countByKey();

//        System.out.println(statAccumulator.value().toString());
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();

        for (Map.Entry<String, Long> entity : countMap.entrySet()) {
            String dateHour = entity.getKey();
            String dateDay = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = entity.getValue();

            Map<String, Long> hourCountMap = dateHourCountMap.get(dateDay);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(dateDay, hourCountMap);
            }
            hourCountMap.put(hour, count);
            // 开始实现我们的按时间比例随机抽取算法

            // 总共要抽取100个session，先按照天数，进行平分

        }

        int extractNumberPerDay = 100 / dateHourCountMap.size();

        /**
         * session随机抽取功能
         *
         * 用到了一个比较大的变量，随机抽取索引map
         * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
         * 还是比较消耗内存和网络传输性能的
         *
         * 将map做成广播变量
         *
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();


        for (Map.Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
            String dateDay = entry.getKey();
            Map<String, Long> hourCountMap = entry.getValue();
            long sessionCount2Day = 0l;

            for (Map.Entry<String, Long> hourEntry : hourCountMap.entrySet()) {
                sessionCount2Day += hourEntry.getValue();
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(dateDay);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(dateDay, hourExtractMap);
            }
            //遍历每小时对应的数据
            for (Map.Entry<String, Long> hourEntry : hourCountMap.entrySet()) {
                String hour = hourEntry.getKey();
                Long count = hourEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量

                long hourExtractNumber = (long) (((double) count / (double) sessionCount2Day) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (long) count;
                }
                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt(count.intValue());
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt(count.intValue());
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
                new HashMap<String, Map<String, IntList>>();

        for (Map.Entry<String, Map<String, List<Integer>>> entry : dateHourExtractMap.entrySet()) {
            String date = entry.getKey();
            Map<String, List<Integer>> hourExtractMap = entry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for (Map.Entry<String, List<Integer>> mapEntry : hourExtractMap.entrySet()) {
                String hour = mapEntry.getKey();
                List<Integer> extractList = mapEntry.getValue();
                IntList intList = new IntArrayList();

                for (Integer extractNum : extractList) {
                    intList.add(extractNum);
                }
                fastutilHourExtractMap.put(hour, intList);
            }
            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }
//        System.out.println(fastutilDateHourExtractMap.toString());

        /**
         * 广播变量，很简单
         * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
         */
        Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sparkContext.broadcast(fastutilDateHourExtractMap);
        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */

        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
        // 然后呢，会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

                String date = tuple2._1;
                String dateDay = date.split("_")[0];
                String dateHour = date.split("_")[1];
                Iterator<String> aggrInfos = tuple2._2.iterator();

                Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.getValue();

                List<Integer> extractIndexList = dateHourExtractMap.get(dateDay).get(dateHour);
                ISessionRandomExtractDAO sessionRandomExtractDAO =
                        DAOFactory.getSessionRandomExtractDAO();

                int index = 0;
                while (aggrInfos.hasNext()) {
                    String sessionAggrInfo = aggrInfos.next();
                    if (extractIndexList.contains(index)) {
                        String sessionid = StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        // 将数据写入MySQL
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskid(taskId);
                        sessionRandomExtract.setSessionid(sessionid);
                        sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                        sessionRandomExtractDAO.insert(sessionRandomExtract);

                        // 将sessionid加入list
                        extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                    }
                    index++;
                }
                return extractSessionids.iterator();
            }
        });
        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(session2ActionRdd);

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(statAccumulator.value(), taskId);

        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple2 = tuple2Iterator.next();
                    Row row = tuple2._2._2;
                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskid(taskId);
                    if (!row.isNullAt(1)) {
                        sessionDetail.setUserid(row.getLong(1));
                    }
                    if (!row.isNullAt(2)) {
                        sessionDetail.setSessionid(row.getString(2));
                    }
                    if (!row.isNullAt(3)) {
                        sessionDetail.setPageid(row.getLong(3));
                    }
                    if (!row.isNullAt(4)) {
                        sessionDetail.setActionTime(row.getString(4));
                    }
                    if (!row.isNullAt(5)) {
                        sessionDetail.setSearchKeyword(row.getString(5));
                    }
                    if (!row.isNullAt(6)) {
                        sessionDetail.setClickCategoryId(row.getLong(6));
                    }
                    if (!row.isNullAt(7)) {
                        sessionDetail.setClickProductId(row.getLong(7));
                    }
                    if (!row.isNullAt(8)) {
                        sessionDetail.setOrderCategoryIds(row.getString(8));
                    }
                    if (!row.isNullAt(9)) {
                        sessionDetail.setOrderProductIds(row.getString(9));
                    }
                    if (!row.isNullAt(10)) {
                        sessionDetail.setPayCategoryIds(row.getString(10));
                    }
                    if (!row.isNullAt(10)) {
                        sessionDetail.setPayProductIds(row.getString(11));
                    }
                    sessionDetails.add(sessionDetail);

//                    System.out.println(row);
                }
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });
    }

    /**
     * 计算各个时间点占的比例
     *
     * @param value
     * @param taskId
     */
    private static void calculateAndPersistAggrStat(AggrStatAccumulatorModel value, Long taskId) {

        Long session_count = value.getSessioncount();
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod1To3s() / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod4To6s() / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod7To9s() / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod10To30s() / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod30To60s() / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod1To3m() / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod3To10m() / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod10To30m() / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) value.getTimePeriod30m() / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod1To3() / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod4To6() / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod7To9() / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod10To30() / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod30To60() / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) value.getStepPeriod60() / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    /**
     * 获取top10入门品类
     *
     * @param taskId
     * @param session2DetailRdd
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(Long taskId, JavaPairRDD<String, Row> session2DetailRdd) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */

        // 获取session访问过的所有品类id
        // 访问过：指的是，点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryidRDD = session2DetailRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                if (!tuple._2.isNullAt(6)) {
                    list.add(new Tuple2<Long, Long>(tuple._2.getLong(6), tuple._2.getLong(6)));
                }
                if (!tuple._2.isNullAt(8)) {
                    String[] clickCategoryIds = tuple._2.getString(8).split(",");
                    for (String clickCategoryId : clickCategoryIds) {
                        list.add(new Tuple2<>(Long.valueOf(clickCategoryId), Long.valueOf(clickCategoryId)));
                    }
                }
                if (!tuple._2.isNullAt(10)) {
                    String[] payCategoryIds = tuple._2.getString(10).split(",");
                    for (String payCategoryId : payCategoryIds) {
                        list.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                    }
                }
                return list.iterator();
            }
        });


        /**
         * 必须要进行去重
         * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        JavaPairRDD<Long, Long> distinctCategoryIdRdd = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */

        // 访问明细中，其中三种访问行为是：点击、下单和支付
        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(session2DetailRdd);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(session2DetailRdd);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(session2DetailRdd);


        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
         *
         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0了
         *
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(distinctCategoryIdRdd, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        /**
         * 第四步：自定义二次排序key
         */

        /**
         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> categorySortKeyRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                String countInfo = tuple._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(categorySortKey, countInfo);
            }
        });


        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = categorySortKeyRDD.sortByKey(false);


        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);

        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String categoryInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    categoryInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    categoryInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    categoryInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    categoryInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskId);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }
        return top10CategoryList;
    }


    /**
     * 计算各个品类的点击次数
     *
     * @param session2DetailRdd
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> session2DetailRdd) {
        JavaPairRDD<String, Row> filterNullCategoryId = session2DetailRdd.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                if (tuple._2.isNullAt(6)) {
                    return false;
                }
                return true;
            }
        });

        JavaPairRDD<Long, Long> clickCategoryIdMap = filterNullCategoryId.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                long clickCategoryId = tuple._2.getLong(6);
                return new Tuple2<>(clickCategoryId, 1L);

            }
        });

        JavaPairRDD<Long, Long> reduceClickCategoryId = clickCategoryIdMap.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return reduceClickCategoryId;
    }

    /**
     * 下单的分类
     *
     * @param session2DetailRdd
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> session2DetailRdd) {
        JavaPairRDD<String, Row> filterNullOrderCategoryIds = session2DetailRdd.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return v1._2.isNullAt(8) ? false : true;
            }
        });

        JavaPairRDD<Long, Long> orderCategoryIdMap = filterNullOrderCategoryIds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                String[] orderCategoryIds = tuple2._2.getString(8).split(",");
                for (String orderCategoryId : orderCategoryIds) {
                    list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Long, Long> reduceOrderCategoryId = orderCategoryIdMap.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        return reduceOrderCategoryId;
    }

    /**
     * 支付的分类
     *
     * @param session2DetailRdd
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> session2DetailRdd) {
        JavaPairRDD<String, Row> filterNullPayCategoryIds = session2DetailRdd.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return v1._2.isNullAt(10) ? false : true;
            }
        });

        JavaPairRDD<Long, Long> payCategoryIdMap = filterNullPayCategoryIds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                String[] orderCategoryIds = tuple2._2.getString(10).split(",");
                for (String orderCategoryId : orderCategoryIds) {
                    list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Long, Long> reducePayCategoryId = payCategoryIdMap.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return reducePayCategoryId;
    }

    /**
     * 连接
     *
     * @param distinctCategoryIdRdd
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> distinctCategoryIdRdd, JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD, JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        JavaPairRDD<Long, String> tmpMapRDD = distinctCategoryIdRdd.leftOuterJoin(clickCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                Long categoryId = tuple._1;
                Optional<Long> optional = tuple._2._2;
                long clickCount = 0L;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                        Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<>(categoryId, value);
            }
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryId = tuple._1;
                String value = tuple._2._1;

                Optional<Long> optional = tuple._2._2;
                long orderCount = 0L;

                if (optional.isPresent()) {
                    orderCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                return new Tuple2<>(categoryId, value);
            }
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryId = tuple._1;
                String value = tuple._2._1;

                Optional<Long> optional = tuple._2._2;
                long payCount = 0L;

                if (optional.isPresent()) {
                    payCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
                return new Tuple2<>(categoryId, value);
            }
        });
        return tmpMapRDD;
    }


}
