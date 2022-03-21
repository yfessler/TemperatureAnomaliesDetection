import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TemperatureAnomalyDetection {

    static final int defaultNumberOfDevices = 2000;
    static final int defaultNumberOfMeasurementsPerMinute = 100;
    static final int defaultWindowSizeInSeconds = 60;

    public  static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = getDevicesStreams(env, params);

        int windowSizeInSeconds = defaultWindowSizeInSeconds;
        if (params.has("measurementsPerMinute")) {
            windowSizeInSeconds = Integer.parseInt(params.get("windowSize"));
        }

        DataStream<Tuple5<String, Double, Long, Double, Double>> devicesMeasurementStream  = dataStream
            .flatMap(new FlatMapFunction<String, Tuple3<String, Double, Long>>() {
                @Override
                public void flatMap(
                        String input,
                        Collector<Tuple3<String, Double, Long>> collector) throws Exception {
                    // Input example: Device 455weg75uew, measurement 95.4 C, time 45587456
                    String[] inputArr = input.split(",");
                    String deviceId = inputArr[0].trim();
                    Double temperature = Double.parseDouble(inputArr[1]/*.split(" ")[1]*/.trim());
                    Long timeStamp =  Long.parseLong(inputArr[2]/*.split(" ")[1]*/.trim());
                    collector.collect(new Tuple3<String, Double, Long>(
                            deviceId,
                            temperature,
                            timeStamp));
                }
            })
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Double, Long>>() {
                @Override
                public long extractAscendingTimestamp(Tuple3<String, Double, Long> measurement) {
                    return measurement.f2;
                }
            })
            .keyBy(new KeySelector<Tuple3<String, Double, Long>, String>()
            {
                @Override
                public String getKey(Tuple3<String, Double, Long> value) {
                    return (String) value.getField(0);
                }
            })
            .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSizeInSeconds),
                    Time.seconds((int)(windowSizeInSeconds * 0.25))))
            .apply(new AnomalyDetectionWindowFunction());

        devicesMeasurementStream.print();

        env.execute("Temperature Anomaly Detection");

    }

    private static DataStream<String> getDevicesStreams(
            StreamExecutionEnvironment env,
            final ParameterTool params) {

        int numOfDevices = defaultNumberOfDevices;
        if (params.has("numOfDevices")) {
            numOfDevices = Integer.parseInt(params.get("numOfDevices"));
        }

        DataStream<String> dataStream = env.addSource(new IOTDeviceDataSource(params));
        DataStream<String>  deviceStream;
        for(var i=0; i < numOfDevices -1 ; i++) {
            deviceStream = env.addSource(new IOTDeviceDataSource(params));
            dataStream= dataStream.union(deviceStream);
        }
        return dataStream;
    }


    private static class IOTDeviceDataSource extends RichParallelSourceFunction<String> {

        private volatile boolean running = true;
        private UUID deviceId = UUID.randomUUID();
        private int numMeasurementsPerMinute;

        public IOTDeviceDataSource(final ParameterTool params)
        {
            System.out.println(String.format("Creating new Device Data source: %s", this.deviceId.toString()));

            numMeasurementsPerMinute = defaultNumberOfMeasurementsPerMinute;
            if (params.has("measurementsPerMinute")) {
                numMeasurementsPerMinute = Integer.parseInt(params.get("measurementsPerMinute"));
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String val = null;
            Random rand = new Random();

            Long sleepTimeInMillis = Math.round((double)(60 * 1000) / numMeasurementsPerMinute);

            System.out.println(String.format("Starting Stream for device %s", this.deviceId.toString()));

            while (running) {
                int rand_int = rand.nextInt(10000);
                double rand_temp = (double)rand_int / 100;

                val = String.format("%s,%f,%d",
                        this.deviceId.toString(),
                        rand_temp,
                        System.currentTimeMillis());
                ctx.collect(val);
                Thread.sleep(sleepTimeInMillis);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class AnomalyDetectionWindowFunction
            implements WindowFunction<Tuple3<String, Double, Long>,
            Tuple5<String, Double, Long, Double, Double>, String, TimeWindow> {

        private static Tuple2<Double, Double> calculateStatistics(List<Double> numArray)
        {
            double sum = 0.0, standardDeviation = 0.0;
            int length = numArray.size();

            for(double num : numArray) {
                sum += num;
            }

            double mean = sum/length;

            for(double num: numArray) {
                standardDeviation += Math.pow(num - mean, 2);
            }

            double sd = Math.sqrt(standardDeviation/length);

            return new Tuple2<>(mean, sd);
        }

        @Override
        public void apply(
                String s,
                TimeWindow timeWindow,
                Iterable<Tuple3<String, Double, Long>> iterable,
                Collector<Tuple5<String, Double, Long, Double, Double>> out) throws Exception {

            List<Tuple3<String, Double, Long>> measurements = new ArrayList<Tuple3<String, Double, Long>>();
            iterable.forEach(m-> measurements.add(m));

            List<Double> values = new ArrayList<>();
            measurements.forEach(m-> values.add(m.f1));

            Tuple2<Double, Double> statistics =  calculateStatistics(values);

            for(Tuple3<String, Double, Long> m : measurements)
            {
                if (Math.abs(m.f1 - statistics.f0) > 1 * statistics.f1) {
                    out.collect(
                            new Tuple5<>(
                                    m.f0,
                                    m.f1,
                                    m.f2,
                                    statistics.f0,
                                    statistics.f1
                            ));

                    System.out.println(String.format("Device %s, measurement %f C, time %d",
                            m.f0.toString(),
                            m.f1,
                            m.f2));
                }
            }

        }
    }
}


