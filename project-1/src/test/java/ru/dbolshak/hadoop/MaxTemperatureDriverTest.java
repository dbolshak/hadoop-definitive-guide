package ru.dbolshak.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import ru.dbolshak.hadoop.job.MaxTemperatureDriver;
import ru.dbolshak.hadoop.mapreduce.MaxTemperatureMapper;
import ru.dbolshak.hadoop.mapreduce.MaxTemperatureReducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MaxTemperatureDriverTest {
    private static final int A_YEAR_FOR_TEST = 1950;
    private static final int LOW_AIR_TEMPERATURE = -11;
    private static final int HIGH_AIR_TEMPERATURE = 10;
    private static final int AIR_TEMPERATURE_CODE_QUALITY = 1;

    private static Text createYearAsText() {
        return new Text(String.valueOf(A_YEAR_FOR_TEST));
    }

    private static String readLineFromFile() {
        return buildLineWithWellKnownValues();
    }

    private static String buildLineWithWellKnownValues() {
        return String.format(
                "004301199099999%d051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9%05d%d+99999999999",
                A_YEAR_FOR_TEST, LOW_AIR_TEMPERATURE, AIR_TEMPERATURE_CODE_QUALITY);
    }

    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text(readLineFromFile());

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(createYearAsText(), new IntWritable(LOW_AIR_TEMPERATURE))
                .runTest();
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(createYearAsText(), Arrays.asList(new IntWritable(HIGH_AIR_TEMPERATURE), new IntWritable(LOW_AIR_TEMPERATURE)))
                .withOutput(createYearAsText(), new IntWritable(HIGH_AIR_TEMPERATURE))
                .runTest();
    }

    @Test
    public void testMaxTemperatureDriver() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        Path input = new Path(this.getClass().getClassLoader().getResource ("sample.txt").toString());
        Path output = new Path("output");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output
        int exitCode = driver.run(new String[] {input.toString(), output.toString() });

        assertThat(exitCode, is(0));
        checkOutput(output, fs);
    }

    private void checkOutput(Path output, FileSystem fs) throws IOException {
        InputStream inputStream = fs.open(new Path(output, "part-r-00000"));
        byte[] mapReduceOutput = new byte[17];
        inputStream.read(mapReduceOutput);
        assertThat(new String(mapReduceOutput), is("1949\t111\n1950\t22\n"));
    }

}
