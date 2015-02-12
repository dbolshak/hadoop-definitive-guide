package ru.dbolshak.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import ru.dbolshak.hadoop.mapreduce.MaxTemperatureMapper;
import ru.dbolshak.hadoop.mapreduce.MaxTemperatureReducer;

import java.io.IOException;
import java.util.Arrays;

public class MaxTemperatureTest {
    private static final String A_YEAR_FOR_TEST = String.valueOf(1950);
    private static final int LOW_TEMPERATURE = -5;
    private static final int HIGH_TEMPERATURE = 10;

    private static Text createYearAsText() {
        return new Text(A_YEAR_FOR_TEST);
    }

    private static String readLineFromFile() {
        return "0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999";
    }

    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text(readLineFromFile());

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(createYearAsText(), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(createYearAsText(), Arrays.asList(new IntWritable(HIGH_TEMPERATURE), new IntWritable(LOW_TEMPERATURE)))
                .withOutput(createYearAsText(), new IntWritable(HIGH_TEMPERATURE))
                .runTest();
    }
}
