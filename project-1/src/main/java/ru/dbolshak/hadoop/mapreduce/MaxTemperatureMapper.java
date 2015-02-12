package ru.dbolshak.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.dbolshak.hadoop.model.AirTemperature;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    private static final String REG_EXP_QUALITY_MATCHER = "[01459]";

    private static boolean isValidParam(int param, String quality) {
        return param != MISSING && quality.matches(REG_EXP_QUALITY_MATCHER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirTemperature airTemperature = new AirTemperature(value);

        int temperature = airTemperature.getAirTemperature();
        if (isValidParam(temperature, airTemperature.getQuality())) {
            context.write(new Text(airTemperature.getYear()), new IntWritable(temperature));
        }
    }
}
