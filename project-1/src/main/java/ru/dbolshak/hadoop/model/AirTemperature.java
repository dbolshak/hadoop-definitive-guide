package ru.dbolshak.hadoop.model;

import org.apache.hadoop.io.Text;

/**
 * Just a Pojo, which knows how to handle a line from file to present a temperature information.
 */
public class AirTemperature {
    private static final int MISSING = 9999;
    private static final String REG_EXP_QUALITY_MATCHER = "[01459]";

    private final String line;

    public AirTemperature(Text line) {
        this.line = line.toString();
    }

    public String getYear() {
        return line.substring(15, 19);
    }

    public int getAirTemperature() {
        return Integer.parseInt(line.substring(87, 92));
    }

    public boolean isValid() {
        return getAirTemperature() != MISSING && getQuality().matches(REG_EXP_QUALITY_MATCHER);
    }

    private String getQuality() {
        return line.substring(92, 93);
    }
}
