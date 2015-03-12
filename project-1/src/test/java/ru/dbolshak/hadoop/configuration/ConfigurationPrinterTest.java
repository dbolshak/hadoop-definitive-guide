package ru.dbolshak.hadoop.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Created by dbolshak on 12/03/15.
 */
public class ConfigurationPrinterTest {
    private final ConfigurationPrinter configurationPrinter = new ConfigurationPrinter();

    @BeforeClass
    public static void initClass() {
        Configuration.addDefaultResource("configuration-1.xml");
        Configuration.addDefaultResource("configuration-2.xml");
    }

    @Test
    public void run() throws Exception {
        int exitCode = ToolRunner.run(configurationPrinter, new String[]{"-D color=yellow "});
        System.exit(exitCode);
    }
}

class ConfigurationPrinter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        System.out.println(conf.get("color"));
        for (Map.Entry<String, String> entry: conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }
}
