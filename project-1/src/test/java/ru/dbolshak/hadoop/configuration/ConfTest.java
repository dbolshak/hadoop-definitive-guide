package ru.dbolshak.hadoop.configuration;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Created by dbolshak on 12/03/15.
 */
public class ConfTest {
    private final Configuration conf = new Configuration();

    @Before
    public void setUp() {
        conf.addResource("configuration-1.xml");

    }

    @Test
    public void readDefaultSettings() {
        assertThat(conf.getInt("size", 0), is(10));
        assertThat(conf.get("breadth", "wide"), is("wide"));
    }

    @Test
    public void overrideDefaultSettings() {
        conf.addResource("configuration-2.xml");
        assertThat(conf.getInt("size", 0), is(12));
        assertThat(conf.get("breadth", "wide"), is("wide"));
    }

    @Test
    public void finalPropertiesCannotBeOverridden() {
        conf.addResource("configuration-2.xml");
        assertThat(conf.get("weight"), is("heavy"));
    }

    @Test
    public void expandingProperties() {
        conf.addResource("configuration-2.xml");
        assertThat(conf.get("size-weight"), is("12,heavy"));
    }

    @Test
    public void systemPropertiesTakePriority() {
        conf.addResource("configuration-2.xml");
        System.setProperty("size", "14");
        assertThat(conf.get("size-weight"), is("14,heavy"));
        System.clearProperty("size");
    }

    @Test
    public void tryingToOverrideUnexactingProperty() {
        System.setProperty("length", "2");
        assertThat(conf.get("length"), is((String) null));
    }
}
