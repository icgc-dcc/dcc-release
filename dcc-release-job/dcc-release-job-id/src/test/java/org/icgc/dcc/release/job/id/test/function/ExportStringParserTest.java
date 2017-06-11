package org.icgc.dcc.release.job.id.test.function;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;
import org.icgc.dcc.release.job.id.parser.ExportStringParser;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;

/**
 * Created by gguo on 6/11/17.
 */
public class ExportStringParserTest {

    @Test
    public void testParse() {
        String str =
            Joiner.on('\n').join(
                Joiner.on('\t').join("a", "b", "c"),
                Joiner.on('\t').join("d", "e", "f")
            );
        ExportStringParser<Pair<String, String>> parser = new ExportStringParser<>();

        Map<Pair<String,String>, String> map = parser.parse(str, fields-> Pair.of(Pair.of(fields.get(1), fields.get(2)), fields.get(0)));

        String key1 = map.get(Pair.of("b", "c"));

        assertNotNull(key1);
        assertEquals(key1, "a");

        key1 = map.get(Pair.of("e", "f"));

        assertNotNull(key1);
        assertEquals(key1, "d");

    }
}
