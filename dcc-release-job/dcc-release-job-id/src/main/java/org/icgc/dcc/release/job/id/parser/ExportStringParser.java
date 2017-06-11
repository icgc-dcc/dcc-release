package org.icgc.dcc.release.job.id.parser;

import org.apache.commons.lang3.tuple.Pair;
import org.icgc.dcc.common.core.util.Separators;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by gguo on 6/8/17.
 */
public class ExportStringParser<IDTYPE> {

    public Map<IDTYPE, String> parse(String str, GenerateIDTypeInstance<IDTYPE> generator){
        return
            Separators.getCorrespondingSplitter(Separators.NEWLINE).splitToList(str).stream().map(
                    line -> generator.generate(Separators.getCorrespondingSplitter(Separators.TAB).splitToList(line))
            ).collect(Collectors.toMap(
                    pair -> pair.getKey(), pair -> pair.getValue()
            ));
    }

    public interface GenerateIDTypeInstance<IDTYPE>{
        Pair<IDTYPE, String> generate(List<String> fields);
    }
}
