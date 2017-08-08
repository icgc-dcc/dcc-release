package org.icgc.dcc.release.job.id.parser;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.icgc.dcc.common.core.util.Splitters.NEWLINE;
import static org.icgc.dcc.common.core.util.Splitters.TAB;

/**
 Copyright (c) $today.year The Ontario Institute for Cancer Research. All rights reserved.

 This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 You should have received a copy of the GNU General Public License along with
 this program. If not, see <http://www.gnu.org/licenses/>.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
public class ExportStringParser<IDTYPE> {

    public Map<IDTYPE, String> parse(String str, GenerateIDTypeInstance<IDTYPE> generator){
        return
                NEWLINE.trimResults().omitEmptyStrings().splitToList(str).stream()
                        .map(TAB::splitToList)
                        .map(generator::generate)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public interface GenerateIDTypeInstance<IDTYPE>{
        Entry<IDTYPE, String> generate(List<String> fields);
    }
}
