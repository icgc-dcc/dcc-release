/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.release.job.imports.core;

import static java.lang.String.format;

import java.io.File;

import org.icgc.dcc.common.test.mongodb.EmbeddedMongo;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.imports.config.MongoProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import lombok.SneakyThrows;
import lombok.val;

public class ImportJobTest extends AbstractJobTest {

  @Rule
  public final static EmbeddedMongo embeddedMongo = new EmbeddedMongo();
  public final static String TEST_DB = "dcc-import";
  public final static String[] Collections = { "Project", "Gene", "GeneSet", "Drug", "Diagram" };
  public final static String TEST_DATA_PATH = "test_data";
  /**
   * Class under test.
   */
  ImportJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();

    insertTestData(getDB());

    val properties = new MongoProperties();
    properties.setUri(getUri());

    this.job = new ImportJob(properties);
  }

  private String getUri() {
    val address = embeddedMongo.getMongo().getAddress();
    return format("mongodb://%s:%d/%s", address.getHost(), address.getPort(), TEST_DB);
  }

  private DB getDB() {
    return embeddedMongo.getMongo().getDB(TEST_DB);
  }

  private void insertTestData(DB db) {
    val status = db.command("dropDatabase");
    System.out.printf("Drop database %s returned %s\n", TEST_DB, status.toString());
    for (val collection : Collections) {
      System.out.printf("Adding test data for collection %s\n", collection);
      val json = getJSON(collection);
      populateCollection(db, collection, json);
    }
  }

  private void populateCollection(DB db, String collection, String jsonDocument) {
    val c = db.getCollection(collection);
    val bson = (DBObject) JSON.parse(jsonDocument);

    val status = c.insert(bson);
    System.out.printf("Insert for collection %s returned %s\n", collection, status);
  }

  @SneakyThrows
  private String getJSON(String collection) {
    val f = new File(TEST_DATA_PATH, collection + ".json");
    return Files.toString(f, Charsets.UTF_8);
  }

  @Test
  public void testExecute() {
    job.execute(createJobContext(job.getType()));

    val results = producesFile(FileType.GENE);
    for (val gene : results) {
      System.out.println(gene);
    }
  }

}
