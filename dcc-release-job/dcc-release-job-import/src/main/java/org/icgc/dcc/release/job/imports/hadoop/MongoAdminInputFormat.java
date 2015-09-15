/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.imports.hadoop;

import static com.google.common.base.Preconditions.checkState;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * {@code MongoInputFormat} extension that first authenticates against the configured mongo URI's {@code admin} db to
 * permit commands such as {@code splitVector} to succeed.
 */
public class MongoAdminInputFormat extends MongoInputFormat {

  private static final String MONGO_ADMIN_DATABASE_NAME = "admin";

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) {
    // Authenticate with mongo-hadoop admin against the collection uri. This is the purpose of this class
    authenticateAdmin(collectionUri(job));

    // Delegate everything else to the parent
    return super.getSplits(job, numSplits);
  }

  /**
   * Method to workaround {@code mongo-hadoop}'s lack of authentication support for the {@code splitVector} command
   * which first requires the client to authenticate against the {@code admin} db. This may be related to the referenced
   * links below.
   * 
   * @param collectionUri a Mongo collection URI to any collection on the target database
   * @see https://jira.mongodb.org/browse/HADOOP-43
   * @see https://groups.google.com/forum/?fromgroups=#!topic/mongodb-user/aTh_o0u-_B0
   */
  private static void authenticateAdmin(String collectionUri) {
    MongoURI mongoUri = mongoUri(collectionUri);
    if (mongoUri.getCredentials() == null) {
      // No need for the workaround
      return;
    }

    // Need to use this method which caches an instance of Mongo that is later
    // used for 'splitVector'ing in MongoSplitter
    Mongo hadoopMongo = getHadoopMongo(mongoUri);

    // Ensure that the downstream 'splitVector' command will succeed by
    // first authenticating against the admin db using the shared 'db' connection
    // object.
    authenticateAdmin(mongoUri, hadoopMongo);
  }

  private static void authenticateAdmin(MongoURI mongoUri, Mongo hadoopMongo) {
    DB admin = hadoopMongo.getDB(MONGO_ADMIN_DATABASE_NAME);
    boolean authenticated = admin.authenticate(mongoUri.getUsername(), mongoUri.getPassword());

    checkState(authenticated, "Could not authenticate user '%s' to database '%s' using MongoURI '%s'", //
        mongoUri.getUsername(), MONGO_ADMIN_DATABASE_NAME, mongoUri);
  }

  private static Mongo getHadoopMongo(MongoURI mongoUri) {
    DBCollection collection = MongoConfigUtil.getCollection(mongoUri);
    return collection.getDB().getMongo();
  }

  private static String collectionUri(JobConf job) {
    MongoConfig mongoConfig = new MongoConfig(job);
    return mongoConfig.getInputURI().toString();
  }

  @SuppressWarnings("deprecation")
  private static MongoURI mongoUri(String collectionUri) {
    // Need to use the deprecated version until mongo-hadoop upgrades
    return new MongoURI(collectionUri);
  }

}
