/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.imports.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.net.URI;

import lombok.val;

import com.mongodb.MongoClientURI;

public class MongoClientURIBuilder {

  /**
   * Constants.
   */
  private static final String PREFIX = "mongodb://";

  /**
   * State.
   */
  private String username;
  private String password;
  private String host;
  private Integer port;
  private String database;
  private String collection;

  public MongoClientURIBuilder() {
  }

  public MongoClientURIBuilder(String text) {
    uri(text);
  }

  public MongoClientURIBuilder uri(String text) {
    val uri = URI.create(text);
    host = uri.getHost();
    port = uri.getPort();

    val path = uri.getPath();
    if (path != null) {
      String[] parts = path.split("\\.");
      database = parts[0];

      if (parts.length > 1) {
        collection = parts[1];
      }
    }

    val usernameInfo = uri.getUserInfo();
    if (usernameInfo != null) {
      username = usernameInfo.substring(0, usernameInfo.indexOf(':'));
      password = usernameInfo.substring(usernameInfo.indexOf(':') + 1, usernameInfo.length());
    }

    return this;
  }

  public MongoClientURIBuilder username(String username) {
    this.username = username;
    return this;
  }

  public MongoClientURIBuilder password(String password) {
    this.password = password;
    return this;
  }

  public MongoClientURIBuilder host(String host) {
    this.host = host;
    return this;
  }

  public MongoClientURIBuilder port(Integer port) {
    this.port = port;
    return this;
  }

  public MongoClientURIBuilder port(String port) {
    if (isNullOrEmpty(port)) {

    } else {
      this.port = Integer.parseInt(port);
    }
    return this;
  }

  public MongoClientURIBuilder database(String database) {
    this.database = database;
    return this;
  }

  public MongoClientURIBuilder collection(String collection) {
    this.collection = collection;
    return this;
  }

  public MongoClientURI build() {
    return build(true);
  }

  public MongoClientURI buildWithoutusernameInfo() {
    return build(false);
  }

  private MongoClientURI build(boolean includeusernameInfo) {
    checkNotNull(host);

    val builder = new StringBuilder(PREFIX);
    if (!isNullOrEmpty(username) && !isNullOrEmpty(password) && includeusernameInfo) {
      builder.append(username).append(':').append(password).append('@');
    }

    builder.append(host);

    if (port != null) {
      builder.append(':').append(port);
    }
    if (database != null) {
      builder.append('/').append(database);
    }
    if (collection != null) {
      builder.append('.').append(collection);
    }

    return new MongoClientURI(builder.toString());
  }

}
