/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.resolver;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.util.FileMutex;

/**
 * A {@link ResourceResolver} implementation that is protected by a {@link FileMutext}.
 * 
 * @param <T>
 */
@Slf4j
public abstract class FileMutexResolver<T> implements ResourceResolver<T> {

  private final static File LOCK_DIR = new File("/tmp");

  @Override
  public T resolve() {
    val result = new AtomicReference<T>();
    val lockFile = getLockFile();
    log.info("Resolving using lock file '{}'...", lockFile);

    lock(result, lockFile);

    val value = result.get();
    log.info("Resolved '{}'", value);

    return value;
  }

  @SneakyThrows
  private void lock(final AtomicReference<T> result, final File lockFile) {
    new FileMutex(lockFile) {

      @Override
      public void withLock() {
        log.info("Acquired lock '{}'", lockFile);
        T value = get();

        result.set(value);
      }

    };
  }

  protected File getLockFile() {
    return getLockFile(this.getClass());
  }

  protected File getLockFile(Class<?> type) {
    val lockFileName = type.getName() + ".v2.lock";
    val lockFile = new File(LOCK_DIR, lockFileName);

    return lockFile;
  }

  /**
   * Template method that actually resolves the resource
   */
  abstract protected T get();

}
