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
package org.icgc.dcc.etl2.core.util;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class FileMutex {

  /**
   * Constants.
   */
  private static final int MAX_RETRIES = 10;
  private static final int RETRY_DELAY_SECONDS = 60;
  private static final String READ_WRITE_MODE = "rw";

  public FileMutex(@NonNull File lockFile) throws InterruptedException, IOException {
    @Cleanup
    val delegate = new RandomAccessFile(lockFile, READ_WRITE_MODE);
    @Cleanup
    val channel = delegate.getChannel();

    int counter = 0;
    FileLock lock = null;
    try {
      while (lock == null && counter < MAX_RETRIES) {
        try {
          lock = channel.lock();

          // Delegate to derived class
          withLock();

          // Success
          return;
        } catch (OverlappingFileLockException e) {
          // Thrown when an attempt is made to acquire a lock on a a file that overlaps
          // a region already locked by the same JVM or when another thread is already
          // waiting to lock an overlapping region of the same file
          delay(lockFile);

          counter++;
        }
      }

      // Failure
      fail(lockFile);
    } finally {
      if (lock != null) {
        // Release the lock
        lock.release();
      }
    }
  }

  /**
   * Template method.
   */
  abstract public void withLock();

  private void delay(File lockFile) throws InterruptedException {
    log.error(
        "*** Overlapping file lock on '{}' error due to same JVM inter-thread contention. Waiting {}s before retrying...",
        lockFile.getAbsolutePath(), RETRY_DELAY_SECONDS);
    Thread.sleep(SECONDS.toMillis(RETRY_DELAY_SECONDS));
  }

  private void fail(File lockFile) {
    val message = String.format("Failed to acquire lock '%s' after %s retries with %s seconds delay. Aborting...",
        lockFile, MAX_RETRIES, RETRY_DELAY_SECONDS);
    log.error(message);
    throw new IllegalStateException(message);
  }

}