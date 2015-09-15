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

import static com.google.common.base.Preconditions.checkState;

import java.io.File;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.util.FileMutex;

/**
 * Utility class that ensures the resource base dir is created before delegating to implementations.
 * 
 * @param <T> the resource type
 */
@Slf4j
@Getter
public abstract class DirectoryResourceResolver<T> extends FileMutexResolver<T> {

  /**
   * The base directory for the local resources.
   */
  private final File resourceDir;

  public DirectoryResourceResolver(final @NonNull File resourceDir) {
    this.resourceDir = resourceDir;

    ensureResourceDir(resourceDir);
  }

  @SneakyThrows
  private void ensureResourceDir(final File resourceDir) {
    new FileMutex(getLockFile(DirectoryResourceResolver.class)) {

      @Override
      public void withLock() {
        if (!resourceDir.exists()) {
          log.info("Creating resource dir '{}'...", resourceDir);
          checkState(resourceDir.mkdirs(), "Could not make base dir '%s'", resourceDir);
          log.info("Finished resource base dir");
        }
      }

    };
  }

}
