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
package org.icgc.dcc.etl2.job.annotate.model;

import java.util.List;

import lombok.NonNull;
import lombok.Value;
import lombok.val;

import com.google.common.collect.Lists;

/**
 * This class is populated when a consequence annotation produced by the snpEff tool has warning, error or is
 * incorrectly formatted.
 */
@Value
public class ParseState {

  List<ParseNotification> errorCodes;
  List<String> errorMessages;

  public ParseState() {
    this.errorMessages = Lists.newArrayList();
    this.errorCodes = Lists.newArrayList();
  }

  public ParseState(@NonNull ParseNotification errorCode, @NonNull String errorMessage) {
    this.errorCodes = Lists.newArrayList(errorCode);
    this.errorMessages = Lists.newArrayList(errorMessage);
  }

  public ParseState(@NonNull ParseNotification errorCode, @NonNull String errorMessageTemplate, @NonNull Object... args) {
    this.errorCodes = Lists.newArrayList(errorCode);
    this.errorMessages = Lists.newArrayList(String.format(errorMessageTemplate, args));
  }

  public ParseState(@NonNull List<ParseNotification> errorCodes, @NonNull List<String> errorMessages) {
    this.errorCodes = errorCodes;
    this.errorMessages = errorMessages;
  }

  public void addErrorCode(@NonNull ParseNotification newCode) {
    errorCodes.add(newCode);
  }

  public void addAllErrors(@NonNull List<ParseNotification> newCode) {
    errorCodes.addAll(newCode);
  }

  public void addErrorMessage(@NonNull String newMessage) {
    errorMessages.add(newMessage);
  }

  public void addAllMessages(@NonNull List<String> newMessage) {
    errorMessages.addAll(newMessage);
  }

  public void addErrorAndMessage(@NonNull ParseNotification error, @NonNull String message) {
    errorCodes.add(error);
    errorMessages.add(message);
  }

  public void addErrorAndMessage(@NonNull ParseNotification errorCode, @NonNull String errorMessageTemplate,
      @NonNull Object... args) {
    errorCodes.add(errorCode);
    errorMessages.add(String.format(errorMessageTemplate, args));
  }

  public boolean hasError() {
    return !errorCodes.isEmpty();
  }

  public boolean containsAnyError(@NonNull ParseNotification... errors) {
    for (val error : errors) {
      if (errorCodes.contains(error)) {
        return true;
      }
    }

    return false;
  }

}