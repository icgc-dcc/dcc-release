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
package org.icgc.dcc.etl2.workflow.config;

import org.icgc.dcc.common.core.util.resolver.Resolver.CodeListsResolver;
import org.icgc.dcc.common.core.util.resolver.Resolver.DictionaryResolver;
import org.icgc.dcc.common.core.util.resolver.RestfulCodeListsResolver;
import org.icgc.dcc.common.core.util.resolver.RestfulDictionaryResolver;
import org.icgc.dcc.etl2.core.submission.SubmissionFileSchemas;
import org.icgc.dcc.etl2.core.submission.SubmissionMetadataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Submission system configuration.
 */
@Lazy
@Configuration
public class SubmissionConfig {

  @Value("${dcc.submission.url}")
  String submissionUrl;

  @Bean
  public SubmissionFileSchemas submissionFileSchemas(SubmissionMetadataService submissionMetadataService) {
    return new SubmissionFileSchemas(submissionMetadataService.getMetadata());
  }

  @Bean
  public DictionaryResolver dictionaryResolver() {
    return new RestfulDictionaryResolver(submissionUrl);
  }

  @Bean
  public CodeListsResolver codeListsResolver() {
    return new RestfulCodeListsResolver(submissionUrl);
  }

}
