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
package org.icgc.dcc.etl2.job.export.core;

import static java.util.stream.Collectors.toList;

import java.util.Collection;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.export.model.ExportTable;
import org.icgc.dcc.etl2.job.export.task.ExportTableTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * See https://github.com/unicredit/hbase-rdd/blob/master/src/main/scala/unicredit/spark/hbase/HFileSupport.scala
 */
@Slf4j
@Component
class ExportJob implements Job {

  /**
   * Dependencies.
   */
  private final Configuration conf;

  @Autowired
  public ExportJob(@NonNull @Qualifier("hbaseConf") Configuration conf) {
    this.conf = conf;
  }

  @Override
  public JobType getType() {
    return JobType.EXPORT;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    val exportTasks = createExportTasks();
    jobContext.execute(exportTasks);
  }

  private Collection<ExportTableTask> createExportTasks() {
    val tables = ExportTable.stream();

    return tables.map(table -> {
      log.info("Adding task to export to table '{}'...", table);
      return new ExportTableTask(table, conf);
    }).collect(toList());
  }

}
