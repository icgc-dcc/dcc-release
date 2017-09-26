package org.icgc.dcc.release.core.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static org.icgc.dcc.release.core.model.ConsequenceType.*;

/**
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 * <p>
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 * <p>
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

public class CodingTypes {

  public static final String fieldNameForCoding = "coding";

  private static final Set<String> CODING_TYPES = ImmutableSet.of(
      FRAMESHIFT_VARIANT.getConsequenceName(),
      MISSENSE_VARIANT.getConsequenceName(),
      INITIATOR_CODON_VARIANT.getConsequenceName(),
      STOP_GAINED.getConsequenceName(),
      STOP_LOST.getConsequenceName(),
      RARE_AMINO_ACID_VARIANT.getConsequenceName(),
      CODING_SEQUENCE_VARIANT.getConsequenceName(),
      NON_CANONICAL_START_CODON.getConsequenceName(),
      DISRUPTIVE_INFRAME_DELETION.getConsequenceName(),
      INFRAME_DELETION.getConsequenceName(),
      DISRUPTIVE_INFRAME_INSERTION.getConsequenceName(),
      INFRAME_INSERTION.getConsequenceName(),
      SYNONYMOUS_VARIANT.getConsequenceName(),
      STOP_RETAINED_VARIANT.getConsequenceName()
  );

  public static boolean isCoding(String consequenceType) {
    return CODING_TYPES.contains(consequenceType);
  }
}
