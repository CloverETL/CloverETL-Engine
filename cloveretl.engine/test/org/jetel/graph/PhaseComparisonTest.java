/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph;

import org.jetel.test.CloverTestCase;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.3.2017
 */
public class PhaseComparisonTest extends CloverTestCase {

	public void testPhaseComparison() {
		
		assertTrue("Phase 1 is not lower than phase 2", Phase.comparePhaseNumber(1, 2) < 0);
		assertTrue("Phase 2 is not higher than phase 1", Phase.comparePhaseNumber(2, 1) > 0);
		assertTrue("Phase 1 is not equal to phase 1", Phase.comparePhaseNumber(1, 1) == 0);
		assertTrue("Initial phase is not lower than final", Phase.comparePhaseNumber(Phase.INITIAL_PHASE_ID, Phase.FINAL_PHASE_ID) < 0);
		assertTrue("Final phase is not higher than initial phase", Phase.comparePhaseNumber(Phase.FINAL_PHASE_ID, Phase.INITIAL_PHASE_ID) > 0);
		assertTrue("Initial phase is not lower than phase 0", Phase.comparePhaseNumber(Phase.INITIAL_PHASE_ID, 0) < 0);
		assertTrue("Phase 0 is not higher than initial phase", Phase.comparePhaseNumber(0, Phase.INITIAL_PHASE_ID) > 0);
		assertTrue("Final phase is not higher than phase 0", Phase.comparePhaseNumber(Phase.FINAL_PHASE_ID, 0) > 0);
		assertTrue("Phase 0 is not lower than final phase", Phase.comparePhaseNumber(0, Phase.FINAL_PHASE_ID) < 0);
		assertTrue("Initial phase is not equal to initial phase", Phase.comparePhaseNumber(Phase.INITIAL_PHASE_ID, Phase.INITIAL_PHASE_ID) == 0);
	}
}
