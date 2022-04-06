package ca.pfv.spmf.algorithms.sequentialpatterns.clofast.model;

import java.util.ArrayList;
import java.util.List;

/*
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* 
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
*/

/**
 * This is a vertical ID list as used by the Fast and CloFast algorithms
 * @author fabiofumarola on 15/11/14.
 * @see AlgoFast
 * @see ALgoCloFast
 */
public class VerticalIdList {

    private ListNode[] elements;

    public VerticalIdList(ListNode[] elements, int absoluteSupport){
        this.elements = elements;
    }

    public ListNode[] getElements() {
        return elements;
    }

    private List<String> getCoveredSequnceIds() {
        ArrayList<String> seqIds = new ArrayList<String>();

        for (int i = 0; i < elements.length; i++) {
            ListNode element = elements[i];
            if (element != null) {
                seqIds.add(Integer.toString(i));
            }
        }

        return seqIds;
    }

    @Override
    public String toString() {
        return " #SID: " + String.join(" ", getCoveredSequnceIds());
    }
}
