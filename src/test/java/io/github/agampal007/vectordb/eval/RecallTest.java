package io.github.agampal007.vectordb.eval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.agampal007.vectordb.SearchResult;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecallTest {

    private static List<SearchResult> ids(long... ids) {
        return Arrays.stream(ids).mapToObj(id -> new SearchResult(id, 0f)).toList();
    }

    @Test
    void perfectAndZeroRecall() {
        assertEquals(1.0, Recall.atK(ids(1, 2, 3), ids(3, 2, 1), 3));
        assertEquals(0.0, Recall.atK(ids(1, 2, 3), ids(4, 5, 6), 3));
    }

    @Test
    void countsOnlyTheFirstKOfEachList() {
        // Truth top-2 is {1, 2}; the approximate top-2 is {2, 9}, so one hit out of two.
        assertEquals(0.5, Recall.atK(ids(1, 2, 3), ids(2, 9, 1), 2));
    }

    @Test
    void shortApproximateListLosesRecall() {
        assertEquals(1.0 / 3, Recall.atK(ids(1, 2, 3), ids(1), 3), 1e-12);
    }

    @Test
    void smallIndexCapsTheDenominator() {
        assertEquals(1.0, Recall.atK(ids(1, 2), ids(2, 1), 10));
    }

    @Test
    void meanAveragesQueries() {
        double mean = Recall.meanAtK(List.of(ids(1, 2), ids(3, 4)), List.of(ids(1, 2), ids(3, 9)), 2);
        assertEquals(0.75, mean);
    }

    @Test
    void rejectsBadInput() {
        assertThrows(IllegalArgumentException.class, () -> Recall.atK(ids(1), ids(1), 0));
        assertThrows(IllegalArgumentException.class, () -> Recall.meanAtK(List.of(ids(1)), List.of(), 1));
        assertThrows(IllegalArgumentException.class, () -> Recall.meanAtK(List.of(), List.of(), 1));
    }
}
