package com.titankv.cluster;

import com.titankv.cluster.HistoryChecker.Kind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The checker itself: it must accept what last-write-wins at QUORUM allows and catch what it
 * forbids. Times are in milliseconds for readability; operations are (start, end).
 */
class HistoryCheckerTest {

    private final HistoryChecker history = new HistoryChecker();

    private void put(String value, long start, long end) {
        history.record(Kind.PUT, "k", value, ms(start), ms(end), true, 1);
    }

    private void failedPut(String value, long start) {
        history.record(Kind.PUT, "k", value, ms(start), ms(start + 1), false, 2);
    }

    private void delete(long start, long end) {
        history.record(Kind.DELETE, "k", null, ms(start), ms(end), true, 3);
    }

    private void read(String value, long start, long end) {
        history.record(Kind.READ, "k", value, ms(start), ms(end), true, 4);
    }

    private static long ms(long millis) {
        return millis * 1_000_000;
    }

    @Test
    void readsOfTheLatestAcknowledgedWriteAreAccepted() {
        read(null, 0, 1);
        put("a", 10, 20);
        read("a", 30, 40);
        put("b", 50, 60);
        read("b", 70, 80);
        delete(90, 100);
        read(null, 110, 120);
        put("c", 130, 140);
        read("c", 150, 160);

        assertThat(history.check()).isEmpty();
    }

    @Test
    void staleReadIsCaught() {
        put("a", 10, 20);
        put("b", 30, 40);
        read("a", 50, 60);

        assertThat(history.check()).singleElement().asString()
                .contains("read 'a'").contains("the write of 'b'");
    }

    @Test
    void readThatOverlapsTheNewerWriteMaySeeEitherValue() {
        put("a", 10, 20);
        put("b", 30, 60);
        read("a", 40, 50); // b had not been acknowledged when this read started
        read("b", 45, 55);
        read("a", 59, 70); // started before b was acknowledged

        assertThat(history.check()).isEmpty();
    }

    @Test
    void overlappingWritesMayEndInEitherOrder() {
        put("a", 10, 30);
        put("b", 20, 40);
        read("a", 50, 60);
        read("b", 70, 80); // weaker than linearizability: this is not reported

        assertThat(history.check()).isEmpty();
    }

    @Test
    void writeWithUnknownOutcomeMayAppearAtAnyLaterTime() {
        put("a", 10, 20);
        failedPut("lost-or-not", 30);
        put("b", 40, 50);
        read("b", 60, 70);
        read("lost-or-not", 80, 90); // it took effect late, after b
        read("lost-or-not", 100, 110);

        assertThat(history.check()).isEmpty();
    }

    @Test
    void valueNobodyWroteIsCaught() {
        put("a", 10, 20);
        read("phantom", 30, 40);

        assertThat(history.check()).singleElement().asString().contains("nobody wrote");
    }

    @Test
    void valueReadBeforeItsWriteStartedIsCaught() {
        read("a", 0, 5);
        put("a", 10, 20);

        assertThat(history.check()).singleElement().asString().contains("before the write of it started");
    }

    @Test
    void deletedKeyThatComesBackIsCaught() {
        put("a", 10, 20);
        delete(30, 40);
        read("a", 50, 60);

        assertThat(history.check()).singleElement().asString().contains("read 'a'").contains("a delete");
    }

    @Test
    void missingKeyAfterAnAcknowledgedPutIsCaught() {
        delete(1, 5);
        put("a", 10, 20);
        read(null, 30, 40);

        assertThat(history.check()).singleElement().asString().contains("found nothing").contains("'a'");
    }

    @Test
    void missingKeyIsExplainedByADeleteThatOverlapsOrFollowsThePut() {
        put("a", 10, 30);
        delete(20, 40);
        read(null, 50, 60);
        put("b", 70, 80);
        history.record(Kind.DELETE, "k", null, ms(90), ms(91), false, 3); // outcome unknown
        read("b", 100, 110);
        read(null, 120, 130); // the delete took effect late

        assertThat(history.check()).isEmpty();
    }

    @Test
    void keysAreCheckedSeparately() {
        history.record(Kind.PUT, "x", "x1", ms(10), ms(20), true, 1);
        history.record(Kind.PUT, "y", "y1", ms(30), ms(40), true, 1);
        history.record(Kind.READ, "x", "x1", ms(50), ms(60), true, 2);
        history.record(Kind.READ, "y", null, ms(50), ms(60), true, 2);

        assertThat(history.check()).singleElement().asString().startsWith("y:");
    }
}
