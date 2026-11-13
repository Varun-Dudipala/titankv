package com.titankv.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records what many clients did to shared keys, with the time each operation started and ended,
 * and checks the reads against what QUORUM promises for a last-write-wins register:
 *
 * <ul>
 *   <li>a read returns a value some client wrote, and not before that write started;</li>
 *   <li>a read does not return a write (or the absence left by a delete) when another write to the
 *       key was acknowledged after it finished and before the read started.</li>
 * </ul>
 *
 * A write whose outcome its client never learned (it failed or timed out) may take effect at any
 * time after it started, or never; so may a write that overlaps another in time. Both are legal.
 * This is weaker than linearizability: two reads that overlap an unfinished write may see it and
 * then not see it. It is exactly "a QUORUM read sees every QUORUM write acknowledged before it".
 *
 * Every written value must be unique, so a read identifies the write it came from.
 */
final class HistoryChecker {

    enum Kind { PUT, DELETE, READ }

    /**
     * @param value the value written, or the value read (null if the key was not found)
     * @param start {@link System#nanoTime()} before the request was sent
     * @param end   {@link System#nanoTime()} after the reply arrived
     * @param ok    whether the client got an answer; a write without one has an unknown outcome
     */
    record Op(Kind kind, String key, String value, long start, long end, boolean ok, int client) {
        /** When the write is known to have finished: never, if its outcome is unknown. */
        long finished() {
            return ok ? end : Long.MAX_VALUE;
        }
    }

    private final List<Op> ops = Collections.synchronizedList(new ArrayList<>());

    void record(Kind kind, String key, String value, long start, long end, boolean ok, int client) {
        ops.add(new Op(kind, key, value, start, end, ok, client));
    }

    int size() {
        return ops.size();
    }

    long count(Kind kind, boolean ok) {
        synchronized (ops) {
            return ops.stream().filter(op -> op.kind == kind && op.ok == ok).count();
        }
    }

    /**
     * @return a description of every read that breaks the rules above (empty if none)
     */
    List<String> check() {
        Map<String, List<Op>> byKey = new HashMap<>();
        synchronized (ops) {
            for (Op op : ops) {
                byKey.computeIfAbsent(op.key, key -> new ArrayList<>()).add(op);
            }
        }
        List<String> violations = new ArrayList<>();
        for (Map.Entry<String, List<Op>> entry : byKey.entrySet()) {
            checkKey(entry.getKey(), entry.getValue(), violations);
        }
        return violations;
    }

    private static void checkKey(String key, List<Op> history, List<String> violations) {
        Map<String, Op> putByValue = new HashMap<>();
        List<Op> acknowledged = new ArrayList<>();
        List<Op> deletes = new ArrayList<>();
        for (Op op : history) {
            if (op.kind == Kind.PUT) {
                if (putByValue.put(op.value, op) != null) {
                    throw new IllegalArgumentException("Value " + op.value + " was written twice to " + key);
                }
            } else if (op.kind == Kind.DELETE) {
                deletes.add(op);
            }
            if (op.kind != Kind.READ && op.ok) {
                acknowledged.add(op);
            }
        }

        for (Op read : history) {
            if (read.kind != Kind.READ || !read.ok) {
                continue;
            }
            if (read.value != null) {
                Op source = putByValue.get(read.value);
                if (source == null) {
                    violations.add(key + ": client " + read.client + " read '" + read.value + "', which nobody wrote");
                } else if (source.start > read.end) {
                    violations.add(key + ": client " + read.client + " read '" + read.value
                            + "' before the write of it started");
                } else {
                    Op newer = acknowledgedBetween(acknowledged, source, source.finished(), read.start, false);
                    if (newer != null) {
                        violations.add(key + ": client " + read.client + " read '" + read.value + "' (client "
                                + source.client + "), but " + describe(newer) + " was acknowledged after that write "
                                + "finished and " + millis(read.start - newer.end) + " ms before this read started");
                    }
                }
            } else {
                // Not found: explained by the key never having been written, or by any delete that
                // no acknowledged put separates from this read
                Op put = acknowledgedBetween(acknowledged, null, Long.MIN_VALUE, read.start, true);
                if (put == null) {
                    continue; // nothing was acknowledged before the read: the initial state explains it
                }
                boolean explained = false;
                for (Op delete : deletes) {
                    if (delete.start < read.end
                            && acknowledgedBetween(acknowledged, delete, delete.finished(), read.start, true) == null) {
                        explained = true;
                        break;
                    }
                }
                if (!explained) {
                    violations.add(key + ": client " + read.client + " found nothing, but " + describe(put)
                            + " was acknowledged " + millis(read.start - put.end) + " ms before this read started "
                            + "and no delete came after it");
                }
            }
        }
    }

    /**
     * @return an acknowledged write other than {@code source} that started after {@code after} and
     *         finished before {@code before}, or null if there is none
     */
    private static Op acknowledgedBetween(List<Op> acknowledged, Op source, long after, long before,
            boolean putsOnly) {
        for (Op write : acknowledged) {
            if (write != source && write.start > after && write.end < before
                    && (!putsOnly || write.kind == Kind.PUT)) {
                return write;
            }
        }
        return null;
    }

    private static String describe(Op write) {
        return write.kind == Kind.PUT ? "the write of '" + write.value + "' by client " + write.client
                : "a delete by client " + write.client;
    }

    private static long millis(long nanos) {
        return nanos / 1_000_000;
    }
}
