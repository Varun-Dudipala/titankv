package com.titankv.cluster;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A node's saved view of the cluster: its other members and the nodes that were removed. Kept on
 * disk so a restarted node knows the whole ring before it hears from anyone, instead of trusting
 * whichever peer answers first.
 *
 * Format: [magic:4][version:2][member count:4] then per member [id][host][port:4][generation:8],
 * [removed count:4] then per removed node [id][generation:8], and a CRC32 of everything before it.
 * Strings are as {@link DataOutputStream#writeUTF} writes them. The file is replaced atomically.
 */
final class MembershipFile {

    private static final int MAGIC = 0x544B4D42; // "TKMB"
    private static final short VERSION = 1;
    private static final int MAX_ENTRIES = 100_000;

    record Member(String id, String host, int port, long generation) {
    }

    record Contents(List<Member> members, Map<String, Long> removed) {
    }

    private MembershipFile() {
    }

    /**
     * @return the saved membership, or null if there is no file
     * @throws IOException if the file exists but cannot be read or fails its checksum
     */
    static Contents load(Path file) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < 4 + 2 + 4 + 4 + 4) {
            throw new IOException("Membership file " + file + " is truncated");
        }
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - 4);
        if ((int) crc.getValue() != ByteBuffer.wrap(bytes, bytes.length - 4, 4).getInt()) {
            throw new IOException("Membership file " + file + " fails its checksum");
        }
        try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes, 0, bytes.length - 4))) {
            if (in.readInt() != MAGIC || in.readShort() != VERSION) {
                throw new IOException("Membership file " + file + " has an unknown format");
            }
            int memberCount = checkedCount(in.readInt(), file);
            List<Member> members = new ArrayList<>(memberCount);
            for (int i = 0; i < memberCount; i++) {
                members.add(new Member(in.readUTF(), in.readUTF(), in.readInt(), in.readLong()));
            }
            int removedCount = checkedCount(in.readInt(), file);
            Map<String, Long> removed = new LinkedHashMap<>();
            for (int i = 0; i < removedCount; i++) {
                removed.put(in.readUTF(), in.readLong());
            }
            return new Contents(members, removed);
        }
    }

    private static int checkedCount(int count, Path file) throws IOException {
        if (count < 0 || count > MAX_ENTRIES) {
            throw new IOException("Membership file " + file + " has an invalid entry count " + count);
        }
        return count;
    }

    /**
     * Write the membership to a temporary file, sync it, and move it over the old one.
     */
    static void save(Path file, Contents contents) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeInt(contents.members().size());
            for (Member member : contents.members()) {
                out.writeUTF(member.id());
                out.writeUTF(member.host());
                out.writeInt(member.port());
                out.writeLong(member.generation());
            }
            out.writeInt(contents.removed().size());
            for (Map.Entry<String, Long> removed : contents.removed().entrySet()) {
                out.writeUTF(removed.getKey());
                out.writeLong(removed.getValue());
            }
        }
        byte[] body = bytes.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(body);
        ByteBuffer buffer = ByteBuffer.allocate(body.length + 4).put(body).putInt((int) crc.getValue());
        buffer.flip();

        Files.createDirectories(file.toAbsolutePath().getParent());
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        // The rename is not durable until its directory is
        try (FileChannel directory = FileChannel.open(file.toAbsolutePath().getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        } catch (IOException e) {
            // Some platforms cannot open or sync a directory; nothing more can be done there
        }
    }
}
