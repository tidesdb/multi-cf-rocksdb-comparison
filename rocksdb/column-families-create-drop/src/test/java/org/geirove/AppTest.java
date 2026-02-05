package org.geirove;

import org.junit.Test;
import org.rocksdb.*;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class AppTest {

    private static final Charset UTF_8 = Charset.forName("UTF-8");


    private RocksDB open(String path) throws RocksDBException {
        System.out.println("Opening " + path);
        final List<ColumnFamilyDescriptor> cfNames = new ArrayList<>();
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();

        if (Files.exists(Paths.get(path))) {
            Options options = new Options();
            List<byte[]> columnFamilies = RocksDB.listColumnFamilies(options, path);
            if (columnFamilies != null) {
                for (byte[] cfName : columnFamilies) {
                    if (!cfNames.contains(cfName)) {
                        cfNames.add(new ColumnFamilyDescriptor(cfName, cfOpts));
                    }
                }
            }
        }

        if (cfNames.isEmpty()) {
            cfNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
        }

        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(dbOptions, path, cfNames, cfHandles);

        Map<String, ColumnFamilyHandle> handles = new ConcurrentHashMap<>();
        for (int i = 0; i < cfNames.size(); i++) {
            handles.put(new String(cfNames.get(i).columnFamilyName(), UTF_8), cfHandles.get(i));
        }
        return db;
    }

    @Test
    public void testColumnFamiliesLifecycle() throws Exception {
        Path directory = Files.createTempDirectory("x");
        int times = 200;

        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();

        byte[] keyBytes = "a".getBytes(UTF_8);
        byte[] valueBytes = "b".getBytes(UTF_8);

        RocksDB db = open(directory.toString());
        try {
            Map<String, ColumnFamilyHandle> cfHandles = new HashMap<>();
            for (int i = 0; i < times; i++) {
                String cfName = String.format("%d", i);
                ColumnFamilyHandle cfh = db.createColumnFamily(new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOpts));
                cfHandles.put(cfName, cfh);

                db.put(cfh, keyBytes, valueBytes);
                // db.dropColumnFamily(cfh);  // dropping it here is fast
            }
            for (int i = 0; i < times; i++) {
                String cfName = String.format("%d", i);
                ColumnFamilyHandle cfh = cfHandles.get(cfName);
                byte[] foundBytes = db.get(cfh, keyBytes);
                assert Arrays.equals(valueBytes, foundBytes);

                if (cfh != null) {
                    db.dropColumnFamily(cfh);  // dropping it here is slow
                }
            }
        } finally {
            db.close();
        }
    }
}
