package org.geirove;

import org.junit.Test;
import org.rocksdb.*;

import com.tidesdb.*;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Comparison test for RocksDB vs TidesDB column family create/drop operations.
 */
public class ComparisonTest {

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final int TIMES = 200;

    private RocksDB openRocksDB(String path) throws RocksDBException {
        System.out.println("Opening RocksDB at " + path);
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
        return RocksDB.open(dbOptions, path, cfNames, cfHandles);
    }

    @Test
    public void testRocksDBColumnFamiliesLifecycle() throws Exception {
        System.out.println("\n========== RocksDB Column Families Lifecycle ==========");
        Path directory = Files.createTempDirectory("rocksdb_test");

        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();

        byte[] keyBytes = "a".getBytes(UTF_8);
        byte[] valueBytes = "b".getBytes(UTF_8);

        long startTime = System.currentTimeMillis();

        RocksDB db = openRocksDB(directory.toString());
        try {
            Map<String, ColumnFamilyHandle> cfHandles = new HashMap<>();

            long createStart = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                String cfName = String.format("%d", i);
                ColumnFamilyHandle cfh = db.createColumnFamily(new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOpts));
                cfHandles.put(cfName, cfh);
                db.put(cfh, keyBytes, valueBytes);
            }
            long createEnd = System.currentTimeMillis();
            System.out.println("RocksDB - Create " + TIMES + " column families + write: " + (createEnd - createStart) + " ms");

            long dropStart = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                String cfName = String.format("%d", i);
                ColumnFamilyHandle cfh = cfHandles.get(cfName);
                byte[] foundBytes = db.get(cfh, keyBytes);
                assert Arrays.equals(valueBytes, foundBytes);

                if (cfh != null) {
                    db.dropColumnFamily(cfh);
                }
            }
            long dropEnd = System.currentTimeMillis();
            System.out.println("RocksDB - Read + drop " + TIMES + " column families: " + (dropEnd - dropStart) + " ms");

        } finally {
            db.close();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("RocksDB - Total time: " + (endTime - startTime) + " ms");
    }

    @Test
    public void testTidesDBColumnFamiliesLifecycle() throws Exception {
        System.out.println("\n========== TidesDB Column Families Lifecycle ==========");
        Path directory = Files.createTempDirectory("tidesdb_test");

        byte[] keyBytes = "a".getBytes(UTF_8);
        byte[] valueBytes = "b".getBytes(UTF_8);

        long startTime = System.currentTimeMillis();

        Config config = Config.builder(directory.toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            System.out.println("Opening TidesDB at " + directory);

            Map<String, ColumnFamily> cfHandles = new HashMap<>();

            long createStart = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                String cfName = String.format("%d", i);
                ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
                db.createColumnFamily(cfName, cfConfig);
                ColumnFamily cf = db.getColumnFamily(cfName);
                cfHandles.put(cfName, cf);

                try (com.tidesdb.Transaction txn = db.beginTransaction()) {
                    txn.put(cf, keyBytes, valueBytes);
                    txn.commit();
                }
            }
            long createEnd = System.currentTimeMillis();
            System.out.println("TidesDB - Create " + TIMES + " column families + write: " + (createEnd - createStart) + " ms");

            long dropStart = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                String cfName = String.format("%d", i);
                ColumnFamily cf = cfHandles.get(cfName);

                try (com.tidesdb.Transaction txn = db.beginTransaction()) {
                    byte[] foundBytes = txn.get(cf, keyBytes);
                    assert Arrays.equals(valueBytes, foundBytes);
                }

                db.dropColumnFamily(cfName);
            }
            long dropEnd = System.currentTimeMillis();
            System.out.println("TidesDB - Read + drop " + TIMES + " column families: " + (dropEnd - dropStart) + " ms");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("TidesDB - Total time: " + (endTime - startTime) + " ms");
    }

    @Test
    public void testCompareRocksDBvsTidesDB() throws Exception {
        System.out.println("\n========== COMPARISON: RocksDB vs TidesDB ==========");
        System.out.println("Operations: Create " + TIMES + " column families, write 1 key each, read, then drop all");
        System.out.println();

        testRocksDBColumnFamiliesLifecycle();

        System.out.println();

        testTidesDBColumnFamiliesLifecycle();

        System.out.println("\n========== COMPARISON COMPLETE ==========");
    }
}
