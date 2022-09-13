package abc.xyz.core;

import abc.xyz.exception.MiniDBExceptionType;
import abc.xyz.exception.MiniDbException;
import abc.xyz.hint.HintEntry;
import abc.xyz.index.IndexEntry;
import abc.xyz.index.IndexManager;
import abc.xyz.util.ByteUtil;
import abc.xyz.util.Crc32Util;
import abc.xyz.util.FileReadWriteUtil;
import abc.xyz.util.SerializeUtil;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class MiniDB implements Storage {

    private static final Logger log = LoggerFactory.getLogger(MiniDB.class);

    private static final String META_FILE_NAME = "meta";
    private static final String LOCK_FILE_NAME = "lock";
    private static final String DATA_DIRECTORY_NAME = "data";
    private static final String HINT_DIRECTORY_NAME = "hint";
    private static final int    SINGLE_DATA_FILE_MAX_SIZE = 1024 * 1024 * 10; //单个数据文件的最大大小

    private final double compactThreshold = 0.50;
    private final long   compactMinWriteCountThreshold = 2 * 10000;

    private final String name;
    private final String storagePath;
    private final String metaFileName;
    private final String lockFileName;
    private final String dataFilePath;
    private final String hintFilePath;

    private final Meta meta = Meta.INITIAL;
    private final IndexManager index = new IndexManager();
    private final ScheduledExecutorService compactService = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean reloadTaskIsDone = false;

    private volatile long writeOffSet = 0; //记录active数据文件的写位移
    private volatile int active;
    private final List<String> archiveNameList = new LinkedList<>();
    private final AtomicLong writtenCount = new AtomicLong(0); //记录kv对的总写入数量

    private MiniDB(String dbName, String path){
        this.name = dbName;
        this.storagePath  = path + File.separator + dbName;
        this.metaFileName = this.storagePath + File.separator + META_FILE_NAME;
        this.lockFileName = this.storagePath + File.separator + LOCK_FILE_NAME;
        this.dataFilePath = this.storagePath + File.separator + DATA_DIRECTORY_NAME;
        this.hintFilePath = this.storagePath + File.separator + HINT_DIRECTORY_NAME;
    }

    public static MiniDB open(String dbName, String path) throws MiniDbException {
        MiniDB db = new MiniDB(dbName, path);
        db.checkPath(); //路径检查
        db.reload(); //加载数据
        db.setupCompactTask(); //文件压缩定时任务
        return db;
    }

    @Override
    public boolean put(String key, Object value) throws MiniDbException {
        long timestamp = System.currentTimeMillis();
        EncodeResult encodeResult = encode(key, value, timestamp);
        boolean res = false;
        synchronized (this){
            AppendResult result = append(encodeResult.bytes, false);//1.追加数据到数据文件尾部
            if(result != AppendResult.FAILED){
                this.index.put(key, IndexEntry.builder()
                        .key(key)
                        .fileName(result.fileName)
                        .offset(result.offset)
                        .vSize(encodeResult.vSize)
                        .kSize(encodeResult.kSize)
                        .timeStamp(timestamp)
                        .build()); //2.更新索引
                res = true;
            }
        }
        return res;
    }

    @Override
    public Object get(String key) throws MiniDbException {
        if (!this.index.containsKey(key)){
            return null;
        }
        return get(this.index.get(key));
    }

    private Object get(IndexEntry ie) throws MiniDbException {
        byte[] bytes = FileReadWriteUtil.readBytes(ie.getFileName(), ie.getOffset(),
                ie.getVSize() + ie.getKSize() + 24); //24 bytes: crc-timestamp-kSize-vSize
        return decode(bytes, ie.getVSize());
    }

    @Override
    public boolean delete(String key) throws MiniDbException {
        if (this.exist(key)){
            long timestamp = System.currentTimeMillis();
            EncodeResult encodeResult = encode(key, null, timestamp); //value `null` 特殊标记 key 被删除
            AppendResult result = append(encodeResult.bytes, false);
            if(result != AppendResult.FAILED){
                this.index.remove(key);
                return true;
            } else return false;
        }
        return true;
    }

    @Override
    public boolean exist(String key) {
        return this.index.containsKey(key);
    }

    public int getSize(){
        return this.index.getSize();
    }

    public String getName(){
        return this.name;
    }


    private void setupCompactTask(){
        this.compactService.scheduleAtFixedRate(() -> {
            if (!this.reloadTaskIsDone){
                log.info("[compact] reloading task is running now, we need to wait a moment.");
                return;
            }
            if (this.writtenCount.get() < this.compactMinWriteCountThreshold){
                log.info("[compact] not reach to min written count threshold, {} of {}.",
                        this.writtenCount.get(), this.compactMinWriteCountThreshold);
                return;
            }
            double ratio = (this.getSize() * 1.0) / this.writtenCount.get();
            if(ratio >= this.compactThreshold){
                log.info("[compact] no need to compact in this round, ratio:{}, total size:{}, " +
                                "total written count:{}.",
                        ratio, this.index.getSize(), this.writtenCount.get());
                return;
            }

            try {
                log.info("[compact] do compaction, ratio:{}, total size:{}, total written count:{}.",
                        ratio, this.index.getSize(), this.writtenCount.get());
                compact();
            } catch (MiniDbException e) {
                log.error("[compact] current compaction is failed, caused by => {}", e.toString());
                throw new RuntimeException(e);
            }
        }, 30, 15, TimeUnit.SECONDS); //启动 30 秒后开始，每15 秒一次
    }

    public void close() throws MiniDbException {
        File lockFile = new File(this.lockFileName);
        if (lockFile.exists()){
            if(!lockFile.delete()){
                throw new MiniDbException(MiniDBExceptionType.LOCK_FILE_DELETE_ERR);
            }
        }
    }

    private synchronized void reload() throws MiniDbException {
        File dataFile = new File(this.dataFilePath);
        File hintFile = new File(this.hintFilePath);

        // load meta info and set writeOffset
        loadMeta();
        this.active = this.meta.getActiveFileName();
        File activeFile = new File(this.dataFilePath + File.separator + this.active);
        this.writeOffSet = activeFile.length();

        //清理无效的旧数据文件
        cleanOldDataFile();
        cleanOldHintFile();

        //load
        String[] dataFileNames = dataFile.list((dir, name) -> Integer.parseInt(name) > meta.getLastCompactFileName());
        if(dataFileNames == null || dataFileNames.length == 0) {
            this.reloadTaskIsDone = true;
            return;
        }

        //加载 hint 文件目录
        String[] hintFileNames = hintFile.list((dir, name) -> Integer.parseInt(name) > meta.getLastCompactFileName());
        Set<String> hintFileSet = new HashSet<>();
        if (hintFileNames != null){
            hintFileSet.addAll(Arrays.asList(hintFileNames));
        }

        Arrays.sort(dataFileNames, Comparator.comparingInt(Integer::parseInt));

        for(int i = 0; i < dataFileNames.length; i++){
            //判断是否有 hint 文件， 如果有从 hint 文件中恢复；没有则从 data 文件恢复
            if(hintFileSet.contains(dataFileNames[i])){
                reloadFromHintFile(dataFileNames[i]);
                continue;
            }

            //从 data 文件中恢复
            if (i != this.meta.getActiveFileName()){
                this.archiveNameList.add(dataFileNames[i]);
            }
            String fileName = this.dataFilePath + File.separator + dataFileNames[i];
            try(RandomAccessFile file = new RandomAccessFile(fileName, "r")){
                long index = 0, length = file.length();
                int readEntries = 0;
                long begin = System.currentTimeMillis();

                while (index < length) {
                    readEntries++;

                    file.seek(index + 8); // skip crc
                    long timestamp = file.readLong();
                    int kSize = file.readInt();
                    int vSize = file.readInt();
                    byte[] kBytes = new byte[kSize];
                    file.read(kBytes);
                    String key = (String) SerializeUtil.deserialize(kBytes);

                    if (vSize != 0) { // vSize == 0 表示是一个 deleted entry
                        IndexEntry entry = IndexEntry.builder()
                                .kSize(kSize)
                                .vSize(vSize)
                                .key(key)
                                .timeStamp(timestamp)
                                .fileName(fileName)
                                .offset(index)
                                .build();
                        this.index.put(key, entry); //加载到 index
                    } else {
                        this.index.remove(key);
                    }

                    index = index + 24 + kSize + vSize;
                }
                this.writtenCount.addAndGet(readEntries);
                long cost = System.currentTimeMillis() - begin;

                log.info("[reload] from data file '{}', total {} bytes => {} MB, " +
                                "entries: {}, cost {}ms, rate: {} entries/s.",
                        fileName,
                        file.length(),
                        String.format("%.2f", (file.length()*1.0 / (1024*1024))),
                        readEntries,
                        cost,
                        String.format("%.2f", readEntries * 1.0 / (cost == 0 ? 1 : cost) * 1000));

            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.RELOAD_FROM_DATA_ERR, e);
            }
        }

        this.reloadTaskIsDone = true;
        log.info("[reload] done, the meta info is: {}", this.meta);
    }

    //从 hint 文件中恢复数据。
    //比如 data 文件 19, 它在 hint 目录下可能会存在一个同名的 hint 文件, 帮助启动时提高恢复速度
    private void reloadFromHintFile(String hintFileName) throws MiniDbException {
        String hintFileFullName = this.hintFilePath + File.separator + hintFileName;
        String dataFileFullName = this.dataFilePath + File.separator + hintFileName;
        long begin = System.currentTimeMillis();

        try(RandomAccessFile hintFile = new RandomAccessFile(hintFileFullName, "r")) {
            long length = hintFile.length();
            long index = 0L;
            long entryCount = 0L;
            while (index < length){
                hintFile.seek(index);
                long offset = hintFile.readLong();
                long timeStamp = hintFile.readLong();
                int  kSize = hintFile.readInt();
                int  vSize = hintFile.readInt();
                byte[] keyBytes = new byte[kSize];
                hintFile.read(keyBytes);
                String key = (String) SerializeUtil.deserialize(keyBytes);

                IndexEntry entry = IndexEntry.builder()
                        .key(key)
                        .fileName(dataFileFullName)
                        .offset(offset)
                        .kSize(kSize)
                        .vSize(vSize)
                        .timeStamp(timeStamp)
                        .build(); //构建内存索引 entry
                this.index.put(key, entry);
                this.writtenCount.incrementAndGet();

                index += (HintEntry.FIX_SIZE + kSize);

                entryCount++;
            }

            long cost = System.currentTimeMillis() - begin;
            log.info("[reload] from hint file '{}', total {} entries, rate: {} entries/s.",
                    hintFileFullName,
                    entryCount,
                    String.format("%.2f", entryCount * 1.0 / (cost == 0 ? 1 : cost) * 1000));

        } catch (IOException e) {
            throw new MiniDbException(MiniDBExceptionType.RELOAD_FROM_HINT_ERR, e);
        }
    }


    private void checkPath() throws MiniDbException {
        // check storage path
        File file = new File(this.storagePath);
        if (!file.exists()){
            if(!file.mkdirs()){
                throw new MiniDbException(MiniDBExceptionType.STORAGE_FOLDER_ERR);
            }
        }

        // check hint path
        File hint = new File(this.hintFilePath);
        if(!hint.exists()){
            try {
                if (!hint.mkdirs()){
                    throw new MiniDbException(MiniDBExceptionType.HINT_PATH_CREATE_ERR);
                }
            } catch (Exception e) {
                throw new MiniDbException(MiniDBExceptionType.HINT_PATH_CREATE_ERR, e);
            }
        }

        // check lock file
        File lockFile = new File(this.lockFileName);
        if (lockFile.exists() && lockFile.isFile()){
            throw new MiniDbException(MiniDBExceptionType.DB_HAS_BEEN_OPENED);
        } else {
            try {
                if (!lockFile.createNewFile()){
                    throw new MiniDbException(MiniDBExceptionType.LOCK_FILE_CREATE_ERR);
                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.LOCK_FILE_CREATE_ERR);
            }
        }

        // check meta file
        File meta = new File(this.metaFileName);
        if (!meta.exists()){
            try {
                if (!meta.createNewFile()){
                    throw new MiniDbException(MiniDBExceptionType.META_FILE_CREATE_ERR);
                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.META_FILE_CREATE_ERR);
            }
        }
        // check data file
        File data = new File(this.dataFilePath);
        if (!data.exists()){
            if(!data.mkdirs()){
                throw new MiniDbException(MiniDBExceptionType.DATA_PATH_CREATE_ERR);
            }

            String data0Name = this.dataFilePath + File.separator + "0";
            File data0 = new File(data0Name);
            try {
                if (!data0.createNewFile()){
                    throw new MiniDbException(MiniDBExceptionType.DATA_ZERO_CREATE_ERR);
                }
                this.active = 0;
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.DATA_ZERO_CREATE_ERR);
            }
        }
    }


    private synchronized AppendResult append(byte[] bytes, boolean compact) throws MiniDbException {
        if(this.writeOffSet + bytes.length >= SINGLE_DATA_FILE_MAX_SIZE){
            this.archiveNameList.add(String.valueOf(this.active));
            this.active++;
            // new active data file.
            createNewDataFile(this.active);

            // update meta info and flush it to disk.
            this.meta.setActiveFileName(this.active);
            syncMeta();

            // 更新写指针
            this.writeOffSet = 0;
        }
        String fileName = this.dataFilePath + File.separator + this.active;
        AppendResult result;
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {

            file.seek(this.writeOffSet);
            long crc = Crc32Util.getCrc32(bytes);
            file.writeLong(crc);
            file.write(bytes);
            // file.getFD().sync();
            result = AppendResult.builder()
                    .fileName(fileName)
                    .offset(this.writeOffSet)
                    .build();
            this.writeOffSet = file.length();
            if(!compact){
                this.writtenCount.incrementAndGet();
            }
        } catch (IOException e) {
            throw new MiniDbException(MiniDBExceptionType.APPEND_DATA_ERR);
        }
        return result;
    }

    private void createNewDataFile(int fileNo) throws MiniDbException {
        String fileName = this.dataFilePath + File.separator + fileNo;
        File file = new File(fileName);

        if (!file.exists()){
            log.info("[append] new data file '{}'", fileName);
            try {
                if(!file.createNewFile()){
                    throw new MiniDbException(MiniDBExceptionType.DATA_FILE_CREATE_ERR);
                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.DATA_FILE_CREATE_ERR, e);
            }
        }
    }

    private void createNewHintFile(String fileName) throws MiniDbException {
        File file = new File(fileName);
        if (!file.exists()){
            log.info("[compact] new hint file '{}'", fileName);
            try {
                if(!file.createNewFile()){
                    throw new MiniDbException(MiniDBExceptionType.HINT_FILE_CREATE_ERR);
                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.HINT_FILE_CREATE_ERR, e);
            }
        }
    }

    private void compact() throws MiniDbException {
        doCompact(this.archiveNameList);

        int lastCompactedFile = this.meta.getLastCompactFileName();
        synchronized (this){
            this.archiveNameList.removeIf(e -> Integer.parseInt(e) <= lastCompactedFile); // 从归档文件表中清除旧活跃文件
        }
        cleanOldDataFile(); //清除旧的数据文件
        cleanOldHintFile(); //清理过期无效的 hint 文件
        createHintFile(); //创建新 hint 文件
    }

    private void doCompact(List<String> needCompactedFileList) throws MiniDbException {
        if(needCompactedFileList == null || needCompactedFileList.size() == 0){
            log.info("[compact] no need to compact in this round.");
            return;
        }

        final int size = needCompactedFileList.size();
        this.meta.setCurrentCompactRoundActiveFileName(this.active);
        this.syncMeta();

        for (int i = 0; i < size; i++){
            // check current compact range
            if (Integer.parseInt(needCompactedFileList.get(i)) >= this.meta.getCurrentCompactRoundActiveFileName()){
                break;
            }
            String oldDataFileName =  this.dataFilePath + File.separator + needCompactedFileList.get(i);
            try (RandomAccessFile oldFile = new RandomAccessFile(oldDataFileName, "r")){
                long oldLength = oldFile.length(), oldIndex  = 0;

                while (oldIndex < oldLength) {
                    oldFile.seek(oldIndex + 8); // skip crc
                    long timestamp = oldFile.readLong();
                    int kSize = oldFile.readInt();
                    int vSize = oldFile.readInt();

                    byte[] kBytes = new byte[kSize];
                    oldFile.read(kBytes);
                    String key = (String) SerializeUtil.deserialize(kBytes);

                    int currentEntrySize = 24 + kSize + vSize;

                    this.writtenCount.decrementAndGet();

                    if (vSize != 0 && this.index.containsKey(key)){
                        IndexEntry entry = this.index.get(key);
                        if (entry.getTimeStamp() <= timestamp){
                            // read current entry bytes from file
                            byte[] currentEntryBytes = new byte[currentEntrySize - 8]; //not include crc
                            oldFile.seek(oldIndex + 8);
                            oldFile.read(currentEntryBytes);

                            AppendResult result = append(currentEntryBytes, true); //call 'sync' block.
                            IndexEntry copyEntry = entry.copy();

                            // ready entry copy
                            copyEntry.setFileName(result.fileName);
                            copyEntry.setOffset(result.offset);

                            synchronized (this) { //与put方法竞争锁
                                if(this.index.containsKey(key)){
                                    entry = this.index.get(key);

                                    //拿到锁后，再次检查数据版本，防止旧版本数据覆盖新版本数据
                                    if(entry.getVSize() != 0 && entry.getTimeStamp() <= copyEntry.getTimeStamp()){
                                        this.index.put(key, copyEntry);
                                        this.writtenCount.incrementAndGet();
                                    }
                                }
                            }
                        }
                    }
                    oldIndex += currentEntrySize;

                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.COMPACT_ERR, e);
            }

            // update meta info and flush when each old file is compacted
            this.meta.setLastCompactFileName(Integer.parseInt(needCompactedFileList.get(i)));
            this.syncMeta();


            // delete old data file when we have updated meta info
            File oldFile = new File(oldDataFileName);
            if(!oldFile.delete()){
                throw new MiniDbException(MiniDBExceptionType.DATA_FILE_DELETE_ERR);
            }
            log.info("[compact] delete old file: " + oldDataFileName);
        }
    }

    private void cleanOldDataFile(){
        File dataFile = new File(this.dataFilePath); //加载 data 文件目录
        String[] dataFileNames = dataFile.list((dir, name) -> Integer.parseInt(name) <= meta.getLastCompactFileName());
        if (dataFileNames == null || dataFileNames.length == 0){
            return;
        }
        for (String name: dataFileNames){
            File file = new File(this.dataFilePath + File.separator + name);
            if (file.exists()){
                if (!file.delete()){
                    log.warn("[compact] unable to delete old data file '{}'.", name);
                }
            }
        }
    }

    private void cleanOldHintFile(){
        File hintFile = new File(this.hintFilePath); //加载 hint 文件目录
        String[] hintFileNames = hintFile.list((dir, name) -> Integer.parseInt(name) <= meta.getLastCompactFileName());
        if (hintFileNames == null || hintFileNames.length == 0){
            return;
        }
        for (String name: hintFileNames){
            File file = new File(this.hintFilePath + File.separator + name);
            if (file.exists()){
                if (!file.delete()){
                    log.warn("[compact] unable to delete hint file '{}'.", name);
                }
            }
        }
    }

    private void createHintFile() throws MiniDbException {
        int size = this.archiveNameList.size();
        for (int i = 0; i < size; i++){
            String fileName =  this.dataFilePath + File.separator + this.archiveNameList.get(i);
            String hintName = this.hintFilePath + File.separator + this.archiveNameList.get(i);
            createNewHintFile(hintName);

            try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                    RandomAccessFile hint = new RandomAccessFile(hintName, "rw")
                ){
                long length = file.length(), index  = 0;
                while (index < length) {
                    file.seek(index + 8); // skip crc
                    long timestamp = file.readLong();
                    int kSize = file.readInt();
                    int vSize = file.readInt();
                    byte[] kBytes = new byte[kSize];
                    file.read(kBytes);
                    String key = (String) SerializeUtil.deserialize(kBytes);
                    int currentEntrySize = 24 + kSize + vSize;

                    HintEntry hintEntry = HintEntry.builder()
                            .offset(index)
                            .kSize(kSize)
                            .vSize(vSize)
                            .timeStamp(timestamp)
                            .key(key)
                            .build();
                    hint.write(hintEntry.encode());

                    index += currentEntrySize;

                }
            } catch (IOException e) {
                throw new MiniDbException(MiniDBExceptionType.HINT_FILE_CREATE_ERR, e);
            }
        }

    }

    private void loadMeta(){
        try (RandomAccessFile meta = new RandomAccessFile(this.metaFileName, "rw")){
            if(meta.length() == Meta.SIZE) {
                meta.seek(0);
                int active = meta.readInt();
                int currentActive = meta.readInt();
                int lastCompact = meta.readInt();

                // update meta info
                this.meta.setActiveFileName(active);
                this.meta.setCurrentCompactRoundActiveFileName(currentActive);
                this.meta.setLastCompactFileName(lastCompact);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void syncMeta(){
        try (RandomAccessFile meta = new RandomAccessFile(this.metaFileName, "rw")){
            log.info("[·meta·] sync meta to disk, " + this.meta);
            meta.seek(0);
            byte[] bytes = this.meta.getBytes();
            meta.write(bytes);
            meta.getFD().sync(); // sync it to disk
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private EncodeResult encode(String key, Object value, long time) throws MiniDbException {
        byte[] kBytes = SerializeUtil.serialize(key);
        byte[] vBytes = value == null ? new byte[0] : SerializeUtil.serialize(value);
        byte[] kSize  = ByteUtil.intToBytes(kBytes.length);
        byte[] vSize  = ByteUtil.intToBytes(vBytes.length);
        byte[] timestamp = ByteUtil.longToBytes(time);

        byte[] entry = ByteUtil.combineByteArray(timestamp, kSize, vSize, kBytes, vBytes);
        return EncodeResult.builder()
                .bytes(entry)
                .vSize(vBytes.length)
                .kSize(kBytes.length)
                .build();
    }

    // bytes 表示存储在磁盘上的一个日志条目
    private Object decode(byte[] bytes, int vSize) throws MiniDbException {// 从后往前取
        byte[] vBytes = new byte[vSize];
        System.arraycopy(bytes, bytes.length - vSize , vBytes, 0, vSize);
        return SerializeUtil.deserialize(vBytes);
    }

    @Builder
    private static class AppendResult {
        private String fileName;
        private long offset;
        public static final AppendResult FAILED = null;
    }

    @Builder
    private static class EncodeResult {
        private int vSize;
        private int kSize;
        private byte[] bytes;
    }
}
