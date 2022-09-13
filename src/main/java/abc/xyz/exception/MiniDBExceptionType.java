package abc.xyz.exception;

public enum MiniDBExceptionType {
    STORAGE_FOLDER_ERR  (701, "Unable to create db storage folder"),
    DATA_FILE_CREATE_ERR(702, "Unable to create data file"),
    DATA_ZERO_CREATE_ERR(703, "Unable to create data file 0"),
    DATA_FILE_DELETE_ERR(704, "Unable to delete old data file"),
    DATA_PATH_CREATE_ERR(705, "Unable to delete old data file"),
    HINT_FILE_CREATE_ERR(706, "Unable to create hint file"),
    HINT_FILE_DELETE_ERR(707, "Unable to delete old hint file"),
    HINT_PATH_CREATE_ERR(706, "Unable to create hint folder"),
    LOCK_FILE_CREATE_ERR(708, "Unable to create lock file"),
    LOCK_FILE_DELETE_ERR(709, "Unable to delete lock file"),
    META_FILE_CREATE_ERR(710, "Unable to create meta file"),
    META_FILE_DELETE_ERR(711, "Unable to delete meta file"),
    RELOAD_FROM_DATA_ERR(712, "An error occurred when we reload from data file"),
    RELOAD_FROM_HINT_ERR(713, "An error occurred when we reload from hint file"),
    COMPACT_ERR         (714, "An error occurred when we compact data"),
    OPEN_CHECK_ERR      (715, "An error occurred when we check db storage path"),
    DB_HAS_BEEN_OPENED  (716, "Db has been opened, please ensure that you have the ownership"),
    APPEND_DATA_ERR     (717, "Failed to append data"),
    SERIALIZE_ERR       (718, "object serialize  error"),
    DESERIALIZE_ERR     (719, "object deserialize error"),
    NORMAL_ERR          (700, "An error occurred in db");

    private final int code;
    private final String desc;

    MiniDBExceptionType(int code, String desc){
        this.code  = code;
        this.desc  = desc;
    }

    public String getDesc() {
        return desc;
    }

    public int getCode() {
        return code;
    }
}
