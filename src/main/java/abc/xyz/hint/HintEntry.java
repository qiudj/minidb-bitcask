package abc.xyz.hint;

import abc.xyz.exception.MiniDbException;
import abc.xyz.util.ByteUtil;
import abc.xyz.util.SerializeUtil;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HintEntry {
    // 24 字节 + key 的长度
    private long offset;
    private long timeStamp;
    private int  kSize;
    private int  vSize;
    private String key;

    public static final int FIX_SIZE = 24;

    public byte[] encode() throws MiniDbException {
        byte[] offsetBytes = ByteUtil.longToBytes(offset);
        byte[] timestampBytes = ByteUtil.longToBytes(timeStamp);
        byte[] kSizeBytes = ByteUtil.intToBytes(kSize);
        byte[] vSizeBytes = ByteUtil.intToBytes(vSize);
        byte[] keyBytes = SerializeUtil.serialize(key);

        return ByteUtil.combineByteArray(offsetBytes, timestampBytes,
                kSizeBytes, vSizeBytes, keyBytes);
    }
}
