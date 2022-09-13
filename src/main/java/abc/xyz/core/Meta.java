package abc.xyz.core;

import abc.xyz.util.ByteUtil;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Meta {
    private int activeFileName;
    private int currentCompactRoundActiveFileName; // when compact task is starting, hold current active data file name.
    private int lastCompactFileName;

    public static final int SIZE = 12;

    public static final Meta INITIAL = Meta.builder()
            .activeFileName(0)
            .currentCompactRoundActiveFileName(-1)
            .lastCompactFileName(-1)
            .build();

    public byte[] getBytes(){
        byte[] activeFileNameBytes = ByteUtil.intToBytes(activeFileName);
        byte[] currentActiveFileNameBytes    = ByteUtil.intToBytes(currentCompactRoundActiveFileName);
        byte[] lastCompactFileNameBytes  = ByteUtil.intToBytes(lastCompactFileName);
        return ByteUtil.combineByteArray(activeFileNameBytes, currentActiveFileNameBytes, lastCompactFileNameBytes);
    }

}
