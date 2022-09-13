package abc.xyz.util;

import java.util.zip.CRC32;

public class Crc32Util {

    public static long getCrc32(byte[] bytes){
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return crc32.getValue();
    }

}
