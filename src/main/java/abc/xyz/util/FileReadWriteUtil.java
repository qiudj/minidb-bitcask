package abc.xyz.util;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileReadWriteUtil {

    public static byte[] readBytes(String fileName, long offset, int size){
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            byte[] bytes = new byte[size];
            file.seek(offset);
            file.read(bytes);
            return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
