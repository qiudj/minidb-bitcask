package abc.xyz.util;

public class ByteUtil {

    public static byte[] combineByteArray(byte[]... arr){
        int length = 0;
        for (byte[] a: arr){
            if (a != null && a.length > 0){
                length += a.length;
            }
        }

        byte[] res = new byte[length];
        int index = 0;
        for (byte[] a: arr){
            if (a != null && a.length > 0){
                System.arraycopy(a, 0, res, index, a.length);
                index += a.length;
            }

        }
        return res;
    }

    public static byte[] intToBytes(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[0] = (byte) (n >> 24 & 0xff);
        return b;
    }

    public static byte[] longToBytes(long num) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; ++i) {
            int offset = 64 - (i + 1) * 8;
            b[i] = (byte) ((num >> offset) & 0xff);
        }
        return b;
    }

    public static long bytesToLong(byte[] byteNum) {
        long num = 0;
        for (int i = 0; i < 8; ++i) {
            num <<= 8;
            num |= (byteNum[i] & 0xff);
        }
        return num;
    }

}
