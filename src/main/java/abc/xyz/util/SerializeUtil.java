package abc.xyz.util;

import abc.xyz.exception.MiniDBExceptionType;
import abc.xyz.exception.MiniDbException;

import java.io.*;

public class SerializeUtil {
    public static byte[] serialize(Object object) throws MiniDbException {
        byte[] bytes = null;
        try(ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos)
        ){
            oos.writeObject(object);
            bytes = bos.toByteArray();
        } catch (IOException e) {
            throw new MiniDbException(MiniDBExceptionType.SERIALIZE_ERR);
        }
        return bytes;
    }

    public static Object deserialize(byte[] bytes) throws MiniDbException {
        Object object = null;
        try( ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ){
            object = ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new MiniDbException(MiniDBExceptionType.DESERIALIZE_ERR);
        }
        return object;
    }
}
