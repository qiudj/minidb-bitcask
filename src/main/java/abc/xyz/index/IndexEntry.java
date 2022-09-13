package abc.xyz.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexEntry {
    private String key;
    private String fileName;
    private long offset;
    private int kSize;
    private int vSize;
    private long timeStamp;

    public IndexEntry copy() {
        return IndexEntry.builder()
                .key(key)
                .fileName(fileName)
                .offset(offset)
                .kSize(kSize)
                .vSize(vSize)
                .timeStamp(timeStamp)
                .build();
    }
}
