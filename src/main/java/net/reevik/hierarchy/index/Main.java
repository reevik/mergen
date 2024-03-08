package net.reevik.hierarchy.index;

import java.util.Set;
import javax.xml.crypto.Data;

public class Main {

  public static void main(String[] args) {
    BTreeIndex bTreeIndex = new BTreeIndex();
    bTreeIndex.upsert(createRecord("500", "500"));
    bTreeIndex.upsert(createRecord("400", "400"));
    bTreeIndex.upsert(createRecord("600", "600"));
    bTreeIndex.upsert(createRecord("700", "700"));
    bTreeIndex.upsert(createRecord("300", "300"));
    bTreeIndex.upsert(createRecord("450", "450"));
    Set<DataRecord> query = bTreeIndex.query("450");
    System.out.println(query);
  }

  static DataRecord createRecord(String indexKey, String payload) {
    return new DataRecord(indexKey, payload.getBytes());
  }
}