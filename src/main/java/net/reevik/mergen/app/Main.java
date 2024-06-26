package net.reevik.mergen.app;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import net.reevik.mergen.index.BTreeIndex;
import net.reevik.mergen.index.DataEntity;
import net.reevik.mergen.index.DataRecord;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.ioc.MikronContext;

@ManagedApplication(packages = {"net.reevik.hierarchy.*"})
public class Main {

  public static void main(String[] args) {
    try (var context = MikronContext.init(Main.class)) {
      var bTreeIndex = context.<BTreeIndex>getInstance(BTreeIndex.class.getName())
          .orElseThrow(fail());
      bTreeIndex.upsert(createRecord("500", "500"));
      bTreeIndex.upsert(createRecord("400", "400"));
      bTreeIndex.upsert(createRecord("600", "600"));
      bTreeIndex.upsert(createRecord("700", "700"));
      bTreeIndex.upsert(createRecord("300", "300"));
      bTreeIndex.upsert(createRecord("450", "450"));
      List<DataRecord> query = bTreeIndex.query("450");
      System.out.println(query);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Supplier<Exception> fail() {
    return () -> new RuntimeException("No managed index instance found.");
  }

  static DataEntity createRecord(String indexKey, String payload) {
    return new DataEntity(indexKey, payload.getBytes());
  }
}