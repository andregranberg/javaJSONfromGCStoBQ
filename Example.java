import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import java.io.BufferedWriter;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;

public class Example implements HttpFunction {
  @Override
  public void service(HttpRequest request, HttpResponse response) throws Exception {
    String datasetName = "cloudFunctionETL";
    String tableName = "java";
    String sourceUri = "gs://crazyswede/newLine.json";

    BufferedWriter writer = response.getWriter();

    try {
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      TableId tableId = TableId.of(datasetName, tableName);
      LoadJobConfiguration loadConfig =
        LoadJobConfiguration.newBuilder(tableId, sourceUri)
            .setFormatOptions(FormatOptions.json())
            .setAutodetect(true)
            .build();

      Job job = bigquery.create(JobInfo.of(loadConfig));
      
      job = job.waitFor();
        if (job.isDone()) {
          writer.write("Json from GCS successfully loaded in a table");
        } else {
          writer.write(
            "BigQuery was unable to load into the table due to an error:"
                + job.getStatus().getError());
        }
    } catch (BigQueryException | InterruptedException e) {
        String problem = e.toString();
        writer.write(problem);
    }
  }
}
