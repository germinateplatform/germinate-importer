package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.ImportStatus;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.Set;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;

public abstract class DatasetMemberImporterTask implements Runnable
{
	private final Set<Integer>  markerIdsInFile;
	private final Set<Integer>  germplasmIdsInFile;
	private final int           datasetId;
	private final ErrorCallback callback;

	public DatasetMemberImporterTask(Set<Integer> markerIdsInFile, Set<Integer> germplasmIdsInFile, int datasetId, ErrorCallback callback)
	{
		this.markerIdsInFile = markerIdsInFile;
		this.germplasmIdsInFile = germplasmIdsInFile;
		this.datasetId = datasetId;
		this.callback = callback;
	}

	@Override
	public void run()
	{
		try
		{
			// Write the data to a temporary file
			File temp = Files.createTempFile("datasetmembers", "txt").toFile();
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp, StandardCharsets.UTF_8)))
			{
				bw.write("dataset_id\tforeign_id\tdatasetmembertype_id");
				bw.newLine();

				markerIdsInFile.forEach(id -> {
					try
					{
						bw.write(datasetId + "\t" + id + "\t1");
						bw.newLine();
					}
					catch (IOException e)
					{
						// Do nothing here
					}
				});

				germplasmIdsInFile.forEach(id -> {
					try
					{
						bw.write(datasetId + "\t" + id + "\t2");
						bw.newLine();
					}
					catch (IOException e)
					{
						// Do nothing here
					}
				});
			}

			try (Connection conn = Database.getConnection())
			{
				DSLContext context = Database.getContext(conn);
				context.execute("SET autocommit=0;");
				context.execute("SET unique_checks=0;");
				context.execute("SET foreign_key_checks=0;");

				// Then load it using the LOAD INTO mechanism
				context.loadInto(DATASETMEMBERS)
					   .bulkAfter(2000)
					   .loadCSV(temp, StandardCharsets.UTF_8)
					   .fields(DATASETMEMBERS.DATASET_ID, DATASETMEMBERS.FOREIGN_ID, DATASETMEMBERS.DATASETMEMBERTYPE_ID)
					   .separator('\t')
					   .execute();

				context.execute("SET autocommit=1;");
				context.execute("SET unique_checks=1;");
				context.execute("SET foreign_key_checks=1;");

				temp.delete();
			}
			catch (SQLException e)
			{
				callback.onError(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			}
		}
		catch (IOException e)
		{
			callback.onError(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
		finally
		{
			this.onFinished();
		}
	}

	protected abstract void onFinished();
}
