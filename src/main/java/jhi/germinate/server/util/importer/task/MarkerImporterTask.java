package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.ImportStatus;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.List;

import static jhi.germinate.server.database.codegen.tables.Markers.MARKERS;

public class MarkerImporterTask implements Runnable
{
	private final List<String>  newMarkers;
	private final int           markerTypeId;
	private final ErrorCallback callback;

	public MarkerImporterTask(List<String> newMarkers, int markerTypeId, ErrorCallback callback)
	{
		this.newMarkers = newMarkers;
		this.markerTypeId = markerTypeId;
		this.callback = callback;
	}

	@Override
	public void run()
	{
		try (Connection conn = Database.getConnection())
		{
			File tempMarkers = Files.createTempFile("markers", "txt").toFile();
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempMarkers, StandardCharsets.UTF_8)))
			{
				bw.write("markertype_id\tmarker_name");
				bw.newLine();

				newMarkers.forEach(marker -> {
					try
					{
						bw.write(markerTypeId + "\t" + marker);
						bw.newLine();
					}
					catch (IOException e)
					{
						// Do nothing here
					}
				});
			}

			DSLContext context = Database.getContext(conn);
			context.execute("SET autocommit=0;");
			context.execute("SET unique_checks=0;");
			context.execute("SET foreign_key_checks=0;");

			// Then load it using the LOAD INTO mechanism
			context.loadInto(MARKERS)
				   .bulkAfter(2000)
				   .loadCSV(tempMarkers, StandardCharsets.UTF_8)
				   .fields(MARKERS.MARKERTYPE_ID, MARKERS.MARKER_NAME)
				   .separator('\t')
				   .execute();

			context.execute("SET autocommit=1;");
			context.execute("SET unique_checks=1;");
			context.execute("SET foreign_key_checks=1;");

			tempMarkers.delete();
		}
		catch (IOException | SQLException e)
		{
			callback.onError(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}
}
