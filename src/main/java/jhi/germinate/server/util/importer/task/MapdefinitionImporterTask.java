package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.ImportStatus;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;

import static jhi.germinate.server.database.codegen.tables.Mapdefinitions.*;

public abstract class MapdefinitionImporterTask implements Runnable
{
	private final String[]      markers;
	private final int[]         markerIds;
	private final int           mapId;
	private final int           mapFeatureTypeId;
	private final String[]      chromosomes;
	private final String[]      positions;
	private final ErrorCallback callback;

	public MapdefinitionImporterTask(String[] markers, int[] markerIds, int mapId, int mapFeatureTypeId, String[] chromosomes, String[] positions, ErrorCallback callback)
	{
		this.markers = markers;
		this.markerIds = markerIds;
		this.mapId = mapId;
		this.mapFeatureTypeId = mapFeatureTypeId;
		this.chromosomes = chromosomes;
		this.positions = positions;
		this.callback = callback;
	}

	@Override
	public void run()
	{
		try
		{
			// Write the data to a temporary file
			File temp = Files.createTempFile("mapdefinitions", "txt").toFile();
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp, StandardCharsets.UTF_8)))
			{
				bw.write("mapfeaturetype_id\tmarker_id\tmap_id\tdefinition_start\tdefinition_end\tchromosome");
				bw.newLine();
				for (int i = 0; i < markers.length; i++)
				{
					int markerId = markerIds[i];
					String chromosome = chromosomes[i];
					Double position = 0d;
					try
					{
						position = Double.parseDouble(positions[i]);
					}
					catch (Exception e)
					{
					}

					bw.write(mapFeatureTypeId + "\t" + markerId + "\t" + mapId + "\t" + position + "\t" + position + "\t" + chromosome);
					bw.newLine();
				}
			}

			try (Connection conn = Database.getConnection())
			{
				DSLContext context = Database.getContext(conn);
				context.execute("SET autocommit=0;");
				context.execute("SET unique_checks=0;");
				context.execute("SET foreign_key_checks=0;");

				// Then load it using the LOAD INTO mechanism
				context.loadInto(MAPDEFINITIONS)
					   .bulkAfter(2000)
					   .loadCSV(temp, StandardCharsets.UTF_8)
					   .fields(MAPDEFINITIONS.MAPFEATURETYPE_ID, MAPDEFINITIONS.MARKER_ID, MAPDEFINITIONS.MAP_ID, MAPDEFINITIONS.DEFINITION_START, MAPDEFINITIONS.DEFINITION_END, MAPDEFINITIONS.CHROMOSOME)
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
