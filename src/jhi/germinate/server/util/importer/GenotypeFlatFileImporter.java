package jhi.germinate.server.util.importer;

import com.google.gson.Gson;

import org.jooq.DSLContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

import jhi.germinate.resource.ImportResult;
import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.tables.records.*;
import jhi.germinate.server.util.CollectionUtils;

import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Mapdefinitions.*;
import static jhi.germinate.server.database.tables.Mapfeaturetypes.*;
import static jhi.germinate.server.database.tables.Maps.*;
import static jhi.germinate.server.database.tables.Markers.*;
import static jhi.germinate.server.database.tables.Markertypes.*;

/**
 * @author Sebastian Raubach
 */
public class GenotypeFlatFileImporter
{
	private static final List<String> headers = Arrays.asList("Linkage Group / Chromosome", "Position", "Lines/Markers");

	private   Map<String, Integer>            germplasmToId = new HashMap<>();
	private   Map<String, Integer>            markerToId    = new HashMap<>();
	protected boolean                         isUpdate;
	private   boolean                         deleteOnFail;
	protected File                            input;
	private   Map<ImportStatus, ImportResult> errorMap      = new HashMap<>();

	private String[]            markers       = null;
	private int[]               markerIds     = null;
	private String[]            chromosomes   = null;
	private String[]            positions     = null;
	private Map<String, String> headerMapping = new HashMap<>();

	public GenotypeFlatFileImporter(File input, boolean isUpdate, boolean deleteOnFail)
	{
		this.input = input;
		this.isUpdate = isUpdate;
		this.deleteOnFail = deleteOnFail;
	}

	protected void init(String[] args)
	{
		Database.init(args[0], args[1], args[2], args[3], args[4], false);
	}

	public void run(AbstractImporter.RunType runtype)
	{
		try
		{
			prepare();

			if (runtype.includesCheck())
				checkFile();

			if (errorMap.size() < 1)
			{
				if (runtype.includesImport())
				{
					if (isUpdate)
						updateFile();
					else
						importFile();
				}
			}
			else if (deleteOnFail)
			{
				input.delete();
			}

			List<ImportResult> result = getImportResult();

			String jsonFilename = this.input.getName().substring(0, this.input.getName().lastIndexOf("."));
			File json = new File(input.getParent(), jsonFilename + ".json");
			Files.write(json.toPath(), Collections.singletonList(new Gson().toJson(result)), StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorMap.values());
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!errorMap.containsKey(status))
			errorMap.put(status, new ImportResult(status, rowIndex, message));
	}

	protected void prepare()
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			germplasmToId = context.selectFrom(GERMINATEBASE)
								   .fetchMap(GERMINATEBASE.NAME, GERMINATEBASE.ID);
			markerToId = context.selectFrom(MARKERS)
								.fetchMap(MARKERS.MARKER_NAME, MARKERS.ID);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			// TODO
		}
	}

	protected void checkFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.input.toPath(), StandardCharsets.UTF_8))
		{
			String line = readHeaders(br);

			if (CollectionUtils.isEmpty(markers))
			{
				addImportResult(ImportStatus.GENOTYPE_MISSING_ROW, -1, "Markers row empty");
			}
			else if (!CollectionUtils.isEmpty(positions) || !CollectionUtils.isEmpty(chromosomes))
			{
				if (positions.length != chromosomes.length || positions.length != markers.length)
					addImportResult(ImportStatus.GENOTYPE_HEADER_LENGTH_MISMATCH, -1, "Mismatch between markers, positions and chromosomes.");
			}

			for (String requiredHeader : headers)
			{
				if (!headerMapping.containsKey(requiredHeader))
				{
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, "Missing header: " + requiredHeader);
				}
			}

			// Skip the marker definitions
			br.readLine();
			int counter = 7;
			while ((line = br.readLine()) != null)
			{
				String germplasm = line.substring(0, line.indexOf("\t"));

				if (!germplasmToId.containsKey(germplasm))
					addImportResult(ImportStatus.GENOTYPE_INVALID_GERMPLASM, counter, germplasm);

				counter++;
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private String readHeaders(BufferedReader br)
		throws IOException
	{
		String line;

		// Read the headers, set defaults first
		headerMapping.put("dataset", input.getName());
		headerMapping.put("map", input.getName());
		headerMapping.put("markerType", "UNKNOWN");
		while ((line = br.readLine()) != null && line.startsWith("#"))
		{
			String[] parts = line.split("=", -1);
			if (parts.length == 2)
			{
				parts[0] = parts[0].trim();
				parts[1] = parts[1].trim();

				headerMapping.put(parts[0], parts[1]);
			}
		}

		// Now the map
		while ((markers == null || chromosomes == null || positions == null) && line != null)
		{
			if (line.startsWith("Linkage Group / Chromosome"))
				chromosomes = line.replace("Linkage Group / Chromosome\t", "").split("\t", -1);
			if (line.startsWith("Position"))
				positions = line.replace("Position\t", "").split("\t", -1);
			if (line.startsWith("Lines/Markers"))
			{
				markers = line.replace("Line/Markers\t", "").split("\t", -1);
				markerIds = new int[markers.length];
			}

			line = br.readLine();
		}

		return line;
	}

	protected void importFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.input.toPath(), StandardCharsets.UTF_8);
			 Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			String line = readHeaders(br);

			// TODO: Create dataset

			String markerTypeName = headerMapping.get("markerType");
			MarkertypesRecord markerType = context.selectFrom(MARKERTYPES)
												  .where(MARKERTYPES.DESCRIPTION.eq(markerTypeName))
												  .fetchAny();

			if (markerType == null)
			{
				markerType = context.newRecord(MARKERTYPES);
				markerType.setDescription(markerTypeName);
				markerType.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				markerType.store();
			}

			MapfeaturetypesRecord mapFeatureType = context.selectFrom(MAPFEATURETYPES)
														  .where(MAPFEATURETYPES.DESCRIPTION.eq(markerTypeName))
														  .fetchAny();

			if (mapFeatureType == null)
			{
				mapFeatureType = context.newRecord(MAPFEATURETYPES);
				mapFeatureType.setDescription(markerTypeName);
				mapFeatureType.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				mapFeatureType.store();
			}

			MapsRecord map = context.newRecord(MAPS);
			map.setName(headerMapping.get("map"));
			map.setDescription(headerMapping.get("map"));
			map.setVisibility(true);
			map.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			map.store();

			// Import markers
			Map<MarkersRecord, Integer> newMarkersToIndices = new LinkedHashMap<>();
			for (int i = 0; i < markers.length; i++)
			{
				String marker = markers[i];
				Integer id = markerToId.get(marker);

				if (id == null)
				{
					MarkersRecord record = context.newRecord(MARKERS);
					record.setMarkerName(marker);
					record.setMarkertypeId(markerType.getId());
					record.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					newMarkersToIndices.put(record, i);

					if (newMarkersToIndices.size() > 10000)
					{
						insertMarkers(context, newMarkersToIndices);
						newMarkersToIndices.clear();
					}
				}
				else
				{
					markerIds[i] = id;
				}
			}

			if (newMarkersToIndices.size() > 10000)
			{
				insertMarkers(context, newMarkersToIndices);
				newMarkersToIndices.clear();
			}

			if (!CollectionUtils.isEmpty(positions) && !CollectionUtils.isEmpty(chromosomes))
			{
				List<MapdefinitionsRecord> newDataToIndices = new ArrayList<>();

				for (int i = 0; i < markers.length; i++)
				{
					int markerId = markerIds[i];
					int mapId = map.getId();
					int mapFeatureTypeId = mapFeatureType.getId();
					String chromosome = chromosomes[i];
					Double position = 0d;
					try
					{
						position = Double.parseDouble(positions[i]);
					}
					catch (Exception e)
					{
					}


					MapdefinitionsRecord record = context.newRecord(MAPDEFINITIONS);
					record.setMarkerId(markerId);
					record.setMapId(mapId);
					record.setMapfeaturetypeId(mapFeatureTypeId);
					record.setChromosome(chromosome);
					record.setDefinitionStart(position);
					record.setDefinitionEnd(position);
					record.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					newDataToIndices.add(record);

					if (newDataToIndices.size() > 10000)
					{
						context.batchStore(newDataToIndices)
							   .execute();
					}
				}

				if (newDataToIndices.size() > 10000)
				{
					context.batchStore(newDataToIndices)
						   .execute();
				}
			}

			// TODO: Now dataset members

			// TODO: Now the data. Write to new file, then generate HDF5
		}
		catch (IOException | SQLException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void insertMarkers(DSLContext context, Map<MarkersRecord, Integer> newDataToIndices)
	{
		int[] ids = context.batchStore(newDataToIndices.keySet())
						   .execute();

		List<Integer> indices = new ArrayList<>(newDataToIndices.values());

		for (int j = 0; j < indices.size(); j++)
		{
			markerIds[indices.get(j)] = ids[j];
		}
	}

	protected void updateFile()
	{
		// We don't update, just import
		importFile();
	}
}
