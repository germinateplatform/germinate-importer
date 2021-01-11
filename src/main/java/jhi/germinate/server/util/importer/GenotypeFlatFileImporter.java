package jhi.germinate.server.util.importer;

import com.google.gson.Gson;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.*;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Experiments.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Mapdefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Mapfeaturetypes.*;
import static jhi.germinate.server.database.codegen.tables.Maps.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;
import static jhi.germinate.server.database.codegen.tables.Markertypes.*;

/**
 * @author Sebastian Raubach
 */
public class GenotypeFlatFileImporter
{
	private   Map<String, Integer>            germplasmToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private   Map<String, Integer>            markerToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	protected boolean                         isUpdate;
	private   boolean                         deleteOnFail;
	protected File                            input;
	private   File                            hdf5;
	private   Map<ImportStatus, ImportResult> errorMap      = new HashMap<>();

	private String[]            markers       = null;
	private int[]               markerIds     = null;
	private String[]            chromosomes   = null;
	private String[]            positions     = null;
	private Map<String, String> headerMapping = new HashMap<>();

	private Set<Integer> markerIdsInFile    = new HashSet<>();
	private Set<Integer> germplasmIdsInFile = new HashSet<>();

	private DatasetsRecord dataset;

	public static void main(String[] args)
	{
		if (args.length != 10)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		GenotypeFlatFileImporter importer = new GenotypeFlatFileImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Boolean.parseBoolean(args[7]));
		importer.init(args);
		importer.run(AbstractImporter.RunType.getType(args[8]));
	}

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
		try (DSLContext context = Database.getContext())
		{
			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> germplasmToId.put(g.getName(), g.getId()));
			context.selectFrom(MARKERS)
				   .forEach(m -> markerToId.put(m.getMarkerName(), m.getId()));
		}
	}

	protected Map<ImportStatus, ImportResult> checkFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.input.toPath(), StandardCharsets.UTF_8))
		{
			String line = readHeaders(br);

			if (CollectionUtils.isEmpty(markers))
			{
				addImportResult(ImportStatus.GENOTYPE_MISSING_ROW, -1, "Markers row empty");
			}
			else if (positions == null)
			{
				addImportResult(ImportStatus.GENOTYPE_MISSING_ROW, -1, "Marker position row is missing.");
			}
			else if (chromosomes == null)
			{
				addImportResult(ImportStatus.GENOTYPE_MISSING_ROW, -1, "Marker chromosomes row is missing.");
			}
			else if (!CollectionUtils.isEmpty(positions) || !CollectionUtils.isEmpty(chromosomes))
			{
				if (positions.length != chromosomes.length || positions.length != markers.length)
					addImportResult(ImportStatus.GENOTYPE_HEADER_LENGTH_MISMATCH, -1, "Mismatch between markers, positions and chromosomes.");

				for (String position : positions)
				{
					try
					{
						Float.parseFloat(position);
					}
					catch (NumberFormatException e)
					{
						addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, -1, "Marker position has to be a number.");
					}
				}
			}

			int counter = 1;
			do
			{
				if (StringUtils.isEmpty(line))
					continue;

				int index = line.indexOf("\t");

				if (index == -1)
					continue;

				String germplasm = line.substring(0, index);

				if (!germplasmToId.containsKey(germplasm))
					addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, counter, germplasm);

				counter++;
			} while ((line = br.readLine()) != null);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}

		return errorMap;
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
			String[] parts = line.substring(1).split("=", -1);
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
				markers = line.replace("Lines/Markers\t", "").split("\t", -1);
				markerIds = new int[markers.length];
			}

			line = br.readLine();
		}

		return line;
	}

	protected void importFile()
	{
		// We need to navigate to the correct location for the resulting hdf5 file
		this.hdf5 = new File(new File(new File(input.getParentFile().getParentFile().getParentFile(), "data"), "genotypes"), input.getName() + ".hdf5");
		this.hdf5.getParentFile().mkdirs();

		try (BufferedReader br = Files.newBufferedReader(this.input.toPath(), StandardCharsets.UTF_8);
			 DSLContext context = Database.getContext())
		{
			String line = readHeaders(br);
//			line = br.readLine();
//			line = br.readLine();

			do
			{
				if (StringUtils.isEmpty(line))
					continue;

				int index = line.indexOf("\t");

				if (index == -1)
					continue;

				// Remember the germplasm ids
				String germplasm = line.substring(0, index);
				germplasmIdsInFile.add(germplasmToId.get(germplasm));
			}
			while ((line = br.readLine()) != null);

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
					markerIdsInFile.add(id);
				}
			}

			if (newMarkersToIndices.size() > 0)
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
						newDataToIndices.clear();
					}
				}

				if (newDataToIndices.size() > 0)
				{
					context.batchStore(newDataToIndices)
						   .execute();
					newDataToIndices.clear();
				}
			}

			ExperimentsRecord experiment = context.selectFrom(EXPERIMENTS)
												  .where(EXPERIMENTS.EXPERIMENT_NAME.eq(headerMapping.get("dataset")))
												  .fetchAny();

			if (experiment == null)
			{
				experiment = context.newRecord(EXPERIMENTS);
				experiment.setExperimentName(headerMapping.get("dataset"));
				experiment.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				experiment.store();
			}

			if (dataset == null)
			{
				dataset = context.newRecord(DATASETS);
				dataset.setExperimentId(experiment.getId());
				dataset.setDatasettypeId(1);
				// Hide it initially. We don't want people using half-imported data.
				dataset.setDatasetStateId(3);
				dataset.setName(headerMapping.get("dataset"));
				dataset.setDescription(headerMapping.get("dataset"));
				dataset.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			}
			else
			{
				// We need to fetch it again, because its database connection has been closed and updates won't be possible
				dataset = context.selectFrom(DATASETS)
								 .where(DATASETS.ID.eq(dataset.getId()))
								 .fetchAny();
			}

			dataset.setSourceFile(input.getName() + ".hdf5");
			dataset.store();

			List<DatasetmembersRecord> dsMembers = markerIdsInFile.stream()
																  .map(id -> {
																	  DatasetmembersRecord dsm = context.newRecord(DATASETMEMBERS);
																	  dsm.setDatasetId(dataset.getId());
																	  dsm.setDatasetmembertypeId(1);
																	  dsm.setForeignId(id);
																	  dsm.setCreatedOn(new Timestamp(System.currentTimeMillis()));
																	  return dsm;
																  })
																  .collect(Collectors.toList());
			int startPosition = 0;

			while (startPosition < dsMembers.size())
			{
				List<DatasetmembersRecord> batch = dsMembers.subList(startPosition, startPosition + Math.min(10000, dsMembers.size() - startPosition));
				startPosition += batch.size();

				context.batchStore(batch)
					   .execute();
			}

			dsMembers = germplasmIdsInFile.stream()
										  .map(id -> {
											  DatasetmembersRecord dsm = context.newRecord(DATASETMEMBERS);
											  dsm.setDatasetId(dataset.getId());
											  dsm.setDatasetmembertypeId(2);
											  dsm.setForeignId(id);
											  dsm.setCreatedOn(new Timestamp(System.currentTimeMillis()));
											  return dsm;
										  })
										  .collect(Collectors.toList());

			startPosition = 0;

			while (startPosition < dsMembers.size())
			{
				List<DatasetmembersRecord> batch = dsMembers.subList(startPosition, startPosition + Math.min(10000, dsMembers.size() - startPosition));
				startPosition += batch.size();

				context.batchStore(batch)
					   .execute();
			}

			FJTabbedToHdf5Converter converter = new FJTabbedToHdf5Converter(input, hdf5);
			// Tell it to skip the map definition. It skips the other headers automatically anyway.
			converter.setSkipLines(2);
			converter.convertToHdf5();

			// Now set it to be public. Everything has been imported successfully.
			dataset.setDatasetStateId(1);
			dataset.store(DATASETS.DATASET_STATE_ID);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void insertMarkers(DSLContext context, Map<MarkersRecord, Integer> newDataToIndices)
	{
		context.batchStore(newDataToIndices.keySet())
			   .execute();

		markerToId = context.selectFrom(MARKERS)
							.fetchMap(MARKERS.MARKER_NAME, MARKERS.ID);

		for (Map.Entry<MarkersRecord, Integer> entry : newDataToIndices.entrySet())
		{
			markerIds[entry.getValue()] = markerToId.get(entry.getKey().getMarkerName());
			markerIdsInFile.add(markerIds[entry.getValue()]);
		}
	}

	public void setDataset(DatasetsRecord dataset)
	{
		this.dataset = dataset;
	}

	protected void updateFile()
	{
		// We don't update, just import
		importFile();
	}
}
