package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import jhi.germinate.server.util.importer.task.*;
import jhi.germinate.server.util.importer.util.GermplasmNotFoundException;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasetfileresources.DATASETFILERESOURCES;
import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Experiments.EXPERIMENTS;
import static jhi.germinate.server.database.codegen.tables.Fileresources.FILERESOURCES;
import static jhi.germinate.server.database.codegen.tables.Fileresourcetypes.FILERESOURCETYPES;
import static jhi.germinate.server.database.codegen.tables.Mapfeaturetypes.MAPFEATURETYPES;
import static jhi.germinate.server.database.codegen.tables.Maps.MAPS;
import static jhi.germinate.server.database.codegen.tables.Markers.MARKERS;
import static jhi.germinate.server.database.codegen.tables.Markertypes.MARKERTYPES;

/**
 * @author Sebastian Raubach
 */
public class GenotypeFlatFileImporter extends AbstractFlatFileImporter
{
	private final Map<String, Integer> markerToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private       File                 hdf5TargetFolder;

	private       String[]            markers       = null;
	private       int[]               markerIds     = null;
	private       String[]            chromosomes   = null;
	private       String[]            positions     = null;
	private final Map<String, String> headerMapping = new HashMap<>();

	private final Set<Integer> markerIdsInFile    = new HashSet<>();
	private final Set<Integer> germplasmIdsInFile = new HashSet<>();

	private DatasetsRecord dataset;

	private CountDownLatch latch;

	private GermplasmLookup germplasmLookup;

	public static void main(String[] args)
			throws SQLException, IOException
	{
		GenotypeFlatFileImporter importer;

		if (args.length == 6)
		{
			importer = new GenotypeFlatFileImporter(Integer.parseInt(args[5]));
		}
		else if (args.length == 9)
		{
			importer = new GenotypeFlatFileImporter(createImportJobFromCommandline(args, DataImportJobsDatatype.genotype));
		}
		else
		{
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));
		}

		importer.init(args);
		importer.run();
	}

	public GenotypeFlatFileImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		germplasmLookup = new GermplasmLookup();

		this.hdf5TargetFolder = new File(new File(this.jobDetails.getJobConfig().getBaseFolder(), "data"), "genotypes");
		this.hdf5TargetFolder.mkdirs();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(MARKERS)
				   .forEach(m -> markerToId.put(m.getMarkerName(), m.getId()));
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void postImport()
	{
		File input = getInputFile();
		// Create a backup copy of the uploaded file and link it to the newly created dataset.
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			FileresourcetypesRecord type = context.selectFrom(FILERESOURCETYPES)
												  .where(FILERESOURCETYPES.NAME.eq("Dataset resource"))
												  .and(FILERESOURCETYPES.DESCRIPTION.eq("Automatically created linked backups of uploaded data resources."))
												  .fetchAny();

			if (type == null)
			{
				type = context.newRecord(FILERESOURCETYPES);
				type.setName("Dataset resource");
				type.setDescription("Automatically created linked backups of uploaded data resources.");
				type.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				type.store();
			}

			File typeFolder = new File(new File(new File(jobDetails.getJobConfig().getBaseFolder(), "data"), "download"), Integer.toString(type.getId()));
			typeFolder.mkdirs();
			File target = new File(typeFolder, input.getName());

			FileresourcesRecord fileRes = context.newRecord(FILERESOURCES);
			fileRes.setName(this.jobDetails.getOriginalFilename());
			fileRes.setPath(target.getName());
			fileRes.setFilesize(input.length());
			fileRes.setDescription("Automatic upload backup.");
			fileRes.setFileresourcetypeId(type.getId());
			fileRes.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.store();

			// Now update the name with the file resource id
			target = new File(typeFolder, fileRes.getId() + "-" + input.getName());
			fileRes.setPath(target.getName());
			fileRes.store();

			importJobStats.setFileResourceId(fileRes.getId());

			// Finally copy the file
			Files.copy(input.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);

			DatasetfileresourcesRecord link = context.newRecord(DATASETFILERESOURCES);
			link.setDatasetId(this.dataset.getId());
			link.setFileresourceId(fileRes.getId());
			link.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			link.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			link.store();
		}
		catch (SQLException | IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Failed to create file resource for dataset: " + e.getMessage());
		}

		importJobStats.setDatasetId(dataset.getId());
		importJobStats.setGermplasm(germplasmIdsInFile.size());
		importJobStats.setMarkers(markerIdsInFile.size());
	}

	@Override
	protected void checkFile(BufferedReader br)
	{
		try
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
						Double.parseDouble(position);
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

				try
				{
					germplasmLookup.getGermplasmId(germplasm);
				}
				catch (GermplasmNotFoundException e)
				{
					addImportResult(e.getReason(), counter, germplasm);
				}

				counter++;
			}
			while ((line = br.readLine()) != null);
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
		headerMapping.put("dataset", this.getInputFile().getName());
		headerMapping.put("map", this.getInputFile().getName());
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
		while (markers == null && line != null)
		{
			if (line.startsWith("Linkage Group / Chromosome"))
				chromosomes = line.replace("Linkage Group / Chromosome\t", "").split("\t", -1);
			else if (line.startsWith("Position"))
				positions = line.replace("Position\t", "").split("\t", -1);
			else if (line.startsWith("Lines/Markers"))
			{
				markers = line.replace("Lines/Markers\t", "").split("\t", -1);
				markerIds = new int[markers.length];
			}

			line = br.readLine();
		}

		if (chromosomes == null)
			chromosomes = new String[0];
		if (positions == null)
			positions = new String[0];

		return line;
	}

	@Override
	protected void importFile(BufferedReader br)
	{
		// We need to navigate to the correct location for the resulting hdf5 file
		File hdf5 = new File(this.hdf5TargetFolder, this.getInputFile().getName() + ".hdf5");
		File hdf5Transposed = new File(this.hdf5TargetFolder, "transposed-" + this.getInputFile().getName() + ".hdf5");
		hdf5.getParentFile().mkdirs();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
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
				germplasmIdsInFile.add(germplasmLookup.getGermplasmId(germplasm));
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

			// Import markers
			int markerTypeId = markerType.getId();
			List<String> newMarkers = Arrays.stream(markers).filter(m -> !markerToId.containsKey(m)).collect(Collectors.toList());

			// Run the marker importer synchronously (we need the markers before we can continue
			new MarkerImporterTask(newMarkers, markerTypeId, this::addImportResult).run();

			// Now get the marker ids
			markerToId.clear();
			context.selectFrom(MARKERS)
				   .forEach(m -> markerToId.put(m.getMarkerName(), m.getId()));

			for (int i = 0; i < markers.length; i++)
			{
				String marker = markers[i];
				Integer id = markerToId.get(marker);

				markerIds[i] = id;
				markerIdsInFile.add(id);
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
				dataset.setDatasetStateId(this.jobDetails.getDatasetstateId());
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

			dataset.setSourceFile(hdf5.getName());
			dataset.store();

			latch = new CountDownLatch(4);

			if (!CollectionUtils.isEmpty(positions) && !CollectionUtils.isEmpty(chromosomes))
			{
				int chromosomeCount = 0;
				int positionCount = 0;

				for (int i = 0; i < chromosomes.length; i++)
				{
					if (!StringUtils.isEmpty(chromosomes[i]))
						chromosomeCount++;
					if (!StringUtils.isEmpty(positions[i]))
						positionCount++;
				}

				if (chromosomeCount > 0 && positionCount > 0)
				{
					MapsRecord map = context.newRecord(MAPS);
					map.setName(headerMapping.get("map"));
					map.setDescription(headerMapping.get("map"));
					map.setVisibility(true);
					map.setUserId(this.jobDetails.getUserId());
					map.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					map.store();

					// Start the mapdefinition importer
					new Thread(new MapdefinitionImporterTask(
							markers,
							markerIds,
							map.getId(),
							mapFeatureType.getId(),
							chromosomes,
							positions,
							this::addImportResult)
					{
						@Override
						protected void onFinished()
						{
							latch.countDown();
						}
					}).start();
				}
				else
				{
					latch.countDown();
				}
			}
			else
			{
				// If there are none, just count this as finished
				latch.countDown();
			}

			// Import the dataset members
			new Thread(new DatasetMemberImporterTask(
					markerIdsInFile,
					germplasmIdsInFile,
					dataset.getId(),
					this::addImportResult)
			{
				@Override
				protected void onFinished()
				{
					latch.countDown();
				}
			}).start();

			int skipLines = 0;
			if (chromosomes.length != 0)
				skipLines++;
			if (positions.length != 0)
				skipLines++;

			// Convert the Flapjack file to HDF5
			new Thread(new FJTabbedToHdf5Task(this.getInputFile(), hdf5, false, skipLines, this::addImportResult)
			{
				@Override
				protected void onFinished()
				{
					latch.countDown();
				}
			}).start();

			// Convert the Flapjack file to transposed HDF5
			new Thread(new FJTabbedToHdf5Task(this.getInputFile(), hdf5Transposed, true, skipLines, this::addImportResult)
			{
				@Override
				protected void onFinished()
				{
					latch.countDown();
				}
			}).start();

			try
			{
				// Wait for the others to finish
				latch.await();

				// Now set it to be public. Everything has been imported successfully.
				dataset.setDatasetStateId(1);
				dataset.store(DATASETS.DATASET_STATE_ID);
			}
			catch (InterruptedException e)
			{
				addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			}
		}
		catch (SQLException | IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	public void setDataset(DatasetsRecord dataset)
	{
		this.dataset = dataset;
	}

	@Override
	protected void updateFile(BufferedReader br)
	{
		// We don't update, just import
		this.importFile(br);
	}

	@Override
	protected int getMarkerCount()
	{
		return markerIdsInFile.size();
	}

	@Override
	protected int getGermplasmCount()
	{
		return germplasmIdsInFile.size();
	}
}
