package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import jhi.germinate.server.util.importer.task.*;
import org.jooq.DSLContext;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Experiments.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Mapfeaturetypes.*;
import static jhi.germinate.server.database.codegen.tables.Maps.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;
import static jhi.germinate.server.database.codegen.tables.Markertypes.*;

public class GenotypeHapmapImporter extends AbstractFlatFileImporter
{
	private final Map<String, Integer> germplasmToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer> markerToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> markerIdsInFile    = new HashSet<>();
	private final Set<Integer> germplasmIdsInFile = new HashSet<>();

	private final File hdf5TargetFolder;

	private       DatasetsRecord dataset;
	private final int            datasetStateId;

	public static void main(String[] args)
	{
		if (args.length != 12)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		GenotypeHapmapImporter importer = new GenotypeHapmapImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Integer.parseInt(args[10]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]), new File(args[11]));
		importer.init(args);
		importer.run(AbstractExcelImporter.RunType.getType(args[8]));
	}

	public GenotypeHapmapImporter(File input, boolean isUpdate, int datasetStateId, boolean deleteOnFail, Integer userId, File hdf5TargetFolder)
	{
		super(input, isUpdate, deleteOnFail, userId);

		this.datasetStateId = datasetStateId;
		this.hdf5TargetFolder = hdf5TargetFolder;
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> germplasmToId.put(g.getName(), g.getId()));
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
	protected void checkFile(BufferedReader br)
	{
		try
		{
			String line = br.readLine();

			if (!line.startsWith("rs#\talleles\tchrom\tpos\tstrand\tassembly#\tcenter\tprotLSID\tassayLSID\tpanelLSID\tQCcode"))
				addImportResult(ImportStatus.GENOTYPE_HAPMAP_INCORRECT_HEADER, 1, "Hapmap header wrong incorrect");

			String[] parts = line.split("\t", -1);
			int headerLength = parts.length;

			String[] germplasm = Arrays.copyOfRange(parts, 11, parts.length);

			for (String g : germplasm)
			{
				if (!germplasmToId.containsKey(g))
					addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, 1, g);
			}

			int counter = 1;
			while ((line = br.readLine()) != null)
			{
				counter++;
				parts = line.split("\t", -1);

				if (parts.length != headerLength)
					addImportResult(ImportStatus.GENOTYPE_HAPMAP_INCORRECT_ROW_LENGTH, counter, parts[0]);

				String markerName = parts[0];
				String position = parts[3];

				if (StringUtils.isEmpty(markerName))
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, counter, "Marker name missing.");
				if (!StringUtils.isEmpty(position))
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
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void importFile(BufferedReader br)
	{
		// We need to navigate to the correct location for the resulting hdf5 file
		File hdf5 = new File(this.hdf5TargetFolder, this.getInputFile().getName() + ".hdf5");
		hdf5.getParentFile().mkdirs();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			String line = br.readLine();

			String[] parts = line.split("\t", -1);

			String[] germplasm = Arrays.copyOfRange(parts, 11, parts.length);

			for (String g : germplasm)
				germplasmIdsInFile.add(germplasmToId.get(g));

			List<String> markers = new ArrayList<>();
			List<String> chromosomes = new ArrayList<>();
			List<String> positions = new ArrayList<>();

			while ((line = br.readLine()) != null)
			{
				parts = line.split("\t", -1);
				String markerName = parts[0];
				String chromosome = parts[2];
				String position = parts[3];

				markers.add(markerName);
				chromosomes.add(chromosome);
				positions.add(position);
			}

			String markerTypeName = "SNP";
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

			String fileName = getInputFile().getName();

			MapsRecord map = context.newRecord(MAPS);
			map.setName(fileName);
			map.setDescription(fileName);
			map.setVisibility(true);
			map.setUserId(userId);
			map.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			map.store();

			// Import markers
			int markerTypeId = markerType.getId();
			List<String> newMarkers = markers.stream().filter(m -> !markerToId.containsKey(m)).collect(Collectors.toList());

			// Run the marker importer synchronously (we need the markers before we can continue
			new MarkerImporterTask(newMarkers, markerTypeId, this::addImportResult).run();

			// Now get the marker ids
			markerToId.clear();
			context.selectFrom(MARKERS)
				   .forEach(m -> markerToId.put(m.getMarkerName(), m.getId()));

			int[] markerIds = new int[markers.size()];

			for (int i = 0; i < markers.size(); i++)
			{
				String marker = markers.get(i);
				Integer id = markerToId.get(marker);

				markerIds[i] = id;
				markerIdsInFile.add(id);
			}

			ExperimentsRecord experiment = context.selectFrom(EXPERIMENTS)
												  .where(EXPERIMENTS.EXPERIMENT_NAME.eq(fileName))
												  .fetchAny();

			if (experiment == null)
			{
				experiment = context.newRecord(EXPERIMENTS);
				experiment.setExperimentName(fileName);
				experiment.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				experiment.store();
			}

			if (dataset == null)
			{
				dataset = context.newRecord(DATASETS);
				dataset.setExperimentId(experiment.getId());
				dataset.setDatasettypeId(1);
				dataset.setDatasetStateId(datasetStateId);
				// Hide it initially. We don't want people using half-imported data.
				dataset.setDatasetStateId(3);
				dataset.setName(fileName);
				dataset.setDescription(fileName);
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

			CountDownLatch latch = new CountDownLatch(3);

			if (!CollectionUtils.isEmpty(positions) && !CollectionUtils.isEmpty(chromosomes))
			{
				// Start the mapdefinition importer
				new Thread(new MapdefinitionImporterTask(
					markers.toArray(new String[0]),
					markerIds,
					map.getId(),
					mapFeatureType.getId(),
					chromosomes.toArray(new String[0]),
					positions.toArray(new String[0]),
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

			// Convert the Flapjack file to HDF5
			new Thread(new HapmapToHdf5Task(this.getInputFile(), hdf5, this::addImportResult)
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

	@Override
	protected void updateFile(BufferedReader br)
	{
		// We don't update, just import
		this.importFile(br);
	}
}
