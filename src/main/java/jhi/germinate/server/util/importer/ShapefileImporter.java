package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.*;
import org.geotools.data.*;
import org.geotools.feature.*;
import org.jooq.DSLContext;
import org.opengis.feature.Property;
import org.opengis.feature.simple.*;
import org.opengis.filter.Filter;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.zip.*;

import static jhi.germinate.server.database.codegen.tables.Datasetfileresources.DATASETFILERESOURCES;
import static jhi.germinate.server.database.codegen.tables.Fileresources.FILERESOURCES;
import static jhi.germinate.server.database.codegen.tables.Fileresourcetypes.FILERESOURCETYPES;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;

/**
 * @author Sebastian Raubach
 */
public class ShapefileImporter extends AbstractImporter
{
	private final Set<String> accenumbs = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

	public static void main(String[] args)
	{
		if (args.length != 6) throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		ShapefileImporter importer = new ShapefileImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public ShapefileImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			context.select(GERMINATEBASE.NAME, GERMINATEBASE.ID, TRIALSETUP.TRIAL_ROW, TRIALSETUP.TRIAL_COLUMN)
				   .from(TRIALSETUP)
				   .leftJoin(GERMINATEBASE).on(GERMINATEBASE.ID.eq(TRIALSETUP.GERMINATEBASE_ID))
				   .where(TRIALSETUP.DATASET_ID.eq(jobDetails.getJobConfig().getTargetDatasetId()))
				   .forEach(g -> accenumbs.add(g.get(TRIALSETUP.TRIAL_ROW) + "|" + g.get(TRIALSETUP.TRIAL_COLUMN) + "|" + g.get(GERMINATEBASE.NAME)));
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void extractZip(File zipFile, File folder)
	{
		//buffer for read and write data to file
		byte[] buffer = new byte[1024];
		try (FileInputStream fis = new FileInputStream(zipFile);
			 ZipInputStream zis = new ZipInputStream(fis);)
		{
			ZipEntry ze = zis.getNextEntry();
			while (ze != null)
			{
				String fileName = ze.getName();
				File newFile = new File(folder, fileName);
				// Delete the file if it exists
				if (newFile.exists())
					newFile.delete();
				//create directories for sub directories in zip
				new File(newFile.getParent()).mkdirs();
				try (FileOutputStream fos = new FileOutputStream(newFile))
				{
					int len;
					while ((len = zis.read(buffer)) > 0)
						fos.write(buffer, 0, len);
				}
				//close this ZipEntry
				zis.closeEntry();
				ze = zis.getNextEntry();
			}
			//close last ZipEntry
			zis.closeEntry();
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	protected void checkFile()
	{
		// Extract the zip file
		File zipFile = getInputFile();
		File folder = zipFile.getParentFile();
		this.extractZip(zipFile, folder);

		File[] shp = folder.listFiles(file -> file.getName().endsWith(".shp"));

		if (CollectionUtils.isEmpty(shp))
		{
			addImportResult(ImportStatus.SHAPEFILE_MISSING_SHP, -1, "Missing shape file in zip container.");
			return;
		}

		try
		{
			Map<String, Object> map = new HashMap<>();
			map.put("url", shp[0].toURI().toURL());

			DataStore dataStore = DataStoreFinder.getDataStore(map);
			String typeName = dataStore.getTypeNames()[0];

			FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
			Set<String> keys = new HashSet<>(Arrays.asList("germplasm", "row", "column"));

			FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(Filter.INCLUDE);
			try (FeatureIterator<SimpleFeature> features = collection.features())
			{
				while (features.hasNext())
				{
					SimpleFeature feature = features.next();

					Collection<Property> properties = feature.getProperties();
					Map<String, String> matchingProps = new HashMap<>();

					// Get required properties
					properties.forEach(p -> {
						String key = p.getName() != null ? p.getName().toString() : null;
						String value = p.getValue() != null ? p.getValue().toString() : null;
						if (!StringUtils.isEmpty(key) && !StringUtils.isEmpty(value) && keys.contains(p.getName().toString()))
							matchingProps.put(p.getName().toString(), p.getValue().toString());
					});

					String row = matchingProps.get("row");
					String col = matchingProps.get("column");
					String germplasm = matchingProps.get("germplasm");
					Short numRow = null;
					Short numCol = null;

					// Check if germplasm exists, send warning if not
					if (StringUtils.isEmpty(germplasm))
					{
						addImportResult(ImportStatus.SHAPEFILE_WARNING_MISSING_ACCENUMB, -1, "Missing germplasm in row: " + row + ", column: " + col, ImportResult.StatusType.WARNING);
						continue;
					}

					// Check row
					if (StringUtils.isEmpty(row))
					{
						addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, "Row field not found. Make sure that each shape has a `row` field (mind lowercase).");
					}
					else
					{
						try
						{
							numRow = Short.parseShort(row);
						}
						catch (Exception e)
						{
							addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, -1, "Invalid row data type. Please only use integers.");
						}
					}

					// Check column
					if (StringUtils.isEmpty(col))
					{
						addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, "Column field not found. Make sure that each shape has a `column` field (mind lowercase).");
					}
					else
					{
						try
						{
							numCol = Short.parseShort(col);
						}
						catch (Exception e)
						{
							addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, -1, "Invalid column data type. Please only use integers.");
						}
					}

					boolean match = accenumbs.contains(numRow + "|" + numCol + "|" + germplasm);

					// Check the row and column indices match
					if (!match)
						addImportResult(ImportStatus.SHAPEFILE_ROW_COL_GERMPLASM_CONFLICT, -1, "A conflict between the (`germplasm`, `row`, `column`) tuple in the shapefile and the trials data setup has been found. Please correct this inconsistency: " + numRow + "|" + numCol + "|" + germplasm);
				}
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void importFile()
	{
		// Extract the zip file
		File zipFile = getInputFile();
		File folder = zipFile.getParentFile();
		this.extractZip(zipFile, folder);

		// Create a backup copy of the uploaded file and link it to the newly created dataset.
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			FileresourcetypesRecord type = context.selectFrom(FILERESOURCETYPES)
												  .where(FILERESOURCETYPES.NAME.eq("Trials Shapefile"))
												  .and(FILERESOURCETYPES.DESCRIPTION.eq("Shape file associated with a phenotypic trial. Fields within the shape file have to match the database entries."))
												  .fetchAny();

			if (type == null)
			{
				type = context.newRecord(FILERESOURCETYPES);
				type.setName("Trials Shapefile");
				type.setDescription("Shape file associated with a phenotypic trial. Fields within the shape file have to match the database entries.");
				type.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				type.store();
			}

			File source = getInputFile();
			File typeFolder = new File(new File(new File(jobDetails.getJobConfig().getBaseFolder(), "data"), "download"), Integer.toString(type.getId()));
			typeFolder.mkdirs();
			File target = new File(typeFolder, source.getName());

			FileresourcesRecord fileRes = context.newRecord(FILERESOURCES);
			fileRes.setName(this.jobDetails.getOriginalFilename());
			fileRes.setPath(target.getName());
			fileRes.setFilesize(source.length());
			fileRes.setDescription("Shapefile associated with trials dataset.");
			fileRes.setFileresourcetypeId(type.getId());
			fileRes.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.store();

			// Now update the name with the file resource id
			target = new File(typeFolder, fileRes.getId() + "-" + source.getName());
			fileRes.setPath(target.getName());
			fileRes.store();

			importJobStats.setFileResourceId(fileRes.getId());

			// Finally copy the file
			Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);

			DatasetfileresourcesRecord dsf = context.newRecord(DATASETFILERESOURCES);
			dsf.setDatasetId(jobDetails.getJobConfig().getTargetDatasetId());
			dsf.setFileresourceId(fileRes.getId());
			dsf.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			dsf.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			dsf.store();
		}
		catch (SQLException | IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Failed to create file resource for dataset: " + e.getMessage());
		}
	}

	@Override
	protected void updateFile()
	{
		// We don't update
		importFile();
	}

	@Override
	protected void postImport()
	{
	}
}
