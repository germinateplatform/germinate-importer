package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.MapoverlaysReferenceTable;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableImages;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.StringUtils;
import jhi.germinate.server.util.importer.util.GdalUtils;
import org.apache.commons.io.FileUtils;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.math.*;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Climates.*;
import static jhi.germinate.server.database.codegen.tables.Datasetfileresources.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Fileresources.*;
import static jhi.germinate.server.database.codegen.tables.Fileresourcetypes.*;
import static jhi.germinate.server.database.codegen.tables.Mapoverlays.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;

/**
 * @author Sebastian Raubach
 */
public class GeotiffImporter extends AbstractImporter
{
	private final Set<Integer>                 datasetIds      = new TreeSet<>();
	private final Map<String, Integer>         climateNameToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>         traitNameToId   = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, ViewTableImages> filenameToImage = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private Path         templateUnzipped;
	private Set<Integer> referencedDsIds = new TreeSet<>();

	public static void main(String[] args)
	{
		if (args.length != 6) throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		GeotiffImporter importer = new GeotiffImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public GeotiffImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(DATASETS).forEach(d -> datasetIds.add(d.getId()));
			context.selectFrom(CLIMATES).forEach(g -> climateNameToId.put(g.getName(), g.getId()));
			context.selectFrom(PHENOTYPES).forEach(g -> traitNameToId.put(g.getName(), g.getId()));
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void checkFile()
	{
		File zipFile = getInputFile();

//		String prefix = zipFile.getAbsolutePath().replace("\\", "/");
//		if (prefix.startsWith("/")) prefix = prefix.substring(1);
//		URI uri = URI.create("jar:file:/" + prefix);

		try (FileSystem fs = FileSystems.newFileSystem(zipFile.toPath(), null))
		{
			Path template = fs.getPath("geotiffs.xlsx");

			if (!Files.exists(template))
			{
				addImportResult(ImportStatus.IMAGE_TEMPLATE_MISSING, -1, "Image template file missing. Be sure to include the template file inside the .zip file using the name 'geotiffs.xlsx'.");
			}
			else
			{
				templateUnzipped = Files.createTempFile("geotiff-template", ".xlsx");
				Files.copy(template, templateUnzipped, StandardCopyOption.REPLACE_EXISTING);

				try (ReadableWorkbook wb = new ReadableWorkbook(templateUnzipped.toFile()))
				{
					try
					{
						wb.getSheets().filter(s -> Objects.equals(s.getName(), "IMAGES")).findFirst().ifPresent(s -> {
							try
							{
								List<Row> rows = s.read();
								Row headers = rows.get(0);
								if (headers.getCellCount() < 6)
								{
									addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Check colum headers. Invalid number of columns found.");
								}
								else
								{
									String datasetId = getCellValue(headers, 0);
									String id = getCellValue(headers, 1);
									String name = getCellValue(headers, 2);
									String reference = getCellValue(headers, 3);
									String filename = getCellValue(headers, 4);
									String description = getCellValue(headers, 5);
									String recordingDate = getCellValue(headers, 6);

									if (!Objects.equals("Dataset Id", datasetId))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Dataset Id' not found at position 1.");
									if (!Objects.equals("Reference Id", id))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Reference Id' not found at position 2.");
									if (!Objects.equals("Reference Name", name))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Reference Name' not found at position 3.");
									if (!Objects.equals("Image reference type", reference))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image reference type' not found at position 4.");
									if (!Objects.equals("Image filename", filename))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image filename' not found at position 5.");
									if (!Objects.equals("Image description", description))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image description' not found at position 6.");
									if (!Objects.equals("Recording Date", recordingDate))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Recording Date not found at position 7.");

									for (int i = 1; i < rows.size(); i++)
									{
										Row current = rows.get(i);

										datasetId = getCellValue(current, 0);
										id = getCellValue(current, 1);
										name = getCellValue(current, 2);
										reference = getCellValue(current, 3);
										filename = getCellValue(current, 4);
										description = getCellValue(current, 5);
										recordingDate = getCellValue(current, 6);

										Integer intId = null;

										if (!StringUtils.isEmpty(datasetId))
										{
											try
											{
												if (!datasetIds.contains(Integer.parseInt(datasetId)))
													addImportResult(ImportStatus.GENERIC_INVALID_REFERENCE, i, "A specified 'Dataset Id' has not been found in the database. Only reference existing datasets.");
											}
											catch (Exception e)
											{
												addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, i, "A specified 'Dataset Id' is not a valid number. Please only specify numeric ids.");
											}
										}
										if (!StringUtils.isEmpty(reference))
										{
											try
											{
												MapoverlaysReferenceTable.valueOf(reference);
											}
											catch (IllegalArgumentException e)
											{
												addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, i, "Invalid 'Image reference type': " + reference + ". Valid values are: " + Arrays.toString(MapoverlaysReferenceTable.values()));
											}

											if (StringUtils.isEmpty(id) && StringUtils.isEmpty(name))
												addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, i, "Either 'Reference Id' or 'Reference Name' have to be defined if 'Image reference type' is selected.");
											else
											{
												if (!StringUtils.isEmpty(id))
												{
													try
													{
														intId = Integer.parseInt(id);

														try
														{
															boolean contains = false;
															switch (reference)
															{
																case "climates":
																	contains = climateNameToId.containsValue(intId);
																	break;
																case "phenotypes":
																	contains = traitNameToId.containsValue(intId);
																	break;
															}

															if (!contains)
																addImportResult(ImportStatus.GENERIC_INVALID_REFERENCE, i, "A referenced id is not valid. Make sure to only reference database ids that actually exist: " + intId);
														}
														catch (Exception e)
														{
															// Ignore, we've handled this earlier already
														}
													}
													catch (NumberFormatException e)
													{
														e.printStackTrace();
														addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, i, "The specified 'Reference Id' is not of type integer: " + id);
													}
												}
												if (!StringUtils.isEmpty(name))
												{
													try
													{
														boolean contains = false;
														switch (reference)
														{
															case "climates":
																contains = climateNameToId.containsKey(name);
																break;
															case "phenotypes":
																contains = traitNameToId.containsKey(name);
																break;
														}

														if (!contains)
															addImportResult(ImportStatus.GENERIC_INVALID_REFERENCE, i, "A referenced name is not valid. Make sure to only reference database names that actually exist: " + name);
													}
													catch (Exception e)
													{
														// Ignore, we've handled this earlier already
													}
												}
											}
										}
										if (StringUtils.isEmpty(filename))
											addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, i, "Required value 'Image filename' not found. Please specify the image filename (just the filename not full path).");

										if (filenameToImage.containsKey(filename))
											addImportResult(ImportStatus.GENERIC_DUPLICATE_VALUE, i, "An image filename has been specified more than once: " + filename);
										else filenameToImage.put(filename, null);

										if (!StringUtils.isEmpty(recordingDate))
										{
											Date date = getCellValueDate(current, 6);

											if (date == null)
												addImportResult(ImportStatus.GENERIC_INVALID_DATE, i, "An invalid 'Recording Date' has been specified. Please use YYYYMMDD format and set the cell type to 'Text'.");
										}
									}
								}
							}
							catch (IOException e)
							{
								addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
								e.printStackTrace();
							}
							catch (NullPointerException e)
							{
								addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Missing header row.");
								e.printStackTrace();
							}
						});
					}
					catch (NullPointerException e)
					{
						addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'IMAGES' sheet not found");
					}
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}
			}

			// Keep track of all files in the zip container
			List<String> filenames = new ArrayList<>();

			// Iterate through the zip
			for (Path root : fs.getRootDirectories())
			{
				Files.walk(root, 1).forEachOrdered(image -> {
					if (Files.isRegularFile(image))
					{
						String filename = image.getFileName().toString();

						if (!Objects.equals(filename, template.getFileName().toString()))
						{
							// Check if the image has a template definition
							if (!filenameToImage.containsKey(filename))
							{
								addImportResult(ImportStatus.IMAGE_DEFINITION_MISSING, -1, "Image template definition missing for image: '" + filename + "'.");
							}

							// Remember the filename
							filenames.add(filename);
						}
					}
				});
			}

			// Check for each entry in the template, whether the file exists in the zip container
			for (Map.Entry<String, ViewTableImages> entry : filenameToImage.entrySet())
			{
				if (!filenames.contains(entry.getKey()))
					addImportResult(ImportStatus.IMAGE_IMAGE_MISSING, -1, "Image not found for template definition: '" + entry.getValue().getImageForeignId() + "-" + entry.getValue().getReferenceName());
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
		File zipFile = getInputFile();

//		String prefix = zipFile.getAbsolutePath().replace("\\", "/");
//		if (prefix.startsWith("/")) prefix = prefix.substring(1);
//		URI uri = URI.create("jar:file:/" + prefix);

		try (FileSystem fs = FileSystems.newFileSystem(zipFile.toPath(), null))
		{
			Path template = fs.getPath("geotiffs.xlsx");

			if (!Files.exists(template))
			{
				addImportResult(ImportStatus.IMAGE_TEMPLATE_MISSING, -1, "Image template file missing. Be sure to include the template file inside the .zip file using the name 'geotiffs.xlsx'.");
			}
			else
			{
				try (Connection conn = Database.getConnection())
				{
					DSLContext context = Database.getContext(conn);
					templateUnzipped = Files.createTempFile("geotiff-template", "xlsx");
					Files.copy(template, templateUnzipped, StandardCopyOption.REPLACE_EXISTING);

					File imageFolder = new File(new File(jobDetails.getJobConfig().getBaseFolder(), "images"), "mapoverlay");
					imageFolder.mkdirs();

					try (ReadableWorkbook wb = new ReadableWorkbook(templateUnzipped.toFile()))
					{
						try
						{
							wb.getSheets().filter(s -> Objects.equals(s.getName(), "IMAGES")).findFirst().ifPresent(s -> {
								try
								{
									List<Row> rows = s.read();

									for (int i = 1; i < rows.size(); i++)
									{
										Row current = rows.get(i);

										String datasetId = getCellValue(current, 0);
										String id = getCellValue(current, 1);
										String name = getCellValue(current, 2);
										String reference = getCellValue(current, 3);
										String filename = getCellValue(current, 4);
										String description = getCellValue(current, 5);
										if (StringUtils.isEmpty(description)) description = filename;
										Date recordingDate = getCellValueDate(current, 6);

										Integer intId = null;
										Integer intDsId = null;
										MapoverlaysReferenceTable ref = null;

										try
										{
											ref = MapoverlaysReferenceTable.valueOf(reference);
										}
										catch (Exception e)
										{
											// Do nothing here
										}

										try
										{
											intDsId = Integer.parseInt(datasetId);

											referencedDsIds.add(intDsId);
										}
										catch (NullPointerException e)
										{
											// Do nothing here, it just doesn't exist
										}

										try
										{
											intId = Integer.parseInt(id);
										}
										catch (NullPointerException | NumberFormatException e)
										{
											if (ref != null)
											{
												switch (ref)
												{
													case climates:
														intId = climateNameToId.get(name);
														break;
													case phenotypes:
														intId = traitNameToId.get(name);
														break;
												}
											}
										}

										String uuid = UUID.randomUUID().toString();
										Path tempFolder = Files.createTempDirectory("geotiff");
										Path source = fs.getPath(filename);
										Path target = new File(tempFolder.toFile(), uuid + "-" + filename).toPath();
										Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

										File warped = new File(tempFolder.toFile(), uuid + "-warped-" + filename);
										File png = new File(tempFolder.toFile(), uuid + "-warped-" + filename + ".png");

										try
										{
											GdalUtils.gdalwarp(target.toFile(), warped);
											GdalUtils.gdal_translate(warped, png);
											GdalUtils.GdalInfo info = GdalUtils.gdalinfo(png);

											// Create a new database entry
											MapoverlaysRecord record = context.newRecord(MAPOVERLAYS);
											record.setDatasetId(intDsId);
											record.setForeignId(intId);
											record.setReferenceTable(ref);
											record.setName(filename);
											record.setDescription(description);
											record.setBottomLeftLat(new BigDecimal(info.getWgs84Extent().getCoordinates()[0][1][1], MathContext.DECIMAL64));
											record.setBottomLeftLng(new BigDecimal(info.getWgs84Extent().getCoordinates()[0][1][0], MathContext.DECIMAL64));
											record.setTopRightLat(new BigDecimal(info.getWgs84Extent().getCoordinates()[0][3][1], MathContext.DECIMAL64));
											record.setTopRightLng(new BigDecimal(info.getWgs84Extent().getCoordinates()[0][3][0], MathContext.DECIMAL64));
											if (recordingDate != null)
												record.setRecordingDate(new Timestamp(recordingDate.getTime()));
											record.setCreatedOn(new Timestamp(System.currentTimeMillis()));
											record.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
											record.store();

											// Copy the file
											File imageTarget = new File(imageFolder, record.getId() + ".png");
											Files.copy(png.toPath(), imageTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
										}
										catch (InterruptedException e)
										{
											addImportResult(ImportStatus.GENERIC_IO_ERROR, i, e.getMessage());
										}

										FileUtils.deleteDirectory(tempFolder.toFile());
									}
								}
								catch (IOException e)
								{
									addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
									e.printStackTrace();
								}
							});
						}
						catch (NullPointerException e)
						{
							addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'IMAGES' sheet not found");
							e.printStackTrace();
						}
					}
				}
			}
		}
		catch (IOException | SQLException e)

		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
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
		// Create a backup copy of the uploaded file and link it to the newly created dataset.
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			FileresourcetypesRecord type = context.selectFrom(FILERESOURCETYPES).where(FILERESOURCETYPES.NAME.eq("Dataset resource")).and(FILERESOURCETYPES.DESCRIPTION.eq("Automatically created linked backups of uploaded data resources.")).fetchAny();

			if (type == null)
			{
				type = context.newRecord(FILERESOURCETYPES);
				type.setName("Dataset resource");
				type.setDescription("Automatically created linked backups of uploaded data resources.");
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
			fileRes.setDescription("Automatic upload backup.");
			fileRes.setFileresourcetypeId(type.getId());
			fileRes.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.store();

			// Now update the name with the file resource id
			target = new File(typeFolder, fileRes.getId() + "-" + source.getName());
			fileRes.setPath(target.getName());
			fileRes.store();

			for (Integer dsId : referencedDsIds)
			{
				DatasetfileresourcesRecord link = context.newRecord(DATASETFILERESOURCES);
				link.setDatasetId(dsId);
				link.setFileresourceId(fileRes.getId());
				link.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				link.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
				link.store();
			}

			// Finally copy the file
			Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
		catch (SQLException | IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Failed to create file resource for dataset: " + e.getMessage());
		}
	}

	private String getCellValue(Row r, Integer index)
	{
		if (index == null) return null;

		try
		{
			String value = r.getCellText(index).replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");

			if (Objects.equals(value, "")) return null;
			else return value.trim();
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Date getCellValueDate(Row r, int index)
	{
		String value = getCellValue(r, index);

		java.util.Date date = null;
		if (!StringUtils.isEmpty(value))
		{
			if (!StringUtils.isEmpty(value))
			{
				if (value.length() == 10)
				{
					try
					{
						date = SDF_FULL_DASH.parse(value);
					}
					catch (Exception e)
					{
					}
				}
				else
				{
					// Replace all hyphens with zeros so that we only have one case to handle.
					value = value.replace("-", "0");

					try
					{
						boolean noMonth = value.startsWith("00", 4);
						boolean noDay = value.startsWith("00", 6);

						if (noDay && noMonth)
							date = SDF_YEAR.parse(value.substring(0, 4));
						else if (noDay)
							date = SDF_YEAR_MONTH.parse(value.substring(0, 6));
						else if (noMonth)
							date = SDF_YEAR_DAY.parse(value.substring(0, 4) + value.substring(6, 8));
						else
							date = SDF_FULL.parse(value);
					}
					catch (Exception e)
					{
					}
				}
			}
		}

		if (date != null)
			return new Date(date.getTime());
		else
			return null;
	}
}
