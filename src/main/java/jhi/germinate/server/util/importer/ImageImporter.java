package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableImages;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.StringUtils;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.*;
import org.jooq.*;

import java.io.File;
import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Fileresources.*;
import static jhi.germinate.server.database.codegen.tables.Fileresourcetypes.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.ImageToTags.*;
import static jhi.germinate.server.database.codegen.tables.Images.*;
import static jhi.germinate.server.database.codegen.tables.Imagetags.*;
import static jhi.germinate.server.database.codegen.tables.Imagetypes.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;

/**
 * @author Sebastian Raubach
 */
public class ImageImporter extends AbstractImporter
{
	private final Map<String, Integer>         accenumbToId     = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>         traitNameToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, ViewTableImages> filenameToImage  = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>         tagToImageTagId  = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>         imageTypeToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> germplasmIds = new HashSet<>();
	private final Set<Integer> traitIds = new HashSet<>();

	private List<String> validReferenceTypes = Arrays.asList("germinatebase", "phenotypes");

	private Path templateUnzipped;

	public static void main(String[] args)
	{
		if (args.length != 6) throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		ImageImporter importer = new ImageImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public ImageImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE).forEach(g -> accenumbToId.put(g.getName(), g.getId()));
			context.selectFrom(PHENOTYPES).forEach(g -> traitNameToId.put(g.getName(), g.getId()));
			context.selectFrom(IMAGETAGS).forEach(g -> tagToImageTagId.put(g.getTagName(), g.getId()));
			context.selectFrom(IMAGETYPES).forEach(g -> imageTypeToId.put(g.getReferenceTable(), g.getId()));
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
			Path template = fs.getPath("images.xlsx");

			if (!Files.exists(template))
			{
				addImportResult(ImportStatus.IMAGE_TEMPLATE_MISSING, -1, "Image template file missing. Be sure to include the template file inside the .zip file using the name 'images.xlsx'.");
			}
			else
			{
				templateUnzipped = Files.createTempFile("image-template", ".xlsx");
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
									String id = getCellValue(headers, 0);
									String name = getCellValue(headers, 1);
									String reference = getCellValue(headers, 2);
									String filename = getCellValue(headers, 3);
									String description = getCellValue(headers, 4);
									String tags = getCellValue(headers, 5);

									if (!Objects.equals("Reference Id", id))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Reference Id' not found at position 1.");
									if (!Objects.equals("Reference Name", name))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Reference Name' not found at position 2.");
									if (!Objects.equals("Image reference type", reference))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image reference type' not found at position 3.");
									if (!Objects.equals("Image filename", filename))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image filename' not found at position 4.");
									if (!Objects.equals("Image description", description))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Image description' not found at position 5.");
									if (!Objects.equals("Tags (comma separated list)", tags))
										addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Column 'Tags (comma separated list)' not found at position 6.");

									for (int i = 1; i < rows.size(); i++)
									{
										Row current = rows.get(i);

										id = getCellValue(current, 0);
										name = getCellValue(current, 1);
										reference = getCellValue(current, 2);
										filename = getCellValue(current, 3);
										description = getCellValue(current, 4);
										tags = getCellValue(current, 5);

										Integer intId = null;

										if (StringUtils.isEmpty(reference) || !validReferenceTypes.contains(reference))
											addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, i, "Invalid 'Image reference type': " + reference + ". Valid values are: " + validReferenceTypes.toString());
										if (StringUtils.isEmpty(id) && StringUtils.isEmpty(name))
											addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, i, "Either 'Reference Id' or 'Reference Name' have to be defined.");
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
															case "germinatebase":
																contains = accenumbToId.containsValue(intId);
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
														case "germinatebase":
															contains = accenumbToId.containsKey(name);
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
										if (StringUtils.isEmpty(filename))
											addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, i, "Required value 'Image filename' not found. Please specify the image filename (just the filename not full path).");

										if (filenameToImage.containsKey(filename))
											addImportResult(ImportStatus.GENERIC_DUPLICATE_VALUE, i, "An image filename has been specified more than once: " + filename);
										else
											filenameToImage.put(filename, null);
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
			Path template = fs.getPath("images.xlsx");

			if (!Files.exists(template))
			{
				addImportResult(ImportStatus.IMAGE_TEMPLATE_MISSING, -1, "Image template file missing. Be sure to include the template file inside the .zip file using the name 'images.xlsx'.");
			}
			else
			{
				try (Connection conn = Database.getConnection())
				{
					DSLContext context = Database.getContext(conn);
					templateUnzipped = Files.createTempFile("image-template", "xlsx");
					Files.copy(template, templateUnzipped, StandardCopyOption.REPLACE_EXISTING);

					File imageFolder = new File(new File(new File(jobDetails.getJobConfig().getBaseFolder(), "images"), "database"), "upload");
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

										String id = getCellValue(current, 0);
										String name = getCellValue(current, 1);
										String reference = getCellValue(current, 2);
										String filename = getCellValue(current, 3);
										String description = getCellValue(current, 4);
										if (StringUtils.isEmpty(description))
											description = filename;
										String tags = getCellValue(current, 5);

										Integer intId = null;
										Integer imageTypeId = null;

										try
										{
											intId = Integer.parseInt(id);
										}
										catch (NullPointerException | NumberFormatException e)
										{
											switch (reference)
											{
												case "germinatebase":
													intId = accenumbToId.get(name);
													germplasmIds.add(intId);
													break;
												case "phenotypes":
													intId = traitNameToId.get(name);
													traitIds.add(intId);
													break;
											}
										}

										switch (reference)
										{
											case "germinatebase":
												imageTypeId = imageTypeToId.get("germinatebase");
												germplasmIds.add(intId);
												break;
											case "phenotypes":
												imageTypeId = imageTypeToId.get("phenotypes");
												traitIds.add(intId);
												break;
										}

										// Copy the file
										Path source = fs.getPath(filename);
										File imageTarget = new File(imageFolder, UUID.randomUUID().toString() + "-" + filename);
										Files.copy(source, imageTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);

										// Create a new database entry
										ImagesRecord image = context.newRecord(IMAGES);
										image.setForeignId(intId);
										image.setDescription(description);
										image.setImagetypeId(imageTypeId);
										image.setCreatedOn(new Timestamp(System.currentTimeMillis()));
										image.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
										image.setPath("upload/" + imageTarget.getName());
										image.store();

										if (!StringUtils.isEmpty(tags))
										{
											// Parse the tags, split on comma
											List<Integer> ids = Arrays.stream(tags.split(",", -1)).map(str -> {
												// Trim
												str = str.trim();
												// Check if it exists
												Integer tagId = tagToImageTagId.get(str);

												if (tagId == null)
												{
													// Create if not
													ImagetagsRecord tag = context.newRecord(IMAGETAGS);
													tag.setTagName(str);
													tag.setCreatedOn(new Timestamp(System.currentTimeMillis()));
													tag.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
													tag.store();

													// Remember for next time
													tagToImageTagId.put(str, tag.getId());
													tagId = tag.getId();
												}

												return tagId;
											}).collect(Collectors.toList());

											try (InsertValuesStep4<ImageToTagsRecord, Integer, Integer, Timestamp, Timestamp> res = context.insertInto(IMAGE_TO_TAGS, IMAGE_TO_TAGS.IMAGE_ID, IMAGE_TO_TAGS.IMAGETAG_ID, IMAGE_TO_TAGS.CREATED_ON, IMAGE_TO_TAGS.UPDATED_ON))
											{
												ids.forEach(tagId -> {
													res.values(image.getId(), tagId, new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()));
												});

												res.execute();
											}
										}
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
		catch (IOException |
			   SQLException e)

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
		importJobStats.setGermplasm(germplasmIds.size());
		importJobStats.setTraits(traitIds.size());
		importJobStats.setImages(filenameToImage.size());

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

			importJobStats.setFileResourceId(fileRes.getId());

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
}
