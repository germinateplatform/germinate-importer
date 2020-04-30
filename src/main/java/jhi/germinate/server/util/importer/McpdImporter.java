package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.*;
import org.jooq.*;

import java.io.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.enums.AttributesDatatype;
import jhi.germinate.server.database.tables.records.*;
import jhi.germinate.server.util.StringUtils;

import static jhi.germinate.server.database.tables.Attributedata.*;
import static jhi.germinate.server.database.tables.Attributes.*;
import static jhi.germinate.server.database.tables.Biologicalstatus.*;
import static jhi.germinate.server.database.tables.Collectingsources.*;
import static jhi.germinate.server.database.tables.Countries.*;
import static jhi.germinate.server.database.tables.Entitytypes.*;
import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Institutions.*;
import static jhi.germinate.server.database.tables.Locations.*;
import static jhi.germinate.server.database.tables.Pedigreedefinitions.*;
import static jhi.germinate.server.database.tables.Pedigreenotations.*;
import static jhi.germinate.server.database.tables.Storage.*;
import static jhi.germinate.server.database.tables.Storagedata.*;
import static jhi.germinate.server.database.tables.Taxonomies.*;

/**
 * @author Sebastian Raubach
 */
public class McpdImporter extends AbstractImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"PUID", "INSTCODE", "ACCENUMB", "COLLNUMB", "COLLCODE", "COLLNAME", "COLLINSTADDRESS", "COLLMISSID", "GENUS", "SPECIES", "SPAUTHOR", "SUBTAXA", "SUBTAUTHOR", "CROPNAME", "ACCENAME", "ACQDATE", "ORIGCTY", "COLLSITE", "DECLATITUDE", "LATITUDE", "DECLONGITUDE", "LONGITUDE", "COORDUNCERT", "COORDDATUM", "GEOREFMETH", "ELEVATION", "COLLDATE", "BREDCODE", "BREDNAME", "SAMPSTAT", "ANCEST", "COLLSRC", "DONORCODE", "DONORNAME", "DONORNUMB", "OTHERNUMB", "DUPLSITE", "DUPLINSTNAME", "STORAGE", "MLSSTAT", "REMARKS"};

	private Map<String, Integer>              columnNameToIndex;
	private Set<String>                       foundAccenumb = new HashSet<>();
	private List<Integer>                     attributeIds  = new ArrayList<>();
	private Map<String, Integer>              gidToId;
	private Map<String, Integer>              countryCodeToId;
	private Map<String, Integer>              attributeToId;
	private Map<String, Integer>              accenumbToId;
	private Map<Integer, GerminatebaseRecord> germinatebaseRecords;
	private Map<Integer, LocationsRecord>     locationRecords;
	private List<Integer>                     validCollsrc;
	private List<Integer>                     validSampstat;
	private List<Integer>                     validStorage;
	private Map<String, Integer>              entityTypeToId;

	public static void main(String[] args)
	{
		if (args.length != 10)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		McpdImporter importer = new McpdImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]));
		importer.init(args);
		importer.run(RunType.getType(args[8]));
	}

	public McpdImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		super(input, isUpdate, deleteOnFail, userId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			gidToId = context.selectFrom(GERMINATEBASE)
							 .fetchMap(GERMINATEBASE.GENERAL_IDENTIFIER, GERMINATEBASE.ID);

			countryCodeToId = context.selectFrom(COUNTRIES)
									 .fetchMap(COUNTRIES.COUNTRY_CODE3, COUNTRIES.ID);

			validCollsrc = context.selectFrom(COLLECTINGSOURCES)
								  .fetch(COLLECTINGSOURCES.ID);

			validSampstat = context.selectFrom(BIOLOGICALSTATUS)
								   .fetch(BIOLOGICALSTATUS.ID);

			validStorage = context.selectFrom(STORAGE)
								  .fetch(STORAGE.ID);

			entityTypeToId = context.selectFrom(ENTITYTYPES)
									.fetchMap(ENTITYTYPES.NAME, ENTITYTYPES.ID);

			attributeToId = context.selectFrom(ATTRIBUTES)
								   .fetchMap(ATTRIBUTES.NAME, ATTRIBUTES.ID);

			accenumbToId = context.selectFrom(GERMINATEBASE)
								  .fetchMap(GERMINATEBASE.NAME, GERMINATEBASE.ID);

			germinatebaseRecords = context.selectFrom(GERMINATEBASE)
										  .fetchMap(GERMINATEBASE.ID);

			locationRecords = context.selectFrom(LOCATIONS)
									 .fetchMap(LOCATIONS.ID);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			// TODO
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		try
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .findFirst()
					   .ifPresent(this::getHeaderMapping);
					  // Check the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(this::check);
					  // Check the entity parent for each row
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(this::checkEntityParent);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'DATA' sheet not found");
		}
		try
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "ADDITIONAL_ATTRIBUTES"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(this::checkGermplasm);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'ADDITIONAL_ATTRIBUTES' sheet not found");
		}
	}

	private void checkGermplasm(Row r)
	{
		String germplasm = r.getCellText(0);

		if (!StringUtils.isEmpty(germplasm) && !foundAccenumb.contains(germplasm))
			addImportResult(ImportStatus.MCPD_MISSING_ACCENUMB, r.getRowNum(), germplasm);
	}

	private void getHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			columnNameToIndex = IntStream.range(0, r.getPhysicalCellCount())
										 .boxed()
										 .collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS)
				  .forEach(c -> {
					  if (!columnNameToIndex.containsKey(c))
						  addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, -1, c);
				  });
		}
		catch (IllegalStateException e)
		{
			addImportResult(ImportStatus.GENERIC_DUPLICATE_COLUMN, 1, e.getMessage());
		}
	}

	private void checkEntityParent(Row r)
	{
		try
		{
			String entityParentAccenumb = getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB");
			if (!StringUtils.isEmpty(entityParentAccenumb))
			{
				if (!gidToId.containsKey(entityParentAccenumb) && !foundAccenumb.contains(entityParentAccenumb))
					addImportResult(ImportStatus.MCPD_INVALID_ENTITY_PARENT_ACCENUMB, r.getRowNum(), entityParentAccenumb);
			}
		}
		catch (NullPointerException e)
		{
			// We get here if the column isn't present. This can be the case in older versions of the template. Let this slide...
		}
	}

	private void check(Row r)
	{
		if (allCellsEmpty(r))
			return;

		// Check the accenumb isn't a duplicate
		boolean alreadyFoundInFile = false;
		String accenumb = getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name());
		if (StringUtils.isEmpty(accenumb))
		{
			addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), McpdField.ACCENUMB.name());
		}
		else
		{
			alreadyFoundInFile = foundAccenumb.contains(accenumb);
			foundAccenumb.add(accenumb);
		}

		// If it's not an update, but ACCENUMB found in the database OR it's a duplicate in the file, add error
		if ((gidToId.containsKey(accenumb) && !isUpdate) || alreadyFoundInFile)
			addImportResult(ImportStatus.MCPD_DUPLICATE_ACCENUMB, r.getRowNum(), accenumb);
		if (isUpdate && !gidToId.containsKey(accenumb))
			addImportResult(ImportStatus.GENERIC_MISSING_DB_ITEM_UPDATE, r.getRowNum(), accenumb);

		if (!isUpdate)
		{
			String genus = getCellValue(r, columnNameToIndex, McpdField.GENUS.name());
			String species = getCellValue(r, columnNameToIndex, McpdField.SPECIES.name());
			String subtaxa = getCellValue(r, columnNameToIndex, McpdField.SUBTAXA.name());
			// If there's a species, but no genus
			if (!StringUtils.isEmpty(species) && StringUtils.isEmpty(genus))
				addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), McpdField.GENUS.name());
			// Or there's a subtaxa, but no species
			if (!StringUtils.isEmpty(subtaxa) && StringUtils.isEmpty(species))
				addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), McpdField.SPECIES.name());
		}

		// Check the date is int he correct format
		String acqdateString = getCellValue(r, columnNameToIndex, McpdField.ACQDATE.name());
		Date acqdate = getCellValueDate(r, columnNameToIndex, McpdField.ACQDATE.name());
		// There is a date, but it couldn't be parsed
		if (!StringUtils.isEmpty(acqdateString) && acqdate == null)
		{
			addImportResult(ImportStatus.GENERIC_INVALID_DATE, r.getRowNum(), McpdField.ACQDATE.name() + ": " + acqdate);
		}

		// Check if country is a valid 3-letter code
		String countryCode = getCellValue(r, columnNameToIndex, McpdField.ORIGCTY.name());
		if (!StringUtils.isEmpty(countryCode) && !countryCodeToId.containsKey(countryCode))
			addImportResult(ImportStatus.GENERIC_INVALID_COUNTRY_CODE, r.getRowNum(), countryCode);


		// Check if declatitute is a number
		String declatitude = getCellValue(r, columnNameToIndex, McpdField.DECLATITUDE.name());
		if (!StringUtils.isEmpty(declatitude))
		{
			try
			{
				Double.parseDouble(declatitude);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.DECLATITUDE.name() + ": " + declatitude);
			}
		}

		// Check if declongitude is a number
		String declongitude = getCellValue(r, columnNameToIndex, McpdField.DECLONGITUDE.name());
		if (!StringUtils.isEmpty(declongitude))
		{
			try
			{
				Double.parseDouble(declongitude);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.DECLONGITUDE.name() + ": " + declongitude);
			}
		}

		// Check if elevation is a valid number
		String elevation = getCellValue(r, columnNameToIndex, McpdField.ELEVATION.name());
		if (!StringUtils.isEmpty(elevation))
		{
			try
			{
				Double.parseDouble(elevation);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.ELEVATION.name() + ": " + elevation);
			}
		}

		// Check the date is int he correct format
		String colldateString = getCellValue(r, columnNameToIndex, McpdField.COLLDATE.name());
		Date colldate = getCellValueDate(r, columnNameToIndex, McpdField.COLLDATE.name());
		// There is a date, but it couldn't be parsed
		if (!StringUtils.isEmpty(colldateString) && colldate == null)
		{
			addImportResult(ImportStatus.GENERIC_INVALID_DATE, r.getRowNum(), McpdField.COLLDATE.name() + ": " + colldate);
		}

		// Check SAMPSTAT
		String sampstat = getCellValue(r, columnNameToIndex, McpdField.SAMPSTAT.name());
		if (!StringUtils.isEmpty(sampstat))
		{
			try
			{
				if (!validSampstat.contains(Integer.parseInt(sampstat)))
					addImportResult(ImportStatus.MCPD_INVALID_SAMPSTAT, r.getRowNum(), sampstat);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.SAMPSTAT.name() + ": " + sampstat);
			}
		}

		// Check COLLSRC
		String collsrc = getCellValue(r, columnNameToIndex, McpdField.COLLSRC.name());
		if (!StringUtils.isEmpty(collsrc))
		{
			try
			{
				if (!validCollsrc.contains(Integer.parseInt(collsrc)))
					addImportResult(ImportStatus.MCPD_INVALID_COLLSRC, r.getRowNum(), collsrc);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.COLLSRC.name() + ": " + collsrc);
			}
		}

		// Check STORAGE
		String storage = getCellValue(r, columnNameToIndex, McpdField.STORAGE.name());
		if (!StringUtils.isEmpty(storage))
		{
			try
			{
				String[] parts = storage.split(";");

				for (int i = 0; i < parts.length; i++)
				{
					if (!validStorage.contains(Integer.parseInt(parts[i].trim())))
						addImportResult(ImportStatus.MCPD_INVALID_STORAGE, r.getRowNum(), parts[i]);
				}
			}
			catch (NumberFormatException | NullPointerException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.STORAGE.name() + ": " + storage);
			}
		}

		try
		{
			// Check entity type
			String entityType = getCellValue(r, columnNameToIndex, "Entity type");
			if (!StringUtils.isEmpty(entityType))
			{
				if (!entityTypeToId.containsKey(entityType))
					addImportResult(ImportStatus.MCPD_INVALID_ENTITY_TYPE, r.getRowNum(), entityType);
			}
		}
		catch (NullPointerException e)
		{
			// We get here if the column isn't present. This can be the case in older versions of the template. Let this slide...
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			// Import the data
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .findFirst()
					   .ifPresent(this::getHeaderMapping);
					  // Import the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(r -> insert(context, r, false));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }

				  // Set entity parents
				  try
				  {
					  // Import the sheet
					  s.openStream()
					   .skip(1)
					   .filter(r -> getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB") != null)
					   .forEachOrdered(r -> setEntityParent(context, r));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

			// Import the attributes
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "ADDITIONAL_ATTRIBUTES"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  s.openStream()
					   .findFirst()
					   .ifPresent(r -> getOrCreateAttributes(context, r));
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(r -> insertAttributeData(context, r));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .findFirst()
					   .ifPresent(this::getHeaderMapping);
					  // Update the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(r -> insert(context, r, true));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }

				  // Set entity parents
				  try
				  {
					  // Import the sheet
					  s.openStream()
					   .skip(1)
					   .filter(r -> getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB") != null)
					   .forEachOrdered(r -> setEntityParent(context, r));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	private void insertAttributeData(DSLContext context, Row r)
	{
		if (allCellsEmpty(r))
			return;

		String accenumb = getCellValue(r.getCell(0));
		Integer germplasmId = accenumbToId.get(accenumb);

		r.stream()
		 .skip(1)
		 .forEachOrdered(c -> {
			 Integer attributeId = attributeIds.get(c.getColumnIndex() - 1);
			 String value = getCellValue(c);

			 if (!StringUtils.isEmpty(value))
			 {
				 AttributedataRecord data = context.selectFrom(ATTRIBUTEDATA)
												   .where(ATTRIBUTEDATA.ATTRIBUTE_ID.eq(attributeId))
												   .and(ATTRIBUTEDATA.FOREIGN_ID.eq(germplasmId))
												   .and(ATTRIBUTEDATA.VALUE.eq(value))
												   .fetchAny();

				 if (data == null)
				 {
					 data = context.newRecord(ATTRIBUTEDATA);
					 data.setAttributeId(attributeId);
					 data.setForeignId(germplasmId);
					 data.setValue(value);
					 data.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					 data.store();
				 }
			 }
		 });

	}

	private void getOrCreateAttributes(DSLContext context, Row r)
	{
		if (allCellsEmpty(r))
			return;

		r.stream()
		 .skip(1)
		 .forEachOrdered(c -> {
			 String name = getCellValue(c);
			 Integer id = attributeToId.get(name);

			 if (id == null)
			 {
				 AttributesRecord attribute = context.newRecord(ATTRIBUTES);
				 attribute.setName(name);
				 attribute.setDatatype(AttributesDatatype.char_);
				 attribute.setTargetTable("germinatebase");
				 attribute.store();

				 attributeToId.put(name, attribute.getId());
				 id = attribute.getId();
			 }

			 attributeIds.add(id);
		 });
	}

	private void setEntityParent(DSLContext context, Row r)
	{
		Integer parentId = accenumbToId.get(getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB"));
		Integer childId = accenumbToId.get(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));

		GerminatebaseRecord parent = germinatebaseRecords.get(parentId);
		GerminatebaseRecord child = germinatebaseRecords.get(childId);

		if (parent != null && child != null)
		{
			child = context.newRecord(GERMINATEBASE, child);
			child.setEntityparentId(parent.getId());
			child.update(GERMINATEBASE.ENTITYPARENT_ID);
			germinatebaseRecords.put(child.getId(), child);
		}
		else
		{
			throw new RuntimeException("Invalid child or parent: " + child + " " + parent);
		}
	}

	private void insert(DSLContext context, Row r, boolean isUpdate)
	{
		if (allCellsEmpty(r))
			return;

		Germplasm insert = parseMcpd(r);

		if (!StringUtils.isEmpty(insert.taxonomy.getGenus()))
		{
			insert.taxonomy = getOrCreateTaxonomy(context, insert.taxonomy);
			insert.germinatebase.setTaxonomyId(insert.taxonomy.getId());
		}

		if (!StringUtils.isEmpty(insert.country.getCountryCode3()))
		{
			insert.country = getCountry(context, insert.country);
			insert.location.setCountryId(insert.country.getId());
		}

		if (!StringUtils.isEmpty(insert.location.getSiteName()))
		{
			insert.location = getOrCreateLocation(context, insert.location);
		}
		else if (!StringUtils.isEmpty(insert.country.getCountryCode3()))
		{
			// We get here, if there isn't a specific location, but a country. In this case, we add a dummy location.
			insert.location.setSiteName("UNKNOWN LOCATION");
			insert.location = getOrCreateLocation(context, insert.location);
		}

		if (!StringUtils.isEmpty(insert.institution.getName()))
		{
			insert.institution = getOrCreateInstitution(context, insert.institution);
		}

		if (insert.location != null)
			insert.germinatebase.setLocationId(insert.location.getId());
		if (insert.taxonomy != null)
			insert.germinatebase.setTaxonomyId(insert.taxonomy.getId());
		if (insert.institution != null)
			insert.germinatebase.setInstitutionId(insert.institution.getId());

		if (isUpdate)
		{
			String accenumb = getCellValue(r, columnNameToIndex, "ACCENUMB");
			Integer id = gidToId.get(accenumb);

			// The new data object from the file
			GerminatebaseRecord newStuff = insert.germinatebase;
			// The existing record from the database
			GerminatebaseRecord existing = germinatebaseRecords.get(id);

			insert.germinatebase = (GerminatebaseRecord) writeUpdate(context.newRecord(GERMINATEBASE, existing), newStuff, GERMINATEBASE.ID);
		}
		else
		{
			insert.germinatebase = getOrCreateGermplasm(context, insert.germinatebase);
		}

		if (!StringUtils.isEmpty(insert.pedigree.getDefinition()))
		{
			PedigreenotationsRecord pedigreeNotation = getOrCreatePedigreeNotation(context, "MCPD");
			insert.pedigree.setPedigreenotationId(pedigreeNotation.getId());
			insert.pedigree.setGerminatebaseId(insert.germinatebase.getId());
			insert.pedigree = getOrCreatePedigree(context, insert.pedigree);
		}

		if (insert.storage.size() > 0)
		{
			getOrCreateStorage(context, insert);
		}

		if (!StringUtils.isEmpty(insert.remarks))
		{
			getOrCreateRemarks(context, insert);
		}
	}

	private UpdatableRecord writeUpdate(UpdatableRecord existing, UpdatableRecord newStuff, Field<?> idColumn)
	{
		// Load the changes into the record
		List<Field<?>> fields = new ArrayList<>(Arrays.asList(existing.fields()));
		fields.remove(idColumn);
		existing.from(newStuff, fields.toArray(new Field<?>[0]));

		// Remember which fields to update
		List<Field<?>> toUpdate = new ArrayList<>();
		for (int i = 0; i < existing.fields().length; i++)
		{
			// If the value has changed and it hasn't changed to null (that would mean that the update just didn't specify anything for this field), then remember the field.
			if (!Objects.equals(existing.get(i), existing.original(i)))
			{
				Object o = existing.get(i);

				boolean u;
				if (o instanceof String)
					u = !StringUtils.isEmpty((String) o);
				else
					u = o != null;

				if (u)
					toUpdate.add(existing.field(i));
			}
			else
			{
				existing.changed(i, false);
			}
		}

		// Update the changed fields
		existing.update(toUpdate);

		return existing;
	}

	private void getOrCreateRemarks(DSLContext context, Germplasm germplasm)
	{
		Integer attributeId = attributeToId.get("Remarks");

		if (attributeId == null)
		{
			AttributesRecord attribute = context.newRecord(ATTRIBUTES);
			attribute.setName("Remarks");
			attribute.setDescription("Remarks");
			attribute.setDatatype(AttributesDatatype.char_);
			attribute.setTargetTable("germinatebase");
			attribute.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			attribute.store();
			attributeToId.put("Remarks", attribute.getId());
			attributeId = attribute.getId();
		}

		String value = germplasm.remarks;

		if (!StringUtils.isEmpty(value))
		{
			AttributedataRecord data = context.selectFrom(ATTRIBUTEDATA)
											  .where(ATTRIBUTEDATA.ATTRIBUTE_ID.eq(attributeId))
											  .and(ATTRIBUTEDATA.FOREIGN_ID.eq(germplasm.germinatebase.getId()))
											  .and(ATTRIBUTEDATA.VALUE.eq(value))
											  .fetchAny();

			if (data == null)
			{
				data = context.newRecord(ATTRIBUTEDATA);
				data.setAttributeId(attributeId);
				data.setForeignId(germplasm.germinatebase.getId());
				data.setValue(value);
				data.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				data.store();
			}
		}
	}

	private void getOrCreateStorage(DSLContext context, Germplasm germplasm)
	{
		germplasm.storage.removeAll(context.selectFrom(STORAGEDATA)
										   .where(STORAGEDATA.GERMINATEBASE_ID.eq(germplasm.germinatebase.getId()))
										   .and(STORAGEDATA.STORAGE_ID.in(germplasm.storage))
										   .fetchSet(STORAGEDATA.STORAGE_ID));

		for (Integer id : germplasm.storage)
		{
			StoragedataRecord record = context.newRecord(STORAGEDATA);
			record.setGerminatebaseId(germplasm.germinatebase.getId());
			record.setStorageId(id);
			record.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			record.store();
		}
	}

	private GerminatebaseRecord getOrCreateGermplasm(DSLContext context, GerminatebaseRecord germinatebase)
	{
		GerminatebaseRecord result = context.selectFrom(GERMINATEBASE)
											.where(GERMINATEBASE.NUMBER.isNotDistinctFrom(germinatebase.getNumber()))
											.and(GERMINATEBASE.NAME.isNotDistinctFrom(germinatebase.getName()))
											.and(GERMINATEBASE.GENERAL_IDENTIFIER.isNotDistinctFrom(germinatebase.getGeneralIdentifier()))
											.and(GERMINATEBASE.ACQDATE.isNotDistinctFrom(germinatebase.getAcqdate()))
											.and(GERMINATEBASE.BREEDERS_CODE.isNotDistinctFrom(germinatebase.getBreedersCode()))
											.and(GERMINATEBASE.BREEDERS_NAME.isNotDistinctFrom(germinatebase.getBreedersName()))
											.and(GERMINATEBASE.COLLDATE.isNotDistinctFrom(germinatebase.getColldate()))
											.and(GERMINATEBASE.COLLMISSID.isNotDistinctFrom(germinatebase.getCollmissid()))
											.and(GERMINATEBASE.COLLNAME.isNotDistinctFrom(germinatebase.getCollname()))
											.and(GERMINATEBASE.COLLNUMB.isNotDistinctFrom(germinatebase.getCollnumb()))
											.and(GERMINATEBASE.COLLSRC_ID.isNotDistinctFrom(germinatebase.getCollsrcId()))
											.and(GERMINATEBASE.DONOR_CODE.isNotDistinctFrom(germinatebase.getDonorCode()))
											.and(GERMINATEBASE.DONOR_NAME.isNotDistinctFrom(germinatebase.getDonorName()))
											.and(GERMINATEBASE.DONOR_NUMBER.isNotDistinctFrom(germinatebase.getDonorNumber()))
											.and(GERMINATEBASE.DUPLINSTNAME.isNotDistinctFrom(germinatebase.getDuplinstname()))
											.and(GERMINATEBASE.DUPLSITE.isNotDistinctFrom(germinatebase.getDuplsite()))
											.and(GERMINATEBASE.MLSSTATUS_ID.isNotDistinctFrom(germinatebase.getMlsstatusId()))
											.and(GERMINATEBASE.OTHERNUMB.isNotDistinctFrom(germinatebase.getOthernumb()))
											.and(GERMINATEBASE.PUID.isNotDistinctFrom(germinatebase.getPuid()))
											.and(GERMINATEBASE.BIOLOGICALSTATUS_ID.isNotDistinctFrom(germinatebase.getBiologicalstatusId()))
											.and(GERMINATEBASE.LOCATION_ID.isNotDistinctFrom(germinatebase.getLocationId()))
											.and(GERMINATEBASE.TAXONOMY_ID.isNotDistinctFrom(germinatebase.getTaxonomyId()))
											.and(GERMINATEBASE.INSTITUTION_ID.isNotDistinctFrom(germinatebase.getInstitutionId()))
											.fetchAnyInto(GerminatebaseRecord.class);

		if (result == null)
		{
			result = context.newRecord(GERMINATEBASE, germinatebase);
			result.store();

			germinatebaseRecords.put(result.getId(), result);
			accenumbToId.put(result.getName(), result.getId());
		}

		return result;
	}

	private TaxonomiesRecord getOrCreateTaxonomy(DSLContext context, TaxonomiesRecord taxonomy)
	{
		TaxonomiesRecord result = context.selectFrom(TAXONOMIES)
										 .where(TAXONOMIES.GENUS.isNotDistinctFrom(taxonomy.getGenus()))
										 .and(TAXONOMIES.SPECIES.isNotDistinctFrom(taxonomy.getSpecies()))
										 .and(TAXONOMIES.SUBTAXA.isNotDistinctFrom(taxonomy.getSubtaxa()))
										 .and(TAXONOMIES.SPECIES_AUTHOR.isNotDistinctFrom(taxonomy.getSpeciesAuthor()))
										 .and(TAXONOMIES.SUBTAXA_AUTHOR.isNotDistinctFrom(taxonomy.getSubtaxaAuthor()))
										 .fetchAnyInto(TaxonomiesRecord.class);

		if (result == null)
		{
			// Store
			result = context.newRecord(TAXONOMIES, taxonomy);
			result.store();
		}

		return result;
	}

	private PedigreenotationsRecord getOrCreatePedigreeNotation(DSLContext context, String notation)
	{
		PedigreenotationsRecord result = context.selectFrom(PEDIGREENOTATIONS)
												.where(PEDIGREENOTATIONS.NAME.eq(notation))
												.fetchAnyInto(PedigreenotationsRecord.class);

		if (result == null)
		{
			result = context.newRecord(PEDIGREENOTATIONS);
			result.setName(notation);
			result.store();
		}

		return result;
	}

	private InstitutionsRecord getOrCreateInstitution(DSLContext context, InstitutionsRecord institution)
	{
		InstitutionsRecord result = context.selectFrom(INSTITUTIONS)
										   .where(INSTITUTIONS.CODE.isNotDistinctFrom(institution.getCode()))
										   .and(INSTITUTIONS.NAME.isNotDistinctFrom(institution.getName()))
										   .and(INSTITUTIONS.ADDRESS.isNotDistinctFrom(institution.getAddress()))
										   .and(INSTITUTIONS.COUNTRY_ID.isNotDistinctFrom(institution.getCountryId()))
										   .fetchAnyInto(InstitutionsRecord.class);

		if (result == null)
		{
			result = context.newRecord(INSTITUTIONS, institution);
			result.store();
		}

		return result;
	}

	private PedigreedefinitionsRecord getOrCreatePedigree(DSLContext context, PedigreedefinitionsRecord pedigree)
	{
		PedigreedefinitionsRecord result = context.selectFrom(PEDIGREEDEFINITIONS)
												  .where(PEDIGREEDEFINITIONS.GERMINATEBASE_ID.isNotDistinctFrom(pedigree.getGerminatebaseId()))
												  .and(PEDIGREEDEFINITIONS.DEFINITION.isNotDistinctFrom(pedigree.getDefinition()))
												  .and(PEDIGREEDEFINITIONS.PEDIGREENOTATION_ID.isNotDistinctFrom(pedigree.getPedigreenotationId()))
												  .and(PEDIGREEDEFINITIONS.PEDIGREEDESCRIPTION_ID.isNotDistinctFrom(pedigree.getPedigreedescriptionId()))
												  .and(PEDIGREEDEFINITIONS.DEFINITION.isNotDistinctFrom(pedigree.getDefinition()))
												  .fetchAnyInto(PedigreedefinitionsRecord.class);

		if (result == null)
		{
			result = context.newRecord(PEDIGREEDEFINITIONS, pedigree);
			result.store();
		}

		return result;
	}

	private LocationsRecord getOrCreateLocation(DSLContext context, LocationsRecord location)
	{
		LocationsRecord result = context.selectFrom(LOCATIONS)
										.where(LOCATIONS.SITE_NAME.isNotDistinctFrom(location.getSiteName()))
										.and(LOCATIONS.COUNTRY_ID.isNotDistinctFrom(location.getCountryId()))
										.and(LOCATIONS.LATITUDE.isNotDistinctFrom(location.getLatitude()))
										.and(LOCATIONS.LONGITUDE.isNotDistinctFrom(location.getLongitude()))
										.and(LOCATIONS.ELEVATION.isNotDistinctFrom(location.getElevation()))
										.and(LOCATIONS.COORDINATE_DATUM.isNotDistinctFrom(location.getCoordinateDatum()))
										.and(LOCATIONS.COORDINATE_UNCERTAINTY.isNotDistinctFrom(location.getCoordinateUncertainty()))
										.and(LOCATIONS.SITE_NAME_SHORT.isNotDistinctFrom(location.getSiteNameShort()))
										.and(LOCATIONS.STATE.isNotDistinctFrom(location.getState()))
										.and(LOCATIONS.REGION.isNotDistinctFrom(location.getRegion()))
										.fetchAnyInto(LocationsRecord.class);

		if (result == null)
		{
			result = context.newRecord(LOCATIONS, location);
			result.store();

			locationRecords.put(result.getId(), result);
		}

		return result;
	}

	private CountriesRecord getCountry(DSLContext context, CountriesRecord country)
	{
		return context.selectFrom(COUNTRIES)
					  .where(COUNTRIES.COUNTRY_CODE3.isNotDistinctFrom(country.getCountryCode3()))
					  .fetchAnyInto(CountriesRecord.class);
	}

	private Germplasm parseMcpd(Row r)
	{
		Germplasm germplasm = new Germplasm();

		germplasm.germinatebase.setNumber(getCellValue(r, columnNameToIndex, McpdField.ACCENAME.name()));
		germplasm.germinatebase.setName(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));
		germplasm.germinatebase.setGeneralIdentifier(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));
		germplasm.germinatebase.setAcqdate(getCellValue(r, columnNameToIndex, McpdField.ACQDATE.name()));
		germplasm.pedigree.setDefinition(getCellValue(r, columnNameToIndex, McpdField.ANCEST.name()));
		germplasm.germinatebase.setBreedersCode(getCellValue(r, columnNameToIndex, McpdField.BREDCODE.name()));
		germplasm.germinatebase.setBreedersName(getCellValue(r, columnNameToIndex, McpdField.BREDNAME.name()));
		germplasm.germinatebase.setCollcode(getCellValue(r, columnNameToIndex, McpdField.COLLCODE.name()));
		germplasm.germinatebase.setColldate(getCellValueDate(r, columnNameToIndex, McpdField.COLLDATE.name()));
		germplasm.institution.setAddress(getCellValue(r, columnNameToIndex, McpdField.COLLINSTADDRESS.name()));
		germplasm.germinatebase.setCollmissid(getCellValue(r, columnNameToIndex, McpdField.COLLMISSID.name()));
		germplasm.germinatebase.setCollname(getCellValue(r, columnNameToIndex, McpdField.COLLNAME.name()));
		germplasm.germinatebase.setCollnumb(getCellValue(r, columnNameToIndex, McpdField.COLLNUMB.name()));
		germplasm.germinatebase.setCollsrcId(getCellValueInteger(r, columnNameToIndex, McpdField.COLLSRC.name()));
		germplasm.location.setCoordinateDatum(getCellValue(r, columnNameToIndex, McpdField.COORDDATUM.name()));
		germplasm.location.setCoordinateUncertainty(getCellValueInteger(r, columnNameToIndex, McpdField.COORDUNCERT.name()));
		germplasm.taxonomy.setCropname(getCellValue(r, columnNameToIndex, McpdField.CROPNAME.name()));
		germplasm.location.setSiteName(getCellValue(r, columnNameToIndex, McpdField.COLLSITE.name()));
		germplasm.location.setLatitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLATITUDE.name()));
		germplasm.location.setLongitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLONGITUDE.name()));
		germplasm.germinatebase.setDonorCode(getCellValue(r, columnNameToIndex, McpdField.DONORCODE.name()));
		germplasm.germinatebase.setDonorName(getCellValue(r, columnNameToIndex, McpdField.DONORNAME.name()));
		germplasm.germinatebase.setDonorNumber(getCellValue(r, columnNameToIndex, McpdField.DONORNUMB.name()));
		germplasm.germinatebase.setDuplinstname(getCellValue(r, columnNameToIndex, McpdField.DUPLINSTNAME.name()));
		germplasm.germinatebase.setDuplsite(getCellValue(r, columnNameToIndex, McpdField.DUPLSITE.name()));
		germplasm.location.setElevation(getCellValueBigDecimal(r, columnNameToIndex, McpdField.ELEVATION.name()));
		germplasm.germinatebase.setEntityparentId(gidToId.get(getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB")));
		germplasm.germinatebase.setEntitytypeId(entityTypeToId.get(getCellValue(r, columnNameToIndex, "Entity type")));
		germplasm.taxonomy.setGenus(getCellValue(r, columnNameToIndex, McpdField.GENUS.name()));
		germplasm.location.setGeoreferencingMethod(getCellValue(r, columnNameToIndex, McpdField.GEOREFMETH.name()));
		germplasm.institution.setCode(getCellValue(r, columnNameToIndex, McpdField.INSTCODE.name()));
		germplasm.institution.setName(getCellValue(r, columnNameToIndex, McpdField.INSTCODE.name()));
		germplasm.germinatebase.setMlsstatusId(getCellValueInteger(r, columnNameToIndex, McpdField.MLSSTAT.name()));
		germplasm.country.setCountryCode3(getCellValue(r, columnNameToIndex, McpdField.ORIGCTY.name()));
		germplasm.germinatebase.setOthernumb(getCellValue(r, columnNameToIndex, McpdField.OTHERNUMB.name()));
		germplasm.germinatebase.setPuid(getCellValue(r, columnNameToIndex, McpdField.PUID.name()));
		germplasm.germinatebase.setBiologicalstatusId(getCellValueInteger(r, columnNameToIndex, McpdField.SAMPSTAT.name()));
		germplasm.taxonomy.setSpeciesAuthor(getCellValue(r, columnNameToIndex, McpdField.SPAUTHOR.name()));
		germplasm.taxonomy.setSpecies(getCellValue(r, columnNameToIndex, McpdField.SPECIES.name()));
		germplasm.taxonomy.setSubtaxaAuthor(getCellValue(r, columnNameToIndex, McpdField.SUBTAUTHOR.name()));
		germplasm.taxonomy.setSubtaxa(getCellValue(r, columnNameToIndex, McpdField.SUBTAXA.name()));

		if (germplasm.germinatebase.getEntitytypeId() == null)
			germplasm.germinatebase.setEntitytypeId(1);

		germplasm.location.setLocationtypeId(1);

		germplasm.remarks = getCellValue(r, columnNameToIndex, McpdField.REMARKS.name());
		String storage = getCellValue(r, columnNameToIndex, McpdField.STORAGE.name());

		if (!StringUtils.isEmpty(storage))
		{
			String[] parts = storage.split(";");

			for (int i = 0; i < parts.length; i++)
			{
				try
				{
					germplasm.storage.add(Integer.parseInt(parts[i].trim()));
				}
				catch (Exception e)
				{
					// Ignore anything that goes on here.
				}
			}
		}

		return germplasm;
	}

	private static class Germplasm
	{
		private GerminatebaseRecord       germinatebase = new GerminatebaseRecord();
		private LocationsRecord           location      = new LocationsRecord();
		private CountriesRecord           country       = new CountriesRecord();
		private TaxonomiesRecord          taxonomy      = new TaxonomiesRecord();
		private InstitutionsRecord        institution   = new InstitutionsRecord();
		private PedigreedefinitionsRecord pedigree      = new PedigreedefinitionsRecord();
		private List<Integer>             storage       = new ArrayList<>();
		private String                    remarks;

		public Germplasm()
		{
		}
	}
}
