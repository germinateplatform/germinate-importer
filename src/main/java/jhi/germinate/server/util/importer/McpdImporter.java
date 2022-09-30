package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.*;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.*;
import org.jooq.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import static jhi.germinate.server.database.codegen.tables.Attributedata.*;
import static jhi.germinate.server.database.codegen.tables.Attributes.*;
import static jhi.germinate.server.database.codegen.tables.Biologicalstatus.*;
import static jhi.germinate.server.database.codegen.tables.Collectingsources.*;
import static jhi.germinate.server.database.codegen.tables.Countries.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Entitytypes.*;
import static jhi.germinate.server.database.codegen.tables.Experiments.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Germplasminstitutions.*;
import static jhi.germinate.server.database.codegen.tables.Institutions.*;
import static jhi.germinate.server.database.codegen.tables.Locations.*;
import static jhi.germinate.server.database.codegen.tables.Mcpd.*;
import static jhi.germinate.server.database.codegen.tables.Mlsstatus.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreedefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreenotations.*;
import static jhi.germinate.server.database.codegen.tables.Synonyms.*;
import static jhi.germinate.server.database.codegen.tables.Taxonomies.*;

/**
 * @author Sebastian Raubach
 */
public class McpdImporter extends AbstractExcelImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"PUID", "INSTCODE", "ACCENUMB", "COLLNUMB", "COLLCODE", "COLLNAME", "COLLINSTADDRESS", "COLLMISSID", "GENUS", "SPECIES", "SPAUTHOR", "SUBTAXA", "SUBTAUTHOR", "CROPNAME", "ACCENAME", "ACQDATE", "ORIGCTY", "COLLSITE", "DECLATITUDE", "LATITUDE", "DECLONGITUDE", "LONGITUDE", "COORDUNCERT", "COORDDATUM", "GEOREFMETH", "ELEVATION", "COLLDATE", "BREDCODE", "BREDNAME", "SAMPSTAT", "ANCEST", "COLLSRC", "DONORCODE", "DONORNAME", "DONORNUMB", "OTHERNUMB", "DUPLSITE", "DUPLINSTNAME", "STORAGE", "MLSSTAT", "REMARKS"};

	private       Map<String, Integer>              columnNameToIndex;
	private final Set<String>                       foundAccenumb   = new HashSet<>();
	private final List<Integer>                     attributeIds    = new ArrayList<>();
	private final Map<String, Integer>              gidToId         = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>              countryCodeToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>              attributeToId   = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>              accenumbToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>              entityTypeToId  = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private       Map<Integer, GerminatebaseRecord> germinatebaseRecords;
	private       Map<Integer, McpdRecord>          mcpdRecords;
	private       Map<Integer, LocationsRecord>     locationRecords;
	private       List<Integer>                     validCollsrc;
	private       List<Integer>                     validSampstat;
	private       List<Integer>                     validMlsStatus;

	private Map<String, InstitutionsRecord> institutionCodes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	public static void main(String[] args)
	{
		if (args.length != 6) throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		McpdImporter importer = new McpdImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public McpdImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE).forEach(g -> gidToId.put(g.getGeneralIdentifier(), g.getId()));

			context.selectFrom(COUNTRIES).forEach(c -> countryCodeToId.put(c.getCountryCode3(), c.getId()));

			validMlsStatus = context.selectFrom(MLSSTATUS).fetch(MLSSTATUS.ID);

			validCollsrc = context.selectFrom(COLLECTINGSOURCES).fetch(COLLECTINGSOURCES.ID);

			validSampstat = context.selectFrom(BIOLOGICALSTATUS).fetch(BIOLOGICALSTATUS.ID);

			context.selectFrom(ENTITYTYPES).forEach(e -> entityTypeToId.put(e.getName(), e.getId()));

			context.selectFrom(ATTRIBUTES).forEach(a -> attributeToId.put(a.getName(), a.getId()));

			context.selectFrom(GERMINATEBASE).forEach(g -> accenumbToId.put(g.getName(), g.getId()));

			germinatebaseRecords = context.selectFrom(GERMINATEBASE).fetchMap(GERMINATEBASE.ID);

			mcpdRecords = context.selectFrom(MCPD).fetchMap(MCPD.GERMINATEBASE_ID);

			locationRecords = context.selectFrom(LOCATIONS).fetchMap(LOCATIONS.ID);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		try
		{
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "DATA")).findFirst().ifPresent(s -> {
				try
				{
					// Map headers to their index
					s.openStream().findFirst().ifPresent(this::getHeaderMapping);
					// Check the sheet
					s.openStream().skip(1).forEachOrdered(this::check);
					// Check the entity parent for each row
					s.openStream().skip(1).forEachOrdered(this::checkEntityParent);
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
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "ADDITIONAL_ATTRIBUTES")).findFirst().ifPresent(s -> {
				try
				{
					s.openStream().skip(1).forEachOrdered(this::checkGermplasm);
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
		String germplasm = getCellValue(r, 0);

		if (!StringUtils.isEmpty(germplasm) && !foundAccenumb.contains(germplasm))
			addImportResult(ImportStatus.MCPD_MISSING_ACCENUMB, r.getRowNum(), germplasm);
	}

	private void getHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			columnNameToIndex = IntStream.range(0, r.getCellCount()).filter(i -> !cellEmpty(r, i)).boxed().collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS).forEach(c -> {
				if (!columnNameToIndex.containsKey(c)) addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, -1, c);
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
		if (allCellsEmpty(r)) return;

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
		if ((gidToId.containsKey(accenumb) && !jobDetails.getIsUpdate()) || alreadyFoundInFile)
			addImportResult(ImportStatus.MCPD_DUPLICATE_ACCENUMB, r.getRowNum(), accenumb);
		if (jobDetails.getIsUpdate() && !gidToId.containsKey(accenumb))
			addImportResult(ImportStatus.GENERIC_MISSING_DB_ITEM_UPDATE, r.getRowNum(), accenumb);

		if (!jobDetails.getIsUpdate())
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
		Integer countryId = null;
		if (!StringUtils.isEmpty(countryCode))
		{
			countryId = countryCodeToId.get(countryCode);
			if (countryId == null) addImportResult(ImportStatus.GENERIC_INVALID_COUNTRY_CODE, r.getRowNum(), countryCode);
		}

		// Check if the collsite is specified, but the country isn't
		String collsite = getCellValue(r, columnNameToIndex, McpdField.COLLSITE.name());
		if (!StringUtils.isEmpty(collsite) && countryId == null)
			addImportResult(ImportStatus.GENERIC_MISSING_COUNTRY, r.getRowNum(), collsite + " " + countryCode);

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
		// Check if latitude is valid DMS
		String latitude = getCellValue(r, columnNameToIndex, McpdField.LATITUDE.name());
		if (!StringUtils.isEmpty(latitude))
		{
			BigDecimal dms = getCellValueDMS(r, columnNameToIndex, McpdField.LATITUDE.name());

			if (dms == null) addImportResult(ImportStatus.MCPD_INVALID_DMS, r.getRowNum(), McpdField.LATITUDE.name() + ": " + latitude);
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
		// Check if longitude is valid DMS
		String longitude = getCellValue(r, columnNameToIndex, McpdField.LONGITUDE.name());
		if (!StringUtils.isEmpty(longitude))
		{
			BigDecimal dms = getCellValueDMS(r, columnNameToIndex, McpdField.LONGITUDE.name());

			if (dms == null) addImportResult(ImportStatus.MCPD_INVALID_DMS, r.getRowNum(), McpdField.LONGITUDE.name() + ": " + longitude);
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
				if (!validSampstat.contains(Integer.parseInt(sampstat))) addImportResult(ImportStatus.MCPD_INVALID_SAMPSTAT, r.getRowNum(), sampstat);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.SAMPSTAT.name() + ": " + sampstat);
			}
		}

		// Check MLSSTATUS
		String mlsStatus = getCellValue(r, columnNameToIndex, McpdField.MLSSTAT.name());
		if (!StringUtils.isEmpty(mlsStatus))
		{
			try
			{
				if (!validMlsStatus.contains(Integer.parseInt(mlsStatus)))
					addImportResult(ImportStatus.MCPD_INVALID_MLSSTATUS, r.getRowNum(), mlsStatus);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.MLSSTAT.name() + ": " + mlsStatus);
			}
		}

		// Check COLLSRC
		String collsrc = getCellValue(r, columnNameToIndex, McpdField.COLLSRC.name());
		if (!StringUtils.isEmpty(collsrc))
		{
			try
			{
				if (!validCollsrc.contains(Integer.parseInt(collsrc))) addImportResult(ImportStatus.MCPD_INVALID_COLLSRC, r.getRowNum(), collsrc);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), McpdField.COLLSRC.name() + ": " + collsrc);
			}
		}

		try
		{
			// Check entity type
			String entityType = getCellValue(r, columnNameToIndex, "Entity type");
			if (!StringUtils.isEmpty(entityType))
			{
				if (!entityTypeToId.containsKey(entityType)) addImportResult(ImportStatus.MCPD_INVALID_ENTITY_TYPE, r.getRowNum(), entityType);
			}
		}
		catch (NullPointerException e)
		{
			// We get here if the column isn't present. This can be the case in older versions of the template. Let this slide...
		}

		// Check DUPLINST
		String duplinst = getCellValue(r, columnNameToIndex, McpdField.DUPLSITE.name());
		String duplname = getCellValue(r, columnNameToIndex, McpdField.DUPLINSTNAME.name());

		if (!StringUtils.isEmpty(duplinst) || !StringUtils.isEmpty(duplname))
		{
			if (StringUtils.isEmpty(duplinst))
				duplinst = "";
			if (StringUtils.isEmpty(duplname))
				duplname = "";

			String[] codes = duplinst.split(";", -1);
			String[] names = duplname.split(";", -1);

			if (codes.length != names.length)
			{
				addImportResult(ImportStatus.MCPD_INVALID_DUPLINST_NAME_MAPPING, r.getRowNum(), duplinst + " - " + duplname);
			}
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			context.selectFrom(INSTITUTIONS).where(INSTITUTIONS.CODE.isNotNull())
				   .forEach(i -> {
					   institutionCodes.put(i.getCode(), i);
				   });

			// Import the data
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "DATA")).findFirst().ifPresent(s -> {
				try
				{
					// Map headers to their index
					s.openStream().findFirst().ifPresent(this::getHeaderMapping);
					// Import the sheet
					s.openStream().skip(1).forEachOrdered(r -> insert(context, r, false));
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}

				// Set entity parents
				try
				{
					// Import the sheet
					s.openStream().skip(1).filter(r -> getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB") != null).forEachOrdered(r -> setEntityParent(context, r));
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}
			});

			// Import the attributes
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "ADDITIONAL_ATTRIBUTES")).findFirst().ifPresent(s -> {
				try
				{
					s.openStream().findFirst().ifPresent(r -> getOrCreateAttributes(context, r));
					s.openStream().skip(1).forEachOrdered(r -> insertAttributeData(context, r));
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
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "DATA")).findFirst().ifPresent(s -> {
				try
				{
					// Map headers to their index
					s.openStream().findFirst().ifPresent(this::getHeaderMapping);
					// Update the sheet
					s.openStream().skip(1).forEachOrdered(r -> insert(context, r, true));
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}

				// Set entity parents
				try
				{
					// Import the sheet
					s.openStream().skip(1).filter(r -> getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB") != null).forEachOrdered(r -> setEntityParent(context, r));
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}
			});

			// Import the attributes
			wb.getSheets().filter(s -> Objects.equals(s.getName(), "ADDITIONAL_ATTRIBUTES")).findFirst().ifPresent(s -> {
				try
				{
					s.openStream().findFirst().ifPresent(r -> getOrCreateAttributes(context, r));
					s.openStream().skip(1).forEachOrdered(r -> updateAttributeData(context, r));
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
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void updateAttributeData(DSLContext context, Row r)
	{
		if (allCellsEmpty(r)) return;

		String accenumb = getCellValue(r.getCell(0));
		Integer germplasmId = accenumbToId.get(accenumb);

		r.stream().skip(1).forEachOrdered(c -> {
			String value = getCellValue(c);

			if (!StringUtils.isEmpty(value))
			{
				Integer attributeId = attributeIds.get(c.getColumnIndex() - 1);

				AttributedataRecord data = context.selectFrom(ATTRIBUTEDATA).where(ATTRIBUTEDATA.ATTRIBUTE_ID.eq(attributeId)).and(ATTRIBUTEDATA.FOREIGN_ID.eq(germplasmId)).fetchAny();

				if (data != null)
				{
					// Update it
					data.setValue(value);
					data.store();
				}
				else
				{
					// Insert new entry
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

	private void insertAttributeData(DSLContext context, Row r)
	{
		if (allCellsEmpty(r)) return;

		String accenumb = getCellValue(r, 0);
		Integer germplasmId = accenumbToId.get(accenumb);

		// Get all the attribute data for this germplasm
		Set<String> existingData = new HashSet<>();
		context.selectFrom(ATTRIBUTEDATA).where(ATTRIBUTEDATA.FOREIGN_ID.eq(germplasmId)).forEach(a -> existingData.add(a.getAttributeId() + "|" + a.getValue()));

		r.stream().skip(1).forEachOrdered(c -> {
			String value = getCellValue(c);

			// Is there a value?
			if (!StringUtils.isEmpty(value))
			{
				// Get the attribute id
				Integer attributeId = attributeIds.get(c.getColumnIndex() - 1);

				// Check if the data exists
				if (!existingData.contains(attributeId + "|" + value))
				{
					// Insert it if not
					AttributedataRecord data = context.newRecord(ATTRIBUTEDATA);
					data.setAttributeId(attributeId);
					data.setForeignId(germplasmId);
					data.setValue(value);
					data.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					data.store();

					// Remember the newly inserted data to prevent duplicates
					existingData.add(attributeId + "|" + value);
				}
			}
		});

	}

	private void getOrCreateAttributes(DSLContext context, Row r)
	{
		if (allCellsEmpty(r)) return;

		r.stream().skip(1).forEachOrdered(c -> {
			String name = getCellValue(c);
			Integer id = attributeToId.get(name);

			if (id == null)
			{
				AttributesRecord attribute = context.newRecord(ATTRIBUTES);
				attribute.setName(name);
				attribute.setDatatype(AttributesDatatype.text);
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
		if (allCellsEmpty(r)) return;

		Germplasm insert = parseMcpd(r);

		if (!StringUtils.isEmpty(insert.taxonomy.getGenus()))
		{
			insert.taxonomy = getOrCreateTaxonomy(context, insert.taxonomy);
			insert.germinatebase.setTaxonomyId(insert.taxonomy.getId());
		}

		String countryCode = insert.country.getCountryCode3();
		int countryId = StringUtils.isEmpty(countryCode) ? -1 : countryCodeToId.get(countryCode);
		insert.location.setCountryId(countryId);

		if (!StringUtils.isEmpty(insert.location.getSiteName()))
		{
			insert.location = getOrCreateLocation(context, insert.location);
		}
		else if (countryId != -1)
		{
			// We get here, if there isn't a specific location, but a country. In this case, we add a dummy location.
			insert.location.setSiteName("N/A");
			insert.location = getOrCreateLocation(context, insert.location);
		}

		if (insert.location != null) insert.germinatebase.setLocationId(insert.location.getId());
		if (insert.taxonomy != null) insert.germinatebase.setTaxonomyId(insert.taxonomy.getId());

		if (isUpdate)
		{
			String accenumb = getCellValue(r, columnNameToIndex, "ACCENUMB");
			Integer id = gidToId.get(accenumb);

			// The new data object from the file
			GerminatebaseRecord newGermplasm = insert.germinatebase;
			// The existing record from the database
			GerminatebaseRecord existingGermplasm = germinatebaseRecords.get(id);

			insert.germinatebase = (GerminatebaseRecord) writeUpdate(context.newRecord(GERMINATEBASE, existingGermplasm), newGermplasm, GERMINATEBASE.ID);

			McpdRecord newMcpd = insert.mcpd;
			McpdRecord existingMcpd = mcpdRecords.get(id);

			insert.mcpd = (McpdRecord) writeUpdate(context.newRecord(MCPD, existingMcpd), newMcpd, MCPD.GERMINATEBASE_ID);
		}
		else
		{
			getOrCreateGermplasm(context, insert);
		}

		List<String> synonyms = new ArrayList<>();
		// Add the ACCENAME as a synonym
		if (!StringUtils.isEmpty(insert.germinatebase.getNumber())) synonyms.add(insert.germinatebase.getNumber());
		// Add all OTHERNUMB parts as synonyms
		if (!StringUtils.isEmpty(insert.mcpd.getOthernumb()))
		{
			Arrays.stream(insert.mcpd.getOthernumb().split(";")).map(String::trim).forEach(synonyms::add);
		}

		// Insert/Update the synonyms if there are any.
		if (synonyms.size() > 0)
		{
			SynonymsRecord synRec = context.selectFrom(SYNONYMS).where(SYNONYMS.SYNONYMTYPE_ID.eq(1).and(SYNONYMS.FOREIGN_ID.eq(insert.germinatebase.getId()))).fetchAny();

			if (synRec == null)
			{
				synRec = context.newRecord(SYNONYMS);
				synRec.setForeignId(insert.germinatebase.getId());
				synRec.setSynonymtypeId(1);
				synRec.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			}

			synRec.setSynonyms(synonyms.toArray(new String[0]));
			synRec.store();
		}

		if (!StringUtils.isEmpty(insert.pedigree.getDefinition()))
		{
			DatasetsRecord pedigreeDataset = getOrCreatePedigreeDataset(context, "MCPD");
			PedigreenotationsRecord pedigreeNotation = getOrCreatePedigreeNotation(context, "MCPD");
			insert.pedigree.setPedigreenotationId(pedigreeNotation.getId());
			insert.pedigree.setGerminatebaseId(insert.germinatebase.getId());
			insert.pedigree.setDatasetId(pedigreeDataset.getId());
			insert.pedigree = getOrCreatePedigree(context, insert.pedigree);
		}

		if (!StringUtils.isEmpty(insert.remarks))
		{
			getOrCreateRemarks(context, insert);
		}

		if (insert.maintInst != null)
		{
			insert.maintInst = getOrCreateInstitution(context, insert.maintInst);
			getOrCreateGermplasmInstitution(context, insert.germinatebase, insert.maintInst, GermplasminstitutionsType.maintenance);
		}
		if (insert.collectInst != null)
		{
			insert.collectInst = getOrCreateInstitution(context, insert.collectInst);
			getOrCreateGermplasmInstitution(context, insert.germinatebase, insert.collectInst, GermplasminstitutionsType.collection);
		}
		if (insert.breedingInst != null)
		{
			insert.breedingInst = getOrCreateInstitution(context, insert.breedingInst);
			getOrCreateGermplasmInstitution(context, insert.germinatebase, insert.breedingInst, GermplasminstitutionsType.breeding);
		}
		if (insert.donoInst != null)
		{
			insert.donoInst = getOrCreateInstitution(context, insert.donoInst);
			getOrCreateGermplasmInstitution(context, insert.germinatebase, insert.donoInst, GermplasminstitutionsType.donor);
		}
		if (!CollectionUtils.isEmpty(insert.dupliInst))
		{
			for (int i = 0; i < insert.dupliInst.size(); i++)
			{
				insert.dupliInst.set(i, getOrCreateInstitution(context, insert.dupliInst.get(i)));
				getOrCreateGermplasmInstitution(context, insert.germinatebase, insert.dupliInst.get(i), GermplasminstitutionsType.duplicate);
			}
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
				if (o instanceof String) u = !StringUtils.isEmpty((String) o);
				else u = o != null;

				if (u) toUpdate.add(existing.field(i));
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
			attribute.setDatatype(AttributesDatatype.text);
			attribute.setTargetTable("germinatebase");
			attribute.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			attribute.store();
			attributeToId.put("Remarks", attribute.getId());
			attributeId = attribute.getId();
		}

		String value = germplasm.remarks;

		if (!StringUtils.isEmpty(value))
		{
			AttributedataRecord data = context.selectFrom(ATTRIBUTEDATA).where(ATTRIBUTEDATA.ATTRIBUTE_ID.eq(attributeId)).and(ATTRIBUTEDATA.FOREIGN_ID.eq(germplasm.germinatebase.getId())).and(ATTRIBUTEDATA.VALUE.eq(value)).fetchAny();

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

	private void getOrCreateGermplasm(DSLContext context, Germplasm germplasm)
	{
		GerminatebaseRecord germinatebase = context.selectFrom(GERMINATEBASE)
												   .where(GERMINATEBASE.NUMBER.isNotDistinctFrom(germplasm.germinatebase.getNumber()))
												   .and(GERMINATEBASE.NAME.isNotDistinctFrom(germplasm.germinatebase.getName()))
												   .and(GERMINATEBASE.GENERAL_IDENTIFIER.isNotDistinctFrom(germplasm.germinatebase.getGeneralIdentifier()))
												   .and(GERMINATEBASE.LOCATION_ID.isNotDistinctFrom(germplasm.germinatebase.getLocationId()))
												   .and(GERMINATEBASE.TAXONOMY_ID.isNotDistinctFrom(germplasm.germinatebase.getTaxonomyId()))
												   .fetchAny();

		if (germinatebase == null)
		{
			germinatebase = context.newRecord(GERMINATEBASE, germplasm.germinatebase);
			germinatebase.store();

			germinatebaseRecords.put(germinatebase.getId(), germinatebase);
			accenumbToId.put(germinatebase.getName(), germinatebase.getId());
			germplasm.germinatebase = germinatebase;
		}

		McpdRecord mcpd = context.selectFrom(MCPD)
								 .where(MCPD.GERMINATEBASE_ID.isNotDistinctFrom(germinatebase.getId()))
								 .and(MCPD.ACCENAME.isNotDistinctFrom(germplasm.mcpd.getAccename()))
								 .and(MCPD.ACCENUMB.isNotDistinctFrom(germplasm.mcpd.getAccenumb()))
								 .and(MCPD.ACQDATE.isNotDistinctFrom(germplasm.mcpd.getAcqdate()))
								 .and(MCPD.BREDCODE.isNotDistinctFrom(germplasm.mcpd.getBredcode()))
								 .and(MCPD.BREDNAME.isNotDistinctFrom(germplasm.mcpd.getBredname()))
								 .and(MCPD.COLLDATE.isNotDistinctFrom(germplasm.mcpd.getColldate()))
								 .and(MCPD.COLLMISSID.isNotDistinctFrom(germplasm.mcpd.getCollmissid()))
								 .and(MCPD.COLLNAME.isNotDistinctFrom(germplasm.mcpd.getCollname()))
								 .and(MCPD.COLLNUMB.isNotDistinctFrom(germplasm.mcpd.getCollnumb()))
								 .and(MCPD.COLLSRC.isNotDistinctFrom(germplasm.mcpd.getCollsrc()))
								 .and(MCPD.DONORCODE.isNotDistinctFrom(germplasm.mcpd.getDonorcode()))
								 .and(MCPD.DONORNAME.isNotDistinctFrom(germplasm.mcpd.getDonorname()))
								 .and(MCPD.DONORNUMB.isNotDistinctFrom(germplasm.mcpd.getDonornumb()))
								 .and(MCPD.DUPLINSTNAME.isNotDistinctFrom(germplasm.mcpd.getDuplinstname()))
								 .and(MCPD.DUPLSITE.isNotDistinctFrom(germplasm.mcpd.getDuplsite()))
								 .and(MCPD.MLSSTAT.isNotDistinctFrom(germplasm.mcpd.getMlsstat()))
								 .and(MCPD.OTHERNUMB.isNotDistinctFrom(germplasm.mcpd.getOthernumb()))
								 .and(MCPD.PUID.isNotDistinctFrom(germplasm.mcpd.getPuid()))
								 .and(MCPD.SAMPSTAT.isNotDistinctFrom(germplasm.mcpd.getSampstat()))
								 .fetchAny();

		if (mcpd == null)
		{
			mcpd = context.newRecord(MCPD, germplasm.mcpd);
			mcpd.setGerminatebaseId(germinatebase.getId());
			mcpd.store();
			germplasm.mcpd = mcpd;
		}
	}

	private TaxonomiesRecord getOrCreateTaxonomy(DSLContext context, TaxonomiesRecord taxonomy)
	{
		TaxonomiesRecord result = context.selectFrom(TAXONOMIES).where(TAXONOMIES.GENUS.isNotDistinctFrom(taxonomy.getGenus())).and(TAXONOMIES.SPECIES.isNotDistinctFrom(taxonomy.getSpecies())).and(TAXONOMIES.SUBTAXA.isNotDistinctFrom(taxonomy.getSubtaxa())).and(TAXONOMIES.SPECIES_AUTHOR.isNotDistinctFrom(taxonomy.getSpeciesAuthor())).and(TAXONOMIES.SUBTAXA_AUTHOR.isNotDistinctFrom(taxonomy.getSubtaxaAuthor())).fetchAnyInto(TaxonomiesRecord.class);

		if (result == null)
		{
			// Store
			result = context.newRecord(TAXONOMIES, taxonomy);
			result.store();
		}

		return result;
	}

	private DatasetsRecord getOrCreatePedigreeDataset(DSLContext context, String name)
	{
		DatasetsRecord result = context.selectFrom(DATASETS).where(DATASETS.DATASETTYPE_ID.eq(7)).and(DATASETS.NAME.eq(name)).fetchAny();

		if (result == null)
		{
			ExperimentsRecord exp = context.selectFrom(EXPERIMENTS).where(EXPERIMENTS.EXPERIMENT_NAME.eq(name)).fetchAny();

			if (exp == null)
			{
				exp = context.newRecord(EXPERIMENTS);
				exp.setExperimentName(name);
				exp.store();
			}

			result = context.newRecord(DATASETS);
			result.setName(name);
			result.setDescription(name);
			result.setDatasettypeId(7);
			result.setDatasetStateId(1);
			result.setExperimentId(exp.getId());
			result.store();
		}

		return result;
	}

	private PedigreenotationsRecord getOrCreatePedigreeNotation(DSLContext context, String notation)
	{
		PedigreenotationsRecord result = context.selectFrom(PEDIGREENOTATIONS).where(PEDIGREENOTATIONS.NAME.eq(notation)).fetchAny();

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
		InstitutionsRecord result = null;

		if (!StringUtils.isEmpty(institution.getCode()))
		{
			result = institutionCodes.get(institution.getCode());
		}

		if (result == null)
		{
			result = context.selectFrom(INSTITUTIONS)
							.where(INSTITUTIONS.CODE.isNotDistinctFrom(institution.getCode()))
							.and(INSTITUTIONS.NAME.isNotDistinctFrom(institution.getName()))
							.and(INSTITUTIONS.ADDRESS.isNotDistinctFrom(institution.getAddress()))
							.and(INSTITUTIONS.COUNTRY_ID.isNotDistinctFrom(institution.getCountryId()))
							.fetchAnyInto(InstitutionsRecord.class);
		}
		else
		{
			// We get here if there is an institution with the same code
			boolean changed = false;

			// If we have a name AND (the DB institution doesn't or it's different) then update it
			if (!StringUtils.isEmpty(institution.getName()) && !Objects.equals("N/A", institution.getName()) && (StringUtils.isEmpty(result.getName()) || !Objects.equals(result.getName(), institution.getName())))
			{
				changed = true;
				result.setName(institution.getName());
			}
			// If we have a address AND (the DB institution doesn't or it's different) then update it
			if (!StringUtils.isEmpty(institution.getAddress()) && !Objects.equals("N/A", institution.getAddress()) && (StringUtils.isEmpty(result.getAddress()) || !Objects.equals(result.getAddress(), institution.getAddress())))
			{
				changed = true;
				result.setAddress(institution.getAddress());
			}

			if (changed)
			{
				// If there are changes, store it back
				result.store();
			}
		}

		if (result == null)
		{
			result = context.newRecord(INSTITUTIONS, institution);
			result.store();
		}

		if (!StringUtils.isEmpty(result.getCode()))
			institutionCodes.put(result.getCode(), result);

		return result;
	}

	private GermplasminstitutionsRecord getOrCreateGermplasmInstitution(DSLContext context, GerminatebaseRecord germplasm, InstitutionsRecord institution, GermplasminstitutionsType type)
	{
		GermplasminstitutionsRecord result = context.selectFrom(GERMPLASMINSTITUTIONS).where(GERMPLASMINSTITUTIONS.GERMINATEBASE_ID.eq(germplasm.getId())).and(GERMPLASMINSTITUTIONS.INSTITUTION_ID.eq(institution.getId())).and(GERMPLASMINSTITUTIONS.TYPE.eq(type)).fetchAny();

		if (result == null)
		{
			result = context.newRecord(GERMPLASMINSTITUTIONS);
			result.setGerminatebaseId(germplasm.getId());
			result.setInstitutionId(institution.getId());
			result.setType(type);
			result.store();
		}

		return result;
	}

	private PedigreedefinitionsRecord getOrCreatePedigree(DSLContext context, PedigreedefinitionsRecord pedigree)
	{
		PedigreedefinitionsRecord result = context.selectFrom(PEDIGREEDEFINITIONS).where(PEDIGREEDEFINITIONS.GERMINATEBASE_ID.isNotDistinctFrom(pedigree.getGerminatebaseId())).and(PEDIGREEDEFINITIONS.DEFINITION.isNotDistinctFrom(pedigree.getDefinition())).and(PEDIGREEDEFINITIONS.PEDIGREENOTATION_ID.isNotDistinctFrom(pedigree.getPedigreenotationId())).and(PEDIGREEDEFINITIONS.PEDIGREEDESCRIPTION_ID.isNotDistinctFrom(pedigree.getPedigreedescriptionId())).and(PEDIGREEDEFINITIONS.DEFINITION.isNotDistinctFrom(pedigree.getDefinition())).fetchAnyInto(PedigreedefinitionsRecord.class);

		if (result == null)
		{
			result = context.newRecord(PEDIGREEDEFINITIONS, pedigree);
			result.store();
		}

		return result;
	}

	private LocationsRecord getOrCreateLocation(DSLContext context, LocationsRecord location)
	{
		LocationsRecord result = context.selectFrom(LOCATIONS).where(LOCATIONS.SITE_NAME.isNotDistinctFrom(location.getSiteName())).and(LOCATIONS.COUNTRY_ID.isNotDistinctFrom(location.getCountryId())).and(LOCATIONS.LATITUDE.isNotDistinctFrom(location.getLatitude())).and(LOCATIONS.LONGITUDE.isNotDistinctFrom(location.getLongitude())).and(LOCATIONS.ELEVATION.isNotDistinctFrom(location.getElevation())).and(LOCATIONS.COORDINATE_DATUM.isNotDistinctFrom(location.getCoordinateDatum())).and(LOCATIONS.COORDINATE_UNCERTAINTY.isNotDistinctFrom(location.getCoordinateUncertainty())).and(LOCATIONS.SITE_NAME_SHORT.isNotDistinctFrom(location.getSiteNameShort())).and(LOCATIONS.STATE.isNotDistinctFrom(location.getState())).and(LOCATIONS.REGION.isNotDistinctFrom(location.getRegion())).fetchAnyInto(LocationsRecord.class);

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
		return context.selectFrom(COUNTRIES).where(COUNTRIES.COUNTRY_CODE3.isNotDistinctFrom(country.getCountryCode3())).fetchAnyInto(CountriesRecord.class);
	}

	private Germplasm parseMcpd(Row r)
	{
		Germplasm germplasm = new Germplasm();

		germplasm.germinatebase.setNumber(getCellValue(r, columnNameToIndex, McpdField.ACCENAME.name()));
		germplasm.mcpd.setAccename(getCellValue(r, columnNameToIndex, McpdField.ACCENAME.name()));
		germplasm.germinatebase.setName(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));
		germplasm.germinatebase.setGeneralIdentifier(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));
		germplasm.mcpd.setAccenumb(getCellValue(r, columnNameToIndex, McpdField.ACCENUMB.name()));
		germplasm.mcpd.setAcqdate(getCellValue(r, columnNameToIndex, McpdField.ACQDATE.name()));
		germplasm.pedigree.setDefinition(getCellValue(r, columnNameToIndex, McpdField.ANCEST.name()));
		germplasm.mcpd.setAncest(getCellValue(r, columnNameToIndex, McpdField.ANCEST.name()));
		germplasm.mcpd.setBredcode(getCellValue(r, columnNameToIndex, McpdField.BREDCODE.name()));
		germplasm.mcpd.setBredname(getCellValue(r, columnNameToIndex, McpdField.BREDNAME.name()));
		germplasm.mcpd.setCollcode(getCellValue(r, columnNameToIndex, McpdField.COLLCODE.name()));
		germplasm.mcpd.setColldate(getCellValue(r, columnNameToIndex, McpdField.COLLDATE.name()));
		germplasm.mcpd.setCollinstaddress(getCellValue(r, columnNameToIndex, McpdField.COLLINSTADDRESS.name()));
		germplasm.mcpd.setCollmissid(getCellValue(r, columnNameToIndex, McpdField.COLLMISSID.name()));
		germplasm.mcpd.setCollname(getCellValue(r, columnNameToIndex, McpdField.COLLNAME.name()));
		germplasm.mcpd.setCollnumb(getCellValue(r, columnNameToIndex, McpdField.COLLNUMB.name()));
		germplasm.mcpd.setCollsrc(getCellValueInteger(r, columnNameToIndex, McpdField.COLLSRC.name()));
		germplasm.location.setCoordinateDatum(getCellValue(r, columnNameToIndex, McpdField.COORDDATUM.name()));
		germplasm.mcpd.setCoorddatum(getCellValue(r, columnNameToIndex, McpdField.COORDDATUM.name()));
		germplasm.location.setCoordinateUncertainty(getCellValueInteger(r, columnNameToIndex, McpdField.COORDUNCERT.name()));
		germplasm.mcpd.setCoorduncert(getCellValueInteger(r, columnNameToIndex, McpdField.COORDUNCERT.name()));
		germplasm.taxonomy.setCropname(getCellValue(r, columnNameToIndex, McpdField.CROPNAME.name()));
		germplasm.mcpd.setCropname(getCellValue(r, columnNameToIndex, McpdField.CROPNAME.name()));
		germplasm.location.setSiteName(getCellValue(r, columnNameToIndex, McpdField.COLLSITE.name()));
		germplasm.mcpd.setCollsite(getCellValue(r, columnNameToIndex, McpdField.COLLSITE.name()));
		germplasm.location.setLatitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLATITUDE.name()));
		germplasm.mcpd.setDeclatitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLATITUDE.name()));
		germplasm.mcpd.setLatitude(getCellValue(r, columnNameToIndex, McpdField.LATITUDE.name()));
		// If there's no decimal, try and parse the DMS
		if (germplasm.location.getLatitude() == null && !StringUtils.isEmpty(getCellValue(r, columnNameToIndex, McpdField.LATITUDE.name())))
			germplasm.location.setLatitude(getCellValueDMS(r, columnNameToIndex, McpdField.LATITUDE.name()));
		germplasm.location.setLongitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLONGITUDE.name()));
		germplasm.mcpd.setDeclongitude(getCellValueBigDecimal(r, columnNameToIndex, McpdField.DECLONGITUDE.name()));
		germplasm.mcpd.setLongitude(getCellValue(r, columnNameToIndex, McpdField.LONGITUDE.name()));
		// If there's no decimal, try and parse the DMS
		if (germplasm.location.getLongitude() == null && !StringUtils.isEmpty(getCellValue(r, columnNameToIndex, McpdField.LONGITUDE.name())))
			germplasm.location.setLongitude(getCellValueDMS(r, columnNameToIndex, McpdField.LONGITUDE.name()));
		germplasm.mcpd.setDonorcode(getCellValue(r, columnNameToIndex, McpdField.DONORCODE.name()));
		germplasm.mcpd.setDonorname(getCellValue(r, columnNameToIndex, McpdField.DONORNAME.name()));
		germplasm.mcpd.setDonornumb(getCellValue(r, columnNameToIndex, McpdField.DONORNUMB.name()));
		germplasm.mcpd.setDuplinstname(getCellValue(r, columnNameToIndex, McpdField.DUPLINSTNAME.name()));
		germplasm.mcpd.setDuplsite(getCellValue(r, columnNameToIndex, McpdField.DUPLSITE.name()));
		germplasm.location.setElevation(getCellValueBigDecimal(r, columnNameToIndex, McpdField.ELEVATION.name()));
		germplasm.mcpd.setElevation(getCellValueBigDecimal(r, columnNameToIndex, McpdField.ELEVATION.name()));
		String parentName = getCellValue(r, columnNameToIndex, "Entity parent ACCENUMB");
		if (!StringUtils.isEmpty(parentName))
		{
			germplasm.germinatebase.setEntityparentId(gidToId.get(parentName));

			Integer entityTypeId = 1;
			String entityType = getCellValue(r, columnNameToIndex, "Entity type");

			if (!StringUtils.isEmpty(entityType)) entityTypeId = entityTypeToId.get(entityType);

			germplasm.germinatebase.setEntitytypeId(entityTypeId);
		}
		germplasm.taxonomy.setGenus(getCellValue(r, columnNameToIndex, McpdField.GENUS.name()));
		germplasm.mcpd.setGenus(getCellValue(r, columnNameToIndex, McpdField.GENUS.name()));
		germplasm.location.setGeoreferencingMethod(getCellValue(r, columnNameToIndex, McpdField.GEOREFMETH.name()));
		germplasm.mcpd.setGeorefmeth(getCellValue(r, columnNameToIndex, McpdField.GEOREFMETH.name()));
		germplasm.mcpd.setInstcode(getCellValue(r, columnNameToIndex, McpdField.INSTCODE.name()));
		germplasm.mcpd.setMlsstat(getCellValueInteger(r, columnNameToIndex, McpdField.MLSSTAT.name()));
		germplasm.country.setCountryCode3(getCellValue(r, columnNameToIndex, McpdField.ORIGCTY.name()));
		germplasm.mcpd.setOrigcty(getCellValue(r, columnNameToIndex, McpdField.ORIGCTY.name()));
		germplasm.mcpd.setOthernumb(getCellValue(r, columnNameToIndex, McpdField.OTHERNUMB.name()));
		germplasm.mcpd.setPuid(getCellValue(r, columnNameToIndex, McpdField.PUID.name()));
		germplasm.mcpd.setSampstat(getCellValueInteger(r, columnNameToIndex, McpdField.SAMPSTAT.name()));
		germplasm.taxonomy.setSpeciesAuthor(getCellValue(r, columnNameToIndex, McpdField.SPAUTHOR.name()));
		germplasm.mcpd.setSpauthor(getCellValue(r, columnNameToIndex, McpdField.SPAUTHOR.name()));
		germplasm.taxonomy.setSpecies(getCellValue(r, columnNameToIndex, McpdField.SPECIES.name()));
		germplasm.mcpd.setSpecies(getCellValue(r, columnNameToIndex, McpdField.SPECIES.name()));
		germplasm.taxonomy.setSubtaxaAuthor(getCellValue(r, columnNameToIndex, McpdField.SUBTAUTHOR.name()));
		germplasm.mcpd.setSubtauthor(getCellValue(r, columnNameToIndex, McpdField.SUBTAUTHOR.name()));
		germplasm.taxonomy.setSubtaxa(getCellValue(r, columnNameToIndex, McpdField.SUBTAXA.name()));
		germplasm.mcpd.setSubtaxa(getCellValue(r, columnNameToIndex, McpdField.SUBTAXA.name()));

		if (germplasm.germinatebase.getEntitytypeId() == null) germplasm.germinatebase.setEntitytypeId(1);

		germplasm.location.setLocationtypeId(1);

		germplasm.remarks = getCellValue(r, columnNameToIndex, McpdField.REMARKS.name());
		germplasm.mcpd.setRemarks(getCellValue(r, columnNameToIndex, McpdField.REMARKS.name()));
		germplasm.mcpd.setStorage(getCellValue(r, columnNameToIndex, McpdField.STORAGE.name()));

		if (!StringUtils.isEmpty(germplasm.mcpd.getInstcode()))
		{
			germplasm.maintInst = new InstitutionsRecord();
			germplasm.maintInst.setCode(germplasm.mcpd.getInstcode());
			germplasm.maintInst.setName("N/A");
		}
		if (!StringUtils.isEmpty(germplasm.mcpd.getBredname()) || !StringUtils.isEmpty(germplasm.mcpd.getBredcode()))
		{
			germplasm.breedingInst = new InstitutionsRecord();
			germplasm.breedingInst.setCode(germplasm.mcpd.getBredcode());
			germplasm.breedingInst.setName(germplasm.mcpd.getBredname());
			if (StringUtils.isEmpty(germplasm.breedingInst.getName()))
				germplasm.breedingInst.setName("N/A");
		}
		if (!StringUtils.isEmpty(germplasm.mcpd.getDonorcode()) || !StringUtils.isEmpty(germplasm.mcpd.getDonorname()))
		{
			germplasm.donoInst = new InstitutionsRecord();
			germplasm.donoInst.setCode(germplasm.mcpd.getDonorcode());
			germplasm.donoInst.setName(germplasm.mcpd.getDonorname());
			if (StringUtils.isEmpty(germplasm.donoInst.getName()))
				germplasm.donoInst.setName("N/A");
		}
		if (!StringUtils.isEmpty(germplasm.mcpd.getCollcode()) || !StringUtils.isEmpty(germplasm.mcpd.getCollname()) || !StringUtils.isEmpty(germplasm.mcpd.getCollinstaddress()))
		{
			germplasm.collectInst = new InstitutionsRecord();
			germplasm.collectInst.setCode(germplasm.mcpd.getCollcode());
			germplasm.collectInst.setName(germplasm.mcpd.getCollname());
			germplasm.collectInst.setAddress(germplasm.mcpd.getCollinstaddress());
			if (StringUtils.isEmpty(germplasm.collectInst.getName()))
				germplasm.collectInst.setName("N/A");
		}
		if (!StringUtils.isEmpty(germplasm.mcpd.getDuplsite()) || !StringUtils.isEmpty(germplasm.mcpd.getDuplinstname()))
		{
			if (StringUtils.isEmpty(germplasm.mcpd.getDuplsite()))
				germplasm.mcpd.setDuplsite("");
			if (StringUtils.isEmpty(germplasm.mcpd.getDuplinstname()))
				germplasm.mcpd.setDuplinstname("");
			List<String> codes = Arrays.stream(germplasm.mcpd.getDuplsite().split(";")).map(String::trim).collect(Collectors.toList());
			List<String> names = Arrays.stream(germplasm.mcpd.getDuplinstname().split(";")).map(String::trim).collect(Collectors.toList());

			germplasm.dupliInst = new ArrayList<>();
			for (int i = 0; i < codes.size(); i++)
			{
				InstitutionsRecord record = new InstitutionsRecord();
				record.setCode(codes.get(i));
				record.setName(names.get(i));
				if (StringUtils.isEmpty(record.getName()))
					record.setName("N/A");
				germplasm.dupliInst.add(record);
			}
		}

		return germplasm;
	}

	private static class Germplasm
	{
		private GerminatebaseRecord       germinatebase = new GerminatebaseRecord();
		private McpdRecord                mcpd          = new McpdRecord();
		private LocationsRecord           location      = new LocationsRecord();
		private InstitutionsRecord        maintInst     = null;
		private InstitutionsRecord        breedingInst  = null;
		private InstitutionsRecord        collectInst   = null;
		private InstitutionsRecord        donoInst      = null;
		private List<InstitutionsRecord>  dupliInst     = new ArrayList<>();
		private CountriesRecord           country       = new CountriesRecord();
		private TaxonomiesRecord          taxonomy      = new TaxonomiesRecord();
		private PedigreedefinitionsRecord pedigree      = new PedigreedefinitionsRecord();
		private String                    remarks;

		public Germplasm()
		{
		}
	}
}
