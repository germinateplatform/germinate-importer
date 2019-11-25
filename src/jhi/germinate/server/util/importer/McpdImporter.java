package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.*;
import org.jooq.*;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.tables.records.*;
import jhi.germinate.server.util.StringUtils;

import static jhi.germinate.server.database.tables.Biologicalstatus.*;
import static jhi.germinate.server.database.tables.Collectingsources.*;
import static jhi.germinate.server.database.tables.Countries.*;
import static jhi.germinate.server.database.tables.Entitytypes.*;
import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Storage.*;
import static jhi.germinate.server.database.tables.Taxonomies.*;

/**
 * @author Sebastian Raubach
 */
public class McpdImporter extends AbstractImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"PUID", "INSTCODE", "ACCENUMB", "COLLNUMB", "COLLCODE", "COLLNAME", "COLLINSTADDRESS", "COLLMISSID", "GENUS", "SPECIES", "SPAUTHOR", "SUBTAXA", "SUBTAUTHOR", "CROPNAME", "ACCENAME", "ACQDATE", "ORIGCTY", "COLLSITE", "DECLATITUDE", "LATITUDE", "DECLONGITUDE", "LONGITUDE", "COORDUNCERT", "COORDDATUM", "GEOREFMETH", "ELEVATION", "COLLDATE", "BREDCODE", "BREDNAME", "SAMPSTAT", "ANCEST", "COLLSRC", "DONORCODE", "DONORNAME", "DONORNUMB", "OTHERNUMB", "DUPLSITE", "DUPLINSTNAME", "STORAGE", "MLSSTAT", "REMARKS"};

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	private Map<String, Integer>              columnNameToIndex;
	private Set<String>                       foundAccenumb = new HashSet<>();
	private Map<String, Integer>              gidToId;
	private Map<String, Integer>              countryCodeToId;
	private Map<Integer, GerminatebaseRecord> germinatebaseRecords;
	private List<Integer>                     validCollsrc;
	private List<Integer>                     validSampstat;
	private List<Integer>                     validStorage;
	private Map<String, Integer>              entityTypeToId;

	public McpdImporter(File input, boolean isUpdate)
	{
		super(input, isUpdate);
	}

	public static void main(String[] args)
	{
		Database.init("localhost", "germinate_demo_api", null, "root", null, false);
//		File file = new File("test/mcpd-update.xlsx");
		File file = new File("D:\\workspaces\\web-intellij\\germinate\\datatemplates\\example-germplasm-mcpd.xlsx");
		new McpdImporter(file, false).run();
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
//					   .filter(r -> r.hasCell(columnNameToIndex.get("ACCENUMB")))
					   .forEachOrdered(this::check);
					  // Check the entity parent for each row
					  s.openStream()
					   .skip(1)
//					   .filter(r -> r.hasCell(columnNameToIndex.get("ACCENUMB")))
					   .forEachOrdered(this::checkEntityParent);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

			// TODO: Check Entity parent ACCENUMB
			// TODO: Check ADDITIONAL_ATTRIBUTES
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'DATA' sheet not found");
		}
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
		// Check the accenumb isn't a duplicate
		boolean alreadyFoundInFile = false;
		String accenumb = getCellValue(r, columnNameToIndex, "ACCENUMB");
		if (StringUtils.isEmpty(accenumb))
		{
			addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), "ACCENUMB");
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
			// Check the genus is present
			String genus = getCellValue(r, columnNameToIndex, "GENUS");
			if (StringUtils.isEmpty(genus))
				addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), "GENUS");
		}

		// Check the date is in the correct format
		String acqdate = getCellValue(r, columnNameToIndex, "ACQDATE");
		try
		{
			// TODO: Make sure to check "-" values
			if (!StringUtils.isEmpty(acqdate))
				sdf.parse(acqdate);
		}
		catch (ParseException e)
		{
			addImportResult(ImportStatus.MCPD_INVALID_DATE, r.getRowNum(), "ACQDATE: " + acqdate);
		}

		// Check if country is a valid 3-letter code
		String countryCode = getCellValue(r, columnNameToIndex, "ORIGCTY");
		if (!StringUtils.isEmpty(countryCode) && !countryCodeToId.containsKey(countryCode))
			addImportResult(ImportStatus.MCPD_INVALID_COUNTRY_CODE, r.getRowNum(), countryCode);

		// Check if declatitute is a number
		String declatitude = getCellValue(r, columnNameToIndex, "DECLATITUDE");
		if (!StringUtils.isEmpty(declatitude))
		{
			try
			{
				Double.parseDouble(declatitude);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "DECLATITUDE: " + declatitude);
			}
		}

		// Check if declongitude is a number
		String declongitude = getCellValue(r, columnNameToIndex, "DECLONGITUDE");
		if (!StringUtils.isEmpty(declongitude))
		{
			try
			{
				Double.parseDouble(declongitude);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "DECLONGITUDE: " + declongitude);
			}
		}

		// Check if elevation is a valid number
		String elevation = getCellValue(r, columnNameToIndex, "ELEVATION");
		if (!StringUtils.isEmpty(elevation))
		{
			try
			{
				Double.parseDouble(elevation);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "ELEVATION: " + elevation);
			}
		}

		// Check the date is int he correct format
		String colldate = getCellValue(r, columnNameToIndex, "COLLDATE");
		try
		{
			// TODO: Make sure to check "-" values
			if (!StringUtils.isEmpty(colldate))
				sdf.parse(colldate);
		}
		catch (ParseException e)
		{
			addImportResult(ImportStatus.MCPD_INVALID_DATE, r.getRowNum(), "COLLDATE: " + colldate);
		}

		// Check SAMPSTAT
		String sampstat = getCellValue(r, columnNameToIndex, "SAMPSTAT");
		if (!StringUtils.isEmpty(sampstat))
		{
			try
			{
				if (!validSampstat.contains(Integer.parseInt(sampstat)))
					addImportResult(ImportStatus.MCPD_INVALID_SAMPSTAT, r.getRowNum(), sampstat);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "SAMPSTAT: " + sampstat);
			}
		}

		// Check COLLSRC
		String collsrc = getCellValue(r, columnNameToIndex, "COLLSRC");
		if (!StringUtils.isEmpty(collsrc))
		{
			try
			{
				if (!validCollsrc.contains(Integer.parseInt(collsrc)))
					addImportResult(ImportStatus.MCPD_INVALID_COLLSRC, r.getRowNum(), collsrc);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "COLLSRC: " + collsrc);
			}
		}

		// Check STORAGE
		String storage = getCellValue(r, columnNameToIndex, "STORAGE");
		if (!StringUtils.isEmpty(storage))
		{
			try
			{
				if (!validStorage.contains(Integer.parseInt(storage)))
					addImportResult(ImportStatus.MCPD_INVALID_STORAGE, r.getRowNum(), storage);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.MCPD_INVALID_NUMBER, r.getRowNum(), "STORAGE: " + storage);
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
			germinatebaseRecords = context.selectFrom(GERMINATEBASE)
										  .fetchMap(GERMINATEBASE.ID);

			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Check the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(r -> insert(context, r));
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

			// TODO: entity parent
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
			germinatebaseRecords = context.selectFrom(GERMINATEBASE)
										  .fetchMap(GERMINATEBASE.ID);

			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Check the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(this::update);
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

	private void insert(DSLContext context, Row r)
	{
		Germplasm insert = parseMcpd(r);

		TaxonomiesRecord taxonomy = getOrCreateTaxonomy(context, insert.taxonomy);
	}

	private void update(Row r)
	{
		String accenumb = getCellValue(r, columnNameToIndex, "ACCENUMB");
		Integer id = gidToId.get(accenumb);

		Germplasm update = parseMcpd(r);

		// The new data object from the file
		GerminatebaseRecord newStuff = update.germinatebase;
		// The existing record from the database
		GerminatebaseRecord existing = germinatebaseRecords.get(id);

		writeUpdate(existing, newStuff, GERMINATEBASE.ID);
	}

	private void writeUpdate(UpdatableRecord existing, UpdatableRecord newStuff, Field<?> idColumn)
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
			taxonomy.store();
			// Convert result back to POJO
			result = taxonomy;
		}

		return result;
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
//		germplasm.germinatebase.setColldate(getCellValue(r, columnNameToIndex, McpdField.COLLCODE.name()));
		germplasm.institution.setAddress(getCellValue(r, columnNameToIndex, McpdField.COLLINSTADDRESS.name()));
		germplasm.germinatebase.setCollmissid(getCellValue(r, columnNameToIndex, McpdField.COLLMISSID.name()));
		germplasm.germinatebase.setCollname(getCellValue(r, columnNameToIndex, McpdField.COLLNAME.name()));
		germplasm.germinatebase.setCollnumb(getCellValue(r, columnNameToIndex, McpdField.COLLNUMB.name()));
		germplasm.germinatebase.setCollsrcId(getCellValueInteger(r, columnNameToIndex, McpdField.COLLSRC.name()));
		germplasm.location.setCoordinateDatum(getCellValue(r, columnNameToIndex, McpdField.COORDDATUM.name()));
		germplasm.location.setCoordinateUncertainty(getCellValueInteger(r, columnNameToIndex, McpdField.COORDUNCERT.name()));
		germplasm.taxonomy.setCropname(getCellValue(r, columnNameToIndex, McpdField.CROPNAME.name()));
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
//		germplasm.location.setLatitude(getCellValue(r, columnNameToIndex, McpdField.LATITUDE.name()));
//		germplasm.location.setLongitude(getCellValue(r, columnNameToIndex, McpdField.LONGITUDE.name()));
		germplasm.germinatebase.setMlsstatusId(getCellValueInteger(r, columnNameToIndex, McpdField.MLSSTAT.name()));
		germplasm.country.setCountryCode3(getCellValue(r, columnNameToIndex, McpdField.ORIGCTY.name()));
		germplasm.germinatebase.setOthernumb(getCellValue(r, columnNameToIndex, McpdField.OTHERNUMB.name()));
		germplasm.germinatebase.setPuid(getCellValue(r, columnNameToIndex, McpdField.PUID.name()));
//		result.setRemarks(getCellValue(r, columnNameToIndex, McpdField.REMARKS.name()));
		germplasm.germinatebase.setBiologicalstatusId(getCellValueInteger(r, columnNameToIndex, McpdField.SAMPSTAT.name()));
		germplasm.taxonomy.setSpeciesAuthor(getCellValue(r, columnNameToIndex, McpdField.SPAUTHOR.name()));
		germplasm.taxonomy.setSpecies(getCellValue(r, columnNameToIndex, McpdField.SPECIES.name()));
//		result.setStorage(getCellValue(r, columnNameToIndex, McpdField.STORAGE.name()));
		germplasm.taxonomy.setSubtaxaAuthor(getCellValue(r, columnNameToIndex, McpdField.SUBTAUTHOR.name()));
		germplasm.taxonomy.setSubtaxa(getCellValue(r, columnNameToIndex, McpdField.SUBTAXA.name()));
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

		public Germplasm()
		{
		}
	}
}
