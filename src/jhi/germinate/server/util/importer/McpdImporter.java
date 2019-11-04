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
import jhi.germinate.server.database.tables.pojos.Germinatebase;
import jhi.germinate.server.database.tables.records.GerminatebaseRecord;
import jhi.germinate.server.util.StringUtils;

import static jhi.germinate.server.database.tables.Biologicalstatus.*;
import static jhi.germinate.server.database.tables.Collectingsources.*;
import static jhi.germinate.server.database.tables.Countries.*;
import static jhi.germinate.server.database.tables.Entitytypes.*;
import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Storage.*;

/**
 * @author Sebastian Raubach
 */
public class McpdImporter extends AbstractImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"PUID", "INSTCODE", "ACCENUMB", "COLLNUMB", "COLLCODE", "COLLNAME", "COLLINSTADDRESS", "COLLMISSID", "GENUS", "SPECIES", "SPAUTHOR", "SUBTAXA", "SUBTAUTHOR", "CROPNAME", "ACCENAME", "ACQDATE", "ORIGCTY", "COLLSITE", "DECLATITUDE", "LATITUDE", "DECLONGITUDE", "LONGITUDE", "COORDUNCERT", "COORDDATUM", "GEOREFMETH", "ELEVATION", "COLLDATE", "BREDCODE", "BREDNAME", "SAMPSTAT", "ANCEST", "COLLSRC", "DONORCODE", "DONORNAME", "DONORNUMB", "OTHERNUMB", "DUPLSITE", "DUPLINSTNAME", "STORAGE", "MLSSTAT", "REMARKS"};

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	private Map<String, Integer> columnNameToIndex;
	private Set<String>          foundAccenumb = new HashSet<>();
	private Map<String, Integer> gidToId;
	private Map<String, Integer> countryCodeToId;
	private List<Integer>        validCollsrc;
	private List<Integer>        validSampstat;
	private List<Integer>        validStorage;
	private Map<String, Integer> entityTypeToId;

	public McpdImporter(File input, boolean isUpdate)
	{
		super(input, isUpdate);
	}

	public static void main(String[] args)
	{
		Database.init("localhost", "germinate_demo_api", null, "root", null, false);
		File file = new File("D:\\workspaces\\web-intellij\\germinate\\datatemplates\\faulty-example-germplasm-mcpd.xlsx");
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
		String accenumb = getCellValue(r, columnNameToIndex, "ACCENUMB");
		if (StringUtils.isEmpty(accenumb))
			addImportResult(ImportStatus.MCPD_MISSING_FIELD, r.getRowNum(), "ACCENUMB");
		else
			foundAccenumb.add(accenumb);

		boolean alreadyFoundInFile = foundAccenumb.contains(accenumb);
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
		// TODO
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		// TODO

		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "DATA"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // Check the sheet
					  s.openStream()
					   .skip(1)
					   .forEachOrdered(r -> update(context, r));
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

	private void update(DSLContext context, Row row)
	{
		// TODO
		// The new data object from the file
		Germinatebase a = new Germinatebase();
		// The existing record from the database
		GerminatebaseRecord b = new GerminatebaseRecord();

		// Load the changes into the record
		b.from(a);

		// Remember which fields to update
		List<Field<?>> toUpdate = new ArrayList<>();
		for (int i = 0; i < b.fields().length; i++)
		{
			// If the value has changed and it hasn't changed to null (that would mean that the update just didn't specify anything for this field), then remember the field.
			if (!Objects.equals(b.get(i), b.original(i)) && b.get(i) != null)
				toUpdate.add(b.field(i));
		}

		// Update the changed fields
		b.store(toUpdate);

	}
}
