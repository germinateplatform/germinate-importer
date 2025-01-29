package jhi.germinate.server.util.importer;

import com.google.gson.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.ClimatesDatatype;
import jhi.germinate.server.database.codegen.tables.pojos.Climates;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.StringUtils;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import static jhi.germinate.server.database.codegen.tables.Climatedata.*;
import static jhi.germinate.server.database.codegen.tables.Climates.*;
import static jhi.germinate.server.database.codegen.tables.Units.*;

/**
 * @author Sebastian Raubach
 */
public class ClimateDataImporter extends DatasheetImporter
{
	/**
	 * Required column headers
	 */
	private static final String[] COLUMN_HEADERS = {"Name", "Short Name", "Description", "Data Type", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};

	private List<String>          locationNames;
	private Map<String, Integer>  climateNameToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Integer>  columnNameToIndex;
	/**
	 * Used to check climate values against climate definitions during checking stage
	 */
	private Map<String, Climates> climateDefinitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> locationIds = new HashSet<>();
	private final Set<Integer> climateIds  = new HashSet<>();

	public static void main(String[] args)
	{
		if (args.length != 6)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		ClimateDataImporter importer = new ClimateDataImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public ClimateDataImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(CLIMATES)
				   .forEach(p -> climateNameToId.put(p.getName(), p.getId()));

			context.selectFrom(CLIMATES)
				   .fetchInto(Climates.class)
				   .forEach(p -> climateDefinitions.put(p.getName(), p));
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
		super.checkFile(wb);

		try
		{
			Optional<Sheet> climateSheet = wb.getSheets().filter(s -> Objects.equals(s.getName(), "ENVIRONMENTAL VARIABLES")).findAny();

			if (climateSheet.isEmpty())
				climateSheet = wb.getSheets().filter(s -> Objects.equals(s.getName(), "CLIMATES")).findAny();

			climateSheet.ifPresent(s ->
			{
				try
				{
					// Map headers to their index
					s.openStream()
					 .findFirst()
					 .ifPresent(this::getHeaderMapping);
					// Check the sheet
					s.openStream()
					 .skip(1)
					 .forEachOrdered(this::checkClimate);
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}
			});

			this.locationNames = checkLocationSheet(wb.findSheet("LOCATION").orElse(null));

			checkDataSheet(wb.findSheet("DATA").orElse(null));
		}
		catch (NullPointerException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'CLIMATES' sheet not found");
		}
	}

	private void checkDataSheet(Sheet data)
	{
		try
		{
			if (data == null)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'DATA' sheet not found");
				return;
			}

			// Check climate names in data sheet against database and phenotypes sheet
			data.openStream()
				.findFirst()
				.ifPresent(this::checkClimateNames);
			// Check location names in data sheet against the database
			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkLocationNamesAndDates);

			checkData(data);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void checkLocationNamesAndDates(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String location = getCellValue(r, 0);
		Date date = getCellValueDate(r, 1);

		if (StringUtils.isEmpty(location))
		{
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Location name is missing in 'DATA' sheet.");
		}
		else
		{
			boolean definitionFound = locationNames.contains(location);
			if (!definitionFound)
				addImportResult(ImportStatus.CLIMATE_MISSING_LOCATION_DECLARATION, r.getRowNum(), "A location referenced in 'DATA' is not defined in 'LOCATION': " + location);
		}
		if (date == null)
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "'Date' value is missing.");
	}

	private void checkClimateNames(Row r)
	{
		String date = getCellValue(r, 1);

		if (!Objects.equals(date, "Date"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Date' column is missing");

		for (int i = 2; i < r.getCellCount(); i++)
		{
			String climateName = getCellValue(r, i);
			if (!StringUtils.isEmpty(climateName) && !climateDefinitions.containsKey(climateName))
			{
				addImportResult(ImportStatus.CLIMATE_MISSING_CLIMATE_DECLARATION, 0, climateName);
			}
		}
	}

	private void getHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			columnNameToIndex = IntStream.range(0, r.getCellCount())
										 .filter(i -> !cellEmpty(r, i))
										 .boxed()
										 .collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS)
				  .forEach(c ->
				  {
					  if (!columnNameToIndex.containsKey(c))
						  addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, -1, c);
				  });
		}
		catch (IllegalStateException e)
		{
			addImportResult(ImportStatus.GENERIC_DUPLICATE_COLUMN, 1, e.getMessage());
		}
	}

	private void checkClimate(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, columnNameToIndex.get("Name"));

		if (StringUtils.isEmpty(name))
			return;

		String description = getCellValue(r, columnNameToIndex.get("Description"));
		String shortName = getCellValue(r, columnNameToIndex.get("Short Name"));
		String dataType = getCellValue(r, columnNameToIndex.get("Data Type"));
		String unitAbbr = getCellValue(r, columnNameToIndex.get("Unit Abbreviation"));
		String unitName = getCellValue(r, columnNameToIndex.get("Unit Name"));

		if (StringUtils.isEmpty(name))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Name: " + name);

		if (!StringUtils.isEmpty(shortName) && shortName.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Short name: " + shortName + " exceeds 10 characters.");

		if (!StringUtils.isEmpty(name) && name.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Name: " + name + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(unitName) && unitName.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Name: " + unitName + " exceeds 255 characters.");


		try
		{
			getDataType(dataType);
		}
		catch (IllegalArgumentException e)
		{
			addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_DATATYPE, r.getRowNum(), "Data Type: " + dataType);
		}

		if (!StringUtils.isEmpty(unitAbbr) && unitAbbr.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Abbreviation: " + unitAbbr + " exceeds 10 characters.");

		// Remember the name, cause we need to check the data sheets against them
		Climates climate = null;
		try
		{
			climate = new Climates();
			climate.setName(name);
			climate.setShortName(shortName);
			// TODO: other fields
			climate.setDescription(description);
			try
			{
				climate.setDatatype(getDataType(dataType));
			}
			catch (IllegalArgumentException e)
			{
			}
		}
		catch (Exception e)
		{
		}
		climateDefinitions.put(name, climate);
	}

	private void checkData(Sheet s)
	{
		try
		{
			// Get the header row
			Row headers = s.openStream()
						   .findFirst()
						   .orElse(null);

			if (headers != null)
			{
				List<String> climateHeaders = headers.stream()
											   .skip(2)
											   .map(this::getCellValue)
											   .filter(c -> !StringUtils.isEmpty(c))
											   .toList();
				// Get the data type for each column
				List<ClimatesDatatype> dataTypes = climateHeaders.stream()
														  .map(c -> climateDefinitions.get(c).getDatatype())
														  .toList();

				// Now check them to make sure their content fits the data type
				s.openStream()
				 .skip(1)
				 .forEachOrdered(r -> {
					 for (int i = 2; i < r.getPhysicalCellCount(); i++)
					 {
						 String cellValue = getCellValue(r, i);

						 if (StringUtils.isEmpty(cellValue))
							 continue;

						 switch (dataTypes.get(i - 2))
						 {
							 case numeric:
								 try
								 {
									 Double.parseDouble(cellValue);
								 }
								 catch (NumberFormatException e)
								 {
									 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Value of a numeric climate " + climateHeaders.get(i - 2) +" isn't a number: " + cellValue);
								 }
								 break;
							 case date:
								 Date date = getCellValueDate(r, i);
								 if (date == null)
									 addImportResult(ImportStatus.GENERIC_INVALID_DATE, r.getRowNum(), "Value of a date climate " + climateHeaders.get(i - 2) +" isn't a date: " + cellValue);
								 break;

							 case categorical:
							 case text:
							 default:
								 // Do nothing here
						 }
					 }
				 });
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			Optional<Sheet> climateSheet = wb.getSheets().filter(s -> Objects.equals(s.getName(), "ENVIRONMENTAL VARIABLES")).findAny();

			if (climateSheet.isEmpty())
				climateSheet = wb.getSheets().filter(s -> Objects.equals(s.getName(), "CLIMATES")).findAny();

			climateSheet.ifPresent(s -> {
				try
				{
					// Map headers to their index
					s.openStream()
					 .findFirst()
					 .ifPresent(this::getHeaderMapping);
				}
				catch (IOException e)
				{
					addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				}

				importTraits(context, s);
			});

			Sheet data = wb.findSheet("DATA").orElse(null);
			importData(context, data);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void importTraits(DSLContext context, Sheet s)
	{
		try
		{
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String name = getCellValue(r, columnNameToIndex.get("Name"));

				 if (StringUtils.isEmpty(name))
					 return;

				 String shortName = getCellValue(r, columnNameToIndex.get("Short Name"));
				 String description = getCellValue(r, columnNameToIndex.get("Description"));
				 String dataTypeString = getCellValue(r, columnNameToIndex.get("Data Type"));
				 ClimatesDatatype dataType = ClimatesDatatype.text;
				 if (!StringUtils.isEmpty(dataTypeString))
				 {
					 try
					 {
						 dataType = getDataType(dataTypeString);
					 }
					 catch (IllegalArgumentException e)
					 {
					 }
				 }

				 String unitName = getCellValue(r, columnNameToIndex.get("Unit Name"));
				 String unitAbbr = getCellValue(r, columnNameToIndex.get("Unit Abbreviation"));
				 String unitDescription = getCellValue(r, columnNameToIndex.get("Unit Descriptions"));

				 UnitsRecord unit = context.selectFrom(UNITS)
										   .where(UNITS.UNIT_NAME.isNotDistinctFrom(unitName))
										   .and(UNITS.UNIT_DESCRIPTION.isNotDistinctFrom(unitDescription))
										   .and(UNITS.UNIT_ABBREVIATION.isNotDistinctFrom(unitAbbr))
										   .fetchAny();

				 if (!StringUtils.isEmpty(unitName) && unit == null)
				 {
					 unit = context.newRecord(UNITS);
					 unit.setUnitName(unitName);
					 unit.setUnitDescription(unitDescription);
					 unit.setUnitAbbreviation(unitAbbr);
					 unit.store();
				 }

				 TraitRestrictions restrictions = null;
				 String categories = getCellValue(r, columnNameToIndex.get("Trait categories (comma separated)"));
				 String minimum = getCellValue(r, columnNameToIndex.get("Min (only for numeric climates)"));
				 String maximum = getCellValue(r, columnNameToIndex.get("Max (only for numeric climates)"));

				 if (!StringUtils.isEmpty(categories))
				 {
					 try
					 {
						 // Try to parse it
						 String[][] cats = new Gson().fromJson(categories, String[][].class);

						 if (cats != null && cats.length > 0)
						 {
							 restrictions = new TraitRestrictions();
							 restrictions.setCategories(cats);
						 }
					 }
					 catch (JsonSyntaxException | NullPointerException e)
					 {
						 addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_CATEGORIES, r.getRowNum(), "Trait categories: " + categories + " has invalid format.");
					 }
				 }

				 if (!StringUtils.isEmpty(minimum))
				 {
					 try
					 {
						 double min = Double.parseDouble(minimum);
						 if (restrictions == null)
							 restrictions = new TraitRestrictions();
						 restrictions.setMin(min);
					 }
					 catch (NumberFormatException e)
					 {
						 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Minimum isn't a valid number: " + minimum);
					 }
				 }

				 if (!StringUtils.isEmpty(maximum))
				 {
					 try
					 {
						 double max = Double.parseDouble(maximum);
						 if (restrictions == null)
							 restrictions = new TraitRestrictions();
						 restrictions.setMax(max);
					 }
					 catch (NumberFormatException e)
					 {
						 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Maximum isn't a valid number: " + maximum);
					 }
				 }

				 ClimatesRecord climate = context.selectFrom(CLIMATES)
												 .where(CLIMATES.NAME.isNotDistinctFrom(name))
												 .and(CLIMATES.SHORT_NAME.isNotDistinctFrom(shortName))
												 .and(CLIMATES.DESCRIPTION.isNotDistinctFrom(description))
												 .and(CLIMATES.DATATYPE.isNotDistinctFrom(dataType))
												 .and(CLIMATES.UNIT_ID.isNotDistinctFrom(unit == null ? null : unit.getId()))
												 .fetchAny();

				 if (climate == null)
				 {
					 climate = context.newRecord(CLIMATES);
					 climate.setName(name);
					 climate.setShortName(shortName);
					 climate.setDescription(description);
					 climate.setDatatype(dataType);
					 climate.setUnitId(unit == null ? null : unit.getId());
					 climate.store();
				 }

				 climateIds.add(climate.getId());

				 climateNameToId.put(climate.getName(), climate.getId());
			 });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void importData(DSLContext context, Sheet data)
	{
		try
		{
			List<Row> dataRows = data.read();

			List<ClimatedataRecord> newData = new ArrayList<>();

			Row headerRow = dataRows.get(0);

			for (int r = 1; r < dataRows.size(); r++)
			{
				Row dataRow = dataRows.get(r);

				if (allCellsEmpty(dataRow))
					continue;

				String locationName = getCellValue(dataRow, 0);
				Date date = getCellValueDate(dataRow, 1);
				Integer locationId = locationNameToId.get(locationName);

				locationIds.add(locationId);

				for (int c = 2; c < dataRow.getCellCount(); c++)
				{
					String name = getCellValue(headerRow, c);

					if (StringUtils.isEmpty(name))
						continue;

					Integer climateId = climateNameToId.get(name);

					Double value = getCellValueDouble(dataRow, c);

					if (value == null)
						continue;

					ClimatedataRecord record = context.newRecord(CLIMATEDATA);
					record.setLocationId(locationId);
					record.setClimateId(climateId);
					record.setDatasetId(dataset.getId());
					record.setClimateValue(value);
					if (date != null)
						record.setRecordingDate(new Timestamp(date.getTime()));

					newData.add(record);

					if (newData.size() >= 10000)
					{
						context.batchStore(newData)
							   .execute();
						newData.clear();
					}
				}

				if (newData.size() > 0)
				{
					context.batchStore(newData)
						   .execute();
					newData.clear();
				}
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		// We don't support updating, so just import
		importFile(wb);
	}

	@Override
	protected int getDatasetTypeId()
	{
		return 5;
	}

	@Override
	protected void postImport()
	{
		super.postImport();

		importJobStats.setDatasetId(dataset.getId());
		importJobStats.setClimates(climateIds.size());
		importJobStats.setLocations(locationIds.size());
	}

	private ClimatesDatatype getDataType(String dt)
	{
		ClimatesDatatype result = null;

		if (Objects.equals(dt, "int") || Objects.equals(dt, "float") || Objects.equals(dt, "numeric"))
		{
			result = ClimatesDatatype.numeric;
		}
		else if (Objects.equals(dt, "char") || Objects.equals(dt, "text"))
		{
			result = ClimatesDatatype.text;
		}
		else if (Objects.equals(dt, "date"))
		{
			result = ClimatesDatatype.date;
		}
		else if (Objects.equals(dt, "categorical"))
		{
			result = ClimatesDatatype.categorical;
		}

		if (result == null)
			throw new IllegalArgumentException();
		else
			return result;
	}
}
