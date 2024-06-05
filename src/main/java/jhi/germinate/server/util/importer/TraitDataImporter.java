package jhi.germinate.server.util.importer;

import com.google.gson.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.PhenotypesDatatype;
import jhi.germinate.server.database.codegen.tables.pojos.Phenotypes;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.StringUtils;
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

import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.PHENOTYPES;
import static jhi.germinate.server.database.codegen.tables.Treatments.TREATMENTS;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;
import static jhi.germinate.server.database.codegen.tables.Units.UNITS;

/**
 * @author Sebastian Raubach
 */
public class TraitDataImporter extends DatasheetImporter
{
	/**
	 * Required column headers
	 */
	private static final String[] COLUMN_HEADERS_TRAITS = {"Name", "Short Name", "Description", "Data Type", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};
	private static final String[] COLUMN_HEADERS_DATA   = {"Line/Phenotype", "Rep", "Block", "Row", "Column", "Treatment", "Location", "Latitude", "Longitude", "Elevation"};

	private final Map<String, Integer>    traitNameToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>    germplasmToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>    treatmentToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private       Map<String, Integer>    traitColumnNameToIndex;
	private       Map<String, Integer>    dataColumnNameToIndex;
	private       Map<String, String>     rowColToGermplasm;
	/**
	 * Used to check trait values against trait definitions during checking stage
	 */
	private final Map<String, Phenotypes> traitDefinitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> traitIds     = new HashSet<>();
	private final Set<Integer> germplasmIds = new HashSet<>();

	private List<String> locationNames;

	private int traitColumnStartIndex = 10;

	public static void main(String[] args)
	{
		if (args.length != 6)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		TraitDataImporter importer = new TraitDataImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public TraitDataImporter(Integer importJobId)
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
			context.selectFrom(PHENOTYPES)
				   .forEach(p -> traitNameToId.put(p.getName(), p.getId()));

			context.selectFrom(PHENOTYPES)
				   .fetchInto(Phenotypes.class)
				   .forEach(p -> traitDefinitions.put(p.getName(), p));

			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> germplasmToId.put(g.getName(), g.getId()));

			context.selectFrom(TREATMENTS)
				   .forEach(t -> treatmentToId.put(t.getName(), t.getId()));
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
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "PHENOTYPES"))
			  .findFirst()
			  .ifPresent(s ->
						 {
							 try
							 {
								 // Map headers to their index
								 s.openStream()
								  .findFirst()
								  .ifPresent(this::getTraitHeaderMapping);
								 // Check the sheet
								 s.openStream()
								  .skip(1)
								  .forEachOrdered(this::checkTrait);
							 }
							 catch (IOException e)
							 {
								 addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
							 }
						 });

			this.locationNames = checkLocationSheet(wb.findSheet("LOCATION").orElse(null));

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			checkDataAndRecordingDates(data, dates);
		}
		catch (NullPointerException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'PHENOTYPES' sheet not found");
		}
	}

	private void checkPredefinedHeaders(Row headers)
	{
		// Check the predefined column headers are correct
		if (!dataColumnNameToIndex.containsKey("Line/Phenotype"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Line/Phenotype' column not found");
		if (!dataColumnNameToIndex.containsKey("Rep"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Rep' column not found");
//		if (!dataColumnNameToIndex.containsKey("Block"))
//			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Block' column not found");
		if (!dataColumnNameToIndex.containsKey("Treatment"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Treatment' column not found");
//		if (!dataColumnNameToIndex.containsKey("Location"))
//			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Location' column not found");
//		if (!dataColumnNameToIndex.containsKey("Latitude"))
//			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Latitude' column not found");
//		if (!dataColumnNameToIndex.containsKey("Longitude"))
//			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Longitude' column not found");
//		if (!dataColumnNameToIndex.containsKey("Elevation"))
//			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Elevation' column not found");
	}

	private void checkDataAndRecordingDates(Sheet data, Sheet dates)
	{
		rowColToGermplasm = new HashMap<>();

		try
		{
			if (data == null)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "DATA");
				return;
			}

			data.openStream()
				.findFirst()
				.ifPresent(this::getDataHeaderMapping);

			data.openStream()
				.findFirst()
				.ifPresent(this::checkPredefinedHeaders);

			// Check trait names in data sheet against database and phenotypes sheet
			data.openStream()
				.findFirst()
				.ifPresent(this::checkTraitNames);
			// Check germplasm names in data sheet against the database
			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkGermplasmNameAndRep);

			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkLocationName);

			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkGpsInformation);

			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkRowColumn);

			checkData(data);

			if (dates != null)
			{
				dates.openStream()
					 .findFirst()
					 .ifPresent(this::checkPredefinedHeaders);

				// Check trait names in dates sheet against database and phenotypes sheet
				dates.openStream()
					 .findFirst()
					 .ifPresent(this::checkTraitNames);

				// Check germplasm names in dates sheet against the database
				dates.openStream()
					 .skip(1)
					 .forEachOrdered(this::checkGermplasmNameAndRep);

				List<Row> dataRows = data.read();
				List<Row> datesRows = dates.read();

				long dataCount = dataRows.stream().filter(r -> !allCellsEmpty(r)).count();
				long dateCount = datesRows.stream().filter(r -> !allCellsEmpty(r)).count();

				// If there is date information
				if (dateCount > 1 && datesRows.get(0).getCellCount() > 1)
				{
					// But there aren't the same number of germplasm
					if (dataCount != dateCount)
					{
						addImportResult(ImportStatus.TRIALS_DATA_DATE_IDENTIFIER_MISMATCH, 0, "Number of rows on DATA and RECORDING_DATE sheets don't match");
					}
					else
					{
						boolean areEqual = areEqual(dataRows.get(0), datesRows.get(0));

						if (!areEqual)
						{
							// Header rows aren't identical
							addImportResult(ImportStatus.TRIALS_DATA_DATE_HEADER_MISMATCH, 0, "DATA and RECORDING_DATES headers don't match");
						}
						else
						{
							for (int i = 1; i < dataRows.size(); i++)
							{
								Row datesRow = datesRows.get(i);

								// Germplasm identifier isn't identical
								if (!Objects.equals(getCellValue(dataRows.get(i), 0), getCellValue(datesRow, 0)))
									addImportResult(ImportStatus.TRIALS_DATA_DATE_IDENTIFIER_MISMATCH, i, "DATA and RECORDING_DATES headers don't match");

								for (int c = this.traitColumnStartIndex; c < datesRow.getCellCount(); c++)
								{
									String dateString = getCellValue(datesRow, c);
									Date date = getCellValueDate(datesRow, c);

									if (!StringUtils.isEmpty(dateString) && date == null)
									{
										addImportResult(ImportStatus.GENERIC_INVALID_DATE, i, dateString);
									}
								}
							}
						}
					}
				}
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void checkRowColumn(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String germplasm = getCellValue(r, dataColumnNameToIndex, "Line/Phenotype");
		String row = getCellValue(r, dataColumnNameToIndex, "Row");
		String column = getCellValue(r, dataColumnNameToIndex, "Column");
		String location = getCellValue(r, dataColumnNameToIndex, "Location");

		if (row != null)
		{
			try
			{
				Short.parseShort(row);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "'Row' has invalid numeric value: " + row);
			}
		}

		if (column != null)
		{
			try
			{
				Short.parseShort(column);
			}
			catch (NumberFormatException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "'Column' has invalid numeric value: " + column);
			}
		}

		if (!StringUtils.isEmpty(row) || !StringUtils.isEmpty(column))
		{
			String key = row + "|" + column + "|" + location;
			String value = rowColToGermplasm.get(key);

			if (!StringUtils.isEmpty(value) && !Objects.equals(germplasm, value))
				addImportResult(ImportStatus.TRIALS_ROW_COL_MISMATCH, r.getRowNum(), "Row: " + row + ", Column: " + column + ", Location: " + location);
			else
				rowColToGermplasm.put(key, germplasm);
		}
	}

	private void checkGpsInformation(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String latitude = getCellValue(r, dataColumnNameToIndex, "Latitude");
		String longitude = getCellValue(r, dataColumnNameToIndex, "Longitude");
		String elevation = getCellValue(r, dataColumnNameToIndex, "Elevation");

		Double lat = null;
		Double lng = null;

		try
		{
			lat = Double.parseDouble(latitude);
		}
		catch (NullPointerException e)
		{
			// Ignore
		}
		catch (NumberFormatException e)
		{
			addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "Specified 'Latitude' is not a decimal value: " + latitude);
		}
		try
		{
			lng = Double.parseDouble(longitude);
		}
		catch (NullPointerException e)
		{
			// Ignore
		}
		catch (NumberFormatException e)
		{
			addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "Specified 'Longitude' is not a decimal value: " + longitude);
		}
		try
		{
			Double.parseDouble(elevation);
		}
		catch (NullPointerException e)
		{
			// Ignore
		}
		catch (NumberFormatException e)
		{
			addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "Specified 'Elevation' is not a decimal value: " + elevation);
		}

		if ((lat == null && lng != null) || (lat != null && lng == null))
		{
			addImportResult(ImportStatus.GENERIC_INVALID_LOCATION, r.getRowNum(), "Either 'Latitude' or 'Longitude' is missing.");
		}
	}

	private void checkLocationName(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String location = getCellValue(r, dataColumnNameToIndex, "Location");

		if (!StringUtils.isEmpty(location) && !this.locationNames.contains(location))
			addImportResult(ImportStatus.CLIMATE_MISSING_LOCATION_DECLARATION, r.getRowNum(), "A location referenced in 'DATA' is not defined in 'LOCATION': " + location);
	}

	private void checkGermplasmNameAndRep(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String germplasmName = getCellValue(r, 0);
		String rep = getCellValue(r, 1);

		if (StringUtils.isEmpty(germplasmName))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "ACCENUMB missing");
		else if (!germplasmToId.containsKey(germplasmName))
			addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, r.getRowNum(), germplasmName);

		if (StringUtils.isEmpty(rep))
			addImportResult(ImportStatus.TRIALS_DATA_REP_MISSING, r.getRowNum(), "Rep missing");
	}

	private void checkTraitNames(Row r)
	{
		for (int i = this.traitColumnStartIndex; i < r.getCellCount(); i++)
		{
			String traitName = getCellValue(r, i);
			if (!StringUtils.isEmpty(traitName) && !traitDefinitions.containsKey(traitName))
			{
				addImportResult(ImportStatus.TRIALS_MISSING_TRAIT_DECLARATION, 0, traitName);
			}
		}
	}

	private void getDataHeaderMapping(Row r)
	{
		// Map column names to their index
		dataColumnNameToIndex = IntStream.range(0, r.getCellCount())
										 .filter(i -> !cellEmpty(r, i))
										 .boxed()
										 .collect(Collectors.toMap(r::getCellText, Function.identity()));

		traitColumnStartIndex = (int) Arrays.stream(COLUMN_HEADERS_DATA)
											.filter(h -> dataColumnNameToIndex.containsKey(h))
											.count();
	}

	private void getTraitHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			traitColumnNameToIndex = IntStream.range(0, r.getCellCount())
											  .filter(i -> !cellEmpty(r, i))
											  .boxed()
											  .collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS_TRAITS)
				  .forEach(c ->
						   {
							   if (!traitColumnNameToIndex.containsKey(c))
								   addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, -1, c);
						   });
		}
		catch (IllegalStateException e)
		{
			addImportResult(ImportStatus.GENERIC_DUPLICATE_COLUMN, 1, e.getMessage());
		}
	}

	private void checkTrait(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, traitColumnNameToIndex.get("Name"));

		if (StringUtils.isEmpty(name))
			return;

		String description = getCellValue(r, traitColumnNameToIndex.get("Description"));
		String shortName = getCellValue(r, traitColumnNameToIndex.get("Short Name"));
		String dataType = getCellValue(r, traitColumnNameToIndex.get("Data Type"));
		String unitAbbr = getCellValue(r, traitColumnNameToIndex.get("Unit Abbreviation"));
		String unitName = getCellValue(r, traitColumnNameToIndex.get("Unit Name"));
		String categories = getCellValue(r, traitColumnNameToIndex.get("Trait categories (comma separated)"));
		String minimum = getCellValue(r, traitColumnNameToIndex.get("Min (only for numeric traits)"));
		String maximum = getCellValue(r, traitColumnNameToIndex.get("Max (only for numeric traits)"));
		TraitRestrictions restrictions = null;

		if (StringUtils.isEmpty(name))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Name: " + name);

		if (!StringUtils.isEmpty(shortName) && shortName.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Short name: " + shortName + " exceeds 10 characters.");

		if (!StringUtils.isEmpty(name) && name.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Name: " + name + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(unitName) && unitName.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Name: " + unitName + " exceeds 255 characters.");

		PhenotypesDatatype dt;
		try
		{
			dt = getDataType(dataType);
		}
		catch (Exception e)
		{
			dt = PhenotypesDatatype.text;
			addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_DATATYPE, r.getRowNum(), "Data Type: " + dataType);
		}

		if (!StringUtils.isEmpty(unitAbbr) && unitAbbr.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Abbreviation: " + unitAbbr + " exceeds 10 characters.");

		if (!StringUtils.isEmpty(categories))
		{
			try
			{
				// Try to parse it
				String[][] cats = new Gson().fromJson(categories, String[][].class);

				if (cats != null && cats.length > 1)
				{
					for (int i = 1; i < cats.length; i++)
					{
						if (cats[i - 1].length != cats[i].length)
						{
							addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_CATEGORIES, r.getRowNum(), "Trait categories: " + categories + " has invalid format.");
						}
					}

					restrictions = new TraitRestrictions();
					restrictions.setCategories(cats);
				}
			}
			catch (JsonSyntaxException | NullPointerException e)
			{
				e.printStackTrace();
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

		// Remember the name, cause we need to check the data sheets against them
		Phenotypes trait = null;
		try
		{
			trait = new Phenotypes();
			trait.setName(name);
			trait.setShortName(shortName);
			trait.setDescription(description);
			trait.setDatatype(dt);
			trait.setRestrictions(restrictions);
		}
		catch (Exception e)
		{
		}
		traitDefinitions.put(name, trait);
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
				// Get the data type for each column
				List<PhenotypesDatatype> dataTypes = headers.stream()
															.skip(this.traitColumnStartIndex)
															.map(this::getCellValue)
															.filter(c -> !StringUtils.isEmpty(c))
															.map(c -> traitDefinitions.get(c).getDatatype())
															.collect(Collectors.toList());

				// Now check them to make sure their content fits the data type
				s.openStream()
				 .skip(1)
				 .forEachOrdered(r -> {
					 for (int i = this.traitColumnStartIndex; i < r.getPhysicalCellCount(); i++)
					 {
						 String cellValue = getCellValue(r, i);

						 if (StringUtils.isEmpty(cellValue))
							 continue;

						 switch (dataTypes.get(i - this.traitColumnStartIndex))
						 {
							 case numeric:
								 try
								 {
									 Double.parseDouble(cellValue);
								 }
								 catch (NumberFormatException e)
								 {
									 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Value of a numeric trait isn't a number: " + cellValue);
								 }
								 break;
							 case date:
								 Date date = getCellValueDate(r, i);
								 if (date == null)
									 addImportResult(ImportStatus.GENERIC_INVALID_DATE, r.getRowNum(), "Value of a date trait isn't a date: " + cellValue);
								 break;

							 case categorical:
							 case text:
							 default:
								 // Do nothing here
						 }
					 }
				 });

				// Get all the traits that have restrictions
				List<String> traitsWithRestrictions = traitDefinitions.values().stream()
																	  .filter(t -> !StringUtils.isEmpty(t.getName()))
																	  .filter(t -> t.getRestrictions() != null)
																	  .map(Phenotypes::getName)
																	  .collect(Collectors.toList());

				if (traitsWithRestrictions.size() > 0)
				{
					// Store trait name to column index mapping
					Map<String, Integer> traitIndex = new HashMap<>();
					for (int i = this.traitColumnStartIndex; i < headers.getPhysicalCellCount(); i++)
						traitIndex.put(getCellValue(headers, i), i);

					s.openStream()
					 .skip(1)
					 .forEachOrdered(r -> {
						 traitsWithRestrictions.forEach(t -> {
							 Integer index = traitIndex.get(t);

							 if (index == null)
								 return;

							 TraitRestrictions restrictions = traitDefinitions.get(t).getRestrictions();
							 String cellValue = getCellValue(r, index);

							 if (StringUtils.isEmpty(cellValue))
								 return;

							 // Check minimum restriction
							 if (restrictions.getMin() != null)
							 {
								 try
								 {
									 double value = Double.parseDouble(cellValue);
									 if (value < restrictions.getMin())
										 addImportResult(ImportStatus.TRIALS_DATA_VIOLATES_RESTRICTION, r.getRowNum(), "Data point above valid maximum (" + t + "): " + +value + " < " + restrictions.getMin());
								 }
								 catch (NumberFormatException e)
								 {
									 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Value of a numeric trait isn't a number: " + cellValue);
								 }
							 }
							 // Check maximum restriction
							 if (restrictions.getMax() != null)
							 {
								 try
								 {
									 double value = Double.parseDouble(cellValue);
									 if (value > restrictions.getMax())
										 addImportResult(ImportStatus.TRIALS_DATA_VIOLATES_RESTRICTION, r.getRowNum(), "Data point above valid maximum (" + t + "): " + value + " > " + restrictions.getMax());
								 }
								 catch (NumberFormatException e)
								 {
									 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Value of a numeric trait isn't a number: " + cellValue);
								 }
							 }
							 // Check categorical set restriction
							 if (restrictions.getCategories() != null)
							 {
								 boolean found = false;
								 outer:
								 for (String[] cat : restrictions.getCategories())
								 {
									 for (String possValue : cat)
									 {
										 if (Objects.equals(cellValue, possValue))
										 {
											 found = true;
											 break outer;
										 }
									 }
								 }

								 if (!found)
									 addImportResult(ImportStatus.TRIALS_DATA_VIOLATES_RESTRICTION, r.getRowNum(), "Cell value not within valid category range: " + cellValue + " not in " + restrictions.toString());
							 }
						 });
					 });
				}
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
			wb.findSheet("PHENOTYPES")
			  .ifPresent(s -> {
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .findFirst()
					   .ifPresent(this::getTraitHeaderMapping);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }

				  importTraits(context, s);
			  });

			wb.findSheet("DATA")
			  .ifPresent(s -> {
				  try
				  {
					  s.openStream()
					   .findFirst()
					   .ifPresent(this::getDataHeaderMapping);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }

				  importTreatments(context, s);
			  });

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			importData(context, data, dates);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void importTreatments(DSLContext context, Sheet s)
	{
		try
		{
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String treatment = getCellValue(r, dataColumnNameToIndex, "Treatment");

				 if (!StringUtils.isEmpty(treatment) && !treatmentToId.containsKey(treatment))
				 {
					 TreatmentsRecord tRecord = context.newRecord(TREATMENTS);
					 tRecord.setName(treatment);
					 tRecord.setDescription(treatment);
					 tRecord.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					 tRecord.store();

					 treatmentToId.put(treatment, tRecord.getId());
				 }
			 });
		}
		catch (
				IOException e)

		{
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

				 String name = getCellValue(r, traitColumnNameToIndex.get("Name"));

				 if (StringUtils.isEmpty(name))
					 return;

				 String shortName = getCellValue(r, traitColumnNameToIndex.get("Short Name"));
				 String description = getCellValue(r, traitColumnNameToIndex.get("Description"));
				 String dataTypeString = getCellValue(r, traitColumnNameToIndex.get("Data Type"));
				 PhenotypesDatatype dataType = PhenotypesDatatype.text;
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

				 String unitName = getCellValue(r, traitColumnNameToIndex.get("Unit Name"));
				 String unitAbbr = getCellValue(r, traitColumnNameToIndex.get("Unit Abbreviation"));
				 String unitDescription = getCellValue(r, traitColumnNameToIndex.get("Unit Descriptions"));

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
				 String categories = getCellValue(r, traitColumnNameToIndex.get("Trait categories (comma separated)"));
				 String minimum = getCellValue(r, traitColumnNameToIndex.get("Min (only for numeric traits)"));
				 String maximum = getCellValue(r, traitColumnNameToIndex.get("Max (only for numeric traits)"));

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

				 SelectConditionStep<PhenotypesRecord> query = context.selectFrom(PHENOTYPES)
																	  .where(PHENOTYPES.NAME.isNotDistinctFrom(name))
																	  .and(PHENOTYPES.DATATYPE.isNotDistinctFrom(dataType));

				 if (unit != null)
					 query.and(PHENOTYPES.UNIT_ID.isNotDistinctFrom(unit.getId()));
				 if (!StringUtils.isEmpty(description))
					 query.and(PHENOTYPES.DESCRIPTION.isNotDistinctFrom(description));
				 if (!StringUtils.isEmpty(shortName))
					 query.and(PHENOTYPES.SHORT_NAME.isNotDistinctFrom(shortName));

//				 if (restrictions != null)
//					 query.and(PHENOTYPES.RESTRICTIONS.isNotDistinctFrom(restrictions));

				 List<Phenotypes> potentialMatches = query.fetchInto(Phenotypes.class);

				 Phenotypes match = findMatch(restrictions, potentialMatches);

				 if (match != null) {
					 traitIds.add(match.getId());
					 traitNameToId.put(match.getName(), match.getId());
				 } else {
					 PhenotypesRecord trait = context.newRecord(PHENOTYPES);
					 trait.setName(name);
					 trait.setShortName(shortName);
					 trait.setDescription(description);
					 trait.setDatatype(dataType);
					 trait.setUnitId(unit == null ? null : unit.getId());
					 trait.setRestrictions(restrictions);
					 trait.store();

					 traitIds.add(trait.getId());
					 traitNameToId.put(trait.getName(), trait.getId());
				 }
			 });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private Phenotypes findMatch(TraitRestrictions restrictions, List<Phenotypes> potentialMatches) {
		if (restrictions != null)
		{
			for (Phenotypes p : potentialMatches)
			{
				if (p.getRestrictions() != null)
				{
					if (Objects.equals(p.getRestrictions().getMin(), restrictions.getMin())
							&& Objects.equals(p.getRestrictions().getMax(), restrictions.getMax()))
					{
						if (p.getRestrictions().getCategories() == null && restrictions.getCategories() == null)
						{
							return p;
						}
						else if (p.getRestrictions().getCategories() != null && restrictions.getCategories() != null)
						{
							if (p.getRestrictions().getCategories().length == restrictions.getCategories().length)
							{
								List<Set<String>> reference = new ArrayList<>();

								int matchCount = 0;

								for (String[] s : p.getRestrictions().getCategories()) {
									Set<String> t = new HashSet<>();
									for (String st : s) {
										t.add(st);
									}
									reference.add(t);
								}

								for (String[] s : p.getRestrictions().getCategories()) {
									Set<String> t = new HashSet<>();
									for (String st : s) {
										t.add(st);
									}

									for (Set<String> ref : reference) {
										Set<String> intersection = new HashSet<>(t);
										intersection.retainAll(ref);

										if (intersection.size() == t.size()) {
											matchCount++;
											break;
										}
									}
								}

								if (matchCount == p.getRestrictions().getCategories().length) {
									return p;
								}
							}
						}
					}
				}
			}
		}
		else
		{
			return potentialMatches.get(0);
		}

		return null;
	}

	private void importData(DSLContext context, Sheet data, Sheet dates)
	{
		try
		{
			// Before we start, let's check the headers again to set the correct trait start index
			data.openStream()
				.findFirst()
				.ifPresent(this::checkPredefinedHeaders);

			List<Row> dataRows = data.read();
			List<Row> datesRows = null;

			if (dates != null)
				datesRows = dates.read();

			if (datesRows != null && (datesRows.size() < 2 || datesRows.get(0).getCellCount() < 4))
				datesRows = null;

			Row headerRow = dataRows.get(0);

			Map<Integer, Integer> rowToTrialsetupId = new HashMap<>();

			for (int r = 1; r < dataRows.size(); r++)
			{
				Row dataRow = dataRows.get(r);

				if (allCellsEmpty(dataRow))
					continue;

				String germplasmName = getCellValue(dataRow, dataColumnNameToIndex, "Line/Phenotype");
				String rep = getCellValue(dataRow, dataColumnNameToIndex, "Rep");
				String block = getCellValue(dataRow, dataColumnNameToIndex, "Block");
				Short row = getCellValueShort(dataRow, dataColumnNameToIndex, "Row");
				Short column = getCellValueShort(dataRow, dataColumnNameToIndex, "Column");
				BigDecimal latitude = getCellValueBigDecimal(dataRow, dataColumnNameToIndex, "Latitude");
				BigDecimal longitude = getCellValueBigDecimal(dataRow, dataColumnNameToIndex, "Longitude");
				BigDecimal elevation = getCellValueBigDecimal(dataRow, dataColumnNameToIndex, "Elevation");
				if (StringUtils.isEmpty(rep))
					rep = "1";
				if (StringUtils.isEmpty(block))
					block = "1";
				String locationName = getCellValue(dataRow, dataColumnNameToIndex, "Location");
				Integer germplasmId = germplasmToId.get(germplasmName);
				String treatmentName = getCellValue(dataRow, dataColumnNameToIndex, "Treatment");
				Integer treatmentId = null;

				germplasmIds.add(germplasmId);

				if (!StringUtils.isEmpty(treatmentName))
					treatmentId = treatmentToId.get(treatmentName);

				TrialsetupRecord ts = context.newRecord(TRIALSETUP);
				ts.setGerminatebaseId(germplasmId);
				ts.setRep(rep);
				ts.setBlock(block);
				ts.setTrialRow(row);
				ts.setTrialColumn(column);
				ts.setLatitude(latitude);
				ts.setLongitude(longitude);
				ts.setElevation(elevation);
				ts.setTreatmentId(treatmentId);
				ts.setDatasetId(dataset.getId());
				if (!StringUtils.isEmpty(locationName))
					ts.setLocationId(this.locationNameToId.get(locationName));

				ts.store();

				rowToTrialsetupId.put(r, ts.getId());
			}

			List<PhenotypedataRecord> newData = new ArrayList<>();

			for (int r = 1; r < dataRows.size(); r++)
			{
				Row dataRow = dataRows.get(r);
				Row datesRow = (datesRows == null || r > datesRows.size() - 1) ? null : datesRows.get(r);

				if (allCellsEmpty(dataRow))
					continue;

				for (int c = this.traitColumnStartIndex; c < dataRow.getCellCount(); c++)
				{
					String name = getCellValue(headerRow, c);

					if (StringUtils.isEmpty(name))
						continue;

					Integer traitId = traitNameToId.get(name);

					String value = getCellValue(dataRow, c);

					if (StringUtils.isEmpty(value))
						continue;

					Date date = null;

					if (datesRow != null)
						date = getCellValueDate(datesRow, c);

					PhenotypedataRecord record = context.newRecord(PHENOTYPEDATA);
					record.setTrialsetupId(rowToTrialsetupId.get(r));
					record.setPhenotypeId(traitId);
					record.setPhenotypeValue(value);
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

				if (!newData.isEmpty())
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
		return 3;
	}

	@Override
	protected void postImport()
	{
		super.postImport();

		importJobStats.setDatasetId(dataset.getId());
		importJobStats.setTraits(traitIds.size());
		importJobStats.setGermplasm(germplasmIds.size());
	}

	private PhenotypesDatatype getDataType(String dt)
	{
		PhenotypesDatatype result = null;

		if (Objects.equals(dt, "int") || Objects.equals(dt, "float") || Objects.equals(dt, "numeric"))
		{
			result = PhenotypesDatatype.numeric;
		}
		else if (Objects.equals(dt, "char") || Objects.equals(dt, "text"))
		{
			result = PhenotypesDatatype.text;
		}
		else if (Objects.equals(dt, "date"))
		{
			result = PhenotypesDatatype.date;
		}
		else if (Objects.equals(dt, "categorical"))
		{
			result = PhenotypesDatatype.categorical;
		}

		if (result == null)
			throw new IllegalArgumentException();
		else
			return result;
	}
}
