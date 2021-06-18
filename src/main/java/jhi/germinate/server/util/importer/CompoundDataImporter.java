package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import static jhi.germinate.server.database.codegen.tables.Compounddata.*;
import static jhi.germinate.server.database.codegen.tables.Compounds.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Units.*;

/**
 * @author Sebastian Raubach
 */
public class CompoundDataImporter extends DatasheetImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"Name", "Description", "Molecular Formula", "Monoisotopic Mass", "Class", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};

	private Map<String, Integer> compoundNameToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Integer> germplasmToId    = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Integer> columnNameToIndex;
	private Set<String>          compoundNames;

	public static void main(String[] args)
	{
		if (args.length != 10)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		CompoundDataImporter importer = new CompoundDataImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]));
		importer.init(args);
		importer.run(RunType.getType(args[8]));
	}

	public CompoundDataImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		super(input, isUpdate, deleteOnFail, userId);
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(COMPOUNDS)
				   .forEach(c -> compoundNameToId.put(c.getName(), c.getId()));

			compoundNames = context.selectFrom(COMPOUNDS)
								   .fetchSet(COMPOUNDS.NAME);

			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> germplasmToId.put(g.getName(), g.getId()));
		}
		catch (SQLException e) {
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
			  .filter(s -> Objects.equals(s.getName(), "COMPOUNDS"))
			  .findFirst()
			  .ifPresent(s ->
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
					   .forEachOrdered(this::checkCompounds);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			checkDataAndRecordingDates(data, dates);
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'COMPOUNDS' sheet not found");
		}
	}

	private void checkDataAndRecordingDates(Sheet data, Sheet dates)
	{
		try
		{
			if (data == null)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "DATA");
				return;
			}

			// Check compound names in data sheet against database and phenotypes sheet
			data.openStream()
				.findFirst()
				.ifPresent(this::checkCompoundNames);
			// Check germplasm names in data sheet against the database
			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkGermplasmName);

			if (dates != null)
			{
				// Check compound names in dates sheet against database and phenotypes sheet
				dates.openStream()
					 .findFirst()
					 .ifPresent(this::checkCompoundNames);

				// Check germplasm names in dates sheet against the database
				dates.openStream()
					 .skip(1)
					 .forEachOrdered(this::checkGermplasmName);

				List<Row> dataRows = data.read();
				List<Row> datesRows = dates.read();

				// If there is data
				if (!CollectionUtils.isEmpty(dataRows) && dataRows.size() > 1)
				{
					// Check each data row, skipping the headers
					for (int i = 1; i < dataRows.size(); i++)
					{
						Row row = dataRows.get(i);

						// If it's empty, continue
						if (allCellsEmpty(row))
							continue;

						// For each column, skipping the first
						for (int j = 1; j < row.getCellCount(); j++)
						{
							// Get the value
							String value = getCellValue(row, j);

							// If it's not blank
							if (!StringUtils.isEmpty(value))
							{
								try
								{
									// Try to parse as a number
									Double.parseDouble(value);
								}
								catch (IllegalArgumentException e)
								{
									addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, row.getRowNum(), "Column: " + (j + 1) + " Value: " + value);
								}
							}
						}
					}
				}

				// If there is date information
				if (datesRows.size() > 1 && datesRows.get(0).getCellCount() > 1)
				{
					// But there aren't the same number of germplasm
					if (dataRows.size() != datesRows.size())
					{
						addImportResult(ImportStatus.COMPOUND_DATA_DATE_IDENTIFIER_MISMATCH, 0, "Number of rows on DATA and RECORDING_DATE sheets don't match");
					}
					else
					{
						boolean areEqual = areEqual(dataRows.get(0), datesRows.get(0));

						if (!areEqual)
						{
							// Header rows aren't identical
							addImportResult(ImportStatus.COMPOUND_DATA_DATE_HEADER_MISMATCH, 0, "DATA and RECORDING_DATES headers don't match");
						}
						else
						{
							for (int i = 1; i < dataRows.size(); i++)
							{
								Row datesRow = datesRows.get(i);

								// Germplasm identifier isn't identical
								if (!Objects.equals(getCellValue(dataRows.get(i), 0), getCellValue(datesRow, 0)))
									addImportResult(ImportStatus.COMPOUND_DATA_DATE_IDENTIFIER_MISMATCH, i, "DATA and RECORDING_DATES headers don't match");

								for (int c = 3; c < datesRow.getCellCount(); c++)
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
		catch (
			IOException e)

		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}

	}

	private void checkGermplasmName(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String germplasmName = getCellValue(r, 0);

		if (!germplasmToId.containsKey(germplasmName))
			addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, r.getRowNum(), germplasmName);
	}

	private void checkCompoundNames(Row r)
	{
		for (int i = 3; i < r.getCellCount(); i++)
		{
			String compoundName = getCellValue(r, i);
			if (!StringUtils.isEmpty(compoundName) && !compoundNames.contains(compoundName))
			{
				addImportResult(ImportStatus.COMPOUND_MISSING_COMPOUND_DECLARATION, 0, compoundName);
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

	private void checkCompounds(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, columnNameToIndex, "Name");

		if (StringUtils.isEmpty(name))
			return;

		String description = getCellValue(r, columnNameToIndex, "Description");
		String molecularFormula = getCellValue(r, columnNameToIndex, "Molecular Formula");
		String compoundClass = getCellValue(r, columnNameToIndex, "Class");
		String monoisotopicMass = getCellValue(r, columnNameToIndex, "Monoisotopic Mass");
		String averageMass = getCellValue(r, columnNameToIndex, "Average Mass");
		String unitAbbr = getCellValue(r, columnNameToIndex, "Unit Abbreviation");
		String unitName = getCellValue(r, columnNameToIndex, "Unit Name");

		if (StringUtils.isEmpty(name))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Name: " + name);

		if (!StringUtils.isEmpty(name) && name.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Name: " + name + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(unitName) && unitName.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Name: " + unitName + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(molecularFormula) && molecularFormula.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Molecular Formula: " + molecularFormula + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(compoundClass) && compoundClass.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Class: " + compoundClass + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(monoisotopicMass))
		{
			try
			{
				Double.parseDouble(monoisotopicMass);
			}
			catch (IllegalArgumentException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), monoisotopicMass);
			}
		}

		if (!StringUtils.isEmpty(averageMass))
		{
			try
			{
				Double.parseDouble(averageMass);
			}
			catch (IllegalArgumentException e)
			{
				addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), averageMass);
			}
		}

		if (!StringUtils.isEmpty(unitAbbr) && unitAbbr.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Abbreviation: " + unitAbbr + " exceeds 10 characters.");

		// Remember the name, cause we need to check the data sheets against them
		compoundNames.add(name);
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			wb.findSheet("COMPOUNDS")
			  .ifPresent(s -> {
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

				  importCompounds(context, s);
			  });

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			importData(context, data, dates);
		}
		catch (SQLException e) {
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void importCompounds(DSLContext context, Sheet s)
	{
		try
		{
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String name = getCellValue(r, columnNameToIndex, "Name");

				 if (StringUtils.isEmpty(name))
					 return;

				 String description = getCellValue(r, columnNameToIndex, "Description");
				 String molecularFormula = getCellValue(r, columnNameToIndex, "Molecular Formula");
				 String compoundClass = getCellValue(r, columnNameToIndex, "Class");
				 BigDecimal monoisotopicMass = getCellValueBigDecimal(r, columnNameToIndex, "Monoisotopic Mass");
				 BigDecimal averageMass = getCellValueBigDecimal(r, columnNameToIndex, "Average Mass");

				 String unitName = getCellValue(r, columnNameToIndex, "Unit Name");
				 String unitAbbr = getCellValue(r, columnNameToIndex, "Unit Abbreviation");
				 String unitDescription = getCellValue(r, columnNameToIndex, "Unit Descriptions");

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

				 CompoundsRecord compound = context.selectFrom(COMPOUNDS)
												   .where(COMPOUNDS.NAME.isNotDistinctFrom(name))
												   .and(COMPOUNDS.DESCRIPTION.isNotDistinctFrom(description))
												   .and(COMPOUNDS.MOLECULAR_FORMULA.isNotDistinctFrom(molecularFormula))
												   .and(COMPOUNDS.MONOISOTOPIC_MASS.isNotDistinctFrom(monoisotopicMass))
												   .and(COMPOUNDS.AVERAGE_MASS.isNotDistinctFrom(averageMass))
												   .and(COMPOUNDS.COMPOUND_CLASS.isNotDistinctFrom(compoundClass))
												   .and(COMPOUNDS.UNIT_ID.isNotDistinctFrom(unit == null ? null : unit.getId()))
												   .fetchAny();

				 if (compound == null)
				 {
					 compound = context.newRecord(COMPOUNDS);
					 compound.setName(name);
					 compound.setDescription(description);
					 compound.setMolecularFormula(molecularFormula);
					 compound.setMonoisotopicMass(monoisotopicMass);
					 compound.setAverageMass(averageMass);
					 compound.setCompoundClass(compoundClass);
					 compound.setUnitId(unit == null ? null : unit.getId());
					 compound.store();
				 }

				 compoundNameToId.put(compound.getName(), compound.getId());
			 });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void importData(DSLContext context, Sheet data, Sheet dates)
	{
		try
		{
			List<Row> dataRows = data.read();
			List<Row> datesRows = null;

			if (dates != null)
				datesRows = dates.read();

			if (datesRows != null && (datesRows.size() < 2 || datesRows.get(0).getCellCount() < 2))
				datesRows = null;

			List<CompounddataRecord> newData = new ArrayList<>();

			Row headerRow = dataRows.get(0);

			for (int r = 1; r < dataRows.size(); r++)
			{
				Row dataRow = dataRows.get(r);
				Row datesRow = datesRows == null ? null : datesRows.get(r);

				if (allCellsEmpty(dataRow))
					continue;

				String germplasmName = getCellValue(dataRow, 0);
				Integer germplasmId = germplasmToId.get(germplasmName);

				for (int c = 1; c < dataRow.getCellCount(); c++)
				{
					String name = getCellValue(headerRow, c);

					if (StringUtils.isEmpty(name))
						continue;

					Integer compoundId = compoundNameToId.get(name);

					BigDecimal value = getCellValueBigDecimal(dataRow, c);

					if (value == null)
						continue;

					Date date = null;

					if (datesRow != null)
						date = getCellValueDate(datesRow, c);

					CompounddataRecord record = context.newRecord(COMPOUNDDATA);
					record.setGerminatebaseId(germplasmId);
					record.setCompoundId(compoundId);
					record.setDatasetId(dataset.getId());
					record.setCompoundValue(value);
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
		return 6;
	}
}
