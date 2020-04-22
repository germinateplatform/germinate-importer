package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.enums.PhenotypesDatatype;
import jhi.germinate.server.database.tables.records.*;
import jhi.germinate.server.util.StringUtils;

import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Phenotypedata.*;
import static jhi.germinate.server.database.tables.Phenotypes.*;
import static jhi.germinate.server.database.tables.Treatments.*;
import static jhi.germinate.server.database.tables.Units.*;

/**
 * @author Sebastian Raubach
 */
public class TraitDataImporter extends DatasheetImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"Name", "Short Name", "Description", "Data Type", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};

	private Map<String, Integer> traitNameToId;
	private Map<String, Integer> germplasmToId;
	private Map<String, Integer> columnNameToIndex;
	private Map<String, Integer> treatmentToId;
	private Set<String>          traitNames;

	public static void main(String[] args)
	{
		if (args.length != 10)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		TraitDataImporter importer = new TraitDataImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]));
		importer.init(args);
		importer.run(RunType.getType(args[8]));
	}

	public TraitDataImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		super(input, isUpdate, deleteOnFail, userId);
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			traitNameToId = context.selectFrom(PHENOTYPES)
								   .fetchMap(PHENOTYPES.NAME, PHENOTYPES.ID);

			traitNames = context.selectFrom(PHENOTYPES)
								.fetchSet(PHENOTYPES.NAME);

			germplasmToId = context.selectFrom(GERMINATEBASE)
								   .fetchMap(GERMINATEBASE.NAME, GERMINATEBASE.ID);

			treatmentToId = context.selectFrom(TREATMENTS)
								   .fetchMap(TREATMENTS.NAME, TREATMENTS.ID);
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
					   .ifPresent(this::getHeaderMapping);
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

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			checkDataAndRecordingDates(data, dates);
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'PHENOTYPES' sheet not found");
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

			// Check trait names in data sheet against database and phenotypes sheet
			data.openStream()
				.findFirst()
				.ifPresent(this::checkTraitNames);
			// Check germplasm names in data sheet against the database
			data.openStream()
				.skip(1)
				.forEachOrdered(this::checkGermplasmName);

			if (dates != null)
			{
				// Check trait names in dates sheet against database and phenotypes sheet
				dates.openStream()
					 .findFirst()
					 .ifPresent(this::checkTraitNames);

				// Check germplasm names in dates sheet against the database
				dates.openStream()
					 .skip(1)
					 .forEachOrdered(this::checkGermplasmName);

				List<Row> dataRows = data.read();
				List<Row> datesRows = dates.read();

				// If there is date information
				if (datesRows.size() > 1 && datesRows.get(0).getPhysicalCellCount() > 1)
				{
					// But there aren't the same number of germplasm
					if (dataRows.size() != datesRows.size())
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

								for (int c = 3; c < datesRow.getPhysicalCellCount(); c++)
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

	private void checkGermplasmName(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String germplasmName = getCellValue(r, 0);

		if (!germplasmToId.containsKey(germplasmName))
			addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, r.getRowNum(), germplasmName);
	}

	private void checkTraitNames(Row r)
	{
		for (int i = 3; i < r.getPhysicalCellCount(); i++)
		{
			String traitName = getCellValue(r, i);
			if (!traitNames.contains(traitName))
			{
				addImportResult(ImportStatus.TRIALS_MISSING_TRAIT_DECLARATION, 0, traitName);
			}
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

	private void checkTrait(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, columnNameToIndex, "Name");
		String description = getCellValue(r, columnNameToIndex, "Description");
		String shortName = getCellValue(r, columnNameToIndex, "Short Name");
		String dataType = getCellValue(r, columnNameToIndex, "Data Type");
		String unitAbbr = getCellValue(r, columnNameToIndex, "Unit Abbreviation");
		String unitDescription = getCellValue(r, columnNameToIndex, "Unit Descriptions");

		if (StringUtils.isEmpty(name))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Name: " + name);

		if (!StringUtils.isEmpty(shortName) && shortName.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Short name: " + shortName + " exceeds 10 characters.");

		if (!StringUtils.isEmpty(description) && description.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Description: " + description + " exceeds 255 characters.");

		if (!StringUtils.isEmpty(unitDescription) && unitDescription.length() > 255)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Descriptions: " + unitDescription + " exceeds 255 characters.");

		try
		{
			PhenotypesDatatype.valueOf(dataType + "_");
		}
		catch (IllegalArgumentException e)
		{
			addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_DATATYPE, r.getRowNum(), "Data Type: " + dataType);
		}

		if (!StringUtils.isEmpty(unitAbbr) && unitAbbr.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Unit Abbreviation: " + unitAbbr + " exceeds 10 characters.");

		// Remember the name, cause we need to check the data sheets against them
		traitNames.add(name);
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);

		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			wb.findSheet("PHENOTYPES")
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

				  importTraits(context, s);
			  });

			wb.findSheet("DATA")
			  .ifPresent(s -> importGermplasmAndTreatments(context, s));

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			importData(context, data, dates);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			// TODO
		}
	}

	private void importGermplasmAndTreatments(DSLContext context, Sheet s)
	{
		try
		{
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String germplasm = getCellValue(r, 0);
				 String rep = getCellValue(r, 1);
				 String treatment = getCellValue(r, 2);

				 if (!StringUtils.isEmpty(rep))
				 {
					 String sampleName = germplasm + "-" + dataset.getId() + "-" + rep;

					 if (!germplasmToId.containsKey(sampleName))
					 {
						 Integer germplasmId = germplasmToId.get(germplasm);

						 GerminatebaseRecord sample = context.newRecord(GERMINATEBASE);
						 sample.setEntityparentId(germplasmId);
						 sample.setEntitytypeId(2);
						 sample.setName(sampleName);
						 sample.setGeneralIdentifier(sampleName);
						 sample.setNumber(sampleName);
						 sample.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						 sample.store();

						 germplasmToId.put(sampleName, sample.getId());
					 }
				 }

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

				 String name = getCellValue(r, columnNameToIndex, "Name");
				 String shortName = getCellValue(r, columnNameToIndex, "Short Name");
				 String description = getCellValue(r, columnNameToIndex, "Description");
				 String dataTypeString = getCellValue(r, columnNameToIndex, "Data Type");
				 PhenotypesDatatype dataType = PhenotypesDatatype.char_;
				 if (!StringUtils.isEmpty(dataTypeString))
				 {
					 try
					 {
						 dataType = PhenotypesDatatype.valueOf(dataTypeString + "_");
					 }
					 catch (IllegalArgumentException | NullPointerException e)
					 {
					 }
				 }

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

				 PhenotypesRecord trait = context.selectFrom(PHENOTYPES)
												 .where(PHENOTYPES.NAME.isNotDistinctFrom(name))
												 .and(PHENOTYPES.SHORT_NAME.isNotDistinctFrom(shortName))
												 .and(PHENOTYPES.DESCRIPTION.isNotDistinctFrom(description))
												 .and(PHENOTYPES.DATATYPE.isNotDistinctFrom(dataType))
												 .and(PHENOTYPES.UNIT_ID.isNotDistinctFrom(unit == null ? null : unit.getId()))
												 .fetchAny();

				 if (trait == null)
				 {
					 trait = context.newRecord(PHENOTYPES);
					 trait.setName(name);
					 trait.setShortName(shortName);
					 trait.setDescription(description);
					 trait.setDatatype(dataType);
					 trait.setUnitId(unit == null ? null : unit.getId());
					 trait.store();
				 }

				 traitNameToId.put(trait.getName(), trait.getId());
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

			if (datesRows != null && (datesRows.size() < 2 || datesRows.get(0).getPhysicalCellCount() < 2))
				datesRows = null;

			List<PhenotypedataRecord> newData = new ArrayList<>();

			Row headerRow = dataRows.get(0);

			for (int r = 1; r < dataRows.size(); r++)
			{
				Row dataRow = dataRows.get(r);
				Row datesRow = datesRows == null ? null : datesRows.get(r);

				if (allCellsEmpty(dataRow))
					continue;

				String germplasmName = getCellValue(dataRow, 0);
				String rep = getCellValue(dataRow, 1);
				Integer germplasmId;

				// If it's a rep, adjust the name
				if (!StringUtils.isEmpty(rep))
					germplasmId = germplasmToId.get(germplasmName + "-" + dataset.getId() + "-" + rep);
				else
					germplasmId = germplasmToId.get(germplasmName);

				String treatmentName = getCellValue(dataRow, 2);
				Integer treatmentId = null;

				if (!StringUtils.isEmpty(treatmentName))
					treatmentId = treatmentToId.get(treatmentName);

				for (int c = 3; c < dataRow.getPhysicalCellCount(); c++)
				{
					Integer traitId = traitNameToId.get(getCellValue(headerRow, c));

					String value = getCellValue(dataRow, c);

					if (StringUtils.isEmpty(value))
						continue;

					Date date = null;

					if (datesRow != null)
						date = getCellValueDate(datesRow, c);

					PhenotypedataRecord record = context.newRecord(PHENOTYPEDATA);
					record.setGerminatebaseId(germplasmId);
					record.setPhenotypeId(traitId);
					record.setTreatmentId(treatmentId);
					record.setDatasetId(dataset.getId());
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
		return 3;
	}
}
