package jhi.germinate.server.util.importer;

import com.google.gson.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.*;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableTraits;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.StringUtils;
import jhi.germinate.server.util.importer.util.GermplasmNotFoundException;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

import static jhi.germinate.server.database.codegen.tables.Methods.METHODS;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Scales.SCALES;
import static jhi.germinate.server.database.codegen.tables.Traitcategories.TRAITCATEGORIES;
import static jhi.germinate.server.database.codegen.tables.Traits.TRAITS;
import static jhi.germinate.server.database.codegen.tables.Treatments.TREATMENTS;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;
import static jhi.germinate.server.database.codegen.tables.Variables.VARIABLES;
import static jhi.germinate.server.database.codegen.tables.ViewTableTraits.VIEW_TABLE_TRAITS;

/**
 * @author Sebastian Raubach
 */
public class TraitDataImporter extends DatasheetImporter
{
	/**
	 * Required column headers
	 */
	private static final String[] COLUMN_HEADERS_TRAITS    = {"Name", "Short Name", "Description", "Data Type", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};
	private static final String[] COLUMN_HEADERS_VARIABLES = {"Variable cropontology id", "Variable name", "Variable description", "Trait cropontology id", "Trait name", "Trait description", "Trait abbreviation", "Trait class", "Trait category", "Method cropontology id", "Method name", "Method description", "Method class", "Method set size", "Method is timeseries", "Scale cropontology id", "Scale name", "Scale description", "Scale unit", "Scale data type", "Scale minimum", "Scale maximum", "Scale valid values"};
	private static final String[] COLUMN_HEADERS_DATA      = {"Line/Phenotype", "Rep", "Block", "Row", "Column", "Treatment", "Location", "Latitude", "Longitude", "Elevation"};

	private final Map<String, Integer>         traitNameToId       = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer>         treatmentToId       = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private       Map<String, Integer>         traitColumnNameToIndex;
	private       Map<String, Integer>         dataColumnNameToIndex;
	private       Map<String, String>          rowColToGermplasm;
	/**
	 * Used to check trait values against trait definitions during checking stage
	 */
	private final Map<String, ViewTableTraits> variableDefinitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> traitIds     = new HashSet<>();
	private final Set<Integer> germplasmIds = new HashSet<>();

	private final Set<String> validPositiveBoolean = new HashSet<>(Arrays.asList("true", "yes", "1"));
	private final Set<String> validNegativeBoolean = new HashSet<>(Arrays.asList("false", "no", "0"));

	private List<String> locationNames;

	private GermplasmLookup germplasmLookup;

	private int traitColumnStartIndex = 10;

	public static void main(String[] args)
			throws SQLException, IOException
	{
		if (args.length != 6)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		TraitDataImporter importer = new TraitDataImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();

//		args = new String[]{"localhost", "germinate_demo", null, "root", null, "C:/Users/sr41756/germinate/demo", "C:/Users/sr41756/Downloads/example-trials-data.xlsx", RunType.CHECK_AND_IMPORT.name(), "109"};
//		args = new String[]{"localhost", "germinate_demo", null, "root", null, "C:/Users/sr41756/germinate/demo", "C:/Users/sr41756/Downloads/example-ontologies-data.xlsx", RunType.CHECK_AND_IMPORT.name(), "109"};
//
//		TraitDataImporter importer = new TraitDataImporter(createImportJobFromCommandline(args, DataImportJobsDatatype.trial));
//		importer.init(args);
//		importer.run();
	}

	public TraitDataImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		germplasmLookup = new GermplasmLookup();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(VARIABLES)
			       .forEach(p -> traitNameToId.put(p.getName(), p.getId()));

			context.selectFrom(VIEW_TABLE_TRAITS)
			       .fetchInto(ViewTableTraits.class)
			       .forEach(p -> variableDefinitions.put(p.getVariableName(), p));

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
			  .filter(s -> Objects.equals(s.getName(), "TRAITS"))
			  .findFirst()
			  .ifPresentOrElse(s ->
			  {
				  // New ontology sheet found!
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .skip(2)
					   .findFirst()
					   .ifPresent(this::getVariableHeaderMapping);
					  // Check the sheet
					  s.openStream()
					   .skip(3)
					   .forEachOrdered(this::checkVariable);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  }, () -> {
				  wb.getSheets()
				    .filter(s -> Objects.equals(s.getName(), "PHENOTYPES"))
				    .findFirst()
				    .ifPresent(s ->
					{
						// Fall back to old traits sheet
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
			  });

			this.locationNames = checkLocationSheet(wb.findSheet("LOCATION").orElse(null));

			Sheet data = wb.findSheet("DATA").orElse(null);
			Sheet dates = wb.findSheet("RECORDING_DATES").orElse(null);
			checkDataAndRecordingDates(data, dates);
		}
		catch (NullPointerException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "Generic error: " + e.getMessage());
		}
	}

	private void checkPredefinedHeaders(Row headers)
	{
		// Check the predefined column headers are correct
		if (!dataColumnNameToIndex.containsKey("Line/Phenotype"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Line/Phenotype' column not found");
		if (!dataColumnNameToIndex.containsKey("Rep"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Rep' column not found");
		if (!dataColumnNameToIndex.containsKey("Treatment"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "'Treatment' column not found");
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
				double v = Double.parseDouble(row);
				if (v < -32768 || v > 32767 || v != (short) v)
					throw new NumberFormatException();
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
				double v = Double.parseDouble(column);
				if (v < -32768 || v > 32767 || v != (short) v)
					throw new NumberFormatException();
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
		String block = getCellValue(r, dataColumnNameToIndex, "Block");

		if (!StringUtils.isEmpty(block) && block.length() > 10)
			addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Block: " + block + " exceeds 10 characters.");

		if (StringUtils.isEmpty(germplasmName))
			addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "ACCENUMB missing");
		else
		{
			try
			{
				germplasmLookup.getGermplasmId(germplasmName);
			}
			catch (GermplasmNotFoundException e)
			{
				addImportResult(e.getReason(), r.getRowNum(), germplasmName);
			}
		}

		if (StringUtils.isEmpty(rep))
			addImportResult(ImportStatus.TRIALS_DATA_REP_MISSING, r.getRowNum(), "Rep missing");
	}

	private void checkTraitNames(Row r)
	{
		for (int i = this.traitColumnStartIndex; i < r.getCellCount(); i++)
		{
			String traitName = getCellValue(r, i);
			if (!StringUtils.isEmpty(traitName) && !variableDefinitions.containsKey(traitName))
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

	private void getVariableHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			traitColumnNameToIndex = IntStream.range(0, r.getCellCount())
			                                  .filter(i -> !cellEmpty(r, i))
			                                  .boxed()
			                                  .collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS_VARIABLES)
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

	/**
	 * Finds the best matching existing scale for a newly submitted scale,
	 * updates its min/max bounds, and extends its allowedValues if needed.
	 *
	 * @param newScale   the incoming scale to reconcile
	 * @param candidates pre-filtered list of potential matches (by name/description/unit etc.)
	 * @return the best-matching existing scale (mutated in place), or null if no acceptable match is found
	 */
	public ScalesRecord findBestMatchAndMerge(ScalesRecord newScale, List<ScalesRecord> candidates)
	{
		if (candidates == null || candidates.isEmpty())
		{
			return null;
		}

		ScalesRecord bestMatch = null;
		double bestScore = -1;

		for (ScalesRecord candidate : candidates)
		{
			double score = computeCompatibilityScore(newScale, candidate);
			if (score > bestScore)
			{
				bestScore = score;
				bestMatch = candidate;
			}
		}

		// Reject if compatibility is too low (tune this threshold to your needs)
		if (bestMatch == null || bestScore < 0.5)
		{
			return null;
		}

		mergeInto(bestMatch, newScale);
		return bestMatch;
	}

	/**
	 * Scores how compatible a candidate scale is with the new scale.
	 * Returns a value in [0.0, 1.0]. Higher is better.
	 * <p>
	 * Four possible restriction combinations are handled:
	 * - Both have restrictions:    scored normally (range + allowedValues)
	 * - Neither has restrictions:  neutral full score (1.0) — both are unrestricted
	 * - Only one has restrictions: low score (0.1) — structurally mismatched
	 */
	private double computeCompatibilityScore(ScalesRecord newScale, ScalesRecord candidate)
	{
		TraitRestrictions newR = newScale.getRestrictions();
		TraitRestrictions candR = candidate.getRestrictions();

		// Neither has restrictions — structurally identical, other fields (name/unit etc.) already matched
		if (newR == null && candR == null)
		{
			return 1.0;
		}

		// One has restrictions and the other doesn't — structural mismatch, very unlikely to be the same scale
		if (newR == null || candR == null)
		{
			return 0.1;
		}

		// Both have restrictions — score on range and allowedValues
		double score = 0.0;

		// --- Numeric range compatibility (weight: 0.4) ---
		boolean newHasRange = newR.getMin() != null && newR.getMax() != null;
		boolean candHasRange = candR.getMin() != null && candR.getMax() != null;

		if (newHasRange && candHasRange)
		{
			double newMin = newR.getMin(), newMax = newR.getMax();
			double candMin = candR.getMin(), candMax = candR.getMax();

			// Completely disjoint ranges — almost certainly not the same scale
			if (newMax < candMin || newMin > candMax)
			{
				return 0.0;
			}

			double overlapSize = Math.min(newMax, candMax) - Math.max(newMin, candMin);
			double unionSize = Math.max(newMax, candMax) - Math.min(newMin, candMin);
			score += 0.4 * (unionSize > 0 ? overlapSize / unionSize : 1.0);

		}
		else if (!newHasRange && !candHasRange)
		{
			score += 0.2; // Neither is numeric — neutral partial credit
		}
		// If only one side has a range, give 0 for this component

		// --- AllowedValues overlap (weight: 0.6) ---
		String[][] newVals  = newR.getCategories();
		String[][] candVals = candR.getCategories();

		boolean newHasVals  = newVals  != null && newVals.length  > 0;
		boolean candHasVals = candVals != null && candVals.length > 0;

		if (newHasVals && candHasVals) {
			score += 0.6 * computeAllowedValuesSimilarity(newVals, candVals);
		} else if (!newHasVals && !candHasVals) {
			score += 0.3;
		}
		// If only one side has allowedValues, give 0 for this component

		return score;
	}

	/**
	 * Scores the similarity between two allowedValues arrays.
	 * Uses best-match Jaccard similarity on inner arrays rather than requiring exact equality,
	 * so e.g. ["2","6","intermediate"] and ["2","6"] are recognised as similar (score: 2/3 ≈ 0.67).
	 */
	private double computeAllowedValuesSimilarity(String[][] newVals, String[][] candVals) {
		Set<Set<String>> newSets  = toLevelSets(newVals);
		Set<Set<String>> candSets = toLevelSets(candVals);

		// For each level in the new scale, find the best-matching level in the candidate
		double totalScore = 0.0;
		for (Set<String> newLevel : newSets) {
			double bestLevelScore = candSets.stream()
			                                .mapToDouble(candLevel -> jaccardSimilarity(newLevel, candLevel))
			                                .max()
			                                .orElse(0.0);
			totalScore += bestLevelScore;
		}

		// Normalise by the larger of the two level counts (penalises missing/extra levels)
		double normalised = totalScore / Math.max(newSets.size(), candSets.size());
		return normalised;
	}

	/**
	 * Jaccard similarity between two sets: |intersection| / |union|.
	 */
	private double jaccardSimilarity(Set<String> a, Set<String> b) {
		if (a.isEmpty() && b.isEmpty()) return 1.0;

		long intersection = a.stream().filter(b::contains).count();
		long union = a.size() + b.size() - intersection;
		return union > 0 ? (double) intersection / union : 0.0;
	}

	/**
	 * Converts a String[][] into a Set of Sets for order-insensitive level comparison.
	 * Each inner String[] represents one "level" (e.g. synonyms for a single value).
	 */
	private Set<Set<String>> toLevelSets(String[][] values)
	{
		return Arrays.stream(values)
		             .map(level -> Arrays.stream(level)
		                                 .map(String::toLowerCase)
		                                 .map(String::trim)
		                                 .collect(Collectors.toSet()))
		             .collect(Collectors.toSet());
	}

	/**
	 * Merges the new scale's restriction data into the existing (matched) scale:
	 * 1. Expands numeric min/max if the new scale extends the range.
	 * 2. Appends any allowedValues levels not already present.
	 * <p>
	 * If the new scale has no restrictions, there is nothing to merge.
	 * If the existing scale has no restrictions, they are created from the new scale's values.
	 */
	private void mergeInto(ScalesRecord existing, ScalesRecord newScale)
	{
		TraitRestrictions newR = newScale.getRestrictions();
		if (newR == null)
		{
			return; // Nothing to merge
		}

		// Initialise restrictions on the existing record if absent
		TraitRestrictions existingR = existing.getRestrictions();
		if (existingR == null)
		{
			existingR = new TraitRestrictions();
			existing.setRestrictions(existingR);
		}

		// --- Expand numeric range ---
		if (newR.getMin() != null)
		{
			existingR.setMin(existingR.getMin() == null
					? newR.getMin()
					: Math.min(existingR.getMin(), newR.getMin()));
		}
		if (newR.getMax() != null)
		{
			existingR.setMax(existingR.getMax() == null
					? newR.getMax()
					: Math.max(existingR.getMax(), newR.getMax()));
		}

		// --- Extend allowedValues with genuinely new levels ---
		String[][] newVals = newR.getCategories();
		if (newVals == null || newVals.length == 0)
		{
			return;
		}

		String[][] existingVals = existingR.getCategories();
		List<String[]> merged = (existingVals != null)
				? new ArrayList<>(Arrays.asList(existingVals))
				: new ArrayList<>();

		for (String[] newLevel : newVals) {
			Set<String> newNormalised = Arrays.stream(newLevel)
			                                  .map(String::toLowerCase)
			                                  .map(String::trim)
			                                  .collect(Collectors.toSet());

			// Find the best-matching existing level (if any)
			int bestMatchIndex = -1;
			double bestMatchScore = 0.0;
			for (int i = 0; i < merged.size(); i++) {
				Set<String> existingNormalised = Arrays.stream(merged.get(i))
				                                       .map(String::toLowerCase)
				                                       .map(String::trim)
				                                       .collect(Collectors.toSet());
				double similarity = jaccardSimilarity(existingNormalised, newNormalised);
				if (similarity >= 0.5 && similarity > bestMatchScore) {
					bestMatchScore = similarity;
					bestMatchIndex = i;
				}
			}

			if (bestMatchIndex >= 0) {
				// Merge: union of both levels, preserving original casing from each side
				Set<String> existingNormalised = Arrays.stream(merged.get(bestMatchIndex))
				                                       .map(String::toLowerCase)
				                                       .map(String::trim)
				                                       .collect(Collectors.toSet());

				List<String> union = new ArrayList<>(Arrays.asList(merged.get(bestMatchIndex)));
				for (String newValue : newLevel) {
					if (!existingNormalised.contains(newValue.toLowerCase().trim())) {
						union.add(newValue);
					}
				}
				merged.set(bestMatchIndex, union.toArray(new String[0]));
			} else {
				// Genuinely new level — append it
				merged.add(newLevel);
			}
		}

		existingR.setCategories(merged.toArray(new String[0][]));
		// Store back so jOOQ realises it has updated
		existing.setRestrictions(existingR);
	}

	private void checkVariable(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, traitColumnNameToIndex.get("Variable name"));

		if (StringUtils.isEmpty(name))
			return;

		ImportVariable variable = new ImportVariable(false);
		variable.ontologyId = getCellValue(r, traitColumnNameToIndex.get("Variable cropontology id"));
		variable.name = getCellValue(r, traitColumnNameToIndex.get("Variable name"));
		variable.description = getCellValue(r, traitColumnNameToIndex.get("Variable description"));

		ImportTrait trait = new ImportTrait(false);
		trait.ontologyId = getCellValue(r, traitColumnNameToIndex.get("Trait cropontology id"));
		trait.name = getCellValue(r, traitColumnNameToIndex.get("Trait name"));
		trait.description = getCellValue(r, traitColumnNameToIndex.get("Trait description"));
		trait.abbreviation = getCellValue(r, traitColumnNameToIndex.get("Trait abbreviation"));
		trait.clazz = getCellValue(r, traitColumnNameToIndex.get("Trait class"));
		trait.category = getCellValue(r, traitColumnNameToIndex.get("Trait category"));

		ImportMethod method = new ImportMethod(false);
		method.ontologyId = getCellValue(r, traitColumnNameToIndex.get("Method cropontology id"));
		method.name = getCellValue(r, traitColumnNameToIndex.get("Method name"));
		method.description = getCellValue(r, traitColumnNameToIndex.get("Method description"));
		method.clazz = getCellValue(r, traitColumnNameToIndex.get("Method class"));
		method.setSize = getCellValue(r, traitColumnNameToIndex.get("Method set size"));
		method.isTimeseries = getCellValue(r, traitColumnNameToIndex.get("Method is timeseries"));

		ImportScale scale = new ImportScale(false);
		scale.ontologyId = getCellValue(r, traitColumnNameToIndex.get("Scale cropontology id"));
		scale.name = getCellValue(r, traitColumnNameToIndex.get("Scale name"));
		scale.description = getCellValue(r, traitColumnNameToIndex.get("Scale description"));
		scale.unit = getCellValue(r, traitColumnNameToIndex.get("Scale unit"));
		scale.dataType = getCellValue(r, traitColumnNameToIndex.get("Scale data type"));
		scale.minimum = getCellValue(r, traitColumnNameToIndex.get("Scale minimum"));
		scale.maximum = getCellValue(r, traitColumnNameToIndex.get("Scale maximum"));
		scale.validValues = getCellValue(r, traitColumnNameToIndex.get("Scale valid values"));

		ViewTableTraits container = new ViewTableTraits();
		// Check all fields for validity
		variable.check(r, container);
		trait.check(r, container);
		method.check(r, container);
		scale.check(r, container);

		variableDefinitions.put(name, container);
	}

	private void checkTrait(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String name = getCellValue(r, traitColumnNameToIndex.get("Name"));

		if (StringUtils.isEmpty(name))
			return;

		ImportVariable variable = new ImportVariable(true);
		variable.name = getCellValue(r, traitColumnNameToIndex.get("Name"));
		variable.description = getCellValue(r, traitColumnNameToIndex.get("Description"));

		ImportTrait trait = new ImportTrait(true);
		trait.name = getCellValue(r, traitColumnNameToIndex.get("Name"));
		trait.description = getCellValue(r, traitColumnNameToIndex.get("Description"));
		trait.abbreviation = getCellValue(r, traitColumnNameToIndex.get("Short Name"));
		trait.clazz = "other";
		trait.category = getCellValue(r, traitColumnNameToIndex.get("Trait category"));

		ImportMethod method = new ImportMethod(true);
		method.name = "Measurement";
		method.clazz = "measurement";
		method.setSize = getCellValue(r, traitColumnNameToIndex.get("Set size"));
		method.isTimeseries = getCellValue(r, traitColumnNameToIndex.get("Is timeseries"));

		ImportScale scale = new ImportScale(true);
		scale.name = StringUtils.coalesce(getCellValue(r, traitColumnNameToIndex.get("Unit Name")), getCellValue(r, traitColumnNameToIndex.get("Name")), "N/A");
		scale.description = StringUtils.coalesce(getCellValue(r, traitColumnNameToIndex.get("Unit Descriptions")), getCellValue(r, traitColumnNameToIndex.get("Description")));
		scale.unit = StringUtils.coalesce(getCellValue(r, traitColumnNameToIndex.get("Unit Abbreviation")), getCellValue(r, traitColumnNameToIndex.get("Short name")));
		scale.dataType = getCellValue(r, traitColumnNameToIndex.get("Data Type"));
		scale.minimum = getCellValue(r, traitColumnNameToIndex.get("Min (only for numeric traits)"));
		scale.maximum = getCellValue(r, traitColumnNameToIndex.get("Max (only for numeric traits)"));
		scale.validValues = getCellValue(r, traitColumnNameToIndex.get("Trait categories (comma separated)"));

		ViewTableTraits container = new ViewTableTraits();
		// Check all fields for validity
		variable.check(r, container);
		trait.check(r, container);
		method.check(r, container);
		scale.check(r, container);

		variableDefinitions.put(name, container);
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
				List<ViewTableTraitsScaleDatatype> dataTypes = headers.stream()
				                                                      .skip(this.traitColumnStartIndex)
				                                                      .map(this::getCellValue)
				                                                      .filter(c -> !StringUtils.isEmpty(c) && variableDefinitions.containsKey(c))
				                                                      .map(c -> variableDefinitions.get(c).getScaleDatatype())
				                                                      .toList();

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
				List<String> traitsWithRestrictions = variableDefinitions.values().stream()
				                                                         .filter(t -> !StringUtils.isEmpty(t.getVariableName()))
				                                                         .filter(t -> t.getScaleRestrictions() != null && (t.getScaleDatatype() == ViewTableTraitsScaleDatatype.numeric || t.getScaleDatatype() == ViewTableTraitsScaleDatatype.categorical))
				                                                         .map(ViewTableTraits::getVariableName)
				                                                         .toList();

				if (!traitsWithRestrictions.isEmpty())
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

							 TraitRestrictions restrictions = variableDefinitions.get(t).getScaleRestrictions();
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
										 if (Objects.equals(cellValue, possValue) || Objects.equals(cellValue.toLowerCase(), possValue.toLowerCase()))
										 {
											 found = true;
											 break outer;
										 }
										 else
										 {
											 // Try parsing both as numbers, then compare
											 try
											 {
												 double one = Double.parseDouble(cellValue);
												 double two = Double.parseDouble(possValue);

												 // Fuzzy double comparison
												 if ((one == two) || (Math.abs(one - two) < 0.000001d))
												 {
													 found = true;
													 break outer;
												 }
											 }
											 catch (Exception e)
											 {
												 // Ignore
											 }
										 }
									 }
								 }

								 if (!found)
									 addImportResult(ImportStatus.TRIALS_DATA_VIOLATES_RESTRICTION, r.getRowNum(), "Cell value not within valid category range: " + cellValue + " not in " + variableDefinitions.get(t));
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

			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "TRAITS"))
			  .findFirst()
			  .ifPresentOrElse(s ->
			  {
				  // New ontology sheet found!
				  try
				  {
					  // Map headers to their index
					  s.openStream()
					   .skip(2)
					   .findFirst()
					   .ifPresent(this::getVariableHeaderMapping);
					  // Check the sheet
					  s.openStream()
					   .skip(3)
					   .forEachOrdered(this::checkVariable);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }

				  importTraits(context, s, "Variable name");
			  }, () -> {
				  wb.findSheet("PHENOTYPES")
				    .ifPresent(s -> {
						try
						{
							// Map headers to their index
							s.openStream()
						     .findFirst()
						     .ifPresent(this::getTraitHeaderMapping);
							// Check the sheet to get the trait mapping
							s.openStream()
						     .skip(2)
						     .forEachOrdered(this::checkTrait);
						}
						catch (IOException e)
						{
							addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
						}

						importTraits(context, s, "Name");
					});
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

			context.execute("SET autocommit=0;");
			context.execute("SET unique_checks=0;");
			context.execute("SET foreign_key_checks=0;");
			importData(context, data, dates);
			context.execute("SET autocommit=1;");
			context.execute("SET unique_checks=1;");
			context.execute("SET foreign_key_checks=1;");
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

	private void importTraits(DSLContext context, Sheet s, String nameKey)
	{
		try
		{
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String name = getCellValue(r, traitColumnNameToIndex.get(nameKey));

				 if (StringUtils.isEmpty(name))
					 return;

				 ViewTableTraits container = variableDefinitions.get(name);

				 if (container == null)
					 return;

				 List<ScalesRecord> potential = context.selectFrom(SCALES)
				                                       .where(SCALES.NAME.isNotDistinctFrom(container.getScaleName()))
				                                       .and(SCALES.DATATYPE.isNotDistinctFrom(ScalesDatatype.lookupLiteral(container.getScaleDatatype().getLiteral())))
				                                       .and(SCALES.DESCRIPTION.isNotDistinctFrom(container.getScaleDescription()))
				                                       .and(SCALES.UNIT.isNotDistinctFrom(container.getScaleUnit()))
				                                       .fetchInto(ScalesRecord.class);

				 ScalesRecord scale = context.newRecord(SCALES);
				 scale.setName(container.getScaleName());
				 scale.setDescription(container.getScaleDescription());
				 scale.setUnit(container.getScaleUnit());
				 scale.setDatatype(ScalesDatatype.lookupLiteral(container.getScaleDatatype().getLiteral()));
				 scale.setRestrictions(container.getScaleRestrictions());

				 ScalesRecord bestMatch = findBestMatchAndMerge(scale, potential);

				 if (bestMatch == null)
				 {
					 scale.store();
				 }
				 else
				 {
					 scale = bestMatch;
					 scale.store(SCALES.RESTRICTIONS);
				 }

				 TraitcategoriesRecord category = context.selectFrom(TRAITCATEGORIES)
				                                         .where(TRAITCATEGORIES.NAME.isNotDistinctFrom(container.getTraitCategoryName()))
				                                         .fetchAny();

				 if (!StringUtils.isEmpty(container.getTraitCategoryName()) && category == null)
				 {
					 category = context.newRecord(TRAITCATEGORIES);
					 category.setName(container.getTraitCategoryName());
					 category.store();
				 }

				 MethodsRecord method = context.selectFrom(METHODS)
				                               .where(METHODS.NAME.isNotDistinctFrom(container.getMethodName()))
				                               .and(METHODS.DESCRIPTION.isNotDistinctFrom(container.getMethodDescription()))
				                               .and(METHODS.SETSIZE.isNotDistinctFrom(container.getMethodSetSize()))
				                               .and(METHODS.IS_TIMESERIES.isNotDistinctFrom(container.getMethodIsTimeseries()))
				                               .and(METHODS.METHOD_CLASS.isNotDistinctFrom(MethodsMethodClass.lookupLiteral(container.getMethodClass().getLiteral())))
				                               .fetchAny();

				 if (method == null)
				 {
					 method = context.newRecord(METHODS);
					 method.setName(container.getMethodName());
					 method.setDescription(container.getMethodDescription());
					 method.setSetsize(container.getMethodSetSize());
					 if (container.getMethodIsTimeseries() != null)
					 	 method.setIsTimeseries(container.getMethodIsTimeseries());
					 else
						 method.setIsTimeseries(true);
					 method.setMethodClass(MethodsMethodClass.lookupLiteral(container.getMethodClass().getLiteral()));
					 method.store();
				 }

				 TraitsRecord trait = context.selectFrom(TRAITS)
				                             .where(TRAITS.NAME.isNotDistinctFrom(container.getTraitName()))
				                             .and(TRAITS.DESCRIPTION.isNotDistinctFrom(container.getTraitDescription()))
				                             .and(TRAITS.ABBREVIATION.isNotDistinctFrom(container.getTraitAbbreviation()))
				                             .and(TRAITS.SYNONYMS.isNotDistinctFrom(container.getTraitSynonyms()))
				                             .and(TRAITS.TRAIT_CLASS.isNotDistinctFrom(TraitsTraitClass.lookupLiteral(container.getTraitClass().getLiteral())))
				                             .and(TRAITS.TRAITCATEGORY_ID.isNotDistinctFrom(category != null ? category.getId() : null))
				                             .fetchAny();

				 if (trait == null)
				 {
					 trait = context.newRecord(TRAITS);
					 trait.setName(container.getTraitName());
					 trait.setDescription(container.getTraitDescription());
					 trait.setAbbreviation(container.getTraitAbbreviation());
					 trait.setSynonyms(container.getTraitSynonyms());
					 trait.setTraitClass(TraitsTraitClass.lookupLiteral(container.getTraitClass().getLiteral()));
					 trait.setTraitcategoryId(category != null ? category.getId() : null);
					 trait.store();
				 }

				 VariablesRecord variable = context.selectFrom(VARIABLES)
				                                   .where(VARIABLES.NAME.isNotDistinctFrom(container.getVariableName()))
				                                   .and(VARIABLES.DESCRIPTION.isNotDistinctFrom(container.getVariableDescription()))
				                                   .and(VARIABLES.SCALE_ID.isNotDistinctFrom(scale.getId()))
				                                   .and(VARIABLES.TRAIT_ID.isNotDistinctFrom(trait.getId()))
				                                   .and(VARIABLES.METHOD_ID.isNotDistinctFrom(method.getId()))
				                                   .fetchAny();

				 if (variable == null)
				 {
					 variable = context.newRecord(VARIABLES);
					 variable.setName(container.getVariableName());
					 variable.setDescription(container.getVariableDescription());
					 variable.setTraitId(trait.getId());
					 variable.setMethodId(method.getId());
					 variable.setScaleId(scale.getId());
					 variable.store();
				 }

				 container.setVariableId(variable.getId());
				 container.setTraitId(trait.getId());
				 container.setMethodId(method.getId());
				 container.setScaleId(scale.getId());
				 traitNameToId.put(variable.getName(), variable.getId());
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
			Map<String, TrialsetupRecord> existingTrialsetups = new HashMap<>();

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
				Integer germplasmId = germplasmLookup.getGermplasmId(germplasmName);
				String treatmentName = getCellValue(dataRow, dataColumnNameToIndex, "Treatment");
				Integer treatmentId = null;

				germplasmIds.add(germplasmId);

				if (!StringUtils.isEmpty(treatmentName))
					treatmentId = treatmentToId.get(treatmentName);

				String id = germplasmId + "|" + row + "|" + column + "|" + rep + "|" + treatmentId + "|" + locationName;

				TrialsetupRecord ts = existingTrialsetups.get(id);

				if (ts == null)
				{
					ts = context.newRecord(TRIALSETUP);
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
				}

				existingTrialsetups.put(id, ts);

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

//					Integer traitId = traitNameToId.get(name);
					ViewTableTraits trait = variableDefinitions.get(name);

					String value = getCellValue(dataRow, c);

					if (StringUtils.isEmpty(value))
						continue;

					Date date = null;

					if (datesRow != null)
						date = getCellValueDate(datesRow, c);

					// See if it's a date column, if so, get the actual date value, then reformat
					if (trait.getScaleDatatype() == ViewTableTraitsScaleDatatype.date)
					{
						Date dateValue = getCellValueDate(dataRow, c);

						if (dateValue != null)
							value = SDF_FULL_DASH.format(dateValue);
					}

					// See if we can match the value to its actual trait category restriction equivalent. This is used to map e.g. "2.0" to "2".
					if (trait.getScaleRestrictions() != null && trait.getScaleRestrictions().getCategories() != null)
					{
						outer:
						for (String[] categoryGroup : trait.getScaleRestrictions().getCategories())
						{
							for (String cat : categoryGroup)
							{
								// Try parsing both as numbers, then compare
								try
								{
									double one = Double.parseDouble(cat);
									double two = Double.parseDouble(value);

									// Fuzzy double comparison
									if ((one == two) || (Math.abs(one - two) < 0.000001d))
									{
										value = cat;
										break outer;
									}
								}
								catch (Exception e)
								{
									// Ignore
								}
							}
						}
					}

					PhenotypedataRecord record = context.newRecord(PHENOTYPEDATA);
					record.setTrialsetupId(rowToTrialsetupId.get(r));
					record.setVariableId(trait.getVariableId());
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

	private ViewTableTraitsScaleDatatype getDataType(String dt)
	{
		ViewTableTraitsScaleDatatype result = null;

		if (Objects.equals(dt, "int") || Objects.equals(dt, "float") || Objects.equals(dt, "numeric"))
		{
			result = ViewTableTraitsScaleDatatype.numeric;
		}
		else if (Objects.equals(dt, "char") || Objects.equals(dt, "text"))
		{
			result = ViewTableTraitsScaleDatatype.text;
		}
		else if (Objects.equals(dt, "date"))
		{
			result = ViewTableTraitsScaleDatatype.date;
		}
		else if (Objects.equals(dt, "categorical"))
		{
			result = ViewTableTraitsScaleDatatype.categorical;
		}

		if (result == null)
			throw new IllegalArgumentException();
		else
			return result;
	}

	private abstract class ImportBase
	{
		boolean legacy;
		Integer dbId;
		String  ontologyId;
		String  name;
		String  description;

		public ImportBase(boolean legacy)
		{
			this.legacy = legacy;
		}

		public abstract void check(Row r, ViewTableTraits container);

		protected void checkBase(Row r, String id)
		{
			if (StringUtils.isEmpty(name))
				addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), id + ": " + name);
			else if (name.length() > 255)
				addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), id + ": " + name + " exceeds 255 characters.");
		}
	}

	private class ImportVariable extends ImportBase
	{
		public ImportVariable(boolean legacy)
		{
			super(legacy);
		}

		@Override
		public void check(Row r, ViewTableTraits container)
		{
			checkBase(r, legacy ? "Name" : "Variable name");
			container.setVariableName(name);
			container.setVariableDescription(description);
		}
	}

	private class ImportTrait extends ImportBase
	{
		String abbreviation;
		String clazz;
		String category;

		public ImportTrait(boolean legacy)
		{
			super(legacy);
		}

		@Override
		public void check(Row r, ViewTableTraits container)
		{
			super.checkBase(r, legacy ? "Name" : "Trait name");

			if (!StringUtils.isEmpty(abbreviation) && abbreviation.length() > 255)
				addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), (legacy ? "Short Name: " : "Trait abbreviation: ") + abbreviation + " exceeds 255 characters.");

			container.setTraitName(name);
			container.setTraitDescription(description);
			container.setTraitAbbreviation(abbreviation);
			container.setTraitCategoryName(category);

			if (!StringUtils.isEmpty(clazz))
			{
				try
				{
					container.setTraitClass(ViewTableTraitsTraitClass.valueOf(clazz));
				}
				catch (Exception e)
				{
					addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_CLASS, r.getRowNum(), (legacy ? "N/A: " : "Trait class: ") + clazz);
				}
			}
			else
			{
				container.setTraitClass(ViewTableTraitsTraitClass.other);
			}
		}
	}

	private class ImportMethod extends ImportBase
	{
		String clazz;
		String setSize;
		String isTimeseries;

		public ImportMethod(boolean legacy)
		{
			super(legacy);
		}

		@Override
		public void check(Row r, ViewTableTraits container)
		{
			super.checkBase(r, legacy ? "N/A" : "Method name");

			container.setMethodName(name);
			container.setMethodDescription(description);

			if (!StringUtils.isEmpty(clazz))
			{
				try
				{
					container.setMethodClass(ViewTableTraitsMethodClass.valueOf(clazz));
				}
				catch (Exception e)
				{
					addImportResult(ImportStatus.TRIALS_INVALID_METHOD_CLASS, r.getRowNum(), (legacy ? "N/A: " : "Method class: ") + clazz);
				}
			}
			else
			{
				container.setMethodClass(ViewTableTraitsMethodClass.measurement);
			}

			if (!StringUtils.isEmpty(setSize))
			{
				try
				{
					container.setMethodSetSize(Integer.parseInt(setSize));
				}
				catch (NumberFormatException e)
				{
					addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Set size isn't a valid number: " + setSize);
				}
			}

			if (!StringUtils.isEmpty(isTimeseries))
			{
				if (validPositiveBoolean.contains(isTimeseries.toLowerCase()))
					container.setMethodIsTimeseries(true);
				else if (validNegativeBoolean.contains(isTimeseries.toLowerCase()))
					container.setMethodIsTimeseries(false);
				else
				{
					container.setMethodIsTimeseries(true);
					addImportResult(ImportStatus.GENERIC_INVALID_BOOLEAN, r.getRowNum(), "Is timeseries flag isn't a valid boolean: " + isTimeseries);
				}
			} else {
				container.setMethodIsTimeseries(true);
			}
		}
	}

	private class ImportScale extends ImportBase
	{
		String unit;
		String dataType;
		String minimum;
		String maximum;
		String validValues;

		public ImportScale(boolean legacy)
		{
			super(legacy);
		}

		@Override
		public void check(Row r, ViewTableTraits container)
		{
			super.checkBase(r, legacy ? "Unit Name OR Name" : "Scale name");

			container.setScaleName(name);
			container.setScaleDescription(description);

			if (!StringUtils.isEmpty(unit) && unit.length() > 255)
				addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), (legacy ? "Unit Abbreviations OR Short Name: " : "Scale unit: ") + unit + " exceeds 255 characters.");

			container.setScaleUnit(unit);

			try
			{
				container.setScaleDatatype(getDataType(dataType));
			}
			catch (Exception e)
			{
				container.setScaleDatatype(ViewTableTraitsScaleDatatype.text);
				addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_DATATYPE, r.getRowNum(), (legacy ? "Data Type: " : "Scale data Type: ") + dataType);
			}

			if (!StringUtils.isEmpty(validValues))
			{
				try
				{
					// Try to parse it
					String[][] cats = new Gson().fromJson(validValues, String[][].class);

					if (cats != null && cats.length > 1)
					{
						for (int i = 1; i < cats.length; i++)
						{
							if (cats[i - 1].length != cats[i].length)
							{
								addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_CATEGORIES, r.getRowNum(), (legacy ? "Trait categories (comma separated): " : "Trait categories: ") + validValues + " has invalid format.");
							}
						}
					}

					if (container.getScaleRestrictions() == null)
						container.setScaleRestrictions(new TraitRestrictions());

					container.getScaleRestrictions().setCategories(cats);
				}
				catch (JsonSyntaxException | NullPointerException e)
				{
					String[] cats = Arrays.stream(validValues.split(",")).filter(c -> !StringUtils.isEmpty(c)).map(String::trim).toArray(String[]::new);

					if (cats.length > 1)
					{
						if (container.getScaleRestrictions() == null)
							container.setScaleRestrictions(new TraitRestrictions());

						container.getScaleRestrictions().setCategories(new String[][]{cats});
					}
					else
					{
						e.printStackTrace();
						addImportResult(ImportStatus.TRIALS_INVALID_TRAIT_CATEGORIES, r.getRowNum(), (legacy ? "Trait categories (comma separated): " : "Trait categories: ") + validValues + " has invalid format.");
					}
				}
			}

			if (!StringUtils.isEmpty(minimum))
			{
				try
				{
					Double min = Double.parseDouble(minimum);
					if (container.getScaleRestrictions() == null)
						container.setScaleRestrictions(new TraitRestrictions());

					container.getScaleRestrictions().setMin(min);
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
					Double max = Double.parseDouble(maximum);
					if (container.getScaleRestrictions() == null)
						container.setScaleRestrictions(new TraitRestrictions());

					container.getScaleRestrictions().setMax(max);
				}
				catch (NumberFormatException e)
				{
					addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), "Maximum isn't a valid number: " + maximum);
				}
			}
		}
	}
}
