package jhi.germinate.server.util.importer;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.importer.task.FlatFileTransposeTask;
import org.dhatim.fastexcel.reader.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sebastian Raubach
 */
public class GenotypeExcelTransposedImporter extends DatasheetImporter
{
	private static final String[]                 ADDITIONAL_METADATA_LABELS = {"Map Name", "Marker Technology", "Genetic or Physical", "Map Units"};
	private              File                     txtFile;
	private              File                     transposedTxtFile;
	private              GenotypeFlatFileImporter flatFileImporter;

	private final Set<Integer> germplasmIds = new HashSet<>();
	private final Set<Integer> markerIds    = new HashSet<>();

	public static void main(String[] args)
		throws SQLException, IOException
	{
		GenotypeExcelTransposedImporter importer;

		if (args.length == 6)
		{
			importer = new GenotypeExcelTransposedImporter(Integer.parseInt(args[5]));
		}
		else if (args.length == 9)
		{
			importer = new GenotypeExcelTransposedImporter(createImportJobFromCommandline(args, DataImportJobsDatatype.genotype));
		}
		else
		{
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));
		}

		importer.init(args);
		importer.run();
	}

	public GenotypeExcelTransposedImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		File input = getInputFile();
		this.txtFile = new File(input.getParentFile(), input.getName().replace(".xlsx", ".txt"));
		this.transposedTxtFile = new File(txtFile.getParentFile(), "transposed-" + txtFile.getName());

		flatFileImporter = new GenotypeFlatFileImporter(this.importJobId);
		flatFileImporter.init(getArgs());
		flatFileImporter.setInputFile(this.transposedTxtFile);
		flatFileImporter.prepare();
	}

	@Override
	protected void checkMetadataLabels(Sheet s)
	{
		super.checkMetadataLabels(s);

		Arrays.stream(ADDITIONAL_METADATA_LABELS)
			  .forEachOrdered(c -> {
				  if (!metadataLabelToRowIndex.containsKey(c))
					  addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, c);
			  });
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		super.checkFile(wb);

		exportData(wb);

		flatFileImporter.checkFile();

		Map<ImportStatus, ImportResult> result = flatFileImporter.getErrorMap();

		for (Map.Entry<ImportStatus, ImportResult> entry : result.entrySet())
			addImportResult(entry.getKey(), entry.getValue().getRowIndex(), entry.getValue().getMessage());
	}

	private void exportData(ReadableWorkbook wb)
	{
		List<String> prefix = new ArrayList<>();
		wb.findSheet("METADATA")
		  .ifPresent(s -> {
			  try
			  {
				  List<Row> rows = s.read();

				  prefix.add("# dataset = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Title")), 2));
				  prefix.add("# markerType = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Marker Technology")), 2));
				  prefix.add("# map = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Map Name")), 2));
			  }
			  catch (IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });

		// Write the DATA sheet to a plain text file, so the plain text file importer can check it.
		// This doesn't have a huge effect on performance, as genotypic data that fits into the spreadsheet is usually quite low volume.
		try (BufferedWriter bw = Files.newBufferedWriter(txtFile.toPath(), StandardCharsets.UTF_8))
		{
			wb.findSheet("DATA")
			  .ifPresent(s -> {
				  try
				  {
					  final long markerCount = s.read().get(2).getCellCount();
					  s.openStream()
					   .forEachOrdered(r -> {
						   if (allCellsEmpty(r))
							   return;

						   try
						   {
							   // We have to iterate them like this, because simply streaming over all cells would exclude blank cells at the end.
							   List<String> values = new ArrayList<>();

							   for (int i = 0; i < markerCount; i++)
								   values.add(getCellValue(r, i));

							   bw.write(values.stream()
											  .map(c -> c == null ? "" : c)
											  .collect(Collectors.joining("\t", "", System.lineSeparator())));
						   }
						   catch (IOException e)
						   {
							   addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
						   }
					   });
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}

		// Now transpose it so we can use the old code to import it.
		new FlatFileTransposeTask(txtFile, transposedTxtFile, prefix).run();
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);

		// Only export the file again if it doesn't exist. If we get to the import step, a check will either have been run (then the file will exist),
		// or this is a direct call to import without a check and then it won't exist at all.
		if (!this.txtFile.exists() || this.txtFile.length() < 1)
			exportData(wb);

		flatFileImporter.setDataset(dataset);
		flatFileImporter.importFile();
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		this.importFile(wb);
	}

	@Override
	protected int getDatasetTypeId()
	{
		return 1;
	}

	@Override
	protected void postImport()
	{
		super.postImport();

		importJobStats.setDatasetId(dataset.getId());
		importJobStats.setGermplasm(germplasmIds.size());
		importJobStats.setMarkers(markerIds.size());
	}
}
