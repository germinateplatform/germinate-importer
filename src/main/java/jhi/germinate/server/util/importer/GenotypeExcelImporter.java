package jhi.germinate.server.util.importer;

import jhi.germinate.server.database.pojo.*;
import org.dhatim.fastexcel.reader.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sebastian Raubach
 */
public class GenotypeExcelImporter extends DatasheetImporter
{
	private static final String[]                 ADDITIONAL_METADATA_LABELS = {"Map Name", "Marker Technology", "Genetic or Physical", "Map Units"};
	private              File                     txtFile;
	private              GenotypeFlatFileImporter flatFileImporter;

	public static void main(String[] args)
	{
		if (args.length != 12)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		GenotypeExcelImporter importer = new GenotypeExcelImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Integer.parseInt(args[10]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]), new File(args[11]));
		importer.init(args);
		importer.run(RunType.getType(args[8]));
	}

	public GenotypeExcelImporter(File input, boolean isUpdate, int datasetStateId, boolean deleteOnFail, int userId, File hdf5TargetFolder)
	{
		super(input, isUpdate, datasetStateId, deleteOnFail, userId);

		this.txtFile = new File(input.getParentFile(), input.getName().replace(".xlsx", ".txt"));

		flatFileImporter = new GenotypeFlatFileImporter(txtFile, isUpdate, datasetStateId, deleteOnFail, userId, hdf5TargetFolder);
	}

	@Override
	protected void prepare()
	{
		super.prepare();
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
		// Write the DATA sheet to a plain text file, so the plain text file importer can check it.
		// This doesn't have a huge effect on performance, as genotypic data that fits into the spreadsheet is usually quite low volume.
		try (BufferedWriter bw = Files.newBufferedWriter(txtFile.toPath(), StandardCharsets.UTF_8))
		{
			wb.findSheet("METADATA")
			  .ifPresent(s -> {
				  try
				  {
					  List<Row> rows = s.read();

					  bw.write("# dataset = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Title")), 2));
					  bw.newLine();
					  bw.write("# markerType = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Marker Technology")), 2));
					  bw.newLine();
					  bw.write("# map = " + getCellValue(rows.get(metadataLabelToRowIndex.get("Map Name")), 2));
					  bw.newLine();
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

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
}
