package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;

import jhi.germinate.resource.enums.ImportStatus;

/**
 * @author Sebastian Raubach
 */
public class GenotypeImporter extends DatasheetImporter
{
	private static final String[] ADDITIONAL_METADATA_LABELS = {"Map Name", "Marker Technology", "Genetic or Physical", "Map Units"};
	private              File     txtFile;

	public GenotypeImporter(File input, boolean isUpdate, boolean deleteOnFail)
	{
		super(input, isUpdate, deleteOnFail);

		this.txtFile = new File(input.getParent(), input.getName().replace(".xlsx", ".txt"));
	}

	@Override
	protected void prepare()
	{
		super.prepare();
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

		wb.findSheet("DATA")
		  .ifPresent(s -> {
			  // Write the DATA sheet to a plain text file, so the plain text file importer can check it.
			  // This doesn't have a huge effect on performance, as genotypic data that fits into the spreadsheet is usually quite low volume.
			  try (BufferedWriter bw = Files.newBufferedWriter(txtFile.toPath(), StandardCharsets.UTF_8))
			  {
				  s.openStream()
				   .forEachOrdered(r -> {
					   try
					   {
						   bw.write(r.stream()
									 .map(this::getCellValue)
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

			  // TODO: Let plain text file checker check it
		  });
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		super.updateFile(wb);
	}

	@Override
	protected int getDatasetTypeId()
	{
		return 1;
	}
}
