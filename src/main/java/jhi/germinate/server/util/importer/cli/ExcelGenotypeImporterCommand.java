package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.*;
import picocli.CommandLine;

@CommandLine.Command(
		name = "excel",
		description = "Genotype importer for files in Germinate Data Template Excel sheets.",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class ExcelGenotypeImporterCommand extends AbstractTransposableGenotypeImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "genotype", "excel"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new ExcelGenotypeImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.genotype;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return switch (genotypeOrientation)
		{
			case GENOTYPE_MARKER_BY_GERMPLASM -> GenotypeExcelTransposedImporter.class;
			default -> GenotypeExcelImporter.class;
		};
	}
}
