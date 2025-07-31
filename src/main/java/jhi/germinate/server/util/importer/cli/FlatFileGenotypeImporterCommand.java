package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.GenotypeFlatFileImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "flat",
		description = "Genotype importer for files in flat tab-delimited format.",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class FlatFileGenotypeImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "genotype", "flat"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new FlatFileGenotypeImporterCommand()).execute(args);
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
		return GenotypeFlatFileImporter.class;
	}
}
