package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.GenotypeHapmapImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "hapmap",
		description = "Genotype importer for files in Hapmap format.",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class HapmapGenotypeImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "genotype", "hapmap"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new HapmapGenotypeImporterCommand()).execute(args);
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
		return GenotypeHapmapImporter.class;
	}
}
