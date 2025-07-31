package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.PedigreeImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "pedigree",
		description = "Import germplasm pedigree Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class PedigreeImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "pedigree"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new PedigreeImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.pedigree;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return PedigreeImporter.class;
	}
}