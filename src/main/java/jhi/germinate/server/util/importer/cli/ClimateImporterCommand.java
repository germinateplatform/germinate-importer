package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.ClimateDataImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "climate",
		description = "Import climate data Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class ClimateImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "climate"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new ClimateImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.climate;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return ClimateDataImporter.class;
	}
}