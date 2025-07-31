package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.TraitDataImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "trial",
		description = "Import trial trait data Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class TrialImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "trial"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new TrialImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.trial;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return TraitDataImporter.class;
	}
}