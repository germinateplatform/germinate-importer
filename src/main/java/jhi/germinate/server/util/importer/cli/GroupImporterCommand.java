package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.GroupImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "group",
		description = "Import group Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class GroupImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "group"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new GroupImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.groups;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return GroupImporter.class;
	}
}