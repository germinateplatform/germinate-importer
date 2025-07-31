package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.McpdImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "mcpd",
		description = "Import MCPD germplasm Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class McpdImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "mcpd"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new McpdImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.mcpd;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return McpdImporter.class;
	}
}