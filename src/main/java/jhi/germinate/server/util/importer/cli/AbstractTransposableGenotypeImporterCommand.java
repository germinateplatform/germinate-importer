package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.pojo.DataOrientation;
import picocli.CommandLine;

public abstract class AbstractTransposableGenotypeImporterCommand extends AbstractImporterCommand
{
	@CommandLine.Option(
			names = {"-go", "--genotype-orientation"},
			paramLabel = "genotypeOrientation",
			required = true,
			defaultValue = "GERMPLASM_BY_MARKERS",
			description = "Orientation of this genotype file. Either one of: ${COMPLETION-CANDIDATES}"
	)
	protected DataOrientation genotypeOrientation;
}
