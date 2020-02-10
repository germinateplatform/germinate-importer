package jhi.germinate.server.util.importer;

import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.jooq.DSLContext;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jhi.germinate.server.database.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.tables.Phenotypes.PHENOTYPES;

/**
 * @author Sebastian Raubach
 */
public class TraitDataImporter extends AbstractImporter
{
	/** Required column headers */
	private static final String[] COLUMN_HEADERS = {"Name", "Short Name", "Description", "Data Type", "Unit Name", "Unit Abbreviation", "Unit Descriptions"};

	private Map<String, Integer> traitNameToId;
	private Map<String, Integer> germplasmToId;
	private Map<String, Integer> columnNameToIndex;

	public TraitDataImporter(File input, boolean isUpdate, boolean deleteOnFail)
	{
		super(input, isUpdate, deleteOnFail);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			traitNameToId = context.selectFrom(PHENOTYPES)
					.fetchMap(PHENOTYPES.NAME, PHENOTYPES.ID);

			germplasmToId = context.selectFrom(GERMINATEBASE)
					.fetchMap(GERMINATEBASE.NAME, GERMINATEBASE.ID);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			// TODO
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		try
		{
//			wb.getSheets()
//					.filter(s -> Objects.equals(s.getName(), "PHENOTYPES"))
//					.findFirst()
//					.ifPresent(s ->
//					{
//						try
//						{
//							// Map headers to their index
//							s.openStream()
//									.findFirst()
//									.ifPresent(this::getHeaderMapping);
//							// Check the sheet
//							s.openStream()
//									.skip(1)
//									.forEachOrdered(this::check);
//							// Check the entity parent for each row
//							s.openStream()
//									.skip(1)
//									.forEachOrdered(this::checkEntityParent);
//						}
//						catch (IOException e)
//						{
//							addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
//						}
//					});
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'PHENOTYPES' sheet not found");
		}
	}

	private void getHeaderMapping(Row r)
	{
		try
		{
			// Map column names to their index
			columnNameToIndex = IntStream.range(0, r.getPhysicalCellCount())
					.boxed()
					.collect(Collectors.toMap(r::getCellText, Function.identity()));

			// Check if all columns are there
			Arrays.stream(COLUMN_HEADERS)
					.forEach(c ->
					{
						if (!columnNameToIndex.containsKey(c))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, -1, c);
					});
		}
		catch (IllegalStateException e)
		{
			addImportResult(ImportStatus.GENERIC_DUPLICATE_COLUMN, 1, e.getMessage());
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{

	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{

	}
}
