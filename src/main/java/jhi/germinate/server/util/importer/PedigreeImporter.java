package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.PedigreesRelationshipType;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.*;
import org.jooq.*;

import java.io.*;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreedefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreedescriptions.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreenotations.*;
import static jhi.germinate.server.database.codegen.tables.Pedigrees.*;

/**
 * @author Sebastian Raubach
 */
public class PedigreeImporter extends DatasheetImporter
{
	public static String FIELD_ACCENUMB              = "ACCENUMB";
	public static String FIELD_ACCENUMB_PARENT_1     = "Parent 1 ACCENUMB";
	public static String FIELD_ACCENUMB_PARENT_2     = "Parent 2 ACCENUMB";
	public static String FIELD_DESCRIPTION_PROCEDURE = "Description of Crossing Procedure (if applicable)";
	public static String FIELD_DESCRIPTION           = "Pedigree Description";
	public static String FIELD_AUTHOR                = "Pedigree Author";

	public static String FIELD_STRING   = "Pedigree string";
	public static String FIELD_NOTATION = "Pedigree Notation";


	private Map<String, Integer> germplasmToId           = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Integer> pedigreeDescriptionToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Integer> notationToId            = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	public static void main(String[] args)
	{
		if (args.length != 11)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		PedigreeImporter importer = new PedigreeImporter(new File(args[5]), Boolean.parseBoolean(args[6]), Integer.parseInt(args[10]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[9]));
		importer.init(args);
		importer.run(RunType.getType(args[8]));
	}

	public PedigreeImporter(File input, boolean isUpdate, int datasetStateId, boolean deleteOnFail, int userId)
	{
		super(input, isUpdate, datasetStateId, deleteOnFail, userId);
	}

	@Override
	protected int getDatasetTypeId()
	{
		return 7;
	}

	@Override
	protected void prepare()
	{
		super.prepare();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> germplasmToId.put(g.getName(), g.getId()));

			Field<String> concat = PEDIGREEDESCRIPTIONS.NAME.concat("|").concat(PEDIGREEDESCRIPTIONS.AUTHOR);
			pedigreeDescriptionToId = context.select(concat, PEDIGREEDESCRIPTIONS.ID)
											 .from(PEDIGREEDESCRIPTIONS)
											 .fetchMap(concat, PEDIGREEDESCRIPTIONS.ID);

			context.selectFrom(PEDIGREENOTATIONS)
				   .forEach(p -> notationToId.put(p.getName(), p.getId()));
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		super.checkFile(wb);

		Sheet data = wb.findSheet("DATA").orElse(null);
		Sheet dataString = wb.findSheet("DATA-STRING").orElse(null);

		if (data == null)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "DATA");
		}
		else
		{
			try
			{
				List<Row> rows = data.read();

				if (!CollectionUtils.isEmpty(rows))
				{
					Row headers = rows.get(0);

					if (headers.getCellCount() < 6)
						addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, "Headers in DATA sheet don't match template.");
					else
					{
						String accenumb = getCellValue(headers, 0);
						String parentOne = getCellValue(headers, 1);
						String parentTwo = getCellValue(headers, 2);
						String procedure = getCellValue(headers, 3);
						String description = getCellValue(headers, 4);
						String author = getCellValue(headers, 5);

						if (!Objects.equals(accenumb, FIELD_ACCENUMB))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_ACCENUMB);
						if (!Objects.equals(parentOne, FIELD_ACCENUMB_PARENT_1))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_ACCENUMB_PARENT_1);
						if (!Objects.equals(parentTwo, FIELD_ACCENUMB_PARENT_2))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_ACCENUMB_PARENT_2);
						if (!Objects.equals(procedure, FIELD_DESCRIPTION_PROCEDURE))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_DESCRIPTION_PROCEDURE);
						if (!Objects.equals(description, FIELD_DESCRIPTION))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_DESCRIPTION);
						if (!Objects.equals(author, FIELD_AUTHOR))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_AUTHOR);
					}

					for (int i = 1; i < rows.size(); i++)
					{
						Row row = rows.get(i);

						if (allCellsEmpty(row))
							continue;

						String accenumb = getCellValue(row, 0);
						String parentOne = getCellValue(row, 1);
						String parentTwo = getCellValue(row, 2);
						String description = getCellValue(row, 4);

						if (!germplasmToId.containsKey(accenumb))
							addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, row.getRowNum(), accenumb);
						if (!StringUtils.isEmpty(parentOne) && !germplasmToId.containsKey(parentOne))
							addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, row.getRowNum(), parentOne);
						if (!StringUtils.isEmpty(parentTwo) && !germplasmToId.containsKey(parentTwo))
							addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, row.getRowNum(), parentTwo);
						if (StringUtils.isEmpty(description))
							addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, row.getRowNum(), FIELD_DESCRIPTION_PROCEDURE);
					}
				}
			}
			catch (IOException e)
			{
				addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			}
		}

		if (dataString == null)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "DATA-STRING");
		}
		else
		{
			try
			{
				List<Row> rows = dataString.read();

				if (!CollectionUtils.isEmpty(rows))
				{
					Row headers = rows.get(0);

					if (headers.getCellCount() < 3)
						addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, "Headers in DATA-STRING sheet don't match template.");
					else
					{
						String accenumb = getCellValue(headers, 0);
						String str = getCellValue(headers, 1);
						String notation = getCellValue(headers, 2);

						if (!Objects.equals(accenumb, FIELD_ACCENUMB))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_ACCENUMB);
						if (!Objects.equals(str, FIELD_STRING))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_STRING);
						if (!Objects.equals(notation, FIELD_NOTATION))
							addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, FIELD_NOTATION);
					}

					for (int i = 1; i < rows.size(); i++)
					{
						Row row = rows.get(i);

						if (allCellsEmpty(row))
							continue;

						String accenumb = getCellValue(row, 0);
						String str = getCellValue(row, 1);
						String notation = getCellValue(row, 2);

						if (!germplasmToId.containsKey(accenumb))
							addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, row.getRowNum(), accenumb);
						if (StringUtils.isEmpty(str))
							addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, row.getRowNum(), FIELD_STRING);
						if (StringUtils.isEmpty(notation))
							addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, row.getRowNum(), FIELD_NOTATION);
					}
				}
			}
			catch (IOException e)
			{
				addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			}
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		super.importFile(wb);

		wb.findSheet("DATA")
		  .ifPresent(s -> {
			  try (Connection conn = Database.getConnection())
			  {
				  DSLContext context = Database.getContext(conn);
				  s.openStream()
				   .skip(1)
				   .forEachOrdered(r -> {
					   if (allCellsEmpty(r))
						   return;

					   String accenumb = getCellValue(r, 0);
					   String parentOne = getCellValue(r, 1);
					   String parentTwo = getCellValue(r, 2);
					   String procedure = getCellValue(r, 3);
					   String description = getCellValue(r, 4);
					   String author = getCellValue(r, 5);

					   Integer germplasmId = germplasmToId.get(accenumb);
					   Integer parentOneId = germplasmToId.get(parentOne);
					   Integer parentTwoId = germplasmToId.get(parentTwo);
					   Integer descriptionId = pedigreeDescriptionToId.get(description + "|" + author);

					   if (germplasmId != null)
					   {
						   if (descriptionId == null)
						   {
							   PedigreedescriptionsRecord pdr = context.newRecord(PEDIGREEDESCRIPTIONS);
							   pdr.setName(description);
							   pdr.setDescription(description);
							   pdr.setAuthor(author);
							   pdr.setCreatedOn(new Timestamp(System.currentTimeMillis()));
							   pdr.store();
							   descriptionId = pdr.getId();
							   pedigreeDescriptionToId.put(description + "|" + author, descriptionId);
						   }

						   if (parentOneId != null)
						   {
							   PedigreesRecord pedigree = context.newRecord(PEDIGREES);
							   pedigree.setDatasetId(this.dataset.getId());
							   pedigree.setGerminatebaseId(germplasmId);
							   pedigree.setParentId(parentOneId);
							   pedigree.setRelationshipType(PedigreesRelationshipType.OTHER); // TODO: Add to template!
							   pedigree.setRelationshipDescription(procedure);
							   pedigree.setPedigreedescriptionId(descriptionId);
							   pedigree.setCreatedOn(new Timestamp(System.currentTimeMillis()));
							   pedigree.store();
						   }
						   if (parentTwoId != null && !Objects.equals(parentOneId, parentTwoId))
						   {
							   PedigreesRecord pedigree = context.newRecord(PEDIGREES);
							   pedigree.setDatasetId(this.dataset.getId());
							   pedigree.setGerminatebaseId(germplasmId);
							   pedigree.setParentId(parentTwoId);
							   pedigree.setRelationshipType(PedigreesRelationshipType.OTHER); // TODO: Add to template!
							   pedigree.setRelationshipDescription(procedure);
							   pedigree.setPedigreedescriptionId(descriptionId);
							   pedigree.setCreatedOn(new Timestamp(System.currentTimeMillis()));
							   pedigree.store();
						   }
					   }
				   });
			  }
			  catch (SQLException | IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });

		wb.findSheet("DATA-STRING")
		  .ifPresent(s -> {
			  try (Connection conn = Database.getConnection())
			  {
				  DSLContext context = Database.getContext(conn);
				  s.openStream()
				   .skip(1)
				   .forEachOrdered(r -> {
					   if (allCellsEmpty(r))
						   return;

					   String accenumb = getCellValue(r, 0);
					   String str = getCellValue(r, 1);
					   String notation = getCellValue(r, 2);

					   Integer germplasmId = germplasmToId.get(accenumb);
					   Integer notationId = notationToId.get(notation);

					   if (germplasmId != null)
					   {
						   if (notationId == null)
						   {
							   PedigreenotationsRecord ntr = context.newRecord(PEDIGREENOTATIONS);
							   ntr.setName(notation);
							   ntr.setDescription(notation);
							   ntr.setCreatedOn(new Timestamp(System.currentTimeMillis()));
							   ntr.store();
							   notationId = ntr.getId();
							   notationToId.put(notation, notationId);
						   }

						   PedigreedefinitionsRecord def = context.newRecord(PEDIGREEDEFINITIONS);
						   def.setDatasetId(this.dataset.getId());
						   def.setGerminatebaseId(germplasmId);
						   def.setPedigreenotationId(notationId);
						   def.setDefinition(str);
						   def.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						   def.store();
					   }
				   });
			  }
			  catch (SQLException | IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		this.importFile(wb);
	}
}
