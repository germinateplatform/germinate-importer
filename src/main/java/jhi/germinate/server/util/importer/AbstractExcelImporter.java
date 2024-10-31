package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.StringUtils;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.math.*;
import java.nio.file.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Fileresources.*;
import static jhi.germinate.server.database.codegen.tables.Fileresourcetypes.*;

/**
 * @author Sebastian Raubach
 */
public abstract class AbstractExcelImporter extends AbstractImporter
{
	public AbstractExcelImporter(Integer importJobId)
	{
		super(importJobId);
	}

	protected BigDecimal getCellValueBigDecimal(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		return getCellValueBigDecimal(r, columnNameToIndex.get(column));
	}

	protected BigDecimal getCellValueDMS(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		String degreeMinuteSecond = getCellValue(r, columnNameToIndex, column);

		if (StringUtils.isEmpty(degreeMinuteSecond))
			return null;

		if (degreeMinuteSecond.length() == 7 || degreeMinuteSecond.length() == 8)
		{
			boolean lat = degreeMinuteSecond.length() == 7;

			int degree;
			int minute = 0;
			int second = 0;

			try
			{
				if (lat)
					degree = Integer.parseInt(degreeMinuteSecond.substring(0, 2));
				else
					degree = Integer.parseInt(degreeMinuteSecond.substring(0, 3));
			}
			catch (NumberFormatException e)
			{
				return null;
			}
			try
			{
				if (lat)
					minute = Integer.parseInt(degreeMinuteSecond.substring(2, 4));
				else
					minute = Integer.parseInt(degreeMinuteSecond.substring(3, 5));
			}
			catch (NumberFormatException e)
			{
			}
			try
			{
				if (lat)
					second = Integer.parseInt(degreeMinuteSecond.substring(4, 6));
				else
					second = Integer.parseInt(degreeMinuteSecond.substring(5, 7));
			}
			catch (NumberFormatException e)
			{
			}

			double value = degree + minute / 60d + second / 3600d;

			if (degreeMinuteSecond.endsWith("S") || degreeMinuteSecond.endsWith("W"))
				value = -value;

			BigDecimal result = new BigDecimal(value, MathContext.DECIMAL64);
			result = result.setScale(10, RoundingMode.HALF_UP);
			return result;
		}
		else
		{
			return null;
		}
	}

	protected BigDecimal getCellValueBigDecimal(Row r, int index)
	{
		try
		{
			BigDecimal result = new BigDecimal(Double.parseDouble(getCellValue(r, index)), MathContext.DECIMAL64);
			result = result.setScale(10, RoundingMode.HALF_UP);
			return result;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Double getCellValueDouble(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return Double.parseDouble(getCellValue(r, columnNameToIndex, column));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Integer getCellValueInteger(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return Integer.parseInt(getCellValue(r, columnNameToIndex, column));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Short getCellValueShort(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return Short.parseShort(getCellValue(r, columnNameToIndex, column));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Date getCellValueDate(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		return getCellValueDate(r, columnNameToIndex.get(column));
	}

	protected Date getCellValueDate(Row r, int index)
	{
		String value = getCellValue(r, index);

		java.util.Date date = null;
		if (!StringUtils.isEmpty(value))
		{
			if (!StringUtils.isEmpty(value))
			{
				if (value.length() == 10)
				{
					try
					{
						date = SDF_FULL_DASH.parse(value);
					}
					catch (Exception e)
					{
					}
				}
				else
				{
					// Replace all hyphens with zeros so that we only have one case to handle.
					value = value.replace("-", "0");

					try
					{
						boolean noMonth = value.startsWith("00", 4);
						boolean noDay = value.startsWith("00", 6);

						if (noDay && noMonth)
							date = SDF_YEAR.parse(value.substring(0, 4));
						else if (noDay)
							date = SDF_YEAR_MONTH.parse(value.substring(0, 6));
						else if (noMonth)
							date = SDF_YEAR_DAY.parse(value.substring(0, 4) + value.substring(6, 8));
						else
							date = SDF_FULL.parse(value);
					}
					catch (Exception e)
					{
					}
				}
			}
		}

		if (date != null)
			return new Date(date.getTime());
		else
			return null;
	}

	protected String getCellValue(Cell c)
	{
		if (c == null)
			return null;

		String result = c.getText();

		if (result != null)
			result = result.replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");

		if (Objects.equals(result.trim(), ""))
			return null;
		else
			return result.trim();
	}

	protected String getCellValue(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			String value = r.getCellText(columnNameToIndex.get(column)).replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");

			if (Objects.equals(value.trim(), ""))
				return null;
			else
				return value.trim();
		}
		catch (Exception e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, r.getRowNum(), "Column missing: '" + column + "'");
			return null;
		}
	}

	protected String getCellValue(Row r, Integer index)
	{
		if (index == null)
			return null;

		try
		{
			String value = r.getCellText(index).replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");

			if (Objects.equals(value.trim(), ""))
				return null;
			else
				return value;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Double getCellValueDouble(Row r, Integer index)
	{
		try
		{
			return Double.parseDouble(getCellValue(r, index));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected boolean cellEmpty(Row r, int i)
	{
		return r.getCell(i) == null || r.getCell(i).getType() == CellType.EMPTY || StringUtils.isEmpty(r.getCellText(i).replaceAll("\u00A0", ""));
	}

	protected boolean allCellsEmpty(Row r)
	{
		for (int i = 0; i < r.getCellCount(); i++)
		{
			if (r.getCell(i) != null && r.getCell(i).getType() != CellType.EMPTY && !StringUtils.isEmpty(r.getCellText(i).replaceAll("\u00A0", "")))
				return false;
		}

		return true;
	}

	@Override
	protected final void checkFile()
	{
		try (ReadableWorkbook wb = new ReadableWorkbook(this.getInputFile()))
		{
			checkFile(wb);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected final void importFile()
	{
		try (ReadableWorkbook wb = new ReadableWorkbook(this.getInputFile()))
		{
			importFile(wb);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected final void updateFile()
	{
		try (ReadableWorkbook wb = new ReadableWorkbook(this.getInputFile()))
		{
			updateFile(wb);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	protected abstract void checkFile(ReadableWorkbook wb);

	protected abstract void importFile(ReadableWorkbook wb);

	protected abstract void updateFile(ReadableWorkbook wb);

	@Override
	protected void postImport()
	{
		File input = this.getInputFile();
		// Create a backup copy of the uploaded file and link it to the newly created dataset.
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			FileresourcetypesRecord type = context.selectFrom(FILERESOURCETYPES)
												  .where(FILERESOURCETYPES.NAME.eq("Dataset resource"))
												  .and(FILERESOURCETYPES.DESCRIPTION.eq("Automatically created linked backups of uploaded data resources."))
												  .fetchAny();

			if (type == null)
			{
				type = context.newRecord(FILERESOURCETYPES);
				type.setName("Dataset resource");
				type.setDescription("Automatically created linked backups of uploaded data resources.");
				type.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				type.store();
			}

			File typeFolder = new File(new File(new File(jobDetails.getJobConfig().getBaseFolder(), "data"), "download"), Integer.toString(type.getId()));
			typeFolder.mkdirs();
			File target = new File(typeFolder, input.getName());

			FileresourcesRecord fileRes = context.newRecord(FILERESOURCES);
			fileRes.setName(this.jobDetails.getOriginalFilename());
			fileRes.setPath(target.getName());
			fileRes.setFilesize(input.length());
			fileRes.setDescription("Automatic upload backup.");
			fileRes.setFileresourcetypeId(type.getId());
			fileRes.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			fileRes.store();

			// Now update the name with the file resource id
			target = new File(typeFolder, fileRes.getId() + "-" + input.getName());
			fileRes.setPath(target.getName());
			fileRes.store();

			importJobStats.setFileResourceId(fileRes.getId());

			// Finally copy the file
			Files.copy(input.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
		catch (SQLException | IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Failed to create file resource for dataset: " + e.getMessage());
		}
	}
}
