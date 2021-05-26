package jhi.germinate.server.util.importer;

import com.google.gson.Gson;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.StringUtils;
import org.dhatim.fastexcel.reader.*;

import java.io.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.*;

/**
 * @author Sebastian Raubach
 */
public abstract class AbstractImporter
{
	private SimpleDateFormat SDF_FULL_DASH  = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat SDF_FULL       = new SimpleDateFormat("yyyyMMdd");
	private SimpleDateFormat SDF_YEAR_MONTH = new SimpleDateFormat("yyyyMM");
	private SimpleDateFormat SDF_YEAR_DAY   = new SimpleDateFormat("yyyydd");
	private SimpleDateFormat SDF_YEAR       = new SimpleDateFormat("yyyy");

	protected       boolean                         isUpdate;
	private         boolean                         deleteOnFail;
	private         File                            input;
	protected final int                             userId;
	private         Map<ImportStatus, ImportResult> errorMap = new HashMap<>();

	public AbstractImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		this.input = input;
		this.isUpdate = isUpdate;
		this.deleteOnFail = deleteOnFail;
		this.userId = userId;
	}

	protected void init(String[] args)
	{
		Database.init(args[0], args[1], args[2], args[3], args[4], false);
	}

	public void run(RunType runtype)
	{
		try (ReadableWorkbook wb = new ReadableWorkbook(input))
		{
			prepare();

			if (runtype.includesCheck())
				checkFile(wb);

			Logger.getLogger("").log(Level.INFO, errorMap.toString());

			if (errorMap.size() < 1)
			{
				if (runtype.includesImport())
				{
					if (isUpdate)
						updateFile(wb);
					else
						importFile(wb);
				}
			}
			else if (deleteOnFail)
			{
				input.delete();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}

		List<ImportResult> result = getImportResult();

		String jsonFilename = this.input.getName().substring(0, this.input.getName().lastIndexOf("."));
		File json = new File(input.getParent(), jsonFilename + ".json");
		try
		{
			Files.write(json.toPath(), Collections.singletonList(new Gson().toJson(result)), StandardCharsets.UTF_8);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
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
		{
			result = result.replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");
		}

		if (Objects.equals(result, ""))
			return null;
		else
			return result.trim();
	}

	protected String getCellValue(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			String value = r.getCellText(columnNameToIndex.get(column)).replaceAll("\u00A0", "").replaceAll("\\r?\\n", " ");

			if (Objects.equals(value, ""))
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

			if (Objects.equals(value, ""))
				return null;
			else
				return value.trim();
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

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!errorMap.containsKey(status))
			errorMap.put(status, new ImportResult(status, rowIndex, message));
	}

	private List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorMap.values());
	}

	protected abstract void prepare();

	protected abstract void checkFile(ReadableWorkbook wb);

	protected abstract void importFile(ReadableWorkbook wb);

	protected abstract void updateFile(ReadableWorkbook wb);

	public static class ImportConfig
	{
		private String uuid;
		private Thread thread;

		public ImportConfig(String uuid, Thread thread)
		{
			this.uuid = uuid;
			this.thread = thread;
		}

		public String getUuid()
		{
			return uuid;
		}

		public ImportConfig setUuid(String uuid)
		{
			this.uuid = uuid;
			return this;
		}

		public Thread getThread()
		{
			return thread;
		}

		public ImportConfig setThread(Thread thread)
		{
			this.thread = thread;
			return this;
		}
	}

	public enum RunType
	{
		CHECK,
		IMPORT,
		CHECK_AND_IMPORT;

		public boolean includesCheck()
		{
			return this == CHECK || this == CHECK_AND_IMPORT;
		}

		public boolean includesImport()
		{
			return this == IMPORT || this == CHECK_AND_IMPORT;
		}

		public static RunType getType(String input)
		{
			try
			{
				return RunType.valueOf(input);
			}
			catch (Exception e)
			{
				return CHECK;
			}
		}
	}
}
