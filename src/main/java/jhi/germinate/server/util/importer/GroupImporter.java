package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.ImportStatus;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;
import org.jooq.tools.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Groupmembers.*;
import static jhi.germinate.server.database.codegen.tables.Groups.*;
import static jhi.germinate.server.database.codegen.tables.Locations.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;

/**
 * @author Sebastian Raubach
 */
public class GroupImporter extends AbstractExcelImporter
{
	private final Map<String, Integer> accenumbToId     = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer> markerNameToId   = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Integer> locationNameToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private final Set<Integer> germplasmIds = new HashSet<>();
	private final Set<Integer> markerIds = new HashSet<>();
	private final Set<Integer> locationIds = new HashSet<>();
	private final Set<Integer> groupIds = new HashSet<>();

	private final Map<Integer, Map<Integer, Integer>> groupIndexToIdPerType = new HashMap<>();

	{
		groupIndexToIdPerType.put(1, new HashMap<>());
		groupIndexToIdPerType.put(2, new HashMap<>());
		groupIndexToIdPerType.put(3, new HashMap<>());
	}

	public static void main(String[] args)
	{
		if (args.length != 6)
			throw new RuntimeException("Invalid number of arguments: " + Arrays.toString(args));

		GroupImporter importer = new GroupImporter(Integer.parseInt(args[5]));
		importer.init(args);
		importer.run();
	}

	public GroupImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> accenumbToId.put(g.getName(), g.getId()));

			context.selectFrom(MARKERS)
				   .forEach(m -> markerNameToId.put(m.getMarkerName(), m.getId()));

			context.selectFrom(LOCATIONS)
				   .forEach(l -> locationNameToId.put(l.getSiteName(), l.getId()));
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
		try
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "GERMPLASM"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // First two rows
					  List<Row> rows = s.openStream()
										.limit(3)
										.collect(Collectors.toList());

					  if (rows.get(1).getCellCount() != rows.get(2).getCellCount())
						  addImportResult(ImportStatus.GROUP_HEADER_MISMATCH, 1, "Mismatch between germplasm group names and group visibility.");

					  checkGroupVisibility(rows.get(1));
					  checkGroupNames(rows.get(2));

					  s.openStream()
					   .skip(3)
					   .forEachOrdered(this::checkGermplasm);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'GERMPLASM' sheet not found");
		}

		try
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "MARKERS"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // First two rows
					  List<Row> rows = s.openStream()
										.limit(3)
										.collect(Collectors.toList());

					  if (rows.get(1).getCellCount() != rows.get(2).getCellCount())
						  addImportResult(ImportStatus.GROUP_HEADER_MISMATCH, 1, "Mismatch between marker group names and group visibility.");

					  checkGroupVisibility(rows.get(1));
					  checkGroupNames(rows.get(2));

					  s.openStream()
					   .skip(3)
					   .forEachOrdered(this::checkMarker);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'MARKERS' sheet not found");
		}

		try
		{
			wb.getSheets()
			  .filter(s -> Objects.equals(s.getName(), "LOCATIONS"))
			  .findFirst()
			  .ifPresent(s -> {
				  try
				  {
					  // First two rows
					  List<Row> rows = s.openStream()
										.limit(3)
										.collect(Collectors.toList());

					  if (rows.get(1).getCellCount() != rows.get(2).getCellCount())
						  addImportResult(ImportStatus.GROUP_HEADER_MISMATCH, 1, "Mismatch between location group names and group visibility.");

					  checkGroupVisibility(rows.get(1));
					  checkGroupNames(rows.get(2));

					  s.openStream()
					   .skip(3)
					   .forEachOrdered(this::checkLocation);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });
		}
		catch (NullPointerException e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'LOCATIONS' sheet not found");
		}
	}

	private void checkGroupNames(Row r)
	{
		r.stream()
		 .skip(1)
		 .forEachOrdered(c -> {
			 String name = getCellValue(c);

			 if (StringUtils.isEmpty(name))
				 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Column: " + c.getColumnIndex() + " Missing group name");
			 else if (name.length() > 255)
				 addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Column: " + c.getColumnIndex() + " Value: " + name);
		 });
	}

	private void checkGroupVisibility(Row r)
	{
		r.stream()
		 .skip(1)
		 .forEachOrdered(c -> {
			 String visibility = getCellValue(c);

			 if (StringUtils.isEmpty(visibility))
				 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Column: " + c.getColumnIndex() + " Missing group visibility");
			 else if (!Objects.equals(visibility, "public") && !Objects.equals(visibility, "private"))
				 addImportResult(ImportStatus.GROUP_INVALID_GROUP_VISIBILITY, r.getRowNum(), "Column: " + c.getColumnIndex() + " Value: " + visibility);
		 });
	}

	private void checkCells(Row r)
	{
		r.stream()
		 .skip(1)
		 .forEach(c -> {
			 String value = getCellValue(c);

			 if (!StringUtils.isEmpty(value) && !Objects.equals(value, "x"))
				 addImportResult(ImportStatus.GROUP_INVALID_CELL_VALUE, r.getRowNum(), "Column: " + c.getColumnIndex() + " Value: " + value);
		 });
	}

	private void checkGermplasm(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String germplasm = getCellValue(r, 0);

		if (!accenumbToId.containsKey(germplasm))
			addImportResult(ImportStatus.GENERIC_INVALID_GERMPLASM, r.getRowNum(), germplasm);

		checkCells(r);
	}

	private void checkMarker(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String marker = getCellValue(r, 0);

		if (!markerNameToId.containsKey(marker))
			addImportResult(ImportStatus.GENERIC_INVALID_MARKER, r.getRowNum(), marker);

		checkCells(r);
	}

	private void checkLocation(Row r)
	{
		if (allCellsEmpty(r))
			return;

		String location = getCellValue(r, 0);

		if (!locationNameToId.containsKey(location))
			addImportResult(ImportStatus.GENERIC_INVALID_LOCATION, r.getRowNum(), location);

		checkCells(r);
	}

	private void importGroups(DSLContext context, List<Row> rows, int groupTypeId)
	{
		Row descriptions = rows.get(0);
		Row visibility = rows.get(1);
		Row names = rows.get(2);

		for (int i = 1; i < names.getCellCount(); i++)
		{
			String name = getCellValue(names, i);

			if (StringUtils.isEmpty(name))
				continue;

			GroupsRecord group = context.newRecord(GROUPS);
			group.setName(name);
			group.setDescription(descriptions.getCellCount() > i ? getCellValue(descriptions, i) : null);
			group.setGrouptypeId(groupTypeId);
			group.setCreatedBy(jobDetails.getUserId());
			group.setVisibility(Objects.equals("public", getCellValue(visibility, i)));
			group.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			group.store();

			groupIds.add(group.getId());

			// Store for this type the id of the group in this column
			groupIndexToIdPerType.get(groupTypeId).put(i, group.getId());
		}
	}

	private void importGroupMembers(DSLContext context, Sheet s, int groupTypeId)
		throws IOException
	{
		List<GroupmembersRecord> newGroupMembers = new ArrayList<>();

		// For each column/group
		for (int i = 0; i < groupIndexToIdPerType.get(groupTypeId).size(); i++)
		{
			final int index = i + 1;
			// Go through all rows
			s.openStream()
			 .skip(3)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String isPart = getCellValue(r, index);
				 String identifier = getCellValue(r, 0);

				 if (Objects.equals(isPart, "x"))
				 {
					 GroupmembersRecord groupMember = context.newRecord(GROUPMEMBERS);
					 groupMember.setGroupId(groupIndexToIdPerType.get(groupTypeId).get(index));
					 groupMember.setCreatedOn(new Timestamp(System.currentTimeMillis()));

					 switch (groupTypeId)
					 {
						 case 1:
							 groupMember.setForeignId(locationNameToId.get(identifier));
							 locationIds.add(groupMember.getForeignId());
							 break;
						 case 2:
							 groupMember.setForeignId(markerNameToId.get(identifier));
							 markerIds.add(groupMember.getForeignId());
							 break;
						 case 3:
							 groupMember.setForeignId(accenumbToId.get(identifier));
							 germplasmIds.add(groupMember.getForeignId());
							 break;
					 }
					 newGroupMembers.add(groupMember);

					 if (newGroupMembers.size() > 10000)
					 {
						 context.batchStore(newGroupMembers)
								.execute();
						 newGroupMembers.clear();
					 }
				 }
			 });
		}

		if (newGroupMembers.size() > 0)
		{
			context.batchStore(newGroupMembers)
				   .execute();
			newGroupMembers.clear();
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			try
			{
				wb.getSheets()
				  .filter(s -> Objects.equals(s.getName(), "GERMPLASM"))
				  .findFirst()
				  .ifPresent(s -> {
					  try
					  {
						  // First two rows
						  List<Row> rows = s.openStream()
											.limit(3)
											.collect(Collectors.toList());

						  importGroups(context, rows, 3);
						  importGroupMembers(context, s, 3);
					  }
					  catch (IOException e)
					  {
						  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
					  }
				  });
			}
			catch (NullPointerException e)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'GERMPLASM' sheet not found");
			}

			try
			{
				wb.getSheets()
				  .filter(s -> Objects.equals(s.getName(), "MARKERS"))
				  .findFirst()
				  .ifPresent(s -> {
					  try
					  {
						  // First two rows
						  List<Row> rows = s.openStream()
											.limit(3)
											.collect(Collectors.toList());

						  importGroups(context, rows, 2);
						  importGroupMembers(context, s, 2);
					  }
					  catch (IOException e)
					  {
						  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
					  }
				  });
			}
			catch (NullPointerException e)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'GERMPLASM' sheet not found");
			}

			try
			{
				wb.getSheets()
				  .filter(s -> Objects.equals(s.getName(), "LOCATIONS"))
				  .findFirst()
				  .ifPresent(s -> {
					  try
					  {
						  // First two rows
						  List<Row> rows = s.openStream()
											.limit(3)
											.collect(Collectors.toList());

						  importGroups(context, rows, 1);
						  importGroupMembers(context, s, 1);
					  }
					  catch (IOException e)
					  {
						  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
					  }
				  });
			}
			catch (NullPointerException e)
			{
				addImportResult(ImportStatus.GENERIC_MISSING_EXCEL_SHEET, -1, "'GERMPLASM' sheet not found");
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void postImport()
	{
		super.postImport();

		importJobStats.setMarkers(markerIds.size());
		importJobStats.setGermplasm(germplasmIds.size());
		importJobStats.setGroups(groupIds.size());
		importJobStats.setLocations(locationIds.size());
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		// We don't support updates
		importFile(wb);
	}
}
