package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.*;
import jhi.germinate.server.util.importer.util.GermplasmNotFoundException;
import org.jooq.DSLContext;

import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;

public class GermplasmLookup
{
	private final Map<String, Integer>      germplasmToId   = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private final Map<String, Set<Integer>> displayNameToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	public GermplasmLookup()
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			context.selectFrom(GERMINATEBASE)
				   .forEach(g -> {
					   germplasmToId.put(g.getName(), g.getId());
					   if (!StringUtils.isEmpty(g.getDisplayName()))
					   {
						   Set<Integer> ids = displayNameToId.get(g.getDisplayName());

						   if (ids == null)
							   ids = new HashSet<>();

						   ids.add(g.getId());

						   displayNameToId.put(g.getDisplayName(), ids);
					   }
				   });
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	public boolean containsGermplasmId(Integer id)
	{
		return germplasmToId.containsKey(id);
	}

	public Integer getGermplasmId(String germplasmName)
	{
		// Check if a germplasm with this exact name as the identifier exists
		Integer result = germplasmToId.get(germplasmName);

		if (result == null)
		{
			// If not, check the display names
			Set<Integer> possibleIds = displayNameToId.get(germplasmName);

			if (CollectionUtils.isEmpty(possibleIds))
			{
				// There is no germplasm with this display name, so no match found overall
				throw new GermplasmNotFoundException(ImportStatus.GENERIC_INVALID_GERMPLASM);
			}
			else if (possibleIds.size() > 1)
			{
				// There is germplasm with this display name, but it's not unique, this is an issue
				throw new GermplasmNotFoundException(ImportStatus.GENERIC_DISPLAY_NAME_USED_BUT_NOT_UNIQUE);
			}

			// Otherwise, just use the one single match based on display name
			result = possibleIds.iterator().next();
		}

		// Return any result (which will exist at this point, otherwise we'd have thrown an exception
		return result;
	}
}
