package jhi.germinate.server.util.importer.util;

import java.beans.*;
import java.lang.reflect.Method;

public class Util
{
	public static boolean hasAnyFieldSet(Object pojo)
	{
		if (pojo == null)
		{
			return false;
		}

		try
		{
			for (PropertyDescriptor pd : Introspector.getBeanInfo(pojo.getClass(), Object.class).getPropertyDescriptors())
			{
				Method getter = pd.getReadMethod();
				if (getter != null)
				{
					Object value = getter.invoke(pojo);
					if (value != null)
					{
						return true;
					}
				}
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}

		return false;
	}
}
