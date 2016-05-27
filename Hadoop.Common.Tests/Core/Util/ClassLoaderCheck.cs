using System;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class ClassLoaderCheck
	{
		/// <summary>Verifies the class is loaded by the right classloader.</summary>
		public static void CheckClassLoader(Type cls, bool shouldBeLoadedByAppClassLoader
			)
		{
			bool loadedByAppClassLoader = cls.GetClassLoader() is ApplicationClassLoader;
			if ((shouldBeLoadedByAppClassLoader && !loadedByAppClassLoader) || (!shouldBeLoadedByAppClassLoader
				 && loadedByAppClassLoader))
			{
				throw new RuntimeException("incorrect classloader used");
			}
		}
	}
}
