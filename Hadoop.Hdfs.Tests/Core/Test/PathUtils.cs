using System;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class PathUtils
	{
		public static Path GetTestPath(Type caller)
		{
			return GetTestPath(caller, true);
		}

		public static Path GetTestPath(Type caller, bool create)
		{
			return new Path(GetTestDirName(caller));
		}

		public static FilePath GetTestDir(Type caller)
		{
			return GetTestDir(caller, true);
		}

		public static FilePath GetTestDir(Type caller, bool create)
		{
			FilePath dir = new FilePath(Runtime.GetProperty("test.build.data", "target/test/data"
				) + "/" + RandomStringUtils.RandomAlphanumeric(10), caller.Name);
			if (create)
			{
				dir.Mkdirs();
			}
			return dir;
		}

		public static string GetTestDirName(Type caller)
		{
			return GetTestDirName(caller, true);
		}

		public static string GetTestDirName(Type caller, bool create)
		{
			return GetTestDir(caller, create).GetAbsolutePath();
		}
	}
}
