using System.Collections;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class SysPropsForTestsLoader
	{
		public const string TestPropertiesProp = "test.properties";

		static SysPropsForTestsLoader()
		{
			try
			{
				string testFileName = Runtime.GetProperty(TestPropertiesProp, "test.properties");
				FilePath currentDir = new FilePath(testFileName).GetAbsoluteFile().GetParentFile(
					);
				FilePath testFile = new FilePath(currentDir, testFileName);
				while (currentDir != null && !testFile.Exists())
				{
					testFile = new FilePath(testFile.GetAbsoluteFile().GetParentFile().GetParentFile(
						), testFileName);
					currentDir = currentDir.GetParentFile();
					if (currentDir != null)
					{
						testFile = new FilePath(currentDir, testFileName);
					}
				}
				if (testFile.Exists())
				{
					System.Console.Out.WriteLine();
					System.Console.Out.WriteLine(">>> " + TestPropertiesProp + " : " + testFile.GetAbsolutePath
						());
					Properties testProperties = new Properties();
					testProperties.Load(new FileReader(testFile));
					foreach (DictionaryEntry entry in testProperties)
					{
						if (!Runtime.GetProperties().Contains(entry.Key))
						{
							Runtime.SetProperty((string)entry.Key, (string)entry.Value);
						}
					}
				}
				else
				{
					if (Runtime.GetProperty(TestPropertiesProp) != null)
					{
						System.Console.Error.WriteLine(MessageFormat.Format("Specified 'test.properties' file does not exist [{0}]"
							, Runtime.GetProperty(TestPropertiesProp)));
						System.Environment.Exit(-1);
					}
					else
					{
						System.Console.Out.WriteLine(">>> " + TestPropertiesProp + " : <NONE>");
					}
				}
			}
			catch (IOException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		public static void Init()
		{
		}
	}
}
