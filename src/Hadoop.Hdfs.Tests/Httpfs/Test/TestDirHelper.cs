using System;
using System.IO;
using NUnit.Framework.Rules;
using NUnit.Framework.Runners.Model;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestDirHelper : MethodRule
	{
		[NUnit.Framework.Test]
		public virtual void Dummy()
		{
		}

		static TestDirHelper()
		{
			SysPropsForTestsLoader.Init();
		}

		public const string TestDirProp = "test.dir";

		internal static string TestDirRoot;

		/// <exception cref="System.IO.IOException"/>
		private static void Delete(FilePath file)
		{
			if (file.GetAbsolutePath().Length < 5)
			{
				throw new ArgumentException(MessageFormat.Format("Path [{0}] is too short, not deleting"
					, file.GetAbsolutePath()));
			}
			if (file.Exists())
			{
				if (file.IsDirectory())
				{
					FilePath[] children = file.ListFiles();
					if (children != null)
					{
						foreach (FilePath child in children)
						{
							Delete(child);
						}
					}
				}
				if (!file.Delete())
				{
					throw new RuntimeException(MessageFormat.Format("Could not delete path [{0}]", file
						.GetAbsolutePath()));
				}
			}
		}

		static TestDirHelper()
		{
			try
			{
				TestDirRoot = Runtime.GetProperty(TestDirProp, new FilePath("target").GetAbsolutePath
					());
				if (!new FilePath(TestDirRoot).IsAbsolute())
				{
					System.Console.Error.WriteLine(MessageFormat.Format("System property [{0}]=[{1}] must be set to an absolute path"
						, TestDirProp, TestDirRoot));
					System.Environment.Exit(-1);
				}
				else
				{
					if (TestDirRoot.Length < 4)
					{
						System.Console.Error.WriteLine(MessageFormat.Format("System property [{0}]=[{1}] must be at least 4 chars"
							, TestDirProp, TestDirRoot));
						System.Environment.Exit(-1);
					}
				}
				TestDirRoot = new FilePath(TestDirRoot, "test-dir").GetAbsolutePath();
				Runtime.SetProperty(TestDirProp, TestDirRoot);
				FilePath dir = new FilePath(TestDirRoot);
				Delete(dir);
				if (!dir.Mkdirs())
				{
					System.Console.Error.WriteLine(MessageFormat.Format("Could not create test dir [{0}]"
						, TestDirRoot));
					System.Environment.Exit(-1);
				}
				System.Console.Out.WriteLine(">>> " + TestDirProp + "        : " + Runtime.GetProperty
					(TestDirProp));
			}
			catch (IOException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		private static ThreadLocal<FilePath> TestDirTl = new InheritableThreadLocal<FilePath
			>();

		public virtual Statement Apply(Statement statement, FrameworkMethod frameworkMethod
			, object o)
		{
			return new _Statement_96(frameworkMethod, statement);
		}

		private sealed class _Statement_96 : Statement
		{
			public _Statement_96(FrameworkMethod frameworkMethod, Statement statement)
			{
				this.frameworkMethod = frameworkMethod;
				this.statement = statement;
			}

			/// <exception cref="System.Exception"/>
			public override void Evaluate()
			{
				FilePath testDir = null;
				TestDir testDirAnnotation = frameworkMethod.GetAnnotation<TestDir>();
				if (testDirAnnotation != null)
				{
					testDir = TestDirHelper.ResetTestCaseDir(frameworkMethod.GetName());
				}
				try
				{
					TestDirHelper.TestDirTl.Set(testDir);
					statement.Evaluate();
				}
				finally
				{
					TestDirHelper.TestDirTl.Remove();
				}
			}

			private readonly FrameworkMethod frameworkMethod;

			private readonly Statement statement;
		}

		/// <summary>
		/// Returns the local test directory for the current test, only available when the
		/// test method has been annotated with
		/// <see cref="TestDir"/>
		/// .
		/// </summary>
		/// <returns>
		/// the test directory for the current test. It is an full/absolute
		/// <code>File</code>.
		/// </returns>
		public static FilePath GetTestDir()
		{
			FilePath testDir = TestDirTl.Get();
			if (testDir == null)
			{
				throw new InvalidOperationException("This test does not use @TestDir");
			}
			return testDir;
		}

		private static AtomicInteger counter = new AtomicInteger();

		private static FilePath ResetTestCaseDir(string testName)
		{
			FilePath dir = new FilePath(TestDirRoot);
			dir = new FilePath(dir, testName + "-" + counter.GetAndIncrement());
			dir = dir.GetAbsoluteFile();
			try
			{
				Delete(dir);
			}
			catch (IOException ex)
			{
				throw new RuntimeException(MessageFormat.Format("Could not delete test dir[{0}], {1}"
					, dir, ex.Message), ex);
			}
			if (!dir.Mkdirs())
			{
				throw new RuntimeException(MessageFormat.Format("Could not create test dir[{0}]", 
					dir));
			}
			return dir;
		}
	}
}
