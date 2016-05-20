using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>
	/// JUnit test class for
	/// <see cref="Count"/>
	/// </summary>
	public class TestCount
	{
		private const string WITH_QUOTAS = "Content summary with quotas";

		private const string NO_QUOTAS = "Content summary without quotas";

		private const string HUMAN = "human: ";

		private const string BYTES = "bytes: ";

		private static org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.fs.FileSystem mockFs;

		private static org.apache.hadoop.fs.FileStatus fileStat;

		private static org.apache.hadoop.fs.ContentSummary mockCs;

		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCount.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem>();
			fileStat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus>();
			mockCs = org.mockito.Mockito.mock<org.apache.hadoop.fs.ContentSummary>();
			org.mockito.Mockito.when(fileStat.isFile()).thenReturn(true);
		}

		[NUnit.Framework.SetUp]
		public virtual void resetMock()
		{
			org.mockito.Mockito.reset(mockFs);
		}

		[NUnit.Framework.Test]
		public virtual void processOptionsHumanReadable()
		{
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("-h");
			options.add("dummy");
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.processOptions(options);
			NUnit.Framework.Assert.IsFalse(count.isShowQuotas());
			NUnit.Framework.Assert.IsTrue(count.isHumanReadable());
		}

		[NUnit.Framework.Test]
		public virtual void processOptionsAll()
		{
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("-q");
			options.add("-h");
			options.add("dummy");
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.processOptions(options);
			NUnit.Framework.Assert.IsTrue(count.isShowQuotas());
			NUnit.Framework.Assert.IsTrue(count.isHumanReadable());
		}

		// check quotas are reported correctly
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void processPathShowQuotas()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mockfs:/test");
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
			org.apache.hadoop.fs.shell.PathData pathData = new org.apache.hadoop.fs.shell.PathData
				(path.ToString(), conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.@out = @out;
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("-q");
			options.add("dummy");
			count.processOptions(options);
			count.processPath(pathData);
			org.mockito.Mockito.verify(@out).WriteLine(BYTES + WITH_QUOTAS + path.ToString());
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
		}

		// check counts without quotas are reported correctly
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void processPathNoQuotas()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mockfs:/test");
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
			org.apache.hadoop.fs.shell.PathData pathData = new org.apache.hadoop.fs.shell.PathData
				(path.ToString(), conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.@out = @out;
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("dummy");
			count.processOptions(options);
			count.processPath(pathData);
			org.mockito.Mockito.verify(@out).WriteLine(BYTES + NO_QUOTAS + path.ToString());
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void processPathShowQuotasHuman()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mockfs:/test");
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
			org.apache.hadoop.fs.shell.PathData pathData = new org.apache.hadoop.fs.shell.PathData
				(path.ToString(), conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.@out = @out;
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("-q");
			options.add("-h");
			options.add("dummy");
			count.processOptions(options);
			count.processPath(pathData);
			org.mockito.Mockito.verify(@out).WriteLine(HUMAN + WITH_QUOTAS + path.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void processPathNoQuotasHuman()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mockfs:/test");
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
			org.apache.hadoop.fs.shell.PathData pathData = new org.apache.hadoop.fs.shell.PathData
				(path.ToString(), conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			count.@out = @out;
			System.Collections.Generic.LinkedList<string> options = new System.Collections.Generic.LinkedList
				<string>();
			options.add("-h");
			options.add("dummy");
			count.processOptions(options);
			count.processPath(pathData);
			org.mockito.Mockito.verify(@out).WriteLine(HUMAN + NO_QUOTAS + path.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void getCommandName()
		{
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			string actual = count.getCommandName();
			string expected = "count";
			NUnit.Framework.Assert.AreEqual("Count.getCommandName", expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void isDeprecated()
		{
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			bool actual = count.isDeprecated();
			bool expected = false;
			NUnit.Framework.Assert.AreEqual("Count.isDeprecated", expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void getReplacementCommand()
		{
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			string actual = count.getReplacementCommand();
			string expected = null;
			NUnit.Framework.Assert.AreEqual("Count.getReplacementCommand", expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void getName()
		{
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			string actual = count.getName();
			string expected = "count";
			NUnit.Framework.Assert.AreEqual("Count.getName", expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void getUsage()
		{
			org.apache.hadoop.fs.shell.Count count = new org.apache.hadoop.fs.shell.Count();
			string actual = count.getUsage();
			string expected = "-count [-q] [-h] <path> ...";
			NUnit.Framework.Assert.AreEqual("Count.getUsage", expected, actual);
		}

		internal class MockContentSummary : org.apache.hadoop.fs.ContentSummary
		{
			public MockContentSummary()
			{
			}

			// mock content system
			// suppress warning on the usage of deprecated ContentSummary constructor
			public override string toString(bool qOption, bool hOption)
			{
				if (qOption)
				{
					if (hOption)
					{
						return (HUMAN + WITH_QUOTAS);
					}
					else
					{
						return (BYTES + WITH_QUOTAS);
					}
				}
				else
				{
					if (hOption)
					{
						return (HUMAN + NO_QUOTAS);
					}
					else
					{
						return (BYTES + NO_QUOTAS);
					}
				}
			}
		}

		internal class MockFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			internal org.apache.hadoop.conf.Configuration conf;

			internal MockFileSystem()
				: base(mockFs)
			{
			}

			// mock file system for use in testing
			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.conf = conf;
			}

			public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
				 path)
			{
				return path;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
				 f)
			{
				return new org.apache.hadoop.fs.shell.TestCount.MockContentSummary();
			}

			public override org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}
		}
	}
}
