using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>
	/// JUnit test class for
	/// <see cref="Count"/>
	/// </summary>
	public class TestCount
	{
		private const string WithQuotas = "Content summary with quotas";

		private const string NoQuotas = "Content summary without quotas";

		private const string Human = "human: ";

		private const string Bytes = "bytes: ";

		private static Configuration conf;

		private static FileSystem mockFs;

		private static FileStatus fileStat;

		private static ContentSummary mockCs;

		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestCount.MockFileSystem), typeof(FileSystem
				));
			mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			fileStat = Org.Mockito.Mockito.Mock<FileStatus>();
			mockCs = Org.Mockito.Mockito.Mock<ContentSummary>();
			Org.Mockito.Mockito.When(fileStat.IsFile()).ThenReturn(true);
		}

		[SetUp]
		public virtual void ResetMock()
		{
			Org.Mockito.Mockito.Reset(mockFs);
		}

		[Fact]
		public virtual void ProcessOptionsHumanReadable()
		{
			List<string> options = new List<string>();
			options.AddItem("-h");
			options.AddItem("dummy");
			Count count = new Count();
			count.ProcessOptions(options);
			NUnit.Framework.Assert.IsFalse(count.IsShowQuotas());
			Assert.True(count.IsHumanReadable());
		}

		[Fact]
		public virtual void ProcessOptionsAll()
		{
			List<string> options = new List<string>();
			options.AddItem("-q");
			options.AddItem("-h");
			options.AddItem("dummy");
			Count count = new Count();
			count.ProcessOptions(options);
			Assert.True(count.IsShowQuotas());
			Assert.True(count.IsHumanReadable());
		}

		// check quotas are reported correctly
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void ProcessPathShowQuotas()
		{
			Path path = new Path("mockfs:/test");
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(fileStat);
			PathData pathData = new PathData(path.ToString(), conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			Count count = new Count();
			count.@out = @out;
			List<string> options = new List<string>();
			options.AddItem("-q");
			options.AddItem("dummy");
			count.ProcessOptions(options);
			count.ProcessPath(pathData);
			Org.Mockito.Mockito.Verify(@out).WriteLine(Bytes + WithQuotas + path.ToString());
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
		}

		// check counts without quotas are reported correctly
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void ProcessPathNoQuotas()
		{
			Path path = new Path("mockfs:/test");
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(fileStat);
			PathData pathData = new PathData(path.ToString(), conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			Count count = new Count();
			count.@out = @out;
			List<string> options = new List<string>();
			options.AddItem("dummy");
			count.ProcessOptions(options);
			count.ProcessPath(pathData);
			Org.Mockito.Mockito.Verify(@out).WriteLine(Bytes + NoQuotas + path.ToString());
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void ProcessPathShowQuotasHuman()
		{
			Path path = new Path("mockfs:/test");
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(fileStat);
			PathData pathData = new PathData(path.ToString(), conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			Count count = new Count();
			count.@out = @out;
			List<string> options = new List<string>();
			options.AddItem("-q");
			options.AddItem("-h");
			options.AddItem("dummy");
			count.ProcessOptions(options);
			count.ProcessPath(pathData);
			Org.Mockito.Mockito.Verify(@out).WriteLine(Human + WithQuotas + path.ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void ProcessPathNoQuotasHuman()
		{
			Path path = new Path("mockfs:/test");
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(fileStat);
			PathData pathData = new PathData(path.ToString(), conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			Count count = new Count();
			count.@out = @out;
			List<string> options = new List<string>();
			options.AddItem("-h");
			options.AddItem("dummy");
			count.ProcessOptions(options);
			count.ProcessPath(pathData);
			Org.Mockito.Mockito.Verify(@out).WriteLine(Human + NoQuotas + path.ToString());
		}

		[Fact]
		public virtual void GetCommandName()
		{
			Count count = new Count();
			string actual = count.GetCommandName();
			string expected = "count";
			Assert.Equal("Count.getCommandName", expected, actual);
		}

		[Fact]
		public virtual void IsDeprecated()
		{
			Count count = new Count();
			bool actual = count.IsDeprecated();
			bool expected = false;
			Assert.Equal("Count.isDeprecated", expected, actual);
		}

		[Fact]
		public virtual void GetReplacementCommand()
		{
			Count count = new Count();
			string actual = count.GetReplacementCommand();
			string expected = null;
			Assert.Equal("Count.getReplacementCommand", expected, actual);
		}

		[Fact]
		public virtual void GetName()
		{
			Count count = new Count();
			string actual = count.GetName();
			string expected = "count";
			Assert.Equal("Count.getName", expected, actual);
		}

		[Fact]
		public virtual void GetUsage()
		{
			Count count = new Count();
			string actual = count.GetUsage();
			string expected = "-count [-q] [-h] <path> ...";
			Assert.Equal("Count.getUsage", expected, actual);
		}

		internal class MockContentSummary : ContentSummary
		{
			public MockContentSummary()
			{
			}

			// mock content system
			// suppress warning on the usage of deprecated ContentSummary constructor
			public override string ToString(bool qOption, bool hOption)
			{
				if (qOption)
				{
					if (hOption)
					{
						return (Human + WithQuotas);
					}
					else
					{
						return (Bytes + WithQuotas);
					}
				}
				else
				{
					if (hOption)
					{
						return (Human + NoQuotas);
					}
					else
					{
						return (Bytes + NoQuotas);
					}
				}
			}
		}

		internal class MockFileSystem : FilterFileSystem
		{
			internal Configuration conf;

			internal MockFileSystem()
				: base(mockFs)
			{
			}

			// mock file system for use in testing
			public override void Initialize(URI uri, Configuration conf)
			{
				this.conf = conf;
			}

			public override Path MakeQualified(Path path)
			{
				return path;
			}

			/// <exception cref="System.IO.IOException"/>
			public override ContentSummary GetContentSummary(Path f)
			{
				return new TestCount.MockContentSummary();
			}

			public override Configuration GetConf()
			{
				return conf;
			}
		}
	}
}
