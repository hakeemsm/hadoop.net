using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestXAttrCommands
	{
		private readonly ByteArrayOutputStream errContent = new ByteArrayOutputStream();

		private Configuration conf = null;

		private TextWriter initialStdErr;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			errContent.Reset();
			initialStdErr = System.Console.Error;
			Runtime.SetErr(new TextWriter(errContent));
			conf = new Configuration();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanUp()
		{
			errContent.Reset();
			Runtime.SetErr(initialStdErr);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetfattrValidations()
		{
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail without path", 0 == RunCommand
				(new string[] { "-getfattr", "-d" }));
			Assert.True(errContent.ToString().Contains("<path> is missing")
				);
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail with extra argument", 0 == RunCommand
				(new string[] { "-getfattr", "extra", "-d", "/test" }));
			Assert.True(errContent.ToString().Contains("Too many arguments"
				));
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail without \"-n name\" or \"-d\""
				, 0 == RunCommand(new string[] { "-getfattr", "/test" }));
			Assert.True(errContent.ToString().Contains("Must specify '-n name' or '-d' option"
				));
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail with invalid encoding", 0 ==
				 RunCommand(new string[] { "-getfattr", "-d", "-e", "aaa", "/test" }));
			Assert.True(errContent.ToString().Contains("Invalid/unsupported encoding option specified: aaa"
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSetfattrValidations()
		{
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail without path", 0 == RunCommand
				(new string[] { "-setfattr", "-n", "user.a1" }));
			Assert.True(errContent.ToString().Contains("<path> is missing")
				);
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail with extra arguments", 0 == 
				RunCommand(new string[] { "-setfattr", "extra", "-n", "user.a1", "/test" }));
			Assert.True(errContent.ToString().Contains("Too many arguments"
				));
			errContent.Reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail without \"-n name\" or \"-x name\""
				, 0 == RunCommand(new string[] { "-setfattr", "/test" }));
			Assert.True(errContent.ToString().Contains("Must specify '-n name' or '-x name' option"
				));
		}

		/// <exception cref="System.Exception"/>
		private int RunCommand(string[] commands)
		{
			return ToolRunner.Run(conf, new FsShell(), commands);
		}
	}
}
