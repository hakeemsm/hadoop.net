using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestXAttrCommands
	{
		private readonly java.io.ByteArrayOutputStream errContent = new java.io.ByteArrayOutputStream
			();

		private org.apache.hadoop.conf.Configuration conf = null;

		private System.IO.TextWriter initialStdErr;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			errContent.reset();
			initialStdErr = System.Console.Error;
			Sharpen.Runtime.setErr(new System.IO.TextWriter(errContent));
			conf = new org.apache.hadoop.conf.Configuration();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void cleanUp()
		{
			errContent.reset();
			Sharpen.Runtime.setErr(initialStdErr);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetfattrValidations()
		{
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail without path", 0 == runCommand
				(new string[] { "-getfattr", "-d" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("<path> is missing")
				);
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail with extra argument", 0 == runCommand
				(new string[] { "-getfattr", "extra", "-d", "/test" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("Too many arguments"
				));
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail without \"-n name\" or \"-d\""
				, 0 == runCommand(new string[] { "-getfattr", "/test" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("Must specify '-n name' or '-d' option"
				));
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("getfattr should fail with invalid encoding", 0 ==
				 runCommand(new string[] { "-getfattr", "-d", "-e", "aaa", "/test" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("Invalid/unsupported encoding option specified: aaa"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSetfattrValidations()
		{
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail without path", 0 == runCommand
				(new string[] { "-setfattr", "-n", "user.a1" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("<path> is missing")
				);
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail with extra arguments", 0 == 
				runCommand(new string[] { "-setfattr", "extra", "-n", "user.a1", "/test" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("Too many arguments"
				));
			errContent.reset();
			NUnit.Framework.Assert.IsFalse("setfattr should fail without \"-n name\" or \"-x name\""
				, 0 == runCommand(new string[] { "-setfattr", "/test" }));
			NUnit.Framework.Assert.IsTrue(errContent.ToString().contains("Must specify '-n name' or '-x name' option"
				));
		}

		/// <exception cref="System.Exception"/>
		private int runCommand(string[] commands)
		{
			return org.apache.hadoop.util.ToolRunner.run(conf, new org.apache.hadoop.fs.FsShell
				(), commands);
		}
	}
}
