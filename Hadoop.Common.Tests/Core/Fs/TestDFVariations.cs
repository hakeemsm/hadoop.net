using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestDFVariations
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data") + "/TestDFVariations";

		private static FilePath test_root = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			test_root = new FilePath(TestRootDir);
			test_root.Mkdirs();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void After()
		{
			FileUtil.SetWritable(test_root, true);
			FileUtil.FullyDelete(test_root);
			NUnit.Framework.Assert.IsTrue(!test_root.Exists());
		}

		public class XXDF : DF
		{
			/// <exception cref="System.IO.IOException"/>
			public XXDF()
				: base(test_root, 0L)
			{
			}

			protected internal override string[] GetExecString()
			{
				return new string[] { "echo", "IGNORE\n", "/dev/sda3", "453115160", "53037920", "400077240"
					, "11%", "/foo/bar\n" };
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMount()
		{
			TestDFVariations.XXDF df = new TestDFVariations.XXDF();
			string expectedMount = Shell.Windows ? Sharpen.Runtime.Substring(df.GetDirPath(), 
				0, 2) : "/foo/bar";
			NUnit.Framework.Assert.AreEqual("Invalid mount point", expectedMount, df.GetMount
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFileSystem()
		{
			TestDFVariations.XXDF df = new TestDFVariations.XXDF();
			string expectedFileSystem = Shell.Windows ? Sharpen.Runtime.Substring(df.GetDirPath
				(), 0, 2) : "/dev/sda3";
			NUnit.Framework.Assert.AreEqual("Invalid filesystem", expectedFileSystem, df.GetFilesystem
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDFInvalidPath()
		{
			// Generate a path that doesn't exist
			Random random = new Random(unchecked((long)(0xDEADBEEFl)));
			FilePath file = null;
			byte[] bytes = new byte[64];
			while (file == null)
			{
				random.NextBytes(bytes);
				string invalid = new string("/" + bytes);
				FilePath invalidFile = new FilePath(invalid);
				if (!invalidFile.Exists())
				{
					file = invalidFile;
				}
			}
			DF df = new DF(file, 0l);
			try
			{
				df.GetMount();
			}
			catch (FileNotFoundException e)
			{
				// expected, since path does not exist
				GenericTestUtils.AssertExceptionContains(file.GetName(), e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDFMalformedOutput()
		{
			DF df = new DF(new FilePath("/"), 0l);
			BufferedReader reader = new BufferedReader(new StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on\n"
				 + "/dev/sda5       19222656 10597036   7649060  59% /"));
			df.ParseExecResult(reader);
			df.ParseOutput();
			reader = new BufferedReader(new StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on"
				));
			df.ParseExecResult(reader);
			try
			{
				df.ParseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with missing line!");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Fewer lines of output than expected", e
					);
				System.Console.Out.WriteLine(e.ToString());
			}
			reader = new BufferedReader(new StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on\n"
				 + " "));
			df.ParseExecResult(reader);
			try
			{
				df.ParseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with empty line!");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Unexpected empty line", e);
				System.Console.Out.WriteLine(e.ToString());
			}
			reader = new BufferedReader(new StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on\n"
				 + "       19222656 10597036   7649060  59% /"));
			df.ParseExecResult(reader);
			try
			{
				df.ParseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with missing field!");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Could not parse line: ", e);
				System.Console.Out.WriteLine(e.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetMountCurrentDirectory()
		{
			FilePath currentDirectory = new FilePath(".");
			string workingDir = currentDirectory.GetAbsoluteFile().GetCanonicalPath();
			DF df = new DF(new FilePath(workingDir), 0L);
			string mountPath = df.GetMount();
			FilePath mountDir = new FilePath(mountPath);
			NUnit.Framework.Assert.IsTrue("Mount dir [" + mountDir.GetAbsolutePath() + "] should exist."
				, mountDir.Exists());
			NUnit.Framework.Assert.IsTrue("Mount dir [" + mountDir.GetAbsolutePath() + "] should be directory."
				, mountDir.IsDirectory());
			NUnit.Framework.Assert.IsTrue("Working dir [" + workingDir + "] should start with ["
				 + mountPath + "].", workingDir.StartsWith(mountPath));
		}
	}
}
