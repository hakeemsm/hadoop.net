using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestDFVariations
	{
		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data") + "/TestDFVariations";

		private static java.io.File test_root = null;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			test_root = new java.io.File(TEST_ROOT_DIR);
			test_root.mkdirs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void after()
		{
			org.apache.hadoop.fs.FileUtil.setWritable(test_root, true);
			org.apache.hadoop.fs.FileUtil.fullyDelete(test_root);
			NUnit.Framework.Assert.IsTrue(!test_root.exists());
		}

		public class XXDF : org.apache.hadoop.fs.DF
		{
			/// <exception cref="System.IO.IOException"/>
			public XXDF()
				: base(test_root, 0L)
			{
			}

			protected internal override string[] getExecString()
			{
				return new string[] { "echo", "IGNORE\n", "/dev/sda3", "453115160", "53037920", "400077240"
					, "11%", "/foo/bar\n" };
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMount()
		{
			org.apache.hadoop.fs.TestDFVariations.XXDF df = new org.apache.hadoop.fs.TestDFVariations.XXDF
				();
			string expectedMount = org.apache.hadoop.util.Shell.WINDOWS ? Sharpen.Runtime.substring
				(df.getDirPath(), 0, 2) : "/foo/bar";
			NUnit.Framework.Assert.AreEqual("Invalid mount point", expectedMount, df.getMount
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFileSystem()
		{
			org.apache.hadoop.fs.TestDFVariations.XXDF df = new org.apache.hadoop.fs.TestDFVariations.XXDF
				();
			string expectedFileSystem = org.apache.hadoop.util.Shell.WINDOWS ? Sharpen.Runtime.substring
				(df.getDirPath(), 0, 2) : "/dev/sda3";
			NUnit.Framework.Assert.AreEqual("Invalid filesystem", expectedFileSystem, df.getFilesystem
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDFInvalidPath()
		{
			// Generate a path that doesn't exist
			java.util.Random random = new java.util.Random(unchecked((long)(0xDEADBEEFl)));
			java.io.File file = null;
			byte[] bytes = new byte[64];
			while (file == null)
			{
				random.nextBytes(bytes);
				string invalid = new string("/" + bytes);
				java.io.File invalidFile = new java.io.File(invalid);
				if (!invalidFile.exists())
				{
					file = invalidFile;
				}
			}
			org.apache.hadoop.fs.DF df = new org.apache.hadoop.fs.DF(file, 0l);
			try
			{
				df.getMount();
			}
			catch (java.io.FileNotFoundException e)
			{
				// expected, since path does not exist
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains(file.getName(), e
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDFMalformedOutput()
		{
			org.apache.hadoop.fs.DF df = new org.apache.hadoop.fs.DF(new java.io.File("/"), 0l
				);
			java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.StringReader
				("Filesystem     1K-blocks     Used Available Use% Mounted on\n" + "/dev/sda5       19222656 10597036   7649060  59% /"
				));
			df.parseExecResult(reader);
			df.parseOutput();
			reader = new java.io.BufferedReader(new java.io.StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on"
				));
			df.parseExecResult(reader);
			try
			{
				df.parseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with missing line!");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Fewer lines of output than expected"
					, e);
				System.Console.Out.WriteLine(e.ToString());
			}
			reader = new java.io.BufferedReader(new java.io.StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on\n"
				 + " "));
			df.parseExecResult(reader);
			try
			{
				df.parseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with empty line!");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Unexpected empty line"
					, e);
				System.Console.Out.WriteLine(e.ToString());
			}
			reader = new java.io.BufferedReader(new java.io.StringReader("Filesystem     1K-blocks     Used Available Use% Mounted on\n"
				 + "       19222656 10597036   7649060  59% /"));
			df.parseExecResult(reader);
			try
			{
				df.parseOutput();
				NUnit.Framework.Assert.Fail("Expected exception with missing field!");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Could not parse line: "
					, e);
				System.Console.Out.WriteLine(e.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetMountCurrentDirectory()
		{
			java.io.File currentDirectory = new java.io.File(".");
			string workingDir = currentDirectory.getAbsoluteFile().getCanonicalPath();
			org.apache.hadoop.fs.DF df = new org.apache.hadoop.fs.DF(new java.io.File(workingDir
				), 0L);
			string mountPath = df.getMount();
			java.io.File mountDir = new java.io.File(mountPath);
			NUnit.Framework.Assert.IsTrue("Mount dir [" + mountDir.getAbsolutePath() + "] should exist."
				, mountDir.exists());
			NUnit.Framework.Assert.IsTrue("Mount dir [" + mountDir.getAbsolutePath() + "] should be directory."
				, mountDir.isDirectory());
			NUnit.Framework.Assert.IsTrue("Working dir [" + workingDir + "] should start with ["
				 + mountPath + "].", workingDir.StartsWith(mountPath));
		}
	}
}
