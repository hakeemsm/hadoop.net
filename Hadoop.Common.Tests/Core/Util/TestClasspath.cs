using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Tests covering the classpath command-line utility.</summary>
	public class TestClasspath
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClasspath));

		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "TestClasspath");

		private static readonly Encoding Utf8 = Sharpen.Extensions.GetEncoding("UTF-8");

		static TestClasspath()
		{
			ExitUtil.DisableSystemExit();
		}

		private TextWriter oldStdout;

		private TextWriter oldStderr;

		private ByteArrayOutputStream stdout;

		private ByteArrayOutputStream stderr;

		private TextWriter printStdout;

		private TextWriter printStderr;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(TestDir));
			NUnit.Framework.Assert.IsTrue(TestDir.Mkdirs());
			oldStdout = System.Console.Out;
			oldStderr = System.Console.Error;
			stdout = new ByteArrayOutputStream();
			printStdout = new TextWriter(stdout);
			Runtime.SetOut(printStdout);
			stderr = new ByteArrayOutputStream();
			printStderr = new TextWriter(stderr);
			Runtime.SetErr(printStderr);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			Runtime.SetOut(oldStdout);
			Runtime.SetErr(oldStderr);
			IOUtils.Cleanup(Log, printStdout, printStderr);
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(TestDir));
		}

		[NUnit.Framework.Test]
		public virtual void TestGlob()
		{
			Classpath.Main(new string[] { "--glob" });
			string strOut = new string(stdout.ToByteArray(), Utf8);
			NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("java.class.path"), strOut.Trim
				());
			NUnit.Framework.Assert.IsTrue(stderr.ToByteArray().Length == 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJar()
		{
			FilePath file = new FilePath(TestDir, "classpath.jar");
			Classpath.Main(new string[] { "--jar", file.GetAbsolutePath() });
			NUnit.Framework.Assert.IsTrue(stdout.ToByteArray().Length == 0);
			NUnit.Framework.Assert.IsTrue(stderr.ToByteArray().Length == 0);
			NUnit.Framework.Assert.IsTrue(file.Exists());
			AssertJar(file);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJarReplace()
		{
			// Run the command twice with the same output jar file, and expect success.
			TestJar();
			TestJar();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJarFileMissing()
		{
			try
			{
				Classpath.Main(new string[] { "--jar" });
				NUnit.Framework.Assert.Fail("expected exit");
			}
			catch (ExitUtil.ExitException)
			{
				NUnit.Framework.Assert.IsTrue(stdout.ToByteArray().Length == 0);
				string strErr = new string(stderr.ToByteArray(), Utf8);
				NUnit.Framework.Assert.IsTrue(strErr.Contains("requires path of jar"));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestHelp()
		{
			Classpath.Main(new string[] { "--help" });
			string strOut = new string(stdout.ToByteArray(), Utf8);
			NUnit.Framework.Assert.IsTrue(strOut.Contains("Prints the classpath"));
			NUnit.Framework.Assert.IsTrue(stderr.ToByteArray().Length == 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestHelpShort()
		{
			Classpath.Main(new string[] { "-h" });
			string strOut = new string(stdout.ToByteArray(), Utf8);
			NUnit.Framework.Assert.IsTrue(strOut.Contains("Prints the classpath"));
			NUnit.Framework.Assert.IsTrue(stderr.ToByteArray().Length == 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnrecognized()
		{
			try
			{
				Classpath.Main(new string[] { "--notarealoption" });
				NUnit.Framework.Assert.Fail("expected exit");
			}
			catch (ExitUtil.ExitException)
			{
				NUnit.Framework.Assert.IsTrue(stdout.ToByteArray().Length == 0);
				string strErr = new string(stderr.ToByteArray(), Utf8);
				NUnit.Framework.Assert.IsTrue(strErr.Contains("unrecognized option"));
			}
		}

		/// <summary>
		/// Asserts that the specified file is a jar file with a manifest containing a
		/// non-empty classpath attribute.
		/// </summary>
		/// <param name="file">File to check</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private static void AssertJar(FilePath file)
		{
			JarFile jarFile = null;
			try
			{
				jarFile = new JarFile(file);
				Manifest manifest = jarFile.GetManifest();
				NUnit.Framework.Assert.IsNotNull(manifest);
				Attributes mainAttributes = manifest.GetMainAttributes();
				NUnit.Framework.Assert.IsNotNull(mainAttributes);
				NUnit.Framework.Assert.IsTrue(mainAttributes.Contains(Attributes.Name.ClassPath));
				string classPathAttr = mainAttributes.GetValue(Attributes.Name.ClassPath);
				NUnit.Framework.Assert.IsNotNull(classPathAttr);
				NUnit.Framework.Assert.IsFalse(classPathAttr.IsEmpty());
			}
			finally
			{
				// It's too bad JarFile doesn't implement Closeable.
				if (jarFile != null)
				{
					try
					{
						jarFile.Close();
					}
					catch (IOException e)
					{
						Log.Warn("exception closing jarFile: " + jarFile, e);
					}
				}
			}
		}
	}
}
