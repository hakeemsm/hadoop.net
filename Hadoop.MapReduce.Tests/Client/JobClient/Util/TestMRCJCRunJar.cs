using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A test to rest the RunJar class.</summary>
	public class TestMRCJCRunJar
	{
		private static string TestRootDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToString();

		private const string TestJarName = "testjar.jar";

		private const string ClassName = "Hello.class";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunjar()
		{
			FilePath outFile = new FilePath(TestRootDir, "out");
			// delete if output file already exists.
			if (outFile.Exists())
			{
				outFile.Delete();
			}
			FilePath makeTestJar = MakeTestJar();
			string[] args = new string[3];
			args[0] = makeTestJar.GetAbsolutePath();
			args[1] = "org.apache.hadoop.util.Hello";
			args[2] = outFile.ToString();
			RunJar.Main(args);
			NUnit.Framework.Assert.IsTrue("RunJar failed", outFile.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath MakeTestJar()
		{
			FilePath jarFile = new FilePath(TestRootDir, TestJarName);
			JarOutputStream jstream = new JarOutputStream(new FileOutputStream(jarFile));
			InputStream entryInputStream = this.GetType().GetResourceAsStream(ClassName);
			ZipEntry entry = new ZipEntry("org/apache/hadoop/util/" + ClassName);
			jstream.PutNextEntry(entry);
			BufferedInputStream bufInputStream = new BufferedInputStream(entryInputStream, 2048
				);
			int count;
			byte[] data = new byte[2048];
			while ((count = bufInputStream.Read(data, 0, 2048)) != -1)
			{
				jstream.Write(data, 0, count);
			}
			jstream.CloseEntry();
			jstream.Close();
			return jarFile;
		}
	}
}
