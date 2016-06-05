using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	public class TestRunJar : TestCase
	{
		private FilePath TestRootDir;

		private const string TestJarName = "test-runjar.jar";

		private const string TestJar2Name = "test-runjar2.jar";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		protected override void SetUp()
		{
			TestRootDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp"), GetType
				().Name);
			if (!TestRootDir.Exists())
			{
				TestRootDir.Mkdirs();
			}
			MakeTestJar();
		}

		[NUnit.Framework.TearDown]
		protected override void TearDown()
		{
			FileUtil.FullyDelete(TestRootDir);
		}

		/// <summary>
		/// Construct a jar with two files in it in our
		/// test dir.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void MakeTestJar()
		{
			FilePath jarFile = new FilePath(TestRootDir, TestJarName);
			JarOutputStream jstream = new JarOutputStream(new FileOutputStream(jarFile));
			jstream.PutNextEntry(new ZipEntry("foobar.txt"));
			jstream.CloseEntry();
			jstream.PutNextEntry(new ZipEntry("foobaz.txt"));
			jstream.CloseEntry();
			jstream.Close();
		}

		/// <summary>Test default unjarring behavior - unpack everything</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUnJar()
		{
			FilePath unjarDir = new FilePath(TestRootDir, "unjar-all");
			NUnit.Framework.Assert.IsFalse("unjar dir shouldn't exist at test start", new FilePath
				(unjarDir, "foobar.txt").Exists());
			// Unjar everything
			RunJar.UnJar(new FilePath(TestRootDir, TestJarName), unjarDir);
			Assert.True("foobar unpacked", new FilePath(unjarDir, "foobar.txt"
				).Exists());
			Assert.True("foobaz unpacked", new FilePath(unjarDir, "foobaz.txt"
				).Exists());
		}

		/// <summary>Test unjarring a specific regex</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUnJarWithPattern()
		{
			FilePath unjarDir = new FilePath(TestRootDir, "unjar-pattern");
			NUnit.Framework.Assert.IsFalse("unjar dir shouldn't exist at test start", new FilePath
				(unjarDir, "foobar.txt").Exists());
			// Unjar only a regex
			RunJar.UnJar(new FilePath(TestRootDir, TestJarName), unjarDir, Sharpen.Pattern.Compile
				(".*baz.*"));
			NUnit.Framework.Assert.IsFalse("foobar not unpacked", new FilePath(unjarDir, "foobar.txt"
				).Exists());
			Assert.True("foobaz unpacked", new FilePath(unjarDir, "foobaz.txt"
				).Exists());
		}

		/// <summary>
		/// Tests the client classloader to verify the main class and its dependent
		/// class are loaded correctly by the application classloader, and others are
		/// loaded by the system classloader.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestClientClassLoader()
		{
			RunJar runJar = Org.Mockito.Mockito.Spy(new RunJar());
			// enable the client classloader
			Org.Mockito.Mockito.When(runJar.UseClientClassLoader()).ThenReturn(true);
			// set the system classes and blacklist the test main class and the test
			// third class so they can be loaded by the application classloader
			string mainCls = typeof(ClassLoaderCheckMain).FullName;
			string thirdCls = typeof(ClassLoaderCheckThird).FullName;
			string systemClasses = "-" + mainCls + "," + "-" + thirdCls + "," + ApplicationClassLoader
				.SystemClassesDefault;
			Org.Mockito.Mockito.When(runJar.GetSystemClasses()).ThenReturn(systemClasses);
			// create the test jar
			FilePath testJar = MakeClassLoaderTestJar(mainCls, thirdCls);
			// form the args
			string[] args = new string[3];
			args[0] = testJar.GetAbsolutePath();
			args[1] = mainCls;
			// run RunJar
			runJar.Run(args);
		}

		// it should not throw an exception
		/// <exception cref="System.IO.IOException"/>
		private FilePath MakeClassLoaderTestJar(params string[] clsNames)
		{
			FilePath jarFile = new FilePath(TestRootDir, TestJar2Name);
			JarOutputStream jstream = new JarOutputStream(new FileOutputStream(jarFile));
			foreach (string clsName in clsNames)
			{
				string name = clsName.Replace('.', '/') + ".class";
				InputStream entryInputStream = this.GetType().GetResourceAsStream("/" + name);
				ZipEntry entry = new ZipEntry(name);
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
			}
			jstream.Close();
			return jarFile;
		}
	}
}
