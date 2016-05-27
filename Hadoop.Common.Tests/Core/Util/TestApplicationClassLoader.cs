using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	public class TestApplicationClassLoader
	{
		private static FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "appclassloader");

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FileUtil.FullyDelete(testDir);
			testDir.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConstructUrlsFromClasspath()
		{
			FilePath file = new FilePath(testDir, "file");
			NUnit.Framework.Assert.IsTrue("Create file", file.CreateNewFile());
			FilePath dir = new FilePath(testDir, "dir");
			NUnit.Framework.Assert.IsTrue("Make dir", dir.Mkdir());
			FilePath jarsDir = new FilePath(testDir, "jarsdir");
			NUnit.Framework.Assert.IsTrue("Make jarsDir", jarsDir.Mkdir());
			FilePath nonJarFile = new FilePath(jarsDir, "nonjar");
			NUnit.Framework.Assert.IsTrue("Create non-jar file", nonJarFile.CreateNewFile());
			FilePath jarFile = new FilePath(jarsDir, "a.jar");
			NUnit.Framework.Assert.IsTrue("Create jar file", jarFile.CreateNewFile());
			FilePath nofile = new FilePath(testDir, "nofile");
			// don't create nofile
			StringBuilder cp = new StringBuilder();
			cp.Append(file.GetAbsolutePath()).Append(FilePath.pathSeparator).Append(dir.GetAbsolutePath
				()).Append(FilePath.pathSeparator).Append(jarsDir.GetAbsolutePath() + "/*").Append
				(FilePath.pathSeparator).Append(nofile.GetAbsolutePath()).Append(FilePath.pathSeparator
				).Append(nofile.GetAbsolutePath() + "/*").Append(FilePath.pathSeparator);
			Uri[] urls = ApplicationClassLoader.ConstructUrlsFromClasspath(cp.ToString());
			NUnit.Framework.Assert.AreEqual(3, urls.Length);
			NUnit.Framework.Assert.AreEqual(file.ToURI().ToURL(), urls[0]);
			NUnit.Framework.Assert.AreEqual(dir.ToURI().ToURL(), urls[1]);
			NUnit.Framework.Assert.AreEqual(jarFile.ToURI().ToURL(), urls[2]);
		}

		// nofile should be ignored
		[NUnit.Framework.Test]
		public virtual void TestIsSystemClass()
		{
			TestIsSystemClassInternal(string.Empty);
		}

		[NUnit.Framework.Test]
		public virtual void TestIsSystemNestedClass()
		{
			TestIsSystemClassInternal("$Klass");
		}

		private void TestIsSystemClassInternal(string nestedClass)
		{
			NUnit.Framework.Assert.IsFalse(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, null));
			NUnit.Framework.Assert.IsTrue(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, Classes("org.example.Foo")));
			NUnit.Framework.Assert.IsTrue(ApplicationClassLoader.IsSystemClass("/org.example.Foo"
				 + nestedClass, Classes("org.example.Foo")));
			NUnit.Framework.Assert.IsTrue(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, Classes("org.example.")));
			NUnit.Framework.Assert.IsTrue(ApplicationClassLoader.IsSystemClass("net.example.Foo"
				 + nestedClass, Classes("org.example.,net.example.")));
			NUnit.Framework.Assert.IsFalse(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, Classes("-org.example.Foo,org.example.")));
			NUnit.Framework.Assert.IsTrue(ApplicationClassLoader.IsSystemClass("org.example.Bar"
				 + nestedClass, Classes("-org.example.Foo.,org.example.")));
			NUnit.Framework.Assert.IsFalse(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, Classes("org.example.,-org.example.Foo")));
			NUnit.Framework.Assert.IsFalse(ApplicationClassLoader.IsSystemClass("org.example.Foo"
				 + nestedClass, Classes("org.example.Foo,-org.example.Foo")));
		}

		private IList<string> Classes(string classes)
		{
			return Lists.NewArrayList(Splitter.On(',').Split(classes));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetResource()
		{
			Uri testJar = MakeTestJar().ToURI().ToURL();
			ClassLoader currentClassLoader = GetType().GetClassLoader();
			ClassLoader appClassloader = new ApplicationClassLoader(new Uri[] { testJar }, currentClassLoader
				, null);
			NUnit.Framework.Assert.IsNull("Resource should be null for current classloader", 
				currentClassLoader.GetResourceAsStream("resource.txt"));
			InputStream @in = appClassloader.GetResourceAsStream("resource.txt");
			NUnit.Framework.Assert.IsNotNull("Resource should not be null for app classloader"
				, @in);
			NUnit.Framework.Assert.AreEqual("hello", IOUtils.ToString(@in));
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath MakeTestJar()
		{
			FilePath jarFile = new FilePath(testDir, "test.jar");
			JarOutputStream @out = new JarOutputStream(new FileOutputStream(jarFile));
			ZipEntry entry = new ZipEntry("resource.txt");
			@out.PutNextEntry(entry);
			@out.Write(Sharpen.Runtime.GetBytesForString("hello"));
			@out.CloseEntry();
			@out.Close();
			return jarFile;
		}
	}
}
