using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRCJCJobConf
	{
		private const string JarRelativePath = "build/test/mapred/testjar/testjob.jar";

		private const string Classname = "testjar.ClassWordCount";

		private static string TestDirWithSpecialChars = Runtime.GetProperty("test.build.data"
			, "/tmp") + FilePath.separator + "test jobconf with + and spaces";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFindContainingJar()
		{
			TestJarAtPath(JarRelativePath);
		}

		/// <summary>
		/// Test that findContainingJar works correctly even if the
		/// path has a "+" sign or spaces in it
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFindContainingJarWithPlus()
		{
			new FilePath(TestDirWithSpecialChars).Mkdirs();
			Configuration conf = new Configuration();
			FileSystem localfs = FileSystem.GetLocal(conf);
			FileUtil.Copy(localfs, new Path(JarRelativePath), localfs, new Path(TestDirWithSpecialChars
				, "test.jar"), false, true, conf);
			TestJarAtPath(TestDirWithSpecialChars + FilePath.separator + "test.jar");
		}

		/// <summary>
		/// Given a path with a jar, make a classloader with that jar on the
		/// classpath, and check that findContainingJar can correctly
		/// identify the path of the jar.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void TestJarAtPath(string path)
		{
			FilePath jar = new FilePath(path).GetAbsoluteFile();
			NUnit.Framework.Assert.IsTrue(jar.Exists());
			Uri[] urls = new Uri[] { jar.ToURI().ToURL() };
			ClassLoader cl = new URLClassLoader(urls);
			Type clazz = Sharpen.Runtime.GetType(Classname, true, cl);
			NUnit.Framework.Assert.IsNotNull(clazz);
			string containingJar = ClassUtil.FindContainingJar(clazz);
			NUnit.Framework.Assert.AreEqual(jar.GetAbsolutePath(), containingJar);
		}
	}
}
