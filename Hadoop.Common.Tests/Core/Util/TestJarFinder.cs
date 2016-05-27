using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	public class TestJarFinder
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJar()
		{
			//picking a class that is for sure in a JAR in the classpath
			string jar = JarFinder.GetJar(typeof(LogFactory));
			NUnit.Framework.Assert.IsTrue(new FilePath(jar).Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Delete(FilePath file)
		{
			if (file.GetAbsolutePath().Length < 5)
			{
				throw new ArgumentException(MessageFormat.Format("Path [{0}] is too short, not deleting"
					, file.GetAbsolutePath()));
			}
			if (file.Exists())
			{
				if (file.IsDirectory())
				{
					FilePath[] children = file.ListFiles();
					if (children != null)
					{
						foreach (FilePath child in children)
						{
							Delete(child);
						}
					}
				}
				if (!file.Delete())
				{
					throw new RuntimeException(MessageFormat.Format("Could not delete path [{0}]", file
						.GetAbsolutePath()));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExpandedClasspath()
		{
			//picking a class that is for sure in a directory in the classpath
			//in this case the JAR is created on the fly
			string jar = JarFinder.GetJar(typeof(TestJarFinder));
			NUnit.Framework.Assert.IsTrue(new FilePath(jar).Exists());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExistingManifest()
		{
			FilePath dir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"
				), typeof(TestJarFinder).FullName + "-testExistingManifest");
			Delete(dir);
			dir.Mkdirs();
			FilePath metaInfDir = new FilePath(dir, "META-INF");
			metaInfDir.Mkdirs();
			FilePath manifestFile = new FilePath(metaInfDir, "MANIFEST.MF");
			Manifest manifest = new Manifest();
			OutputStream os = new FileOutputStream(manifestFile);
			manifest.Write(os);
			os.Close();
			FilePath propsFile = new FilePath(dir, "props.properties");
			TextWriter writer = new FileWriter(propsFile);
			new Properties().Store(writer, string.Empty);
			writer.Close();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JarOutputStream zos = new JarOutputStream(baos);
			JarFinder.JarDir(dir, string.Empty, zos);
			JarInputStream jis = new JarInputStream(new ByteArrayInputStream(baos.ToByteArray
				()));
			NUnit.Framework.Assert.IsNotNull(jis.GetManifest());
			jis.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoManifest()
		{
			FilePath dir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"
				), typeof(TestJarFinder).FullName + "-testNoManifest");
			Delete(dir);
			dir.Mkdirs();
			FilePath propsFile = new FilePath(dir, "props.properties");
			TextWriter writer = new FileWriter(propsFile);
			new Properties().Store(writer, string.Empty);
			writer.Close();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JarOutputStream zos = new JarOutputStream(baos);
			JarFinder.JarDir(dir, string.Empty, zos);
			JarInputStream jis = new JarInputStream(new ByteArrayInputStream(baos.ToByteArray
				()));
			NUnit.Framework.Assert.IsNotNull(jis.GetManifest());
			jis.Close();
		}
	}
}
