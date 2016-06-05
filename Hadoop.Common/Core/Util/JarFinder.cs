using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;

using Jar;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Finds the Jar for a class.</summary>
	/// <remarks>
	/// Finds the Jar for a class. If the class is in a directory in the
	/// classpath, it creates a Jar on the fly with the contents of the directory
	/// and returns the path to that Jar. If a Jar is created, it is created in
	/// the system temporary directory.
	/// </remarks>
	public class JarFinder
	{
		/// <exception cref="System.IO.IOException"/>
		private static void CopyToZipStream(FilePath file, ZipEntry entry, ZipOutputStream
			 zos)
		{
			InputStream @is = new FileInputStream(file);
			try
			{
				zos.PutNextEntry(entry);
				byte[] arr = new byte[4096];
				int read = @is.Read(arr);
				while (read > -1)
				{
					zos.Write(arr, 0, read);
					read = @is.Read(arr);
				}
			}
			finally
			{
				try
				{
					@is.Close();
				}
				finally
				{
					zos.CloseEntry();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void JarDir(FilePath dir, string relativePath, ZipOutputStream zos)
		{
			Preconditions.CheckNotNull(relativePath, "relativePath");
			Preconditions.CheckNotNull(zos, "zos");
			// by JAR spec, if there is a manifest, it must be the first entry in the
			// ZIP.
			FilePath manifestFile = new FilePath(dir, JarFile.ManifestName);
			ZipEntry manifestEntry = new ZipEntry(JarFile.ManifestName);
			if (!manifestFile.Exists())
			{
				zos.PutNextEntry(manifestEntry);
				new Manifest().Write(new BufferedOutputStream(zos));
				zos.CloseEntry();
			}
			else
			{
				CopyToZipStream(manifestFile, manifestEntry, zos);
			}
			zos.CloseEntry();
			ZipDir(dir, relativePath, zos, true);
			zos.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ZipDir(FilePath dir, string relativePath, ZipOutputStream zos
			, bool start)
		{
			string[] dirList = dir.List();
			foreach (string aDirList in dirList)
			{
				FilePath f = new FilePath(dir, aDirList);
				if (!f.IsHidden())
				{
					if (f.IsDirectory())
					{
						if (!start)
						{
							ZipEntry dirEntry = new ZipEntry(relativePath + f.GetName() + "/");
							zos.PutNextEntry(dirEntry);
							zos.CloseEntry();
						}
						string filePath = f.GetPath();
						FilePath file = new FilePath(filePath);
						ZipDir(file, relativePath + f.GetName() + "/", zos, false);
					}
					else
					{
						string path = relativePath + f.GetName();
						if (!path.Equals(JarFile.ManifestName))
						{
							ZipEntry anEntry = new ZipEntry(path);
							CopyToZipStream(f, anEntry, zos);
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateJar(FilePath dir, FilePath jarFile)
		{
			Preconditions.CheckNotNull(dir, "dir");
			Preconditions.CheckNotNull(jarFile, "jarFile");
			FilePath jarDir = jarFile.GetParentFile();
			if (!jarDir.Exists())
			{
				if (!jarDir.Mkdirs())
				{
					throw new IOException(MessageFormat.Format("could not create dir [{0}]", jarDir));
				}
			}
			JarOutputStream zos = new JarOutputStream(new FileOutputStream(jarFile));
			JarDir(dir, string.Empty, zos);
		}

		/// <summary>Returns the full path to the Jar containing the class.</summary>
		/// <remarks>
		/// Returns the full path to the Jar containing the class. It always return a
		/// JAR.
		/// </remarks>
		/// <param name="klass">class.</param>
		/// <returns>path to the Jar containing the class.</returns>
		public static string GetJar(Type klass)
		{
			Preconditions.CheckNotNull(klass, "klass");
			ClassLoader loader = klass.GetClassLoader();
			if (loader != null)
			{
				string class_file = klass.FullName.ReplaceAll("\\.", "/") + ".class";
				try
				{
					for (IEnumeration itr = loader.GetResources(class_file); itr.MoveNext(); )
					{
						Uri url = (Uri)itr.Current;
						string path = url.AbsolutePath;
						if (path.StartsWith("file:"))
						{
							path = Runtime.Substring(path, "file:".Length);
						}
						path = URLDecoder.Decode(path, "UTF-8");
						if ("jar".Equals(url.Scheme))
						{
							path = URLDecoder.Decode(path, "UTF-8");
							return path.ReplaceAll("!.*$", string.Empty);
						}
						else
						{
							if ("file".Equals(url.Scheme))
							{
								string klassName = klass.FullName;
								klassName = klassName.Replace(".", "/") + ".class";
								path = Runtime.Substring(path, 0, path.Length - klassName.Length);
								FilePath baseDir = new FilePath(path);
								FilePath testDir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"
									));
								testDir = testDir.GetAbsoluteFile();
								if (!testDir.Exists())
								{
									testDir.Mkdirs();
								}
								FilePath tempJar = FilePath.CreateTempFile("hadoop-", string.Empty, testDir);
								tempJar = new FilePath(tempJar.GetAbsolutePath() + ".jar");
								CreateJar(baseDir, tempJar);
								return tempJar.GetAbsolutePath();
							}
						}
					}
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
			return null;
		}
	}
}
