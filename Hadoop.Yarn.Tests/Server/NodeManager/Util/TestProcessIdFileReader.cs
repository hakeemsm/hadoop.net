using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class TestProcessIdFileReader
	{
		public virtual void TestNullPath()
		{
			string pid = null;
			try
			{
				pid = ProcessIdFileReader.GetProcessId(null);
				NUnit.Framework.Assert.Fail("Expected an error to be thrown for null path");
			}
			catch (Exception)
			{
			}
			// expected
			System.Diagnostics.Debug.Assert((pid == null));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSimpleGet()
		{
			string rootDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp")).GetAbsolutePath
				();
			FilePath testFile = null;
			string expectedProcessId = Shell.Windows ? "container_1353742680940_0002_01_000001"
				 : "56789";
			try
			{
				testFile = new FilePath(rootDir, "temp.txt");
				PrintWriter fileWriter = new PrintWriter(testFile);
				fileWriter.WriteLine(expectedProcessId);
				fileWriter.Close();
				string processId = null;
				processId = ProcessIdFileReader.GetProcessId(new Path(rootDir + Path.Separator + 
					"temp.txt"));
				NUnit.Framework.Assert.AreEqual(expectedProcessId, processId);
			}
			finally
			{
				if (testFile != null && testFile.Exists())
				{
					testFile.Delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestComplexGet()
		{
			string rootDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp")).GetAbsolutePath
				();
			FilePath testFile = null;
			string processIdInFile = Shell.Windows ? " container_1353742680940_0002_01_000001 "
				 : " 23 ";
			string expectedProcessId = processIdInFile.Trim();
			try
			{
				testFile = new FilePath(rootDir, "temp.txt");
				PrintWriter fileWriter = new PrintWriter(testFile);
				fileWriter.WriteLine("   ");
				fileWriter.WriteLine(string.Empty);
				fileWriter.WriteLine("abc");
				fileWriter.WriteLine("-123");
				fileWriter.WriteLine("-123 ");
				fileWriter.WriteLine(processIdInFile);
				fileWriter.WriteLine("6236");
				fileWriter.Close();
				string processId = null;
				processId = ProcessIdFileReader.GetProcessId(new Path(rootDir + Path.Separator + 
					"temp.txt"));
				NUnit.Framework.Assert.AreEqual(expectedProcessId, processId);
			}
			finally
			{
				if (testFile != null && testFile.Exists())
				{
					testFile.Delete();
				}
			}
		}
	}
}
