using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFileInputFormatPathFilter : TestCase
	{
		public class DummyFileInputFormat : FileInputFormat
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader GetRecordReader(InputSplit split, JobConf job, Reporter
				 reporter)
			{
				return null;
			}
		}

		private static FileSystem localFs = null;

		static TestFileInputFormatPathFilter()
		{
			try
			{
				localFs = FileSystem.GetLocal(new JobConf());
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestFileInputFormatPathFilter");

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			TearDown();
			localFs.Mkdirs(workDir);
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			if (localFs.Exists(workDir))
			{
				localFs.Delete(workDir, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path CreateFile(string fileName)
		{
			Path file = new Path(workDir, fileName);
			TextWriter writer = new OutputStreamWriter(localFs.Create(file));
			writer.Write(string.Empty);
			writer.Close();
			return localFs.MakeQualified(file);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ICollection<Path> CreateFiles()
		{
			ICollection<Path> files = new HashSet<Path>();
			files.AddItem(CreateFile("a"));
			files.AddItem(CreateFile("b"));
			files.AddItem(CreateFile("aa"));
			files.AddItem(CreateFile("bb"));
			files.AddItem(CreateFile("_hello"));
			files.AddItem(CreateFile(".hello"));
			return files;
		}

		public class TestPathFilter : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				string name = path.GetName();
				return name.Equals("TestFileInputFormatPathFilter") || name.Length == 1;
			}
		}

		/// <exception cref="System.Exception"/>
		private void _testInputFiles(bool withFilter, bool withGlob)
		{
			ICollection<Path> createdFiles = CreateFiles();
			JobConf conf = new JobConf();
			Path inputDir = (withGlob) ? new Path(workDir, "a*") : workDir;
			FileInputFormat.SetInputPaths(conf, inputDir);
			conf.SetInputFormat(typeof(TestFileInputFormatPathFilter.DummyFileInputFormat));
			if (withFilter)
			{
				FileInputFormat.SetInputPathFilter(conf, typeof(TestFileInputFormatPathFilter.TestPathFilter
					));
			}
			TestFileInputFormatPathFilter.DummyFileInputFormat inputFormat = (TestFileInputFormatPathFilter.DummyFileInputFormat
				)conf.GetInputFormat();
			ICollection<Path> computedFiles = new HashSet<Path>();
			foreach (FileStatus file in inputFormat.ListStatus(conf))
			{
				computedFiles.AddItem(file.GetPath());
			}
			createdFiles.Remove(localFs.MakeQualified(new Path(workDir, "_hello")));
			createdFiles.Remove(localFs.MakeQualified(new Path(workDir, ".hello")));
			if (withFilter)
			{
				createdFiles.Remove(localFs.MakeQualified(new Path(workDir, "aa")));
				createdFiles.Remove(localFs.MakeQualified(new Path(workDir, "bb")));
			}
			if (withGlob)
			{
				createdFiles.Remove(localFs.MakeQualified(new Path(workDir, "b")));
				createdFiles.Remove(localFs.MakeQualified(new Path(workDir, "bb")));
			}
			NUnit.Framework.Assert.AreEqual(createdFiles, computedFiles);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithoutPathFilterWithoutGlob()
		{
			_testInputFiles(false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithoutPathFilterWithGlob()
		{
			_testInputFiles(false, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithPathFilterWithoutGlob()
		{
			_testInputFiles(true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithPathFilterWithGlob()
		{
			_testInputFiles(true, true);
		}
	}
}
