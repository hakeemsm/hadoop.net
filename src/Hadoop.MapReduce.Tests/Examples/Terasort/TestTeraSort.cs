using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	public class TestTeraSort : HadoopTestCase
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Examples.Terasort.TestTeraSort
			));

		/// <exception cref="System.IO.IOException"/>
		public TestTeraSort()
			: base(LocalMr, LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			GetFileSystem().Delete(new Path(TestDir), true);
			base.TearDown();
		}

		private static readonly string TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "terasort").GetAbsolutePath();

		private static readonly Path SortInputPath = new Path(TestDir, "sortin");

		private static readonly Path SortOutputPath = new Path(TestDir, "sortout");

		private static readonly Path TeraOutputPath = new Path(TestDir, "validate");

		private const string NumRows = "100";

		// Input/Output paths for sort
		/// <exception cref="System.Exception"/>
		private void RunTeraGen(Configuration conf, Path sortInput)
		{
			string[] genArgs = new string[] { NumRows, sortInput.ToString() };
			// Run TeraGen
			NUnit.Framework.Assert.AreEqual(ToolRunner.Run(conf, new TeraGen(), genArgs), 0);
		}

		/// <exception cref="System.Exception"/>
		private void RunTeraSort(Configuration conf, Path sortInput, Path sortOutput)
		{
			// Setup command-line arguments to 'sort'
			string[] sortArgs = new string[] { sortInput.ToString(), sortOutput.ToString() };
			// Run Sort
			NUnit.Framework.Assert.AreEqual(ToolRunner.Run(conf, new TeraSort(), sortArgs), 0
				);
		}

		/// <exception cref="System.Exception"/>
		private void RunTeraValidator(Configuration job, Path sortOutput, Path valOutput)
		{
			string[] svArgs = new string[] { sortOutput.ToString(), valOutput.ToString() };
			// Run Tera-Validator
			NUnit.Framework.Assert.AreEqual(ToolRunner.Run(job, new TeraValidate(), svArgs), 
				0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTeraSort()
		{
			// Run TeraGen to generate input for 'terasort'
			RunTeraGen(CreateJobConf(), SortInputPath);
			// Run teragen again to check for FAE
			try
			{
				RunTeraGen(CreateJobConf(), SortInputPath);
				Fail("Teragen output overwritten!");
			}
			catch (FileAlreadyExistsException fae)
			{
				Log.Info("Expected exception: ", fae);
			}
			// Run terasort
			RunTeraSort(CreateJobConf(), SortInputPath, SortOutputPath);
			// Run terasort again to check for FAE
			try
			{
				RunTeraSort(CreateJobConf(), SortInputPath, SortOutputPath);
				Fail("Terasort output overwritten!");
			}
			catch (FileAlreadyExistsException fae)
			{
				Log.Info("Expected exception: ", fae);
			}
			// Run tera-validator to check if sort worked correctly
			RunTeraValidator(CreateJobConf(), SortOutputPath, TeraOutputPath);
		}
	}
}
