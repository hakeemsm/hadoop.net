using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Jobcontrol
{
	/// <summary>Utility methods used in various Job Control unit tests.</summary>
	public class JobControlTestUtils
	{
		private static Random rand = new Random();

		private static NumberFormat idFormat = NumberFormat.GetInstance();

		static JobControlTestUtils()
		{
			idFormat.SetMinimumIntegerDigits(4);
			idFormat.SetGroupingUsed(false);
		}

		/// <summary>Cleans the data from the passed Path in the passed FileSystem.</summary>
		/// <param name="fs">FileSystem to delete data from.</param>
		/// <param name="dirPath">Path to be deleted.</param>
		/// <exception cref="System.IO.IOException">If an error occurs cleaning the data.</exception>
		internal static void CleanData(FileSystem fs, Path dirPath)
		{
			fs.Delete(dirPath, true);
		}

		/// <summary>Generates a string of random digits.</summary>
		/// <returns>A random string.</returns>
		private static string GenerateRandomWord()
		{
			return idFormat.Format(rand.NextLong());
		}

		/// <summary>Generates a line of random text.</summary>
		/// <returns>A line of random text.</returns>
		private static string GenerateRandomLine()
		{
			long r = rand.NextLong() % 7;
			long n = r + 20;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < n; i++)
			{
				sb.Append(GenerateRandomWord()).Append(" ");
			}
			sb.Append("\n");
			return sb.ToString();
		}

		/// <summary>Generates data that can be used for Job Control tests.</summary>
		/// <param name="fs">FileSystem to create data in.</param>
		/// <param name="dirPath">Path to create the data in.</param>
		/// <exception cref="System.IO.IOException">If an error occurs creating the data.</exception>
		internal static void GenerateData(FileSystem fs, Path dirPath)
		{
			FSDataOutputStream @out = fs.Create(new Path(dirPath, "data.txt"));
			for (int i = 0; i < 10000; i++)
			{
				string line = GenerateRandomLine();
				@out.Write(Sharpen.Runtime.GetBytesForString(line, "UTF-8"));
			}
			@out.Close();
		}

		/// <summary>Creates a simple copy job.</summary>
		/// <param name="indirs">List of input directories.</param>
		/// <param name="outdir">Output directory.</param>
		/// <returns>JobConf initialised for a simple copy job.</returns>
		/// <exception cref="System.Exception">If an error occurs creating job configuration.
		/// 	</exception>
		internal static JobConf CreateCopyJob(IList<Path> indirs, Path outdir)
		{
			Configuration defaults = new Configuration();
			JobConf theJob = new JobConf(defaults, typeof(TestJobControl));
			theJob.SetJobName("DataMoveJob");
			FileInputFormat.SetInputPaths(theJob, Sharpen.Collections.ToArray(indirs, new Path
				[0]));
			theJob.SetMapperClass(typeof(JobControlTestUtils.DataCopy));
			FileOutputFormat.SetOutputPath(theJob, outdir);
			theJob.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetReducerClass(typeof(JobControlTestUtils.DataCopy));
			theJob.SetNumMapTasks(12);
			theJob.SetNumReduceTasks(4);
			return theJob;
		}

		/// <summary>Simple Mapper and Reducer implementation which copies data it reads in.</summary>
		public class DataCopy : MapReduceBase, Mapper<LongWritable, Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text>, Reducer<Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Org.Apache.Hadoop.IO.Text value, OutputCollector
				<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> output, Reporter reporter
				)
			{
				output.Collect(new Org.Apache.Hadoop.IO.Text(key.ToString()), value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerator<Org.Apache.Hadoop.IO.Text
				> values, OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> 
				output, Reporter reporter)
			{
				Org.Apache.Hadoop.IO.Text dumbKey = new Org.Apache.Hadoop.IO.Text(string.Empty);
				while (values.HasNext())
				{
					Org.Apache.Hadoop.IO.Text data = values.Next();
					output.Collect(dumbKey, data);
				}
			}
		}
	}
}
