using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRIntermediateDataEncryption
	{
		private static readonly Path InputDir = new Path("/test/input");

		private static readonly Path Output = new Path("/test/output");

		// Where MR job's input will reside.
		// Where output goes.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleReducer()
		{
			DoEncryptionTest(3, 1, 2, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUberMode()
		{
			DoEncryptionTest(3, 1, 2, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleMapsPerNode()
		{
			DoEncryptionTest(8, 1, 2, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleReducers()
		{
			DoEncryptionTest(2, 4, 2, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoEncryptionTest(int numMappers, int numReducers, int numNodes
			, bool isUber)
		{
			DoEncryptionTest(numMappers, numReducers, numNodes, 1000, isUber);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoEncryptionTest(int numMappers, int numReducers, int numNodes
			, int numLines, bool isUber)
		{
			MiniDFSCluster dfsCluster = null;
			MiniMRClientCluster mrCluster = null;
			FileSystem fileSystem = null;
			try
			{
				Configuration conf = new Configuration();
				// Start the mini-MR and mini-DFS clusters
				dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numNodes).Build();
				fileSystem = dfsCluster.GetFileSystem();
				mrCluster = MiniMRClientClusterFactory.Create(this.GetType(), numNodes, conf);
				// Generate input.
				CreateInput(fileSystem, numMappers, numLines);
				// Run the test.
				RunMergeTest(new JobConf(mrCluster.GetConfig()), fileSystem, numMappers, numReducers
					, numLines, isUber);
			}
			finally
			{
				if (dfsCluster != null)
				{
					dfsCluster.Shutdown();
				}
				if (mrCluster != null)
				{
					mrCluster.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void CreateInput(FileSystem fs, int numMappers, int numLines)
		{
			fs.Delete(InputDir, true);
			for (int i = 0; i < numMappers; i++)
			{
				OutputStream os = fs.Create(new Path(InputDir, "input_" + i + ".txt"));
				TextWriter writer = new OutputStreamWriter(os);
				for (int j = 0; j < numLines; j++)
				{
					// Create sorted key, value pairs.
					int k = j + 1;
					string formattedNumber = string.Format("%09d", k);
					writer.Write(formattedNumber + " " + formattedNumber + "\n");
				}
				writer.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		private void RunMergeTest(JobConf job, FileSystem fileSystem, int numMappers, int
			 numReducers, int numLines, bool isUber)
		{
			fileSystem.Delete(Output, true);
			job.SetJobName("Test");
			JobClient client = new JobClient(job);
			RunningJob submittedJob = null;
			FileInputFormat.SetInputPaths(job, InputDir);
			FileOutputFormat.SetOutputPath(job, Output);
			job.Set("mapreduce.output.textoutputformat.separator", " ");
			job.SetInputFormat(typeof(TextInputFormat));
			job.SetMapOutputKeyClass(typeof(Text));
			job.SetMapOutputValueClass(typeof(Text));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetMapperClass(typeof(TestMRIntermediateDataEncryption.MyMapper));
			job.SetPartitionerClass(typeof(TestMRIntermediateDataEncryption.MyPartitioner));
			job.SetOutputFormat(typeof(TextOutputFormat));
			job.SetNumReduceTasks(numReducers);
			job.SetInt("mapreduce.map.maxattempts", 1);
			job.SetInt("mapreduce.reduce.maxattempts", 1);
			job.SetInt("mapred.test.num_lines", numLines);
			if (isUber)
			{
				job.SetBoolean("mapreduce.job.ubertask.enable", true);
			}
			job.SetBoolean(MRJobConfig.MrEncryptedIntermediateData, true);
			try
			{
				submittedJob = client.SubmitJob(job);
				try
				{
					if (!client.MonitorAndPrintJob(job, submittedJob))
					{
						throw new IOException("Job failed!");
					}
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
				}
			}
			catch (IOException ioe)
			{
				System.Console.Error.WriteLine("Job failed with: " + ioe);
			}
			finally
			{
				VerifyOutput(submittedJob, fileSystem, numMappers, numLines);
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyOutput(RunningJob submittedJob, FileSystem fileSystem, int numMappers
			, int numLines)
		{
			FSDataInputStream dis = null;
			long numValidRecords = 0;
			long numInvalidRecords = 0;
			string prevKeyValue = "000000000";
			Path[] fileList = FileUtil.Stat2Paths(fileSystem.ListStatus(Output, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			foreach (Path outFile in fileList)
			{
				try
				{
					dis = fileSystem.Open(outFile);
					string record;
					while ((record = dis.ReadLine()) != null)
					{
						// Split the line into key and value.
						int blankPos = record.IndexOf(" ");
						string keyString = Sharpen.Runtime.Substring(record, 0, blankPos);
						string valueString = Sharpen.Runtime.Substring(record, blankPos + 1);
						// Check for sorted output and correctness of record.
						if (string.CompareOrdinal(keyString, prevKeyValue) >= 0 && keyString.Equals(valueString
							))
						{
							prevKeyValue = keyString;
							numValidRecords++;
						}
						else
						{
							numInvalidRecords++;
						}
					}
				}
				finally
				{
					if (dis != null)
					{
						dis.Close();
						dis = null;
					}
				}
			}
			// Make sure we got all input records in the output in sorted order.
			NUnit.Framework.Assert.AreEqual((long)(numMappers * numLines), numValidRecords);
			// Make sure there is no extraneous invalid record.
			NUnit.Framework.Assert.AreEqual(0, numInvalidRecords);
		}

		/// <summary>
		/// A mapper implementation that assumes that key text contains valid integers
		/// in displayable form.
		/// </summary>
		public class MyMapper : MapReduceBase, Mapper<LongWritable, Text, Text, Text>
		{
			private Text keyText;

			private Text valueText;

			public MyMapper()
			{
				keyText = new Text();
				valueText = new Text();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, Text>
				 output, Reporter reporter)
			{
				string record = value.ToString();
				int blankPos = record.IndexOf(" ");
				keyText.Set(Sharpen.Runtime.Substring(record, 0, blankPos));
				valueText.Set(Sharpen.Runtime.Substring(record, blankPos + 1));
				output.Collect(keyText, valueText);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}
		}

		/// <summary>
		/// Partitioner implementation to make sure that output is in total sorted
		/// order.
		/// </summary>
		/// <remarks>
		/// Partitioner implementation to make sure that output is in total sorted
		/// order.  We basically route key ranges to different reducers such that
		/// key values monotonically increase with the partition number.  For example,
		/// in this test, the keys are numbers from 1 to 1000 in the form "000000001"
		/// to "000001000" in each input file.  The keys "000000001" to "000000250" are
		/// routed to partition 0, "000000251" to "000000500" are routed to partition 1
		/// and so on since we have 4 reducers.
		/// </remarks>
		internal class MyPartitioner : Partitioner<Text, Text>
		{
			private JobConf job;

			public MyPartitioner()
			{
			}

			public virtual void Configure(JobConf job)
			{
				this.job = job;
			}

			public virtual int GetPartition(Text key, Text value, int numPartitions)
			{
				int keyValue = 0;
				try
				{
					keyValue = System.Convert.ToInt32(key.ToString());
				}
				catch (FormatException)
				{
					keyValue = 0;
				}
				int partitionNumber = (numPartitions * (Math.Max(0, keyValue - 1))) / job.GetInt(
					"mapred.test.num_lines", 10000);
				return partitionNumber;
			}
		}
	}
}
