using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMerge : TestCase
	{
		private const int NumHadoopDataNodes = 2;

		private const int NumMappers = 10;

		private const int NumReducers = 4;

		private const int NumLines = 1000;

		private static readonly Path InputDir = new Path("/testplugin/input");

		private static readonly Path Output = new Path("/testplugin/output");

		// Number of input files is same as the number of mappers.
		// Number of reducers.
		// Number of lines per input file.
		// Where MR job's input will reside.
		// Where output goes.
		/// <exception cref="System.Exception"/>
		public virtual void TestMerge()
		{
			MiniDFSCluster dfsCluster = null;
			MiniMRClientCluster mrCluster = null;
			FileSystem fileSystem = null;
			try
			{
				Configuration conf = new Configuration();
				// Start the mini-MR and mini-DFS clusters
				dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumHadoopDataNodes).Build
					();
				fileSystem = dfsCluster.GetFileSystem();
				mrCluster = MiniMRClientClusterFactory.Create(this.GetType(), NumHadoopDataNodes, 
					conf);
				// Generate input.
				CreateInput(fileSystem);
				// Run the test.
				RunMergeTest(new JobConf(mrCluster.GetConfig()), fileSystem);
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
		private void CreateInput(FileSystem fs)
		{
			fs.Delete(InputDir, true);
			for (int i = 0; i < NumMappers; i++)
			{
				OutputStream os = fs.Create(new Path(InputDir, "input_" + i + ".txt"));
				TextWriter writer = new OutputStreamWriter(os);
				for (int j = 0; j < NumLines; j++)
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
		private void RunMergeTest(JobConf job, FileSystem fileSystem)
		{
			// Delete any existing output.
			fileSystem.Delete(Output, true);
			job.SetJobName("MergeTest");
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
			job.SetMapperClass(typeof(TestMerge.MyMapper));
			job.SetPartitionerClass(typeof(TestMerge.MyPartitioner));
			job.SetOutputFormat(typeof(TextOutputFormat));
			job.SetNumReduceTasks(NumReducers);
			job.Set(JobContext.MapOutputCollectorClassAttr, typeof(TestMerge.MapOutputCopier)
				.FullName);
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
				VerifyOutput(submittedJob, fileSystem);
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyOutput(RunningJob submittedJob, FileSystem fileSystem)
		{
			FSDataInputStream dis = null;
			long numValidRecords = 0;
			long numInvalidRecords = 0;
			long numMappersLaunched = NumMappers;
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
			NUnit.Framework.Assert.AreEqual((long)(NumMappers * NumLines), numValidRecords);
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
			public MyPartitioner()
			{
			}

			public virtual void Configure(JobConf job)
			{
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
				int partitionNumber = (numPartitions * (Math.Max(0, keyValue - 1))) / NumLines;
				return partitionNumber;
			}
		}

		/// <summary>Implementation of map output copier(that avoids sorting) on the map side.
		/// 	</summary>
		/// <remarks>
		/// Implementation of map output copier(that avoids sorting) on the map side.
		/// It maintains keys in the input order within each partition created for
		/// reducers.
		/// </remarks>
		internal class MapOutputCopier<K, V> : MapOutputCollector<K, V>
		{
			private const int BufSize = 128 * 1024;

			private MapTask mapTask;

			private JobConf jobConf;

			private Task.TaskReporter reporter;

			private int numberOfPartitions;

			private Type keyClass;

			private Type valueClass;

			private TestMerge.KeyValueWriter<K, V>[] recordWriters;

			private ByteArrayOutputStream[] outStreams;

			public MapOutputCopier()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			public override void Init(MapOutputCollector.Context context)
			{
				this.mapTask = context.GetMapTask();
				this.jobConf = context.GetJobConf();
				this.reporter = context.GetReporter();
				numberOfPartitions = jobConf.GetNumReduceTasks();
				keyClass = (Type)jobConf.GetMapOutputKeyClass();
				valueClass = (Type)jobConf.GetMapOutputValueClass();
				recordWriters = new TestMerge.KeyValueWriter[numberOfPartitions];
				outStreams = new ByteArrayOutputStream[numberOfPartitions];
				// Create output streams for partitions.
				for (int i = 0; i < numberOfPartitions; i++)
				{
					outStreams[i] = new ByteArrayOutputStream();
					recordWriters[i] = new TestMerge.KeyValueWriter<K, V>(jobConf, outStreams[i], keyClass
						, valueClass);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Collect(K key, V value, int partitionNumber)
			{
				lock (this)
				{
					if (partitionNumber >= 0 && partitionNumber < numberOfPartitions)
					{
						recordWriters[partitionNumber].Write(key, value);
					}
					else
					{
						throw new IOException("Invalid partition number: " + partitionNumber);
					}
					reporter.Progress();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close()
			{
				long totalSize = 0;
				for (int i = 0; i < numberOfPartitions; i++)
				{
					recordWriters[i].Close();
					outStreams[i].Close();
					totalSize += outStreams[i].Size();
				}
				MapOutputFile mapOutputFile = mapTask.GetMapOutputFile();
				Path finalOutput = mapOutputFile.GetOutputFileForWrite(totalSize);
				Path indexPath = mapOutputFile.GetOutputIndexFileForWrite(numberOfPartitions * mapTask
					.MapOutputIndexRecordLength);
				// Copy partitions to final map output.
				CopyPartitions(finalOutput, indexPath);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public override void Flush()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			private void CopyPartitions(Path mapOutputPath, Path indexPath)
			{
				FileSystem localFs = FileSystem.GetLocal(jobConf);
				FileSystem rfs = ((LocalFileSystem)localFs).GetRaw();
				FSDataOutputStream rawOutput = rfs.Create(mapOutputPath, true, BufSize);
				SpillRecord spillRecord = new SpillRecord(numberOfPartitions);
				IndexRecord indexRecord = new IndexRecord();
				for (int i = 0; i < numberOfPartitions; i++)
				{
					indexRecord.startOffset = rawOutput.GetPos();
					byte[] buffer = outStreams[i].ToByteArray();
					IFileOutputStream checksumOutput = new IFileOutputStream(rawOutput);
					checksumOutput.Write(buffer);
					// Write checksum.
					checksumOutput.Finish();
					// Write index record
					indexRecord.rawLength = (long)buffer.Length;
					indexRecord.partLength = rawOutput.GetPos() - indexRecord.startOffset;
					spillRecord.PutIndex(indexRecord, i);
					reporter.Progress();
				}
				rawOutput.Close();
				spillRecord.WriteToFile(indexPath, jobConf);
			}
		}

		internal class KeyValueWriter<K, V>
		{
			private Type keyClass;

			private Type valueClass;

			private DataOutputBuffer dataBuffer;

			private Org.Apache.Hadoop.IO.Serializer.Serializer<K> keySerializer;

			private Org.Apache.Hadoop.IO.Serializer.Serializer<V> valueSerializer;

			private DataOutputStream outputStream;

			/// <exception cref="System.IO.IOException"/>
			public KeyValueWriter(Configuration conf, OutputStream output, Type kyClass, Type
				 valClass)
			{
				keyClass = kyClass;
				valueClass = valClass;
				dataBuffer = new DataOutputBuffer();
				SerializationFactory serializationFactory = new SerializationFactory(conf);
				keySerializer = (Org.Apache.Hadoop.IO.Serializer.Serializer<K>)serializationFactory
					.GetSerializer(keyClass);
				keySerializer.Open(dataBuffer);
				valueSerializer = (Org.Apache.Hadoop.IO.Serializer.Serializer<V>)serializationFactory
					.GetSerializer(valueClass);
				valueSerializer.Open(dataBuffer);
				outputStream = new DataOutputStream(output);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(K key, V value)
			{
				if (key.GetType() != keyClass)
				{
					throw new IOException("wrong key class: " + key.GetType() + " is not " + keyClass
						);
				}
				if (value.GetType() != valueClass)
				{
					throw new IOException("wrong value class: " + value.GetType() + " is not " + valueClass
						);
				}
				// Append the 'key'
				keySerializer.Serialize(key);
				int keyLength = dataBuffer.GetLength();
				if (keyLength < 0)
				{
					throw new IOException("Negative key-length not allowed: " + keyLength + " for " +
						 key);
				}
				// Append the 'value'
				valueSerializer.Serialize(value);
				int valueLength = dataBuffer.GetLength() - keyLength;
				if (valueLength < 0)
				{
					throw new IOException("Negative value-length not allowed: " + valueLength + " for "
						 + value);
				}
				// Write the record out
				WritableUtils.WriteVInt(outputStream, keyLength);
				WritableUtils.WriteVInt(outputStream, valueLength);
				outputStream.Write(dataBuffer.GetData(), 0, dataBuffer.GetLength());
				// Reset
				dataBuffer.Reset();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				keySerializer.Close();
				valueSerializer.Close();
				WritableUtils.WriteVInt(outputStream, IFile.EofMarker);
				WritableUtils.WriteVInt(outputStream, IFile.EofMarker);
				outputStream.Close();
			}
		}
	}
}
