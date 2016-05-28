using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// A JUnit test to test the Map-Reduce framework's support for the
	/// "mark-reset" functionality in Reduce Values Iterator
	/// </summary>
	public class TestValueIterReset : TestCase
	{
		private const int NumMaps = 1;

		private const int NumTests = 4;

		private const int NumValues = 40;

		private static Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", 
			"/tmp"));

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		static TestValueIterReset()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.TestValueIterReset
			));

		public class TestMapper : Mapper<LongWritable, Text, IntWritable, IntWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				IntWritable outKey = new IntWritable();
				IntWritable outValue = new IntWritable();
				for (int j = 0; j < NumTests; j++)
				{
					for (int i = 0; i < NumValues; i++)
					{
						outKey.Set(j);
						outValue.Set(i);
						context.Write(outKey, outValue);
					}
				}
			}
		}

		public class TestReducer : Reducer<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(IntWritable key, IEnumerable<IntWritable> values, 
				Reducer.Context context)
			{
				int errors = 0;
				MarkableIterator<IntWritable> mitr = new MarkableIterator<IntWritable>(values.GetEnumerator
					());
				switch (key.Get())
				{
					case 0:
					{
						errors += Test0(key, mitr);
						break;
					}

					case 1:
					{
						errors += Test1(key, mitr);
						break;
					}

					case 2:
					{
						errors += Test2(key, mitr);
						break;
					}

					case 3:
					{
						errors += Test3(key, mitr);
						break;
					}

					default:
					{
						break;
					}
				}
				context.Write(key, new IntWritable(errors));
			}
		}

		/// <summary>Test the most common use case.</summary>
		/// <remarks>
		/// Test the most common use case. Mark before start of the iteration and
		/// reset at the end to go over the entire list
		/// </remarks>
		/// <param name="key"/>
		/// <param name="values"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private static int Test0(IntWritable key, MarkableIterator<IntWritable> values)
		{
			int errors = 0;
			IntWritable i;
			AList<IntWritable> expectedValues = new AList<IntWritable>();
			Log.Info("Executing TEST:0 for Key:" + key.ToString());
			values.Mark();
			Log.Info("TEST:0. Marking");
			while (values.HasNext())
			{
				i = values.Next();
				expectedValues.AddItem(i);
				Log.Info(key + ":" + i);
			}
			values.Reset();
			Log.Info("TEST:0. Reset");
			int count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (i != expectedValues[count])
				{
					Log.Info("TEST:0. Check:1 Expected: " + expectedValues[count] + ", Got: " + i);
					errors++;
					return errors;
				}
				count++;
			}
			Log.Info("TEST:0 Done");
			return errors;
		}

		/// <summary>Test the case where we do a mark outside of a reset.</summary>
		/// <remarks>
		/// Test the case where we do a mark outside of a reset. Test for both file
		/// and memory caches
		/// </remarks>
		/// <param name="key"/>
		/// <param name="values"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private static int Test1(IntWritable key, MarkableIterator<IntWritable> values)
		{
			IntWritable i;
			int errors = 0;
			int count = 0;
			AList<IntWritable> expectedValues = new AList<IntWritable>();
			AList<IntWritable> expectedValues1 = new AList<IntWritable>();
			Log.Info("Executing TEST:1 for Key:" + key);
			values.Mark();
			Log.Info("TEST:1. Marking");
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				expectedValues.AddItem(i);
				if (count == 2)
				{
					break;
				}
				count++;
			}
			values.Reset();
			Log.Info("TEST:1. Reset");
			count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count < expectedValues.Count)
				{
					if (i != expectedValues[count])
					{
						errors++;
						Log.Info("TEST:1. Check:1 Expected: " + expectedValues[count] + ", Got: " + i);
						return errors;
					}
				}
				// We have moved passed the first mark, but still in the memory cache
				if (count == 3)
				{
					values.Mark();
					Log.Info("TEST:1. Marking -- " + key + ": " + i);
				}
				if (count >= 3)
				{
					expectedValues1.AddItem(i);
				}
				if (count == 5)
				{
					break;
				}
				count++;
			}
			if (count < expectedValues.Count)
			{
				Log.Info(("TEST:1 Check:2. Iterator returned lesser values"));
				errors++;
				return errors;
			}
			values.Reset();
			count = 0;
			Log.Info("TEST:1. Reset");
			expectedValues.Clear();
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count < expectedValues1.Count)
				{
					if (i != expectedValues1[count])
					{
						errors++;
						Log.Info("TEST:1. Check:3 Expected: " + expectedValues1[count] + ", Got: " + i);
						return errors;
					}
				}
				// We have moved passed the previous mark, but now we are in the file
				// cache
				if (count == 25)
				{
					values.Mark();
					Log.Info("TEST:1. Marking -- " + key + ":" + i);
				}
				if (count >= 25)
				{
					expectedValues.AddItem(i);
				}
				count++;
			}
			if (count < expectedValues1.Count)
			{
				Log.Info(("TEST:1 Check:4. Iterator returned fewer values"));
				errors++;
				return errors;
			}
			values.Reset();
			Log.Info("TEST:1. Reset");
			count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (i != expectedValues[count])
				{
					errors++;
					Log.Info("TEST:1. Check:5 Expected: " + expectedValues[count] + ", Got: " + i);
					return errors;
				}
			}
			Log.Info("TEST:1 Done");
			return errors;
		}

		/// <summary>Test the case where we do a mark inside a reset.</summary>
		/// <remarks>
		/// Test the case where we do a mark inside a reset. Test for both file
		/// and memory
		/// </remarks>
		/// <param name="key"/>
		/// <param name="values"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private static int Test2(IntWritable key, MarkableIterator<IntWritable> values)
		{
			IntWritable i;
			int errors = 0;
			int count = 0;
			AList<IntWritable> expectedValues = new AList<IntWritable>();
			AList<IntWritable> expectedValues1 = new AList<IntWritable>();
			Log.Info("Executing TEST:2 for Key:" + key);
			values.Mark();
			Log.Info("TEST:2 Marking");
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				expectedValues.AddItem(i);
				if (count == 8)
				{
					break;
				}
				count++;
			}
			values.Reset();
			count = 0;
			Log.Info("TEST:2 reset");
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count < expectedValues.Count)
				{
					if (i != expectedValues[count])
					{
						errors++;
						Log.Info("TEST:2. Check:1 Expected: " + expectedValues[count] + ", Got: " + i);
						return errors;
					}
				}
				// We have moved passed the first mark, but still reading from the
				// memory cache
				if (count == 3)
				{
					values.Mark();
					Log.Info("TEST:2. Marking -- " + key + ":" + i);
				}
				if (count >= 3)
				{
					expectedValues1.AddItem(i);
				}
				count++;
			}
			values.Reset();
			Log.Info("TEST:2. Reset");
			expectedValues.Clear();
			count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count < expectedValues1.Count)
				{
					if (i != expectedValues1[count])
					{
						errors++;
						Log.Info("TEST:2. Check:2 Expected: " + expectedValues1[count] + ", Got: " + i);
						return errors;
					}
				}
				// We have moved passed the previous mark, but now we are in the file
				// cache
				if (count == 20)
				{
					values.Mark();
					Log.Info("TEST:2. Marking -- " + key + ":" + i);
				}
				if (count >= 20)
				{
					expectedValues.AddItem(i);
				}
				count++;
			}
			values.Reset();
			count = 0;
			Log.Info("TEST:2. Reset");
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (i != expectedValues[count])
				{
					errors++;
					Log.Info("TEST:2. Check:1 Expected: " + expectedValues[count] + ", Got: " + i);
					return errors;
				}
			}
			Log.Info("TEST:2 Done");
			return errors;
		}

		/// <summary>Test "clearMark"</summary>
		/// <param name="key"/>
		/// <param name="values"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private static int Test3(IntWritable key, MarkableIterator<IntWritable> values)
		{
			int errors = 0;
			IntWritable i;
			AList<IntWritable> expectedValues = new AList<IntWritable>();
			Log.Info("Executing TEST:3 for Key:" + key);
			values.Mark();
			Log.Info("TEST:3. Marking");
			int count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count == 5)
				{
					Log.Info("TEST:3. Clearing Mark");
					values.ClearMark();
				}
				if (count == 8)
				{
					Log.Info("TEST:3. Marking -- " + key + ":" + i);
					values.Mark();
				}
				if (count >= 8)
				{
					expectedValues.AddItem(i);
				}
				count++;
			}
			values.Reset();
			Log.Info("TEST:3. After reset");
			if (!values.HasNext())
			{
				errors++;
				Log.Info("TEST:3, Check:1. HasNext returned false");
				return errors;
			}
			count = 0;
			while (values.HasNext())
			{
				i = values.Next();
				Log.Info(key + ":" + i);
				if (count < expectedValues.Count)
				{
					if (i != expectedValues[count])
					{
						errors++;
						Log.Info("TEST:2. Check:1 Expected: " + expectedValues[count] + ", Got: " + i);
						return errors;
					}
				}
				if (count == 10)
				{
					values.ClearMark();
					Log.Info("TEST:3. After clear mark");
				}
				count++;
			}
			bool successfulClearMark = false;
			try
			{
				Log.Info("TEST:3. Before Reset");
				values.Reset();
			}
			catch (IOException)
			{
				successfulClearMark = true;
			}
			if (!successfulClearMark)
			{
				Log.Info("TEST:3 Check:4 reset was successfule even after clearMark");
				errors++;
				return errors;
			}
			Log.Info("TEST:3 Done.");
			return errors;
		}

		/// <exception cref="System.Exception"/>
		public virtual void CreateInput()
		{
			// Just create one line files. We use this only to
			// control the number of map tasks
			for (int i = 0; i < NumMaps; i++)
			{
				Path file = new Path(TestRootDir + "/in", "test" + i + ".txt");
				localFs.Delete(file, false);
				OutputStream os = localFs.Create(file);
				TextWriter wr = new OutputStreamWriter(os);
				wr.Write("dummy");
				wr.Close();
			}
		}

		public virtual void TestValueIterReset()
		{
			try
			{
				Configuration conf = new Configuration();
				Job job = Job.GetInstance(conf, "TestValueIterReset");
				job.SetJarByClass(typeof(TestValueIterReset));
				job.SetMapperClass(typeof(TestValueIterReset.TestMapper));
				job.SetReducerClass(typeof(TestValueIterReset.TestReducer));
				job.SetNumReduceTasks(NumTests);
				job.SetMapOutputKeyClass(typeof(IntWritable));
				job.SetMapOutputValueClass(typeof(IntWritable));
				job.SetOutputKeyClass(typeof(IntWritable));
				job.SetOutputValueClass(typeof(IntWritable));
				job.GetConfiguration().SetInt(MRJobConfig.ReduceMarkresetBufferSize, 128);
				job.SetInputFormatClass(typeof(TextInputFormat));
				job.SetOutputFormatClass(typeof(TextOutputFormat));
				FileInputFormat.AddInputPath(job, new Path(TestRootDir + "/in"));
				Path output = new Path(TestRootDir + "/out");
				localFs.Delete(output, true);
				FileOutputFormat.SetOutputPath(job, output);
				CreateInput();
				NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
				ValidateOutput();
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.IsTrue(false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateOutput()
		{
			Path[] outputFiles = FileUtil.Stat2Paths(localFs.ListStatus(new Path(TestRootDir 
				+ "/out"), new Utils.OutputFileUtils.OutputFilesFilter()));
			if (outputFiles.Length > 0)
			{
				InputStream @is = localFs.Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				while (line != null)
				{
					StringTokenizer tokeniz = new StringTokenizer(line, "\t");
					string key = tokeniz.NextToken();
					string value = tokeniz.NextToken();
					Log.Info("Output: key: " + key + " value: " + value);
					int errors = System.Convert.ToInt32(value);
					NUnit.Framework.Assert.IsTrue(errors == 0);
					line = reader.ReadLine();
				}
				reader.Close();
			}
		}
	}
}
