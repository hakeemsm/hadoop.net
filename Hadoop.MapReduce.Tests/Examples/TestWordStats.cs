using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	public class TestWordStats
	{
		private const string Input = "src/test/java/org/apache/hadoop/examples/pi/math";

		private const string MeanOutput = "build/data/mean_output";

		private const string MedianOutput = "build/data/median_output";

		private const string StddevOutput = "build/data/stddev_output";

		/// <summary>
		/// Modified internal test class that is designed to read all the files in the
		/// input directory, and find the standard deviation between all of the word
		/// lengths.
		/// </summary>
		public class WordStdDevReader
		{
			private long wordsRead = 0;

			private long wordLengthsRead = 0;

			private long wordLengthsReadSquared = 0;

			public WordStdDevReader()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual double Read(string path)
			{
				FileSystem fs = FileSystem.Get(new Configuration());
				FileStatus[] files = fs.ListStatus(new Path(path));
				foreach (FileStatus fileStat in files)
				{
					if (!fileStat.IsFile())
					{
						continue;
					}
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(new InputStreamReader(fs.Open(fileStat.GetPath())));
						string line;
						while ((line = br.ReadLine()) != null)
						{
							StringTokenizer st = new StringTokenizer(line);
							string word;
							while (st.HasMoreTokens())
							{
								word = st.NextToken();
								this.wordsRead++;
								this.wordLengthsRead += word.Length;
								this.wordLengthsReadSquared += (long)Math.Pow(word.Length, 2.0);
							}
						}
					}
					catch (IOException e)
					{
						System.Console.Out.WriteLine("Output could not be read!");
						throw;
					}
					finally
					{
						br.Close();
					}
				}
				double mean = (((double)this.wordLengthsRead) / ((double)this.wordsRead));
				mean = Math.Pow(mean, 2.0);
				double term = (((double)this.wordLengthsReadSquared / ((double)this.wordsRead)));
				double stddev = Math.Sqrt((term - mean));
				return stddev;
			}
		}

		/// <summary>
		/// Modified internal test class that is designed to read all the files in the
		/// input directory, and find the median length of all the words.
		/// </summary>
		public class WordMedianReader
		{
			private long wordsRead = 0;

			private SortedDictionary<int, int> map = new SortedDictionary<int, int>();

			public WordMedianReader()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual double Read(string path)
			{
				FileSystem fs = FileSystem.Get(new Configuration());
				FileStatus[] files = fs.ListStatus(new Path(path));
				int num = 0;
				foreach (FileStatus fileStat in files)
				{
					if (!fileStat.IsFile())
					{
						continue;
					}
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(new InputStreamReader(fs.Open(fileStat.GetPath())));
						string line;
						while ((line = br.ReadLine()) != null)
						{
							StringTokenizer st = new StringTokenizer(line);
							string word;
							while (st.HasMoreTokens())
							{
								word = st.NextToken();
								this.wordsRead++;
								if (this.map[word.Length] == null)
								{
									this.map[word.Length] = 1;
								}
								else
								{
									int count = this.map[word.Length];
									this.map[word.Length] = count + 1;
								}
							}
						}
					}
					catch (IOException e)
					{
						System.Console.Out.WriteLine("Output could not be read!");
						throw;
					}
					finally
					{
						br.Close();
					}
				}
				int medianIndex1 = (int)Math.Ceil((this.wordsRead / 2.0));
				int medianIndex2 = (int)Math.Floor((this.wordsRead / 2.0));
				foreach (int key in this.map.NavigableKeySet())
				{
					int prevNum = num;
					num += this.map[key];
					if (medianIndex2 >= prevNum && medianIndex1 <= num)
					{
						return key;
					}
					else
					{
						if (medianIndex2 >= prevNum && medianIndex1 < num)
						{
							int nextCurrLen = this.map.NavigableKeySet().GetEnumerator().Next();
							double median = (key + nextCurrLen) / 2.0;
							return median;
						}
					}
				}
				return -1;
			}
		}

		/// <summary>
		/// Modified internal test class that is designed to read all the files in the
		/// input directory, and find the mean length of all the words.
		/// </summary>
		public class WordMeanReader
		{
			private long wordsRead = 0;

			private long wordLengthsRead = 0;

			public WordMeanReader()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual double Read(string path)
			{
				FileSystem fs = FileSystem.Get(new Configuration());
				FileStatus[] files = fs.ListStatus(new Path(path));
				foreach (FileStatus fileStat in files)
				{
					if (!fileStat.IsFile())
					{
						continue;
					}
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(new InputStreamReader(fs.Open(fileStat.GetPath())));
						string line;
						while ((line = br.ReadLine()) != null)
						{
							StringTokenizer st = new StringTokenizer(line);
							string word;
							while (st.HasMoreTokens())
							{
								word = st.NextToken();
								this.wordsRead++;
								this.wordLengthsRead += word.Length;
							}
						}
					}
					catch (IOException e)
					{
						System.Console.Out.WriteLine("Output could not be read!");
						throw;
					}
					finally
					{
						br.Close();
					}
				}
				double mean = (((double)this.wordLengthsRead) / ((double)this.wordsRead));
				return mean;
			}
		}

		/// <summary>Internal class designed to delete the output directory.</summary>
		/// <remarks>
		/// Internal class designed to delete the output directory. Meant solely for
		/// use before and after the test is run; this is so next iterations of the
		/// test do not encounter a "file already exists" error.
		/// </remarks>
		/// <param name="dir">The directory to delete.</param>
		/// <returns>Returns whether the deletion was successful or not.</returns>
		public static bool DeleteDir(FilePath dir)
		{
			if (dir.IsDirectory())
			{
				string[] children = dir.List();
				for (int i = 0; i < children.Length; i++)
				{
					bool success = DeleteDir(new FilePath(dir, children[i]));
					if (!success)
					{
						System.Console.Out.WriteLine("Could not delete directory after test!");
						return false;
					}
				}
			}
			// The directory is now empty so delete it
			return dir.Delete();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			DeleteDir(new FilePath(MeanOutput));
			DeleteDir(new FilePath(MedianOutput));
			DeleteDir(new FilePath(StddevOutput));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTheMean()
		{
			string[] args = new string[2];
			args[0] = Input;
			args[1] = MeanOutput;
			WordMean wm = new WordMean();
			ToolRunner.Run(new Configuration(), wm, args);
			double mean = wm.GetMean();
			// outputs MUST match
			TestWordStats.WordMeanReader wr = new TestWordStats.WordMeanReader();
			NUnit.Framework.Assert.AreEqual(mean, wr.Read(Input), 0.0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTheMedian()
		{
			string[] args = new string[2];
			args[0] = Input;
			args[1] = MedianOutput;
			WordMedian wm = new WordMedian();
			ToolRunner.Run(new Configuration(), wm, args);
			double median = wm.GetMedian();
			// outputs MUST match
			TestWordStats.WordMedianReader wr = new TestWordStats.WordMedianReader();
			NUnit.Framework.Assert.AreEqual(median, wr.Read(Input), 0.0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTheStandardDeviation()
		{
			string[] args = new string[2];
			args[0] = Input;
			args[1] = StddevOutput;
			WordStandardDeviation wsd = new WordStandardDeviation();
			ToolRunner.Run(new Configuration(), wsd, args);
			double stddev = wsd.GetStandardDeviation();
			// outputs MUST match
			TestWordStats.WordStdDevReader wr = new TestWordStats.WordStdDevReader();
			NUnit.Framework.Assert.AreEqual(stddev, wr.Read(Input), 0.0);
		}
	}
}
