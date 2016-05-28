using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestInputSampler
	{
		internal class SequentialSplit : InputSplit
		{
			private int i;

			internal SequentialSplit(int i)
			{
				this.i = i;
			}

			public override long GetLength()
			{
				return 0;
			}

			public override string[] GetLocations()
			{
				return new string[0];
			}

			public virtual int GetInit()
			{
				return i;
			}
		}

		internal class MapredSequentialSplit : InputSplit
		{
			private int i;

			internal MapredSequentialSplit(int i)
			{
				this.i = i;
			}

			public virtual long GetLength()
			{
				return 0;
			}

			public virtual string[] GetLocations()
			{
				return new string[0];
			}

			public virtual int GetInit()
			{
				return i;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}
		}

		internal class TestInputSamplerIF : InputFormat<IntWritable, NullWritable>
		{
			internal readonly int maxDepth;

			internal readonly AList<InputSplit> splits = new AList<InputSplit>();

			internal TestInputSamplerIF(int maxDepth, int numSplits, params int[] splitInit)
			{
				this.maxDepth = maxDepth;
				System.Diagnostics.Debug.Assert(splitInit.Length == numSplits);
				for (int i = 0; i < numSplits; ++i)
				{
					splits.AddItem(new TestInputSampler.SequentialSplit(splitInit[i]));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IList<InputSplit> GetSplits(JobContext context)
			{
				return splits;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader<IntWritable, NullWritable> CreateRecordReader(InputSplit
				 split, TaskAttemptContext context)
			{
				return new _RecordReader_93(this);
			}

			private sealed class _RecordReader_93 : RecordReader<IntWritable, NullWritable>
			{
				public _RecordReader_93(TestInputSamplerIF _enclosing)
				{
					this._enclosing = _enclosing;
					this.i = new IntWritable();
				}

				private int maxVal;

				private readonly IntWritable i;

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
					this.i.Set(((TestInputSampler.SequentialSplit)split).GetInit() - 1);
					this.maxVal = this.i.Get() + this._enclosing.maxDepth + 1;
				}

				public override bool NextKeyValue()
				{
					this.i.Set(this.i.Get() + 1);
					return this.i.Get() < this.maxVal;
				}

				public override IntWritable GetCurrentKey()
				{
					return this.i;
				}

				public override NullWritable GetCurrentValue()
				{
					return NullWritable.Get();
				}

				public override float GetProgress()
				{
					return 1.0f;
				}

				public override void Close()
				{
				}

				private readonly TestInputSamplerIF _enclosing;
			}
		}

		internal class TestMapredInputSamplerIF : TestInputSampler.TestInputSamplerIF, InputFormat
			<IntWritable, NullWritable>
		{
			internal TestMapredInputSamplerIF(int maxDepth, int numSplits, params int[] splitInit
				)
				: base(maxDepth, numSplits, splitInit)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
			{
				IList<InputSplit> splits = null;
				try
				{
					splits = GetSplits(Job.GetInstance(job));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				InputSplit[] retVals = new InputSplit[splits.Count];
				for (int i = 0; i < splits.Count; ++i)
				{
					TestInputSampler.MapredSequentialSplit split = new TestInputSampler.MapredSequentialSplit
						(((TestInputSampler.SequentialSplit)splits[i]).GetInit());
					retVals[i] = split;
				}
				return retVals;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RecordReader<IntWritable, NullWritable> GetRecordReader(InputSplit
				 split, JobConf job, Reporter reporter)
			{
				return new _RecordReader_145(this, split);
			}

			private sealed class _RecordReader_145 : RecordReader<IntWritable, NullWritable>
			{
				public _RecordReader_145(InputSplit split)
				{
					this.split = split;
					this.i = new IntWritable(((TestInputSampler.MapredSequentialSplit)split).GetInit(
						));
					this.maxVal = this.i.Get() + this._enclosing.maxDepth + 1;
				}

				private readonly IntWritable i;

				private int maxVal;

				/// <exception cref="System.IO.IOException"/>
				public bool Next(IntWritable key, NullWritable value)
				{
					this.i.Set(this.i.Get() + 1);
					return this.i.Get() < this.maxVal;
				}

				public IntWritable CreateKey()
				{
					return new IntWritable(this.i.Get());
				}

				public NullWritable CreateValue()
				{
					return NullWritable.Get();
				}

				/// <exception cref="System.IO.IOException"/>
				public long GetPos()
				{
					return 0;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Close()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public float GetProgress()
				{
					return 0;
				}

				private readonly InputSplit split;
			}
		}

		/// <summary>
		/// Verify SplitSampler contract, that an equal number of records are taken
		/// from the first splits.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitSampler()
		{
			// IntWritable comparator not typesafe
			int TotSplits = 15;
			int NumSplits = 5;
			int StepSample = 5;
			int NumSamples = NumSplits * StepSample;
			InputSampler.Sampler<IntWritable, NullWritable> sampler = new InputSampler.SplitSampler
				<IntWritable, NullWritable>(NumSamples, NumSplits);
			int[] inits = new int[TotSplits];
			for (int i = 0; i < TotSplits; ++i)
			{
				inits[i] = i * StepSample;
			}
			Job ignored = Job.GetInstance();
			object[] samples = sampler.GetSample(new TestInputSampler.TestInputSamplerIF(100000
				, TotSplits, inits), ignored);
			NUnit.Framework.Assert.AreEqual(NumSamples, samples.Length);
			Arrays.Sort(samples, new IntWritable.Comparator());
			for (int i_1 = 0; i_1 < NumSamples; ++i_1)
			{
				NUnit.Framework.Assert.AreEqual(i_1, ((IntWritable)samples[i_1]).Get());
			}
		}

		/// <summary>
		/// Verify SplitSampler contract in mapred.lib.InputSampler, which is added
		/// back for binary compatibility of M/R 1.x
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMapredSplitSampler()
		{
			// IntWritable comparator not typesafe
			int TotSplits = 15;
			int NumSplits = 5;
			int StepSample = 5;
			int NumSamples = NumSplits * StepSample;
			InputSampler.Sampler<IntWritable, NullWritable> sampler = new InputSampler.SplitSampler
				<IntWritable, NullWritable>(NumSamples, NumSplits);
			int[] inits = new int[TotSplits];
			for (int i = 0; i < TotSplits; ++i)
			{
				inits[i] = i * StepSample;
			}
			object[] samples = sampler.GetSample(new TestInputSampler.TestMapredInputSamplerIF
				(100000, TotSplits, inits), new JobConf());
			NUnit.Framework.Assert.AreEqual(NumSamples, samples.Length);
			Arrays.Sort(samples, new IntWritable.Comparator());
			for (int i_1 = 0; i_1 < NumSamples; ++i_1)
			{
				// mapred.lib.InputSampler.SplitSampler has a sampling step
				NUnit.Framework.Assert.AreEqual(i_1 % StepSample + TotSplits * (i_1 / StepSample)
					, ((IntWritable)samples[i_1]).Get());
			}
		}

		/// <summary>
		/// Verify IntervalSampler contract, that samples are taken at regular
		/// intervals from the given splits.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIntervalSampler()
		{
			// IntWritable comparator not typesafe
			int TotSplits = 16;
			int PerSplitSample = 4;
			int NumSamples = TotSplits * PerSplitSample;
			double Freq = 1.0 / TotSplits;
			InputSampler.Sampler<IntWritable, NullWritable> sampler = new InputSampler.IntervalSampler
				<IntWritable, NullWritable>(Freq, NumSamples);
			int[] inits = new int[TotSplits];
			for (int i = 0; i < TotSplits; ++i)
			{
				inits[i] = i;
			}
			Job ignored = Job.GetInstance();
			object[] samples = sampler.GetSample(new TestInputSampler.TestInputSamplerIF(NumSamples
				, TotSplits, inits), ignored);
			NUnit.Framework.Assert.AreEqual(NumSamples, samples.Length);
			Arrays.Sort(samples, new IntWritable.Comparator());
			for (int i_1 = 0; i_1 < NumSamples; ++i_1)
			{
				NUnit.Framework.Assert.AreEqual(i_1, ((IntWritable)samples[i_1]).Get());
			}
		}

		/// <summary>
		/// Verify IntervalSampler in mapred.lib.InputSampler, which is added back
		/// for binary compatibility of M/R 1.x
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMapredIntervalSampler()
		{
			// IntWritable comparator not typesafe
			int TotSplits = 16;
			int PerSplitSample = 4;
			int NumSamples = TotSplits * PerSplitSample;
			double Freq = 1.0 / TotSplits;
			InputSampler.Sampler<IntWritable, NullWritable> sampler = new InputSampler.IntervalSampler
				<IntWritable, NullWritable>(Freq, NumSamples);
			int[] inits = new int[TotSplits];
			for (int i = 0; i < TotSplits; ++i)
			{
				inits[i] = i;
			}
			Job ignored = Job.GetInstance();
			object[] samples = sampler.GetSample(new TestInputSampler.TestInputSamplerIF(NumSamples
				, TotSplits, inits), ignored);
			NUnit.Framework.Assert.AreEqual(NumSamples, samples.Length);
			Arrays.Sort(samples, new IntWritable.Comparator());
			for (int i_1 = 0; i_1 < NumSamples; ++i_1)
			{
				NUnit.Framework.Assert.AreEqual(i_1, ((IntWritable)samples[i_1]).Get());
			}
		}
	}
}
