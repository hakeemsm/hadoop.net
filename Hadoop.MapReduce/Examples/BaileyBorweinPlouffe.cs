using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact
	/// digits of Pi.
	/// </summary>
	/// <remarks>
	/// A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact
	/// digits of Pi.
	/// This program is able to calculate digit positions
	/// lower than a certain limit, which is roughly 10^8.
	/// If the limit is exceeded,
	/// the corresponding results may be incorrect due to overflow errors.
	/// For computing higher bits of Pi, consider using distbbp.
	/// Reference:
	/// [1] David H. Bailey, Peter B. Borwein and Simon Plouffe.  On the Rapid
	/// Computation of Various Polylogarithmic Constants.
	/// Math. Comp., 66:903-913, 1996.
	/// </remarks>
	public class BaileyBorweinPlouffe : Configured, Tool
	{
		public const string Description = "A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact digits of Pi.";

		private static readonly string Name = "mapreduce." + typeof(BaileyBorweinPlouffe)
			.Name;

		private static readonly string WorkingDirProperty = Name + ".dir";

		private static readonly string HexFileProperty = Name + ".hex.file";

		private static readonly string DigitStartProperty = Name + ".digit.start";

		private static readonly string DigitSizeProperty = Name + ".digit.size";

		private static readonly string DigitPartsProperty = Name + ".digit.parts";

		private static readonly Log Log = LogFactory.GetLog(typeof(BaileyBorweinPlouffe));

		/// <summary>Mapper class computing digits of Pi.</summary>
		public class BbpMapper : Mapper<LongWritable, IntWritable, LongWritable, BytesWritable
			>
		{
			//custom job properties
			/// <summary>Compute the (offset+1)th to (offset+length)th digits.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable offset, IntWritable length, Mapper.Context
				 context)
			{
				Log.Info("offset=" + offset + ", length=" + length);
				// compute digits
				byte[] bytes = new byte[length.Get() >> 1];
				long d = offset.Get();
				for (int i = 0; i < bytes.Length; d += 4)
				{
					long digits = HexDigits(d);
					bytes[i++] = unchecked((byte)(digits >> 8));
					bytes[i++] = unchecked((byte)digits);
				}
				// output map results
				context.Write(offset, new BytesWritable(bytes));
			}
		}

		/// <summary>Reducer for concatenating map outputs.</summary>
		public class BbpReducer : Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable
			>
		{
			/// <summary>Storing hex digits</summary>
			private readonly IList<byte> hex = new AList<byte>();

			/// <summary>Concatenate map outputs.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable offset, IEnumerable<BytesWritable> values
				, Reducer.Context context)
			{
				// read map outputs
				foreach (BytesWritable bytes in values)
				{
					for (int i = 0; i < bytes.GetLength(); i++)
					{
						hex.AddItem(bytes.GetBytes()[i]);
					}
				}
				Log.Info("hex.size() = " + hex.Count);
			}

			/// <summary>Write output to files.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				Configuration conf = context.GetConfiguration();
				Path dir = new Path(conf.Get(WorkingDirProperty));
				FileSystem fs = dir.GetFileSystem(conf);
				{
					// write hex output
					Path hexfile = new Path(conf.Get(HexFileProperty));
					OutputStream @out = new BufferedOutputStream(fs.Create(hexfile));
					try
					{
						foreach (byte b in hex)
						{
							@out.Write(b);
						}
					}
					finally
					{
						@out.Close();
					}
				}
				// If the starting digit is 1,
				// the hex value can be converted to decimal value.
				if (conf.GetInt(DigitStartProperty, 1) == 1)
				{
					Path outfile = new Path(dir, "pi.txt");
					Log.Info("Writing text output to " + outfile);
					OutputStream outputstream = fs.Create(outfile);
					try
					{
						PrintWriter @out = new PrintWriter(new OutputStreamWriter(outputstream, Charsets.
							Utf8), true);
						// write hex text
						Print(@out, hex.GetEnumerator(), "Pi = 0x3.", "%02X", 5, 5);
						@out.WriteLine("Total number of hexadecimal digits is " + 2 * hex.Count + ".");
						// write decimal text
						BaileyBorweinPlouffe.Fraction dec = new BaileyBorweinPlouffe.Fraction(hex);
						int decDigits = 2 * hex.Count;
						// TODO: this is conservative.
						Print(@out, new _IEnumerator_168(decDigits, dec), "Pi = 3.", "%d", 10, 5);
						@out.WriteLine("Total number of decimal digits is " + decDigits + ".");
					}
					finally
					{
						outputstream.Close();
					}
				}
			}

			private sealed class _IEnumerator_168 : IEnumerator<int>
			{
				public _IEnumerator_168(int decDigits, BaileyBorweinPlouffe.Fraction dec)
				{
					this.decDigits = decDigits;
					this.dec = dec;
					this.i = 0;
				}

				private int i;

				public override bool HasNext()
				{
					return this.i < decDigits;
				}

				public override int Next()
				{
					this.i++;
					return dec.Times10();
				}

				public override void Remove()
				{
				}

				private readonly int decDigits;

				private readonly BaileyBorweinPlouffe.Fraction dec;
			}
		}

		/// <summary>Print out elements in a nice format.</summary>
		private static void Print<T>(PrintWriter @out, IEnumerator<T> iterator, string prefix
			, string format, int elementsPerGroup, int groupsPerLine)
		{
			StringBuilder sb = new StringBuilder("\n");
			for (int i = 0; i < prefix.Length; i++)
			{
				sb.Append(" ");
			}
			string spaces = sb.ToString();
			@out.Write("\n" + prefix);
			for (int i_1 = 0; iterator.HasNext(); i_1++)
			{
				if (i_1 > 0 && i_1 % elementsPerGroup == 0)
				{
					@out.Write((i_1 / elementsPerGroup) % groupsPerLine == 0 ? spaces : " ");
				}
				@out.Write(string.Format(format, iterator.Next()));
			}
			@out.WriteLine();
		}

		/// <summary>
		/// Input split for the
		/// <see cref="BbpInputFormat"/>
		/// .
		/// </summary>
		public class BbpSplit : InputSplit, Writable
		{
			private static readonly string[] Empty = new string[] {  };

			private long offset;

			private int size;

			/// <summary>Public default constructor for the Writable interface.</summary>
			public BbpSplit()
			{
			}

			private BbpSplit(int i, long offset, int size)
			{
				Log.Info("Map #" + i + ": workload=" + Workload(offset, size) + ", offset=" + offset
					 + ", size=" + size);
				this.offset = offset;
				this.size = size;
			}

			private long GetOffset()
			{
				return offset;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override long GetLength()
			{
				return size;
			}

			/// <summary>No location is needed.</summary>
			public override string[] GetLocations()
			{
				return Empty;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				offset = @in.ReadLong();
				size = @in.ReadInt();
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(offset);
				@out.WriteInt(size);
			}
		}

		/// <summary>
		/// Input format for the
		/// <see cref="BbpMapper"/>
		/// .
		/// Keys and values represent offsets and sizes, respectively.
		/// </summary>
		public class BbpInputFormat : InputFormat<LongWritable, IntWritable>
		{
			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override IList<InputSplit> GetSplits(JobContext context)
			{
				//get the property values
				int startDigit = context.GetConfiguration().GetInt(DigitStartProperty, 1);
				int nDigits = context.GetConfiguration().GetInt(DigitSizeProperty, 100);
				int nMaps = context.GetConfiguration().GetInt(DigitPartsProperty, 1);
				//create splits
				IList<InputSplit> splits = new AList<InputSplit>(nMaps);
				int[] parts = Partition(startDigit - 1, nDigits, nMaps);
				for (int i = 0; i < parts.Length; ++i)
				{
					int k = i < parts.Length - 1 ? parts[i + 1] : nDigits + startDigit - 1;
					splits.AddItem(new BaileyBorweinPlouffe.BbpSplit(i, parts[i], k - parts[i]));
				}
				return splits;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override RecordReader<LongWritable, IntWritable> CreateRecordReader(InputSplit
				 generic, TaskAttemptContext context)
			{
				BaileyBorweinPlouffe.BbpSplit split = (BaileyBorweinPlouffe.BbpSplit)generic;
				//return a record reader
				return new _RecordReader_286(split);
			}

			private sealed class _RecordReader_286 : RecordReader<LongWritable, IntWritable>
			{
				public _RecordReader_286(BaileyBorweinPlouffe.BbpSplit split)
				{
					this.split = split;
					this.done = false;
				}

				internal bool done;

				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
				}

				public override bool NextKeyValue()
				{
					//Each record only contains one key.
					return !this.done ? this.done = true : false;
				}

				public override LongWritable GetCurrentKey()
				{
					return new LongWritable(split.GetOffset());
				}

				public override IntWritable GetCurrentValue()
				{
					return new IntWritable((int)split.GetLength());
				}

				public override float GetProgress()
				{
					return this.done ? 1f : 0f;
				}

				public override void Close()
				{
				}

				private readonly BaileyBorweinPlouffe.BbpSplit split;
			}
		}

		/// <summary>Create and setup a job</summary>
		/// <exception cref="System.IO.IOException"/>
		private static Job CreateJob(string name, Configuration conf)
		{
			Job job = Job.GetInstance(conf, Name + "_" + name);
			Configuration jobconf = job.GetConfiguration();
			job.SetJarByClass(typeof(BaileyBorweinPlouffe));
			// setup mapper
			job.SetMapperClass(typeof(BaileyBorweinPlouffe.BbpMapper));
			job.SetMapOutputKeyClass(typeof(LongWritable));
			job.SetMapOutputValueClass(typeof(BytesWritable));
			// setup reducer
			job.SetReducerClass(typeof(BaileyBorweinPlouffe.BbpReducer));
			job.SetOutputKeyClass(typeof(LongWritable));
			job.SetOutputValueClass(typeof(BytesWritable));
			job.SetNumReduceTasks(1);
			// setup input
			job.SetInputFormatClass(typeof(BaileyBorweinPlouffe.BbpInputFormat));
			// disable task timeout
			jobconf.SetLong(MRJobConfig.TaskTimeout, 0);
			// do not use speculative execution
			jobconf.SetBoolean(MRJobConfig.MapSpeculative, false);
			jobconf.SetBoolean(MRJobConfig.ReduceSpeculative, false);
			return job;
		}

		/// <summary>Run a map/reduce job to compute Pi.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void Compute(int startDigit, int nDigits, int nMaps, string workingDir
			, Configuration conf, TextWriter @out)
		{
			string name = startDigit + "_" + nDigits;
			//setup wroking directory
			@out.WriteLine("Working Directory = " + workingDir);
			@out.WriteLine();
			FileSystem fs = FileSystem.Get(conf);
			Path dir = fs.MakeQualified(new Path(workingDir));
			if (fs.Exists(dir))
			{
				throw new IOException("Working directory " + dir + " already exists.  Please remove it first."
					);
			}
			else
			{
				if (!fs.Mkdirs(dir))
				{
					throw new IOException("Cannot create working directory " + dir);
				}
			}
			@out.WriteLine("Start Digit      = " + startDigit);
			@out.WriteLine("Number of Digits = " + nDigits);
			@out.WriteLine("Number of Maps   = " + nMaps);
			// setup a job
			Job job = CreateJob(name, conf);
			Path hexfile = new Path(dir, "pi_" + name + ".hex");
			FileOutputFormat.SetOutputPath(job, new Path(dir, "out"));
			// setup custom properties
			job.GetConfiguration().Set(WorkingDirProperty, dir.ToString());
			job.GetConfiguration().Set(HexFileProperty, hexfile.ToString());
			job.GetConfiguration().SetInt(DigitStartProperty, startDigit);
			job.GetConfiguration().SetInt(DigitSizeProperty, nDigits);
			job.GetConfiguration().SetInt(DigitPartsProperty, nMaps);
			// start a map/reduce job
			@out.WriteLine("\nStarting Job ...");
			long startTime = Runtime.CurrentTimeMillis();
			try
			{
				if (!job.WaitForCompletion(true))
				{
					@out.WriteLine("Job failed.");
					System.Environment.Exit(1);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			finally
			{
				double duration = (Runtime.CurrentTimeMillis() - startTime) / 1000.0;
				@out.WriteLine("Duration is " + duration + " seconds.");
			}
			@out.WriteLine("Output file: " + hexfile);
		}

		/// <summary>Parse arguments and then runs a map/reduce job.</summary>
		/// <returns>a non-zero value if there is an error. Otherwise, return 0.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			if (args.Length != 4)
			{
				System.Console.Error.WriteLine("Usage: java " + GetType().FullName + " <startDigit> <nDigits> <nMaps> <workingDir>"
					);
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
				return -1;
			}
			int startDigit = System.Convert.ToInt32(args[0]);
			int nDigits = System.Convert.ToInt32(args[1]);
			int nMaps = System.Convert.ToInt32(args[2]);
			string workingDir = args[3];
			if (startDigit <= 0)
			{
				throw new ArgumentException("startDigit = " + startDigit + " <= 0");
			}
			else
			{
				if (nDigits <= 0)
				{
					throw new ArgumentException("nDigits = " + nDigits + " <= 0");
				}
				else
				{
					if (nDigits % BbpHexDigits != 0)
					{
						throw new ArgumentException("nDigits = " + nDigits + " is not a multiple of " + BbpHexDigits
							);
					}
					else
					{
						if (nDigits - 1L + startDigit > ImplementationLimit + BbpHexDigits)
						{
							throw new NotSupportedException("nDigits - 1 + startDigit = " + (nDigits - 1L + startDigit
								) + " > IMPLEMENTATION_LIMIT + BBP_HEX_DIGITS," + ", where IMPLEMENTATION_LIMIT="
								 + ImplementationLimit + "and BBP_HEX_DIGITS=" + BbpHexDigits);
						}
						else
						{
							if (nMaps <= 0)
							{
								throw new ArgumentException("nMaps = " + nMaps + " <= 0");
							}
						}
					}
				}
			}
			Compute(startDigit, nDigits, nMaps, workingDir, GetConf(), System.Console.Out);
			return 0;
		}

		/// <summary>The main method for running it as a stand alone command.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			System.Environment.Exit(ToolRunner.Run(null, new BaileyBorweinPlouffe(), argv));
		}

		/// <summary>Limitation of the program.</summary>
		/// <remarks>
		/// Limitation of the program.
		/// The program may return incorrect results if the limit is exceeded.
		/// The default value is 10^8.
		/// The program probably can handle some higher values such as 2^28.
		/// </remarks>
		private const long ImplementationLimit = 100000000;

		private const long AccuracyBit = 32;

		private const long BbpHexDigits = 4;

		private const long BbpMultiplier = 1 << (4 * BbpHexDigits);

		/////////////////////////////////////////////////////////////////////
		// static fields and methods for Bailey-Borwein-Plouffe algorithm. //
		/////////////////////////////////////////////////////////////////////
		/// <summary>
		/// Compute the exact (d+1)th to (d+
		/// <see cref="BbpHexDigits"/>
		/// )th
		/// hex digits of pi.
		/// </summary>
		internal static long HexDigits(long d)
		{
			if (d < 0)
			{
				throw new ArgumentException("d = " + d + " < 0");
			}
			else
			{
				if (d > ImplementationLimit)
				{
					throw new ArgumentException("d = " + d + " > IMPLEMENTATION_LIMIT = " + ImplementationLimit
						);
				}
			}
			double s1 = Sum(1, d);
			double s4 = Sum(4, d);
			double s5 = Sum(5, d);
			double s6 = Sum(6, d);
			double pi = s1 + s1;
			if (pi >= 1)
			{
				pi--;
			}
			pi *= 2;
			if (pi >= 1)
			{
				pi--;
			}
			pi -= s4;
			if (pi < 0)
			{
				pi++;
			}
			pi -= s4;
			if (pi < 0)
			{
				pi++;
			}
			pi -= s5;
			if (pi < 0)
			{
				pi++;
			}
			pi -= s6;
			if (pi < 0)
			{
				pi++;
			}
			return (long)(pi * BbpMultiplier);
		}

		/// <summary>
		/// Approximate the fraction part of
		/// $16^d \sum_{k=0}^\infty \frac{16^{d-k}}{8k+j}$
		/// for d &gt; 0 and j = 1, 4, 5, 6.
		/// </summary>
		private static double Sum(long j, long d)
		{
			long k = j == 1 ? 1 : 0;
			double s = 0;
			if (k <= d)
			{
				s = 1.0 / ((d << 3) | j);
				for (; k < d; k++)
				{
					long n = (k << 3) | j;
					s += Mod((d - k) << 2, n) * 1.0 / n;
					if (s >= 1)
					{
						s--;
					}
				}
				k++;
			}
			if (k >= 1L << (AccuracyBit - 7))
			{
				return s;
			}
			for (; ; k++)
			{
				long n = (k << 3) | j;
				long shift = (k - d) << 2;
				if (AccuracyBit <= shift || 1L << (AccuracyBit - shift) < n)
				{
					return s;
				}
				s += 1.0 / (n << shift);
				if (s >= 1)
				{
					s--;
				}
			}
		}

		/// <summary>Compute $2^e \mod n$ for e &gt; 0, n &gt; 2</summary>
		internal static long Mod(long e, long n)
		{
			long mask = (e & unchecked((long)(0xFFFFFFFF00000000L))) == 0 ? unchecked((long)(
				0x00000000FFFFFFFFL)) : unchecked((long)(0xFFFFFFFF00000000L));
			mask &= (e & unchecked((long)(0xFFFF0000FFFF0000L)) & mask) == 0 ? unchecked((long
				)(0x0000FFFF0000FFFFL)) : unchecked((long)(0xFFFF0000FFFF0000L));
			mask &= (e & unchecked((long)(0xFF00FF00FF00FF00L)) & mask) == 0 ? unchecked((long
				)(0x00FF00FF00FF00FFL)) : unchecked((long)(0xFF00FF00FF00FF00L));
			mask &= (e & unchecked((long)(0xF0F0F0F0F0F0F0F0L)) & mask) == 0 ? unchecked((long
				)(0x0F0F0F0F0F0F0F0FL)) : unchecked((long)(0xF0F0F0F0F0F0F0F0L));
			mask &= (e & unchecked((long)(0xCCCCCCCCCCCCCCCCL)) & mask) == 0 ? unchecked((long
				)(0x3333333333333333L)) : unchecked((long)(0xCCCCCCCCCCCCCCCCL));
			mask &= (e & unchecked((long)(0xAAAAAAAAAAAAAAAAL)) & mask) == 0 ? unchecked((long
				)(0x5555555555555555L)) : unchecked((long)(0xAAAAAAAAAAAAAAAAL));
			long r = 2;
			for (mask >>= 1; mask > 0; mask >>= 1)
			{
				r *= r;
				r %= n;
				if ((e & mask) != 0)
				{
					r += r;
					if (r >= n)
					{
						r -= n;
					}
				}
			}
			return r;
		}

		/// <summary>Represent a number x in hex for 1 &gt; x &gt;= 0</summary>
		private class Fraction
		{
			private readonly int[] integers;

			private int first = 0;

			/// <summary>Construct a fraction represented by the bytes.</summary>
			internal Fraction(IList<byte> bytes)
			{
				// only use 24-bit
				// index to the first non-zero integer
				integers = new int[(bytes.Count + 2) / 3];
				for (int i = 0; i < bytes.Count; i++)
				{
					int b = unchecked((int)(0xFF)) & bytes[i];
					integers[integers.Length - 1 - i / 3] |= b << ((2 - i % 3) << 3);
				}
				SkipZeros();
			}

			/// <summary>
			/// Compute y = 10*x and then set x to the fraction part of y, where x is the
			/// fraction represented by this object.
			/// </summary>
			/// <returns>integer part of y</returns>
			internal virtual int Times10()
			{
				int carry = 0;
				for (int i = first; i < integers.Length; i++)
				{
					integers[i] <<= 1;
					integers[i] += carry + (integers[i] << 2);
					carry = integers[i] >> 24;
					integers[i] &= unchecked((int)(0xFFFFFF));
				}
				SkipZeros();
				return carry;
			}

			private void SkipZeros()
			{
				for (; first < integers.Length && integers[first] == 0; first++)
				{
				}
			}
		}

		/// <summary>
		/// Partition input so that the workload of each part is
		/// approximately the same.
		/// </summary>
		internal static int[] Partition(int offset, int size, int nParts)
		{
			int[] parts = new int[nParts];
			long total = Workload(offset, size);
			int remainder = offset % 4;
			parts[0] = offset;
			for (int i = 1; i < nParts; i++)
			{
				long target = offset + i * (total / nParts) + i * (total % nParts) / nParts;
				//search the closest value
				int low = parts[i - 1];
				int high = offset + size;
				for (; high > low + 4; )
				{
					int mid = (high + low - 2 * remainder) / 8 * 4 + remainder;
					long midvalue = Workload(mid);
					if (midvalue == target)
					{
						high = low = mid;
					}
					else
					{
						if (midvalue > target)
						{
							high = mid;
						}
						else
						{
							low = mid;
						}
					}
				}
				parts[i] = high == low ? high : Workload(high) - target > target - Workload(low) ? 
					low : high;
			}
			return parts;
		}

		private const long MaxN = 4294967295L;

		// prevent overflow
		/// <summary>Estimate the workload for input size n (in some unit).</summary>
		private static long Workload(long n)
		{
			if (n < 0)
			{
				throw new ArgumentException("n = " + n + " < 0");
			}
			else
			{
				if (n > MaxN)
				{
					throw new ArgumentException("n = " + n + " > MAX_N = " + MaxN);
				}
			}
			return (n & 1L) == 0L ? (n >> 1) * (n + 1) : n * ((n + 1) >> 1);
		}

		private static long Workload(long offset, long size)
		{
			return Workload(offset + size) - Workload(offset);
		}
	}
}
