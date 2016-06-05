using System.Collections.Generic;
using System.IO;
using Java.Math;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// A map/reduce program that estimates the value of Pi
	/// using a quasi-Monte Carlo (qMC) method.
	/// </summary>
	/// <remarks>
	/// A map/reduce program that estimates the value of Pi
	/// using a quasi-Monte Carlo (qMC) method.
	/// Arbitrary integrals can be approximated numerically by qMC methods.
	/// In this example,
	/// we use a qMC method to approximate the integral $I = \int_S f(x) dx$,
	/// where $S=[0,1)^2$ is a unit square,
	/// $x=(x_1,x_2)$ is a 2-dimensional point,
	/// and $f$ is a function describing the inscribed circle of the square $S$,
	/// $f(x)=1$ if $(2x_1-1)^2+(2x_2-1)^2 &lt;= 1$ and $f(x)=0$, otherwise.
	/// It is easy to see that Pi is equal to $4I$.
	/// So an approximation of Pi is obtained once $I$ is evaluated numerically.
	/// There are better methods for computing Pi.
	/// We emphasize numerical approximation of arbitrary integrals in this example.
	/// For computing many digits of Pi, consider using bbp.
	/// The implementation is discussed below.
	/// Mapper:
	/// Generate points in a unit square
	/// and then count points inside/outside of the inscribed circle of the square.
	/// Reducer:
	/// Accumulate points inside/outside results from the mappers.
	/// Let numTotal = numInside + numOutside.
	/// The fraction numInside/numTotal is a rational approximation of
	/// the value (Area of the circle)/(Area of the square) = $I$,
	/// where the area of the inscribed circle is Pi/4
	/// and the area of unit square is 1.
	/// Finally, the estimated value of Pi is 4(numInside/numTotal).
	/// </remarks>
	public class QuasiMonteCarlo : Configured, Tool
	{
		internal const string Description = "A map/reduce program that estimates Pi using a quasi-Monte Carlo method.";

		/// <summary>tmp directory for input/output</summary>
		private static readonly string TmpDirPrefix = typeof(QuasiMonteCarlo).Name;

		/// <summary>
		/// 2-dimensional Halton sequence {H(i)},
		/// where H(i) is a 2-dimensional point and i &gt;= 1 is the index.
		/// </summary>
		/// <remarks>
		/// 2-dimensional Halton sequence {H(i)},
		/// where H(i) is a 2-dimensional point and i &gt;= 1 is the index.
		/// Halton sequence is used to generate sample points for Pi estimation.
		/// </remarks>
		private class HaltonSequence
		{
			/// <summary>Bases</summary>
			internal static readonly int[] P = new int[] { 2, 3 };

			/// <summary>Maximum number of digits allowed</summary>
			internal static readonly int[] K = new int[] { 63, 40 };

			private long index;

			private double[] x;

			private double[][] q;

			private int[][] d;

			/// <summary>
			/// Initialize to H(startindex),
			/// so the sequence begins with H(startindex+1).
			/// </summary>
			internal HaltonSequence(long startindex)
			{
				index = startindex;
				x = new double[K.Length];
				q = new double[K.Length][];
				d = new int[K.Length][];
				for (int i = 0; i < K.Length; i++)
				{
					q[i] = new double[K[i]];
					d[i] = new int[K[i]];
				}
				for (int i_1 = 0; i_1 < K.Length; i_1++)
				{
					long k = index;
					x[i_1] = 0;
					for (int j = 0; j < K[i_1]; j++)
					{
						q[i_1][j] = (j == 0 ? 1.0 : q[i_1][j - 1]) / P[i_1];
						d[i_1][j] = (int)(k % P[i_1]);
						k = (k - d[i_1][j]) / P[i_1];
						x[i_1] += d[i_1][j] * q[i_1][j];
					}
				}
			}

			/// <summary>Compute next point.</summary>
			/// <remarks>
			/// Compute next point.
			/// Assume the current point is H(index).
			/// Compute H(index+1).
			/// </remarks>
			/// <returns>a 2-dimensional point with coordinates in [0,1)^2</returns>
			internal virtual double[] NextPoint()
			{
				index++;
				for (int i = 0; i < K.Length; i++)
				{
					for (int j = 0; j < K[i]; j++)
					{
						d[i][j]++;
						x[i] += q[i][j];
						if (d[i][j] < P[i])
						{
							break;
						}
						d[i][j] = 0;
						x[i] -= (j == 0 ? 1.0 : q[i][j - 1]);
					}
				}
				return x;
			}
		}

		/// <summary>Mapper class for Pi estimation.</summary>
		/// <remarks>
		/// Mapper class for Pi estimation.
		/// Generate points in a unit square
		/// and then count points inside/outside of the inscribed circle of the square.
		/// </remarks>
		public class QmcMapper : Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable
			>
		{
			/// <summary>Map method.</summary>
			/// <param name="offset">samples starting from the (offset+1)th sample.</param>
			/// <param name="size">the number of samples for this map</param>
			/// <param name="context">output {ture-&gt;numInside, false-&gt;numOutside}</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable offset, LongWritable size, Mapper.Context
				 context)
			{
				QuasiMonteCarlo.HaltonSequence haltonsequence = new QuasiMonteCarlo.HaltonSequence
					(offset.Get());
				long numInside = 0L;
				long numOutside = 0L;
				for (long i = 0; i < size.Get(); )
				{
					//generate points in a unit square
					double[] point = haltonsequence.NextPoint();
					//count points inside/outside of the inscribed circle of the square
					double x = point[0] - 0.5;
					double y = point[1] - 0.5;
					if (x * x + y * y > 0.25)
					{
						numOutside++;
					}
					else
					{
						numInside++;
					}
					//report status
					i++;
					if (i % 1000 == 0)
					{
						context.SetStatus("Generated " + i + " samples.");
					}
				}
				//output map results
				context.Write(new BooleanWritable(true), new LongWritable(numInside));
				context.Write(new BooleanWritable(false), new LongWritable(numOutside));
			}
		}

		/// <summary>Reducer class for Pi estimation.</summary>
		/// <remarks>
		/// Reducer class for Pi estimation.
		/// Accumulate points inside/outside results from the mappers.
		/// </remarks>
		public class QmcReducer : Reducer<BooleanWritable, LongWritable, WritableComparable
			<object>, Writable>
		{
			private long numInside = 0;

			private long numOutside = 0;

			/// <summary>Accumulate number of points inside/outside results from the mappers.</summary>
			/// <param name="isInside">Is the points inside?</param>
			/// <param name="values">An iterator to a list of point counts</param>
			/// <param name="context">dummy, not used here.</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(BooleanWritable isInside, IEnumerable<LongWritable
				> values, Reducer.Context context)
			{
				if (isInside.Get())
				{
					foreach (LongWritable val in values)
					{
						numInside += val.Get();
					}
				}
				else
				{
					foreach (LongWritable val in values)
					{
						numOutside += val.Get();
					}
				}
			}

			/// <summary>Reduce task done, write output to a file.</summary>
			/// <exception cref="System.IO.IOException"/>
			protected override void Cleanup(Reducer.Context context)
			{
				//write output to a file
				Configuration conf = context.GetConfiguration();
				Path outDir = new Path(conf.Get(FileOutputFormat.Outdir));
				Path outFile = new Path(outDir, "reduce-out");
				FileSystem fileSys = FileSystem.Get(conf);
				SequenceFile.Writer writer = SequenceFile.CreateWriter(fileSys, conf, outFile, typeof(
					LongWritable), typeof(LongWritable), SequenceFile.CompressionType.None);
				writer.Append(new LongWritable(numInside), new LongWritable(numOutside));
				writer.Close();
			}
		}

		/// <summary>Run a map/reduce job for estimating Pi.</summary>
		/// <returns>the estimated value of Pi</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public static BigDecimal EstimatePi(int numMaps, long numPoints, Path tmpDir, Configuration
			 conf)
		{
			Job job = Job.GetInstance(conf);
			//setup job conf
			job.SetJobName(typeof(QuasiMonteCarlo).Name);
			job.SetJarByClass(typeof(QuasiMonteCarlo));
			job.SetInputFormatClass(typeof(SequenceFileInputFormat));
			job.SetOutputKeyClass(typeof(BooleanWritable));
			job.SetOutputValueClass(typeof(LongWritable));
			job.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
			job.SetMapperClass(typeof(QuasiMonteCarlo.QmcMapper));
			job.SetReducerClass(typeof(QuasiMonteCarlo.QmcReducer));
			job.SetNumReduceTasks(1);
			// turn off speculative execution, because DFS doesn't handle
			// multiple writers to the same file.
			job.SetSpeculativeExecution(false);
			//setup input/output directories
			Path inDir = new Path(tmpDir, "in");
			Path outDir = new Path(tmpDir, "out");
			FileInputFormat.SetInputPaths(job, inDir);
			FileOutputFormat.SetOutputPath(job, outDir);
			FileSystem fs = FileSystem.Get(conf);
			if (fs.Exists(tmpDir))
			{
				throw new IOException("Tmp directory " + fs.MakeQualified(tmpDir) + " already exists.  Please remove it first."
					);
			}
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Cannot create input directory " + inDir);
			}
			try
			{
				//generate an input file for each map task
				for (int i = 0; i < numMaps; ++i)
				{
					Path file = new Path(inDir, "part" + i);
					LongWritable offset = new LongWritable(i * numPoints);
					LongWritable size = new LongWritable(numPoints);
					SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(LongWritable
						), typeof(LongWritable), SequenceFile.CompressionType.None);
					try
					{
						writer.Append(offset, size);
					}
					finally
					{
						writer.Close();
					}
					System.Console.Out.WriteLine("Wrote input for Map #" + i);
				}
				//start a map/reduce job
				System.Console.Out.WriteLine("Starting Job");
				long startTime = Runtime.CurrentTimeMillis();
				job.WaitForCompletion(true);
				double duration = (Runtime.CurrentTimeMillis() - startTime) / 1000.0;
				System.Console.Out.WriteLine("Job Finished in " + duration + " seconds");
				//read outputs
				Path inFile = new Path(outDir, "reduce-out");
				LongWritable numInside = new LongWritable();
				LongWritable numOutside = new LongWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
				try
				{
					reader.Next(numInside, numOutside);
				}
				finally
				{
					reader.Close();
				}
				//compute estimated value
				BigDecimal numTotal = BigDecimal.ValueOf(numMaps).Multiply(BigDecimal.ValueOf(numPoints
					));
				return BigDecimal.ValueOf(4).SetScale(20).Multiply(BigDecimal.ValueOf(numInside.Get
					())).Divide(numTotal, RoundingMode.HalfUp);
			}
			finally
			{
				fs.Delete(tmpDir, true);
			}
		}

		/// <summary>Parse arguments and then runs a map/reduce job.</summary>
		/// <remarks>
		/// Parse arguments and then runs a map/reduce job.
		/// Print output in standard out.
		/// </remarks>
		/// <returns>a non-zero if there is an error.  Otherwise, return 0.</returns>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: " + GetType().FullName + " <nMaps> <nSamples>"
					);
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
				return 2;
			}
			int nMaps = System.Convert.ToInt32(args[0]);
			long nSamples = long.Parse(args[1]);
			long now = Runtime.CurrentTimeMillis();
			int rand = new Random().Next(int.MaxValue);
			Path tmpDir = new Path(TmpDirPrefix + "_" + now + "_" + rand);
			System.Console.Out.WriteLine("Number of Maps  = " + nMaps);
			System.Console.Out.WriteLine("Samples per Map = " + nSamples);
			System.Console.Out.WriteLine("Estimated value of Pi is " + EstimatePi(nMaps, nSamples
				, tmpDir, GetConf()));
			return 0;
		}

		/// <summary>main method for running it as a stand alone command.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			System.Environment.Exit(ToolRunner.Run(null, new QuasiMonteCarlo(), argv));
		}
	}
}
