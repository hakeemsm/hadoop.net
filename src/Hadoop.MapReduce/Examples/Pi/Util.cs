using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>Utility methods</summary>
	public class Util
	{
		/// <summary>Output stream</summary>
		public static readonly TextWriter @out = System.Console.Out;

		/// <summary>Error stream</summary>
		public static readonly TextWriter err = System.Console.Out;

		/// <summary>Timer</summary>
		public class Timer
		{
			private readonly bool isAccumulative;

			private readonly long start = Runtime.CurrentTimeMillis();

			private long previous;

			/// <summary>Timer constructor</summary>
			/// <param name="isAccumulative">Is accumulating the time duration?</param>
			public Timer(bool isAccumulative)
			{
				previous = start;
				this.isAccumulative = isAccumulative;
				StackTraceElement[] stack = Sharpen.Thread.CurrentThread().GetStackTrace();
				StackTraceElement e = stack[stack.Length - 1];
				@out.WriteLine(e + " started at " + Sharpen.Extensions.CreateDate(start));
			}

			/// <summary>Same as tick(null).</summary>
			public virtual long Tick()
			{
				return Tick(null);
			}

			/// <summary>Tick</summary>
			/// <param name="s">Output message.  No output if it is null.</param>
			/// <returns>delta</returns>
			public virtual long Tick(string s)
			{
				lock (this)
				{
					long t = Runtime.CurrentTimeMillis();
					long delta = t - (isAccumulative ? start : previous);
					if (s != null)
					{
						@out.Format("%15dms (=%-15s: %s%n", delta, Millis2String(delta) + ")", s);
						@out.Flush();
					}
					previous = t;
					return delta;
				}
			}
		}

		/// <summary>Covert milliseconds to a String.</summary>
		public static string Millis2String(long n)
		{
			if (n < 0)
			{
				return "-" + Millis2String(-n);
			}
			else
			{
				if (n < 1000)
				{
					return n + "ms";
				}
			}
			StringBuilder b = new StringBuilder();
			int millis = (int)(n % 1000L);
			if (millis != 0)
			{
				b.Append(string.Format(".%03d", millis));
			}
			if ((n /= 1000) < 60)
			{
				return b.Insert(0, n).Append("s").ToString();
			}
			b.Insert(0, string.Format(":%02d", (int)(n % 60L)));
			if ((n /= 60) < 60)
			{
				return b.Insert(0, n).ToString();
			}
			b.Insert(0, string.Format(":%02d", (int)(n % 60L)));
			if ((n /= 60) < 24)
			{
				return b.Insert(0, n).ToString();
			}
			b.Insert(0, n % 24L);
			int days = (int)((n /= 24) % 365L);
			b.Insert(0, days == 1 ? " day " : " days ").Insert(0, days);
			if ((n /= 365L) > 0)
			{
				b.Insert(0, n == 1 ? " year " : " years ").Insert(0, n);
			}
			return b.ToString();
		}

		/// <summary>Covert a String to a long.</summary>
		/// <remarks>
		/// Covert a String to a long.
		/// This support comma separated number format.
		/// </remarks>
		public static long String2long(string s)
		{
			return long.Parse(s.Trim().Replace(",", string.Empty));
		}

		/// <summary>Covert a long to a String in comma separated number format.</summary>
		public static string Long2string(long n)
		{
			if (n < 0)
			{
				return "-" + Long2string(-n);
			}
			StringBuilder b = new StringBuilder();
			for (; n >= 1000; n = n / 1000)
			{
				b.Insert(0, string.Format(",%03d", n % 1000));
			}
			return n + b.ToString();
		}

		/// <summary>Parse a variable.</summary>
		public static long ParseLongVariable(string name, string s)
		{
			return String2long(ParseStringVariable(name, s));
		}

		/// <summary>Parse a variable.</summary>
		public static string ParseStringVariable(string name, string s)
		{
			if (!s.StartsWith(name + '='))
			{
				throw new ArgumentException("!s.startsWith(name + '='), name=" + name + ", s=" + 
					s);
			}
			return Sharpen.Runtime.Substring(s, name.Length + 1);
		}

		/// <summary>Execute the callables by a number of threads</summary>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		public static void Execute<T, E>(int nThreads, IList<E> callables)
			where E : Callable<T>
		{
			ExecutorService executor = Executors.NewFixedThreadPool(nThreads);
			IList<Future<T>> futures = executor.InvokeAll(callables);
			foreach (Future<T> f in futures)
			{
				f.Get();
			}
		}

		/// <summary>Print usage messages</summary>
		public static int PrintUsage(string[] args, string usage)
		{
			err.WriteLine("args = " + Arrays.AsList(args));
			err.WriteLine();
			err.WriteLine("Usage: java " + usage);
			err.WriteLine();
			ToolRunner.PrintGenericCommandUsage(err);
			return -1;
		}

		/// <summary>Combine a list of items.</summary>
		public static IList<T> Combine<T>(ICollection<T> items)
			where T : Combinable<T>
		{
			IList<T> sorted = new AList<T>(items);
			if (sorted.Count <= 1)
			{
				return sorted;
			}
			sorted.Sort();
			IList<T> combined = new AList<T>(items.Count);
			T prev = sorted[0];
			for (int i = 1; i < sorted.Count; i++)
			{
				T curr = sorted[i];
				T c = curr.Combine(prev);
				if (c != null)
				{
					prev = c;
				}
				else
				{
					combined.AddItem(prev);
					prev = curr;
				}
			}
			combined.AddItem(prev);
			return combined;
		}

		/// <summary>Check local directory.</summary>
		public static void CheckDirectory(FilePath dir)
		{
			if (!dir.Exists())
			{
				if (!dir.Mkdirs())
				{
					throw new ArgumentException("!dir.mkdirs(), dir=" + dir);
				}
			}
			if (!dir.IsDirectory())
			{
				throw new ArgumentException("dir (=" + dir + ") is not a directory.");
			}
		}

		/// <summary>Create a writer of a local file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static PrintWriter CreateWriter(FilePath dir, string prefix)
		{
			CheckDirectory(dir);
			SimpleDateFormat dateFormat = new SimpleDateFormat("-yyyyMMdd-HHmmssSSS");
			for (; ; )
			{
				FilePath f = new FilePath(dir, prefix + dateFormat.Format(Sharpen.Extensions.CreateDate
					(Runtime.CurrentTimeMillis())) + ".txt");
				if (!f.Exists())
				{
					return new PrintWriter(new OutputStreamWriter(new FileOutputStream(f), Charsets.Utf8
						));
				}
				try
				{
					Sharpen.Thread.Sleep(10);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>Print a "bits skipped" message.</summary>
		public static void PrintBitSkipped(long b)
		{
			@out.WriteLine();
			@out.WriteLine("b = " + Long2string(b) + " (" + (b < 2 ? "bit" : "bits") + " skipped)"
				);
		}

		/// <summary>Convert a pi value to a String.</summary>
		public static string Pi2string(double pi, long terms)
		{
			long value = (long)(pi * (1L << DoublePrecision));
			int acc_bit = Accuracy(terms, false);
			int acc_hex = acc_bit / 4;
			int shift = DoublePrecision - acc_bit;
			return string.Format("%0" + acc_hex + "X %0" + (13 - acc_hex) + "X (%d hex digits)"
				, value >> shift, value & ((1 << shift) - 1), acc_hex);
		}

		internal const int DoublePrecision = 52;

		internal const int MachepsExponent = DoublePrecision + 1;

		//mantissa size
		/// <summary>Estimate accuracy.</summary>
		public static int Accuracy(long terms, bool print)
		{
			double error = terms <= 0 ? 2 : (Math.Log(terms) / Math.Log(2)) / 2;
			int bits = MachepsExponent - (int)Math.Ceil(error);
			if (print)
			{
				@out.WriteLine("accuracy: bits=" + bits + ", terms=" + Long2string(terms) + ", error exponent="
					 + error);
			}
			return bits - bits % 4;
		}

		private const string JobSeparationProperty = "pi.job.separation.seconds";

		private static readonly Semaphore JobSemaphore = Sharpen.Extensions.CreateSemaphore
			(1);

		/// <summary>Run a job.</summary>
		internal static void RunJob(string name, Job job, DistSum.Machine machine, string
			 startmessage, Util.Timer timer)
		{
			JobSemaphore.AcquireUninterruptibly();
			long starttime = null;
			try
			{
				try
				{
					starttime = timer.Tick("starting " + name + " ...\n  " + startmessage);
					//initialize and submit a job
					machine.Init(job);
					job.Submit();
					// Separate jobs
					long sleeptime = 1000L * job.GetConfiguration().GetInt(JobSeparationProperty, 10);
					if (sleeptime > 0)
					{
						Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(name + "> sleep(" + Org.Apache.Hadoop.Examples.PI.Util
							.Millis2String(sleeptime) + ")");
						Sharpen.Thread.Sleep(sleeptime);
					}
				}
				finally
				{
					JobSemaphore.Release();
				}
				if (!job.WaitForCompletion(false))
				{
					throw new RuntimeException(name + " failed.");
				}
			}
			catch (Exception e)
			{
				throw e is RuntimeException ? (RuntimeException)e : new RuntimeException(e);
			}
			finally
			{
				if (starttime != null)
				{
					timer.Tick(name + "> timetaken=" + Org.Apache.Hadoop.Examples.PI.Util.Millis2String
						(timer.Tick() - starttime));
				}
			}
		}

		/// <summary>Read job outputs</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static IList<TaskResult> ReadJobOutputs(FileSystem fs, Path outdir)
		{
			IList<TaskResult> results = new AList<TaskResult>();
			foreach (FileStatus status in fs.ListStatus(outdir))
			{
				if (status.GetPath().GetName().StartsWith("part-"))
				{
					BufferedReader @in = new BufferedReader(new InputStreamReader(fs.Open(status.GetPath
						()), Charsets.Utf8));
					try
					{
						for (string line; (line = @in.ReadLine()) != null; )
						{
							results.AddItem(TaskResult.ValueOf(line));
						}
					}
					finally
					{
						@in.Close();
					}
				}
			}
			if (results.IsEmpty())
			{
				throw new IOException("Output not found");
			}
			return results;
		}

		/// <summary>Write results</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteResults(string name, IList<TaskResult> results, FileSystem
			 fs, string dir)
		{
			Path outfile = new Path(dir, name + ".txt");
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(name + "> writing results to " 
				+ outfile);
			PrintWriter @out = new PrintWriter(new OutputStreamWriter(fs.Create(outfile), Charsets
				.Utf8), true);
			try
			{
				foreach (TaskResult r in results)
				{
					@out.WriteLine(r);
				}
			}
			finally
			{
				@out.Close();
			}
		}

		/// <summary>Create a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static bool CreateNonexistingDirectory(FileSystem fs, Path dir)
		{
			if (fs.Exists(dir))
			{
				Org.Apache.Hadoop.Examples.PI.Util.err.WriteLine("dir (= " + dir + ") already exists."
					);
				return false;
			}
			else
			{
				if (!fs.Mkdirs(dir))
				{
					throw new IOException("Cannot create working directory " + dir);
				}
			}
			fs.SetPermission(dir, new FsPermission((short)0x1ff));
			return true;
		}
	}
}
