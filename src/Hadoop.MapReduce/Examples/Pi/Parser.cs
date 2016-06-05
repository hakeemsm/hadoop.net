using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Examples.PI.Math;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>A class for parsing outputs</summary>
	public sealed class Parser
	{
		internal const string VerboseProperty = "pi.parser.verbose";

		internal readonly bool isVerbose;

		public Parser(bool isVerbose)
		{
			this.isVerbose = isVerbose;
		}

		private void Println(string s)
		{
			if (isVerbose)
			{
				Util.@out.WriteLine(s);
			}
		}

		/// <summary>Parse a line</summary>
		private static void ParseLine(string line, IDictionary<Bellard.Parameter, IList<TaskResult
			>> m)
		{
			//      LOG.info("line = " + line);
			KeyValuePair<string, TaskResult> e = DistSum.String2TaskResult(line);
			if (e != null)
			{
				IList<TaskResult> sums = m[Bellard.Parameter.Get(e.Key)];
				if (sums == null)
				{
					throw new ArgumentException("sums == null, line=" + line + ", e=" + e);
				}
				sums.AddItem(e.Value);
			}
		}

		/// <summary>Parse a file or a directory tree</summary>
		/// <exception cref="System.IO.IOException"/>
		private void Parse(FilePath f, IDictionary<Bellard.Parameter, IList<TaskResult>> 
			sums)
		{
			if (f.IsDirectory())
			{
				Println("Process directory " + f);
				foreach (FilePath child in f.ListFiles())
				{
					Parse(child, sums);
				}
			}
			else
			{
				if (f.GetName().EndsWith(".txt"))
				{
					Println("Parse file " + f);
					IDictionary<Bellard.Parameter, IList<TaskResult>> m = new SortedDictionary<Bellard.Parameter
						, IList<TaskResult>>();
					foreach (Bellard.Parameter p in Bellard.Parameter.Values())
					{
						m[p] = new AList<TaskResult>();
					}
					BufferedReader @in = new BufferedReader(new InputStreamReader(new FileInputStream
						(f), Charsets.Utf8));
					try
					{
						for (string line; (line = @in.ReadLine()) != null; )
						{
							try
							{
								ParseLine(line, m);
							}
							catch (RuntimeException e)
							{
								Util.err.WriteLine("line = " + line);
								throw;
							}
						}
					}
					finally
					{
						@in.Close();
					}
					foreach (Bellard.Parameter p_1 in Bellard.Parameter.Values())
					{
						IList<TaskResult> combined = Util.Combine(m[p_1]);
						if (!combined.IsEmpty())
						{
							Println(p_1 + " (size=" + combined.Count + "):");
							foreach (TaskResult r in combined)
							{
								Println("  " + r);
							}
						}
						Sharpen.Collections.AddAll(sums[p_1], m[p_1]);
					}
				}
			}
		}

		/// <summary>Parse a path</summary>
		/// <exception cref="System.IO.IOException"/>
		private IDictionary<Bellard.Parameter, IList<TaskResult>> Parse(string f)
		{
			IDictionary<Bellard.Parameter, IList<TaskResult>> m = new SortedDictionary<Bellard.Parameter
				, IList<TaskResult>>();
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				m[p] = new AList<TaskResult>();
			}
			Parse(new FilePath(f), m);
			//LOG.info("m=" + m.toString().replace(", ", ",\n  "));
			foreach (Bellard.Parameter p_1 in Bellard.Parameter.Values())
			{
				m[p_1] = m[p_1];
			}
			return m;
		}

		/// <summary>Parse input and re-write results.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal IDictionary<Bellard.Parameter, IList<TaskResult>> Parse(string inputpath
			, string outputdir)
		{
			//parse input
			Util.@out.Write("\nParsing " + inputpath + " ... ");
			Util.@out.Flush();
			IDictionary<Bellard.Parameter, IList<TaskResult>> parsed = Parse(inputpath);
			Util.@out.WriteLine("DONE");
			//re-write the results
			if (outputdir != null)
			{
				Util.@out.Write("\nWriting to " + outputdir + " ...");
				Util.@out.Flush();
				foreach (Bellard.Parameter p in Bellard.Parameter.Values())
				{
					IList<TaskResult> results = parsed[p];
					results.Sort();
					PrintWriter @out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new 
						FilePath(outputdir, p + ".txt")), Charsets.Utf8), true);
					try
					{
						for (int i = 0; i < results.Count; i++)
						{
							@out.WriteLine(DistSum.TaskResult2string(p + "." + i, results[i]));
						}
					}
					finally
					{
						@out.Close();
					}
				}
				Util.@out.WriteLine("DONE");
			}
			return parsed;
		}

		/// <summary>Combine results</summary>
		internal static IDictionary<Bellard.Parameter, T> Combine<T>(IDictionary<Bellard.Parameter
			, IList<T>> m)
			where T : Combinable<T>
		{
			IDictionary<Bellard.Parameter, T> combined = new SortedDictionary<Bellard.Parameter
				, T>();
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				//note: results would never be null due to the design of Util.combine
				IList<T> results = Util.Combine(m[p]);
				Util.@out.Format("%-6s => ", p);
				if (results.Count != 1)
				{
					Util.@out.WriteLine(results.ToString().Replace(", ", ",\n           "));
				}
				else
				{
					T r = results[0];
					combined[p] = r;
					Util.@out.WriteLine(r);
				}
			}
			return combined;
		}

		/// <summary>main</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			if (args.Length < 2 || args.Length > 3)
			{
				Util.PrintUsage(args, typeof(Org.Apache.Hadoop.Examples.PI.Parser).FullName + " <b> <inputpath> [<outputdir>]"
					);
			}
			int i = 0;
			long b = Util.String2long(args[i++]);
			string inputpath = args[i++];
			string outputdir = args.Length >= 3 ? args[i++] : null;
			//read input
			IDictionary<Bellard.Parameter, IList<TaskResult>> parsed = new Org.Apache.Hadoop.Examples.PI.Parser
				(true).Parse(inputpath, outputdir);
			IDictionary<Bellard.Parameter, TaskResult> combined = Combine(parsed);
			long duration = 0;
			foreach (TaskResult r in combined.Values)
			{
				duration += r.GetDuration();
			}
			//print pi
			double pi = Bellard.ComputePi(b, combined);
			Util.PrintBitSkipped(b);
			Util.@out.WriteLine(Util.Pi2string(pi, Bellard.Bit2terms(b)));
			Util.@out.WriteLine("cpu time = " + Util.Millis2String(duration));
		}
	}
}
