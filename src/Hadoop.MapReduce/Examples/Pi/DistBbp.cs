using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Examples.PI.Math;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>
	/// A map/reduce program that uses a BBP-type method to compute exact
	/// binary digits of Pi.
	/// </summary>
	/// <remarks>
	/// A map/reduce program that uses a BBP-type method to compute exact
	/// binary digits of Pi.
	/// This program is designed for computing the n th bit of Pi,
	/// for large n, say n &gt;= 10^8.
	/// For computing lower bits of Pi, consider using bbp.
	/// The actually computation is done by DistSum jobs.
	/// The steps for launching the jobs are:
	/// (1) Initialize parameters.
	/// (2) Create a list of sums.
	/// (3) Read computed values from the given local directory.
	/// (4) Remove the computed values from the sums.
	/// (5) Partition the remaining sums into computation jobs.
	/// (6) Submit the computation jobs to a cluster and then wait for the results.
	/// (7) Write job outputs to the given local directory.
	/// (8) Combine the job outputs and print the Pi bits.
	/// </remarks>
	public sealed class DistBbp : Configured, Tool
	{
		public const string Description = "A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.";

		private readonly Util.Timer timer = new Util.Timer(true);

		/*
		* The command line format is:
		* > hadoop org.apache.hadoop.examples.pi.DistBbp \
		*          <b> <nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>
		*
		* And the parameters are:
		*  <b>         The number of bits to skip, i.e. compute the (b+1)th position.
		*  <nThreads>  The number of working threads.
		*  <nJobs>     The number of jobs per sum.
		*  <type>      'm' for map side job, 'r' for reduce side job, 'x' for mix type.
		*  <nPart>     The number of parts per job.
		*  <remoteDir> Remote directory for submitting jobs.
		*  <localDir>  Local directory for storing output files.
		*
		* Note that it may take a long time to finish all the jobs when <b> is large.
		* If the program is killed in the middle of the execution, the same command with
		* a different <remoteDir> can be used to resume the execution.  For example, suppose
		* we use the following command to compute the (10^15+57)th bit of Pi.
		*
		* > hadoop org.apache.hadoop.examples.pi.DistBbp \
		*          1,000,000,000,000,056 20 1000 x 500 remote/a local/output
		*
		* It uses 20 threads to summit jobs so that there are at most 20 concurrent jobs.
		* Each sum (there are totally 14 sums) is partitioned into 1000 jobs.
		* The jobs will be executed in map-side or reduce-side.  Each job has 500 parts.
		* The remote directory for the jobs is remote/a and the local directory
		* for storing output is local/output.  Depends on the cluster configuration,
		* it may take many days to finish the entire execution.  If the execution is killed,
		* we may resume it by
		*
		* > hadoop org.apache.hadoop.examples.pi.DistBbp \
		*          1,000,000,000,000,056 20 1000 x 500 remote/b local/output
		*/
		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.Exception"/>
		public int Run(string[] args)
		{
			//parse arguments
			if (args.Length != DistSum.Parameters.Count + 1)
			{
				return Org.Apache.Hadoop.Examples.PI.Util.PrintUsage(args, GetType().FullName + " <b> "
					 + DistSum.Parameters.List + "\n  <b> The number of bits to skip, i.e. compute the (b+1)th position."
					 + DistSum.Parameters.Description);
			}
			int i = 0;
			long b = Org.Apache.Hadoop.Examples.PI.Util.String2long(args[i++]);
			DistSum.Parameters parameters = DistSum.Parameters.Parse(args, i);
			if (b < 0)
			{
				throw new ArgumentException("b = " + b + " < 0");
			}
			Org.Apache.Hadoop.Examples.PI.Util.PrintBitSkipped(b);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(parameters);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine();
			//initialize sums
			DistSum distsum = new DistSum();
			distsum.SetConf(GetConf());
			distsum.SetParameters(parameters);
			bool isVerbose = GetConf().GetBoolean(Parser.VerboseProperty, false);
			IDictionary<Bellard.Parameter, IList<TaskResult>> existings = new Parser(isVerbose
				).Parse(parameters.localDir.GetPath(), null);
			Parser.Combine(existings);
			foreach (IList<TaskResult> tr in existings.Values)
			{
				tr.Sort();
			}
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine();
			IDictionary<Bellard.Parameter, Bellard.Sum> sums = Bellard.GetSums(b, parameters.
				nJobs, existings);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine();
			//execute the computations
			Execute(distsum, sums);
			//compute Pi from the sums 
			double pi = Bellard.ComputePi(b, sums);
			Org.Apache.Hadoop.Examples.PI.Util.PrintBitSkipped(b);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(Org.Apache.Hadoop.Examples.PI.Util
				.Pi2string(pi, Bellard.Bit2terms(b)));
			return 0;
		}

		/// <summary>Execute DistSum computations</summary>
		/// <exception cref="System.Exception"/>
		private void Execute(DistSum distsum, IDictionary<Bellard.Parameter, Bellard.Sum>
			 sums)
		{
			IList<DistSum.Computation> computations = new AList<DistSum.Computation>();
			int i = 0;
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				foreach (Summation s in sums[p])
				{
					if (s.GetValue() == null)
					{
						computations.AddItem(new DistSum.Computation(this, i++, p.ToString(), s));
					}
				}
			}
			if (computations.IsEmpty())
			{
				Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine("No computation");
			}
			else
			{
				timer.Tick("execute " + computations.Count + " computation(s)");
				Org.Apache.Hadoop.Examples.PI.Util.Execute(distsum.GetParameters().nThreads, computations
					);
				timer.Tick("done");
			}
		}

		/// <summary>main</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Environment.Exit(ToolRunner.Run(null, new DistBbp(), args));
		}
	}
}
