using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// An operation provides these abstractions and if it desires to perform any
	/// operations it must implement a override of the run() function to provide
	/// varying output to be captured.
	/// </summary>
	internal abstract class Operation
	{
		private ConfigExtractor config;

		private PathFinder finder;

		private string type;

		private Random rnd;

		protected internal Operation(string type, ConfigExtractor cfg, Random rnd)
		{
			this.config = cfg;
			this.type = type;
			this.rnd = rnd;
			// Use a new Random instance so that the sequence of file names produced is
			// the same even in case of unsuccessful operations
			this.finder = new PathFinder(cfg, new Random(rnd.Next()));
		}

		/// <summary>Gets the configuration object this class is using</summary>
		/// <returns>ConfigExtractor</returns>
		protected internal virtual ConfigExtractor GetConfig()
		{
			return this.config;
		}

		/// <summary>Gets the random number generator to use for this operation</summary>
		/// <returns>Random</returns>
		protected internal virtual Random GetRandom()
		{
			return this.rnd;
		}

		/// <summary>Gets the type of operation that this class belongs to</summary>
		/// <returns>String</returns>
		internal virtual string GetType()
		{
			return type;
		}

		/// <summary>Gets the path finding/generating instance that this class is using</summary>
		/// <returns>PathFinder</returns>
		protected internal virtual PathFinder GetFinder()
		{
			return this.finder;
		}

		/*
		* (non-Javadoc)
		*
		* @see java.lang.Object#toString()
		*/
		public override string ToString()
		{
			return GetType();
		}

		/// <summary>
		/// This run() method simply sets up the default output container and adds in a
		/// data member to keep track of the number of operations that occurred
		/// </summary>
		/// <param name="fs">FileSystem object to perform operations with</param>
		/// <returns>
		/// List of operation outputs to be collected and output in the overall
		/// map reduce operation (or empty or null if none)
		/// </returns>
		internal virtual IList<OperationOutput> Run(FileSystem fs)
		{
			IList<OperationOutput> @out = new List<OperationOutput>();
			@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
				.OpCount, 1L));
			return @out;
		}
	}
}
