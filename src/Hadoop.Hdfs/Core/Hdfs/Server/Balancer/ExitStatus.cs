using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>
	/// Exit status - The values associated with each exit status is directly mapped
	/// to the process's exit code in command line.
	/// </summary>
	[System.Serializable]
	public sealed class ExitStatus
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus Success = 
			new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(0);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus InProgress
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(1);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus AlreadyRunning
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-1);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus NoMoveBlock
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-2);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus NoMoveProgress
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-3);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus IoException
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-4);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus IllegalArguments
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-5);

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus Interrupted
			 = new Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus(-6);

		private readonly int code;

		private ExitStatus(int code)
		{
			this.code = code;
		}

		/// <returns>the command line exit code.</returns>
		public int GetExitCode()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Balancer.ExitStatus.code;
		}
	}
}
