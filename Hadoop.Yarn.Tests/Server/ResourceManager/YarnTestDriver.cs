using System;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>Driver for Yarn tests.</summary>
	public class YarnTestDriver
	{
		private ProgramDriver pgd;

		public YarnTestDriver()
			: this(new ProgramDriver())
		{
		}

		public YarnTestDriver(ProgramDriver pgd)
		{
			this.pgd = pgd;
			try
			{
				pgd.AddClass(typeof(TestZKRMStateStorePerf).Name, typeof(TestZKRMStateStorePerf), 
					"ZKRMStateStore i/o benchmark.");
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		public virtual void Run(string[] argv)
		{
			int exitCode = -1;
			try
			{
				exitCode = pgd.Run(argv);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			System.Environment.Exit(exitCode);
		}

		public static void Main(string[] argv)
		{
			new Org.Apache.Hadoop.Test.YarnTestDriver().Run(argv);
		}
	}
}
