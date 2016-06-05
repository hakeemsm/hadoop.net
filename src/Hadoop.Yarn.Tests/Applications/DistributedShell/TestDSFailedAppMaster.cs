using System;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class TestDSFailedAppMaster : ApplicationMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDSFailedAppMaster)
			);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Run()
		{
			base.Run();
			// for the 2nd attempt.
			if (appAttemptID.GetAttemptId() == 2)
			{
				// should reuse the earlier running container, so numAllocatedContainers
				// should be set to 1. And should ask no more containers, so
				// numRequestedContainers should be the same as numTotalContainers.
				// The only container is the container requested by the AM in the first
				// attempt.
				if (numAllocatedContainers.Get() != 1 || numRequestedContainers.Get() != numTotalContainers)
				{
					Log.Info("NumAllocatedContainers is " + numAllocatedContainers.Get() + " and NumRequestedContainers is "
						 + numAllocatedContainers.Get() + ".Application Master failed. exiting");
					System.Environment.Exit(200);
				}
			}
		}

		public static void Main(string[] args)
		{
			bool result = false;
			try
			{
				TestDSFailedAppMaster appMaster = new TestDSFailedAppMaster();
				bool doRun = appMaster.Init(args);
				if (!doRun)
				{
					System.Environment.Exit(0);
				}
				appMaster.Run();
				if (appMaster.appAttemptID.GetAttemptId() == 1)
				{
					try
					{
						// sleep some time, wait for the AM to launch a container.
						Sharpen.Thread.Sleep(3000);
					}
					catch (Exception)
					{
					}
					// fail the first am.
					System.Environment.Exit(100);
				}
				result = appMaster.Finish();
			}
			catch
			{
				System.Environment.Exit(1);
			}
			if (result)
			{
				Log.Info("Application Master completed successfully. exiting");
				System.Environment.Exit(0);
			}
			else
			{
				Log.Info("Application Master failed. exiting");
				System.Environment.Exit(2);
			}
		}
	}
}
