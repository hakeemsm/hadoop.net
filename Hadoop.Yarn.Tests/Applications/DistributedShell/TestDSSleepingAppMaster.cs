using System;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class TestDSSleepingAppMaster : ApplicationMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDSSleepingAppMaster
			));

		private const long SleepTime = 5000;

		public static void Main(string[] args)
		{
			bool result = false;
			try
			{
				TestDSSleepingAppMaster appMaster = new TestDSSleepingAppMaster();
				bool doRun = appMaster.Init(args);
				if (!doRun)
				{
					System.Environment.Exit(0);
				}
				appMaster.Run();
				if (appMaster.appAttemptID.GetAttemptId() <= 2)
				{
					try
					{
						// sleep some time
						Sharpen.Thread.Sleep(SleepTime);
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
