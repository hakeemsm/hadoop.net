using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class ContainerLaunchFailAppMaster : ApplicationMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.ContainerLaunchFailAppMaster
			));

		public ContainerLaunchFailAppMaster()
			: base()
		{
		}

		internal override ApplicationMaster.NMCallbackHandler CreateNMCallbackHandler()
		{
			return new ContainerLaunchFailAppMaster.FailContainerLaunchNMCallbackHandler(this
				, this);
		}

		internal class FailContainerLaunchNMCallbackHandler : ApplicationMaster.NMCallbackHandler
		{
			public FailContainerLaunchNMCallbackHandler(ContainerLaunchFailAppMaster _enclosing
				, ApplicationMaster applicationMaster)
				: base(applicationMaster)
			{
				this._enclosing = _enclosing;
			}

			public override void OnContainerStarted(ContainerId containerId, IDictionary<string
				, ByteBuffer> allServiceResponse)
			{
				base.OnStartContainerError(containerId, new RuntimeException("Inject Container Launch failure"
					));
			}

			private readonly ContainerLaunchFailAppMaster _enclosing;
		}

		public static void Main(string[] args)
		{
			bool result = false;
			try
			{
				ContainerLaunchFailAppMaster appMaster = new ContainerLaunchFailAppMaster();
				Log.Info("Initializing ApplicationMaster");
				bool doRun = appMaster.Init(args);
				if (!doRun)
				{
					System.Environment.Exit(0);
				}
				appMaster.Run();
				result = appMaster.Finish();
			}
			catch (Exception t)
			{
				Log.Fatal("Error running ApplicationMaster", t);
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
