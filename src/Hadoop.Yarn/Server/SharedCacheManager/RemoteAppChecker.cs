using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// An implementation of AppChecker that queries the resource manager remotely to
	/// determine whether the app is running.
	/// </summary>
	public class RemoteAppChecker : AppChecker
	{
		private static readonly EnumSet<YarnApplicationState> ActiveStates = EnumSet.Of(YarnApplicationState
			.New, YarnApplicationState.Accepted, YarnApplicationState.NewSaving, YarnApplicationState
			.Submitted, YarnApplicationState.Running);

		private readonly YarnClient client;

		public RemoteAppChecker()
			: this(YarnClient.CreateYarnClient())
		{
		}

		internal RemoteAppChecker(YarnClient client)
			: base("RemoteAppChecker")
		{
			this.client = client;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			AddService(client);
			base.ServiceInit(conf);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public override bool IsApplicationActive(ApplicationId id)
		{
			ApplicationReport report = null;
			try
			{
				report = client.GetApplicationReport(id);
			}
			catch (ApplicationNotFoundException)
			{
				// the app does not exist
				return false;
			}
			catch (IOException e)
			{
				throw new YarnException(e);
			}
			if (report == null)
			{
				// the app does not exist
				return false;
			}
			return ActiveStates.Contains(report.GetYarnApplicationState());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public override ICollection<ApplicationId> GetActiveApplications()
		{
			try
			{
				IList<ApplicationId> activeApps = new AList<ApplicationId>();
				IList<ApplicationReport> apps = client.GetApplications(ActiveStates);
				foreach (ApplicationReport app in apps)
				{
					activeApps.AddItem(app.GetApplicationId());
				}
				return activeApps;
			}
			catch (IOException e)
			{
				throw new YarnException(e);
			}
		}
	}
}
