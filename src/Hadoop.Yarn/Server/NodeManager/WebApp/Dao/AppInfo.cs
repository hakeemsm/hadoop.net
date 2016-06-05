using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao
{
	public class AppInfo
	{
		protected internal string id;

		protected internal string state;

		protected internal string user;

		protected internal AList<string> containerids;

		public AppInfo()
		{
		}

		public AppInfo(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app)
		{
			// JAXB needs this
			this.id = ConverterUtils.ToString(app.GetAppId());
			this.state = app.GetApplicationState().ToString();
			this.user = app.GetUser();
			this.containerids = new AList<string>();
			IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> appContainers = app.GetContainers();
			foreach (ContainerId containerId in appContainers.Keys)
			{
				string containerIdStr = ConverterUtils.ToString(containerId);
				containerids.AddItem(containerIdStr);
			}
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual string GetState()
		{
			return this.state;
		}

		public virtual AList<string> GetContainers()
		{
			return this.containerids;
		}
	}
}
