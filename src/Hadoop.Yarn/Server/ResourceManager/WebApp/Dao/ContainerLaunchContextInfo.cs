using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	/// <summary>
	/// Simple class to allow users to send information required to create a
	/// ContainerLaunchContext which can then be used as part of the
	/// ApplicationSubmissionContext
	/// </summary>
	public class ContainerLaunchContextInfo
	{
		internal Dictionary<string, LocalResourceInfo> local_resources;

		internal Dictionary<string, string> environment;

		internal IList<string> commands;

		internal Dictionary<string, string> servicedata;

		internal CredentialsInfo credentials;

		internal Dictionary<ApplicationAccessType, string> acls;

		public ContainerLaunchContextInfo()
		{
			local_resources = new Dictionary<string, LocalResourceInfo>();
			environment = new Dictionary<string, string>();
			commands = new AList<string>();
			servicedata = new Dictionary<string, string>();
			credentials = new CredentialsInfo();
			acls = new Dictionary<ApplicationAccessType, string>();
		}

		public virtual IDictionary<string, LocalResourceInfo> GetResources()
		{
			return local_resources;
		}

		public virtual IDictionary<string, string> GetEnvironment()
		{
			return environment;
		}

		public virtual IList<string> GetCommands()
		{
			return commands;
		}

		public virtual IDictionary<string, string> GetAuxillaryServiceData()
		{
			return servicedata;
		}

		public virtual CredentialsInfo GetCredentials()
		{
			return credentials;
		}

		public virtual IDictionary<ApplicationAccessType, string> GetAcls()
		{
			return acls;
		}

		public virtual void SetResources(Dictionary<string, LocalResourceInfo> resources)
		{
			this.local_resources = resources;
		}

		public virtual void SetEnvironment(Dictionary<string, string> environment)
		{
			this.environment = environment;
		}

		public virtual void SetCommands(IList<string> commands)
		{
			this.commands = commands;
		}

		public virtual void SetAuxillaryServiceData(Dictionary<string, string> serviceData
			)
		{
			this.servicedata = serviceData;
		}

		public virtual void SetCredentials(CredentialsInfo credentials)
		{
			this.credentials = credentials;
		}

		public virtual void SetAcls(Dictionary<ApplicationAccessType, string> acls)
		{
			this.acls = acls;
		}
	}
}
