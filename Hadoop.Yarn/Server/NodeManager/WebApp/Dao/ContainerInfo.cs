using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao
{
	public class ContainerInfo
	{
		protected internal string id;

		protected internal string state;

		protected internal int exitCode;

		protected internal string diagnostics;

		protected internal string user;

		protected internal long totalMemoryNeededMB;

		protected internal long totalVCoresNeeded;

		protected internal string containerLogsLink;

		protected internal string nodeId;

		[XmlTransient]
		protected internal string containerLogsShortLink;

		[XmlTransient]
		protected internal string exitStatus;

		public ContainerInfo()
		{
		}

		public ContainerInfo(Context nmContext, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container)
			: this(nmContext, container, string.Empty, string.Empty)
		{
		}

		public ContainerInfo(Context nmContext, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, string requestUri, string pathPrefix)
		{
			// JAXB needs this
			this.id = container.GetContainerId().ToString();
			this.nodeId = nmContext.GetNodeId().ToString();
			ContainerStatus containerData = container.CloneAndGetContainerStatus();
			this.exitCode = containerData.GetExitStatus();
			this.exitStatus = (this.exitCode == ContainerExitStatus.Invalid) ? "N/A" : exitCode
				.ToString();
			this.state = container.GetContainerState().ToString();
			this.diagnostics = containerData.GetDiagnostics();
			if (this.diagnostics == null || this.diagnostics.IsEmpty())
			{
				this.diagnostics = string.Empty;
			}
			this.user = container.GetUser();
			Resource res = container.GetResource();
			if (res != null)
			{
				this.totalMemoryNeededMB = res.GetMemory();
				this.totalVCoresNeeded = res.GetVirtualCores();
			}
			this.containerLogsShortLink = StringHelper.Ujoin("containerlogs", this.id, container
				.GetUser());
			if (requestUri == null)
			{
				requestUri = string.Empty;
			}
			if (pathPrefix == null)
			{
				pathPrefix = string.Empty;
			}
			this.containerLogsLink = StringHelper.Join(requestUri, pathPrefix, this.containerLogsShortLink
				);
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual string GetNodeId()
		{
			return this.nodeId;
		}

		public virtual string GetState()
		{
			return this.state;
		}

		public virtual int GetExitCode()
		{
			return this.exitCode;
		}

		public virtual string GetExitStatus()
		{
			return this.exitStatus;
		}

		public virtual string GetDiagnostics()
		{
			return this.diagnostics;
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual string GetShortLogLink()
		{
			return this.containerLogsShortLink;
		}

		public virtual string GetLogLink()
		{
			return this.containerLogsLink;
		}

		public virtual long GetMemoryNeeded()
		{
			return this.totalMemoryNeededMB;
		}

		public virtual long GetVCoresNeeded()
		{
			return this.totalVCoresNeeded;
		}
	}
}
