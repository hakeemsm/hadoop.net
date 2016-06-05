using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class ClusterInfo
	{
		protected internal long id;

		protected internal long startedOn;

		protected internal Service.STATE state;

		protected internal HAServiceProtocol.HAServiceState haState;

		protected internal string rmStateStoreName;

		protected internal string resourceManagerVersion;

		protected internal string resourceManagerBuildVersion;

		protected internal string resourceManagerVersionBuiltOn;

		protected internal string hadoopVersion;

		protected internal string hadoopBuildVersion;

		protected internal string hadoopVersionBuiltOn;

		protected internal string haZooKeeperConnectionState;

		public ClusterInfo()
		{
		}

		public ClusterInfo(ResourceManager rm)
		{
			// JAXB needs this
			long ts = ResourceManager.GetClusterTimeStamp();
			this.id = ts;
			this.state = rm.GetServiceState();
			this.haState = rm.GetRMContext().GetHAServiceState();
			this.rmStateStoreName = rm.GetRMContext().GetStateStore().GetType().FullName;
			this.startedOn = ts;
			this.resourceManagerVersion = YarnVersionInfo.GetVersion();
			this.resourceManagerBuildVersion = YarnVersionInfo.GetBuildVersion();
			this.resourceManagerVersionBuiltOn = YarnVersionInfo.GetDate();
			this.hadoopVersion = VersionInfo.GetVersion();
			this.hadoopBuildVersion = VersionInfo.GetBuildVersion();
			this.hadoopVersionBuiltOn = VersionInfo.GetDate();
			this.haZooKeeperConnectionState = rm.GetRMContext().GetRMAdminService().GetHAZookeeperConnectionState
				();
		}

		public virtual string GetState()
		{
			return this.state.ToString();
		}

		public virtual string GetHAState()
		{
			return this.haState.ToString();
		}

		public virtual string GetRMStateStore()
		{
			return this.rmStateStoreName;
		}

		public virtual string GetRMVersion()
		{
			return this.resourceManagerVersion;
		}

		public virtual string GetRMBuildVersion()
		{
			return this.resourceManagerBuildVersion;
		}

		public virtual string GetRMVersionBuiltOn()
		{
			return this.resourceManagerVersionBuiltOn;
		}

		public virtual string GetHadoopVersion()
		{
			return this.hadoopVersion;
		}

		public virtual string GetHadoopBuildVersion()
		{
			return this.hadoopBuildVersion;
		}

		public virtual string GetHadoopVersionBuiltOn()
		{
			return this.hadoopVersionBuiltOn;
		}

		public virtual long GetClusterId()
		{
			return this.id;
		}

		public virtual long GetStartedOn()
		{
			return this.startedOn;
		}

		public virtual string GetHAZookeeperConnectionState()
		{
			return this.haZooKeeperConnectionState;
		}
	}
}
