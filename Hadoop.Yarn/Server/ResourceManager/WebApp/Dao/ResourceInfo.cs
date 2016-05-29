using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class ResourceInfo
	{
		internal int memory;

		internal int vCores;

		public ResourceInfo()
		{
		}

		public ResourceInfo(Resource res)
		{
			memory = res.GetMemory();
			vCores = res.GetVirtualCores();
		}

		public virtual int GetMemory()
		{
			return memory;
		}

		public virtual int GetvCores()
		{
			return vCores;
		}

		public override string ToString()
		{
			return "<memory:" + memory + ", vCores:" + vCores + ">";
		}

		public virtual void SetMemory(int memory)
		{
			this.memory = memory;
		}

		public virtual void SetvCores(int vCores)
		{
			this.vCores = vCores;
		}
	}
}
