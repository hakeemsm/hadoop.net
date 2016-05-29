using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Lib
{
	/// <summary>
	/// ZK Registration Library
	/// currently does not use any authorization
	/// </summary>
	public class ZKClient
	{
		private ZooKeeper zkClient;

		/// <summary>
		/// the zookeeper client library to
		/// talk to zookeeper
		/// </summary>
		/// <param name="string">the host</param>
		/// <exception cref="System.IO.IOException"/>
		public ZKClient(string @string)
		{
			zkClient = new ZooKeeper(@string, 30000, new ZKClient.ZKWatcher());
		}

		/// <summary>register the service to a specific path</summary>
		/// <param name="path">the path in zookeeper namespace to register to</param>
		/// <param name="data">the data that is part of this registration</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void RegisterService(string path, string data)
		{
			try
			{
				zkClient.Create(path, Sharpen.Runtime.GetBytesForString(data, Sharpen.Extensions.GetEncoding
					("UTF-8")), ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral);
			}
			catch (KeeperException ke)
			{
				throw new IOException(ke);
			}
		}

		/// <summary>unregister the service.</summary>
		/// <param name="path">the path at which the service was registered</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void UnregisterService(string path)
		{
			try
			{
				zkClient.Delete(path, -1);
			}
			catch (KeeperException ke)
			{
				throw new IOException(ke);
			}
		}

		/// <summary>list the services registered under a path</summary>
		/// <param name="path">
		/// the path under which services are
		/// registered
		/// </param>
		/// <returns>the list of names of services registered</returns>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public virtual IList<string> ListServices(string path)
		{
			IList<string> children = null;
			try
			{
				children = zkClient.GetChildren(path, false);
			}
			catch (KeeperException ke)
			{
				throw new IOException(ke);
			}
			return children;
		}

		/// <summary>get data published by the service at the registration address</summary>
		/// <param name="path">the path where the service is registered</param>
		/// <returns>the data of the registered service</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetServiceData(string path)
		{
			string data;
			try
			{
				Stat stat = new Stat();
				byte[] byteData = zkClient.GetData(path, false, stat);
				data = new string(byteData, Sharpen.Extensions.GetEncoding("UTF-8"));
			}
			catch (KeeperException ke)
			{
				throw new IOException(ke);
			}
			return data;
		}

		/// <summary>
		/// a watcher class that handles what events from
		/// zookeeper.
		/// </summary>
		private class ZKWatcher : Watcher
		{
			public override void Process(WatchedEvent arg0)
			{
			}
		}
	}
}
