using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class ClientCache
	{
		private System.Collections.Generic.IDictionary<javax.net.SocketFactory, org.apache.hadoop.ipc.Client
			> clients = new System.Collections.Generic.Dictionary<javax.net.SocketFactory, org.apache.hadoop.ipc.Client
			>();

		/* Cache a client using its socket factory as the hash key */
		/// <summary>
		/// Construct & cache an IPC client with the user-provided SocketFactory
		/// if no cached client exists.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="factory">SocketFactory for client socket</param>
		/// <param name="valueClass">Class of the expected response</param>
		/// <returns>an IPC client</returns>
		public virtual org.apache.hadoop.ipc.Client getClient(org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory, java.lang.Class valueClass)
		{
			lock (this)
			{
				// Construct & cache client.  The configuration is only used for timeout,
				// and Clients have connection pools.  So we can either (a) lose some
				// connection pooling and leak sockets, or (b) use the same timeout for all
				// configurations.  Since the IPC is usually intended globally, not
				// per-job, we choose (a).
				org.apache.hadoop.ipc.Client client = clients[factory];
				if (client == null)
				{
					client = new org.apache.hadoop.ipc.Client(valueClass, conf, factory);
					clients[factory] = client;
				}
				else
				{
					client.incCount();
				}
				if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Client.LOG.debug("getting client out of cache: " + client);
				}
				return client;
			}
		}

		/// <summary>
		/// Construct & cache an IPC client with the default SocketFactory
		/// and default valueClass if no cached client exists.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>an IPC client</returns>
		public virtual org.apache.hadoop.ipc.Client getClient(org.apache.hadoop.conf.Configuration
			 conf)
		{
			lock (this)
			{
				return getClient(conf, javax.net.SocketFactory.getDefault(), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.ObjectWritable)));
			}
		}

		/// <summary>
		/// Construct & cache an IPC client with the user-provided SocketFactory
		/// if no cached client exists.
		/// </summary>
		/// <remarks>
		/// Construct & cache an IPC client with the user-provided SocketFactory
		/// if no cached client exists. Default response type is ObjectWritable.
		/// </remarks>
		/// <param name="conf">Configuration</param>
		/// <param name="factory">SocketFactory for client socket</param>
		/// <returns>an IPC client</returns>
		public virtual org.apache.hadoop.ipc.Client getClient(org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory)
		{
			lock (this)
			{
				return this.getClient(conf, factory, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ObjectWritable
					)));
			}
		}

		/// <summary>
		/// Stop a RPC client connection
		/// A RPC client is closed only when its reference count becomes zero.
		/// </summary>
		public virtual void stopClient(org.apache.hadoop.ipc.Client client)
		{
			if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
			{
				org.apache.hadoop.ipc.Client.LOG.debug("stopping client from cache: " + client);
			}
			lock (this)
			{
				client.decCount();
				if (client.isZeroReference())
				{
					if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Client.LOG.debug("removing client from cache: " + client);
					}
					Sharpen.Collections.Remove(clients, client.getSocketFactory());
				}
			}
			if (client.isZeroReference())
			{
				if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Client.LOG.debug("stopping actual client because no more references remain: "
						 + client);
				}
				client.stop();
			}
		}
	}
}
