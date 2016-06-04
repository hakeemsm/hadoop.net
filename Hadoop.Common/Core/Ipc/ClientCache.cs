using System;
using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class ClientCache
	{
		private IDictionary<SocketFactory, Client> clients = new Dictionary<SocketFactory
			, Client>();

		/* Cache a client using its socket factory as the hash key */
		/// <summary>
		/// Construct & cache an IPC client with the user-provided SocketFactory
		/// if no cached client exists.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="factory">SocketFactory for client socket</param>
		/// <param name="valueClass">Class of the expected response</param>
		/// <returns>an IPC client</returns>
		public virtual Client GetClient(Configuration conf, SocketFactory factory, Type valueClass
			)
		{
			lock (this)
			{
				// Construct & cache client.  The configuration is only used for timeout,
				// and Clients have connection pools.  So we can either (a) lose some
				// connection pooling and leak sockets, or (b) use the same timeout for all
				// configurations.  Since the IPC is usually intended globally, not
				// per-job, we choose (a).
				Client client = clients[factory];
				if (client == null)
				{
					client = new Client(valueClass, conf, factory);
					clients[factory] = client;
				}
				else
				{
					client.IncCount();
				}
				if (Client.Log.IsDebugEnabled())
				{
					Client.Log.Debug("getting client out of cache: " + client);
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
		public virtual Client GetClient(Configuration conf)
		{
			lock (this)
			{
				return GetClient(conf, SocketFactory.GetDefault(), typeof(ObjectWritable));
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
		public virtual Client GetClient(Configuration conf, SocketFactory factory)
		{
			lock (this)
			{
				return this.GetClient(conf, factory, typeof(ObjectWritable));
			}
		}

		/// <summary>
		/// Stop a RPC client connection
		/// A RPC client is closed only when its reference count becomes zero.
		/// </summary>
		public virtual void StopClient(Client client)
		{
			if (Client.Log.IsDebugEnabled())
			{
				Client.Log.Debug("stopping client from cache: " + client);
			}
			lock (this)
			{
				client.DecCount();
				if (client.IsZeroReference())
				{
					if (Client.Log.IsDebugEnabled())
					{
						Client.Log.Debug("removing client from cache: " + client);
					}
					Sharpen.Collections.Remove(clients, client.GetSocketFactory());
				}
			}
			if (client.IsZeroReference())
			{
				if (Client.Log.IsDebugEnabled())
				{
					Client.Log.Debug("stopping actual client because no more references remain: " + client
						);
				}
				client.Stop();
			}
		}
	}
}
